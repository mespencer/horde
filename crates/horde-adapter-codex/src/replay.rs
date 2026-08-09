//! An [`AgentSession`] backed by recorded app-server output instead of a
//! process.
//!
//! This is how the codec is held to the real protocol. The bytes are what
//! `codex app-server` actually wrote during spike W2c; everything above the
//! transport — decoding, turn assembly, usage idempotence, thread identity — is
//! the same code a live session runs. What is missing is only the part that
//! talks to an operating system.
//!
//! It is therefore not a mock. The production adapter (wave 5) replaces the
//! line source with a child process's stdout and keeps the rest, which is
//! precisely why the conformance suite's verdict on this session means
//! something about the real one.

use std::collections::VecDeque;

use async_trait::async_trait;
use horde_adapter::{
    AdapterError, AgentEvent, AgentSession, PermissionDecision, ProviderSessionId, Result,
    ToolCallId, TurnInput,
};

use crate::codec::decode_line;
use crate::turn::TurnAssembler;

/// Replays recorded app-server output, one turn per message sent.
pub struct ReplaySession {
    /// Remaining turns, each a run of frames ending in the turn's terminal
    /// signal — which for an interrupted turn is `thread/status → idle` and not
    /// `turn/completed`, because Codex emits no terminal frame after an
    /// interrupt.
    turns: VecDeque<Vec<String>>,
    pending: VecDeque<AgentEvent>,
    assembler: TurnAssembler,
    session_id: Option<ProviderSessionId>,
}

impl ReplaySession {
    /// Builds a session from frames already grouped into turns.
    #[must_use]
    pub fn new(turns: Vec<Vec<String>>) -> Self {
        Self {
            turns: turns.into(),
            pending: VecDeque::new(),
            assembler: TurnAssembler::new(),
            session_id: None,
        }
    }

    /// How many recorded turns have not been replayed yet.
    #[must_use]
    pub fn remaining_turns(&self) -> usize {
        self.turns.len()
    }

    fn drain_turn(&mut self, lines: &[String]) -> Result<()> {
        for line in lines {
            let frame = decode_line(line)?;
            self.pending.extend(self.assembler.feed(&frame));
        }
        self.session_id = self.assembler.provider_session_id();
        Ok(())
    }
}

#[async_trait]
impl AgentSession for ReplaySession {
    fn provider_session_id(&self) -> Option<&ProviderSessionId> {
        self.session_id.as_ref()
    }

    async fn send(&mut self, _input: TurnInput) -> Result<()> {
        // The recording fixes what was said and what came back; the message
        // text cannot change the reply, so it is deliberately ignored rather
        // than matched against the recorded request.
        let Some(lines) = self.turns.pop_front() else {
            return Err(AdapterError::HarnessExited { exit_code: None });
        };
        self.drain_turn(&lines)
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        Ok(self.pending.pop_front())
    }

    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        _decision: PermissionDecision,
    ) -> Result<()> {
        // Codex 0.145.0 routes no approvals to the client at all (ADR-0005), so
        // there is never an outstanding request to answer — on a recording or
        // on a live session. Reporting that honestly beats accepting a decision
        // that would go nowhere.
        Err(AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        })
    }

    async fn interrupt(&mut self) -> Result<()> {
        // Tell the assembler, because it cannot infer this: `thread/status →
        // idle` is both how an interrupted turn ends and the ordinary state
        // between turns. Whatever the interrupt did is already in the
        // recording; what is missing is the knowledge that it was asked for.
        self.assembler.note_interrupt_requested();
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.turns.clear();
        self.pending.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentEvent, AgentSession, ToolCallId, TurnInput, TurnStatus};
    use serde_json::json;

    use super::ReplaySession;

    fn one_turn() -> Vec<Vec<String>> {
        vec![vec![
            json!({"method":"thread/started","params":{"thread":{"id":"t1","model":"gpt-5.6-sol"}}})
                .to_string(),
            json!({"method":"turn/started","params":{"turn":{"id":"turn-1"}}}).to_string(),
            json!({"method":"turn/completed",
                   "params":{"turn":{"id":"turn-1","status":"completed","durationMs":10}}})
            .to_string(),
        ]]
    }

    async fn drain(session: &mut ReplaySession) -> Vec<AgentEvent> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = session.next_event().await {
            events.push(event);
        }
        events
    }

    #[tokio::test]
    async fn sending_past_the_end_of_the_recording_reports_the_harness_as_gone() {
        let mut session = ReplaySession::new(one_turn());
        session
            .send(TurnInput::queued("first"))
            .await
            .expect("the recorded turn must replay");

        let error = session
            .send(TurnInput::queued("second"))
            .await
            .expect_err("there is no second recorded turn");
        assert!(error.to_string().contains("exited"));
    }

    #[tokio::test]
    async fn answering_a_permission_request_that_cannot_exist_is_an_error() {
        let mut session = ReplaySession::new(one_turn());
        let error = session
            .answer_permission(
                &ToolCallId::new("item-1"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await
            .expect_err("Codex routes no approvals to the client");
        assert!(error.to_string().contains("item-1"));
    }

    #[tokio::test]
    async fn the_thread_id_becomes_available_once_a_turn_has_replayed() {
        let mut session = ReplaySession::new(one_turn());
        assert!(session.provider_session_id().is_none());

        session
            .send(TurnInput::queued("hi"))
            .await
            .expect("must replay");
        assert_eq!(
            session.provider_session_id().map(ToString::to_string),
            Some("t1".to_owned())
        );
    }

    /// The interrupt path across the session boundary, not just the assembler:
    /// without `interrupt()` having been called, an `idle` closes nothing.
    #[tokio::test]
    async fn idle_closes_a_turn_only_after_an_interrupt_was_asked_for() {
        let idle_only = || {
            vec![vec![
                json!({"method":"thread/started","params":{"thread":{"id":"t1"}}}).to_string(),
                json!({"method":"turn/started","params":{"turn":{"id":"turn-1"}}}).to_string(),
                json!({"method":"thread/status/changed",
                       "params":{"threadId":"t1","status":{"type":"idle"}}})
                .to_string(),
            ]]
        };

        let mut uninterrupted = ReplaySession::new(idle_only());
        uninterrupted
            .send(TurnInput::queued("hi"))
            .await
            .expect("replays");
        let events = drain(&mut uninterrupted).await;
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, AgentEvent::TurnEnded(_))),
            "idle between turns is ordinary and must close nothing"
        );

        let mut interrupted = ReplaySession::new(idle_only());
        interrupted.interrupt().await.expect("noting is infallible");
        interrupted
            .send(TurnInput::queued("hi"))
            .await
            .expect("replays");
        // The interrupt has to land on the turn that is open, so re-assert it
        // now that one exists — this is what a Worker does when the user hits
        // escape mid-turn.
        interrupted.interrupt().await.expect("noting is infallible");
        let events = drain(&mut interrupted).await;
        assert!(
            events.iter().any(|event| matches!(
                event,
                AgentEvent::TurnEnded(outcome) if outcome.status == TurnStatus::Interrupted
            )),
            "after an interrupt, idle is the only signal that ends the turn"
        );
    }
}
