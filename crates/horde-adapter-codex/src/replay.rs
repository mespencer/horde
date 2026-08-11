//! An [`AgentSession`](horde_adapter::AgentSession) backed by recorded
//! app-server output instead of a process.
//!
//! The machinery — replaying turns, draining events, tracking thread identity —
//! is shared with every other provider and lives in [`horde_adapter::replay`].
//! What is Codex-specific, and all that lives here, is what one app-server
//! frame means.
//!
//! The bytes are what `codex app-server` actually wrote during spike W2c, so
//! the conformance suite's verdict on a replayed session means something about
//! the live one.

use horde_adapter::{AdapterError, AgentEvent, LineDecoder, ProviderSessionId, Result, ToolCallId};

use crate::codec::decode_line;
use crate::turn::TurnAssembler;

/// Reads one Codex app-server frame into events.
#[derive(Debug, Default)]
pub struct AppServerFrames {
    assembler: TurnAssembler,
}

impl LineDecoder for AppServerFrames {
    fn decode(&mut self, line: &str) -> Result<Vec<AgentEvent>> {
        let frame = decode_line(line)?;
        Ok(self.assembler.feed(&frame))
    }

    fn provider_session_id(&self) -> Option<ProviderSessionId> {
        self.assembler.provider_session_id()
    }

    fn note_interrupt_requested(&mut self) {
        // The assembler cannot infer this: `thread/status → idle` is both how
        // an interrupted turn ends and the ordinary state between turns.
        self.assembler.note_interrupt_requested();
    }

    fn no_permission_channel(&self, tool_call_id: &ToolCallId) -> AdapterError {
        // Codex 0.145.0 routes no approvals to the client at all (ADR-0005), so
        // there is never an outstanding request to answer — on a recording or
        // on a live session.
        AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        }
    }
}

/// Replays recorded Codex output, one turn per message sent.
pub type ReplaySession = horde_adapter::ReplaySession<AppServerFrames>;

/// Builds a session from frames already grouped into turns.
#[must_use]
pub fn replay(turns: Vec<Vec<String>>) -> ReplaySession {
    ReplaySession::new(AppServerFrames::default(), turns)
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentEvent, AgentSession, ToolCallId, TurnInput, TurnStatus};
    use serde_json::json;

    use super::{ReplaySession, replay};

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
        let mut session = replay(one_turn());
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
        let mut session = replay(one_turn());
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
    async fn remaining_turns_counts_down_as_the_recording_drains() {
        let mut session = replay(one_turn());
        assert_eq!(session.remaining_turns(), 1);

        session
            .send(TurnInput::queued("hi"))
            .await
            .expect("replays");
        assert_eq!(session.remaining_turns(), 0);
    }

    #[tokio::test]
    async fn shutdown_discards_the_rest_of_the_recording() {
        let mut session = replay(one_turn());
        session.shutdown().await.expect("shutdown is infallible");

        assert_eq!(session.remaining_turns(), 0, "the recording is dropped");
        assert!(
            session.next_event().await.expect("no error").is_none(),
            "and so are any events that had not been drained"
        );
        // A shut-down session is finished, not merely quiet.
        assert!(session.send(TurnInput::queued("hi")).await.is_err());
    }

    #[tokio::test]
    async fn the_thread_id_becomes_available_once_a_turn_has_replayed() {
        let mut session = replay(one_turn());
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

        let mut uninterrupted = replay(idle_only());
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

        let mut interrupted = replay(idle_only());
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
