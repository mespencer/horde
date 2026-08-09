//! An [`AgentSession`] backed by recorded harness output instead of a process.
//!
//! This is how the codec is held to the real protocol. The bytes are what
//! `claude` actually wrote during spike W1c; everything above the transport —
//! decoding, turn assembly, usage normalization, session identity — is the same
//! code a live session runs. What is missing is only the part that talks to an
//! operating system.
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

/// Replays recorded harness output, one turn per message sent.
pub struct ReplaySession {
    /// Remaining turns, each a run of stdout lines ending in a `result`.
    turns: VecDeque<Vec<String>>,
    /// Events decoded for the turn currently being drained.
    pending: VecDeque<AgentEvent>,
    assembler: TurnAssembler,
    session_id: Option<ProviderSessionId>,
}

impl ReplaySession {
    /// Builds a session from stdout lines already grouped into turns.
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
}

#[async_trait]
impl AgentSession for ReplaySession {
    fn provider_session_id(&self) -> Option<&ProviderSessionId> {
        self.session_id.as_ref()
    }

    async fn send(&mut self, _input: TurnInput) -> Result<()> {
        // The recording fixes what was said and what came back; the message
        // text cannot change the reply, so it is deliberately ignored rather
        // than matched against the recorded stdin.
        let Some(lines) = self.turns.pop_front() else {
            return Err(AdapterError::HarnessExited { exit_code: None });
        };

        for line in &lines {
            let envelope = decode_line(line)?;
            self.pending.extend(self.assembler.feed(&envelope));
        }
        self.session_id = self.assembler.provider_session_id();
        Ok(())
    }

    async fn next_event(&mut self) -> Result<Option<AgentEvent>> {
        Ok(self.pending.pop_front())
    }

    async fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        _decision: PermissionDecision,
    ) -> Result<()> {
        // Permission prompts reach a live session over the MCP endpoint, not
        // over stdout, so a recording of stdout has nothing to answer. Reporting
        // that honestly beats accepting a decision that would go nowhere.
        Err(AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        })
    }

    async fn interrupt(&mut self) -> Result<()> {
        // Whatever the interrupt did is already in the recording — the
        // transcripts include a turn that ends `aborted_streaming`. There is
        // nothing left to cancel.
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
    use horde_adapter::{AgentSession, ToolCallId, TurnInput};
    use serde_json::json;

    use super::ReplaySession;

    fn one_turn() -> Vec<Vec<String>> {
        vec![vec![
            json!({"type":"system","subtype":"init","session_id":"s1","model":"m"}).to_string(),
            json!({"type":"result","subtype":"success","is_error":false,
                   "session_id":"s1","duration_ms":5,"result":"hi"})
            .to_string(),
        ]]
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
                &ToolCallId::new("toolu_01"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await
            .expect_err("stdout recordings carry no permission channel");
        assert!(error.to_string().contains("toolu_01"));
    }

    #[tokio::test]
    async fn the_session_id_becomes_available_once_a_turn_has_replayed() {
        let mut session = ReplaySession::new(one_turn());
        assert!(session.provider_session_id().is_none());
        session
            .send(TurnInput::queued("hi"))
            .await
            .expect("must replay");
        assert_eq!(
            session.provider_session_id().map(ToString::to_string),
            Some("s1".to_owned())
        );
    }
}
