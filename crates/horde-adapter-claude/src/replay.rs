//! An [`AgentSession`](horde_adapter::AgentSession) backed by recorded harness
//! output instead of a process.
//!
//! The machinery — replaying turns, draining events, tracking session identity
//! — is shared with every other provider and lives in
//! [`horde_adapter::replay`]. What is claude-specific, and all that lives here,
//! is what one line of `claude`'s stdout means.
//!
//! The bytes are what `claude` actually wrote during spike W1c, so the
//! conformance suite's verdict on a replayed session means something about the
//! live one: the production adapter (wave 5) swaps the line source for a child
//! process's stdout and keeps everything above it.

use horde_adapter::{AdapterError, AgentEvent, LineDecoder, ProviderSessionId, Result, ToolCallId};

use crate::codec::decode_line;
use crate::turn::TurnAssembler;

/// Reads one line of `claude` stream-json into events.
#[derive(Debug, Default)]
pub struct StreamJsonLines {
    assembler: TurnAssembler,
}

impl LineDecoder for StreamJsonLines {
    fn decode(&mut self, line: &str) -> Result<Vec<AgentEvent>> {
        let envelope = decode_line(line)?;
        Ok(self.assembler.feed(&envelope))
    }

    fn provider_session_id(&self) -> Option<ProviderSessionId> {
        self.assembler.provider_session_id()
    }

    fn no_permission_channel(&self, tool_call_id: &ToolCallId) -> AdapterError {
        // Permission prompts reach a live session over the MCP endpoint, not
        // over stdout, so a recording of stdout has nothing to answer.
        AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        }
    }
}

/// Replays recorded `claude` output, one turn per message sent.
pub type ReplaySession = horde_adapter::ReplaySession<StreamJsonLines>;

/// Builds a session from stdout lines already grouped into turns.
#[must_use]
pub fn replay(turns: Vec<Vec<String>>) -> ReplaySession {
    ReplaySession::new(StreamJsonLines::default(), turns)
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentSession, ToolCallId, TurnInput};
    use serde_json::json;

    use super::replay;

    fn one_turn() -> Vec<Vec<String>> {
        vec![vec![
            json!({"type":"system","subtype":"init","session_id":"s1","model":"m"}).to_string(),
            json!({"type":"result","subtype":"success","is_error":false,
                   "session_id":"s1","duration_ms":5,"result":"hi"})
            .to_string(),
        ]]
    }

    #[tokio::test]
    async fn a_recorded_turn_establishes_the_session_it_recorded() {
        let mut session = replay(one_turn());
        assert!(session.provider_session_id().is_none());
        session
            .send(TurnInput::queued("hi"))
            .await
            .expect("the recorded turn must replay");
        assert_eq!(
            session.provider_session_id().map(ToString::to_string),
            Some("s1".to_owned())
        );
    }

    #[tokio::test]
    async fn stdout_recordings_carry_no_permission_channel() {
        let mut session = replay(one_turn());
        let error = session
            .answer_permission(
                &ToolCallId::new("toolu_01"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await
            .expect_err("prompts arrive over MCP, not over stdout");
        assert!(error.to_string().contains("toolu_01"), "{error}");
    }
}
