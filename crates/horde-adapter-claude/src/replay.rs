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

use horde_adapter::{
    AdapterError, AgentEvent, LineDecoder, PermissionDecision, ProviderSessionId, Result,
    ToolCallId,
};

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
        // Nothing outstanding under that id: a recording made on the MCP
        // route (W1c) carries no prompts on stdout at all, and a stdio one
        // (W12c) answers each prompt once.
        AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        }
    }

    fn answer_permission(
        &mut self,
        tool_call_id: &ToolCallId,
        decision: &PermissionDecision,
    ) -> Result<()> {
        let _ = decision;
        // On a recording there is no stdin to write the control_response to;
        // the recorded continuation is the harness's reaction to it.
        match self.assembler.resolve_prompt(tool_call_id) {
            Some(_prompt) => Ok(()),
            None => Err(self.no_permission_channel(tool_call_id)),
        }
    }

    fn awaiting_decision(&self) -> Option<ToolCallId> {
        self.assembler.awaiting_decision()
    }
}

/// Replays recorded `claude` output, one turn per message sent.
pub type ReplaySession = horde_adapter::ReplaySession<StreamJsonLines>;

/// Builds a session from stdout lines already grouped into turns.
#[must_use]
pub fn replay(turns: Vec<Vec<String>>) -> ReplaySession {
    ReplaySession::new(StreamJsonLines::default(), turns)
}

/// Builds a session replaying a `--resume`d recording: turn numbering
/// continues after the `turns_taken` a previous process consumed, exactly as
/// the live adapter's revival path arranges it.
#[must_use]
pub fn replay_resuming(turns: Vec<Vec<String>>, turns_taken: u64) -> ReplaySession {
    ReplaySession::new(
        StreamJsonLines {
            assembler: TurnAssembler::resuming_after(turns_taken),
        },
        turns,
    )
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
    async fn an_answer_for_nothing_outstanding_is_refused() {
        let mut session = replay(one_turn());
        let error = session
            .answer_permission(
                &ToolCallId::new("toolu_01"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await
            .expect_err("nothing is waiting under that id");
        assert!(error.to_string().contains("toolu_01"), "{error}");
    }

    /// A recorded stdio prompt stalls the replay exactly as it stalls the
    /// harness, until answered — once.
    #[tokio::test]
    async fn a_recorded_prompt_stalls_until_answered_once() {
        use horde_adapter::{AdapterError, AgentEvent};

        let mut session = replay(vec![vec![
            json!({"type":"system","subtype":"init","session_id":"s1","model":"m"}).to_string(),
            json!({"type":"control_request","request_id":"r1","request":{
                "subtype":"can_use_tool","tool_name":"Write","input":{},"tool_use_id":"toolu_9"}})
            .to_string(),
            json!({"type":"result","subtype":"success","is_error":false,
                   "session_id":"s1","duration_ms":5,"result":"done"})
            .to_string(),
        ]]);
        session.send(TurnInput::queued("go")).await.expect("replays");
        assert!(matches!(
            session.next_event().await,
            Ok(Some(AgentEvent::SessionEstablished { .. }))
        ));
        assert!(matches!(
            session.next_event().await,
            Ok(Some(AgentEvent::PermissionRequested { .. }))
        ));
        assert!(matches!(
            session.next_event().await,
            Err(AdapterError::AwaitingDecision { .. })
        ));
        session
            .answer_permission(
                &ToolCallId::new("toolu_9"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await
            .expect("the prompt is outstanding");
        assert!(
            session
                .answer_permission(
                    &ToolCallId::new("toolu_9"),
                    horde_adapter::PermissionDecision::allow()
                )
                .await
                .is_err(),
            "answered once"
        );
        assert!(matches!(
            session.next_event().await,
            Ok(Some(AgentEvent::TurnEnded(_)))
        ));
    }
}
