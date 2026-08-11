//! Replaying recorded harness output as a session.
//!
//! Every provider's codec needs the same thing to be held to its real protocol:
//! a session whose lines come from a recording instead of a process, so that
//! everything above the transport — decoding, turn assembly, usage
//! normalization, session identity — is the code a live session runs.
//!
//! That machinery is identical across providers; only the per-line decoding
//! differs. So the machinery lives here, once, and each adapter supplies a
//! [`LineDecoder`].
//!
//! This is not a mock. A production adapter replaces the line source with a
//! child process's stdout and keeps the rest, which is precisely why the
//! conformance suite's verdict on a replayed session means something about the
//! live one.

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::adapter::AgentSession;
use crate::error::{AdapterError, Result};
use crate::event::AgentEvent;
use crate::ids::{ProviderSessionId, ToolCallId};
use crate::permission::PermissionDecision;
use crate::spec::TurnInput;

/// One provider's answer to "what does this line mean?".
///
/// Implementations own the wire types and the turn assembler; this trait is
/// only the seam [`ReplaySession`] needs, which is why it speaks in `&str` and
/// [`AgentEvent`] rather than in any provider's frame type.
pub trait LineDecoder: Send {
    /// Decodes one recorded line and returns whatever events it completed.
    ///
    /// # Errors
    ///
    /// Returns [`AdapterError::Protocol`] when the line does not parse — a
    /// recording that no longer decodes means the protocol moved.
    fn decode(&mut self, line: &str) -> Result<Vec<AgentEvent>>;

    /// The provider's own identifier for this conversation, once seen.
    fn provider_session_id(&self) -> Option<ProviderSessionId>;

    /// Notes that the caller asked to interrupt the open turn.
    ///
    /// Whatever the interrupt did is already in the recording; what a decoder
    /// may be missing is the knowledge that it was *asked for*. Codex needs
    /// this — `thread/status → idle` is both how an interrupted turn ends and
    /// the ordinary state between turns, so its assembler cannot tell them
    /// apart on its own. Claude does not, because its recording carries an
    /// explicit `aborted_streaming` result.
    fn note_interrupt_requested(&mut self) {}

    /// Why a recording has no permission channel to answer on.
    ///
    /// Providers differ: one routes approvals over a side channel the recording
    /// does not capture, another routes none at all. Either way there is never
    /// an outstanding request, and saying so beats accepting a decision that
    /// would go nowhere.
    fn no_permission_channel(&self, tool_call_id: &ToolCallId) -> AdapterError {
        AdapterError::UnknownPermissionRequest {
            tool_call_id: tool_call_id.clone(),
        }
    }
}

/// An [`AgentSession`] backed by recorded output, one turn per message sent.
#[derive(Debug)]
pub struct ReplaySession<D: LineDecoder> {
    /// Remaining turns, each a run of lines ending in that provider's terminal
    /// signal for a turn.
    turns: VecDeque<Vec<String>>,
    /// Events decoded for the turn currently being drained.
    pending: VecDeque<AgentEvent>,
    decoder: D,
    session_id: Option<ProviderSessionId>,
}

impl<D: LineDecoder> ReplaySession<D> {
    /// Builds a session from lines already grouped into turns.
    pub fn new(decoder: D, turns: Vec<Vec<String>>) -> Self {
        Self {
            turns: turns.into(),
            pending: VecDeque::new(),
            decoder,
            session_id: None,
        }
    }

    /// How many recorded turns have not been replayed yet.
    #[must_use]
    pub fn remaining_turns(&self) -> usize {
        self.turns.len()
    }

    /// Decodes one turn's lines into pending events.
    fn drain_turn(&mut self, lines: &[String]) -> Result<()> {
        for line in lines {
            self.pending.extend(self.decoder.decode(line)?);
        }
        self.session_id = self.decoder.provider_session_id();
        Ok(())
    }
}

#[async_trait]
impl<D: LineDecoder> AgentSession for ReplaySession<D> {
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
        Err(self.decoder.no_permission_channel(tool_call_id))
    }

    async fn interrupt(&mut self) -> Result<()> {
        // Nothing to cancel — the recording already contains whatever the
        // interrupt did — but a decoder may need to know it was requested.
        self.decoder.note_interrupt_requested();
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
    use super::{LineDecoder, ReplaySession};
    use crate::adapter::AgentSession;
    use crate::error::{AdapterError, Result};
    use crate::event::{AgentEvent, TurnOutcome, TurnStatus};
    use crate::ids::{ProviderSessionId, ToolCallId, TurnId};
    use crate::spec::TurnInput;
    use crate::usage::UsageDelta;

    /// A decoder that treats each line as an event name, so the machinery can
    /// be tested without dragging in a provider's wire types.
    #[derive(Debug, Default)]
    struct Spelled {
        session_id: Option<ProviderSessionId>,
        interrupts: usize,
    }

    impl LineDecoder for Spelled {
        fn decode(&mut self, line: &str) -> Result<Vec<AgentEvent>> {
            match line {
                "established" => {
                    self.session_id = Some(ProviderSessionId::new("s1"));
                    Ok(vec![AgentEvent::SessionEstablished {
                        provider_session_id: ProviderSessionId::new("s1"),
                        model: "test-model".to_owned(),
                    }])
                }
                "end" => Ok(vec![AgentEvent::TurnEnded(TurnOutcome {
                    turn_id: TurnId::new("t1"),
                    status: TurnStatus::Completed,
                    text: None,
                    usage: UsageDelta::default(),
                    denials: Vec::new(),
                    duration: core::time::Duration::ZERO,
                })]),
                other => Err(AdapterError::Protocol {
                    message: format!("unrecognised line `{other}`"),
                    raw: other.to_owned(),
                }),
            }
        }

        fn provider_session_id(&self) -> Option<ProviderSessionId> {
            self.session_id.clone()
        }

        fn note_interrupt_requested(&mut self) {
            self.interrupts += 1;
        }
    }

    fn one_turn() -> Vec<Vec<String>> {
        vec![vec!["established".to_owned(), "end".to_owned()]]
    }

    #[tokio::test]
    async fn a_turn_replays_its_events_in_order_and_publishes_the_session_id() {
        let mut session = ReplaySession::new(Spelled::default(), one_turn());
        assert!(
            session.provider_session_id().is_none(),
            "nothing is known until a turn has replayed"
        );
        session
            .send(TurnInput::queued("hi"))
            .await
            .expect("the recorded turn must replay");

        assert!(matches!(
            session.next_event().await.expect("an event"),
            Some(AgentEvent::SessionEstablished { .. })
        ));
        assert!(matches!(
            session.next_event().await.expect("an event"),
            Some(AgentEvent::TurnEnded(_))
        ));
        assert!(
            session.next_event().await.expect("no error").is_none(),
            "the turn is drained"
        );
        assert_eq!(
            session.provider_session_id().map(ToString::to_string),
            Some("s1".to_owned())
        );
    }

    #[tokio::test]
    async fn sending_past_the_end_of_the_recording_reports_the_harness_as_gone() {
        let mut session = ReplaySession::new(Spelled::default(), one_turn());
        assert_eq!(session.remaining_turns(), 1);
        session
            .send(TurnInput::queued("first"))
            .await
            .expect("the recorded turn must replay");
        assert_eq!(session.remaining_turns(), 0);

        let error = session
            .send(TurnInput::queued("second"))
            .await
            .expect_err("there is no second recorded turn");
        assert!(error.to_string().contains("exited"), "{error}");
    }

    #[tokio::test]
    async fn a_line_that_no_longer_decodes_fails_the_turn() {
        let mut session = ReplaySession::new(Spelled::default(), vec![vec!["moved".to_owned()]]);
        let error = session
            .send(TurnInput::queued("hi"))
            .await
            .expect_err("a recording that stopped decoding means the protocol moved");
        assert!(error.to_string().contains("moved"), "{error}");
    }

    #[tokio::test]
    async fn a_recording_has_no_permission_channel_to_answer_on() {
        let mut session = ReplaySession::new(Spelled::default(), one_turn());
        let error = session
            .answer_permission(
                &ToolCallId::new("toolu_01"),
                crate::permission::PermissionDecision::allow(),
            )
            .await
            .expect_err("a recording carries no permission channel");
        assert!(error.to_string().contains("toolu_01"), "{error}");
    }

    #[tokio::test]
    async fn an_interrupt_reaches_the_decoder_even_though_it_cancels_nothing() {
        let mut session = ReplaySession::new(Spelled::default(), one_turn());
        session.interrupt().await.expect("nothing left to cancel");
        assert_eq!(
            session.decoder.interrupts, 1,
            "a decoder may need to know an interrupt was asked for, even though \
             the recording already contains whatever it did"
        );
    }

    #[tokio::test]
    async fn shutdown_empties_the_recording() {
        let mut session = ReplaySession::new(Spelled::default(), one_turn());
        assert_eq!(session.remaining_turns(), 1);
        session.shutdown().await.expect("shutdown");
        assert_eq!(session.remaining_turns(), 0);
    }
}
