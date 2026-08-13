//! What an adapter streams back while an agent works.
//!
//! Every provider harness is normalized into this vocabulary, so the Worker
//! journals one shape of event regardless of which harness produced it. The
//! variants map onto `horde.v1` event kinds; the conversion happens in the
//! Worker at the transport boundary, which is where protobuf's
//! optional-everything shape is supposed to stop.

use core::time::Duration;

use crate::ids::{ProviderSessionId, ToolCallId, TurnId};
use crate::permission::{PermissionDenial, ProposedToolCall};
use crate::usage::{RateWindow, UsageDelta};

/// Which stream of a turn a chunk belongs to.
///
/// Mirrors `horde.v1.AgentOutputChannel`. Kept distinguishable rather than
/// pre-flattened because clients render these differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputChannel {
    /// Prose the agent addressed to its reader.
    Assistant,
    /// Thinking. Present only when the harness exposes it.
    Reasoning,
    /// A tool invocation, rendered for display.
    ToolCall,
    /// What a tool returned.
    ToolResult,
    /// Harness-level noise: warnings, retries, rate-limit notices.
    HarnessDiagnostic,
}

/// Per-channel chunk numbering for one turn.
///
/// Chunk indices exist so a client can detect a gap; they only do that if they
/// start at zero and advance by one within each turn and channel
/// (`horde-testkit`'s conformance suite enforces it). Every adapter needs the
/// same counter, so it lives here once instead of drifting apart per codec.
#[derive(Debug, Default)]
pub struct ChunkCounters(Vec<(OutputChannel, u64)>);

impl ChunkCounters {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The next index for `channel`, advancing it.
    pub fn next(&mut self, channel: OutputChannel) -> u64 {
        match self.0.iter_mut().find(|(known, _)| *known == channel) {
            Some((_, next)) => {
                let index = *next;
                *next = next.saturating_add(1);
                index
            }
            None => {
                self.0.push((channel, 1));
                0
            }
        }
    }

    /// Starts every channel's numbering again — a new turn, a new count.
    pub fn reset(&mut self) {
        self.0.clear();
    }
}

/// One event from a running agent.
#[derive(Debug, Clone, PartialEq)]
pub enum AgentEvent {
    /// The harness named its conversation. Emitted before any output, and again
    /// on revival, because that identifier is what makes the agent revivable
    /// and it must reach the journal before anything can be lost.
    SessionEstablished {
        provider_session_id: ProviderSessionId,
        /// The model the harness actually bound, which may differ from the one
        /// requested.
        model: String,
    },

    /// A chunk of turn output.
    Output {
        turn_id: TurnId,
        channel: OutputChannel,
        text: String,
        /// Position within the turn and channel, so a client can detect a gap
        /// without consulting the session-wide sequence.
        chunk_index: u64,
    },

    /// The harness wants to run a tool and is waiting for Horde's answer.
    ///
    /// The adapter blocks the turn until
    /// [`AgentSession::answer_permission`](crate::AgentSession::answer_permission)
    /// is called with a matching decision. Spike W1c measured a 180-second
    /// stall answered successfully with no harness timeout, which is what makes
    /// the human-approval layer of DESIGN.md §10 viable.
    PermissionRequested {
        turn_id: TurnId,
        request: ProposedToolCall,
    },

    /// A tool ran and returned.
    ToolCompleted {
        turn_id: TurnId,
        tool_call_id: ToolCallId,
        /// Whether the harness reported the call as failed. A refused call
        /// surfaces here as an error too — from the agent's point of view a
        /// denial *is* a tool failure.
        is_error: bool,
    },

    /// The provider's own rate-limit state, when the harness reports it.
    HeadroomReported { window: RateWindow },

    /// The turn is over. Exactly one of these closes every turn.
    TurnEnded(TurnOutcome),
}

/// How a turn finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnStatus {
    /// Ran to completion.
    Completed,
    /// Cut short by [`AgentSession::interrupt`](crate::AgentSession::interrupt).
    /// The session remains usable.
    Interrupted,
    /// The harness failed the turn.
    Failed { message: String },
}

impl TurnStatus {
    #[must_use]
    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }
}

/// The terminal record of one turn.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnOutcome {
    pub turn_id: TurnId,
    pub status: TurnStatus,
    /// The harness's final answer text, when it produced one. Absent for
    /// interrupted turns.
    pub text: Option<String>,
    /// What this turn alone consumed. Adapters normalize running totals into
    /// deltas before they reach here — see [`crate::usage`].
    pub usage: UsageDelta,
    /// Tool calls the permission chain refused during this turn.
    pub denials: Vec<PermissionDenial>,
    /// Wall-clock time for this turn.
    pub duration: Duration,
}

impl TurnOutcome {
    /// True when the turn both finished and was not refused work.
    ///
    /// Deliberately not a check of the status alone: a completed turn that hit
    /// permission denials is not a clean success from the orchestrator's point
    /// of view, and the caller should be forced to look.
    #[must_use]
    pub fn is_clean_success(&self) -> bool {
        self.status.is_completed() && self.denials.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use core::time::Duration;
    use serde_json::json;

    use super::{TurnOutcome, TurnStatus};
    use crate::ids::{ToolCallId, TurnId};
    use crate::permission::PermissionDenial;
    use crate::usage::UsageDelta;

    fn outcome(status: TurnStatus, denials: Vec<PermissionDenial>) -> TurnOutcome {
        TurnOutcome {
            turn_id: TurnId::new("t1"),
            status,
            text: None,
            usage: UsageDelta::default(),
            denials,
            duration: Duration::from_secs(1),
        }
    }

    /// The counter's whole contract: zero first, plus one each time, per
    /// channel independently, and reset starts everything again.
    #[test]
    fn chunk_counters_number_each_channel_independently_from_zero() {
        use super::{ChunkCounters, OutputChannel};

        let mut counters = ChunkCounters::new();
        assert_eq!(counters.next(OutputChannel::Assistant), 0);
        assert_eq!(counters.next(OutputChannel::Assistant), 1);
        assert_eq!(
            counters.next(OutputChannel::Reasoning),
            0,
            "channels do not share a counter"
        );
        assert_eq!(counters.next(OutputChannel::Assistant), 2);

        counters.reset();
        assert_eq!(
            counters.next(OutputChannel::Assistant),
            0,
            "a new turn starts its numbering again"
        );
    }

    #[test]
    fn a_completed_turn_with_no_denials_is_a_clean_success() {
        assert!(outcome(TurnStatus::Completed, Vec::new()).is_clean_success());
    }

    #[test]
    fn a_completed_turn_that_hit_a_denial_is_not_clean() {
        let denial = PermissionDenial {
            tool_call_id: ToolCallId::new("toolu_01"),
            tool_name: "Write".to_owned(),
            attempted_input: json!({}),
        };
        assert!(!outcome(TurnStatus::Completed, vec![denial]).is_clean_success());
    }

    #[test]
    fn an_interrupted_turn_is_not_a_success() {
        assert!(!outcome(TurnStatus::Interrupted, Vec::new()).is_clean_success());
        assert!(!TurnStatus::Interrupted.is_completed());
    }
}
