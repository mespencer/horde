//! Claude Code's `stream-json` protocol, as a codec.
//!
//! ADR-0003 has the Worker drive `claude --input-format stream-json
//! --output-format stream-json` directly, with no Node sidecar and no Agent
//! SDK. Spike W1c verified that end to end; this crate is the protocol half of
//! that work, factored so it can be tested without a live process, an API key,
//! or a network.
//!
//! # Layout
//!
//! * [`wire`] — serde types transcribed from real captures.
//! * [`codec`] — `String` in, [`wire::Envelope`] out, and the encoders for the
//!   two things Horde ever sends: a user message and an interrupt.
//! * [`turn`] — a pure state machine assembling envelopes into
//!   [`horde_adapter::AgentEvent`]s, including usage normalization.
//! * [`permission`] — the `--permission-prompt-tool` payloads, which arrive on
//!   the MCP endpoint rather than on stdout.
//! * [`launch`] — the isolation flags every supervised launch carries, shared
//!   by the Worker's spawn and the live smokes so the two cannot drift.
//! * [`identity`] — what it takes to find a conversation again. A session id
//!   alone is not enough: sessions live inside `CLAUDE_CONFIG_DIR`, so the
//!   account directory is part of an agent's revival identity.
//!
//! # What is deliberately not here
//!
//! Process supervision. Spawning the binary, wiring stdio, isolating
//! `CLAUDE_CONFIG_DIR` per account, and merging the stdout stream with the MCP
//! endpoint's permission requests belong to the Worker (wave 3) and to the
//! productionized adapter (wave 5). Keeping the protocol pure is what lets the
//! conformance suite replay recorded sessions and get exactly what a live
//! process would produce.
//!
//! # Capabilities
//!
//! [`capabilities`] states what the harness supports, as measured by spike W1c
//! rather than as advertised. Notably `--permission-prompt-tool` does not
//! appear in `claude --help` but is real and load-bearing, and
//! [`Capability::InterruptibleTurn`] holds only via the control message —
//! SIGINT ends the process and closing stdin does not interrupt at all.

pub mod codec;
pub mod identity;
pub mod launch;
pub mod permission;
pub mod replay;
pub mod turn;
pub mod wire;

pub use codec::{decode_line, encode_interrupt, encode_user_message};
pub use identity::RevivalIdentity;
pub use replay::ReplaySession;
pub use turn::TurnAssembler;

use horde_adapter::{Capabilities, Capability};

/// The adapter key for Claude Code, matching `horde.v1.ConcreteModel.provider`.
pub const PROVIDER: &str = "claude";

/// What Claude Code supports, as verified by spike W1c.
///
/// Every entry here was demonstrated against a real binary (2.1.226); see
/// `docs/spikes/claude-stream-json.md` for the evidence behind each one.
///
/// `AmendActiveTurn` is **not** claimed. W1c found no way to deliver into a
/// running Claude turn — its only steering-adjacent finding is the control
/// message's abort-and-keep-steering, which is `InterruptibleTurn`. Per that
/// capability's contract, the absence means the orchestrator queues the message
/// to the turn boundary itself, which works here as it does everywhere. Dropped
/// when the capability was renamed from `SteerAtTurnBoundary`, whose old name
/// this adapter did satisfy.
#[must_use]
pub fn capabilities() -> Capabilities {
    Capabilities::none()
        .with(Capability::Resumable)
        .with(Capability::InterruptibleTurn)
        .with(Capability::PreAuthorizeToolCalls)
        .with(Capability::RewritesToolInput)
        .with(Capability::ReportsHeadroom)
        .with(Capability::SystemPromptOverlay)
        .with(Capability::ModelSelection)
        .with(Capability::AccountIsolation)
}

#[cfg(test)]
mod tests {
    use horde_adapter::Capability;

    use super::{PROVIDER, capabilities};

    #[test]
    fn the_provider_key_matches_the_wire_contract() {
        assert_eq!(PROVIDER, "claude");
    }

    /// Spike W1c found every capability present, so this is currently a full
    /// set. Stated as an explicit list rather than "all of them" so that adding
    /// a capability forces a deliberate answer for Claude instead of silently
    /// widening this claim.
    #[test]
    fn every_capability_measured_by_the_spike_is_claimed() {
        let claimed = capabilities();
        for capability in [
            Capability::Resumable,
            Capability::InterruptibleTurn,
            Capability::PreAuthorizeToolCalls,
            Capability::RewritesToolInput,
            Capability::ReportsHeadroom,
            Capability::SystemPromptOverlay,
            Capability::ModelSelection,
            Capability::AccountIsolation,
        ] {
            assert!(
                claimed.supports(capability),
                "{capability:?} must be claimed"
            );
        }
    }
}
