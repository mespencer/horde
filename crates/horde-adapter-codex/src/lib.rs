//! Codex's `app-server` JSON-RPC protocol, as a codec.
//!
//! ADR-0005 has the Worker drive `codex app-server --stdio` — the native
//! protocol, not `codex exec` (one-shot, no streaming deltas), not
//! `codex mcp-server` (inverts supervision), and not ACP (not native). Spike
//! W2c verified that end to end against codex-cli 0.145.0 under a ChatGPT
//! subscription; this crate is the protocol half, factored so it can be tested
//! without a live process, an account, or a network.
//!
//! # Layout
//!
//! * [`wire`] — serde types transcribed from the W2c golden transcripts.
//! * [`codec`] — `String` in, [`wire::Frame`] out, plus encoders for the
//!   requests Horde sends.
//! * [`turn`] — a pure state machine assembling frames into
//!   [`horde_adapter::AgentEvent`]s.
//! * [`identity`] — what it takes to find a thread again. A thread id alone is
//!   not enough: threads live under `CODEX_HOME`, so the account directory is
//!   part of an agent's revival identity.
//!
//! # The three traps this codec exists to absorb
//!
//! Each was measured, not assumed, and each would be a production bug if the
//! codec followed the protocol's shape naively:
//!
//! 1. **Interrupt emits no terminal frame.** `turn/interrupt` ends the turn via
//!    `thread/status → idle`, and already-generated deltas keep arriving after
//!    it (transcript 11 recorded 394). An assembler waiting for
//!    `turn/completed` hangs forever.
//! 2. **A steered turn has two final answers.** `turn/steer` amends the
//!    *active* turn and returns the same turn id, so one turn emits two
//!    `agentMessage` items with `phase: final_answer` (transcript 15). A Result
//!    inferred from the first is wrong.
//! 3. **Resume replays the previous turn's usage.** `thread/resume` re-emits
//!    the prior `thread/tokenUsage/updated` before the new turn's (transcript
//!    16), so usage must be idempotent per `(turnId, itemId)` or every revival
//!    double-bills.
//!
//! # What is deliberately not here
//!
//! Process supervision. Spawning the binary, wiring stdio, isolating
//! `CODEX_HOME` per account, and pumping the JSON-RPC id/response correlation
//! belong to the Worker (wave 3) and the productionized adapter (wave 5).
//! Keeping the protocol pure is what lets the conformance suite replay recorded
//! sessions and get exactly what a live process would produce.

pub mod codec;
pub mod identity;
pub mod replay;
pub mod turn;
pub mod wire;

pub use codec::{decode_line, encode_interrupt, encode_steer, encode_turn_start};
pub use identity::RevivalIdentity;
pub use replay::ReplaySession;
pub use turn::TurnAssembler;

use horde_adapter::{Capabilities, Capability};

/// The adapter key for Codex, matching `horde.v1.ConcreteModel.provider`.
pub const PROVIDER: &str = "codex";

/// What Codex supports, as measured by spike W2c against codex-cli 0.145.0.
///
/// The absence of [`Capability::PreAuthorizeToolCalls`] is the load-bearing
/// claim, not an oversight. Six configurations — including explicitly enabling
/// `exec_permission_approvals` and `request_permissions_tool` — produced zero
/// client-facing approval requests, because both features are under development
/// and off by default. Codex adjudicates against its own sandbox tiers and
/// tells the client afterwards, which is why
/// [`Capability::SandboxTierAtSpawn`] is claimed instead: that is the honest
/// description of what containment Codex actually offers (ADR-0005).
///
/// Consequently [`Capability::RewritesToolInput`] is absent too — arguments
/// cannot be rewritten by a chain that never sees the call.
#[must_use]
pub fn capabilities() -> Capabilities {
    Capabilities::none()
        .with(Capability::Resumable)
        .with(Capability::InterruptibleTurn)
        // Ruling applied: the capability is `AmendActiveTurn`, named for what
        // `turn/steer` does to the turn rather than for when the message lands.
        // Transcript 15 is the evidence — same turn id start to finish, two
        // final answers inside it.
        .with(Capability::AmendActiveTurn)
        .with(Capability::SandboxTierAtSpawn)
        .with(Capability::ReportsHeadroom)
        .with(Capability::SystemPromptOverlay)
        .with(Capability::ModelSelection)
        .with(Capability::AccountIsolation)
}

#[cfg(test)]
mod tests {
    use horde_adapter::{Capability, PermissionEnforcement};

    use super::{PROVIDER, capabilities};

    #[test]
    fn the_provider_key_matches_the_wire_contract() {
        assert_eq!(PROVIDER, "codex");
    }

    /// The claim ADR-0005 turns on. Stated as a refusal rather than an omission
    /// so that anyone widening this set has to argue with the spike.
    #[test]
    fn codex_does_not_claim_to_gate_tool_calls() {
        let claimed = capabilities();

        assert!(
            !claimed.supports(Capability::PreAuthorizeToolCalls),
            "W2c found no client-facing approval request in six configurations"
        );
        assert!(
            !claimed.supports(Capability::RewritesToolInput),
            "arguments cannot be rewritten by a chain that never sees the call"
        );
        assert!(
            claimed.supports(Capability::SandboxTierAtSpawn),
            "the sandbox tier is what Codex offers instead of interception"
        );
    }

    /// The single answer the permission layer asks for must come out
    /// `SandboxAtSpawn` — not `Unenforced`, which would understate what Codex
    /// does, and not `PerCall`, which would overstate it.
    #[test]
    fn permission_enforcement_reads_as_sandbox_at_spawn() {
        assert_eq!(
            capabilities().permission_enforcement(),
            PermissionEnforcement::SandboxAtSpawn
        );
    }

    #[test]
    fn the_capabilities_the_spike_verified_are_claimed() {
        let claimed = capabilities();
        for capability in [
            Capability::Resumable,
            Capability::InterruptibleTurn,
            Capability::AmendActiveTurn,
            Capability::ReportsHeadroom,
            Capability::ModelSelection,
            Capability::AccountIsolation,
        ] {
            assert!(
                claimed.supports(capability),
                "{capability:?} was verified by W2c and must be claimed"
            );
        }
    }
}
