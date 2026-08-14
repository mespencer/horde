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
pub mod sandbox;
pub mod turn;
pub mod wire;

pub use codec::{
    ThreadConfig, decode_line, encode_decision_decline, encode_elicitation_response,
    encode_initialize, encode_initialized, encode_interrupt, encode_method_not_found, encode_steer,
    encode_thread_resume, encode_thread_start, encode_turn_start,
};
pub use identity::RevivalIdentity;
pub use replay::{ReplaySession, replay};
pub use sandbox::{SandboxPolicy, SandboxTier, TierChoice};
pub use turn::TurnAssembler;

use horde_adapter::{Capabilities, Capability};

/// The adapter key for Codex, matching `horde.v1.ConcreteModel.provider`.
pub const PROVIDER: &str = "codex";

/// What Codex supports, as measured by spike W2c against codex-cli 0.145.0 and
/// re-measured for MCP interception at 0.147.0 (transcripts 17–19).
///
/// The absence of [`Capability::PreAuthorizeToolCalls`] is the load-bearing
/// claim, not an oversight. For **shell/exec**, six configurations — including
/// explicitly enabling `exec_permission_approvals` and
/// `request_permissions_tool` — produced zero client-facing approval requests
/// (both flags re-checked, still off at 0.147.0). Codex adjudicates those
/// against its own sandbox tiers and tells the client afterwards, which is why
/// [`Capability::SandboxTierAtSpawn`] is claimed instead.
///
/// For **MCP tool calls** the ADR-0005 amendment reversed that finding:
/// every one is routed through `mcpServer/elicitation/request` *before* a byte
/// reaches the MCP server, so [`Capability::PreAuthorizeMcpToolCalls`] — the
/// per-call-kind claim — is honestly held. The broad claim still is not, and
/// `permission_enforcement()` (the kind-blind floor) still reads
/// `SandboxAtSpawn`.
///
/// [`Capability::RewritesToolInput`] stays absent: the elicitation reply is
/// accept-or-decline, with nowhere to carry replacement arguments, so a
/// narrowing decision must be refused rather than silently widened.
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
        .with(Capability::PreAuthorizeMcpToolCalls)
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

    /// The claim ADR-0005 turns on, per call kind since the amendment. Stated
    /// as a refusal rather than an omission so that anyone widening this set
    /// has to argue with the spikes.
    #[test]
    fn codex_gates_mcp_calls_and_only_mcp_calls() {
        let claimed = capabilities();

        assert!(
            !claimed.supports(Capability::PreAuthorizeToolCalls),
            "W2c found no client-facing exec approval in six configurations, \
             and both feature flags are still off at 0.147.0"
        );
        assert!(
            claimed.supports(Capability::PreAuthorizeMcpToolCalls),
            "every MCP call elicits before it runs (transcripts 17-18)"
        );
        assert!(
            !claimed.supports(Capability::RewritesToolInput),
            "the elicitation reply is accept-or-decline; there is nowhere to \
             carry replacement arguments"
        );
        assert!(
            claimed.supports(Capability::SandboxTierAtSpawn),
            "the sandbox tier is what Codex offers instead of exec interception"
        );
    }

    /// The kind-blind answer must stay `SandboxAtSpawn` — not `Unenforced`,
    /// which would understate what Codex does, and not `PerCall`, which the
    /// MCP-only claim must never inflate into. Per kind, MCP is `PerCall`.
    #[test]
    fn permission_enforcement_reads_as_sandbox_at_spawn_with_mcp_gated() {
        use horde_adapter::ToolCallKind;

        assert_eq!(
            capabilities().permission_enforcement(),
            PermissionEnforcement::SandboxAtSpawn
        );
        assert_eq!(
            capabilities().permission_enforcement_for(ToolCallKind::Mcp),
            PermissionEnforcement::PerCall
        );
        assert_eq!(
            capabilities().permission_enforcement_for(ToolCallKind::Exec),
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
