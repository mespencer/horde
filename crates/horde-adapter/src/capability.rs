//! What a provider harness can actually do.
//!
//! Horde treats capabilities as facts to be honoured, not hints. A provider
//! that cannot resume gets honestly-terminal agents (DESIGN.md §5) rather than
//! a revival path that quietly loses context, and a provider that cannot
//! pre-authorize tool calls must be refused high-trust work rather than given
//! it with a permission chain that silently does nothing.
//!
//! Capabilities are data, queried at runtime. The permission layer asks
//! [`Capabilities::permission_enforcement`]; the scheduler asks about
//! [`Capability::Resumable`] and [`Capability::AccountIsolation`]. Nothing here
//! is a doc-comment promise.

use std::collections::BTreeSet;

/// One thing a harness may or may not support.
///
/// A closed enum on purpose: an open one would let a Worker silently ignore a
/// capability it did not recognize, whereas adding a variant here forces every
/// adapter's claim to be revisited in a reviewable diff. These crates are
/// internal (`publish = false`), so there is no semver cost to that choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Capability {
    /// A conversation survives its process and can be revived by
    /// [`Adapter::resume`](crate::Adapter::resume).
    ///
    /// Without this, an agent is terminal once its process ends.
    Resumable,

    /// A turn in flight can be aborted while leaving the session usable.
    ///
    /// Deliberately stronger than "the process can be killed". Spike W1c found
    /// that for Claude Code only a control message qualifies: SIGINT aborts the
    /// turn but ends the process, and closing stdin does not abort at all — it
    /// lets the turn run to completion. An adapter claiming this capability is
    /// promising the session is still steerable afterwards.
    InterruptibleTurn,

    /// A message can be delivered **into a turn already running**, and the
    /// agent acts on it before that turn ends.
    ///
    /// Named for what happens to the turn: an amendment extends it, where
    /// [`Self::InterruptibleTurn`] aborts it. Codex exposes `turn/steer`
    /// alongside `turn/interrupt` for exactly this reason.
    ///
    /// Spike W2c's transcript `15-steer.jsonl` is the evidence, and it carries
    /// a trap worth stating plainly: **a `final_answer` is not terminal within
    /// a turn.** `turn/steer` returns the *same* turn id, the in-flight work
    /// finishes and emits its own `final_answer`, and the steered instruction
    /// then emits a second one — two final answers under one turn id. Only
    /// `turn/completed` ends the turn. An adapter that treats the first final
    /// answer as the end reports a Result built from work the user just
    /// redirected away from.
    ///
    /// **There is deliberately no boundary-steering capability**, and this
    /// paragraph exists so nobody re-adds one. Queueing a message to the next
    /// turn boundary works against *every* provider, because the orchestrator
    /// simply holds the message and calls
    /// [`AgentSession::send`](crate::AgentSession::send) when the turn ends.
    /// That is not a harness capability, so advertising it would be
    /// meaningless. The absence of `AmendActiveTurn` therefore means precisely
    /// "the orchestrator holds the message itself" — never "steering is
    /// unavailable".
    AmendActiveTurn,

    /// **Horde sees a proposed tool call before it runs and decides its fate.**
    ///
    /// This is the capability the permission chain (DESIGN.md §10) is built on,
    /// and it is the one providers most often lack. Claude Code has it via
    /// `--permission-prompt-tool` (spike W1c). Codex 0.145.0 does **not**:
    /// spike W2c found that across six configurations no client-facing approval
    /// request ever arrived, because the two features that would route them are
    /// under development and off. Codex adjudicates against its own sandbox
    /// tiers and tells the client afterwards.
    ///
    /// Its absence is not a missing nicety — it means the deterministic-deny,
    /// judge, and human layers cannot gate that provider's tool calls at all.
    /// See [`Capabilities::permission_enforcement`].
    PreAuthorizeToolCalls,

    /// A permission decision may replace the tool's arguments, not just permit
    /// or refuse the call. Implies [`Self::PreAuthorizeToolCalls`] in practice.
    RewritesToolInput,

    /// Enforcement is a sandbox tier and working directory chosen when the
    /// agent starts, which the harness then polices itself.
    ///
    /// The honest description of what Codex offers instead of interception. A
    /// provider with this and without [`Self::PreAuthorizeToolCalls`] is still
    /// containable, but only coarsely and only at spawn.
    SandboxTierAtSpawn,

    /// The harness reports the provider's own rate-limit state, so headroom
    /// does not have to be inferred from Horde's metering.
    ReportsHeadroom,

    /// Horde's prompt overlays can be appended to the harness's system prompt.
    SystemPromptOverlay,

    /// The model can be chosen per agent.
    ModelSelection,

    /// Credentials and session state can be isolated per provider account, so
    /// one Worker can hold several accounts for the same provider.
    AccountIsolation,
}

impl Capability {
    /// Every capability, for exhaustiveness checks in tests and reports.
    pub const ALL: [Self; 10] = [
        Self::Resumable,
        Self::InterruptibleTurn,
        Self::AmendActiveTurn,
        Self::PreAuthorizeToolCalls,
        Self::RewritesToolInput,
        Self::SandboxTierAtSpawn,
        Self::ReportsHeadroom,
        Self::SystemPromptOverlay,
        Self::ModelSelection,
        Self::AccountIsolation,
    ];
}

/// How much control Horde's permission chain actually has over a provider.
///
/// The permission layer asks this rather than reading capability flags itself,
/// so "can this provider be gated?" has one answer in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionEnforcement {
    /// Every proposed tool call is offered to Horde before it runs. The full
    /// chain of DESIGN.md §10 applies.
    PerCall,

    /// The harness polices itself against a sandbox tier Horde picked at spawn,
    /// and reports what it did afterwards. Horde can constrain the agent but
    /// cannot adjudicate an individual call, so a high-trust assignment does
    /// not belong here.
    SandboxAtSpawn,

    /// Neither. Tool calls run unobserved and unconstrained; only placement and
    /// the workspace itself limit the blast radius.
    Unenforced,
}

impl PermissionEnforcement {
    /// Whether an individual tool call can be adjudicated before it runs.
    #[must_use]
    pub fn can_gate_individual_calls(&self) -> bool {
        matches!(self, Self::PerCall)
    }
}

/// The set of capabilities one adapter claims.
///
/// ```
/// use horde_adapter::{Capabilities, Capability};
///
/// let claimed = Capabilities::none()
///     .with(Capability::Resumable)
///     .with(Capability::PreAuthorizeToolCalls);
///
/// assert!(claimed.supports(Capability::Resumable));
/// // Anything unclaimed must be refused rather than silently no-oped.
/// assert!(!claimed.supports(Capability::RewritesToolInput));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Capabilities {
    supported: BTreeSet<Capability>,
}

impl Capabilities {
    /// A harness that supports nothing beyond running a turn.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with(mut self, capability: Capability) -> Self {
        self.supported.insert(capability);
        self
    }

    #[must_use]
    pub fn supports(&self, capability: Capability) -> bool {
        self.supported.contains(&capability)
    }

    /// Iterates the claimed capabilities in a stable order.
    pub fn iter(&self) -> impl Iterator<Item = Capability> + '_ {
        self.supported.iter().copied()
    }

    /// What the permission chain can actually do with this provider.
    ///
    /// ```
    /// use horde_adapter::{Capabilities, Capability, PermissionEnforcement};
    ///
    /// // Claude: proposed calls are offered to Horde before they run.
    /// let claude = Capabilities::none().with(Capability::PreAuthorizeToolCalls);
    /// assert_eq!(claude.permission_enforcement(), PermissionEnforcement::PerCall);
    ///
    /// // Codex: no interception, only a sandbox tier picked at spawn.
    /// let codex = Capabilities::none().with(Capability::SandboxTierAtSpawn);
    /// assert_eq!(codex.permission_enforcement(), PermissionEnforcement::SandboxAtSpawn);
    /// assert!(!codex.permission_enforcement().can_gate_individual_calls());
    /// ```
    #[must_use]
    pub fn permission_enforcement(&self) -> PermissionEnforcement {
        if self.supports(Capability::PreAuthorizeToolCalls) {
            PermissionEnforcement::PerCall
        } else if self.supports(Capability::SandboxTierAtSpawn) {
            PermissionEnforcement::SandboxAtSpawn
        } else {
            PermissionEnforcement::Unenforced
        }
    }
}

impl FromIterator<Capability> for Capabilities {
    fn from_iter<I: IntoIterator<Item = Capability>>(iter: I) -> Self {
        Self {
            supported: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Capabilities, Capability, PermissionEnforcement};

    #[test]
    fn an_empty_set_supports_nothing() {
        let capabilities = Capabilities::none();
        for capability in Capability::ALL {
            assert!(
                !capabilities.supports(capability),
                "{capability:?} should not be supported"
            );
        }
    }

    #[test]
    fn claimed_capabilities_are_reported_and_others_are_not() {
        let capabilities = Capabilities::none()
            .with(Capability::Resumable)
            .with(Capability::PreAuthorizeToolCalls);

        assert!(capabilities.supports(Capability::Resumable));
        assert!(capabilities.supports(Capability::PreAuthorizeToolCalls));
        assert!(!capabilities.supports(Capability::RewritesToolInput));
    }

    #[test]
    fn iteration_order_is_stable_and_deduplicated() {
        let capabilities: Capabilities = [
            Capability::ModelSelection,
            Capability::Resumable,
            Capability::Resumable,
        ]
        .into_iter()
        .collect();

        let listed: Vec<_> = capabilities.iter().collect();
        assert_eq!(
            listed,
            vec![Capability::Resumable, Capability::ModelSelection]
        );
    }

    #[test]
    fn interception_outranks_a_sandbox_when_a_provider_offers_both() {
        let both = Capabilities::none()
            .with(Capability::PreAuthorizeToolCalls)
            .with(Capability::SandboxTierAtSpawn);
        assert_eq!(
            both.permission_enforcement(),
            PermissionEnforcement::PerCall,
            "per-call adjudication is strictly more control than a spawn-time tier"
        );
    }

    #[test]
    fn a_provider_with_neither_control_is_reported_as_unenforced() {
        let bare = Capabilities::none().with(Capability::Resumable);
        assert_eq!(
            bare.permission_enforcement(),
            PermissionEnforcement::Unenforced
        );
        assert!(
            !bare.permission_enforcement().can_gate_individual_calls(),
            "an unenforced provider must never look gateable"
        );
    }

    /// Steering and interruption are separate mechanisms in both harnesses
    /// Horde drives, so claiming one must not imply the other.
    #[test]
    fn steering_and_interruption_are_independent_claims() {
        let steer_only = Capabilities::none().with(Capability::AmendActiveTurn);
        assert!(!steer_only.supports(Capability::InterruptibleTurn));

        let interrupt_only = Capabilities::none().with(Capability::InterruptibleTurn);
        assert!(!interrupt_only.supports(Capability::AmendActiveTurn));
    }
}

#[cfg(test)]
mod enforcement_tests {
    use super::{Capabilities, Capability, PermissionEnforcement};

    /// Only per-call enforcement can adjudicate an individual tool call.
    ///
    /// The `false` mutant survived, which would have said *nothing* can gate —
    /// and a placement layer reading that refuses high-trust work everywhere,
    /// including on Claude, which demonstrably can gate (W1c).
    #[test]
    fn only_per_call_enforcement_gates_individual_calls() {
        assert!(PermissionEnforcement::PerCall.can_gate_individual_calls());
        assert!(
            !PermissionEnforcement::SandboxAtSpawn.can_gate_individual_calls(),
            "a sandbox tier constrains the agent but cannot adjudicate one call"
        );
        assert!(!PermissionEnforcement::Unenforced.can_gate_individual_calls());
    }

    /// `none()` must actually be empty.
    ///
    /// It is the base every capability set is built from, so a `none()` that
    /// returned something else would silently widen every adapter's claim.
    #[test]
    fn the_empty_set_supports_nothing() {
        let empty = Capabilities::none();

        for capability in Capability::ALL {
            assert!(
                !empty.supports(capability),
                "{capability:?} must not be claimed by an empty set"
            );
        }
        assert_eq!(
            empty.permission_enforcement(),
            PermissionEnforcement::Unenforced,
            "and an empty set enforces nothing"
        );
    }
}
