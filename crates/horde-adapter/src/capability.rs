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

    /// Horde sees a proposed **MCP** tool call before it runs — and only those.
    ///
    /// The per-call-kind wording ADR-0005's 2026-08-11 amendment calls for.
    /// Codex routes every MCP tool call through a client-facing
    /// `mcpServer/elicitation/request` *before* a byte reaches the MCP server
    /// (transcripts 17–18, re-verified live at codex-cli 0.147.0), while its
    /// shell/exec calls remain adjudicated internally against the sandbox tier.
    /// One flag could not say both truths, so this one claims exactly the MCP
    /// half; a provider with [`Self::PreAuthorizeToolCalls`] does not also need
    /// this, because the broader claim covers every call kind.
    PreAuthorizeMcpToolCalls,

    /// Horde sees a proposed **shell/exec** command before it runs — and only
    /// those.
    ///
    /// The other half of the per-call-kind story. The 2026-08-19 re-probe
    /// (transcripts 20–33) reversed ADR-0005's exec finding for current codex:
    /// `item/commandExecution/requestApproval` reaches the client *before* the
    /// command executes under the granular policy from codex-cli 0.147.0, and
    /// under plain `on-request` from 0.148.0. The claim is version-gated at
    /// spawn — an adapter must probe the launched binary before holding it,
    /// because at 0.146 and below no configuration routed the request.
    ///
    /// Deliberately does **not** cover file changes: `apply_patch` approvals
    /// never reach the client in any measured policy — the thread parks at
    /// `waitingOnApproval` forever instead (transcripts 28 and 31) — so
    /// [`ToolCallKind::FileChange`] stays gated only by the broad claim.
    PreAuthorizeExecCommands,

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
    pub const ALL: [Self; 12] = [
        Self::Resumable,
        Self::InterruptibleTurn,
        Self::AmendActiveTurn,
        Self::PreAuthorizeToolCalls,
        Self::PreAuthorizeMcpToolCalls,
        Self::PreAuthorizeExecCommands,
        Self::RewritesToolInput,
        Self::SandboxTierAtSpawn,
        Self::ReportsHeadroom,
        Self::SystemPromptOverlay,
        Self::ModelSelection,
        Self::AccountIsolation,
    ];
}

/// Which kind of tool call is being asked about.
///
/// Exists because enforcement is no longer uniform per provider: ADR-0005's
/// amendments scoped Codex's "cannot pre-authorize" first away from MCP calls
/// (2026-08-11) and then away from shell/exec (2026-08-19, on a current CLI) —
/// leaving file changes as the one kind still adjudicated only by the sandbox
/// tier. A per-provider answer would have to pick one of those truths and lie
/// about the others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallKind {
    /// A call to an MCP server's tool.
    Mcp,
    /// A shell/exec command the harness runs itself.
    ExecCommand,
    /// A file edit the harness applies itself (codex's `apply_patch`).
    ///
    /// Split from [`Self::ExecCommand`] by measurement, not taxonomy: codex
    /// 0.148 routes exec approvals to the client but parks a file-change
    /// approval forever (transcripts 28, 31), so the two kinds genuinely have
    /// different enforcement answers on the same provider.
    FileChange,
}

impl ToolCallKind {
    /// Every kind, for the floor computation and exhaustiveness checks.
    pub const ALL: [Self; 3] = [Self::Mcp, Self::ExecCommand, Self::FileChange];
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

    /// How much control this enforcement gives Horde, for choosing the floor
    /// across call kinds. More is strictly better: per-call adjudication
    /// subsumes a spawn-time sandbox, which subsumes nothing at all.
    fn strength(self) -> u8 {
        match self {
            Self::PerCall => 2,
            Self::SandboxAtSpawn => 1,
            Self::Unenforced => 0,
        }
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
    // #[gamma::skip(fn_value.default, reason = "the body is `Self::default()` over a `Capabilities` that derives `Default`, so the mutant `Default::default()` is the same expression")]
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
        // The floor across call kinds, deliberately: a caller that does not
        // say which kind it is asking about must get the weakest answer, so a
        // provider gated for MCP and exec but not for file changes can never
        // be treated as per-call gated overall.
        ToolCallKind::ALL
            .into_iter()
            .map(|kind| self.permission_enforcement_for(kind))
            .min_by_key(|enforcement| enforcement.strength())
            .unwrap_or(PermissionEnforcement::Unenforced)
    }

    /// What the permission chain can do with this provider **for one kind of
    /// tool call** — the per-call-kind answer ADR-0005's amendments require.
    ///
    /// ```
    /// use horde_adapter::{Capabilities, Capability, PermissionEnforcement, ToolCallKind};
    ///
    /// // Codex at 0.147+: MCP calls and exec commands are intercepted;
    /// // file changes are not (their approval parks instead of routing).
    /// let codex = Capabilities::none()
    ///     .with(Capability::PreAuthorizeMcpToolCalls)
    ///     .with(Capability::PreAuthorizeExecCommands)
    ///     .with(Capability::SandboxTierAtSpawn);
    /// assert_eq!(
    ///     codex.permission_enforcement_for(ToolCallKind::Mcp),
    ///     PermissionEnforcement::PerCall
    /// );
    /// assert_eq!(
    ///     codex.permission_enforcement_for(ToolCallKind::ExecCommand),
    ///     PermissionEnforcement::PerCall
    /// );
    /// assert_eq!(
    ///     codex.permission_enforcement_for(ToolCallKind::FileChange),
    ///     PermissionEnforcement::SandboxAtSpawn
    /// );
    /// // And the kind-blind answer stays the floor.
    /// assert_eq!(
    ///     codex.permission_enforcement(),
    ///     PermissionEnforcement::SandboxAtSpawn
    /// );
    /// ```
    #[must_use]
    pub fn permission_enforcement_for(&self, kind: ToolCallKind) -> PermissionEnforcement {
        let gated = match kind {
            // The broad claim covers every kind; the scoped claims one each.
            ToolCallKind::Mcp => {
                self.supports(Capability::PreAuthorizeToolCalls)
                    || self.supports(Capability::PreAuthorizeMcpToolCalls)
            }
            ToolCallKind::ExecCommand => {
                self.supports(Capability::PreAuthorizeToolCalls)
                    || self.supports(Capability::PreAuthorizeExecCommands)
            }
            // No scoped claim exists for file changes: no measured provider
            // routes them separately (codex parks them, transcripts 28/31),
            // and an unclaimable capability would only invite a false claim.
            ToolCallKind::FileChange => self.supports(Capability::PreAuthorizeToolCalls),
        };
        if gated {
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

    /// The per-kind answers ADR-0005's amendments require: each scoped claim
    /// gates its own kind and no other, and the broad claim gates every kind
    /// without needing either scoped one.
    #[test]
    fn scoped_claims_gate_their_own_kind_and_the_broad_claim_covers_all() {
        use super::ToolCallKind;

        let codex_0146_shaped = Capabilities::none()
            .with(Capability::PreAuthorizeMcpToolCalls)
            .with(Capability::SandboxTierAtSpawn);
        assert_eq!(
            codex_0146_shaped.permission_enforcement_for(ToolCallKind::Mcp),
            PermissionEnforcement::PerCall,
            "an MCP call is offered to Horde before it runs (transcripts 17-18)"
        );
        assert_eq!(
            codex_0146_shaped.permission_enforcement_for(ToolCallKind::ExecCommand),
            PermissionEnforcement::SandboxAtSpawn,
            "without the exec claim, shell stays adjudicated by the tier"
        );

        let codex_0147_shaped = codex_0146_shaped
            .clone()
            .with(Capability::PreAuthorizeExecCommands);
        assert_eq!(
            codex_0147_shaped.permission_enforcement_for(ToolCallKind::ExecCommand),
            PermissionEnforcement::PerCall,
            "exec approvals route to the client at 0.147+ (transcripts 20-33)"
        );
        assert_eq!(
            codex_0147_shaped.permission_enforcement_for(ToolCallKind::FileChange),
            PermissionEnforcement::SandboxAtSpawn,
            "a file-change approval parks instead of routing (transcripts 28/31)"
        );
        assert_eq!(
            codex_0146_shaped.permission_enforcement_for(ToolCallKind::Mcp),
            codex_0147_shaped.permission_enforcement_for(ToolCallKind::Mcp),
            "the exec claim must not change the MCP answer"
        );

        let claude_shaped = Capabilities::none().with(Capability::PreAuthorizeToolCalls);
        for kind in ToolCallKind::ALL {
            assert_eq!(
                claude_shaped.permission_enforcement_for(kind),
                PermissionEnforcement::PerCall,
                "the broad claim covers every call kind, {kind:?} included"
            );
        }
    }

    /// The kind-blind answer must be the floor across kinds, never the best
    /// case — a provider gated for MCP and exec but read as per-call overall
    /// would let placement put a high-trust assignment on ungated file edits.
    #[test]
    fn the_kind_blind_answer_is_the_floor_not_the_best_case() {
        let codex_shaped = Capabilities::none()
            .with(Capability::PreAuthorizeMcpToolCalls)
            .with(Capability::PreAuthorizeExecCommands)
            .with(Capability::SandboxTierAtSpawn);
        assert_eq!(
            codex_shaped.permission_enforcement(),
            PermissionEnforcement::SandboxAtSpawn,
            "file changes stay tier-enforced, so the floor stays the tier"
        );

        let mcp_only = Capabilities::none().with(Capability::PreAuthorizeMcpToolCalls);
        assert_eq!(
            mcp_only.permission_enforcement(),
            PermissionEnforcement::Unenforced,
            "without even a sandbox, the floor is unenforced"
        );

        let broad = Capabilities::none().with(Capability::PreAuthorizeToolCalls);
        assert_eq!(
            broad.permission_enforcement(),
            PermissionEnforcement::PerCall,
            "only the broad claim lifts the floor to per-call"
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
