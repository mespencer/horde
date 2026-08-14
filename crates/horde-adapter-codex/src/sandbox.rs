//! The sandbox tier: Codex's real permission control for shell/exec.
//!
//! For shell and exec calls, Horde never sees a Codex tool call before it runs
//! (ADR-0005, unmoved by the 2026-08-11 amendment, flags re-checked still off
//! at 0.147.0). The tier chosen at `thread/start` *is* the enforcement, so
//! choosing it is a permission decision and gets a permission decision's
//! ceremony: a named mapping from the agent's permission mode, a conservative
//! default, and a stated reason that the Worker journals.
//!
//! # Why the mapping is worker-side configuration
//!
//! Permission modes are named, user-defined, and resolved server-side (DESIGN
//! §10, wave 4); the Worker sees only the *name*. A Worker cannot derive a
//! tier from rules it never receives, so the honest mapping is an explicit
//! table from mode names it was configured with — and everything else falls to
//! the read-only default, because an unknown mode must never widen the
//! sandbox.

use std::collections::BTreeMap;

/// The tiers Horde will run a Codex agent under.
///
/// Deliberately two, not three: the protocol's `danger-full-access` is
/// unrepresentable here. Spike W2c's verdict was "never use it" — with no
/// per-call interception, full access would mean no enforcement at all — so
/// the type makes the forbidden tier unwritable rather than merely
/// discouraged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SandboxTier {
    /// No writes anywhere; network blocked. The default, because the sandbox
    /// is the only control and a surprise must fail closed.
    #[default]
    ReadOnly,
    /// Writes inside the cwd (and `/private/tmp` on macOS — measured, spike
    /// W2c #10); network still blocked.
    WorkspaceWrite,
}

impl SandboxTier {
    /// The tier as `thread/start` spells it.
    #[must_use]
    pub fn wire_name(self) -> &'static str {
        match self {
            Self::ReadOnly => "read-only",
            Self::WorkspaceWrite => "workspace-write",
        }
    }
}

/// One tier choice, with the reason a human reads in the journal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierChoice {
    pub tier: SandboxTier,
    /// Why this tier: which mode named it, or which default caught it. The
    /// Worker journals this alongside the spawn, so "why could this agent
    /// write?" has an answer in the log rather than in someone's memory.
    pub reason: String,
}

/// The worker-configured mapping from permission-mode names to sandbox tiers.
///
/// The `Default` policy maps nothing, so every agent runs read-only — there
/// is deliberately no separate constructor for that, because a second name
/// for `default()` is a provably equivalent mutation waiting to be excluded.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SandboxPolicy {
    tiers: BTreeMap<String, SandboxTier>,
}

impl SandboxPolicy {
    /// Maps one permission-mode name to a tier.
    #[must_use]
    pub fn with_mode(mut self, mode: impl Into<String>, tier: SandboxTier) -> Self {
        self.tiers.insert(mode.into(), tier);
        self
    }

    /// The tier an agent under `permission_mode` runs at, and why.
    ///
    /// Unknown names get the read-only default *and a reason that says so* —
    /// distinct from the empty-name reason, because "nobody chose a mode" and
    /// "somebody chose a mode this worker has no mapping for" call for
    /// different fixes.
    #[must_use]
    pub fn tier_for(&self, permission_mode: &str) -> TierChoice {
        if permission_mode.is_empty() {
            return TierChoice {
                tier: SandboxTier::ReadOnly,
                reason: "the spawn names no permission mode; codex runs read-only by default"
                    .to_owned(),
            };
        }
        match self.tiers.get(permission_mode) {
            Some(tier) => TierChoice {
                tier: *tier,
                reason: format!(
                    "permission mode {permission_mode:?} maps to {} by worker sandbox policy",
                    tier.wire_name()
                ),
            },
            None => TierChoice {
                tier: SandboxTier::ReadOnly,
                reason: format!(
                    "permission mode {permission_mode:?} has no sandbox mapping on this worker; \
                     codex falls back to read-only"
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SandboxPolicy, SandboxTier};

    #[test]
    fn the_wire_names_are_the_protocols_own() {
        assert_eq!(SandboxTier::ReadOnly.wire_name(), "read-only");
        assert_eq!(SandboxTier::WorkspaceWrite.wire_name(), "workspace-write");
    }

    #[test]
    fn a_mapped_mode_gets_its_tier_and_names_the_mapping() {
        let policy = SandboxPolicy::default().with_mode("builder", SandboxTier::WorkspaceWrite);
        let choice = policy.tier_for("builder");
        assert_eq!(choice.tier, SandboxTier::WorkspaceWrite);
        assert!(
            choice.reason.contains("builder") && choice.reason.contains("workspace-write"),
            "the journaled reason must name both halves: {}",
            choice.reason
        );
    }

    /// The two fall-through cases are different facts and must journal
    /// differently: an unmapped mode is a configuration gap, an absent mode is
    /// a spawn that never chose.
    #[test]
    fn unmapped_and_absent_modes_both_fail_closed_with_distinct_reasons() {
        let policy = SandboxPolicy::default().with_mode("builder", SandboxTier::WorkspaceWrite);

        let unmapped = policy.tier_for("reviewer");
        assert_eq!(unmapped.tier, SandboxTier::ReadOnly);
        assert!(
            unmapped.reason.contains("no sandbox mapping"),
            "got {}",
            unmapped.reason
        );

        let absent = policy.tier_for("");
        assert_eq!(absent.tier, SandboxTier::ReadOnly);
        assert!(
            absent.reason.contains("no permission mode"),
            "got {}",
            absent.reason
        );
        assert_ne!(unmapped.reason, absent.reason);
    }

    /// An unknown mode must never widen the sandbox, even when every mapped
    /// mode is workspace-write — fail closed is the entire point.
    #[test]
    fn an_unknown_mode_never_inherits_a_wider_mapped_tier() {
        let policy = SandboxPolicy::default()
            .with_mode("a", SandboxTier::WorkspaceWrite)
            .with_mode("b", SandboxTier::WorkspaceWrite);
        assert_eq!(policy.tier_for("typo").tier, SandboxTier::ReadOnly);
    }
}
