//! What a provider harness can actually do.
//!
//! Horde treats capabilities as facts to be honoured, not hints. A provider
//! that cannot resume gets honestly-terminal agents (DESIGN.md §5) rather than
//! a revival path that quietly loses context.

use std::collections::BTreeSet;

/// One thing a harness may or may not support.
///
/// An open enum would let a Worker silently ignore a capability it did not
/// recognize; a closed one makes adding a capability a reviewable diff that
/// every adapter must answer.
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

    /// Proposed tool calls can be routed to Horde for a decision before they
    /// run.
    PermissionPrompt,

    /// A permission decision may replace the tool's arguments, not just
    /// permit or refuse the call.
    RewritesToolInput,

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
    pub const ALL: [Self; 8] = [
        Self::Resumable,
        Self::InterruptibleTurn,
        Self::PermissionPrompt,
        Self::RewritesToolInput,
        Self::ReportsHeadroom,
        Self::SystemPromptOverlay,
        Self::ModelSelection,
        Self::AccountIsolation,
    ];
}

/// The set of capabilities one adapter claims.
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
    use super::{Capabilities, Capability};

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
            .with(Capability::PermissionPrompt);

        assert!(capabilities.supports(Capability::Resumable));
        assert!(capabilities.supports(Capability::PermissionPrompt));
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
}
