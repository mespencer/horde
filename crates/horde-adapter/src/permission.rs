//! The adapter end of the permission chain (DESIGN.md §10).
//!
//! An adapter's job here is narrow: surface the tool call the harness wants to
//! make, block until Horde answers, and apply the answer faithfully. The four
//! layers — deterministic deny, deterministic allow, policy judge, human — live
//! above this seam and are not the adapter's business.

use serde_json::Value;

use crate::ids::ToolCallId;

/// A tool call as the harness proposed it.
#[derive(Debug, Clone, PartialEq)]
pub struct ProposedToolCall {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    /// Arguments as structured data, not a JSON string, so deterministic rules
    /// and the policy judge read the same values.
    pub input: Value,
}

/// Horde's answer to a [`ProposedToolCall`].
///
/// ```
/// use horde_adapter::PermissionDecision;
/// use serde_json::json;
///
/// // Permitting a call as proposed.
/// let allowed = PermissionDecision::allow();
/// assert!(allowed.is_allow() && allowed.updated_input().is_none());
///
/// // Narrowing it: the harness runs these arguments, not the model's.
/// let narrowed = PermissionDecision::allow_with(json!({ "path": "/workspace" }));
/// assert_eq!(narrowed.updated_input(), Some(&json!({ "path": "/workspace" })));
///
/// // An allow that widens the deterministic-allow layer for the Session also
/// // tells the adapter, so a harness with its own session cache (codex's
/// // `acceptForSession`) can mirror the grant instead of re-asking.
/// let cached = PermissionDecision::allow().remembered();
/// assert!(cached.remembers_for_session());
///
/// let refused = PermissionDecision::deny("outside the workspace");
/// assert!(!refused.is_allow() && !refused.remembers_for_session());
/// ```
#[derive(Debug, Clone, PartialEq)]
pub enum PermissionDecision {
    /// Let the call proceed.
    Allow {
        /// Replacement arguments, when the chain narrowed the call rather than
        /// merely permitting it — clamping a path, dropping a flag.
        ///
        /// Spike W1c proved this is real rather than theoretical: Claude Code
        /// executes the arguments the permission tool returns, not the ones the
        /// model proposed. Adapters whose harness cannot rewrite arguments must
        /// report [`Capability::RewritesToolInput`] as unsupported and reject a
        /// decision carrying `updated_input`, so a narrowing that silently
        /// failed to apply can never be mistaken for one that took effect.
        ///
        /// [`Capability::RewritesToolInput`]: crate::capability::Capability::RewritesToolInput
        updated_input: Option<Value>,
        /// Whether the decision also widened the deterministic-allow layer for
        /// the rest of the Session (`PermissionDecision.remember_for_session`
        /// on the wire).
        ///
        /// The Server owns that memory — repeats are auto-allowed by the chain
        /// whether or not the adapter does anything here. The bit rides to the
        /// adapter so a harness with its own session-scoped grant vocabulary
        /// (codex's `acceptForSession`, re-probe transcripts 20–33) can encode
        /// the same scope and skip the round trip; adapters without one ignore
        /// it, which loses nothing but the round trip.
        remember_for_session: bool,
    },
    /// Refuse the call. The message is shown to the agent as the tool's error.
    Deny { message: String },
}

impl PermissionDecision {
    /// Allows the call exactly as proposed.
    #[must_use]
    pub fn allow() -> Self {
        Self::Allow {
            updated_input: None,
            remember_for_session: false,
        }
    }

    /// Allows the call, but with these arguments instead.
    #[must_use]
    pub fn allow_with(updated_input: Value) -> Self {
        Self::Allow {
            updated_input: Some(updated_input),
            remember_for_session: false,
        }
    }

    /// Marks an allow as one that widened the Session's deterministic-allow
    /// layer. A denial is returned unchanged: remembering a refusal as a grant
    /// would be the silently widened permission the wire's own docs forbid,
    /// so the bit is unrepresentable on `Deny` rather than merely unread.
    #[must_use]
    pub fn remembered(self) -> Self {
        match self {
            Self::Allow { updated_input, .. } => Self::Allow {
                updated_input,
                remember_for_session: true,
            },
            deny @ Self::Deny { .. } => deny,
        }
    }

    /// Whether this decision also widened the Session's allow layer.
    #[must_use]
    pub fn remembers_for_session(&self) -> bool {
        matches!(
            self,
            Self::Allow {
                remember_for_session: true,
                ..
            }
        )
    }

    /// Refuses the call.
    #[must_use]
    pub fn deny(message: impl Into<String>) -> Self {
        Self::Deny {
            message: message.into(),
        }
    }

    #[must_use]
    pub fn is_allow(&self) -> bool {
        matches!(self, Self::Allow { .. })
    }

    /// The replacement arguments this decision carries, if any.
    #[must_use]
    pub fn updated_input(&self) -> Option<&Value> {
        match self {
            Self::Allow { updated_input, .. } => updated_input.as_ref(),
            Self::Deny { .. } => None,
        }
    }
}

/// A tool call the permission chain refused, as reported at turn end.
///
/// Kept distinct from the live [`ProposedToolCall`] because this is the audit
/// record: it is what the Worker journals, and it survives the turn.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionDenial {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub attempted_input: Value,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::PermissionDecision;

    #[test]
    fn a_bare_allow_carries_no_rewrite() {
        let decision = PermissionDecision::allow();
        assert!(decision.is_allow());
        assert!(decision.updated_input().is_none());
    }

    #[test]
    fn a_narrowing_allow_carries_the_replacement_arguments() {
        let decision = PermissionDecision::allow_with(json!({"path": "/safe"}));
        assert_eq!(decision.updated_input(), Some(&json!({"path": "/safe"})));
    }

    #[test]
    fn a_denial_is_not_an_allow_and_rewrites_nothing() {
        let decision = PermissionDecision::deny("nope");
        assert!(!decision.is_allow());
        assert!(decision.updated_input().is_none());
    }

    /// The remember bit: settable on an allow (with or without a narrowing,
    /// which must survive the marking), and unrepresentable on a denial — a
    /// remembered refusal read back as a grant would be the silently widened
    /// permission the wire's own docs forbid.
    #[test]
    fn remembering_marks_allows_and_cannot_mark_denials() {
        assert!(!PermissionDecision::allow().remembers_for_session());
        assert!(
            PermissionDecision::allow()
                .remembered()
                .remembers_for_session()
        );

        let narrowed = PermissionDecision::allow_with(json!({"path": "/safe"})).remembered();
        assert!(narrowed.remembers_for_session());
        assert_eq!(
            narrowed.updated_input(),
            Some(&json!({"path": "/safe"})),
            "marking must not drop the narrowing"
        );

        let refused = PermissionDecision::deny("no").remembered();
        assert!(
            !refused.remembers_for_session(),
            "a denial cannot carry the remember bit"
        );
        assert_eq!(refused, PermissionDecision::deny("no"));
    }
}
