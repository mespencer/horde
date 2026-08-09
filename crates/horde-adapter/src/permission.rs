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
        }
    }

    /// Allows the call, but with these arguments instead.
    #[must_use]
    pub fn allow_with(updated_input: Value) -> Self {
        Self::Allow {
            updated_input: Some(updated_input),
        }
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
            Self::Allow { updated_input } => updated_input.as_ref(),
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
}
