//! Failures at the adapter seam.

use crate::capability::Capability;
use crate::ids::ToolCallId;

/// What can go wrong driving a provider harness.
///
/// Variants are distinguished by what the Worker should *do* about them, not by
/// which line of adapter code produced them.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// The harness could not be started. Retrying on this Worker will not help
    /// until the environment changes.
    #[error("failed to start the {provider} harness: {message}")]
    Spawn { provider: String, message: String },

    /// The harness rejected our credentials, or had none.
    ///
    /// Separate from [`Self::Spawn`] because the scheduler's answer is
    /// different: try another provider account, not another Worker. Spike W1c
    /// found Claude Code reports this as a *successful-looking* result envelope
    /// carrying an error flag, so adapters must actively recognize it rather
    /// than let it pass as a completed turn.
    #[error("provider {provider} is not authenticated{}: {message}", account_suffix(.account.as_deref()))]
    Unauthenticated {
        provider: String,
        account: Option<String>,
        message: String,
    },

    /// The harness's output could not be understood. Carries the offending
    /// bytes, because a protocol that drifted is diagnosed from the raw line
    /// and nothing else.
    #[error("could not decode harness output: {message}; raw: {raw}")]
    Protocol { message: String, raw: String },

    /// The harness ended without finishing what it was doing.
    #[error("the harness exited unexpectedly{}", exit_suffix(*.exit_code))]
    HarnessExited { exit_code: Option<i32> },

    /// Something was asked of a harness that cannot do it.
    ///
    /// A hard error rather than a silent no-op: an interrupt that did not
    /// interrupt, or a tool-input rewrite that did not apply, is exactly the
    /// kind of quiet failure the permission chain cannot tolerate.
    #[error("provider {provider} does not support {capability:?}")]
    Unsupported {
        provider: String,
        capability: Capability,
    },

    /// A permission answer arrived for a call that was not waiting on one.
    #[error("no permission request is outstanding for tool call {tool_call_id}")]
    UnknownPermissionRequest { tool_call_id: ToolCallId },

    /// Talking to the harness failed at the transport level.
    #[error("i/o error talking to the harness: {0}")]
    Io(#[from] std::io::Error),
}

impl AdapterError {
    /// Whether the same Worker could sensibly try this agent again.
    ///
    /// Authentication and capability failures are properties of the binding,
    /// not of the moment, so retrying them here only burns time; the scheduler
    /// needs a different account or a different provider.
    #[must_use]
    pub fn is_retryable_here(&self) -> bool {
        match self {
            Self::HarnessExited { .. } | Self::Io(_) | Self::Protocol { .. } => true,
            Self::Spawn { .. }
            | Self::Unauthenticated { .. }
            | Self::Unsupported { .. }
            | Self::UnknownPermissionRequest { .. } => false,
        }
    }
}

fn account_suffix(account: Option<&str>) -> String {
    account.map_or_else(String::new, |name| format!(" for account {name}"))
}

fn exit_suffix(code: Option<i32>) -> String {
    code.map_or_else(
        || " with no exit status".to_owned(),
        |code| format!(" with code {code}"),
    )
}

#[cfg(test)]
mod tests {
    use super::AdapterError;
    use crate::capability::Capability;

    #[test]
    fn an_unauthenticated_error_names_the_account_when_it_has_one() {
        let error = AdapterError::Unauthenticated {
            provider: "claude".to_owned(),
            account: Some("work".to_owned()),
            message: "Not logged in".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "provider claude is not authenticated for account work: Not logged in"
        );
    }

    #[test]
    fn an_unauthenticated_error_reads_cleanly_without_an_account() {
        let error = AdapterError::Unauthenticated {
            provider: "claude".to_owned(),
            account: None,
            message: "Not logged in".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "provider claude is not authenticated: Not logged in"
        );
    }

    #[test]
    fn a_missing_exit_status_is_stated_rather_than_rendered_as_none() {
        let error = AdapterError::HarnessExited { exit_code: None };
        assert_eq!(
            error.to_string(),
            "the harness exited unexpectedly with no exit status"
        );
    }

    #[test]
    fn binding_failures_are_not_retryable_on_this_worker() {
        assert!(
            !AdapterError::Unsupported {
                provider: "codex".to_owned(),
                capability: Capability::Resumable,
            }
            .is_retryable_here()
        );

        assert!(AdapterError::HarnessExited { exit_code: Some(1) }.is_retryable_here());
    }
}
