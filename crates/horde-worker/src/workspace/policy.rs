//! The workspace policy a command carries, read with its defaults.
//!
//! The Server resolves the policy (built-in defaults, then its config, then
//! the Definition's overrides) and stamps it into every spawn, materialize
//! and gc command (ADR-0014); the Worker never reads Server config. A field
//! the Server left unset — an older Server, or a test command — reads as the
//! built-in default here, so the two sides agree on what "unset" means.

use std::time::Duration;

use horde_proto::prost_types::Duration as WireDuration;
use horde_proto::v1::{ReclaimPolicy, ResolvedWorkspacePolicy, reclaim_policy};

use super::base::FetchPolicy;

/// Built-in defaults (ADR-0014's configuration table, with resolution 1's
/// 24 h grace).
pub const DEFAULT_IDLE_RECLAIM_AFTER: Duration = Duration::from_secs(4 * 3600);
pub const DEFAULT_ARCHIVED_GRACE: Duration = Duration::from_secs(24 * 3600);
pub const DEFAULT_RECLAIM_TIMEOUT: Duration = Duration::from_secs(600);

/// One auto rule: a marker file at the repo root selects a command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoRule {
    pub marker_file: String,
    pub command: CommandSpec,
}

/// A reclaim command as the Worker runs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSpec {
    pub argv: Vec<String>,
    pub timeout: Option<Duration>,
    pub output_paths: Vec<String>,
}

impl CommandSpec {
    fn from_wire(command: &reclaim_policy::Command) -> Self {
        Self {
            argv: command.argv.clone(),
            timeout: command.timeout.and_then(duration_of),
            output_paths: command.output_paths.clone(),
        }
    }
}

/// The policy with every default applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspacePolicy {
    pub fetch: FetchPolicy,
    /// `None`: never reclaim an idle agent.
    pub idle_reclaim_after: Option<Duration>,
    pub archived_grace: Duration,
    pub reclaim_timeout: Duration,
    pub auto_rules: Vec<AutoRule>,
    /// `None`: archive pins never expire (ADR-0014 resolution 4).
    pub archived_pin_ttl: Option<Duration>,
}

impl Default for WorkspacePolicy {
    fn default() -> Self {
        Self::from_wire(None)
    }
}

impl WorkspacePolicy {
    /// Reads a wire policy, defaulting every unset field.
    #[must_use]
    pub fn from_wire(wire: Option<&ResolvedWorkspacePolicy>) -> Self {
        let wire = wire.cloned().unwrap_or_default();
        let defaults = FetchPolicy::default();
        let auto_rules = if wire.auto_rules.is_empty() {
            vec![cargo_rule()]
        } else {
            wire.auto_rules
                .iter()
                .filter_map(|rule| {
                    Some(AutoRule {
                        marker_file: rule.marker_file.clone(),
                        command: CommandSpec::from_wire(rule.command.as_ref()?),
                    })
                })
                .collect()
        };
        Self {
            fetch: FetchPolicy {
                throttle: wire
                    .fetch_throttle
                    .and_then(duration_of)
                    .unwrap_or(defaults.throttle),
                timeout: wire
                    .fetch_timeout
                    .and_then(duration_of)
                    .unwrap_or(defaults.timeout),
            },
            idle_reclaim_after: match wire.idle_reclaim_after.and_then(duration_of) {
                None => Some(DEFAULT_IDLE_RECLAIM_AFTER),
                Some(Duration::ZERO) => None,
                Some(after) => Some(after),
            },
            archived_grace: wire
                .archived_grace
                .and_then(duration_of)
                .unwrap_or(DEFAULT_ARCHIVED_GRACE),
            reclaim_timeout: wire
                .reclaim_timeout
                .and_then(duration_of)
                .unwrap_or(DEFAULT_RECLAIM_TIMEOUT),
            auto_rules,
            archived_pin_ttl: wire.archived_pin_ttl.and_then(duration_of),
        }
    }

    /// The command a repo's reclaim policy resolves to, before any marker
    /// is looked at: `Ok(Some)` for an explicit command, `Ok(None)` for auto,
    /// `Err` with the skip reason for a disabled repo.
    pub fn explicit_command(
        reclaim: Option<&ReclaimPolicy>,
    ) -> Result<Option<CommandSpec>, String> {
        match reclaim.and_then(|reclaim| reclaim.policy.as_ref()) {
            Some(reclaim_policy::Policy::Disabled(_)) => Err("no reclaim command".to_owned()),
            Some(reclaim_policy::Policy::Command(command)) => {
                Ok(Some(CommandSpec::from_wire(command)))
            }
            Some(reclaim_policy::Policy::Auto(_)) | None => Ok(None),
        }
    }
}

/// The built-in auto rule: a root `Cargo.toml` reclaims with `cargo clean`.
#[must_use]
pub fn cargo_rule() -> AutoRule {
    AutoRule {
        marker_file: "Cargo.toml".to_owned(),
        command: CommandSpec {
            argv: vec!["cargo".to_owned(), "clean".to_owned()],
            timeout: None,
            output_paths: Vec::new(),
        },
    }
}

/// A wire duration as a std one; negative is no duration at all.
#[must_use]
pub fn duration_of(duration: WireDuration) -> Option<Duration> {
    let seconds = u64::try_from(duration.seconds).ok()?;
    let nanos = u32::try_from(duration.nanos).ok()?;
    Some(Duration::new(seconds, nanos))
}

/// A std duration on the wire.
#[must_use]
pub fn wire_duration(duration: Duration) -> WireDuration {
    WireDuration {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(duration.subsec_nanos()).unwrap_or(0),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::resolved_workspace_policy::MarkerRule;

    use super::*;

    #[test]
    fn an_unset_policy_reads_as_the_built_in_defaults() {
        let policy = WorkspacePolicy::from_wire(None);
        assert_eq!(policy.fetch, FetchPolicy::default());
        assert_eq!(
            policy.idle_reclaim_after,
            Some(Duration::from_secs(14_400)),
            "four hours"
        );
        assert_eq!(policy.archived_grace, Duration::from_secs(86_400));
        assert_eq!(policy.reclaim_timeout, Duration::from_secs(600));
        assert_eq!(policy.auto_rules, vec![cargo_rule()]);
        assert_eq!(policy.archived_pin_ttl, None);
    }

    /// Zero idle means never; zero grace means at archive; a set auto list
    /// replaces the built-in one.
    #[test]
    fn set_fields_override_and_zero_idle_means_never() {
        let wire = ResolvedWorkspacePolicy {
            fetch_throttle: Some(wire_duration(Duration::from_secs(5))),
            fetch_timeout: Some(wire_duration(Duration::from_secs(2))),
            idle_reclaim_after: Some(wire_duration(Duration::ZERO)),
            archived_grace: Some(wire_duration(Duration::ZERO)),
            reclaim_timeout: Some(wire_duration(Duration::from_secs(1))),
            auto_rules: vec![MarkerRule {
                marker_file: "Makefile".to_owned(),
                command: Some(reclaim_policy::Command {
                    argv: vec!["make".to_owned(), "clean".to_owned()],
                    timeout: None,
                    output_paths: vec!["build".to_owned()],
                }),
            }],
            archived_pin_ttl: Some(wire_duration(Duration::from_secs(30))),
        };
        let policy = WorkspacePolicy::from_wire(Some(&wire));
        assert_eq!(policy.fetch.throttle, Duration::from_secs(5));
        assert_eq!(policy.fetch.timeout, Duration::from_secs(2));
        assert_eq!(policy.idle_reclaim_after, None);
        assert_eq!(policy.archived_grace, Duration::ZERO);
        assert_eq!(policy.reclaim_timeout, Duration::from_secs(1));
        assert_eq!(policy.auto_rules.len(), 1);
        assert_eq!(policy.auto_rules[0].marker_file, "Makefile");
        assert_eq!(policy.auto_rules[0].command.output_paths, vec!["build"]);
        assert_eq!(policy.archived_pin_ttl, Some(Duration::from_secs(30)));

        let idle = ResolvedWorkspacePolicy {
            idle_reclaim_after: Some(wire_duration(Duration::from_secs(9))),
            ..ResolvedWorkspacePolicy::default()
        };
        assert_eq!(
            WorkspacePolicy::from_wire(Some(&idle)).idle_reclaim_after,
            Some(Duration::from_secs(9))
        );
    }

    #[test]
    fn a_repos_reclaim_policy_picks_explicit_auto_or_skip() {
        assert_eq!(WorkspacePolicy::explicit_command(None), Ok(None));
        let auto = ReclaimPolicy {
            policy: Some(reclaim_policy::Policy::Auto(reclaim_policy::Auto {})),
        };
        assert_eq!(WorkspacePolicy::explicit_command(Some(&auto)), Ok(None));
        let disabled = ReclaimPolicy {
            policy: Some(reclaim_policy::Policy::Disabled(
                reclaim_policy::Disabled {},
            )),
        };
        assert_eq!(
            WorkspacePolicy::explicit_command(Some(&disabled)),
            Err("no reclaim command".to_owned())
        );
        let command = ReclaimPolicy {
            policy: Some(reclaim_policy::Policy::Command(reclaim_policy::Command {
                argv: vec!["make".to_owned()],
                timeout: Some(wire_duration(Duration::from_secs(3))),
                output_paths: Vec::new(),
            })),
        };
        assert_eq!(
            WorkspacePolicy::explicit_command(Some(&command)),
            Ok(Some(CommandSpec {
                argv: vec!["make".to_owned()],
                timeout: Some(Duration::from_secs(3)),
                output_paths: Vec::new(),
            }))
        );
    }

    #[test]
    fn a_negative_wire_duration_is_none_and_durations_round_trip() {
        assert_eq!(
            duration_of(WireDuration {
                seconds: -1,
                nanos: 0
            }),
            None
        );
        let back = duration_of(wire_duration(Duration::from_millis(1_500)));
        assert_eq!(back, Some(Duration::from_millis(1_500)));
    }
}
