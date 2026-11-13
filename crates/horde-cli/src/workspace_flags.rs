//! The workspace flags ADR-0014 adds: a Definition's checkout mode, per-repo
//! reclaim commands and cleanup overrides, and a spawn's mode, initial
//! repos, bases and branch.
//!
//! Parsing lives here, apart from `cli.rs`, so each flag's grammar is a pure
//! function with its own truth table, and refusals name the flag.

use std::time::Duration;

use clap::Args;
use horde_proto::v1::{
    CheckoutMode, ReclaimPolicy, RepoBase, RepoBaseSource, WorkspaceCleanupPolicy, WorkspaceRepo,
    reclaim_policy,
};

use crate::client::ClientError;

/// The checkout modes, in CLI words.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModeChoice {
    /// Every repo of the workspace is checked out at spawn.
    All,
    /// Only the repos an agent names; the rest on demand.
    Lazy,
}

/// The wire mode one CLI choice asks for.
#[must_use]
pub fn mode_for(choice: Option<ModeChoice>) -> CheckoutMode {
    match choice {
        Some(ModeChoice::All) => CheckoutMode::All,
        Some(ModeChoice::Lazy) => CheckoutMode::Lazy,
        None => CheckoutMode::Unspecified,
    }
}

/// The policy flags `workspace create` and `workspace update` share.
#[derive(Args, Debug, Default, Clone)]
pub struct WorkspacePolicyArgs {
    /// Check out every repo at spawn (`all`, the default) or only the repos
    /// an agent names (`lazy`).
    #[arg(long, value_enum)]
    pub checkout_mode: Option<ModeChoice>,
    /// How one repo's checkouts are soft-reclaimed: `auto` (Cargo.toml runs
    /// `cargo clean`), `none`, or a JSON argv such as '["make","clean"]';
    /// repeatable.
    #[arg(long = "reclaim", value_name = "REPO=auto|none|JSON-ARGV")]
    pub reclaim: Vec<String>,
    /// A per-repo reclaim timeout, e.g. atlas=20m; repeatable.
    #[arg(long = "reclaim-timeout", value_name = "REPO=DUR")]
    pub reclaim_timeout: Vec<String>,
    /// Soft-reclaim a live agent idle this long (0 = never), e.g. 4h.
    #[arg(long, value_name = "DUR")]
    pub idle_reclaim_after: Option<String>,
    /// How long an archived agent's clean checkout is kept before it is
    /// removed automatically (0 = at archive), e.g. 24h.
    #[arg(long, value_name = "DUR")]
    pub archived_grace: Option<String>,
}

fn invalid(message: String) -> ClientError {
    ClientError::Rpc(tonic::Status::invalid_argument(message))
}

/// A duration as `0`, `90s`, `10m`, `4h` or `1d` (a bare number is seconds).
///
/// # Errors
///
/// Anything else, naming the flag and the accepted forms.
pub fn parse_duration(flag: &str, value: &str) -> Result<Duration, ClientError> {
    horde_proto::units::parse_duration(value).map_err(|why| invalid(format!("{flag}: {why}")))
}

/// A std duration on the wire.
#[must_use]
pub fn wire(duration: Duration) -> horde_proto::prost_types::Duration {
    horde_proto::prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: 0,
    }
}

/// One `--reclaim` value as `(repo, policy)`.
///
/// # Errors
///
/// A value that is not `REPO=auto`, `REPO=none` or `REPO=<JSON array of
/// strings>`.
pub fn reclaim_from(value: &str) -> Result<(String, ReclaimPolicy), ClientError> {
    let refuse = |why: &str| {
        invalid(format!(
            "--reclaim takes REPO=auto, REPO=none or REPO='[\"argv\",...]' ({why}), got {value:?}"
        ))
    };
    let (repo, policy) = value.split_once('=').ok_or_else(|| refuse("no '='"))?;
    if repo.is_empty() {
        return Err(refuse("no repo name"));
    }
    let policy = match policy.trim() {
        "auto" => reclaim_policy::Policy::Auto(reclaim_policy::Auto {}),
        "none" => reclaim_policy::Policy::Disabled(reclaim_policy::Disabled {}),
        json => {
            let argv: Vec<String> =
                serde_json::from_str(json).map_err(|_| refuse("not a JSON array of strings"))?;
            if argv.is_empty() {
                return Err(refuse("an empty argv"));
            }
            reclaim_policy::Policy::Command(reclaim_policy::Command {
                argv,
                timeout: None,
                output_paths: Vec::new(),
            })
        }
    };
    Ok((
        repo.to_owned(),
        ReclaimPolicy {
            policy: Some(policy),
        },
    ))
}

/// Applies `--reclaim` and `--reclaim-timeout` to the repos they name.
///
/// # Errors
///
/// A value that does not parse, a repo the list does not hold, or a
/// timeout for a repo with no reclaim command.
pub fn apply_reclaim(
    repos: &mut [WorkspaceRepo],
    args: &WorkspacePolicyArgs,
) -> Result<(), ClientError> {
    for value in &args.reclaim {
        let (name, policy) = reclaim_from(value)?;
        let repo = find_repo(repos, &name, "--reclaim")?;
        repo.reclaim = Some(policy);
    }
    for value in &args.reclaim_timeout {
        let (name, duration) = value
            .split_once('=')
            .ok_or_else(|| invalid(format!("--reclaim-timeout takes REPO=DUR, got {value:?}")))?;
        let duration = parse_duration("--reclaim-timeout", duration)?;
        let repo = find_repo(repos, name, "--reclaim-timeout")?;
        match repo
            .reclaim
            .as_mut()
            .and_then(|reclaim| reclaim.policy.as_mut())
        {
            Some(reclaim_policy::Policy::Command(command)) => {
                command.timeout = Some(wire(duration))
            }
            _ => {
                return Err(invalid(format!(
                    "--reclaim-timeout {name}: repo {name} has no reclaim command (set one with --reclaim)"
                )));
            }
        }
    }
    Ok(())
}

fn find_repo<'a>(
    repos: &'a mut [WorkspaceRepo],
    name: &str,
    flag: &str,
) -> Result<&'a mut WorkspaceRepo, ClientError> {
    let known: Vec<String> = repos.iter().map(|repo| repo.name.clone()).collect();
    repos
        .iter_mut()
        .find(|repo| repo.name == name)
        .ok_or_else(|| {
            invalid(format!(
                "{flag} names repo {name:?}, not one of: {}",
                known.join(", ")
            ))
        })
}

/// The cleanup overrides the flags set, or `None` when they set none.
///
/// # Errors
///
/// A duration that does not parse.
pub fn cleanup_from(
    args: &WorkspacePolicyArgs,
) -> Result<Option<WorkspaceCleanupPolicy>, ClientError> {
    let idle = args
        .idle_reclaim_after
        .as_deref()
        .map(|value| parse_duration("--idle-reclaim-after", value))
        .transpose()?;
    let grace = args
        .archived_grace
        .as_deref()
        .map(|value| parse_duration("--archived-grace", value))
        .transpose()?;
    if idle.is_none() && grace.is_none() {
        return Ok(None);
    }
    Ok(Some(WorkspaceCleanupPolicy {
        idle_reclaim_after: idle.map(wire),
        archived_grace: grace.map(wire),
    }))
}

/// The spawn flags ADR-0014 adds, shared by `agent spawn` and `new`.
#[derive(Args, Debug, Default, Clone)]
pub struct CheckoutArgs {
    /// Override the workspace's checkout mode for this agent.
    #[arg(long, value_enum)]
    pub checkout_mode: Option<ModeChoice>,
    /// The repos checked out at start, comma-separated; implies lazy.
    #[arg(long, value_delimiter = ',', value_name = "REPO,...")]
    pub repos: Vec<String>,
    /// Where a repo's checkout starts: a commit, an origin branch or a tag;
    /// repeatable. A bare REF only for a one-repo workspace.
    #[arg(long = "base", value_name = "[REPO=]REF")]
    pub bases: Vec<String>,
    /// The branch created in every checked-out repo (default
    /// horde/<agent-id>); one on origin is continued.
    #[arg(long)]
    pub branch: Option<String>,
}

/// One `--base` value as a requested base. A bare ref leaves the repo name
/// empty for the Server to fill in a one-repo workspace (and refuse in a
/// larger one).
///
/// # Errors
///
/// An empty repo or ref.
pub fn base_from(value: &str) -> Result<RepoBase, ClientError> {
    let (repo, reference) = match value.split_once('=') {
        Some((repo, reference)) => {
            if repo.is_empty() || repo.starts_with('-') {
                return Err(invalid(format!("--base takes [REPO=]REF, got {value:?}")));
            }
            (repo, reference)
        }
        None => ("", value),
    };
    if reference.is_empty() || reference.starts_with('-') {
        return Err(invalid(format!("--base takes [REPO=]REF, got {value:?}")));
    }
    Ok(RepoBase {
        repo_name: repo.to_owned(),
        r#ref: reference.to_owned(),
        source: RepoBaseSource::Requested.into(),
    })
}

/// The spec fields these flags set: `(mode, initial repos, bases, branch)`.
///
/// # Errors
///
/// A `--base` that does not parse.
pub fn checkout_fields(
    args: &CheckoutArgs,
) -> Result<(CheckoutMode, Vec<String>, Vec<RepoBase>, String), ClientError> {
    let bases = args
        .bases
        .iter()
        .map(|value| base_from(value))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        mode_for(args.checkout_mode),
        args.repos
            .iter()
            .filter(|repo| !repo.is_empty())
            .cloned()
            .collect(),
        bases,
        args.branch.clone().unwrap_or_default(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_take_seconds_minutes_hours_and_days() {
        for (value, seconds) in [
            ("0", 0),
            ("90", 90),
            ("90s", 90),
            ("10m", 600),
            ("4h", 14_400),
            ("1d", 86_400),
        ] {
            assert_eq!(
                parse_duration("--x", value).expect("parses"),
                Duration::from_secs(seconds),
                "{value}"
            );
        }
        for wrong in ["", "4w", "h", "-1s", "1.5h", "99999999999999999999d"] {
            let error = parse_duration("--archived-grace", wrong).expect_err("refused");
            assert!(error.report().contains("--archived-grace"), "{wrong}");
        }
    }

    #[test]
    fn a_reclaim_value_is_auto_none_or_a_json_argv() {
        let (repo, auto) = reclaim_from("atlas=auto").expect("auto");
        assert_eq!(repo, "atlas");
        assert!(matches!(auto.policy, Some(reclaim_policy::Policy::Auto(_))));
        let (_, none) = reclaim_from("atlas=none").expect("none");
        assert!(matches!(
            none.policy,
            Some(reclaim_policy::Policy::Disabled(_))
        ));
        let (_, command) = reclaim_from(r#"atlas=["make","clean"]"#).expect("argv");
        let Some(reclaim_policy::Policy::Command(command)) = command.policy else {
            panic!("a command");
        };
        assert_eq!(command.argv, vec!["make", "clean"]);
        for wrong in [
            "atlas",
            "=auto",
            "atlas=[]",
            "atlas=make clean",
            r#"atlas=[1]"#,
        ] {
            assert!(reclaim_from(wrong).is_err(), "{wrong}");
        }
    }

    fn repos() -> Vec<WorkspaceRepo> {
        vec![WorkspaceRepo {
            name: "atlas".to_owned(),
            ..WorkspaceRepo::default()
        }]
    }

    #[test]
    fn reclaim_flags_land_on_their_repo_and_a_timeout_needs_a_command() {
        let mut list = repos();
        let args = WorkspacePolicyArgs {
            reclaim: vec![r#"atlas=["make","clean"]"#.to_owned()],
            reclaim_timeout: vec!["atlas=20m".to_owned()],
            ..WorkspacePolicyArgs::default()
        };
        apply_reclaim(&mut list, &args).expect("applies");
        let Some(reclaim_policy::Policy::Command(command)) =
            list[0].reclaim.as_ref().and_then(|r| r.policy.clone())
        else {
            panic!("a command");
        };
        assert_eq!(command.timeout.map(|t| t.seconds), Some(1_200));

        let mut list = repos();
        let unknown = WorkspacePolicyArgs {
            reclaim: vec!["ghost=auto".to_owned()],
            ..WorkspacePolicyArgs::default()
        };
        let error = apply_reclaim(&mut list, &unknown).expect_err("ghost is unknown");
        assert!(
            error.report().contains("not one of: atlas"),
            "{}",
            error.report()
        );

        let mut list = repos();
        let orphan_timeout = WorkspacePolicyArgs {
            reclaim_timeout: vec!["atlas=1m".to_owned()],
            ..WorkspacePolicyArgs::default()
        };
        assert!(apply_reclaim(&mut list, &orphan_timeout).is_err());
    }

    #[test]
    fn cleanup_overrides_are_set_only_when_named() {
        assert_eq!(
            cleanup_from(&WorkspacePolicyArgs::default()).expect("none"),
            None
        );
        let cleanup = cleanup_from(&WorkspacePolicyArgs {
            idle_reclaim_after: Some("0".to_owned()),
            archived_grace: Some("24h".to_owned()),
            ..WorkspacePolicyArgs::default()
        })
        .expect("parses")
        .expect("some");
        assert_eq!(cleanup.idle_reclaim_after.map(|d| d.seconds), Some(0));
        assert_eq!(cleanup.archived_grace.map(|d| d.seconds), Some(86_400));
    }

    #[test]
    fn a_base_is_repo_qualified_or_bare() {
        let qualified = base_from("atlas=release/1.4").expect("qualified");
        assert_eq!(qualified.repo_name, "atlas");
        assert_eq!(qualified.r#ref, "release/1.4");
        assert_eq!(qualified.source, i32::from(RepoBaseSource::Requested));
        let bare = base_from("9c1e").expect("bare");
        assert!(bare.repo_name.is_empty());
        assert_eq!(bare.r#ref, "9c1e");
        for wrong in ["", "=main", "atlas=", "--upload-pack=x"] {
            assert!(base_from(wrong).is_err(), "{wrong}");
        }
    }

    #[test]
    fn checkout_fields_carry_every_flag() {
        let (mode, repos, bases, branch) = checkout_fields(&CheckoutArgs {
            checkout_mode: Some(ModeChoice::Lazy),
            repos: vec!["atlas".to_owned(), String::new()],
            bases: vec!["atlas=main".to_owned()],
            branch: Some("feat/login".to_owned()),
        })
        .expect("parses");
        assert_eq!(mode, CheckoutMode::Lazy);
        assert_eq!(repos, vec!["atlas"]);
        assert_eq!(bases.len(), 1);
        assert_eq!(branch, "feat/login");
        assert_eq!(mode_for(None), CheckoutMode::Unspecified);
        assert_eq!(mode_for(Some(ModeChoice::All)), CheckoutMode::All);
    }
}
