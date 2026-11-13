//! The Server's half of ADR-0014: the workspace policy it resolves and
//! stamps into commands, and the validation of every new Definition and
//! spawn field before anything is written.
//!
//! Pure functions over wire types, so each rule is a truth table here and
//! the RPC handlers stay thin.

use std::collections::BTreeSet;
use std::path::{Component, Path};
use std::time::Duration;

use horde_proto::v1::resolved_workspace_policy::MarkerRule;
use horde_proto::v1::{
    AgentSpec, CheckoutMode, CheckoutPolicy, ReclaimPolicy, ResolvedWorkspacePolicy,
    WorkspaceDefinition, WorkspaceRepo, reclaim_policy,
};

use crate::error::ServerError;

/// The Server's workspace defaults (ADR-0014's `workspace.*` keys; flags
/// until W12e's config file).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkspacePolicyConfig {
    pub fetch_throttle: Duration,
    pub fetch_timeout: Duration,
    /// Zero: never.
    pub idle_reclaim_after: Duration,
    /// Default 24 h (ADR-0014 resolution 1); zero removes at archive.
    pub archived_grace: Duration,
    pub reclaim_timeout: Duration,
    /// `None`: archive pins never expire (resolution 4).
    pub archived_pin_ttl: Option<Duration>,
    /// Replaces the built-in list when non-empty.
    pub auto_rules: Vec<MarkerRule>,
}

impl Default for WorkspacePolicyConfig {
    fn default() -> Self {
        Self {
            fetch_throttle: Duration::from_secs(60),
            fetch_timeout: Duration::from_secs(30),
            idle_reclaim_after: Duration::from_secs(4 * 3600),
            archived_grace: Duration::from_secs(24 * 3600),
            reclaim_timeout: Duration::from_secs(600),
            archived_pin_ttl: None,
            auto_rules: vec![MarkerRule {
                marker_file: "Cargo.toml".to_owned(),
                command: Some(reclaim_policy::Command {
                    argv: vec!["cargo".to_owned(), "clean".to_owned()],
                    timeout: None,
                    output_paths: Vec::new(),
                }),
            }],
        }
    }
}

fn wire(duration: Duration) -> horde_proto::prost_types::Duration {
    horde_proto::prost_types::Duration {
        seconds: i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(duration.subsec_nanos()).unwrap_or(0),
    }
}

/// The policy a command carries: built-in defaults, then the Server's
/// configuration, then the Definition's cleanup overrides.
#[must_use]
pub fn resolve(
    config: &WorkspacePolicyConfig,
    definition: Option<&WorkspaceDefinition>,
) -> ResolvedWorkspacePolicy {
    let cleanup = definition.and_then(|definition| definition.cleanup.as_ref());
    ResolvedWorkspacePolicy {
        fetch_throttle: Some(wire(config.fetch_throttle)),
        fetch_timeout: Some(wire(config.fetch_timeout)),
        idle_reclaim_after: cleanup
            .and_then(|cleanup| cleanup.idle_reclaim_after)
            .or(Some(wire(config.idle_reclaim_after))),
        archived_grace: cleanup
            .and_then(|cleanup| cleanup.archived_grace)
            .or(Some(wire(config.archived_grace))),
        reclaim_timeout: Some(wire(config.reclaim_timeout)),
        auto_rules: config.auto_rules.clone(),
        archived_pin_ttl: config.archived_pin_ttl.map(wire),
    }
}

/// Refuses an unmanaged path the Instance cannot hold safely: it must be
/// relative with every component normal, not begin `.horde`, not equal,
/// contain or lie inside a repo layout, and not nest with another.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the path.
pub fn validate_unmanaged_paths(repos: &[WorkspaceRepo], paths: &[String]) -> Result<(), ServerError> {
    let layouts: Vec<&str> = repos.iter().map(layout_of).collect();
    for (index, path) in paths.iter().enumerate() {
        let normal = !path.is_empty()
            && Path::new(path)
                .components()
                .all(|component| matches!(component, Component::Normal(_)));
        if !normal || path.starts_with(".horde") {
            return Err(ServerError::Invalid(format!(
                "unmanaged path {path:?} must be a relative path inside the instance, not under .horde"
            )));
        }
        if let Some(layout) = layouts.iter().find(|layout| nested(path, layout)) {
            return Err(ServerError::Invalid(format!(
                "unmanaged path {path:?} overlaps the repo laid out at {layout:?}"
            )));
        }
        if let Some(other) = paths.iter().skip(index + 1).find(|other| nested(path, other)) {
            return Err(ServerError::Invalid(format!(
                "unmanaged paths {path:?} and {other:?} nest inside each other"
            )));
        }
    }
    Ok(())
}

/// Whether two relative paths are equal or one contains the other, by
/// whole components.
#[must_use]
pub fn nested(a: &str, b: &str) -> bool {
    let (a, b) = (Path::new(a), Path::new(b));
    a.starts_with(b) || b.starts_with(a)
}

fn layout_of(repo: &WorkspaceRepo) -> &str {
    if repo.layout_path.is_empty() {
        &repo.name
    } else {
        &repo.layout_path
    }
}

/// Refuses a layout under `.horde` (the manifest lives there), and — under
/// a lazy Definition — nested layouts, since a lazily added outer repo would
/// land on a non-empty directory.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the layouts.
pub fn validate_layouts(repos: &[WorkspaceRepo], mode: CheckoutMode) -> Result<(), ServerError> {
    for repo in repos {
        if layout_of(repo).starts_with(".horde") {
            return Err(ServerError::Invalid(format!(
                "repo {} lays out under .horde, which holds the workspace manifest",
                repo.name
            )));
        }
    }
    if mode != CheckoutMode::Lazy {
        return Ok(());
    }
    for (index, repo) in repos.iter().enumerate() {
        for other in repos.iter().skip(index + 1) {
            if nested(layout_of(repo), layout_of(other)) {
                return Err(ServerError::Invalid(format!(
                    "a lazy workspace cannot nest repo layouts: {} at {:?} and {} at {:?}",
                    repo.name,
                    layout_of(repo),
                    other.name,
                    layout_of(other)
                )));
            }
        }
    }
    Ok(())
}

/// Refuses a reclaim command the Worker could not run safely: an empty
/// argv, a relative program path, a NUL byte, more than 64 entries or
/// 4 KiB in total, or an output path outside the repo.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the repo.
pub fn validate_reclaim(repos: &[WorkspaceRepo]) -> Result<(), ServerError> {
    for repo in repos {
        let Some(reclaim_policy::Policy::Command(command)) =
            repo.reclaim.as_ref().and_then(|reclaim: &ReclaimPolicy| reclaim.policy.as_ref())
        else {
            continue;
        };
        reclaim_argv_shortfall(&command.argv, &command.output_paths)
            .map_or(Ok(()), |why| {
                Err(ServerError::Invalid(format!(
                    "repo {}'s reclaim command {why}",
                    repo.name
                )))
            })?;
    }
    Ok(())
}

/// What is wrong with a reclaim argv, if anything.
#[must_use]
pub fn reclaim_argv_shortfall(argv: &[String], output_paths: &[String]) -> Option<String> {
    let Some(program) = argv.first() else {
        return Some("is empty".to_owned());
    };
    if argv.len() > 64 {
        return Some("has more than 64 entries".to_owned());
    }
    if argv.iter().map(String::len).sum::<usize>() > 4096 {
        return Some("is longer than 4 KiB".to_owned());
    }
    if argv.iter().any(|arg| arg.contains('\0')) {
        return Some("contains a NUL byte".to_owned());
    }
    let absolute = Path::new(program).is_absolute();
    if !absolute && (program.contains('/') || program.contains('\\') || program.is_empty()) {
        return Some(format!(
            "names {program:?}: the program must be a bare name on PATH or an absolute path"
        ));
    }
    let escaping = output_paths.iter().find(|path| {
        path.is_empty()
            || !Path::new(path)
                .components()
                .all(|component| matches!(component, Component::Normal(_)))
    });
    escaping.map(|path| format!("names output path {path:?}, which is not inside the repo"))
}

/// Every Definition rule ADR-0014 adds, for create and update.
///
/// # Errors
///
/// The first rule broken.
pub fn validate_definition_fields(
    repos: &[WorkspaceRepo],
    unmanaged_paths: &[String],
    mode: CheckoutMode,
) -> Result<(), ServerError> {
    validate_layouts(repos, mode)?;
    validate_unmanaged_paths(repos, unmanaged_paths)?;
    validate_reclaim(repos)
}

/// Refuses a branch name the Worker would refuse or that would take
/// another agent's default name: at most 200 bytes, no leading `-`, no
/// `..`, no control or space characters, none of `~^:?*[\`, not ending in
/// `.lock` or `/`, not `HEAD`, and not under `horde/` unless it is
/// `horde/<this agent>`.
///
/// # Errors
///
/// [`ServerError::Invalid`] saying which rule.
pub fn validate_branch(branch: &str, agent_id: &str) -> Result<(), ServerError> {
    if branch.is_empty() {
        return Ok(());
    }
    let refuse = |why: &str| Err(ServerError::Invalid(format!("branch {branch:?} {why}")));
    if branch.len() > 200 {
        return refuse("is longer than 200 bytes");
    }
    if branch.starts_with('-') || branch.starts_with('/') {
        return refuse("starts with '-' or '/'");
    }
    if branch.contains("..") || branch.contains("//") || branch.contains("@{") {
        return refuse("contains '..', '//' or '@{'");
    }
    if branch
        .chars()
        .any(|c| c.is_ascii_control() || c == ' ' || "~^:?*[\\".contains(c))
    {
        return refuse("contains a control character, a space, or one of ~^:?*[\\");
    }
    if branch.ends_with(".lock") || branch.ends_with('/') || branch.ends_with('.') {
        return refuse("ends with .lock, '/' or '.'");
    }
    if branch == "HEAD" {
        return refuse("is HEAD");
    }
    if branch.starts_with("horde/") && branch != format!("horde/{agent_id}") {
        return refuse("is under horde/, which names other agents' default branches");
    }
    Ok(())
}

/// Validates a spawn's ADR-0014 fields against its Definition, filling a
/// bare `--base` in a one-repo workspace with that repo's name.
///
/// # Errors
///
/// [`ServerError::Invalid`] for an unknown repo, an option-shaped or
/// ambiguous base, an invalid branch, or any of them under `shared`.
pub fn validated_spawn(
    mut spec: AgentSpec,
    definition: Option<&WorkspaceDefinition>,
    agent_id: &str,
) -> Result<AgentSpec, ServerError> {
    let asks = !spec.branch.is_empty()
        || !spec.repo_bases.is_empty()
        || !spec.checkout_repos.is_empty()
        || spec.checkout_mode == i32::from(CheckoutMode::Lazy);
    let Some(definition) = definition else {
        if asks {
            return Err(ServerError::Invalid(
                "a branch, base, repo list or lazy mode needs a workspace".to_owned(),
            ));
        }
        return Ok(spec);
    };
    if asks && spec.checkout_policy == i32::from(CheckoutPolicy::Shared) {
        return Err(ServerError::Invalid(
            "the shared policy works in the instance itself: it takes no branch, base, repo list \
             or lazy mode"
                .to_owned(),
        ));
    }
    validate_branch(&spec.branch, agent_id)?;
    let names: BTreeSet<&str> = definition.repos.iter().map(|repo| repo.name.as_str()).collect();
    let listed = || names.iter().copied().collect::<Vec<_>>().join(", ");
    for repo in &spec.checkout_repos {
        if !names.contains(repo.as_str()) {
            return Err(ServerError::Invalid(format!(
                "no repo {repo:?} in workspace {}; valid repos: {}",
                definition.name,
                listed()
            )));
        }
    }
    for base in &mut spec.repo_bases {
        if base.r#ref.is_empty() || base.r#ref.starts_with('-') {
            return Err(ServerError::Invalid(format!(
                "base {:?} is not a commit, branch or tag",
                base.r#ref
            )));
        }
        if base.repo_name.is_empty() {
            match definition.repos.as_slice() {
                [only] => base.repo_name = only.name.clone(),
                _ => {
                    return Err(ServerError::Invalid(format!(
                        "a bare base names no repo, and workspace {} has several: name it as \
                         REPO=REF ({})",
                        definition.name,
                        listed()
                    )));
                }
            }
        }
        if !names.contains(base.repo_name.as_str()) {
            return Err(ServerError::Invalid(format!(
                "a base names repo {:?}, not one of workspace {}'s: {}",
                base.repo_name,
                definition.name,
                listed()
            )));
        }
    }
    Ok(spec)
}

/// Whether a spawn uses any ADR-0014 field, and so needs a Worker of
/// workspace protocol 2.
#[must_use]
pub fn needs_workspace_protocol(spec: &AgentSpec) -> bool {
    !spec.branch.is_empty()
        || !spec.repo_bases.is_empty()
        || !spec.checkout_repos.is_empty()
        || spec.checkout_mode == i32::from(CheckoutMode::Lazy)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{RepoBase, WorkspaceCleanupPolicy};

    use super::*;

    fn repo(name: &str, layout: &str) -> WorkspaceRepo {
        WorkspaceRepo {
            name: name.to_owned(),
            remote_url: "u".to_owned(),
            layout_path: layout.to_owned(),
            ..WorkspaceRepo::default()
        }
    }

    #[test]
    fn the_policy_layers_defaults_config_and_definition() {
        let config = WorkspacePolicyConfig::default();
        let plain = resolve(&config, None);
        assert_eq!(plain.archived_grace.map(|d| d.seconds), Some(86_400));
        assert_eq!(plain.idle_reclaim_after.map(|d| d.seconds), Some(14_400));
        assert_eq!(plain.fetch_throttle.map(|d| d.seconds), Some(60));
        assert_eq!(plain.fetch_timeout.map(|d| d.seconds), Some(30));
        assert_eq!(plain.reclaim_timeout.map(|d| d.seconds), Some(600));
        assert_eq!(plain.archived_pin_ttl, None);
        assert_eq!(plain.auto_rules[0].marker_file, "Cargo.toml");

        let configured = WorkspacePolicyConfig {
            archived_grace: Duration::ZERO,
            archived_pin_ttl: Some(Duration::from_secs(30)),
            ..WorkspacePolicyConfig::default()
        };
        let definition = WorkspaceDefinition {
            cleanup: Some(WorkspaceCleanupPolicy {
                idle_reclaim_after: Some(wire(Duration::from_secs(5))),
                archived_grace: None,
            }),
            ..WorkspaceDefinition::default()
        };
        let layered = resolve(&configured, Some(&definition));
        assert_eq!(layered.idle_reclaim_after.map(|d| d.seconds), Some(5), "the definition wins");
        assert_eq!(layered.archived_grace.map(|d| d.seconds), Some(0), "the config fills the rest");
        assert_eq!(layered.archived_pin_ttl.map(|d| d.seconds), Some(30));
    }

    #[test]
    fn unmanaged_paths_stay_inside_apart_from_repos_and_each_other() {
        let repos = vec![repo("atlas", "code/atlas")];
        assert!(validate_unmanaged_paths(&repos, &["data".to_owned(), "cache/w".to_owned()]).is_ok());
        for wrong in [
            vec!["/data".to_owned()],
            vec!["../data".to_owned()],
            vec![".horde/x".to_owned()],
            vec![String::new()],
            vec!["code".to_owned()],
            vec!["code/atlas/data".to_owned()],
            vec!["data".to_owned(), "data/sub".to_owned()],
        ] {
            assert!(validate_unmanaged_paths(&repos, &wrong).is_err(), "{wrong:?}");
        }
    }

    #[test]
    fn a_lazy_workspace_refuses_nested_layouts_and_every_workspace_refuses_horde() {
        let nested_repos = vec![repo("outer", "code"), repo("inner", "code/inner")];
        assert!(validate_layouts(&nested_repos, CheckoutMode::All).is_ok());
        assert!(validate_layouts(&nested_repos, CheckoutMode::Lazy).is_err());
        let apart = vec![repo("a", "a"), repo("b", "b")];
        assert!(validate_layouts(&apart, CheckoutMode::Lazy).is_ok());
        assert!(validate_layouts(&[repo("a", ".horde/a")], CheckoutMode::All).is_err());
        assert!(!nested("ab", "a"), "a prefix is not a parent");
    }

    #[test]
    fn a_reclaim_argv_is_bounded_and_runs_no_relative_program() {
        let argv = |items: &[&str]| items.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>();
        assert_eq!(reclaim_argv_shortfall(&argv(&["make", "clean"]), &[]), None);
        assert_eq!(reclaim_argv_shortfall(&argv(&["/usr/bin/make"]), &argv(&["build"])), None);
        assert!(reclaim_argv_shortfall(&[], &[]).is_some());
        assert!(reclaim_argv_shortfall(&argv(&["./clean.sh"]), &[]).is_some());
        assert!(reclaim_argv_shortfall(&argv(&["bin/clean"]), &[]).is_some());
        assert!(reclaim_argv_shortfall(&argv(&["make", "a\0b"]), &[]).is_some());
        assert!(reclaim_argv_shortfall(&vec!["x".to_owned(); 65], &[]).is_some());
        assert!(reclaim_argv_shortfall(&argv(&["make", &"a".repeat(4096)]), &[]).is_some());
        assert!(reclaim_argv_shortfall(&argv(&["make"]), &argv(&["../out"])).is_some());
        let repos = vec![WorkspaceRepo {
            reclaim: Some(ReclaimPolicy {
                policy: Some(reclaim_policy::Policy::Command(reclaim_policy::Command {
                    argv: argv(&["./x"]),
                    ..reclaim_policy::Command::default()
                })),
            }),
            ..repo("atlas", "")
        }];
        assert!(validate_reclaim(&repos).is_err_and(|e| e.to_string().contains("atlas")));
        assert!(validate_definition_fields(&repos, &[], CheckoutMode::All).is_err());
    }

    #[test]
    fn a_branch_name_follows_git_and_leaves_other_agents_names_alone() {
        for good in ["", "feat/login", "release-1.4", "horde/agent-1"] {
            assert!(validate_branch(good, "agent-1").is_ok(), "{good}");
        }
        for bad in [
            "-x", "/x", "a..b", "a b", "a~b", "a^b", "a:b", "a?b", "a*b", "a[b", "a\\b", "a.lock", "a/",
            "a.", "HEAD", "horde/agent-2", "a//b", "a@{b",
        ] {
            assert!(validate_branch(bad, "agent-1").is_err(), "{bad}");
        }
        assert!(validate_branch(&"a".repeat(201), "agent-1").is_err());
    }

    fn definition(repos: &[&str]) -> WorkspaceDefinition {
        WorkspaceDefinition {
            name: "suite".to_owned(),
            repos: repos.iter().map(|name| repo(name, "")).collect(),
            ..WorkspaceDefinition::default()
        }
    }

    fn base(repo: &str, reference: &str) -> RepoBase {
        RepoBase {
            repo_name: repo.to_owned(),
            r#ref: reference.to_owned(),
            ..RepoBase::default()
        }
    }

    #[test]
    fn a_spawn_names_only_its_workspaces_repos_and_fills_a_bare_base() {
        let one = definition(&["atlas"]);
        let spec = AgentSpec {
            repo_bases: vec![base("", "main")],
            ..AgentSpec::default()
        };
        let filled = validated_spawn(spec, Some(&one), "a").expect("valid");
        assert_eq!(filled.repo_bases[0].repo_name, "atlas");

        let two = definition(&["atlas", "proto"]);
        let bare = AgentSpec {
            repo_bases: vec![base("", "main")],
            ..AgentSpec::default()
        };
        assert!(validated_spawn(bare, Some(&two), "a").is_err_and(|e| e.to_string().contains("REPO=REF")));
        for wrong in [
            AgentSpec {
                checkout_repos: vec!["ghost".to_owned()],
                ..AgentSpec::default()
            },
            AgentSpec {
                repo_bases: vec![base("ghost", "main")],
                ..AgentSpec::default()
            },
            AgentSpec {
                repo_bases: vec![base("atlas", "--upload-pack=x")],
                ..AgentSpec::default()
            },
            AgentSpec {
                branch: "horde/other".to_owned(),
                ..AgentSpec::default()
            },
            AgentSpec {
                branch: "feat".to_owned(),
                checkout_policy: CheckoutPolicy::Shared.into(),
                ..AgentSpec::default()
            },
        ] {
            assert!(validated_spawn(wrong.clone(), Some(&two), "a").is_err(), "{wrong:?}");
        }
        let lazy_without_workspace = AgentSpec {
            checkout_mode: CheckoutMode::Lazy.into(),
            ..AgentSpec::default()
        };
        assert!(validated_spawn(lazy_without_workspace.clone(), None, "a").is_err());
        assert!(needs_workspace_protocol(&lazy_without_workspace));
        assert!(validated_spawn(AgentSpec::default(), None, "a").is_ok());
        assert!(!needs_workspace_protocol(&AgentSpec::default()));
        let shared_plain = AgentSpec {
            checkout_policy: CheckoutPolicy::Shared.into(),
            ..AgentSpec::default()
        };
        assert!(validated_spawn(shared_plain, Some(&two), "a").is_ok());
    }
}
