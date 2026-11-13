//! Freshness: where a new checkout starts (ADR-0014).
//!
//! A new agent starts from the latest `origin/<default_branch>`, found by a
//! throttled, single-flight fetch into the Instance repo. Nothing here ever
//! runs `pull`, `reset` or `checkout` in the Instance's own working tree:
//! that tree belongs to `shared`-policy agents. A default base may quietly
//! fall back (cached remote-tracking ref, then the Instance HEAD) with the
//! fallback recorded; a base a human or parent asked for may not.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

// Tokio's clock rather than std's: identical at run time, and pausable in
// tests, which is how the throttle's boundary is observed.
use tokio::time::Instant;

use async_trait::async_trait;
use horde_proto::v1::checkout_prepared::BaseResolution;
use horde_proto::v1::{RepoBase, RepoBaseSource, WorkspaceRepo};

use super::git::{GitRun, commit_of, git, git_check, git_with, is_full_sha, is_option_shaped};
use super::ledger::now_ms;

/// The freshness knobs a spawn's resolved policy carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FetchPolicy {
    /// How long a successful fetch stays fresh. Default 60 s.
    pub throttle: Duration,
    /// The ceiling on one fetch. Default 30 s.
    pub timeout: Duration,
}

impl Default for FetchPolicy {
    fn default() -> Self {
        Self {
            throttle: Duration::from_secs(60),
            timeout: Duration::from_secs(30),
        }
    }
}

/// Where one repo of a new checkout starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBase {
    pub sha: String,
    pub resolution: BaseResolution,
    pub requested_ref: String,
    /// The last successful fetch behind the base, in Unix milliseconds.
    pub fetched_at_ms: Option<i64>,
    /// Non-empty is the warning: why the base is not fresh.
    pub stale_reason: String,
}

/// Makes a commit present in an Instance repo from elsewhere — a peer
/// checkout on this Worker or the Relay. What a `PARENT` base missing after
/// the fetch is resolved through (`ShareBroker::obtain_into`).
#[async_trait]
pub trait CommitSource: Send + Sync {
    /// # Errors
    ///
    /// Why the commit could not be obtained, in words.
    async fn obtain_into(
        &self,
        instance_repo: &Path,
        repo_name: &str,
        sha: &str,
    ) -> Result<(), String>;
}

/// A source that has nothing: every obtain fails. For checkouts with no
/// broker behind them.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSource;

#[async_trait]
impl CommitSource for NoSource {
    async fn obtain_into(&self, _: &Path, repo_name: &str, sha: &str) -> Result<(), String> {
        Err(format!(
            "commit {sha} of repo {repo_name} is not on this worker and nothing else can supply it"
        ))
    }
}

/// The outcome of the last fetch attempt of one Instance repo.
#[derive(Debug, Clone, Default)]
struct FetchState {
    /// When the last attempt finished, and how.
    last_attempt: Option<(Instant, Result<(), String>)>,
    /// The wall time of the last success.
    last_success_ms: Option<i64>,
}

/// What a throttled fetch tells its callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FetchOutcome {
    /// `None` on success; the git error otherwise.
    pub(crate) error: Option<String>,
    pub(crate) last_success_ms: Option<i64>,
}

/// Throttles and single-flights fetches per Instance repo. In memory only:
/// a restart costs one extra fetch.
#[derive(Debug, Default)]
pub struct FetchThrottle {
    repos: Mutex<BTreeMap<PathBuf, Arc<tokio::sync::Mutex<FetchState>>>>,
    /// How many fetches actually ran — what the throttle test observes
    /// besides the remote's own count.
    fetches: std::sync::atomic::AtomicU64,
}

impl FetchThrottle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// How many fetches this throttle has run.
    #[must_use]
    pub fn fetches_run(&self) -> u64 {
        self.fetches.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn state_of(&self, repo: &Path) -> Arc<tokio::sync::Mutex<FetchState>> {
        let mut repos = self.repos.lock().unwrap_or_else(PoisonError::into_inner);
        Arc::clone(repos.entry(repo.to_path_buf()).or_default())
    }

    /// Fetches `repo` from origin unless an attempt finished within the
    /// throttle, in which case its outcome is reused. Callers waiting on the
    /// same repo share one fetch: the lock is held across it.
    pub(crate) async fn fetch(&self, repo: &Path, policy: FetchPolicy) -> FetchOutcome {
        let state = self.state_of(repo);
        let mut state = state.lock().await;
        if let Some((at, result)) = &state.last_attempt
            && at.elapsed() < policy.throttle
        {
            return FetchOutcome {
                error: result.clone().err(),
                last_success_ms: state.last_success_ms,
            };
        }
        self.fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let result = git_with(
            repo,
            &["fetch", "--prune", "--no-tags", "origin"],
            GitRun::remote(policy.timeout),
        )
        .await
        .map(|_| ());
        if result.is_ok() {
            state.last_success_ms = Some(now_ms());
        }
        state.last_attempt = Some((Instant::now(), result.clone()));
        FetchOutcome {
            error: result.err(),
            last_success_ms: state.last_success_ms,
        }
    }
}

/// Resolves where one repo of a new checkout starts.
///
/// `requested` is a `PARENT` or `REQUESTED` base; `branch` is the agent's
/// branch, which continues from `origin/<branch>` when that exists and no
/// base was named for this repo.
///
/// # Errors
///
/// A named base that cannot be resolved after one targeted fetch, or a
/// parent's commit no route can supply. The default base never errors.
pub(crate) async fn prepare_base(
    instance_repo: &Path,
    repo: &WorkspaceRepo,
    requested: Option<&RepoBase>,
    branch: &str,
    throttle: &FetchThrottle,
    policy: FetchPolicy,
    source: &dyn CommitSource,
) -> Result<ResolvedBase, String> {
    if let Some(requested) = requested {
        return resolve_requested(instance_repo, repo, requested, throttle, policy, source).await;
    }
    let fetched = throttle.fetch(instance_repo, policy).await;
    let continued = format!("refs/remotes/origin/{branch}");
    if !is_option_shaped(branch)
        && let Some(sha) = commit_of(instance_repo, &continued).await
    {
        // `--branch` on a branch origin already has continues it
        // (ADR-0014 resolution 5).
        return Ok(ResolvedBase {
            sha,
            resolution: BaseResolution::Requested,
            requested_ref: format!("origin/{branch}"),
            fetched_at_ms: fetched.last_success_ms,
            stale_reason: fetched.error.unwrap_or_default(),
        });
    }
    Ok(default_base(instance_repo, repo, fetched).await)
}

/// The default base: `origin/<default_branch>` (or `origin/HEAD`), else the
/// Instance HEAD, with any fallback recorded.
async fn default_base(
    instance_repo: &Path,
    repo: &WorkspaceRepo,
    fetched: FetchOutcome,
) -> ResolvedBase {
    let tracking = if repo.default_branch.is_empty() || is_option_shaped(&repo.default_branch) {
        "refs/remotes/origin/HEAD".to_owned()
    } else {
        format!("refs/remotes/origin/{}", repo.default_branch)
    };
    if let Some(sha) = commit_of(instance_repo, &tracking).await {
        let (resolution, stale_reason) = match fetched.error {
            None => (BaseResolution::Fresh, String::new()),
            Some(error) => (
                BaseResolution::Cached,
                format!("fetch failed, starting from the cached {tracking}: {error}"),
            ),
        };
        return ResolvedBase {
            sha,
            resolution,
            requested_ref: String::new(),
            fetched_at_ms: fetched.last_success_ms,
            stale_reason,
        };
    }
    let head = commit_of(instance_repo, "HEAD").await.unwrap_or_default();
    let mut stale_reason = format!("no {tracking}; starting from the instance HEAD");
    if let Some(error) = fetched.error {
        stale_reason.push_str(&format!(" (fetch failed: {error})"));
    }
    ResolvedBase {
        sha: head,
        resolution: BaseResolution::InstanceHead,
        requested_ref: String::new(),
        fetched_at_ms: fetched.last_success_ms,
        stale_reason,
    }
}

/// A named base: a full SHA already present is used as-is; otherwise the
/// throttled fetch, the resolution order, one targeted fetch, and — for a
/// parent's commit — the commit source.
async fn resolve_requested(
    instance_repo: &Path,
    repo: &WorkspaceRepo,
    requested: &RepoBase,
    throttle: &FetchThrottle,
    policy: FetchPolicy,
    source: &dyn CommitSource,
) -> Result<ResolvedBase, String> {
    let reference = requested.r#ref.trim();
    if reference.is_empty() || is_option_shaped(reference) {
        return Err(format!(
            "repo {}: base {reference:?} is not a commit, branch or tag",
            repo.name
        ));
    }
    let parent = requested.source == i32::from(RepoBaseSource::Parent);
    let resolution = if parent {
        BaseResolution::Parent
    } else {
        BaseResolution::Requested
    };
    let resolved = |sha: String, fetched_at_ms: Option<i64>| ResolvedBase {
        sha,
        resolution,
        requested_ref: reference.to_owned(),
        fetched_at_ms,
        stale_reason: String::new(),
    };
    if is_full_sha(reference) && commit_of(instance_repo, reference).await.is_some() {
        return Ok(resolved(reference.to_owned(), None));
    }
    let fetched = throttle.fetch(instance_repo, policy).await;
    if let Some(sha) = lookup(instance_repo, reference).await {
        return Ok(resolved(sha, fetched.last_success_ms));
    }
    // One unthrottled, targeted fetch, then once more.
    let targeted = git_with(
        instance_repo,
        &["fetch", "--no-tags", "origin", "--", reference],
        GitRun::remote(policy.timeout),
    )
    .await;
    if targeted.is_ok()
        && let Some(sha) = commit_of(instance_repo, "FETCH_HEAD").await
    {
        return Ok(resolved(sha, Some(now_ms())));
    }
    if parent && is_full_sha(reference) {
        source
            .obtain_into(instance_repo, &repo.name, reference)
            .await
            .map_err(|error| {
                format!(
                    "repo {}: the parent's commit {reference} is unavailable: {error}",
                    repo.name
                )
            })?;
        return Ok(resolved(reference.to_owned(), fetched.last_success_ms));
    }
    Err(format!(
        "repo {}: base {reference:?} names no commit, origin branch or tag{}",
        repo.name,
        targeted
            .err()
            .map(|error| format!(" ({error})"))
            .unwrap_or_default()
    ))
}

/// The resolution order for a named ref: a commit (full or abbreviated SHA),
/// then an origin branch, then a tag. Only a hex-shaped ref is tried as a
/// commit: `rev-parse main` would find the Instance's own `main`, frozen at
/// its clone point, ahead of `origin/main`.
async fn lookup(instance_repo: &Path, reference: &str) -> Option<String> {
    let mut candidates = Vec::with_capacity(3);
    if is_abbreviated_sha(reference) {
        candidates.push(reference.to_owned());
    }
    candidates.push(format!("refs/remotes/origin/{reference}"));
    candidates.push(format!("refs/tags/{reference}"));
    for candidate in candidates {
        if let Some(sha) = commit_of(instance_repo, &candidate).await {
            return Some(sha);
        }
    }
    None
}

/// At least four hex digits: what git accepts as an abbreviated object name.
fn is_abbreviated_sha(value: &str) -> bool {
    value.len() >= 4 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// How the agent's branch was put in place in one Instance repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BranchPlan {
    /// No local branch of that name: create it at the base.
    Create,
    /// A local branch at or behind the base, checked out nowhere: moved to
    /// the base and adopted.
    Adopt,
}

/// Decides how `branch` is put in place at `base` in `instance_repo`.
///
/// # Errors
///
/// The branch is checked out in another worktree (naming its holder), or
/// holds commits the base does not.
pub(crate) async fn plan_branch(
    instance_repo: &Path,
    branch: &str,
    base: &str,
) -> Result<BranchPlan, String> {
    let local = format!("refs/heads/{branch}");
    let Some(tip) = commit_of(instance_repo, &local).await else {
        return Ok(BranchPlan::Create);
    };
    if let Some(holder) = worktree_holding(instance_repo, &local).await? {
        return Err(format!(
            "branch {branch} is checked out by {holder}; remove that checkout first \
             (`horde workspace gc --hard --agent <id> [--force]`)"
        ));
    }
    if tip == base || git_check(instance_repo, &["merge-base", "--is-ancestor", &tip, base]).await?
    {
        return Ok(BranchPlan::Adopt);
    }
    Err(format!(
        "branch {branch} exists with commits its base {base} lacks; refusing to reset or hide them"
    ))
}

/// Who holds `reference` checked out, if anyone: the agent whose checkout
/// path names it, or "a worktree Horde did not create".
async fn worktree_holding(instance_repo: &Path, reference: &str) -> Result<Option<String>, String> {
    let listing = git(instance_repo, &["worktree", "list", "--porcelain"]).await?;
    for block in listing.split("\n\n") {
        let mut path = None;
        let mut holds = false;
        for line in block.lines() {
            if let Some(found) = line.strip_prefix("worktree ") {
                path = Some(found.to_owned());
            }
            if line.strip_prefix("branch ") == Some(reference) {
                holds = true;
            }
        }
        if holds {
            return Ok(Some(holder_name(path.as_deref().unwrap_or_default())));
        }
    }
    Ok(None)
}

/// The agent a worktree path belongs to, by the `.horde-agents/<agent>/…`
/// shape `compose` gives every checkout.
pub(crate) fn holder_name(path: &str) -> String {
    let mut components = Path::new(path).components().map(|c| c.as_os_str());
    while let Some(component) = components.next() {
        if component == super::paths::AGENTS_DIR
            && let Some(agent) = components.next().and_then(|agent| agent.to_str())
        {
            return format!("agent {agent}");
        }
    }
    "a worktree Horde did not create".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::test_git::{commit, git as run_git};

    #[test]
    fn a_holder_is_named_by_its_checkout_path() {
        assert_eq!(
            holder_name("/ws/def-1/.horde-agents/agent-7/atlas"),
            "agent agent-7"
        );
        assert_eq!(
            holder_name("/elsewhere/atlas"),
            "a worktree Horde did not create"
        );
        assert_eq!(holder_name(""), "a worktree Horde did not create");
    }

    #[test]
    fn only_four_or_more_hex_digits_abbreviate_a_commit() {
        assert!(is_abbreviated_sha("9c1e"));
        assert!(is_abbreviated_sha(&"a".repeat(40)));
        assert!(!is_abbreviated_sha("9c1"));
        assert!(!is_abbreviated_sha("main"));
        assert!(!is_abbreviated_sha("release/1.4"));
    }

    #[test]
    fn the_default_fetch_policy_is_a_minute_and_half_a_minute() {
        let policy = FetchPolicy::default();
        assert_eq!(policy.throttle, Duration::from_secs(60));
        assert_eq!(policy.timeout, Duration::from_secs(30));
    }

    /// An origin with one commit on `main`, and an Instance repo cloned
    /// from it: `(origin, instance, the seed commit)`.
    fn origin_and_instance(dir: &Path) -> (PathBuf, PathBuf, String) {
        let origin = dir.join("origin");
        std::fs::create_dir_all(&origin).expect("origin");
        run_git(&origin, &["init", "-q", "-b", "main"]);
        let seed = commit(&origin, "seed");
        let instance = dir.join("instance");
        run_git(
            dir,
            &[
                "clone",
                "-q",
                "--no-local",
                "--",
                &origin.display().to_string(),
                &instance.display().to_string(),
            ],
        );
        (origin, instance, seed)
    }

    fn repo(default_branch: &str) -> WorkspaceRepo {
        WorkspaceRepo {
            name: "atlas".to_owned(),
            default_branch: default_branch.to_owned(),
            ..WorkspaceRepo::default()
        }
    }

    fn requested(reference: &str, source: RepoBaseSource) -> RepoBase {
        RepoBase {
            repo_name: "atlas".to_owned(),
            r#ref: reference.to_owned(),
            source: source.into(),
        }
    }

    /// A throttle counts the fetches it actually runs, and one inside the
    /// throttle window reuses the last outcome instead of fetching.
    #[tokio::test]
    async fn a_fetch_inside_the_throttle_is_reused_and_not_counted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let throttle = FetchThrottle::new();
        assert_eq!(throttle.fetches_run(), 0);
        let policy = FetchPolicy::default();
        let first = throttle.fetch(dir.path(), policy).await;
        let second = throttle.fetch(dir.path(), policy).await;
        assert_eq!(throttle.fetches_run(), 1);
        assert!(first.error.is_some(), "not a repository");
        assert_eq!(first, second, "the outcome is reused");
    }

    /// The window is half-open: a zero throttle never reuses an attempt,
    /// even one that finished at this very instant.
    #[tokio::test(start_paused = true)]
    async fn a_zero_throttle_fetches_again_at_the_same_instant() {
        let dir = tempfile::tempdir().expect("tempdir");
        let throttle = FetchThrottle::new();
        let policy = FetchPolicy {
            throttle: Duration::ZERO,
            timeout: Duration::from_secs(30),
        };
        throttle.fetch(dir.path(), policy).await;
        throttle.fetch(dir.path(), policy).await;
        throttle.fetch(dir.path(), policy).await;
        assert_eq!(throttle.fetches_run(), 3);
    }

    /// A repo with no default branch named starts from `origin/HEAD`, fresh.
    #[tokio::test]
    async fn no_default_branch_starts_from_origin_head() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (origin, instance, _) = origin_and_instance(dir.path());
        let latest = commit(&origin, "latest");
        let resolved = prepare_base(
            &instance,
            &repo(""),
            None,
            "horde/agent-1",
            &FetchThrottle::new(),
            FetchPolicy::default(),
            &NoSource,
        )
        .await
        .expect("the default base never errors");
        assert_eq!(resolved.resolution, BaseResolution::Fresh);
        assert_eq!(resolved.sha, latest);
        assert_eq!(resolved.stale_reason, "");
    }

    /// An empty or option-shaped base is refused before git sees it.
    #[tokio::test]
    async fn an_empty_or_option_shaped_base_is_refused_up_front() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, instance, _) = origin_and_instance(dir.path());
        let throttle = FetchThrottle::new();
        for reference in ["", "--upload-pack=touch /tmp/x"] {
            let error = prepare_base(
                &instance,
                &repo("main"),
                Some(&requested(reference, RepoBaseSource::Requested)),
                "horde/agent-1",
                &throttle,
                FetchPolicy::default(),
                &NoSource,
            )
            .await
            .expect_err("refused");
            assert!(
                error.contains("is not a commit, branch or tag"),
                "{reference:?}: {error}"
            );
        }
        assert_eq!(throttle.fetches_run(), 0, "nothing was fetched");
    }

    /// Only a parent's commit is sought through the commit source: a human's
    /// full SHA that names nothing is simply unknown.
    #[tokio::test]
    async fn only_a_parents_missing_commit_goes_to_the_source() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, instance, _) = origin_and_instance(dir.path());
        let missing = "0123456789abcdef".repeat(3)[..40].to_owned();
        let asked = |source: RepoBaseSource| {
            let instance = instance.clone();
            let missing = missing.clone();
            async move {
                prepare_base(
                    &instance,
                    &repo("main"),
                    Some(&requested(&missing, source)),
                    "horde/agent-1",
                    &FetchThrottle::new(),
                    FetchPolicy::default(),
                    &NoSource,
                )
                .await
                .expect_err("nothing has that commit")
            }
        };
        let human = asked(RepoBaseSource::Requested).await;
        assert!(human.contains("names no commit"), "{human}");
        assert!(!human.contains("parent's commit"), "{human}");
        let parent = asked(RepoBaseSource::Parent).await;
        assert!(parent.contains("the parent's commit"), "{parent}");
        assert!(parent.contains("is not on this worker"), "{parent}");
    }

    /// A branch at or behind the base is adopted; one holding commits the
    /// base lacks is refused rather than reset.
    #[tokio::test]
    async fn a_branch_behind_its_base_is_adopted_and_one_ahead_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (_, instance, seed) = origin_and_instance(dir.path());
        let base = commit(&instance, "base");
        run_git(&instance, &["branch", "behind", &seed]);
        run_git(&instance, &["branch", "same", &base]);
        run_git(&instance, &["checkout", "-q", "-b", "ahead", &seed]);
        commit(&instance, "diverged");
        run_git(&instance, &["checkout", "-q", "main"]);

        assert_eq!(
            plan_branch(&instance, "fresh", &base).await,
            Ok(BranchPlan::Create)
        );
        assert_eq!(
            plan_branch(&instance, "behind", &base).await,
            Ok(BranchPlan::Adopt)
        );
        assert_eq!(
            plan_branch(&instance, "same", &base).await,
            Ok(BranchPlan::Adopt)
        );
        let refused = plan_branch(&instance, "ahead", &base)
            .await
            .expect_err("its commits would be lost");
        assert!(refused.contains("commits its base"), "{refused}");
        let held = plan_branch(&instance, "main", &seed)
            .await
            .expect_err("main is checked out");
        assert!(held.contains("checked out by"), "{held}");
    }

    #[tokio::test]
    async fn the_empty_source_supplies_nothing() {
        let error = NoSource
            .obtain_into(Path::new("/x"), "atlas", "abc")
            .await
            .expect_err("nothing to obtain from");
        assert!(error.contains("abc") && error.contains("atlas"), "{error}");
    }
}
