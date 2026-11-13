//! Hard removal and the reachability checks behind it (ADR-0014).
//!
//! A checkout's worktree and branch go only when a precise check proves
//! nothing is lost; an archive pin goes only when its commit is reachable
//! from origin or the Relay. Every git error in a probe makes the answer
//! "not provable" — never "clean".

use std::path::Path;

use horde_proto::v1::workspace_cleanup::RetainedFile;

use super::git::{GitRun, commit_of, git, git_check, git_with, path_str};

/// The answer to "can this go without losing anything?".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proof {
    Lossless,
    /// Not provable, with the reason and any ignored files outside every
    /// reclaim output path that a forced removal would destroy.
    NotProvable {
        reason: String,
        unrecoverable: Vec<RetainedFile>,
    },
}

impl Proof {
    fn not(reason: impl Into<String>) -> Self {
        Self::NotProvable {
            reason: reason.into(),
            unrecoverable: Vec::new(),
        }
    }

    #[must_use]
    pub fn is_lossless(&self) -> bool {
        matches!(self, Self::Lossless)
    }

    /// The refusal reason, empty when lossless.
    #[must_use]
    pub fn reason(&self) -> &str {
        match self {
            Self::Lossless => "",
            Self::NotProvable { reason, .. } => reason,
        }
    }
}

/// One repo checkout the checks run against.
#[derive(Debug, Clone, Copy)]
pub struct Target<'a> {
    /// The Instance repo `I`.
    pub instance_repo: &'a Path,
    /// The agent's worktree (or clone) `W`.
    pub checkout: &'a Path,
    /// The agent's branch `B`.
    pub branch: &'a str,
    pub fresh_clone: bool,
    /// Repo-relative directories the repo's reclaim command owns: ignored
    /// files under them are build output, not loss (ADR-0014 resolution 2).
    pub output_paths: &'a [String],
}

/// `removable(W)`: the worktree and branch can go without losing anything.
pub async fn removable(target: Target<'_>) -> Proof {
    match checks(target).await {
        Ok(proof) => proof,
        Err(error) => Proof::not(format!("not provable: {error}")),
    }
}

async fn checks(target: Target<'_>) -> Result<Proof, String> {
    if let Some(reason) = dirty(target.checkout).await? {
        return Ok(Proof::not(reason));
    }
    if let Some(operation) = operation_in_progress(target.checkout).await? {
        return Ok(Proof::not(format!("a {operation} is in progress")));
    }
    if !target.fresh_clone && is_locked(target.instance_repo, target.checkout).await? {
        return Ok(Proof::not("the worktree is locked"));
    }
    let head = commit_of(target.checkout, "HEAD")
        .await
        .ok_or_else(|| "HEAD names no commit".to_owned())?;
    let branch_ref = format!("refs/heads/{}", target.branch);
    let branch_holder = if target.fresh_clone {
        target.checkout
    } else {
        target.instance_repo
    };
    if let Some(tip) = commit_of(branch_holder, &branch_ref).await
        && tip != head
        && !git_check(branch_holder, &["merge-base", "--is-ancestor", &tip, &head]).await?
    {
        return Ok(Proof::not(format!(
            "branch {} holds commits HEAD does not",
            target.branch
        )));
    }
    if target.fresh_clone {
        // Every local ref — branches, tags, the stash — minus HEAD's
        // history. Remote-tracking refs are the Instance's own branches,
        // which survive the clone's removal, so they are not this clone's.
        let extra = git(
            target.checkout,
            &[
                "rev-list",
                "--branches",
                "--tags",
                "--glob=refs/stash",
                "--not",
                &head,
            ],
        )
        .await?;
        if !extra.trim().is_empty() {
            return Ok(Proof::not(
                "the clone holds commits outside HEAD's history (another branch or a stash)",
            ));
        }
    }
    let unrecoverable = unrecoverable_ignored(target.checkout, target.output_paths).await?;
    if !unrecoverable.is_empty() {
        let count = unrecoverable.len();
        return Ok(Proof::NotProvable {
            reason: format!(
                "{count} ignored path(s) outside the reclaim command's outputs, e.g. {}",
                unrecoverable[0].path
            ),
            unrecoverable,
        });
    }
    Ok(Proof::Lossless)
}

/// Why the tree is dirty, or `None`: tracked changes and untracked
/// non-ignored files both count as loss.
async fn dirty(checkout: &Path) -> Result<Option<String>, String> {
    let status = git(
        checkout,
        &[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--ignore-submodules=none",
        ],
    )
    .await?;
    let entries: Vec<&str> = status
        .split('\0')
        .filter(|entry| !entry.is_empty())
        .collect();
    if entries.is_empty() {
        return Ok(None);
    }
    let untracked = entries
        .iter()
        .filter(|entry| entry.starts_with("??"))
        .count();
    let changed = entries.len() - untracked;
    Ok(Some(match (changed, untracked) {
        (0, untracked) => format!("untracked files: {untracked}"),
        (changed, 0) => format!("uncommitted changes: {changed}"),
        (changed, untracked) => {
            format!("uncommitted changes: {changed}; untracked files: {untracked}")
        }
    }))
}

/// The operation a git dir shows in progress, if any.
async fn operation_in_progress(checkout: &Path) -> Result<Option<&'static str>, String> {
    let git_dir = git(checkout, &["rev-parse", "--absolute-git-dir"]).await?;
    let git_dir = Path::new(git_dir.trim());
    for (marker, operation) in [
        ("MERGE_HEAD", "merge"),
        ("CHERRY_PICK_HEAD", "cherry-pick"),
        ("REVERT_HEAD", "revert"),
        ("BISECT_LOG", "bisect"),
        ("rebase-merge", "rebase"),
        ("rebase-apply", "rebase"),
    ] {
        if git_dir.join(marker).exists() {
            return Ok(Some(operation));
        }
    }
    Ok(None)
}

/// Whether the Instance lists the worktree as locked.
async fn is_locked(instance_repo: &Path, checkout: &Path) -> Result<bool, String> {
    let listing = git(instance_repo, &["worktree", "list", "--porcelain"]).await?;
    let wanted = std::fs::canonicalize(checkout).unwrap_or_else(|_| checkout.to_path_buf());
    Ok(listing.split("\n\n").any(|block| {
        let mut lines = block.lines();
        let path = lines
            .next()
            .and_then(|line| line.strip_prefix("worktree "))
            .map(|path| std::fs::canonicalize(path).unwrap_or_else(|_| path.into()));
        path.as_deref() == Some(wanted.as_path())
            && block
                .lines()
                .any(|line| line == "locked" || line.starts_with("locked "))
    }))
}

/// Ignored paths outside every output path, with their sizes. Ignored
/// directories are listed whole (`--directory`), so a `target/` is one entry.
async fn unrecoverable_ignored(
    checkout: &Path,
    output_paths: &[String],
) -> Result<Vec<RetainedFile>, String> {
    let listing = git(
        checkout,
        &[
            "ls-files",
            "--others",
            "--ignored",
            "--exclude-standard",
            "--directory",
            "-z",
        ],
    )
    .await?;
    let mut unrecoverable = Vec::new();
    for path in listing.split('\0').filter(|path| !path.is_empty()) {
        if is_under_any(path, output_paths) {
            continue;
        }
        let bytes = super::reclaim::dir_size(&checkout.join(path))
            .await
            .unwrap_or_default();
        unrecoverable.push(RetainedFile {
            path: path.trim_end_matches('/').to_owned(),
            bytes,
        });
    }
    Ok(unrecoverable)
}

/// Whether a repo-relative path lies at or under one of `roots`.
#[must_use]
pub fn is_under_any(path: &str, roots: &[String]) -> bool {
    let path = Path::new(path.trim_end_matches('/'));
    roots
        .iter()
        .map(|root| Path::new(root.trim_end_matches('/')))
        .filter(|root| !root.as_os_str().is_empty())
        .any(|root| path.starts_with(root))
}

/// What one repo's hard removal did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Removed {
    pub pinned_commit_sha: String,
}

/// Removes one repo checkout: pin HEAD under `refs/horde/archived/<agent>`,
/// re-check, remove the worktree (or clone), delete the branch if the
/// Worker created it. Every step is idempotent. With `force`, a dirty tree
/// is removed anyway — its committed HEAD is still pinned first.
///
/// # Errors
///
/// Any step failing, in words; nothing after the failed step runs.
pub async fn remove_repo(
    target: Target<'_>,
    agent_id: &str,
    created_branch: bool,
    force: bool,
) -> Result<Removed, String> {
    if !target.checkout.exists() {
        delete_branch(target, created_branch).await?;
        return Ok(Removed {
            pinned_commit_sha: commit_of(target.instance_repo, &pin_ref(agent_id))
                .await
                .unwrap_or_default(),
        });
    }
    let head = commit_of(target.checkout, "HEAD")
        .await
        .ok_or_else(|| "HEAD names no commit; nothing to pin".to_owned())?;
    let pin = format!("+{head}:{}", pin_ref(agent_id));
    git(
        target.instance_repo,
        &["fetch", "--no-tags", "--", &path_str(target.checkout), &pin],
    )
    .await?;
    if !force {
        // Closes the window between the check and the pin.
        if let Some(reason) = dirty(target.checkout).await? {
            return Err(format!("changed after the check: {reason}"));
        }
        if let Some(operation) = operation_in_progress(target.checkout).await? {
            return Err(format!("a {operation} started after the check"));
        }
    }
    if target.fresh_clone {
        tokio::fs::remove_dir_all(target.checkout)
            .await
            .map_err(|error| format!("could not remove {}: {error}", target.checkout.display()))?;
    } else {
        let destination = path_str(target.checkout);
        let mut arguments = vec!["worktree", "remove"];
        if force {
            // Twice: also through a lock.
            arguments.extend(["--force", "--force"]);
        }
        arguments.push(&destination);
        git(target.instance_repo, &arguments).await?;
    }
    delete_branch(target, created_branch).await?;
    Ok(Removed {
        pinned_commit_sha: head,
    })
}

/// Deletes the agent's branch in the Instance repo when the Worker created
/// it; an adopted branch existed before the agent and stays.
async fn delete_branch(target: Target<'_>, created_branch: bool) -> Result<(), String> {
    if !created_branch || target.fresh_clone {
        return Ok(());
    }
    let branch_ref = format!("refs/heads/{}", target.branch);
    if commit_of(target.instance_repo, &branch_ref).await.is_none() {
        return Ok(());
    }
    git(target.instance_repo, &["branch", "-D", "--", target.branch])
        .await
        .map(|_| ())
}

/// The archive pin of one agent in an Instance repo.
#[must_use]
pub fn pin_ref(agent_id: &str) -> String {
    format!("refs/horde/archived/{agent_id}")
}

/// `unpinnable(P)`'s inputs: what the pin's commit may be reachable from.
#[derive(Debug, Clone, Default)]
pub struct Keepers {
    /// Whether this pass's fetch from origin succeeded; without it nothing
    /// about origin is provable.
    pub origin_fetched: bool,
    /// Share-ref commits the Relay listed for this repo (ADR-0014
    /// resolution 3: a commit held on the Relay counts as pushed).
    pub relay_shares: Vec<String>,
}

/// Fetches origin into the Instance repo for a reachability pass, and lists
/// the Relay's share refs when a Relay URL is given.
pub async fn keepers(
    instance_repo: &Path,
    relay_repo_url: Option<&str>,
    timeout: std::time::Duration,
) -> Keepers {
    let origin_fetched = git_with(
        instance_repo,
        &["fetch", "--prune", "--no-tags", "origin"],
        GitRun::remote(timeout),
    )
    .await
    .is_ok();
    let mut relay_shares = Vec::new();
    if let Some(url) = relay_repo_url
        && let Ok(listing) = git_with(
            instance_repo,
            &["ls-remote", "--", url, "refs/horde/share/*"],
            GitRun::remote(timeout),
        )
        .await
    {
        relay_shares = listing
            .lines()
            .filter_map(|line| line.split_whitespace().next())
            .map(str::to_owned)
            .collect();
    }
    Keepers {
        origin_fetched,
        relay_shares,
    }
}

/// `unpinnable(P)`: whether the commit `reference` names is reachable from
/// origin (pushed or merged with history) or from a Relay share ref.
pub async fn reachable(instance_repo: &Path, reference: &str, keepers: &Keepers) -> Proof {
    let Some(commit) = commit_of(instance_repo, reference).await else {
        return Proof::not(format!("{reference} names no commit"));
    };
    if keepers.origin_fetched {
        match git(
            instance_repo,
            &[
                "for-each-ref",
                "--contains",
                &commit,
                "--format=%(refname)",
                "refs/remotes/origin",
            ],
        )
        .await
        {
            Ok(containing) if !containing.trim().is_empty() => return Proof::Lossless,
            Ok(_) => {}
            Err(error) => return Proof::not(format!("not provable: {error}")),
        }
    }
    for share in &keepers.relay_shares {
        if commit_of(instance_repo, share).await.is_some()
            && git_check(
                instance_repo,
                &["merge-base", "--is-ancestor", &commit, share],
            )
            .await
            .unwrap_or(false)
        {
            return Proof::Lossless;
        }
    }
    if keepers.origin_fetched {
        Proof::not(format!(
            "{commit} is on no origin branch and no Relay share"
        ))
    } else {
        Proof::not("not provable: the fetch from origin failed in this pass")
    }
}

/// Every ref under `refs/heads/` and `refs/horde/` in an Instance repo.
pub async fn local_refs(instance_repo: &Path) -> Result<Vec<String>, String> {
    let listing = git(
        instance_repo,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/heads",
            "refs/horde",
        ],
    )
    .await?;
    Ok(listing.lines().map(str::to_owned).collect())
}

/// Every archive pin in an Instance repo.
pub async fn pins(instance_repo: &Path) -> Result<Vec<String>, String> {
    let listing = git(
        instance_repo,
        &["for-each-ref", "--format=%(refname)", "refs/horde/archived"],
    )
    .await?;
    Ok(listing.lines().map(str::to_owned).collect())
}

/// Drops one ref.
pub async fn delete_ref(instance_repo: &Path, reference: &str) -> Result<(), String> {
    git(instance_repo, &["update-ref", "-d", reference])
        .await
        .map(|_| ())
}

/// Whether the Instance's own working tree is clean (rule 1, applied to the
/// Instance before its removal).
pub async fn instance_tree_clean(instance_repo: &Path) -> Result<Option<String>, String> {
    dirty(instance_repo).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_path_is_under_a_root_only_by_whole_components() {
        let roots = vec!["target".to_owned(), "build/out/".to_owned()];
        assert!(is_under_any("target/", &roots));
        assert!(is_under_any("target/debug/x", &roots));
        assert!(is_under_any("build/out/a.o", &roots));
        assert!(
            !is_under_any("targets/", &roots),
            "a prefix is not a parent"
        );
        assert!(!is_under_any(".env", &roots));
        assert!(
            !is_under_any("build/", &roots),
            "a parent of a root is not under it"
        );
        assert!(
            !is_under_any("anything", &[String::new()]),
            "an empty root covers nothing"
        );
    }

    #[test]
    fn a_proof_says_whether_and_why() {
        assert!(Proof::Lossless.is_lossless());
        assert_eq!(Proof::Lossless.reason(), "");
        let refused = Proof::not("untracked files: 1");
        assert!(!refused.is_lossless());
        assert_eq!(refused.reason(), "untracked files: 1");
    }

    #[test]
    fn the_pin_lives_under_the_archived_namespace() {
        assert_eq!(pin_ref("agent-1"), "refs/horde/archived/agent-1");
    }
}
