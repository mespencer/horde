//! Workspace Instances on this Worker's disk.
//!
//! The Server owns Definitions; this module owns their materialization —
//! "Instances are never self-initiated: they exist because the Server asked"
//! (`MaterializeWorkspaceCommand`) — and the per-agent Checkout Policies from
//! DESIGN §7: `shared` works in the Instance itself, `worktree` (the default)
//! composes one git worktree per repo under an agent-private root, and
//! `fresh-clone` clones every repo for total isolation.
//!
//! # The state file is the truth
//!
//! Each Instance root carries the full `WorkspaceDefinition` it was
//! materialized from, encoded protobuf, at [`STATE_FILE`]. That file is what
//! DESIGN §7 calls the worker-local truth: the Server's view is a cache
//! corrected by reports built from it, and checkout reads it rather than
//! needing the Server to repeat the definition on every spawn.
//!
//! # Failure is a report, not a crash
//!
//! Materialization never errors: a repo that fails to clone produces a
//! `DEGRADED` report whose detail says which repo and why, because the Server
//! and the humans behind it can act on that — while a Worker that dies on a
//! typo'd remote URL takes every other agent down with it. Checkout *does*
//! error, into the spawn-failed lifecycle path: an agent cannot run in a
//! checkout that does not exist.

use std::path::{Path, PathBuf};

use horde_proto::prost::Message;
use horde_proto::v1::{
    CheckoutPolicy, RepoInstanceReport, WorkerId, WorkspaceDefinition, WorkspaceInstanceReport,
    WorkspaceInstanceState, WorkspaceRepo,
};

use crate::error::WorkerError;

/// Where an Instance records the Definition it was materialized from.
pub const STATE_FILE: &str = ".horde-instance.pb";

/// Where per-agent checkouts live inside an Instance root.
const AGENTS_DIR: &str = ".horde-agents";

/// A ceiling on any single git invocation. Generous because a first clone of
/// a real remote is slow; finite because a hung git inside the command loop
/// would otherwise stall every later command forever.
const GIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(600);

/// Every clone goes through git's transport, even from a local path.
///
/// A local-path clone's default is to walk the source's `objects/` and
/// hardlink or copy it entry by entry, which is only safe while nothing else
/// touches that directory. Both our sources are live: a remote URL may be a
/// user's working repo, and an Instance's object store is shared by every
/// agent worktree committing into it. Each `git commit` spawns a detached
/// `git maintenance run --auto`, which creates and deletes
/// `objects/maintenance.lock` (and may repack) behind the copier's back —
/// the clone then dies with "failed to copy file … No such file or
/// directory". Through the transport, `upload-pack` serves the objects
/// reachable from the source's refs as a pack and never lists the directory.
///
/// The price: no hardlinks, so each clone costs the repo's packed size on
/// disk and a `pack-objects` pass (mostly reusing existing deltas) instead of
/// near-free links; unreachable objects are not carried. For a URL remote
/// the flag changes nothing.
const NO_LOCAL: &str = "--no-local";

/// Materializes Instances and applies Checkout Policies under one root.
#[derive(Debug, Clone)]
pub struct WorkspaceManager {
    worker_id: String,
    root: PathBuf,
}

impl WorkspaceManager {
    /// A manager materializing under `root`, reporting as `worker_id`.
    pub fn new(worker_id: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            worker_id: worker_id.into(),
            root: root.into(),
        }
    }

    /// One Instance's root directory.
    fn instance_root(&self, definition_id: &str) -> PathBuf {
        self.root.join(definition_id)
    }

    /// Creates or repairs one Instance, per the Server's command.
    ///
    /// Always returns a report: `READY` when every repo materialized, and
    /// `DEGRADED` naming what failed otherwise.
    pub async fn materialize(&self, definition: &WorkspaceDefinition) -> WorkspaceInstanceReport {
        let definition_id = definition
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !is_path_safe(&definition_id) {
            return self.report(
                &definition_id,
                String::new(),
                Vec::new(),
                vec![format!(
                    "definition id {definition_id:?} is not a directory name"
                )],
            );
        }

        let instance_root = self.instance_root(&definition_id);
        let mut failures = Vec::new();
        let mut repos = Vec::new();

        if let Err(error) = tokio::fs::create_dir_all(&instance_root).await {
            failures.push(format!(
                "could not create {}: {error}",
                instance_root.display()
            ));
            return self.report(
                &definition_id,
                instance_root.display().to_string(),
                repos,
                failures,
            );
        }

        for repo in &definition.repos {
            match materialize_repo(&instance_root, repo).await {
                Ok(report) => repos.push(report),
                Err(failure) => failures.push(format!("repo {}: {failure}", repo.name)),
            }
        }

        // The worker-local truth, written after the work so a crash mid-clone
        // does not leave a state file describing an instance that is not there.
        if let Err(error) =
            tokio::fs::write(instance_root.join(STATE_FILE), definition.encode_to_vec()).await
        {
            failures.push(format!("could not write the instance state file: {error}"));
        }

        self.report(
            &definition_id,
            instance_root.display().to_string(),
            repos,
            failures,
        )
    }

    /// The working directory an agent occupies, per its Checkout Policy.
    ///
    /// `start_points` maps repo names to the commit each new checkout should
    /// begin at — a spawning parent's committed HEAD (DESIGN §5: "children
    /// start from the parent's committed HEAD"), observed by the caller. A
    /// repo absent from the map starts where the Instance's HEAD is, which
    /// is also the whole map's meaning when empty: a root agent has no
    /// parent to inherit from.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when the Instance has not been materialized
    /// or a git operation fails — the spawn's failure, not the Worker's.
    pub async fn checkout(
        &self,
        definition_id: &str,
        agent_id: &str,
        policy: CheckoutPolicy,
        start_points: &std::collections::BTreeMap<String, String>,
    ) -> Result<PathBuf, WorkerError> {
        if !is_path_safe(definition_id) || !is_path_safe(agent_id) {
            return Err(WorkerError::Workspace(format!(
                "{definition_id:?} / {agent_id:?} are not directory names"
            )));
        }
        let instance_root = self.instance_root(definition_id);
        let definition = required_state(&instance_root, definition_id).await?;

        match policy {
            CheckoutPolicy::Shared => Ok(instance_root),
            // The default for mutating agents (DESIGN §7): unspecified is
            // worktree, not an error.
            CheckoutPolicy::Unspecified | CheckoutPolicy::Worktree => {
                self.compose(
                    &instance_root,
                    &definition,
                    agent_id,
                    start_points,
                    worktree_repo,
                )
                .await
            }
            CheckoutPolicy::FreshClone => {
                self.compose(
                    &instance_root,
                    &definition,
                    agent_id,
                    start_points,
                    clone_repo,
                )
                .await
            }
        }
    }

    /// Builds the agent-private root and populates one checkout per repo.
    async fn compose<F, Fut>(
        &self,
        instance_root: &Path,
        definition: &WorkspaceDefinition,
        agent_id: &str,
        start_points: &std::collections::BTreeMap<String, String>,
        checkout_repo: F,
    ) -> Result<PathBuf, WorkerError>
    where
        F: Fn(PathBuf, PathBuf, String, Option<String>) -> Fut,
        Fut: Future<Output = Result<(), WorkerError>>,
    {
        let agent_root = instance_root.join(AGENTS_DIR).join(agent_id);
        for repo in &definition.repos {
            let layout = layout_of(repo).map_err(WorkerError::Workspace)?;
            let source = instance_root.join(layout);
            let destination = agent_root.join(layout);
            if destination.exists() {
                // A redelivered spawn reuses its checkout; at-least-once
                // delivery must not stack a second worktree beside the first.
                continue;
            }
            checkout_repo(
                source,
                destination,
                format!("horde/{agent_id}"),
                start_points.get(&repo.name).cloned(),
            )
            .await?;
        }
        Ok(agent_root)
    }

    /// The repos a checkout of this Instance composes: `(repo name, relative
    /// directory under the checkout root)`.
    ///
    /// Read from the state file — the worker-local truth — so a caller
    /// stamping a Payload names repos exactly as the Definition does
    /// (`GitRef.repo_name` is the Definition's name, never a path or a URL).
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when no Instance is materialized here.
    pub async fn instance_repos(
        &self,
        definition_id: &str,
    ) -> Result<Vec<(String, PathBuf)>, WorkerError> {
        let instance_root = self.instance_root(definition_id);
        let definition = required_state(&instance_root, definition_id).await?;
        definition
            .repos
            .iter()
            .map(|repo| {
                layout_of(repo)
                    .map(|layout| (repo.name.clone(), PathBuf::from(layout)))
                    .map_err(WorkerError::Workspace)
            })
            .collect()
    }

    /// Gives back an archived agent's private checkout: each repo's HEAD is
    /// pinned in the Instance's repo under `refs/horde/archived/<agent>` (so
    /// a commit a result named stays reachable — a branch deleted with
    /// nothing else pointing at it is garbage-collection fodder), then the
    /// worktree is removed, its `horde/<agent>` branch deleted, and the
    /// agent's directory under `.horde-agents/` removed.
    ///
    /// A root that is not an agent-private checkout of this manager — the
    /// `shared` policy's Instance root, or anywhere else — is left alone:
    /// only what [`Self::checkout`] composed for the agent is the agent's to
    /// give back. Uncommitted work in the checkout is discarded; archive is
    /// where an agent's context ends.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when pinning or removal fails. Nothing is
    /// deleted for a repo whose HEAD could not be pinned.
    pub async fn release_checkout(&self, root: &Path) -> Result<(), WorkerError> {
        let Some((instance_root, agent_id)) = self.agent_private(root) else {
            return Ok(());
        };
        let repos = read_state_file(&instance_root)
            .await
            .map(|definition| definition.repos)
            .unwrap_or_default();
        for repo in &repos {
            let layout = layout_of(repo).map_err(WorkerError::Workspace)?;
            let source = instance_root.join(layout);
            let destination = root.join(layout);
            if !destination.exists() {
                continue;
            }
            let destination = path_str(&destination);
            let pin = format!("+HEAD:refs/horde/archived/{agent_id}");
            git(&source, &["fetch", "--no-tags", "--", &destination, &pin])
                .await
                .map_err(WorkerError::Workspace)?;
            if Path::new(&destination).join(".git").is_file() {
                git(&source, &["worktree", "remove", "--force", &destination])
                    .await
                    .map_err(WorkerError::Workspace)?;
            }
            // Absent for a fresh clone (its branch lives in the clone), and
            // for a checkout that never left the instance's HEAD's branch.
            let _ = git(&source, &["branch", "-D", &format!("horde/{agent_id}")]).await;
        }
        match tokio::fs::remove_dir_all(root).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(WorkerError::Workspace(format!(
                "could not remove {}: {error}",
                root.display()
            ))),
        }
    }

    /// `(instance root, agent id)` when `root` is exactly an agent-private
    /// checkout under this manager: `<root>/<definition>/.horde-agents/<agent>`.
    fn agent_private(&self, root: &Path) -> Option<(PathBuf, String)> {
        let agent_id = root.file_name()?.to_str()?;
        let agents_dir = root.parent()?;
        let instance_root = agents_dir.parent()?;
        let definition_id = instance_root.file_name()?.to_str()?;
        let shaped = is_path_safe(agent_id)
            && is_path_safe(definition_id)
            && agents_dir.file_name()?.to_str()? == AGENTS_DIR
            && instance_root.parent()? == self.root;
        shaped.then(|| (instance_root.to_path_buf(), agent_id.to_owned()))
    }

    /// The Instances already on this Worker's disk, for the capability
    /// advertisement and instance reports on connect.
    ///
    /// Reads state files rather than interrogating git: warmth is a placement
    /// preference computed at advertisement time, and a repo directory that
    /// vanished out-of-band downgrades the report to `DEGRADED` so the Server
    /// stops preferring a broken instance.
    pub async fn warm_instances(&self) -> Vec<WorkspaceInstanceReport> {
        let mut reports = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&self.root).await else {
            return reports;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let instance_root = entry.path();
            let Some(definition) = read_state_file(&instance_root).await else {
                continue;
            };
            let definition_id = definition
                .workspace_definition_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();

            let mut repos = Vec::new();
            let mut failures = Vec::new();
            for repo in &definition.repos {
                let path = match layout_of(repo) {
                    Ok(layout) => instance_root.join(layout),
                    Err(failure) => {
                        failures.push(failure);
                        continue;
                    }
                };
                if path.join(".git").exists() {
                    repos.push(RepoInstanceReport {
                        repo_name: repo.name.clone(),
                        path: path.display().to_string(),
                        head_commit_sha: String::new(),
                        dirty: false,
                    });
                } else {
                    failures.push(format!("repo {} is missing from disk", repo.name));
                }
            }
            reports.push(self.report(
                &definition_id,
                instance_root.display().to_string(),
                repos,
                failures,
            ));
        }
        reports
    }

    /// One report, `READY` unless anything failed.
    fn report(
        &self,
        definition_id: &str,
        root_path: String,
        repos: Vec<RepoInstanceReport>,
        failures: Vec<String>,
    ) -> WorkspaceInstanceReport {
        let state = if failures.is_empty() {
            WorkspaceInstanceState::Ready
        } else {
            WorkspaceInstanceState::Degraded
        };
        WorkspaceInstanceReport {
            workspace_definition_id: (!definition_id.is_empty())
                .then(|| horde_proto::v1::WorkspaceDefinitionId::new(definition_id)),
            worker_id: Some(WorkerId::new(self.worker_id.clone())),
            state: state as i32,
            root_path,
            repos,
            detail: failures.join("; "),
            observed_at: None,
        }
    }
}

/// Clones or refreshes one repo of an Instance, reporting what is on disk.
async fn materialize_repo(
    instance_root: &Path,
    repo: &WorkspaceRepo,
) -> Result<RepoInstanceReport, String> {
    let destination = instance_root.join(layout_of(repo)?);

    if destination.join(".git").exists() {
        // Repair path: the checkout exists; refresh its view of the remote.
        git(
            instance_root,
            &["-C", &path_str(&destination), "fetch", "origin"],
        )
        .await?;
    } else {
        // Both values arrive over the wire. A leading `-` would be parsed
        // as an option (`--upload-pack=…` runs a command), so the branch is
        // refused outright and the positionals sit behind `--`.
        if is_option_shaped(&repo.default_branch) || is_option_shaped(&repo.remote_url) {
            return Err(format!(
                "refusing to clone {:?} at branch {:?}: a leading '-' would be read as a git option",
                repo.remote_url, repo.default_branch
            ));
        }
        let mut arguments = vec!["clone".to_owned(), NO_LOCAL.to_owned()];
        if !repo.default_branch.is_empty() {
            arguments.push("--branch".to_owned());
            arguments.push(repo.default_branch.clone());
        }
        arguments.push("--".to_owned());
        arguments.push(repo.remote_url.clone());
        arguments.push(path_str(&destination));
        let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
        git(instance_root, &arguments).await?;
    }

    let head = git(&destination, &["rev-parse", "HEAD"]).await?;
    let status = git(&destination, &["status", "--porcelain"]).await?;

    Ok(RepoInstanceReport {
        repo_name: repo.name.clone(),
        path: destination.display().to_string(),
        head_commit_sha: head.trim().to_owned(),
        dirty: !status.trim().is_empty(),
    })
}

/// One agent worktree: a branch per agent per repo — from `start_point` when
/// the spawn inherited one (the parent's committed HEAD, fix a8), from the
/// instance's HEAD otherwise.
async fn worktree_repo(
    source: PathBuf,
    destination: PathBuf,
    branch: String,
    start_point: Option<String>,
) -> Result<(), WorkerError> {
    let destination = path_str(&destination);
    let mut arguments = vec!["worktree", "add", "-b", branch.as_str(), &destination];
    if let Some(start_point) = &start_point {
        arguments.push(start_point);
    }
    git(&source, &arguments)
        .await
        .map(|_| ())
        .map_err(WorkerError::Workspace)
}

/// One agent clone: full isolation, cloned from the instance's repo through
/// git's transport ([`NO_LOCAL`]), switched onto the inherited start point
/// when the spawn carried one.
async fn clone_repo(
    source: PathBuf,
    destination: PathBuf,
    branch: String,
    start_point: Option<String>,
) -> Result<(), WorkerError> {
    git(
        &source,
        &[
            "clone",
            NO_LOCAL,
            "--",
            &path_str(&source),
            &path_str(&destination),
        ],
    )
    .await
    .map(|_| ())
    .map_err(WorkerError::Workspace)?;
    if let Some(start_point) = &start_point {
        git(&destination, &["switch", "-c", &branch, start_point])
            .await
            .map(|_| ())
            .map_err(WorkerError::Workspace)?;
    }
    Ok(())
}

/// Runs one git invocation to completion, bounded, capturing its output.
///
/// The error is a sentence: the failing subcommand and whatever git printed,
/// because "exit status 128" diagnoses nothing.
async fn git(cwd: &Path, arguments: &[&str]) -> Result<String, String> {
    let ran = tokio::time::timeout(
        GIT_TIMEOUT,
        tokio::process::Command::new("git")
            .args(arguments)
            .current_dir(cwd)
            .output(),
    )
    .await
    .map_err(|_| {
        format!(
            "git {} did not finish within {GIT_TIMEOUT:?}",
            arguments.join(" ")
        )
    })?
    .map_err(|error| format!("git {} could not run: {error}", arguments.join(" ")))?;

    if ran.status.success() {
        Ok(String::from_utf8_lossy(&ran.stdout).into_owned())
    } else {
        Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&ran.stderr).trim()
        ))
    }
}

/// The path a repo occupies inside its Instance, defaulted to its name.
///
/// # Errors
///
/// A layout that would land outside the Instance root — absolute, empty, or
/// climbing with `..` — is refused, not joined: `Path::join` with an
/// absolute path *replaces* the root, so an unchecked layout from a confused
/// or compromised Server would clone (and later delete) wherever it pointed.
fn layout_of(repo: &WorkspaceRepo) -> Result<&str, String> {
    let layout = if repo.layout_path.is_empty() {
        &repo.name
    } else {
        &repo.layout_path
    };
    let mut components = Path::new(layout).components().peekable();
    let contained = components.peek().is_some()
        && components.all(|component| matches!(component, std::path::Component::Normal(_)));
    if contained {
        Ok(layout)
    } else {
        Err(format!(
            "repo {:?} lays out at {layout:?}, which is not a path inside the instance",
            repo.name
        ))
    }
}

/// A clone source or branch git would read as an option. `--` protects the
/// positional URL; a branch rides `--branch` and so must be refused here.
fn is_option_shaped(value: &str) -> bool {
    value.starts_with('-')
}

/// Whether an id may be used as a single directory component.
///
/// Both ids interpolated into paths here are server-minted, so this is a
/// guard against a compromised or confused server, not a validation layer —
/// which is why it refuses rather than sanitizes: a cleaned-up id would
/// silently point two different ids at one directory.
pub(crate) fn is_path_safe(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        && !id.starts_with('.')
}

fn path_str(path: &Path) -> String {
    path.display().to_string()
}

/// The Definition an Instance was materialized from, if one is recorded.
async fn read_state_file(instance_root: &Path) -> Option<WorkspaceDefinition> {
    let bytes = tokio::fs::read(instance_root.join(STATE_FILE)).await.ok()?;
    WorkspaceDefinition::decode(bytes.as_slice()).ok()
}

/// The state file's definition, or the error naming what is not there.
async fn required_state(
    instance_root: &Path,
    definition_id: &str,
) -> Result<WorkspaceDefinition, WorkerError> {
    read_state_file(instance_root).await.ok_or_else(|| {
        WorkerError::Workspace(format!(
            "no instance of {definition_id} is materialized under {}",
            instance_root.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_directory_component_admits_no_traversal_and_no_hidden_names() {
        assert!(is_path_safe("ws-1a2b-3"));
        assert!(is_path_safe("agent_7"));
        assert!(!is_path_safe(""));
        assert!(!is_path_safe(".."));
        assert!(
            !is_path_safe(".hidden"),
            "a leading dot hides the directory"
        );
        assert!(!is_path_safe("a/b"), "a separator escapes the root");
        assert!(!is_path_safe("a b"), "shell-hostile names are refused");
    }

    #[test]
    fn a_repo_lays_out_under_its_name_unless_told_otherwise() {
        let mut repo = WorkspaceRepo {
            name: "atlas".to_owned(),
            ..WorkspaceRepo::default()
        };
        assert_eq!(layout_of(&repo), Ok("atlas"));
        repo.layout_path = "code/atlas".to_owned();
        assert_eq!(layout_of(&repo), Ok("code/atlas"));
    }

    /// A layout that would land outside the Instance is refused: `join` with
    /// an absolute path replaces the root, and `..` climbs out of it.
    #[test]
    fn a_layout_that_escapes_the_instance_is_refused() {
        for hostile in ["../escape", "/etc", "code/../../x", "./atlas", "."] {
            let repo = WorkspaceRepo {
                name: "atlas".to_owned(),
                layout_path: hostile.to_owned(),
                ..WorkspaceRepo::default()
            };
            assert!(
                layout_of(&repo).is_err_and(|e| e.contains("not a path inside")),
                "{hostile:?} must be refused"
            );
        }
        let nameless = WorkspaceRepo::default();
        assert!(layout_of(&nameless).is_err(), "an empty layout is no path");
        let escaping_name = WorkspaceRepo {
            name: "../atlas".to_owned(),
            ..WorkspaceRepo::default()
        };
        assert!(
            layout_of(&escaping_name).is_err(),
            "the name is the layout's default, so it is checked too"
        );
    }

    #[test]
    fn only_a_leading_dash_is_option_shaped() {
        assert!(is_option_shaped("--upload-pack=touch /tmp/x"));
        assert!(is_option_shaped("-b"));
        assert!(!is_option_shaped("https://example.com/a-b.git"));
        assert!(!is_option_shaped(""));
    }

    /// Only an exact agent-private checkout of this manager is releasable:
    /// the shared policy's Instance root, a foreign path, and a hostile
    /// shape are all left alone.
    #[test]
    fn only_an_agent_private_checkout_is_releasable() {
        let manager = WorkspaceManager::new("w", "/ws");
        assert_eq!(
            manager.agent_private(Path::new("/ws/def-1/.horde-agents/agent-1")),
            Some((PathBuf::from("/ws/def-1"), "agent-1".to_owned()))
        );
        for left_alone in [
            "/ws/def-1",
            "/ws/def-1/other/agent-1",
            "/elsewhere/def-1/.horde-agents/agent-1",
            "/ws/nested/def-1/.horde-agents/agent-1",
            "/ws/def-1/.horde-agents/.hidden",
            "/ws/.def/.horde-agents/agent-1",
            "/",
        ] {
            assert_eq!(
                manager.agent_private(Path::new(left_alone)),
                None,
                "{left_alone} is not the agent's to give back"
            );
        }
    }
}
