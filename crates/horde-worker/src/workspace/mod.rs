//! Workspace Instances on this Worker's disk.
//!
//! The Server owns Definitions; this module owns their materialization —
//! "Instances are never self-initiated: they exist because the Server asked"
//! (`MaterializeWorkspaceCommand`) — and the per-agent Checkout Policies from
//! DESIGN §7: `shared` works in the Instance itself, `worktree` (the default)
//! composes one git worktree per repo under an agent-private root, and
//! `fresh-clone` clones every repo for total isolation.
//!
//! ADR-0014 adds how a checkout starts and ends: every new checkout starts
//! from a freshly fetched `origin/<default_branch>` ([`base`]), a lazy
//! workspace checks out only the repos an agent asks for ([`manifest`]), one
//! branch name may span every repo, each checkout is recorded in a ledger
//! beside it ([`ledger`]), and disk comes back in two tiers ([`reclaim`],
//! [`removal`], [`cleanup`]).
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
//! `DEGRADED` report whose detail says which repo and why. Checkout *does*
//! error, into the spawn-failed lifecycle path: an agent cannot run in a
//! checkout that does not exist.

pub mod base;
pub mod cleanup;
pub(crate) mod git;
pub mod ledger;
pub mod manifest;
pub mod policy;
pub mod reclaim;
pub mod removal;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use horde_proto::prost::Message;
use horde_proto::v1::checkout_prepared::{BaseResolution, Repo as PreparedRepo};
use horde_proto::v1::{
    CheckoutMode, CheckoutPolicy, CheckoutPrepared, CheckoutReport, CheckoutState,
    MaterializationProgress, RepoBase, RepoBaseSource, RepoInstanceReport, WorkerId,
    WorkspaceDefinition, WorkspaceInstanceReport, WorkspaceInstanceState, WorkspaceRepo,
};

use crate::error::WorkerError;
use base::{BranchPlan, CommitSource, FetchPolicy, FetchThrottle, ResolvedBase};
use git::{commit_of, git, is_option_shaped, path_str};
use ledger::{LedgerRecord, LedgerRepo, now_ms};

/// Where an Instance records the Definition it was materialized from.
pub const STATE_FILE: &str = ".horde-instance.pb";

/// Where per-agent checkouts live inside an Instance root.
pub(crate) const AGENTS_DIR: &str = ".horde-agents";

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
const NO_LOCAL: &str = "--no-local";

/// The free and total bytes of the filesystem holding a path.
pub trait FsStats: Send + Sync + std::fmt::Debug {
    /// `(free, total)`, or `None` when the filesystem cannot say.
    fn stats(&self, path: &Path) -> Option<(u64, u64)>;
}

/// The production source: `statvfs` on the path.
#[derive(Debug, Clone, Copy, Default)]
pub struct StatvfsStats;

impl FsStats for StatvfsStats {
    fn stats(&self, path: &Path) -> Option<(u64, u64)> {
        let stats = nix::sys::statvfs::statvfs(path).ok()?;
        let fragment = u64::from(stats.fragment_size());
        let free = u64::from(stats.blocks_available()).saturating_mul(fragment);
        let total = u64::from(stats.blocks()).saturating_mul(fragment);
        Some((free, total))
    }
}

/// What a spawn asks of its checkout.
#[derive(Debug, Clone, Default)]
pub struct CheckoutRequest {
    pub definition_id: String,
    pub agent_id: String,
    pub session_id: String,
    pub policy: CheckoutPolicy,
    /// The spawn's override; unspecified reads the Definition's.
    pub mode: CheckoutMode,
    /// The initial repos under LAZY; non-empty implies LAZY.
    pub checkout_repos: Vec<String>,
    pub bases: Vec<RepoBase>,
    /// Empty: `horde/<agent-id>`.
    pub branch: String,
    pub fetch: FetchPolicy,
}

/// A composed checkout.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedCheckout {
    /// The agent's working directory.
    pub root: PathBuf,
    /// `(repo name, relative directory)` for exactly the checked-out repos.
    pub repos: Vec<(String, PathBuf)>,
    /// What each repo started from, for the agent's Session.
    pub prepared: CheckoutPrepared,
    /// The prompt paragraph a lazy agent gets; `None` under ALL.
    pub overlay: Option<String>,
}

/// One repo added by `workspace_checkout`.
#[derive(Debug, Clone, PartialEq)]
pub struct AddedRepo {
    pub path: PathBuf,
    pub relative: PathBuf,
    pub branch: String,
    pub prepared: PreparedRepo,
}

/// Materializes Instances and applies Checkout Policies under one root.
#[derive(Debug, Clone)]
pub struct WorkspaceManager {
    worker_id: String,
    root: PathBuf,
    throttle: Arc<FetchThrottle>,
    fs_stats: Arc<dyn FsStats>,
}

impl WorkspaceManager {
    /// A manager materializing under `root`, reporting as `worker_id`.
    pub fn new(worker_id: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self {
            worker_id: worker_id.into(),
            root: root.into(),
            throttle: Arc::new(FetchThrottle::new()),
            fs_stats: Arc::new(StatvfsStats),
        }
    }

    /// Replaces where free-space facts come from — the disk-pressure tests
    /// supply low numbers through this seam (ADR-0014's test plan).
    #[must_use]
    pub fn with_fs_stats(mut self, stats: Arc<dyn FsStats>) -> Self {
        self.fs_stats = stats;
        self
    }

    /// The Worker this manager reports as.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// The workspace root.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// How many origin fetches this manager has run.
    #[must_use]
    pub fn fetches_run(&self) -> u64 {
        self.throttle.fetches_run()
    }

    /// `(free, total)` bytes of the filesystem holding the workspace root.
    #[must_use]
    pub fn filesystem(&self) -> Option<(u64, u64)> {
        let probe = if self.root.exists() {
            self.root.clone()
        } else {
            self.root
                .ancestors()
                .find(|ancestor| ancestor.exists())
                .map(Path::to_path_buf)?
        };
        self.fs_stats.stats(&probe)
    }

    /// One Instance's root directory.
    fn instance_root(&self, definition_id: &str) -> PathBuf {
        self.root.join(definition_id)
    }

    /// One Instance's agents directory.
    fn agents_dir(&self, definition_id: &str) -> PathBuf {
        self.instance_root(definition_id).join(AGENTS_DIR)
    }

    /// Creates or repairs one Instance, per the Server's command.
    ///
    /// Always returns a report: `READY` when every repo materialized, and
    /// `DEGRADED` naming what failed otherwise.
    pub async fn materialize(&self, definition: &WorkspaceDefinition) -> WorkspaceInstanceReport {
        self.materialize_with(definition, &mut |_| {}).await
    }

    /// [`Self::materialize`], telling `progress` before each repo and before
    /// the unmanaged paths — at most `repos + 1` steps, deliberately coarse.
    pub async fn materialize_with(
        &self,
        definition: &WorkspaceDefinition,
        progress: &mut (dyn FnMut(MaterializationProgress) + Send),
    ) -> WorkspaceInstanceReport {
        let definition_id = definition_id_of(definition);
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

        let total = u32::try_from(definition.repos.len()).unwrap_or(u32::MAX);
        for (index, repo) in definition.repos.iter().enumerate() {
            let verb = if instance_root.join(&repo.name).join(".git").exists() {
                "fetching"
            } else {
                "cloning"
            };
            progress(MaterializationProgress {
                step: format!("{verb} {}", repo.name),
                completed_repos: u32::try_from(index).unwrap_or(u32::MAX),
                total_repos: total,
                detail: String::new(),
            });
            match materialize_repo(&instance_root, repo).await {
                Ok(report) => repos.push(report),
                Err(failure) => failures.push(format!("repo {}: {failure}", repo.name)),
            }
        }

        if !definition.unmanaged_paths.is_empty() {
            progress(MaterializationProgress {
                step: "creating unmanaged paths".to_owned(),
                completed_repos: total,
                total_repos: total,
                detail: String::new(),
            });
            failures.extend(create_unmanaged(&instance_root, &definition.unmanaged_paths).await);
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

    /// The working directory an agent occupies, per its Checkout Policy,
    /// mode, branch and bases (ADR-0014).
    ///
    /// A checkout with a ledger record is recomposed from it — a revival or
    /// a redelivered spawn reuses the same repos, branch and bases, and its
    /// repos report `REUSED`; a fresh base is never re-resolved under a
    /// running agent.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when the Instance has not been
    /// materialized, a repo or base is unknown, a named base cannot be
    /// resolved, the branch is held elsewhere, or a git operation fails —
    /// the spawn's failure, not the Worker's.
    pub async fn checkout_agent(
        &self,
        request: &CheckoutRequest,
        source: &dyn CommitSource,
    ) -> Result<PreparedCheckout, WorkerError> {
        let (definition_id, agent_id) = (&request.definition_id, &request.agent_id);
        if !is_path_safe(definition_id) || !is_path_safe(agent_id) {
            return Err(workspace_error(format!(
                "{definition_id:?} / {agent_id:?} are not directory names"
            )));
        }
        let instance_root = self.instance_root(definition_id);
        let definition = required_state(&instance_root, definition_id).await?;
        let mode = resolve_mode(request, &definition);

        if request.policy == CheckoutPolicy::Shared {
            return shared_checkout(&instance_root, &definition, request, mode).await;
        }
        validate_names(&definition, request)?;
        let branch = branch_of(request);
        check_branch_name(&instance_root, &branch).await?;

        let agents_dir = self.agents_dir(definition_id);
        let agent_root = agents_dir.join(agent_id);
        let mut record = match ledger::read(&agents_dir, agent_id).await {
            Some(record) => record,
            None => LedgerRecord {
                agent_id: agent_id.clone(),
                session_id: request.session_id.clone(),
                policy: policy_name(request.policy).to_owned(),
                mode: mode_name(mode).to_owned(),
                branch: branch.clone(),
                created_at_ms: now_ms(),
                last_active_at_ms: now_ms(),
                ..LedgerRecord::default()
            },
        };
        let reused: Vec<String> = record
            .repos
            .iter()
            .filter(|repo| !repo.pending)
            .map(|repo| repo.name.clone())
            .collect();
        let wanted: Vec<String> = if record.repos.is_empty() {
            match mode {
                CheckoutMode::Lazy => request.checkout_repos.clone(),
                _ => definition.repos.iter().map(|repo| repo.name.clone()).collect(),
            }
        } else {
            record.repos.iter().map(|repo| repo.name.clone()).collect()
        };

        let mut prepared = Vec::new();
        for repo in definition.repos.iter().filter(|repo| wanted.contains(&repo.name)) {
            let base = request.bases.iter().find(|base| base.repo_name == repo.name);
            let added = if reused.contains(&repo.name) {
                reused_repo(&agent_root, repo, &record).await?
            } else {
                self.add_one(
                    &instance_root,
                    &agents_dir,
                    &mut record,
                    repo,
                    base,
                    request.fetch,
                    source,
                )
                .await?
            };
            prepared.push(added);
        }
        link_unmanaged(&instance_root, &agent_root, &definition.unmanaged_paths).await?;
        let manifest = self
            .rewrite_manifest(&agent_root, &definition, &record)
            .await?;
        let overlay = (record.mode == mode_name(CheckoutMode::Lazy))
            .then(|| manifest::lazy_overlay(&manifest, &manifest::manifest_path(&agent_root)));

        Ok(PreparedCheckout {
            repos: checked_out_repos(&definition, &record),
            root: agent_root,
            prepared: CheckoutPrepared {
                repos: prepared,
                mode: if record.mode == mode_name(CheckoutMode::Lazy) {
                    CheckoutMode::Lazy
                } else {
                    CheckoutMode::All
                }
                .into(),
                available_repos: definition.repos.iter().map(|r| r.name.clone()).collect(),
            },
            overlay,
        })
    }

    /// Adds one repo to a live agent's checkout: `workspace_checkout`.
    /// Idempotent — a repo already checked out returns its path and does
    /// nothing else.
    ///
    /// # Errors
    ///
    /// An unknown repo (naming the valid ones), an agent with no ledgered
    /// checkout here (no workspace, or the `shared` policy), or the same
    /// failures as a spawn's checkout.
    pub async fn add_repo(
        &self,
        definition_id: &str,
        agent_id: &str,
        repo_name: &str,
        base: Option<&str>,
        fetch: FetchPolicy,
        source: &dyn CommitSource,
    ) -> Result<AddedRepo, WorkerError> {
        if !is_path_safe(definition_id) || !is_path_safe(agent_id) {
            return Err(workspace_error("not directory names"));
        }
        let instance_root = self.instance_root(definition_id);
        let definition = required_state(&instance_root, definition_id).await?;
        let Some(repo) = definition.repos.iter().find(|repo| repo.name == repo_name) else {
            return Err(workspace_error(format!(
                "no repo {repo_name:?} in workspace {}; valid repos: {}",
                definition.name,
                repo_names(&definition).join(", ")
            )));
        };
        let agents_dir = self.agents_dir(definition_id);
        let mut record = ledger::read(&agents_dir, agent_id).await.ok_or_else(|| {
            workspace_error(format!(
                "agent {agent_id} has no private checkout of workspace {} (a shared policy cannot check out repos)",
                definition.name
            ))
        })?;
        let agent_root = agents_dir.join(agent_id);
        let layout = PathBuf::from(layout_of(repo).map_err(WorkerError::Workspace)?);
        let prepared = if record.repos.iter().any(|r| r.name == repo_name && !r.pending) {
            reused_repo(&agent_root, repo, &record).await?
        } else {
            let requested = base.map(|reference| RepoBase {
                repo_name: repo_name.to_owned(),
                r#ref: reference.to_owned(),
                source: RepoBaseSource::Requested.into(),
            });
            let added = self
                .add_one(
                    &instance_root,
                    &agents_dir,
                    &mut record,
                    repo,
                    requested.as_ref(),
                    fetch,
                    source,
                )
                .await?;
            record.last_active_at_ms = now_ms();
            record.soft_reclaimed_at_ms = None;
            ledger::write(&agents_dir, &record)
                .await
                .map_err(WorkerError::Workspace)?;
            self.rewrite_manifest(&agent_root, &definition, &record)
                .await?;
            added
        };
        Ok(AddedRepo {
            path: agent_root.join(&layout),
            relative: layout,
            branch: record.branch.clone(),
            prepared,
        })
    }

    /// Records commits delivered for a repo the agent has not checked out:
    /// they are in the Instance repo, listed as pending in the manifest.
    ///
    /// # Errors
    ///
    /// The manifest could not be rewritten.
    pub async fn note_pending(
        &self,
        agent_root: &Path,
        repo_name: &str,
        sha: &str,
    ) -> Result<(), WorkerError> {
        if let Some(mut manifest) = manifest::read(agent_root).await {
            manifest.add_pending(repo_name, sha);
            manifest::write(agent_root, &manifest)
                .await
                .map_err(WorkerError::Workspace)?;
        }
        Ok(())
    }

    /// The Instance repo of one repo name, for obtaining a commit into it.
    pub async fn instance_repo_path(
        &self,
        definition_id: &str,
        repo_name: &str,
    ) -> Result<PathBuf, WorkerError> {
        self.instance_repos(definition_id)
            .await?
            .into_iter()
            .find(|(name, _)| name == repo_name)
            .map(|(_, relative)| self.instance_root(definition_id).join(relative))
            .ok_or_else(|| workspace_error(format!("no repo {repo_name} in {definition_id}")))
    }

    /// Checks one repo out for an agent, ledger first.
    #[allow(clippy::too_many_arguments)]
    async fn add_one(
        &self,
        instance_root: &Path,
        agents_dir: &Path,
        record: &mut LedgerRecord,
        repo: &WorkspaceRepo,
        requested: Option<&RepoBase>,
        fetch: FetchPolicy,
        source: &dyn CommitSource,
    ) -> Result<PreparedRepo, WorkerError> {
        let layout = layout_of(repo).map_err(WorkerError::Workspace)?;
        let instance_repo = instance_root.join(layout);
        let destination = agents_dir.join(&record.agent_id).join(layout);
        let known = record.repos.iter().find(|r| r.name == repo.name).cloned();
        // A checkout already on disk that the ledger does not know — one
        // composed before the ledger existed — is adopted as it is, never
        // stacked on or re-based under its agent.
        if known.is_none() && destination.join(".git").exists() {
            let head = commit_of(&destination, "HEAD").await.unwrap_or_default();
            record.repos.push(LedgerRepo {
                name: repo.name.clone(),
                base_commit_sha: head.clone(),
                resolution: resolution_name(BaseResolution::Reused).to_owned(),
                created_branch: true,
                ..LedgerRepo::default()
            });
            ledger::write(agents_dir, record)
                .await
                .map_err(WorkerError::Workspace)?;
            return Ok(PreparedRepo {
                repo_name: repo.name.clone(),
                branch: record.branch.clone(),
                base_commit_sha: head,
                resolution: BaseResolution::Reused.into(),
                ..PreparedRepo::default()
            });
        }
        // A crash mid-`worktree add` leaves a pending record and maybe a
        // half-built directory: prune, remove, and redo.
        if known.is_some_and(|known| known.pending) {
            let _ = git(&instance_repo, &["worktree", "prune"]).await;
            if destination.exists() {
                tokio::fs::remove_dir_all(&destination)
                    .await
                    .map_err(|error| workspace_error(format!("could not clear {}: {error}", destination.display())))?;
            }
        }
        let resolved = base::prepare_base(
            &instance_repo,
            repo,
            requested,
            &record.branch,
            &self.throttle,
            fetch,
            source,
        )
        .await
        .map_err(WorkerError::Workspace)?;

        record.repos.retain(|r| r.name != repo.name);
        record.repos.push(LedgerRepo {
            name: repo.name.clone(),
            base_commit_sha: resolved.sha.clone(),
            resolution: resolution_name(resolved.resolution).to_owned(),
            requested_ref: resolved.requested_ref.clone(),
            created_branch: false,
            pending: true,
        });
        ledger::write(agents_dir, record)
            .await
            .map_err(WorkerError::Workspace)?;

        let created_branch = if record.is_fresh_clone() {
            clone_repo(&instance_repo, &destination, &record.branch, &resolved.sha).await?;
            true
        } else {
            worktree_repo(&instance_repo, &destination, &record.branch, &resolved.sha).await?
        };

        if let Some(entry) = record.repos.iter_mut().find(|r| r.name == repo.name) {
            entry.pending = false;
            entry.created_branch = created_branch;
        }
        ledger::write(agents_dir, record)
            .await
            .map_err(WorkerError::Workspace)?;
        Ok(prepared_repo(&repo.name, &record.branch, &resolved))
    }

    async fn rewrite_manifest(
        &self,
        agent_root: &Path,
        definition: &WorkspaceDefinition,
        record: &LedgerRecord,
    ) -> Result<manifest::Manifest, WorkerError> {
        let previous = manifest::read(agent_root).await;
        let manifest = manifest::Manifest::describe(definition, record, previous.as_ref());
        manifest::write(agent_root, &manifest)
            .await
            .map_err(WorkerError::Workspace)?;
        Ok(manifest)
    }

    /// The repos a checkout of this Instance composes: `(repo name, relative
    /// directory under the checkout root)`, read from the state file.
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

    /// Where [`Self::checkout_agent`] composes `agent_id`'s private checkout
    /// of `definition_id` — whether or not one is there. `None` when either
    /// id is not a directory name.
    #[must_use]
    pub fn agent_root(&self, definition_id: &str, agent_id: &str) -> Option<PathBuf> {
        (is_path_safe(definition_id) && is_path_safe(agent_id))
            .then(|| self.agents_dir(definition_id).join(agent_id))
    }

    /// `(definition id, agent id)` when `root` is exactly an agent-private
    /// checkout under this manager: `<root>/<definition>/.horde-agents/<agent>`.
    #[must_use]
    pub fn agent_private(&self, root: &Path) -> Option<(String, String)> {
        let agent_id = root.file_name()?.to_str()?;
        let agents_dir = root.parent()?;
        let instance_root = agents_dir.parent()?;
        let definition_id = instance_root.file_name()?.to_str()?;
        let shaped = is_path_safe(agent_id)
            && is_path_safe(definition_id)
            && agents_dir.file_name()?.to_str()? == AGENTS_DIR
            && instance_root.parent()? == self.root;
        shaped.then(|| (definition_id.to_owned(), agent_id.to_owned()))
    }

    /// The Definitions of every Instance on disk, by id.
    pub async fn instances(&self) -> Vec<(String, WorkspaceDefinition)> {
        let mut instances = Vec::new();
        let Ok(mut entries) = tokio::fs::read_dir(&self.root).await else {
            return instances;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            if let Some(definition) = read_state_file(&entry.path()).await {
                let id = definition_id_of(&definition);
                if is_path_safe(&id) && entry.file_name().to_str() == Some(id.as_str()) {
                    instances.push((id, definition));
                }
            }
        }
        instances.sort_by(|a, b| a.0.cmp(&b.0));
        instances
    }

    /// The Instances already on this Worker's disk, for the capability
    /// advertisement and instance reports — with no live agents known, so
    /// every unarchived checkout reads as an orphan. See
    /// [`Self::instance_reports`].
    pub async fn warm_instances(&self) -> Vec<WorkspaceInstanceReport> {
        self.instance_reports(&|_| None).await
    }

    /// Every Instance on disk, reported with its checkouts, sizes and disk
    /// facts. `classify` names the state of a checkout whose agent the
    /// Runtime knows; `None` leaves the ledger to say (archived, else
    /// orphan).
    pub async fn instance_reports(
        &self,
        classify: &(dyn Fn(&LedgerRecord) -> Option<CheckoutState> + Sync),
    ) -> Vec<WorkspaceInstanceReport> {
        let filesystem = self.filesystem();
        let mut reports = Vec::new();
        for (definition_id, definition) in self.instances().await {
            let instance_root = self.instance_root(&definition_id);
            let mut repos = Vec::new();
            let mut failures = Vec::new();
            for repo in &definition.repos {
                match observe_instance_repo(&instance_root, repo).await {
                    Ok(report) => repos.push(report),
                    Err(failure) => failures.push(failure),
                }
            }
            let mut report = self.report(
                &definition_id,
                instance_root.display().to_string(),
                repos,
                failures,
            );
            report.checkouts = self.checkout_reports(&definition_id, classify).await;
            report.instance_bytes = reclaim::dir_size(&instance_root).await.unwrap_or_default();
            for path in &definition.unmanaged_paths {
                if unmanaged_path_ok(path) {
                    report.unmanaged_bytes += reclaim::dir_size(&instance_root.join(path))
                        .await
                        .unwrap_or_default();
                }
            }
            if let Some((free, total)) = filesystem {
                report.filesystem_free_bytes = free;
                report.filesystem_total_bytes = total;
            }
            report.sized_at = Some(timestamp(now_ms()));
            reports.push(report);
        }
        reports
    }

    async fn checkout_reports(
        &self,
        definition_id: &str,
        classify: &(dyn Fn(&LedgerRecord) -> Option<CheckoutState> + Sync),
    ) -> Vec<CheckoutReport> {
        let agents_dir = self.agents_dir(definition_id);
        let mut reports = Vec::new();
        for record in self.records(definition_id).await {
            let state = classify(&record).unwrap_or(if record.archived_at_ms.is_some() {
                CheckoutState::ArchivedRetained
            } else {
                CheckoutState::Orphan
            });
            reports.push(CheckoutReport {
                agent_id: Some(horde_proto::v1::AgentId::new(record.agent_id.clone())),
                session_id: (!record.session_id.is_empty())
                    .then(|| horde_proto::v1::SessionId::new(record.session_id.clone())),
                repos: record.repos.iter().map(|repo| repo.name.clone()).collect(),
                branch: record.branch.clone(),
                state: state.into(),
                last_active_at: Some(timestamp(record.last_active_at_ms)),
                soft_reclaimed_at: record.soft_reclaimed_at_ms.map(timestamp),
                bytes: reclaim::dir_size(&agents_dir.join(&record.agent_id))
                    .await
                    .unwrap_or_default(),
                retained_reason: record.retained_reason.clone(),
            });
        }
        reports
    }

    /// Every checkout record of one Instance — ledgered ones, plus a
    /// synthesized record for each agent directory with no ledger at all (a
    /// pre-ledger checkout, or one whose record was lost), so it is still
    /// reported and can still be cleaned up.
    pub async fn records(&self, definition_id: &str) -> Vec<LedgerRecord> {
        let agents_dir = self.agents_dir(definition_id);
        let mut records = ledger::list(&agents_dir).await;
        let Some(definition) = read_state_file(&self.instance_root(definition_id)).await else {
            return records;
        };
        let Ok(mut entries) = tokio::fs::read_dir(&agents_dir).await else {
            return records;
        };
        while let Ok(Some(entry)) = entries.next_entry().await {
            let Some(agent_id) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if !is_path_safe(&agent_id) || records.iter().any(|r| r.agent_id == agent_id) {
                continue;
            }
            records.push(synthesized_record(&entry.path(), &agent_id, &definition));
        }
        records.sort_by(|a, b| a.agent_id.cmp(&b.agent_id));
        records
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
            ..WorkspaceInstanceReport::default()
        }
    }
}

/// A record for an agent directory the ledger does not know: its repos are
/// whichever layouts exist, its branch the pre-ADR default.
fn synthesized_record(
    agent_root: &Path,
    agent_id: &str,
    definition: &WorkspaceDefinition,
) -> LedgerRecord {
    let repos = definition
        .repos
        .iter()
        .filter(|repo| {
            layout_of(repo)
                .map(|layout| agent_root.join(layout).exists())
                .unwrap_or(false)
        })
        .map(|repo| LedgerRepo {
            name: repo.name.clone(),
            created_branch: true,
            ..LedgerRepo::default()
        })
        .collect();
    let fresh_clone = definition.repos.iter().any(|repo| {
        layout_of(repo)
            .map(|layout| agent_root.join(layout).join(".git").is_dir())
            .unwrap_or(false)
    });
    let modified = std::fs::metadata(agent_root)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    LedgerRecord {
        agent_id: agent_id.to_owned(),
        policy: if fresh_clone {
            ledger::FRESH_CLONE
        } else {
            ledger::WORKTREE
        }
        .to_owned(),
        mode: mode_name(CheckoutMode::All).to_owned(),
        branch: format!("horde/{agent_id}"),
        repos,
        created_at_ms: modified,
        last_active_at_ms: modified,
        ..LedgerRecord::default()
    }
}

/// The mode a spawn resolves to: initial repos imply LAZY, then the spawn's
/// override, then the Definition's, then ALL.
fn resolve_mode(request: &CheckoutRequest, definition: &WorkspaceDefinition) -> CheckoutMode {
    if !request.checkout_repos.is_empty() {
        return CheckoutMode::Lazy;
    }
    match request.mode {
        CheckoutMode::Unspecified => match CheckoutMode::try_from(definition.checkout_mode) {
            Ok(CheckoutMode::Lazy) => CheckoutMode::Lazy,
            _ => CheckoutMode::All,
        },
        mode => mode,
    }
}

/// Refuses initial repos and bases naming repos the Definition lacks.
fn validate_names(
    definition: &WorkspaceDefinition,
    request: &CheckoutRequest,
) -> Result<(), WorkerError> {
    let known = repo_names(definition);
    let named = request
        .checkout_repos
        .iter()
        .chain(request.bases.iter().map(|base| &base.repo_name));
    for name in named {
        if !known.contains(name) {
            return Err(workspace_error(format!(
                "no repo {name:?} in workspace {}; valid repos: {}",
                definition.name,
                known.join(", ")
            )));
        }
    }
    Ok(())
}

fn repo_names(definition: &WorkspaceDefinition) -> Vec<String> {
    definition.repos.iter().map(|repo| repo.name.clone()).collect()
}

/// The agent's branch: the spawn's, else `horde/<agent-id>`.
fn branch_of(request: &CheckoutRequest) -> String {
    if request.branch.is_empty() {
        format!("horde/{}", request.agent_id)
    } else {
        request.branch.clone()
    }
}

/// `git check-ref-format --branch`, with option-shaped names refused first.
async fn check_branch_name(instance_root: &Path, branch: &str) -> Result<(), WorkerError> {
    if is_option_shaped(branch)
        || git(instance_root, &["check-ref-format", "--branch", branch])
            .await
            .is_err()
    {
        return Err(workspace_error(format!("{branch:?} is not a valid branch name")));
    }
    Ok(())
}

/// The `shared` policy: the Instance itself, with no branch, base or lazy
/// mode — each would mean mutating the Instance tree.
async fn shared_checkout(
    instance_root: &Path,
    definition: &WorkspaceDefinition,
    request: &CheckoutRequest,
    mode: CheckoutMode,
) -> Result<PreparedCheckout, WorkerError> {
    if mode == CheckoutMode::Lazy || !request.branch.is_empty() || !request.bases.is_empty() {
        return Err(workspace_error(
            "the shared policy works in the instance itself: no branch, base or lazy checkout",
        ));
    }
    let mut repos = Vec::new();
    let mut prepared = Vec::new();
    for repo in &definition.repos {
        let layout = layout_of(repo).map_err(WorkerError::Workspace)?;
        let head = commit_of(&instance_root.join(layout), "HEAD")
            .await
            .unwrap_or_default();
        repos.push((repo.name.clone(), PathBuf::from(layout)));
        prepared.push(PreparedRepo {
            repo_name: repo.name.clone(),
            branch: String::new(),
            base_commit_sha: head,
            resolution: BaseResolution::InstanceHead.into(),
            ..PreparedRepo::default()
        });
    }
    Ok(PreparedCheckout {
        root: instance_root.to_path_buf(),
        repos,
        prepared: CheckoutPrepared {
            repos: prepared,
            mode: CheckoutMode::All.into(),
            available_repos: repo_names(definition),
        },
        overlay: None,
    })
}

/// A repo the ledger says is already checked out: `REUSED` with its
/// current HEAD, or a failure when its directory vanished.
async fn reused_repo(
    agent_root: &Path,
    repo: &WorkspaceRepo,
    record: &LedgerRecord,
) -> Result<PreparedRepo, WorkerError> {
    let layout = layout_of(repo).map_err(WorkerError::Workspace)?;
    let head = commit_of(&agent_root.join(layout), "HEAD")
        .await
        .ok_or_else(|| {
            workspace_error(format!(
                "the ledger records repo {} for agent {}, but its checkout is gone",
                repo.name, record.agent_id
            ))
        })?;
    let requested_ref = record
        .repos
        .iter()
        .find(|r| r.name == repo.name)
        .map(|r| r.base_commit_sha.clone())
        .unwrap_or_default();
    Ok(PreparedRepo {
        repo_name: repo.name.clone(),
        branch: record.branch.clone(),
        base_commit_sha: head,
        resolution: BaseResolution::Reused.into(),
        requested_ref,
        ..PreparedRepo::default()
    })
}

fn prepared_repo(name: &str, branch: &str, resolved: &ResolvedBase) -> PreparedRepo {
    PreparedRepo {
        repo_name: name.to_owned(),
        branch: branch.to_owned(),
        base_commit_sha: resolved.sha.clone(),
        resolution: resolved.resolution.into(),
        requested_ref: resolved.requested_ref.clone(),
        fetched_at: resolved.fetched_at_ms.map(timestamp),
        stale_reason: resolved.stale_reason.clone(),
    }
}

/// `(repo name, relative layout)` for the ledger's checked-out repos, in
/// Definition order.
fn checked_out_repos(
    definition: &WorkspaceDefinition,
    record: &LedgerRecord,
) -> Vec<(String, PathBuf)> {
    definition
        .repos
        .iter()
        .filter(|repo| record.repos.iter().any(|r| r.name == repo.name && !r.pending))
        .filter_map(|repo| Some((repo.name.clone(), PathBuf::from(layout_of(repo).ok()?))))
        .collect()
}

/// The ledger's name for a Checkout Policy.
fn policy_name(policy: CheckoutPolicy) -> &'static str {
    match policy {
        CheckoutPolicy::FreshClone => ledger::FRESH_CLONE,
        _ => ledger::WORKTREE,
    }
}

/// The ledger's and manifest's name for a mode.
#[must_use]
pub fn mode_name(mode: CheckoutMode) -> &'static str {
    match mode {
        CheckoutMode::Lazy => "lazy",
        _ => "all",
    }
}

/// The ledger's and manifest's name for a base resolution.
#[must_use]
pub fn resolution_name(resolution: BaseResolution) -> &'static str {
    match resolution {
        BaseResolution::Fresh => "fresh",
        BaseResolution::Cached => "cached",
        BaseResolution::InstanceHead => "instance-head",
        BaseResolution::Parent => "parent",
        BaseResolution::Requested => "requested",
        BaseResolution::Reused => "reused",
        BaseResolution::Unspecified => "unspecified",
    }
}

/// A Unix-millisecond instant on the wire.
#[must_use]
pub fn timestamp(unix_ms: i64) -> horde_proto::prost_types::Timestamp {
    horde_proto::prost_types::Timestamp {
        seconds: unix_ms.div_euclid(1000),
        nanos: i32::try_from(unix_ms.rem_euclid(1000) * 1_000_000).unwrap_or(0),
    }
}

fn definition_id_of(definition: &WorkspaceDefinition) -> String {
    definition
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

fn workspace_error(message: impl Into<String>) -> WorkerError {
    WorkerError::Workspace(message.into())
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
    observe_instance_repo(instance_root, repo).await
}

/// One Instance repo as it is on disk: its HEAD and whether it is dirty.
async fn observe_instance_repo(
    instance_root: &Path,
    repo: &WorkspaceRepo,
) -> Result<RepoInstanceReport, String> {
    let destination = instance_root.join(layout_of(repo)?);
    if !destination.join(".git").exists() {
        return Err(format!("repo {} is missing from disk", repo.name));
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

/// Creates each unmanaged path inside the Instance, returning failures.
async fn create_unmanaged(instance_root: &Path, paths: &[String]) -> Vec<String> {
    let mut failures = Vec::new();
    for path in paths {
        if !unmanaged_path_ok(path) {
            failures.push(format!(
                "unmanaged path {path:?} is not a relative path inside the instance"
            ));
            continue;
        }
        if let Err(error) = tokio::fs::create_dir_all(instance_root.join(path)).await {
            failures.push(format!("could not create unmanaged path {path}: {error}"));
        }
    }
    failures
}

/// Links each unmanaged path into an agent root: a symlink to the Instance's
/// directory, so the data is shared and never part of any repo.
async fn link_unmanaged(
    instance_root: &Path,
    agent_root: &Path,
    paths: &[String],
) -> Result<(), WorkerError> {
    for path in paths.iter().filter(|path| unmanaged_path_ok(path)) {
        let link = agent_root.join(path);
        if tokio::fs::symlink_metadata(&link).await.is_ok() {
            continue;
        }
        if let Some(parent) = link.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|error| workspace_error(format!("could not create {}: {error}", parent.display())))?;
        }
        let target = instance_root.join(path);
        tokio::fs::symlink(&target, &link)
            .await
            .map_err(|error| workspace_error(format!("could not link unmanaged path {path}: {error}")))?;
    }
    Ok(())
}

/// An unmanaged path is relative, every component normal, and not under
/// `.horde` — the Server validates the same, and this is the Worker's guard
/// against a confused one.
fn unmanaged_path_ok(path: &str) -> bool {
    let mut components = Path::new(path).components().peekable();
    components.peek().is_some()
        && !path.starts_with(".horde")
        && components.all(|component| matches!(component, std::path::Component::Normal(_)))
}

/// One agent worktree on `branch` at `base`, creating or adopting the
/// branch. Returns whether the branch was created.
async fn worktree_repo(
    instance_repo: &Path,
    destination: &Path,
    branch: &str,
    base: &str,
) -> Result<bool, WorkerError> {
    let destination = path_str(destination);
    let plan = base::plan_branch(instance_repo, branch, base)
        .await
        .map_err(WorkerError::Workspace)?;
    match plan {
        BranchPlan::Create => {
            git(instance_repo, &["worktree", "add", "-b", branch, &destination, base])
                .await
                .map_err(WorkerError::Workspace)?;
            Ok(true)
        }
        BranchPlan::Adopt => {
            git(instance_repo, &["branch", "-f", "--", branch, base])
                .await
                .map_err(WorkerError::Workspace)?;
            git(instance_repo, &["worktree", "add", &destination, branch])
                .await
                .map_err(WorkerError::Workspace)?;
            Ok(false)
        }
    }
}

/// One agent clone: full isolation, cloned from the Instance repo through
/// git's transport ([`NO_LOCAL`]), then switched onto `branch` at `base` —
/// fetched from the Instance first, since a fresh base lives under the
/// Instance's remote-tracking refs, which a clone does not carry.
async fn clone_repo(
    instance_repo: &Path,
    destination: &Path,
    branch: &str,
    base: &str,
) -> Result<(), WorkerError> {
    let destination_str = path_str(destination);
    git(
        instance_repo,
        &["clone", NO_LOCAL, "--", &path_str(instance_repo), &destination_str],
    )
    .await
    .map_err(WorkerError::Workspace)?;
    if commit_of(destination, base).await.is_none() {
        git(destination, &["fetch", "--no-tags", "origin", base])
            .await
            .map_err(WorkerError::Workspace)?;
    }
    git(destination, &["switch", "-c", branch, base])
        .await
        .map(|_| ())
        .map_err(WorkerError::Workspace)
}

/// The path a repo occupies inside its Instance, defaulted to its name.
///
/// # Errors
///
/// A layout that would land outside the Instance root — absolute, empty, or
/// climbing with `..` — is refused, not joined: `Path::join` with an
/// absolute path *replaces* the root, so an unchecked layout from a confused
/// or compromised Server would clone (and later delete) wherever it pointed.
pub(crate) fn layout_of(repo: &WorkspaceRepo) -> Result<&str, String> {
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

    /// An agent's private root is composed only from two directory names:
    /// either id unsafe is no path at all.
    #[test]
    fn an_agent_root_needs_both_ids_to_be_directory_names() {
        let manager = WorkspaceManager::new("w", "/root");
        assert_eq!(
            manager.agent_root("ws-1", "agent-1"),
            Some(PathBuf::from("/root/ws-1/.horde-agents/agent-1"))
        );
        assert_eq!(manager.agent_root("../x", "agent-1"), None);
        assert_eq!(manager.agent_root("ws-1", "../x"), None);
    }

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

    /// Only an exact agent-private checkout of this manager is one: the
    /// shared policy's Instance root, a foreign path, and a hostile shape
    /// are all left alone.
    #[test]
    fn only_an_agent_private_checkout_is_recognized() {
        let manager = WorkspaceManager::new("w", "/ws");
        assert_eq!(
            manager.agent_private(Path::new("/ws/def-1/.horde-agents/agent-1")),
            Some(("def-1".to_owned(), "agent-1".to_owned()))
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
                "{left_alone} is not an agent's checkout"
            );
        }
    }

    /// Initial repos imply LAZY; otherwise the spawn's mode wins over the
    /// Definition's, which wins over ALL.
    #[test]
    fn the_mode_resolves_spawn_then_definition_then_all() {
        let lazy_definition = WorkspaceDefinition {
            checkout_mode: CheckoutMode::Lazy.into(),
            ..WorkspaceDefinition::default()
        };
        let plain = WorkspaceDefinition::default();
        let request = CheckoutRequest::default();
        assert_eq!(resolve_mode(&request, &plain), CheckoutMode::All);
        assert_eq!(resolve_mode(&request, &lazy_definition), CheckoutMode::Lazy);
        let all = CheckoutRequest {
            mode: CheckoutMode::All,
            ..CheckoutRequest::default()
        };
        assert_eq!(resolve_mode(&all, &lazy_definition), CheckoutMode::All);
        let named = CheckoutRequest {
            mode: CheckoutMode::All,
            checkout_repos: vec!["atlas".to_owned()],
            ..CheckoutRequest::default()
        };
        assert_eq!(resolve_mode(&named, &plain), CheckoutMode::Lazy);
    }

    #[test]
    fn the_branch_defaults_to_the_agents_own() {
        let mut request = CheckoutRequest {
            agent_id: "agent-1".to_owned(),
            ..CheckoutRequest::default()
        };
        assert_eq!(branch_of(&request), "horde/agent-1");
        request.branch = "feat/login".to_owned();
        assert_eq!(branch_of(&request), "feat/login");
    }

    #[test]
    fn unknown_initial_repos_and_bases_are_refused_with_the_valid_names() {
        let definition = WorkspaceDefinition {
            name: "suite".to_owned(),
            repos: vec![WorkspaceRepo {
                name: "atlas".to_owned(),
                ..WorkspaceRepo::default()
            }],
            ..WorkspaceDefinition::default()
        };
        let fine = CheckoutRequest {
            checkout_repos: vec!["atlas".to_owned()],
            ..CheckoutRequest::default()
        };
        assert!(validate_names(&definition, &fine).is_ok());
        let unknown = CheckoutRequest {
            bases: vec![RepoBase {
                repo_name: "ghost".to_owned(),
                ..RepoBase::default()
            }],
            ..CheckoutRequest::default()
        };
        let error = validate_names(&definition, &unknown)
            .expect_err("ghost is unknown")
            .to_string();
        assert!(error.contains("ghost") && error.contains("valid repos: atlas"), "{error}");
    }

    #[test]
    fn an_unmanaged_path_stays_inside_and_off_the_horde_namespace() {
        assert!(unmanaged_path_ok("data"));
        assert!(unmanaged_path_ok("cache/weights"));
        assert!(!unmanaged_path_ok(""));
        assert!(!unmanaged_path_ok("/data"));
        assert!(!unmanaged_path_ok("../data"));
        assert!(!unmanaged_path_ok(".horde/x"));
        assert!(!unmanaged_path_ok("./data"));
    }

    #[test]
    fn names_for_modes_policies_and_resolutions_are_stable() {
        assert_eq!(mode_name(CheckoutMode::Lazy), "lazy");
        assert_eq!(mode_name(CheckoutMode::All), "all");
        assert_eq!(mode_name(CheckoutMode::Unspecified), "all");
        assert_eq!(policy_name(CheckoutPolicy::FreshClone), "fresh-clone");
        assert_eq!(policy_name(CheckoutPolicy::Worktree), "worktree");
        assert_eq!(policy_name(CheckoutPolicy::Unspecified), "worktree");
        for (resolution, name) in [
            (BaseResolution::Fresh, "fresh"),
            (BaseResolution::Cached, "cached"),
            (BaseResolution::InstanceHead, "instance-head"),
            (BaseResolution::Parent, "parent"),
            (BaseResolution::Requested, "requested"),
            (BaseResolution::Reused, "reused"),
            (BaseResolution::Unspecified, "unspecified"),
        ] {
            assert_eq!(resolution_name(resolution), name);
        }
    }

    #[test]
    fn a_timestamp_splits_milliseconds_into_seconds_and_nanos() {
        let stamp = timestamp(1_700_000_000_123);
        assert_eq!(stamp.seconds, 1_700_000_000);
        assert_eq!(stamp.nanos, 123_000_000);
    }
}
