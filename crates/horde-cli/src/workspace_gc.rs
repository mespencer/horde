//! `horde workspace gc`, `instance list|remove`, `log`, `delete --purge` and
//! `create --from-cwd` (ADR-0014).
//!
//! The cleanup verbs share one shape: the RPC appends a `WorkspaceGcRequested`
//! to the housekeeping stream and returns its position; the verb follows the
//! stream from there, renders each Worker's `WorkspaceCleanup` as a row, and
//! stops when every targeted Worker's `WorkspaceGcCompleted` has arrived or
//! the wait runs out. Exit status 0 means done everywhere with nothing
//! failed, 3 means some Worker has not answered (yet), 1 means refused or
//! failed.

use std::collections::BTreeSet;
use std::time::Duration;

use clap::{Args, Subcommand};
use horde_proto::v1::workspace_cleanup::repo_action::Outcome;
use horde_proto::v1::{
    CleanupTier, CleanupTrigger, CollectWorkspaceGarbageRequest, CreateWorkspaceDefinitionRequest,
    DeleteWorkspaceDefinitionRequest, GcLevel, ListWorkspaceInstancesRequest,
    RemoveWorkspaceInstanceRequest, WorkerId, WorkspaceCleanup, WorkspaceDefinitionId,
    WorkspaceGcCompleted, WorkspaceInstanceReport, event::Kind,
};
use serde_json::json;

use crate::client::{Client, ClientError, StreamUpdate, SyncOptions};
use crate::render::Format;
use crate::workspace_flags::{
    WorkspacePolicyArgs, apply_reclaim, cleanup_from, mode_for, parse_duration,
};

/// The heading every dry run's output starts with.
pub const DRY_RUN_HEADING: &str = "DRY RUN — nothing was changed";

/// The flags of `workspace gc`.
#[derive(Args, Debug, Clone, Default)]
pub struct GcArgs {
    /// Only this workspace's Instances (id or name); every Instance when
    /// omitted.
    pub workspace: Option<String>,
    /// Soft reclaim only: reclaim commands, nothing removed.
    #[arg(long, conflicts_with = "hard")]
    pub soft: bool,
    /// Also orphans, pins (with --include-pins) and retained checkouts —
    /// still only what is provably lossless unless --force.
    #[arg(long)]
    pub hard: bool,
    /// With --hard: remove what cannot be proven recoverable.
    #[arg(long, requires = "hard")]
    pub force: bool,
    /// Show what would happen and change nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// With --hard: also drop archive pins whose commits are pushed.
    #[arg(long, requires = "hard")]
    pub include_pins: bool,
    /// Only these Workers; repeatable.
    #[arg(long = "worker", value_name = "WORKER")]
    pub workers: Vec<String>,
    /// Only these agents' checkouts; repeatable.
    #[arg(long = "agent", value_name = "AGENT")]
    pub agents: Vec<String>,
    /// How long to wait for the Workers' answers, e.g. 2m.
    #[arg(long, default_value = "120s", value_name = "DUR")]
    pub wait: String,
}

impl GcArgs {
    /// The level the flags ask for: neither flag is AUTOMATIC.
    #[must_use]
    pub fn level(&self) -> GcLevel {
        match (self.soft, self.hard) {
            (true, _) => GcLevel::Soft,
            (_, true) => GcLevel::Hard,
            _ => GcLevel::Automatic,
        }
    }
}

/// The `workspace instance` verbs.
#[derive(Subcommand, Debug, Clone)]
pub enum InstanceCommand {
    /// The Instances Workers report, with checkouts and disk facts.
    List {
        /// Only this workspace (id or name).
        workspace: Option<String>,
        #[arg(long = "worker")]
        worker: Option<String>,
    },
    /// Remove one Instance from one Worker. Refused while any agent lives
    /// there.
    Remove {
        workspace: String,
        #[arg(long = "worker")]
        worker: String,
        #[arg(long)]
        force: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long, default_value = "120s", value_name = "DUR")]
        wait: String,
    },
}

/// The flags of `workspace log`.
#[derive(Args, Debug, Clone, Default)]
pub struct LogArgs {
    /// Only this workspace's events (id or name).
    pub workspace: Option<String>,
    #[arg(long = "worker")]
    pub worker: Option<String>,
    /// Keep reading as new events arrive.
    #[arg(long)]
    pub follow: bool,
}

/// What `workspace delete` was asked.
#[derive(Debug, Clone, Copy)]
pub struct DeleteArgs<'a> {
    pub workspace_definition_id: &'a str,
    pub etag: &'a str,
    pub purge: bool,
    pub force: bool,
    pub dry_run: bool,
    pub wait: &'a str,
}

/// The create request `workspace create` sends.
///
/// # Errors
///
/// A `--repo` or policy flag that does not parse, or `--from-cwd` outside a
/// git repo with an origin.
pub async fn create_request(
    name: Option<&str>,
    repos: &[String],
    from_cwd: bool,
    unmanaged_paths: &[String],
    policy: &WorkspacePolicyArgs,
) -> Result<CreateWorkspaceDefinitionRequest, ClientError> {
    let (name, mut repos) = if from_cwd {
        let (dir_name, repo) = repo_from_cwd().await?;
        (name.map_or(dir_name, str::to_owned), vec![repo])
    } else {
        let repos = repos
            .iter()
            .map(|value| crate::cli::repo_from(value))
            .collect::<Result<Vec<_>, _>>()?;
        (name.unwrap_or_default().to_owned(), repos)
    };
    apply_reclaim(&mut repos, policy)?;
    Ok(CreateWorkspaceDefinitionRequest {
        name,
        repos,
        unmanaged_paths: unmanaged_paths.to_vec(),
        checkout_mode: mode_for(policy.checkout_mode).into(),
        cleanup: cleanup_from(policy)?,
    })
}

/// The git repo the shell is in, as `(directory name, repo)`: its origin
/// URL and origin's default branch. The explicit single-repo shortcut.
async fn repo_from_cwd() -> Result<(String, horde_proto::v1::WorkspaceRepo), ClientError> {
    let refuse = |why: String| {
        ClientError::Rpc(tonic::Status::failed_precondition(format!(
            "--from-cwd needs a git work tree with an origin remote: {why}"
        )))
    };
    let root = git_here(&["rev-parse", "--show-toplevel"])
        .await
        .map_err(refuse)?;
    let url = git_here(&["remote", "get-url", "origin"])
        .await
        .map_err(refuse)?;
    let branch = git_here(&["symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
        .await
        .map(|head| head.trim_start_matches("origin/").to_owned())
        .unwrap_or_default();
    let dir_name = std::path::Path::new(&root)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("workspace")
        .to_owned();
    Ok((
        dir_name.clone(),
        horde_proto::v1::WorkspaceRepo {
            name: dir_name,
            remote_url: url,
            default_branch: branch,
            layout_path: String::new(),
            reclaim: None,
        },
    ))
}

async fn git_here(arguments: &[&str]) -> Result<String, String> {
    let output = tokio::process::Command::new("git")
        .args(arguments)
        .output()
        .await
        .map_err(|error| error.to_string())?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&output.stderr).trim().to_owned())
    }
}

/// A workspace reference (id or name) as its definition id.
async fn definition_id(client: &mut Client, reference: &str) -> Result<String, ClientError> {
    Ok(client
        .get_workspace_definition(reference)
        .await?
        .definition
        .and_then(|definition| definition.workspace_definition_id)
        .map(|id| id.to_string())
        .unwrap_or_else(|| reference.to_owned()))
}

/// Runs `workspace gc`.
///
/// # Errors
///
/// The RPC's refusal, [`ClientError::CleanupRefused`] when an action was
/// refused or failed, and [`ClientError::CleanupPending`] when a Worker has
/// not answered.
pub async fn run_gc(client: &mut Client, args: &GcArgs, format: Format) -> Result<(), ClientError> {
    let wait = parse_duration("--wait", &args.wait)?;
    let workspace = match &args.workspace {
        Some(reference) => Some(definition_id(client, reference).await?),
        None => None,
    };
    let response = client
        .collect_workspace_garbage(CollectWorkspaceGarbageRequest {
            workspace_definition_id: workspace.map(WorkspaceDefinitionId::new),
            worker_ids: args.workers.iter().map(WorkerId::new).collect(),
            agent_ids: args
                .agents
                .iter()
                .map(horde_proto::v1::AgentId::new)
                .collect(),
            level: args.level().into(),
            force: args.force,
            dry_run: args.dry_run,
            include_pins: args.include_pins,
        })
        .await?;
    let expected = Expected {
        request_id: response.request_id.clone(),
        targeted: names(&response.targeted),
        offline: names(&response.offline),
        unsupported: names(&response.unsupported),
    };
    await_answers(
        client,
        &expected,
        response.sequence,
        wait,
        args.dry_run,
        format,
    )
    .await
}

/// Runs `workspace instance list|remove`.
///
/// # Errors
///
/// The RPC's refusal, or a removal's refusal or incompleteness.
pub async fn run_instance(
    client: &mut Client,
    command: &InstanceCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        InstanceCommand::List { workspace, worker } => {
            list_instances(client, workspace.as_deref(), worker.as_deref(), format).await
        }
        InstanceCommand::Remove {
            workspace,
            worker,
            force,
            dry_run,
            wait,
        } => {
            let removal = Removal {
                workspace,
                worker,
                force: *force,
                dry_run: *dry_run,
                wait,
            };
            remove_instance(client, removal, format).await
        }
    }
}

/// What `instance remove` was asked.
#[derive(Debug, Clone, Copy)]
struct Removal<'a> {
    workspace: &'a str,
    worker: &'a str,
    force: bool,
    dry_run: bool,
    wait: &'a str,
}

async fn list_instances(
    client: &mut Client,
    workspace: Option<&str>,
    worker: Option<&str>,
    format: Format,
) -> Result<(), ClientError> {
    let workspace = match workspace {
        Some(reference) => Some(definition_id(client, reference).await?),
        None => None,
    };
    let instances = client
        .list_workspace_instances(ListWorkspaceInstancesRequest {
            worker_id: worker.map(WorkerId::new),
            workspace_definition_id: workspace.map(WorkspaceDefinitionId::new),
        })
        .await?;
    println!("{}", render_instances(&instances, format));
    Ok(())
}

async fn remove_instance(
    client: &mut Client,
    removal: Removal<'_>,
    format: Format,
) -> Result<(), ClientError> {
    let wait = parse_duration("--wait", removal.wait)?;
    let id = definition_id(client, removal.workspace).await?;
    let response = client
        .remove_workspace_instance(RemoveWorkspaceInstanceRequest {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(id)),
            worker_id: Some(WorkerId::new(removal.worker)),
            force: removal.force,
            dry_run: removal.dry_run,
        })
        .await?;
    let worker = vec![removal.worker.to_owned()];
    let (targeted, offline) = if response.targeted {
        (worker, Vec::new())
    } else {
        (Vec::new(), worker)
    };
    let expected = Expected {
        request_id: response.request_id,
        targeted,
        offline,
        unsupported: Vec::new(),
    };
    await_answers(
        client,
        &expected,
        response.sequence,
        wait,
        removal.dry_run,
        format,
    )
    .await
}

/// Runs `workspace delete`, with or without `--purge`.
///
/// # Errors
///
/// The RPC's refusal, or the purge's refusal or incompleteness.
pub async fn run_delete(
    client: &mut Client,
    args: DeleteArgs<'_>,
    format: Format,
) -> Result<(), ClientError> {
    let wait = parse_duration("--wait", args.wait)?;
    let response = client
        .delete_workspace_definition(DeleteWorkspaceDefinitionRequest {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(args.workspace_definition_id)),
            etag: args.etag.to_owned(),
            purge: args.purge,
            force: args.force,
            dry_run: args.dry_run,
        })
        .await?;
    if !args.dry_run {
        println!(
            "{}",
            crate::render::workspace_deleted(args.workspace_definition_id, format)
        );
    }
    if response.purge_request_id.is_empty() {
        return Ok(());
    }
    // Who holds Instances is the Server's to say; the completions name them.
    let instances = client
        .list_workspace_instances(ListWorkspaceInstancesRequest {
            worker_id: None,
            workspace_definition_id: Some(WorkspaceDefinitionId::new(args.workspace_definition_id)),
        })
        .await
        .unwrap_or_default();
    let mut targeted: Vec<String> = instances
        .iter()
        .filter_map(|report| report.worker_id.as_ref().map(ToString::to_string))
        .collect();
    targeted.sort();
    targeted.dedup();
    let expected = Expected {
        request_id: response.purge_request_id,
        targeted,
        offline: Vec::new(),
        unsupported: Vec::new(),
    };
    await_answers(
        client,
        &expected,
        response.purge_sequence,
        wait,
        args.dry_run,
        format,
    )
    .await
}

/// Runs `workspace log`: the housekeeping stream, filtered.
///
/// # Errors
///
/// The stream's failure.
pub async fn run_log(
    client: &mut Client,
    args: &LogArgs,
    format: Format,
) -> Result<(), ClientError> {
    let workspace = match &args.workspace {
        Some(reference) => Some(definition_id(client, reference).await?),
        None => None,
    };
    let mut stream = client
        .follow(
            &SyncOptions::new(horde_proto::WORKSPACE_HOUSEKEEPING_STREAM).following(args.follow),
        )
        .await?;
    while let Some(update) = stream.next_update().await? {
        match update {
            StreamUpdate::Event(envelope) => {
                let Some(kind) = envelope.event.and_then(|event| event.kind) else {
                    continue;
                };
                if let Some(line) =
                    log_line(&kind, workspace.as_deref(), args.worker.as_deref(), format)
                {
                    println!("{line}");
                }
            }
            StreamUpdate::CaughtUp { .. } if !args.follow => break,
            _ => {}
        }
    }
    Ok(())
}

/// What a cleanup verb waits for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Expected {
    pub request_id: String,
    pub targeted: Vec<String>,
    pub offline: Vec<String>,
    pub unsupported: Vec<String>,
}

fn names(ids: &[WorkerId]) -> Vec<String> {
    ids.iter().map(ToString::to_string).collect()
}

/// What the wait saw, for the verdict.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Answers {
    pub completed: BTreeSet<String>,
    pub failed_actions: u32,
    pub refused_actions: u32,
    pub errors: Vec<String>,
    pub bytes_freed: u64,
}

impl Answers {
    /// Folds one housekeeping event belonging to `request_id`, returning the
    /// row to print when it is a cleanup.
    pub fn fold(&mut self, request_id: &str, kind: &Kind, format: Format) -> Option<String> {
        match kind {
            Kind::WorkspaceCleanup(cleanup) if cleanup.gc_request_id == request_id => {
                for action in &cleanup.repos {
                    match Outcome::try_from(action.outcome) {
                        Ok(Outcome::Failed) => self.failed_actions += 1,
                        Ok(Outcome::Refused) => self.refused_actions += 1,
                        _ => {}
                    }
                }
                Some(cleanup_rows(cleanup, format))
            }
            Kind::WorkspaceGcCompleted(completed) if completed.request_id == request_id => {
                self.record_completion(completed);
                None
            }
            _ => None,
        }
    }

    fn record_completion(&mut self, completed: &WorkspaceGcCompleted) {
        let worker = completed
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !completed.error.is_empty() {
            self.errors.push(format!("{worker}: {}", completed.error));
        }
        self.bytes_freed = self.bytes_freed.saturating_add(completed.bytes_freed);
        self.completed.insert(worker);
    }

    /// The verdict: `Ok` when every targeted Worker answered and nothing
    /// was refused or failed.
    ///
    /// # Errors
    ///
    /// [`ClientError::CleanupRefused`] or [`ClientError::CleanupPending`].
    pub fn verdict(&self, expected: &Expected) -> Result<(), ClientError> {
        if self.failed_actions > 0 || self.refused_actions > 0 || !self.errors.is_empty() {
            let mut parts = Vec::new();
            if self.refused_actions > 0 {
                parts.push(format!("{} action(s) refused", self.refused_actions));
            }
            if self.failed_actions > 0 {
                parts.push(format!("{} action(s) failed", self.failed_actions));
            }
            parts.extend(self.errors.iter().cloned());
            return Err(ClientError::CleanupRefused(parts.join("; ")));
        }
        let pending: Vec<&String> = expected
            .targeted
            .iter()
            .filter(|worker| !self.completed.contains(*worker))
            .collect();
        let mut parts = Vec::new();
        if !pending.is_empty() {
            parts.push(format!("no answer yet from {}", join(&pending)));
        }
        if !expected.offline.is_empty() {
            parts.push(format!(
                "offline (they act on reconnect): {}",
                expected.offline.join(", ")
            ));
        }
        if !expected.unsupported.is_empty() {
            parts.push(format!(
                "cannot clean up (an older worker): {}",
                expected.unsupported.join(", ")
            ));
        }
        if parts.is_empty() {
            Ok(())
        } else {
            Err(ClientError::CleanupPending(parts.join("; ")))
        }
    }
}

fn join(workers: &[&String]) -> String {
    workers
        .iter()
        .map(|worker| worker.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Follows the housekeeping stream from `sequence` until every targeted
/// Worker answered or `wait` runs out, printing rows as they arrive.
async fn await_answers(
    client: &mut Client,
    expected: &Expected,
    sequence: u64,
    wait: Duration,
    dry_run: bool,
    format: Format,
) -> Result<(), ClientError> {
    if dry_run && format == Format::Human {
        println!("{DRY_RUN_HEADING}");
    }
    let mut answers = Answers::default();
    if !expected.targeted.is_empty() {
        let mut stream = client
            .follow(
                &SyncOptions::new(horde_proto::WORKSPACE_HOUSEKEEPING_STREAM)
                    .from_sequence(sequence)
                    .following(true),
            )
            .await?;
        let deadline = tokio::time::Instant::now() + wait;
        while expected
            .targeted
            .iter()
            .any(|worker| !answers.completed.contains(worker))
        {
            let next = tokio::time::timeout_at(deadline, stream.next_update()).await;
            let Ok(update) = next else {
                break;
            };
            let Some(update) = update? else {
                break;
            };
            if let StreamUpdate::Event(envelope) = update
                && let Some(kind) = envelope.event.and_then(|event| event.kind)
                && let Some(rows) = answers.fold(&expected.request_id, &kind, format)
            {
                println!("{rows}");
            }
        }
    }
    println!("{}", summary(expected, &answers, dry_run, format));
    answers.verdict(expected)
}

/// The closing summary of a cleanup verb.
#[must_use]
pub fn summary(expected: &Expected, answers: &Answers, dry_run: bool, format: Format) -> String {
    match format {
        Format::Json => json!({
            "type": "summary",
            "request_id": expected.request_id,
            "dry_run": dry_run,
            "targeted": expected.targeted,
            "completed": answers.completed,
            "offline": expected.offline,
            "unsupported": expected.unsupported,
            "refused_actions": answers.refused_actions,
            "failed_actions": answers.failed_actions,
            "bytes_freed": answers.bytes_freed,
        })
        .to_string(),
        Format::Human => format!(
            "{} of {} worker(s) answered; {} freed{}",
            answers.completed.len(),
            expected.targeted.len(),
            human_bytes(answers.bytes_freed),
            if dry_run { " (would be)" } else { "" }
        ),
    }
}

/// One cleanup event as rows: one per repo action (worker, agent, repo,
/// outcome, freed, reason), plus any unrecoverable files beneath it.
#[must_use]
pub fn cleanup_rows(cleanup: &WorkspaceCleanup, format: Format) -> String {
    let worker = cleanup
        .worker_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let agent = cleanup
        .agent_id
        .as_ref()
        .map_or_else(|| "-".to_owned(), ToString::to_string);
    let tier = tier_name(cleanup.tier);
    match format {
        Format::Json => json!({
            "type": "cleanup",
            "worker_id": worker,
            "workspace_definition_id": cleanup.workspace_definition_id.as_ref().map(ToString::to_string),
            "agent_id": cleanup.agent_id.as_ref().map(ToString::to_string),
            "session_id": cleanup.session_id.as_ref().map(ToString::to_string),
            "tier": tier,
            "trigger": trigger_name(cleanup.trigger),
            "dry_run": cleanup.dry_run,
            "repos": cleanup.repos.iter().map(|action| json!({
                "repo": action.repo_name,
                "outcome": outcome_name(action.outcome),
                "reason": action.reason,
                "argv": action.argv,
                "exit_code": action.exit_code,
                "bytes_freed": action.bytes_freed,
                "pinned_commit_sha": action.pinned_commit_sha,
                "unrecoverable_files": action.unrecoverable_files.iter().map(|file| json!({
                    "path": file.path,
                    "bytes": file.bytes,
                })).collect::<Vec<_>>(),
            })).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human => {
            let mut lines = Vec::new();
            for action in &cleanup.repos {
                let repo = if action.repo_name.is_empty() {
                    "-"
                } else {
                    action.repo_name.as_str()
                };
                let mut line = format!(
                    "{worker}  {agent}  {repo}  {tier} {}  {}",
                    outcome_name(action.outcome),
                    human_bytes(action.bytes_freed)
                );
                if !action.reason.is_empty() {
                    line.push_str(&format!("  {}", action.reason));
                }
                lines.push(line);
                for file in &action.unrecoverable_files {
                    lines.push(format!("    would lose {}  {}", file.path, human_bytes(file.bytes)));
                }
            }
            if lines.is_empty() {
                lines.push(format!("{worker}  {agent}  -  {tier} nothing to do"));
            }
            lines.join("\n")
        }
    }
}

/// A `checkout_prepared` event as JSON: per repo, where it started and how
/// the base was chosen.
#[must_use]
pub fn prepared_detail(prepared: &horde_proto::v1::CheckoutPrepared) -> serde_json::Value {
    json!({
        "mode": if prepared.mode == i32::from(horde_proto::v1::CheckoutMode::Lazy) { "lazy" } else { "all" },
        "available_repos": prepared.available_repos,
        "repos": prepared.repos.iter().map(|repo| json!({
            "repo": repo.repo_name,
            "branch": repo.branch,
            "base": repo.base_commit_sha,
            "resolution": resolution_name(repo.resolution),
            "requested_ref": repo.requested_ref,
            "stale_reason": repo.stale_reason,
        })).collect::<Vec<_>>(),
    })
}

/// A `checkout_prepared` event as one human line:
/// `atlas@9c1e2f3 fresh on horde/agent-7; docs@41aa… cached (fetch failed…)`.
#[must_use]
pub fn prepared_line(prepared: &horde_proto::v1::CheckoutPrepared) -> String {
    if prepared.repos.is_empty() {
        return format!(
            "lazy: nothing checked out of {}",
            prepared.available_repos.join(", ")
        );
    }
    prepared
        .repos
        .iter()
        .map(|repo| {
            let short: String = repo.base_commit_sha.chars().take(7).collect();
            let mut line = format!(
                "{}@{short} {} on {}",
                repo.repo_name,
                resolution_name(repo.resolution),
                repo.branch
            );
            if !repo.stale_reason.is_empty() {
                line.push_str(&format!(" ({})", repo.stale_reason));
            }
            line
        })
        .collect::<Vec<_>>()
        .join("; ")
}

#[must_use]
pub fn resolution_name(resolution: i32) -> &'static str {
    use horde_proto::v1::checkout_prepared::BaseResolution;
    match BaseResolution::try_from(resolution) {
        Ok(BaseResolution::Fresh) => "fresh",
        Ok(BaseResolution::Cached) => "cached",
        Ok(BaseResolution::InstanceHead) => "instance-head",
        Ok(BaseResolution::Parent) => "parent",
        Ok(BaseResolution::Requested) => "requested",
        Ok(BaseResolution::Reused) => "reused",
        Ok(BaseResolution::Unspecified) | Err(_) => "unknown",
    }
}

/// One `workspace log` line, when the event passes the filters.
#[must_use]
pub fn log_line(
    kind: &Kind,
    workspace: Option<&str>,
    worker: Option<&str>,
    format: Format,
) -> Option<String> {
    let matches = |event_workspace: Option<String>, event_worker: Option<String>| {
        workspace.is_none_or(|wanted| event_workspace.as_deref() == Some(wanted))
            && worker.is_none_or(|wanted| event_worker.as_deref() == Some(wanted))
    };
    match kind {
        Kind::WorkspaceCleanup(cleanup) => matches(
            cleanup
                .workspace_definition_id
                .as_ref()
                .map(ToString::to_string),
            cleanup.worker_id.as_ref().map(ToString::to_string),
        )
        .then(|| cleanup_rows(cleanup, format)),
        Kind::WorkspaceDiskPressure(pressure) => matches(
            workspace.map(str::to_owned),
            pressure.worker_id.as_ref().map(ToString::to_string),
        )
        .then(|| {
            let worker = pressure
                .worker_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            match format {
                Format::Json => json!({
                    "type": "disk_pressure",
                    "worker_id": worker,
                    "free_bytes": pressure.free_bytes,
                    "low_watermark_bytes": pressure.low_watermark_bytes,
                    "exhausted": pressure.exhausted,
                })
                .to_string(),
                Format::Human => format!(
                    "{worker}  disk pressure: {} free, below {}{}",
                    human_bytes(pressure.free_bytes),
                    human_bytes(pressure.low_watermark_bytes),
                    if pressure.exhausted {
                        "; nothing left to reclaim"
                    } else {
                        ""
                    }
                ),
            }
        }),
        Kind::WorkspaceGcRequested(requested) => matches(
            requested
                .workspace_definition_id
                .as_ref()
                .map(ToString::to_string),
            requested.worker_ids.first().map(ToString::to_string),
        )
        .then(|| match format {
            Format::Json => json!({
                "type": "gc_requested",
                "request_id": requested.request_id,
                "level": level_name(requested.level),
                "dry_run": requested.dry_run,
                "force": requested.force,
                "requested_by": requested.requested_by,
            })
            .to_string(),
            Format::Human => format!(
                "gc {} requested by {}: {}{}{}",
                requested.request_id,
                if requested.requested_by.is_empty() {
                    "-"
                } else {
                    &requested.requested_by
                },
                level_name(requested.level),
                if requested.force { " --force" } else { "" },
                if requested.dry_run { " --dry-run" } else { "" }
            ),
        }),
        _ => None,
    }
}

/// The Instance list: one block per Instance, a line per checkout.
#[must_use]
pub fn render_instances(instances: &[WorkspaceInstanceReport], format: Format) -> String {
    match format {
        Format::Json => json!({
            "instances": instances.iter().map(instance_json).collect::<Vec<_>>(),
        })
        .to_string(),
        Format::Human if instances.is_empty() => "no instances reported".to_owned(),
        Format::Human => instances
            .iter()
            .map(instance_lines)
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

/// One Instance as JSON, checkouts and disk facts included.
#[must_use]
pub fn instance_json(report: &WorkspaceInstanceReport) -> serde_json::Value {
    json!({
        "workspace_definition_id": report.workspace_definition_id.as_ref().map(ToString::to_string),
        "worker_id": report.worker_id.as_ref().map(ToString::to_string),
        "state": crate::render::instance_state(report.state),
        "root_path": report.root_path,
        "detail": report.detail,
        "instance_bytes": report.instance_bytes,
        "unmanaged_bytes": report.unmanaged_bytes,
        "filesystem_free_bytes": report.filesystem_free_bytes,
        "filesystem_total_bytes": report.filesystem_total_bytes,
        "checkouts": report.checkouts.iter().map(|checkout| json!({
            "agent_id": checkout.agent_id.as_ref().map(ToString::to_string),
            "session_id": checkout.session_id.as_ref().map(ToString::to_string),
            "repos": checkout.repos,
            "branch": checkout.branch,
            "state": checkout_state_name(checkout.state),
            "bytes": checkout.bytes,
            "soft_reclaimed": checkout.soft_reclaimed_at.is_some(),
            "retained_reason": checkout.retained_reason,
        })).collect::<Vec<_>>(),
    })
}

/// One Instance as human lines.
#[must_use]
pub fn instance_lines(report: &WorkspaceInstanceReport) -> String {
    let mut lines = vec![format!(
        "{} on {}: {}  {} ({} free of {})",
        report
            .workspace_definition_id
            .as_ref()
            .map_or_else(|| "-".to_owned(), ToString::to_string),
        report
            .worker_id
            .as_ref()
            .map_or_else(|| "-".to_owned(), ToString::to_string),
        crate::render::instance_state(report.state),
        human_bytes(report.instance_bytes),
        human_bytes(report.filesystem_free_bytes),
        human_bytes(report.filesystem_total_bytes),
    )];
    for checkout in &report.checkouts {
        let mut line = format!(
            "  {}  {}  {}  [{}]  {}",
            checkout
                .agent_id
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string),
            checkout_state_name(checkout.state),
            checkout.branch,
            checkout.repos.join(","),
            human_bytes(checkout.bytes)
        );
        if checkout.soft_reclaimed_at.is_some() {
            line.push_str("  reclaimed");
        }
        if !checkout.retained_reason.is_empty() {
            line.push_str(&format!("  retained: {}", checkout.retained_reason));
        }
        lines.push(line);
    }
    lines.join("\n")
}

/// Bytes in the largest unit that keeps a number ≥ 1.
#[must_use]
pub fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes;
    let mut unit = 0;
    while value >= 1024 && unit + 1 < UNITS.len() {
        value /= 1024;
        unit += 1;
    }
    format!("{value} {}", UNITS[unit])
}

#[must_use]
pub fn outcome_name(outcome: i32) -> &'static str {
    match Outcome::try_from(outcome) {
        Ok(Outcome::Done) => "done",
        Ok(Outcome::WouldDo) => "would-do",
        Ok(Outcome::Skipped) => "skipped",
        Ok(Outcome::Refused) => "refused",
        Ok(Outcome::Failed) => "failed",
        Ok(Outcome::Unspecified) | Err(_) => "unknown",
    }
}

#[must_use]
pub fn tier_name(tier: i32) -> &'static str {
    match CleanupTier::try_from(tier) {
        Ok(CleanupTier::Soft) => "soft",
        Ok(CleanupTier::Hard) => "hard",
        Ok(CleanupTier::Unpin) => "unpin",
        Ok(CleanupTier::Instance) => "instance",
        Ok(CleanupTier::Unspecified) | Err(_) => "unknown",
    }
}

#[must_use]
pub fn trigger_name(trigger: i32) -> &'static str {
    match CleanupTrigger::try_from(trigger) {
        Ok(CleanupTrigger::Archive) => "archive",
        Ok(CleanupTrigger::Idle) => "idle",
        Ok(CleanupTrigger::DiskPressure) => "disk-pressure",
        Ok(CleanupTrigger::Explicit) => "explicit",
        Ok(CleanupTrigger::Purge) => "purge",
        Ok(CleanupTrigger::Scan) => "scan",
        Ok(CleanupTrigger::Unspecified) | Err(_) => "unknown",
    }
}

#[must_use]
pub fn level_name(level: i32) -> &'static str {
    match GcLevel::try_from(level) {
        Ok(GcLevel::Soft) => "soft",
        Ok(GcLevel::Hard) => "hard",
        Ok(GcLevel::Automatic | GcLevel::Unspecified) | Err(_) => "automatic",
    }
}

#[must_use]
pub fn checkout_state_name(state: i32) -> &'static str {
    match horde_proto::v1::CheckoutState::try_from(state) {
        Ok(horde_proto::v1::CheckoutState::Active) => "active",
        Ok(horde_proto::v1::CheckoutState::Idle) => "idle",
        Ok(horde_proto::v1::CheckoutState::ArchivedRetained) => "archived-retained",
        Ok(horde_proto::v1::CheckoutState::Orphan) => "orphan",
        Ok(horde_proto::v1::CheckoutState::Unspecified) | Err(_) => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::workspace_cleanup::{RepoAction, RetainedFile};

    use super::*;

    #[test]
    fn the_level_is_soft_hard_or_automatic() {
        assert_eq!(GcArgs::default().level(), GcLevel::Automatic);
        let soft = GcArgs {
            soft: true,
            ..GcArgs::default()
        };
        assert_eq!(soft.level(), GcLevel::Soft);
        let hard = GcArgs {
            hard: true,
            ..GcArgs::default()
        };
        assert_eq!(hard.level(), GcLevel::Hard);
    }

    #[derive(clap::Parser, Debug)]
    struct Harness {
        #[command(flatten)]
        args: GcArgs,
    }

    /// `--force` without `--hard` is refused by the parser, as are soft and
    /// hard together.
    #[test]
    fn force_requires_hard_and_soft_excludes_hard() {
        use clap::Parser as _;
        assert!(Harness::try_parse_from(["gc", "--force"]).is_err());
        assert!(Harness::try_parse_from(["gc", "--include-pins"]).is_err());
        assert!(Harness::try_parse_from(["gc", "--soft", "--hard"]).is_err());
        let parsed = Harness::try_parse_from([
            "gc",
            "ws",
            "--hard",
            "--force",
            "--dry-run",
            "--worker",
            "w1",
        ])
        .expect("parses");
        assert!(parsed.args.force && parsed.args.dry_run);
        assert_eq!(parsed.args.workspace.as_deref(), Some("ws"));
        assert_eq!(parsed.args.workers, vec!["w1"]);
    }

    fn cleanup(request: &str, outcome: Outcome) -> Kind {
        Kind::WorkspaceCleanup(WorkspaceCleanup {
            worker_id: Some(WorkerId::new("w1")),
            agent_id: Some(horde_proto::v1::AgentId::new("agent-1")),
            tier: CleanupTier::Hard.into(),
            gc_request_id: request.to_owned(),
            repos: vec![RepoAction {
                repo_name: "atlas".to_owned(),
                outcome: outcome.into(),
                reason: "untracked files: 1".to_owned(),
                unrecoverable_files: vec![RetainedFile {
                    path: ".env".to_owned(),
                    bytes: 12,
                }],
                ..RepoAction::default()
            }],
            ..WorkspaceCleanup::default()
        })
    }

    fn completed(request: &str, worker: &str) -> Kind {
        Kind::WorkspaceGcCompleted(WorkspaceGcCompleted {
            request_id: request.to_owned(),
            worker_id: Some(WorkerId::new(worker)),
            bytes_freed: 2048,
            ..WorkspaceGcCompleted::default()
        })
    }

    fn expected(targeted: &[&str]) -> Expected {
        Expected {
            request_id: "gc-1".to_owned(),
            targeted: targeted.iter().map(|w| (*w).to_owned()).collect(),
            ..Expected::default()
        }
    }

    /// Every answer and nothing refused is success; another request's events
    /// are not this one's.
    #[test]
    fn the_verdict_is_ok_only_when_everyone_answered_cleanly() {
        let mut answers = Answers::default();
        assert!(
            answers
                .fold(
                    "gc-1",
                    &cleanup("gc-other", Outcome::Refused),
                    Format::Human
                )
                .is_none()
        );
        assert!(
            answers
                .fold("gc-1", &cleanup("gc-1", Outcome::Done), Format::Human)
                .is_some()
        );
        assert!(
            answers
                .fold("gc-1", &completed("gc-1", "w1"), Format::Human)
                .is_none()
        );
        assert!(answers.verdict(&expected(&["w1"])).is_ok());
        assert_eq!(answers.bytes_freed, 2048);
    }

    #[test]
    fn a_refusal_is_exit_one_and_a_missing_answer_is_exit_three() {
        let mut refused = Answers::default();
        refused.fold("gc-1", &cleanup("gc-1", Outcome::Refused), Format::Human);
        refused.fold("gc-1", &completed("gc-1", "w1"), Format::Human);
        let error = refused.verdict(&expected(&["w1"])).expect_err("refused");
        assert_eq!(error.exit_status(), 1);
        assert!(error.to_string().contains("1 action(s) refused"), "{error}");

        let pending = Answers::default();
        let error = pending.verdict(&expected(&["w1"])).expect_err("pending");
        assert_eq!(error.exit_status(), 3);
        assert!(
            error.to_string().contains("no answer yet from w1"),
            "{error}"
        );

        let offline = Expected {
            offline: vec!["w2".to_owned()],
            unsupported: vec!["w3".to_owned()],
            ..expected(&[])
        };
        let error = Answers::default()
            .verdict(&offline)
            .expect_err("incomplete");
        assert_eq!(error.exit_status(), 3);
        let text = error.to_string();
        assert!(
            text.contains("offline") && text.contains("w2") && text.contains("w3"),
            "{text}"
        );

        let mut failed = Answers::default();
        failed.fold("gc-1", &cleanup("gc-1", Outcome::Failed), Format::Human);
        assert!(
            failed
                .verdict(&expected(&[]))
                .is_err_and(|e| e.exit_status() == 1)
        );
        let mut errored = Answers::default();
        errored.fold(
            "gc-1",
            &Kind::WorkspaceGcCompleted(WorkspaceGcCompleted {
                request_id: "gc-1".to_owned(),
                worker_id: Some(WorkerId::new("w1")),
                error: "no workspace support".to_owned(),
                ..WorkspaceGcCompleted::default()
            }),
            Format::Human,
        );
        assert!(
            errored
                .verdict(&expected(&["w1"]))
                .is_err_and(|e| e.to_string().contains("no workspace support"))
        );
    }

    /// Another request's completion answers nothing of this one.
    #[test]
    fn a_completion_counts_only_for_its_own_request() {
        let mut answers = Answers::default();
        answers.fold("gc-1", &completed("gc-other", "w1"), Format::Human);
        assert!(answers.completed.is_empty(), "{answers:?}");
        assert_eq!(answers.bytes_freed, 0);
        assert!(
            answers
                .verdict(&expected(&["w1"]))
                .is_err_and(|e| e.exit_status() == 3)
        );
    }

    /// The refusal names only what happened: a refusal alone says nothing
    /// failed, a failure alone says nothing was refused, and both are named
    /// when both happened.
    #[test]
    fn a_refusal_message_names_exactly_the_kinds_that_happened() {
        let message = |outcomes: &[Outcome]| {
            let mut answers = Answers::default();
            for outcome in outcomes {
                answers.fold("gc-1", &cleanup("gc-1", *outcome), Format::Human);
            }
            answers
                .verdict(&expected(&[]))
                .expect_err("refused")
                .to_string()
        };
        let refused = message(&[Outcome::Refused]);
        assert!(refused.contains("1 action(s) refused"), "{refused}");
        assert!(!refused.contains("failed"), "{refused}");
        let failed = message(&[Outcome::Failed]);
        assert!(failed.contains("1 action(s) failed"), "{failed}");
        assert!(!failed.contains("refused"), "{failed}");
        let both = message(&[Outcome::Failed, Outcome::Refused, Outcome::Failed]);
        assert!(
            both.contains("1 action(s) refused; 2 action(s) failed"),
            "{both}"
        );
    }

    #[test]
    fn a_row_names_worker_agent_repo_outcome_and_what_would_be_lost() {
        let Kind::WorkspaceCleanup(event) = cleanup("gc-1", Outcome::Refused) else {
            panic!("a cleanup");
        };
        let human = cleanup_rows(&event, Format::Human);
        assert!(
            human.contains("w1  agent-1  atlas  hard refused"),
            "{human}"
        );
        assert!(human.contains("untracked files: 1"), "{human}");
        assert!(human.contains("would lose .env  12 B"), "{human}");
        let json: serde_json::Value =
            serde_json::from_str(&cleanup_rows(&event, Format::Json)).expect("json");
        assert_eq!(json["repos"][0]["outcome"], "refused");
        assert_eq!(json["repos"][0]["unrecoverable_files"][0]["path"], ".env");
        assert_eq!(json["tier"], "hard");
        let empty = WorkspaceCleanup {
            worker_id: Some(WorkerId::new("w1")),
            ..WorkspaceCleanup::default()
        };
        assert!(cleanup_rows(&empty, Format::Human).contains("nothing to do"));
    }

    #[test]
    fn the_summary_counts_answers_and_marks_a_dry_run() {
        let mut answers = Answers::default();
        answers.fold("gc-1", &completed("gc-1", "w1"), Format::Human);
        let human = summary(&expected(&["w1", "w2"]), &answers, true, Format::Human);
        assert_eq!(human, "1 of 2 worker(s) answered; 2 KiB freed (would be)");
        let json: serde_json::Value =
            serde_json::from_str(&summary(&expected(&["w1"]), &answers, false, Format::Json))
                .expect("json");
        assert_eq!(json["dry_run"], false);
        assert_eq!(json["bytes_freed"], 2048);
    }

    #[test]
    fn log_lines_filter_by_workspace_and_worker() {
        let kind = Kind::WorkspaceCleanup(WorkspaceCleanup {
            worker_id: Some(WorkerId::new("w1")),
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            ..WorkspaceCleanup::default()
        });
        assert!(log_line(&kind, None, None, Format::Human).is_some());
        assert!(log_line(&kind, Some("ws-1"), Some("w1"), Format::Human).is_some());
        assert!(log_line(&kind, Some("ws-2"), None, Format::Human).is_none());
        assert!(log_line(&kind, None, Some("w2"), Format::Human).is_none());
        let pressure = Kind::WorkspaceDiskPressure(horde_proto::v1::WorkspaceDiskPressure {
            worker_id: Some(WorkerId::new("w1")),
            free_bytes: 1024,
            low_watermark_bytes: 2048,
            exhausted: true,
        });
        let line = log_line(&pressure, None, None, Format::Human).expect("a line");
        assert!(line.contains("nothing left to reclaim"), "{line}");
        let requested = Kind::WorkspaceGcRequested(horde_proto::v1::WorkspaceGcRequested {
            request_id: "gc-9".to_owned(),
            level: GcLevel::Hard.into(),
            force: true,
            ..horde_proto::v1::WorkspaceGcRequested::default()
        });
        let line = log_line(&requested, None, None, Format::Human).expect("a line");
        assert!(
            line.contains("gc gc-9") && line.contains("hard --force"),
            "{line}"
        );
        assert!(
            log_line(
                &Kind::NoteSent(horde_proto::v1::NoteSent::default()),
                None,
                None,
                Format::Human
            )
            .is_none()
        );
    }

    #[test]
    fn instances_render_checkouts_and_disk_facts() {
        let report = WorkspaceInstanceReport {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            worker_id: Some(WorkerId::new("w1")),
            state: horde_proto::v1::WorkspaceInstanceState::Ready.into(),
            instance_bytes: 4096,
            filesystem_free_bytes: 1 << 30,
            filesystem_total_bytes: 1 << 31,
            checkouts: vec![horde_proto::v1::CheckoutReport {
                agent_id: Some(horde_proto::v1::AgentId::new("agent-1")),
                repos: vec!["atlas".to_owned()],
                branch: "horde/agent-1".to_owned(),
                state: horde_proto::v1::CheckoutState::Orphan.into(),
                retained_reason: "untracked files: 1".to_owned(),
                ..horde_proto::v1::CheckoutReport::default()
            }],
            ..WorkspaceInstanceReport::default()
        };
        let human = render_instances(std::slice::from_ref(&report), Format::Human);
        assert!(
            human.contains("ws-1 on w1: ready  4 KiB (1 GiB free of 2 GiB)"),
            "{human}"
        );
        assert!(
            human.contains("agent-1  orphan  horde/agent-1  [atlas]"),
            "{human}"
        );
        assert!(human.contains("retained: untracked files: 1"), "{human}");
        let json: serde_json::Value =
            serde_json::from_str(&render_instances(&[report], Format::Json)).expect("json");
        assert_eq!(json["instances"][0]["checkouts"][0]["state"], "orphan");
        assert_eq!(
            render_instances(&[], Format::Human),
            "no instances reported"
        );
    }

    #[test]
    fn a_checkout_prepared_event_renders_each_repos_start() {
        use horde_proto::v1::checkout_prepared::{BaseResolution, Repo};
        let prepared = horde_proto::v1::CheckoutPrepared {
            repos: vec![
                Repo {
                    repo_name: "atlas".to_owned(),
                    branch: "horde/a".to_owned(),
                    base_commit_sha: "9c1e2f3abcdef".to_owned(),
                    resolution: BaseResolution::Fresh.into(),
                    ..Repo::default()
                },
                Repo {
                    repo_name: "docs".to_owned(),
                    branch: "horde/a".to_owned(),
                    base_commit_sha: "41aa".to_owned(),
                    resolution: BaseResolution::Cached.into(),
                    stale_reason: "fetch failed".to_owned(),
                    ..Repo::default()
                },
            ],
            mode: horde_proto::v1::CheckoutMode::All.into(),
            available_repos: vec!["atlas".to_owned(), "docs".to_owned()],
        };
        assert_eq!(
            prepared_line(&prepared),
            "atlas@9c1e2f3 fresh on horde/a; docs@41aa cached on horde/a (fetch failed)"
        );
        let json = prepared_detail(&prepared);
        assert_eq!(json["repos"][1]["resolution"], "cached");
        assert_eq!(json["mode"], "all");
        let lazy = horde_proto::v1::CheckoutPrepared {
            mode: horde_proto::v1::CheckoutMode::Lazy.into(),
            available_repos: vec!["atlas".to_owned()],
            ..horde_proto::v1::CheckoutPrepared::default()
        };
        assert_eq!(prepared_line(&lazy), "lazy: nothing checked out of atlas");
        assert_eq!(prepared_detail(&lazy)["mode"], "lazy");
        for (resolution, name) in [
            (BaseResolution::InstanceHead, "instance-head"),
            (BaseResolution::Parent, "parent"),
            (BaseResolution::Requested, "requested"),
            (BaseResolution::Reused, "reused"),
            (BaseResolution::Unspecified, "unknown"),
        ] {
            assert_eq!(resolution_name(resolution.into()), name);
        }
    }

    #[test]
    fn names_cover_every_enum_value() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(1536), "1 KiB");
        assert_eq!(human_bytes(u64::MAX), "16777215 TiB");
        for (outcome, name) in [
            (Outcome::Done, "done"),
            (Outcome::WouldDo, "would-do"),
            (Outcome::Skipped, "skipped"),
            (Outcome::Refused, "refused"),
            (Outcome::Failed, "failed"),
            (Outcome::Unspecified, "unknown"),
        ] {
            assert_eq!(outcome_name(outcome.into()), name);
        }
        for (tier, name) in [
            (CleanupTier::Soft, "soft"),
            (CleanupTier::Hard, "hard"),
            (CleanupTier::Unpin, "unpin"),
            (CleanupTier::Instance, "instance"),
            (CleanupTier::Unspecified, "unknown"),
        ] {
            assert_eq!(tier_name(tier.into()), name);
        }
        for (trigger, name) in [
            (CleanupTrigger::Archive, "archive"),
            (CleanupTrigger::Idle, "idle"),
            (CleanupTrigger::DiskPressure, "disk-pressure"),
            (CleanupTrigger::Explicit, "explicit"),
            (CleanupTrigger::Purge, "purge"),
            (CleanupTrigger::Scan, "scan"),
            (CleanupTrigger::Unspecified, "unknown"),
        ] {
            assert_eq!(trigger_name(trigger.into()), name);
        }
        assert_eq!(level_name(GcLevel::Soft.into()), "soft");
        assert_eq!(level_name(GcLevel::Hard.into()), "hard");
        assert_eq!(level_name(GcLevel::Unspecified.into()), "automatic");
        for (state, name) in [
            (horde_proto::v1::CheckoutState::Active, "active"),
            (horde_proto::v1::CheckoutState::Idle, "idle"),
            (
                horde_proto::v1::CheckoutState::ArchivedRetained,
                "archived-retained",
            ),
            (horde_proto::v1::CheckoutState::Orphan, "orphan"),
            (horde_proto::v1::CheckoutState::Unspecified, "unknown"),
        ] {
            assert_eq!(checkout_state_name(state.into()), name);
        }
    }
}
