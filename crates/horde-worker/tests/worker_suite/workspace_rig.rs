//! The Runtime rig the ADR-0014 Worker suites share: a Runtime over a
//! workspace root with a steerable disk, a recording reporter and an
//! in-memory journal, and the commands that drive it.
//!
//! Every reader here returns what production wrote (journaled events, the
//! reports the reporter was handed); nothing re-derives a predicate.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use horde_proto::v1::event::Kind;
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    CheckoutPolicy, ResolvedWorkspacePolicy, StreamCommandsResponse, WorkspaceCleanup,
    WorkspaceDefinition, WorkspaceInstanceReport,
};
use horde_worker::workspace::{FsStats, WorkspaceManager};
use horde_worker::{ApplyCommand as _, Journal, Runtime};

use crate::workspaces::harness;

/// The steered filesystem's size.
pub(crate) const TOTAL: u64 = 1 << 41;
/// Free space no watermark notices.
pub(crate) const PLENTY: u64 = 1 << 40;

/// A free-space source the test steers: queued readings are handed out
/// first, one per call, then the steady one.
#[derive(Debug)]
pub(crate) struct ScriptedDisk {
    queued: Mutex<VecDeque<u64>>,
    steady: AtomicU64,
}

impl ScriptedDisk {
    /// Readings handed out, one per call, before the steady one.
    pub(crate) fn queue(&self, readings: &[u64]) {
        if let Ok(mut queued) = self.queued.lock() {
            queued.extend(readings);
        }
    }

    /// The free space every later reading reports.
    pub(crate) fn set_free(&self, bytes: u64) {
        self.steady.store(bytes, Ordering::SeqCst);
    }
}

impl FsStats for ScriptedDisk {
    fn stats(&self, _path: &Path) -> Option<(u64, u64)> {
        let queued = self.queued.lock().ok().and_then(|mut q| q.pop_front());
        Some((
            queued.unwrap_or_else(|| self.steady.load(Ordering::SeqCst)),
            TOTAL,
        ))
    }
}

/// A reporter that records every report it is handed.
#[derive(Debug, Default)]
pub(crate) struct RecordingReporter {
    reports: Mutex<Vec<Vec<WorkspaceInstanceReport>>>,
}

#[async_trait::async_trait]
impl horde_worker::runtime::InstanceReporter for RecordingReporter {
    async fn report(&self, reports: Vec<WorkspaceInstanceReport>) {
        if let Ok(mut recorded) = self.reports.lock() {
            recorded.push(reports);
        }
    }
}

impl RecordingReporter {
    pub(crate) fn count(&self) -> usize {
        self.reports.lock().map(|r| r.len()).unwrap_or(0)
    }

    pub(crate) fn last(&self) -> Vec<WorkspaceInstanceReport> {
        self.reports
            .lock()
            .ok()
            .and_then(|reports| reports.last().cloned())
            .unwrap_or_default()
    }
}

/// A Runtime over a workspace root, a journal it shares, and a reporter.
pub(crate) struct Rig {
    pub(crate) runtime: Runtime,
    pub(crate) journal: Journal,
    pub(crate) reporter: Arc<RecordingReporter>,
    pub(crate) disk: Arc<ScriptedDisk>,
    pub(crate) root: PathBuf,
}

/// The scan is driven by hand unless a test starts the loop.
pub(crate) fn manual() -> horde_worker::runtime::HousekeepingConfig {
    horde_worker::runtime::HousekeepingConfig {
        scan_interval: Duration::from_secs(3600),
        ..horde_worker::runtime::HousekeepingConfig::default()
    }
}

pub(crate) fn rig_with(
    root: &Path,
    journal: Journal,
    config: horde_worker::runtime::HousekeepingConfig,
) -> Rig {
    let reporter = Arc::new(RecordingReporter::default());
    let disk = Arc::new(ScriptedDisk {
        queued: Mutex::new(VecDeque::new()),
        steady: AtomicU64::new(PLENTY),
    });
    let manager = WorkspaceManager::new("worker-a", root).with_fs_stats(disk.clone());
    let runtime = Runtime::new(journal.clone())
        .with_adapter(Arc::new(horde_adapter_fake::FakeAdapter::new(
            horde_adapter_fake::Script::echoing(),
        )))
        .with_workspaces(manager)
        .with_instance_reporter(reporter.clone())
        .with_housekeeping(config);
    Rig {
        runtime,
        journal,
        reporter,
        disk,
        root: root.to_path_buf(),
    }
}

/// A hand-driven rig over `root`.
pub(crate) fn rig(root: &Path, journal: Journal) -> Rig {
    rig_with(root, journal, manual())
}

/// A hand-driven rig over `<temp>/workspaces` with a fresh journal.
pub(crate) fn fresh_rig(temp: &Path) -> Rig {
    rig(
        &temp.join("workspaces"),
        Journal::in_memory().expect("journal"),
    )
}

/// A fresh rig with the three-repo (`a`, `b`, `c`) `ws-1` materialized.
pub(crate) async fn three_repo_rig(temp: &Path) -> Rig {
    let repos: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|name| harness::repo(name, &harness::origin(temp, name)))
        .collect();
    let mut rig = fresh_rig(temp);
    materialize(&mut rig, &harness::definition("ws-1", repos), policy(0, 0)).await;
    rig
}

/// Waits for workspace work — clones, fetches, worktree adds, reclaim
/// commands — under a ceiling sized for real git under a parallel suite's
/// load, while still turning a hang into a failure rather than a stuck run.
pub(crate) async fn settled_within<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(Duration::from_secs(120), future).await {
        Ok(value) => value,
        Err(_) => panic!("{what} must finish rather than hang"),
    }
}

/// Waits for something production writes once work already under way
/// finishes — a journaled event, a report, a removed directory — under a
/// ceiling that ends well inside a mutation run's per-mutant timeout: an
/// event that never comes fails its test instead of stalling the run.
pub(crate) async fn observed_within<F: std::future::Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(Duration::from_secs(30), future).await {
        Ok(value) => value,
        Err(_) => panic!("{what} was never observed"),
    }
}

pub(crate) fn wire(seconds: i64, nanos: i32) -> horde_proto::prost_types::Duration {
    horde_proto::prost_types::Duration { seconds, nanos }
}

pub(crate) fn policy(grace_s: i64, idle_s: i64) -> ResolvedWorkspacePolicy {
    ResolvedWorkspacePolicy {
        archived_grace: Some(wire(grace_s, 0)),
        idle_reclaim_after: Some(wire(idle_s, 0)),
        ..ResolvedWorkspacePolicy::default()
    }
}

pub(crate) fn command(id: &str, kind: Command) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: 0,
        command_id: id.to_owned(),
        log_position: 0,
        command: Some(kind),
    }
}

/// Materializes `definition` under the command id `m-<definition id>`.
pub(crate) async fn materialize(
    rig: &mut Rig,
    definition: &WorkspaceDefinition,
    policy: ResolvedWorkspacePolicy,
) {
    let id = definition
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    rig.runtime
        .apply(command(
            &format!("m-{id}"),
            Command::MaterializeWorkspace(horde_proto::v1::MaterializeWorkspaceCommand {
                definition: Some(definition.clone()),
                session_id: Some(horde_proto::v1::SessionId::new("s1")),
                policy: Some(policy),
            }),
        ))
        .await
        .expect("the materialize applies");
    settled_within("the materialize", rig.runtime.settled()).await;
}

pub(crate) async fn spawn_with(rig: &mut Rig, agent: &str, spec: horde_proto::v1::AgentSpec) {
    rig.runtime
        .apply(command(
            &format!("spawn-{agent}"),
            Command::SpawnAgent(horde_proto::v1::SpawnAgentCommand {
                session_id: Some(horde_proto::v1::SessionId::new("s1")),
                agent_id: Some(horde_proto::v1::AgentId::new(agent)),
                spec: Some(spec),
                ..horde_proto::v1::SpawnAgentCommand::default()
            }),
        ))
        .await
        .expect("the spawn applies");
    settled_within("the spawn", rig.runtime.settled()).await;
}

pub(crate) fn worktree_in(definition_id: &str) -> horde_proto::v1::AgentSpec {
    horde_proto::v1::AgentSpec {
        workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(definition_id)),
        checkout_policy: CheckoutPolicy::Worktree.into(),
        ..horde_proto::v1::AgentSpec::default()
    }
}

pub(crate) async fn spawn_in(rig: &mut Rig, definition_id: &str, agent: &str) {
    spawn_with(rig, agent, worktree_in(definition_id)).await;
}

pub(crate) async fn archive(rig: &mut Rig, agent: &str) {
    rig.runtime
        .apply(command(
            &format!("archive-{agent}"),
            Command::ArchiveAgent(horde_proto::v1::ArchiveAgentCommand {
                agent_id: Some(horde_proto::v1::AgentId::new(agent)),
                reason: "done".to_owned(),
            }),
        ))
        .await
        .expect("the archive applies");
    settled_within("the archive", rig.runtime.settled()).await;
}

/// Every journaled event, in journal order.
pub(crate) async fn events(journal: &Journal) -> Vec<horde_proto::v1::Event> {
    journal
        .read_from(1, 100_000)
        .await
        .expect("the journal reads")
        .into_iter()
        .filter_map(|journaled| journaled.event)
        .collect()
}

/// Every journaled event's kind, in journal order.
pub(crate) async fn kinds(journal: &Journal) -> Vec<Kind> {
    events(journal)
        .await
        .into_iter()
        .filter_map(|event| event.kind)
        .collect()
}

pub(crate) async fn cleanups(journal: &Journal) -> Vec<WorkspaceCleanup> {
    kinds(journal)
        .await
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::WorkspaceCleanup(cleanup) => Some(cleanup),
            _ => None,
        })
        .collect()
}
