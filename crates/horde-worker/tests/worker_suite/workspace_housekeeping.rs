//! ADR-0014 housekeeping on the Worker, at its edges: gc scoped to one
//! Definition and to idle agents, the reports' live states, the disk
//! pressure watermarks and episode, idle and grace boundaries, the scan
//! loop's pin pass and report cadence, materialization progress, a lazy
//! receiver's pending commits, and the daemon's report to a real Server.
//!
//! Every assertion reads what production wrote — git refs, files, journaled
//! events, the reports a recording reporter (or the Server) received.

use std::path::{Path, PathBuf};
use std::time::Duration;

use horde_proto::v1::event::Kind;
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    CheckoutMode, CheckoutState, CleanupTier, CleanupTrigger, GcLevel, ResolvedWorkspacePolicy,
    WorkspaceCleanup, WorkspaceDefinition, WorkspaceInstanceReport,
};
use horde_worker::workspace::WorkspaceManager;
use horde_worker::{ApplyCommand as _, Journal};

use crate::workspace_rig::{
    Rig, TOTAL, archive, cleanups, command, events, fresh_rig, kinds, materialize, observed_within,
    policy, rig_with, settled_within, spawn_in, spawn_with, three_repo_rig, wire, worktree_in,
};
use crate::workspaces::harness;

/// Polls `holds` every 20 ms for up to `limit`, answering whether it held.
async fn eventually<F, Fut>(limit: Duration, mut holds: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + limit;
    while tokio::time::Instant::now() < deadline {
        if holds().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    holds().await
}

fn definition_named(temp: &Path, id: &str, repo: &str) -> WorkspaceDefinition {
    let origin = harness::origin(temp, repo);
    harness::definition(id, vec![harness::repo(repo, &origin)])
}

/// A fresh rig with the one-repo `ws-1` (`atlas`) materialized under
/// `policy`.
async fn atlas_rig(temp: &Path, policy: ResolvedWorkspacePolicy) -> Rig {
    let mut rig = fresh_rig(temp);
    materialize(&mut rig, &definition_named(temp, "ws-1", "atlas"), policy).await;
    rig
}

/// [`atlas_rig`] under a day's grace and no idle reclaim, with `agent`
/// spawned in it.
async fn atlas_rig_with(temp: &Path, agent: &str) -> Rig {
    let mut rig = atlas_rig(temp, policy(86_400, 0)).await;
    spawn_in(&mut rig, "ws-1", agent).await;
    rig
}

/// A rig whose scan loop ticks every `scan_interval`, runs its pin pass on
/// every tick and reports hourly, with `ws-1` materialized.
async fn looping_rig(temp: &Path, scan_interval: Duration) -> Rig {
    let config = horde_worker::runtime::HousekeepingConfig {
        scan_interval,
        pin_scan_interval: Duration::ZERO,
        report_interval: Duration::from_secs(3600),
        ..horde_worker::runtime::HousekeepingConfig::default()
    };
    let mut rig = rig_with(
        &temp.join("workspaces"),
        Journal::in_memory().expect("journal"),
        config,
    );
    materialize(
        &mut rig,
        &definition_named(temp, "ws-1", "atlas"),
        policy(0, 0),
    )
    .await;
    rig
}

async fn gc(rig: &mut Rig, gc: horde_proto::v1::WorkspaceGcCommand) {
    let id = gc.request_id.clone();
    rig.runtime
        .apply(command(&id, Command::WorkspaceGc(gc)))
        .await
        .expect("the gc applies");
    settled_within("the gc", rig.runtime.settled()).await;
}

fn agent_of(cleanup: &WorkspaceCleanup) -> String {
    cleanup
        .agent_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

/// Waits until `count` archive-triggered events for `agent` are journaled:
/// the archive's release runs after the archive command completed.
async fn released(rig: &Rig, agent: &str, count: usize) {
    observed_within("the archive's release", async {
        while cleanups(&rig.journal)
            .await
            .iter()
            .filter(|c| c.trigger == i32::from(CleanupTrigger::Archive) && agent_of(c) == agent)
            .count()
            < count
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

/// Archives `agent` and waits for its release to finish: `count` archive
/// events, then the report the release sends last, still holding the
/// checkout's lock — which a scan skips rather than waits for.
async fn archived_and_released(rig: &mut Rig, agent: &str, count: usize) {
    let before = rig.reporter.count();
    archive(rig, agent).await;
    released(rig, agent, count).await;
    observed_within("the release's report", async {
        while rig.reporter.count() <= before {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

fn agent_root(rig: &Rig, definition_id: &str, agent: &str) -> PathBuf {
    rig.root
        .join(definition_id)
        .join(".horde-agents")
        .join(agent)
}

fn manager(rig: &Rig) -> &WorkspaceManager {
    rig.runtime.workspaces().expect("workspaces are enabled")
}

/// A gc names its Worker in its completion and touches only the Definition
/// it names: the other Instance's archived checkout survives a hard gc.
#[tokio::test]
async fn a_gc_for_one_definition_leaves_the_others_and_names_its_worker() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = fresh_rig(temp.path());
    for (id, repo) in [("ws-1", "atlas"), ("ws-2", "docs")] {
        materialize(
            &mut rig,
            &definition_named(temp.path(), id, repo),
            policy(86_400, 0),
        )
        .await;
    }
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    spawn_in(&mut rig, "ws-2", "agent-2").await;
    archive(&mut rig, "agent-1").await;
    archive(&mut rig, "agent-2").await;
    released(&rig, "agent-1", 1).await;
    released(&rig, "agent-2", 1).await;

    gc(
        &mut rig,
        horde_proto::v1::WorkspaceGcCommand {
            request_id: "gc-ws-1".to_owned(),
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            level: GcLevel::Hard.into(),
            ..horde_proto::v1::WorkspaceGcCommand::default()
        },
    )
    .await;
    assert!(
        !agent_root(&rig, "ws-1", "agent-1").exists(),
        "ws-1 is collected"
    );
    assert!(
        agent_root(&rig, "ws-2", "agent-2").exists(),
        "ws-2 was not named"
    );
    let touched: Vec<String> = cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| c.gc_request_id == "gc-ws-1")
        .map(agent_of)
        .collect();
    assert!(
        !touched.is_empty() && touched.iter().all(|a| a == "agent-1"),
        "{touched:?}"
    );
    let completed: Vec<String> = kinds(&rig.journal)
        .await
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::WorkspaceGcCompleted(c) if c.request_id == "gc-ws-1" => {
                c.worker_id.map(|id| id.to_string())
            }
            _ => None,
        })
        .collect();
    assert_eq!(completed, vec!["worker-a"]);
    rig.runtime.shutdown().await;
}

/// A soft gc reclaims an idle live agent's checkout and skips one with a
/// turn in flight.
#[tokio::test]
async fn a_soft_gc_reclaims_idle_agents_and_never_a_busy_one() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig_with(temp.path(), "agent-busy").await;
    spawn_in(&mut rig, "ws-1", "agent-idle").await;
    rig.runtime.activity().set_busy("agent-busy", true);

    gc(
        &mut rig,
        horde_proto::v1::WorkspaceGcCommand {
            request_id: "gc-soft".to_owned(),
            level: GcLevel::Soft.into(),
            ..horde_proto::v1::WorkspaceGcCommand::default()
        },
    )
    .await;
    let reclaimed: Vec<String> = cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| c.gc_request_id == "gc-soft" && c.tier == i32::from(CleanupTier::Soft))
        .map(agent_of)
        .collect();
    assert_eq!(reclaimed, vec!["agent-idle"]);
    rig.runtime.shutdown().await;
}

/// A live agent seen between turns, never reclaimed for idleness, is
/// reported ACTIVE, not IDLE.
#[tokio::test]
async fn a_live_agent_between_turns_is_reported_active() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = atlas_rig_with(temp.path(), "agent-1").await;
    rig.runtime.activity().set_busy("agent-1", false);
    rig.runtime.scan_workspaces().await;
    assert_eq!(
        rig.reporter.last()[0].checkouts[0].state,
        i32::from(CheckoutState::Active)
    );
    rig.runtime.shutdown().await;
}

fn pressure_events(events: &[WorkspaceCleanup]) -> usize {
    events
        .iter()
        .filter(|c| c.trigger == i32::from(CleanupTrigger::DiskPressure))
        .count()
}

/// Free space under the high watermark but over the low one starts no
/// episode: nothing is reclaimed for pressure.
#[tokio::test]
async fn free_space_between_the_watermarks_starts_no_episode() {
    let temp = tempfile::tempdir().expect("a tempdir");
    // An archived checkout inside its grace: an episode would remove it
    // whatever the agent's activity, so whether one starts is observable.
    let mut rig = atlas_rig_with(temp.path(), "agent-1").await;
    archived_and_released(&mut rig, "agent-1", 1).await;
    let config = horde_worker::runtime::HousekeepingConfig::default();
    let low = config.disk_low_watermark.bytes_of(TOTAL);
    let high = config.disk_high_watermark.bytes_of(TOTAL);
    assert!(low < high);
    rig.disk.set_free(low + (high - low) / 2);

    assert!(!rig.runtime.scan_workspaces().await);
    assert_eq!(pressure_events(&cleanups(&rig.journal).await), 0);
    assert!(
        agent_root(&rig, "ws-1", "agent-1").exists(),
        "the archived checkout stays through its grace"
    );
    rig.runtime.shutdown().await;
}

/// An episode started under the low watermark keeps going until the high
/// one: free space between them still reclaims, here an archived
/// checkout that appeared after the episode began.
#[tokio::test]
async fn an_episode_keeps_reclaiming_between_the_watermarks() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig(temp.path(), policy(86_400, 0)).await;
    let config = horde_worker::runtime::HousekeepingConfig::default();
    let low = config.disk_low_watermark.bytes_of(TOTAL);
    let high = config.disk_high_watermark.bytes_of(TOTAL);
    rig.disk.set_free(low - 1);
    rig.runtime.scan_workspaces().await;

    spawn_in(&mut rig, "ws-1", "agent-1").await;
    archived_and_released(&mut rig, "agent-1", 1).await;
    assert!(
        agent_root(&rig, "ws-1", "agent-1").exists(),
        "kept through its grace by the archive"
    );
    rig.disk.set_free(low + (high - low) / 2);
    assert!(rig.runtime.scan_workspaces().await);
    let removed: Vec<String> = cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| {
            c.trigger == i32::from(CleanupTrigger::DiskPressure)
                && c.tier == i32::from(CleanupTier::Hard)
        })
        .map(agent_of)
        .collect();
    assert_eq!(removed, vec!["agent-1"]);
    assert!(!agent_root(&rig, "ws-1", "agent-1").exists());
    rig.runtime.shutdown().await;
}

/// An episode stops reclaiming the moment free space reaches the high
/// watermark: relieved before the first candidate, nothing is touched.
#[tokio::test]
async fn pressure_relieved_before_a_candidate_reclaims_nothing() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = atlas_rig_with(temp.path(), "agent-1").await;
    // One reading under the low watermark starts the episode; every later
    // reading is plenty.
    rig.disk.queue(&[1]);
    rig.runtime.scan_workspaces().await;
    assert_eq!(pressure_events(&cleanups(&rig.journal).await), 0);
    rig.runtime.shutdown().await;
}

/// Exhaustion is free space still *under* the low watermark: an episode
/// ending exactly on it reports nothing.
#[tokio::test]
async fn ending_an_episode_exactly_at_the_low_watermark_is_not_exhaustion() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = atlas_rig(temp.path(), policy(86_400, 0)).await;
    let low = horde_worker::runtime::HousekeepingConfig::default()
        .disk_low_watermark
        .bytes_of(TOTAL);
    rig.disk.set_free(low);
    rig.disk.queue(&[1]);
    rig.runtime.scan_workspaces().await;
    let exhausted = kinds(&rig.journal)
        .await
        .into_iter()
        .filter(|kind| matches!(kind, Kind::WorkspaceDiskPressure(_)))
        .count();
    assert_eq!(exhausted, 0);
    rig.runtime.shutdown().await;
}

/// Disk pressure removes archived checkouts only: an orphan (left by a
/// reset) is soft-reclaimed at most, never removed by a scan.
#[tokio::test]
async fn pressure_never_removes_an_orphan() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let definition = definition_named(temp.path(), "ws-1", "atlas");
    let root = temp.path().join("workspaces");
    let mut first = crate::workspace_rig::rig(&root, Journal::in_memory().expect("journal"));
    materialize(&mut first, &definition, policy(0, 0)).await;
    spawn_in(&mut first, "ws-1", "agent-1").await;
    first.runtime.shutdown().await;

    let reset = crate::workspace_rig::rig(&root, Journal::in_memory().expect("a fresh journal"));
    reset.disk.set_free(1);
    reset.runtime.scan_workspaces().await;
    assert!(
        agent_root(&reset, "ws-1", "agent-1").exists(),
        "an orphan survives pressure"
    );
    assert!(
        cleanups(&reset.journal)
            .await
            .iter()
            .all(|c| c.tier != i32::from(CleanupTier::Hard)),
        "and no removal was even attempted"
    );
    reset.runtime.shutdown().await;
}

fn idle_events(events: &[WorkspaceCleanup]) -> usize {
    events
        .iter()
        .filter(|c| c.trigger == i32::from(CleanupTrigger::Idle))
        .count()
}

/// An agent active moments ago is not idle under an hour's policy.
#[tokio::test]
async fn a_recently_active_agent_is_not_reclaimed_for_idleness() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig(temp.path(), policy(86_400, 3_600)).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    assert!(!rig.runtime.scan_workspaces().await);
    assert_eq!(idle_events(&cleanups(&rig.journal).await), 0);
    rig.runtime.shutdown().await;
}

/// The idle time is a lower bound reached, not exceeded: under a policy of
/// less than a millisecond, an agent whose last activity is this very
/// millisecond (here: recorded ahead of the clock, so no time has passed)
/// is already idle.
#[tokio::test]
async fn an_idle_time_under_a_millisecond_is_reached_at_once() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = fresh_rig(temp.path());
    let sub_ms = ResolvedWorkspacePolicy {
        archived_grace: Some(wire(86_400, 0)),
        idle_reclaim_after: Some(wire(0, 1)),
        ..ResolvedWorkspacePolicy::default()
    };
    materialize(
        &mut rig,
        &definition_named(temp.path(), "ws-1", "atlas"),
        sub_ms,
    )
    .await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    horde_worker::workspace::cleanup::note_activity(
        manager(&rig),
        "ws-1",
        "agent-1",
        horde_worker::workspace::ledger::now_ms() + 3_600_000,
    )
    .await;
    assert!(rig.runtime.scan_workspaces().await);
    assert_eq!(idle_events(&cleanups(&rig.journal).await), 1);
    rig.runtime.shutdown().await;
}

/// A retained archived checkout costs one event, not one per scan: the
/// scan's repeated refusal, for the same reason, journals nothing.
#[tokio::test]
async fn a_retained_checkout_is_not_rejournaled_for_the_same_reason() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig(temp.path(), policy(0, 0)).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    std::fs::write(
        agent_root(&rig, "ws-1", "agent-1").join("atlas/wip.txt"),
        "wip",
    )
    .expect("dirty");
    archived_and_released(&mut rig, "agent-1", 2).await;
    let records = manager(&rig).records("ws-1").await;
    assert!(
        records
            .iter()
            .any(|r| r.agent_id == "agent-1" && !r.retained_reason.is_empty()),
        "the refusal is remembered: {records:?}"
    );
    let before = cleanups(&rig.journal).await.len();

    assert!(!rig.runtime.scan_workspaces().await);
    assert_eq!(cleanups(&rig.journal).await.len(), before);
    assert!(agent_root(&rig, "ws-1", "agent-1").exists());
    rig.runtime.shutdown().await;
}

/// The grace is reached, not exceeded: a checkout archived at the start of
/// time is past even the longest grace, where the elapsed time and the
/// grace both saturate to the same bound.
#[tokio::test]
async fn the_longest_grace_is_reached_by_the_oldest_archive() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig(temp.path(), policy(i64::MAX, 0)).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    archived_and_released(&mut rig, "agent-1", 1).await;
    assert!(
        agent_root(&rig, "ws-1", "agent-1").exists(),
        "inside its grace"
    );

    let record = rig.root.join("ws-1/.horde-agents/.ledger/agent-1.json");
    let mut json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&record).expect("the ledger record")).expect("json");
    json["archived_at_ms"] = serde_json::Value::from(i64::MIN);
    std::fs::write(&record, serde_json::to_vec(&json).expect("encodes")).expect("rewrites");

    // The archive's release may still hold the checkout's lock, which a
    // scan skips rather than waits for: scan until one acts.
    observed_within("the scan past the grace", async {
        while !rig.runtime.scan_workspaces().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        !agent_root(&rig, "ws-1", "agent-1").exists(),
        "removed: the grace is over"
    );
    rig.runtime.shutdown().await;
}

/// The scan loop, started with the Runtime's first replay, drops a pin
/// origin holds on its pin pass, journals that, and reports the change at
/// once — not at the next report interval.
#[tokio::test]
async fn the_scan_loop_drops_safe_pins_and_reports_the_change() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = looping_rig(temp.path(), Duration::from_millis(50)).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    // Everything up to the spawn has reported; the archive's release
    // reports once more, after it journals.
    let before_archive = rig.reporter.count();
    archive(&mut rig, "agent-1").await;
    released(&rig, "agent-1", 2).await;
    let instance_repo = rig.root.join("ws-1/atlas");
    let pins = || {
        harness::git(
            &instance_repo,
            &["for-each-ref", "--format=%(refname)", "refs/horde/archived"],
        )
    };
    assert!(pins().contains("agent-1"), "the removal pinned HEAD");
    // The archive's own report, then a quiet baseline: nothing else
    // reports until the scan loop starts.
    assert!(
        eventually(Duration::from_secs(30), || async {
            rig.reporter.count() > before_archive
                && rig
                    .reporter
                    .last()
                    .first()
                    .is_some_and(|r| r.checkouts.is_empty())
        })
        .await
    );
    let baseline = rig.reporter.count();

    rig.runtime.replay_inbox().await.expect("the replay runs");
    let journal = rig.journal.clone();
    assert!(
        eventually(Duration::from_secs(30), || {
            let journal = journal.clone();
            async move {
                cleanups(&journal).await.iter().any(|c| {
                    c.tier == i32::from(CleanupTier::Unpin)
                        && c.trigger == i32::from(CleanupTrigger::Scan)
                        && c.repos.iter().any(|a| {
                            a.outcome
                                == i32::from(
                                    horde_proto::v1::workspace_cleanup::repo_action::Outcome::Done,
                                )
                        })
                })
            }
        })
        .await,
        "the pin pass journals the dropped pin"
    );
    assert!(!pins().contains("agent-1"), "and the pin is gone");
    assert!(
        eventually(Duration::from_secs(10), || async {
            rig.reporter.count() > baseline
        })
        .await,
        "the change is reported without waiting an hour"
    );
    rig.runtime.shutdown().await;
}

/// A scan loop with nothing to do reports nothing between its report
/// intervals, however often it ticks and runs its pin pass.
#[tokio::test]
async fn a_quiet_scan_loop_does_not_report() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = looping_rig(temp.path(), Duration::from_millis(20)).await;
    let baseline = rig.reporter.count();
    rig.runtime.replay_inbox().await.expect("the replay runs");
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(rig.reporter.count(), baseline, "nothing changed");
    rig.runtime.shutdown().await;
}

/// Materialization progress names its Instance, says MATERIALIZING, and
/// gives each step its own journal key.
#[tokio::test]
async fn materialization_progress_names_its_instance_under_distinct_keys() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = three_repo_rig(temp.path()).await;
    let progress: Vec<(String, WorkspaceInstanceReport)> = events(&rig.journal)
        .await
        .into_iter()
        .filter_map(|event| match event.kind {
            Some(Kind::WorkspaceInstanceChanged(changed)) if changed.progress.is_some() => {
                Some((event.idempotency_key, changed.report.unwrap_or_default()))
            }
            _ => None,
        })
        .collect();
    let keys: Vec<&str> = progress.iter().map(|(key, _)| key.as_str()).collect();
    assert_eq!(
        keys,
        vec![
            "m-ws-1:progress:0",
            "m-ws-1:progress:1",
            "m-ws-1:progress:2"
        ]
    );
    for (_, report) in &progress {
        assert_eq!(
            report
                .workspace_definition_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("ws-1")
        );
        assert_eq!(
            report.state,
            i32::from(horde_proto::v1::WorkspaceInstanceState::Materializing)
        );
    }
    rig.runtime.shutdown().await;
}

fn head(repo: &Path) -> String {
    harness::git(repo, &["rev-parse", "HEAD"]).trim().to_owned()
}

/// A lazy receiver's payload: the commit for a repo it has checked out
/// goes to its checkout as usual, while one for a repo it has not is made
/// present in the Instance and listed as pending — for that repo only.
#[tokio::test]
async fn a_lazy_receiver_holds_commits_for_unchecked_repos_as_pending() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (mut definition, _atlas, _docs) = harness::two_repo_definition(temp.path());
    definition.checkout_mode = CheckoutMode::Lazy.into();
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, policy(86_400, 0)).await;
    spawn_with(
        &mut rig,
        "agent-1",
        horde_proto::v1::AgentSpec {
            checkout_repos: vec!["atlas".to_owned()],
            ..worktree_in("ws-1")
        },
    )
    .await;
    let atlas_head = head(&rig.root.join("ws-1/atlas"));
    let docs_head = head(&rig.root.join("ws-1/docs"));
    let git_ref = |repo: &str, sha: &str| horde_proto::v1::GitRef {
        repo_name: repo.to_owned(),
        commit_sha: sha.to_owned(),
        branch: String::new(),
    };
    rig.runtime
        .apply(command(
            "deliver-1",
            Command::DeliverMessage(horde_proto::v1::DeliverMessageCommand {
                agent_id: Some(horde_proto::v1::AgentId::new("agent-1")),
                body: Some(horde_proto::v1::deliver_message_command::Body::Assignment(
                    horde_proto::v1::Assignment {
                        assignment_id: None,
                        text: "look at these".to_owned(),
                        payload: Some(horde_proto::v1::Payload {
                            git_refs: vec![
                                git_ref("atlas", &atlas_head),
                                git_ref("docs", &docs_head),
                            ],
                            attachments: Vec::new(),
                        }),
                    },
                )),
                delivery: horde_proto::v1::DeliveryMode::TurnBoundary.into(),
            }),
        ))
        .await
        .expect("the delivery applies");
    settled_within("the delivery", rig.runtime.settled()).await;

    let manifest = horde_worker::workspace::manifest::read(&agent_root(&rig, "ws-1", "agent-1"))
        .await
        .expect("a manifest");
    let pending: Vec<(String, Vec<String>)> = manifest
        .repos
        .iter()
        .map(|repo| (repo.name.clone(), repo.pending_commits.clone()))
        .collect();
    assert_eq!(
        pending,
        vec![
            ("atlas".to_owned(), Vec::new()),
            ("docs".to_owned(), vec![docs_head])
        ]
    );
    rig.runtime.shutdown().await;
}

/// The daemon reports its Instances to the Server it dials, on its report
/// interval and without any command: an Instance that appears on disk
/// after registration (which advertised only the first) reaches the
/// Server by the report alone.
#[tokio::test]
async fn the_daemon_reports_its_instances_to_the_server() {
    use horde_server::EventLog;
    use horde_server::service::ServerCore;

    let temp = tempfile::tempdir().expect("a tempdir");
    let root = temp.path().join("workspaces");
    let definition = definition_named(temp.path(), "ws-1", "atlas");
    let report = WorkspaceManager::new("w-reports", &root)
        .materialize(&definition)
        .await;
    assert_eq!(
        report.state,
        i32::from(horde_proto::v1::WorkspaceInstanceState::Ready),
        "{}",
        report.detail
    );

    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let server = crate::support::serve(core.clone()).await;
    let state = temp.path().join("state");
    let config = horde_worker::startup::invocation(
        [
            "--insecure",
            "--server",
            &server,
            "--name",
            "w-reports",
            "--adapter",
            "fake",
            "--state-dir",
            &state.to_string_lossy(),
            "--workspace-root",
            &root.to_string_lossy(),
            "--workspace-scan-interval",
            "1s",
            "--workspace-report-interval",
            "1s",
        ]
        .into_iter()
        .map(ToOwned::to_owned),
    )
    .expect("the shape parses");
    let mut sink = |_: String| {};
    let prepared = horde_worker::startup::prepare(&config, &mut sink)
        .await
        .expect("an insecure daemon prepares");
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let later = definition_named(temp.path(), "ws-2", "docs");
    let watcher = async {
        let registered = eventually(Duration::from_secs(20), || async {
            !core.instances.for_definition("ws-1").is_empty()
        })
        .await;
        // Out of band, after registration: only a report can carry it.
        WorkspaceManager::new("w-reports", &root)
            .materialize(&later)
            .await;
        let reported = eventually(Duration::from_secs(20), || async {
            !core.instances.for_definition("ws-2").is_empty()
        })
        .await;
        let _ = stop.send(());
        registered && reported
    };
    let mut announce = |_: String| {};
    let (outcome, reached) = tokio::join!(
        horde_worker::startup::run(
            prepared,
            async move {
                let _ = stopped.await;
            },
            &mut announce,
        ),
        watcher
    );
    assert!(outcome.is_ok(), "{outcome:?}");
    assert!(reached, "the Server holds both of the Worker's Instances");
    let held = core.instances.for_definition("ws-2");
    assert_eq!(
        held[0]
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .as_deref(),
        Some("w-reports")
    );
}
