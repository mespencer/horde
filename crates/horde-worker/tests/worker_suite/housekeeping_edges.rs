//! ADR-0014 housekeeping at its boundaries: what a trigger's events carry,
//! the disk-pressure watermarks reached exactly, the order and the guards
//! of a pressure episode, a purge across Instances, and the scan loop's
//! pin pass under a policy with a pin TTL.
//!
//! Every assertion reads what production wrote: journaled events, the
//! checkouts and pins left on disk, what a scan says it changed.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use horde_proto::v1::event::Kind;
use horde_proto::v1::{
    CleanupTier, CleanupTrigger, GcLevel, ResolvedWorkspacePolicy, WorkspaceCleanup,
    WorkspaceGcCommand,
};
use horde_worker::workspace::{FsStats, WorkspaceManager};
use horde_worker::{ApplyCommand as _, Journal};

use crate::workspace_rig::{Rig, TOTAL, agent_root, archive, archived_pins, atlas_rig};
use crate::workspace_rig::{cleanups, command, daemon_config, daemon_while, definition_named};
use crate::workspace_rig::{deliver_refs, dirty_atlas, events, eventually, fresh_rig, head};
use crate::workspace_rig::{kinds, materialize, observed_within, pinned_instance_repo, policy};
use crate::workspace_rig::{rig_with, settled_within, spawn_in, wire};
use crate::workspaces::harness;

/// A day's grace, no idle reclaim.
fn graceful() -> ResolvedWorkspacePolicy {
    policy(86_400, 0)
}

/// [`atlas_rig`] under [`graceful`], with `agents` spawned in it.
async fn graceful_rig(temp: &Path, agents: &[&str]) -> Rig {
    let mut rig = atlas_rig(temp, graceful()).await;
    for agent in agents {
        spawn_in(&mut rig, "ws-1", agent).await;
    }
    rig
}

/// [`atlas_rig`] with no grace, `agent-1` spawned and archived.
async fn archived_atlas_rig(temp: &Path) -> Rig {
    let mut rig = atlas_rig(temp, policy(0, 0)).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    archived(&mut rig, "agent-1", 2).await;
    rig
}

/// Makes `agent-a`'s activity a minute newer than any other's.
async fn agent_a_was_active_last(rig: &Rig) {
    let manager = rig.runtime.workspaces().expect("workspaces");
    horde_worker::workspace::cleanup::note_activity(
        manager,
        "ws-1",
        "agent-a",
        horde_worker::workspace::ledger::now_ms() + 60_000,
    )
    .await;
}

/// `agent`'s checkout survived, and pressure removed nothing; then the
/// Runtime stops.
async fn assert_spared_by_pressure(rig: Rig, agent: &str) {
    assert!(agent_root(&rig, "ws-1", agent).exists());
    assert!(
        touched(&rig, CleanupTrigger::DiskPressure, CleanupTier::Hard)
            .await
            .is_empty()
    );
    rig.runtime.shutdown().await;
}

/// Scans, and answers which agents disk pressure touched at `tier`.
async fn pressure_touches(rig: &Rig, tier: CleanupTier) -> Vec<String> {
    rig.runtime.scan_workspaces().await;
    touched(rig, CleanupTrigger::DiskPressure, tier).await
}

/// A rig under `temp` whose housekeeping loop runs on these intervals.
fn looping_rig(temp: &Path, scan: Duration, pins: Duration, reports: Duration) -> Rig {
    let config = horde_worker::runtime::HousekeepingConfig {
        scan_interval: scan,
        pin_scan_interval: pins,
        report_interval: reports,
        ..horde_worker::runtime::HousekeepingConfig::default()
    };
    rig_with(
        &temp.join("workspaces"),
        Journal::in_memory().expect("journal"),
        config,
    )
}

fn agent_of(cleanup: &WorkspaceCleanup) -> String {
    cleanup
        .agent_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default()
}

fn watermarks() -> (u64, u64) {
    let config = horde_worker::runtime::HousekeepingConfig::default();
    (
        config.disk_low_watermark.bytes_of(TOTAL),
        config.disk_high_watermark.bytes_of(TOTAL),
    )
}

/// The agents a journal's cleanups name under `trigger` at `tier`, in
/// journal order.
async fn touched(rig: &Rig, trigger: CleanupTrigger, tier: CleanupTier) -> Vec<String> {
    cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| c.trigger == i32::from(trigger) && c.tier == i32::from(tier))
        .map(agent_of)
        .collect()
}

/// Every cleanup journaled under `trigger`: at least one, none of them a gc
/// request's or a dry run, and none journaled against an agent's stream
/// (the housekeeping stream names no agent; the event body does).
async fn assert_triggered(rig: &Rig, trigger: CleanupTrigger) {
    let journaled: Vec<_> = events(&rig.journal)
        .await
        .into_iter()
        .filter(|event| {
            matches!(&event.kind, Some(Kind::WorkspaceCleanup(c)) if c.trigger == i32::from(trigger))
        })
        .collect();
    assert!(!journaled.is_empty(), "nothing under {trigger:?}");
    for event in journaled {
        assert_eq!(event.agent_id, None, "{event:?}");
        let Some(Kind::WorkspaceCleanup(cleanup)) = event.kind else {
            unreachable!("filtered to cleanups");
        };
        assert_eq!(cleanup.gc_request_id, "", "{cleanup:?}");
        assert!(!cleanup.dry_run, "{cleanup:?}");
    }
}

/// Archives `agent` and waits until `count` archive events name it.
async fn archived(rig: &mut Rig, agent: &str, count: usize) {
    archive(rig, agent).await;
    observed_within("the archive's release", async {
        while touched_any(rig, CleanupTrigger::Archive, agent).await < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    // The release reports last, still holding the checkout's lock; a
    // scan skips a locked checkout, so wait the report out.
    tokio::time::sleep(Duration::from_millis(50)).await;
}

async fn touched_any(rig: &Rig, trigger: CleanupTrigger, agent: &str) -> usize {
    cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| c.trigger == i32::from(trigger) && agent_of(c) == agent)
        .count()
}

async fn gc(rig: &mut Rig, gc: WorkspaceGcCommand) {
    let id = gc.request_id.clone();
    rig.runtime
        .apply(command(
            &id,
            horde_proto::v1::stream_commands_response::Command::WorkspaceGc(gc),
        ))
        .await
        .expect("the gc applies");
    settled_within("the gc", rig.runtime.settled()).await;
}

/// The archive's own events are triggers', not a gc's; so is a gc's on the
/// housekeeping stream, which names no agent either.
#[tokio::test]
async fn an_archive_journals_trigger_events_on_the_housekeeping_stream() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = archived_atlas_rig(temp.path()).await;
    assert_triggered(&rig, CleanupTrigger::Archive).await;

    spawn_in(&mut rig, "ws-1", "agent-2").await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "gc-1".to_owned(),
            level: GcLevel::Soft.into(),
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    let gc_events: Vec<_> = events(&rig.journal)
        .await
        .into_iter()
        .filter(|event| {
            matches!(&event.kind, Some(Kind::WorkspaceCleanup(c)) if c.gc_request_id == "gc-1")
                || matches!(&event.kind, Some(Kind::WorkspaceGcCompleted(_)))
        })
        .collect();
    assert!(gc_events.len() >= 2, "{gc_events:?}");
    assert!(gc_events.iter().all(|event| event.agent_id.is_none()));
    rig.runtime.shutdown().await;
}

/// An idle reclaim is a trigger's soft reclaim, carried out for real.
#[tokio::test]
async fn an_idle_reclaim_is_a_real_trigger_event() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let sub_ms = ResolvedWorkspacePolicy {
        archived_grace: Some(wire(86_400, 0)),
        idle_reclaim_after: Some(wire(0, 1)),
        ..ResolvedWorkspacePolicy::default()
    };
    let mut rig = atlas_rig(temp.path(), sub_ms).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(rig.runtime.scan_workspaces().await);
    assert_eq!(
        touched(&rig, CleanupTrigger::Idle, CleanupTier::Soft).await,
        vec!["agent-1"]
    );
    assert_triggered(&rig, CleanupTrigger::Idle).await;
    rig.runtime.shutdown().await;
}

/// A live agent under a policy with no idle reclaim gives a scan nothing
/// to do.
#[tokio::test]
async fn a_live_agent_with_no_idle_reclaim_changes_nothing() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-1"]).await;
    assert!(!rig.runtime.scan_workspaces().await);
    rig.runtime.shutdown().await;
}

/// A scan past the grace removes the archived checkout as a trigger.
#[tokio::test]
async fn a_scan_past_the_grace_removes_as_a_trigger() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let one_ms = ResolvedWorkspacePolicy {
        archived_grace: Some(wire(0, 1_000_000)),
        idle_reclaim_after: Some(wire(0, 0)),
        ..ResolvedWorkspacePolicy::default()
    };
    let mut rig = atlas_rig(temp.path(), one_ms).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    archived(&mut rig, "agent-1", 1).await;
    observed_within("the scan past the grace", async {
        while !rig.runtime.scan_workspaces().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert_eq!(
        touched(&rig, CleanupTrigger::Scan, CleanupTier::Hard).await,
        vec!["agent-1"]
    );
    assert!(!agent_root(&rig, "ws-1", "agent-1").exists());
    assert_triggered(&rig, CleanupTrigger::Scan).await;
    rig.runtime.shutdown().await;
}

/// Free space exactly at the low watermark starts no episode.
#[tokio::test]
async fn free_space_exactly_at_the_low_watermark_starts_no_episode() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-1"]).await;
    let (low, _) = watermarks();
    rig.disk.set_free(low);
    assert!(!rig.runtime.scan_workspaces().await);
    assert!(
        touched(&rig, CleanupTrigger::DiskPressure, CleanupTier::Soft)
            .await
            .is_empty()
    );
    rig.runtime.shutdown().await;
}

/// An episode ends with free space exactly at the high watermark: back
/// between the watermarks afterwards, nothing is reclaimed.
#[tokio::test]
async fn an_episode_ends_at_exactly_the_high_watermark() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = atlas_rig(temp.path(), graceful()).await;
    let (low, high) = watermarks();
    rig.disk.set_free(low - 1);
    rig.runtime.scan_workspaces().await;
    rig.disk.set_free(high);
    rig.runtime.scan_workspaces().await;

    spawn_in(&mut rig, "ws-1", "agent-1").await;
    archived(&mut rig, "agent-1", 1).await;
    rig.disk.set_free(low + (high - low) / 2);
    assert!(!rig.runtime.scan_workspaces().await);
    assert_spared_by_pressure(rig, "agent-1").await;
}

/// Exhaustion is a change worth reporting; an episode that then has
/// nothing to do between the watermarks changes nothing.
#[tokio::test]
async fn exhaustion_changes_the_scan_and_an_idle_episode_does_not() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = atlas_rig(temp.path(), graceful()).await;
    let (low, high) = watermarks();
    rig.disk.set_free(low - 1);
    assert!(rig.runtime.scan_workspaces().await, "exhausted");
    let exhausted = kinds(&rig.journal)
        .await
        .into_iter()
        .filter(|kind| matches!(kind, Kind::WorkspaceDiskPressure(_)))
        .count();
    assert_eq!(exhausted, 1);
    rig.disk.set_free(low + (high - low) / 2);
    assert!(!rig.runtime.scan_workspaces().await, "nothing to reclaim");
    rig.runtime.shutdown().await;
}

/// A candidate is reclaimed while free space is a byte short of the high
/// watermark, and that reclaim is the scan's change.
#[tokio::test]
async fn a_byte_short_of_the_high_watermark_still_reclaims() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-1"]).await;
    let (low, high) = watermarks();
    rig.disk.queue(&[low - 1, high - 1]);
    assert!(rig.runtime.scan_workspaces().await);
    assert_eq!(
        touched(&rig, CleanupTrigger::DiskPressure, CleanupTier::Soft).await,
        vec!["agent-1"]
    );
    assert_triggered(&rig, CleanupTrigger::DiskPressure).await;
    rig.runtime.shutdown().await;
}

/// Free space exactly at the high watermark is relief: no candidate is
/// reclaimed.
#[tokio::test]
async fn exactly_the_high_watermark_reclaims_nothing() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-1"]).await;
    let (low, high) = watermarks();
    rig.disk.queue(&[low - 1, high]);
    rig.runtime.scan_workspaces().await;
    assert!(
        touched(&rig, CleanupTrigger::DiskPressure, CleanupTier::Soft)
            .await
            .is_empty()
    );
    rig.runtime.shutdown().await;
}

/// The archived removals stop at relief too: a byte short of the high
/// watermark removes, exactly at it keeps.
#[tokio::test]
async fn archived_removals_stop_at_exactly_the_high_watermark() {
    let (low, high) = watermarks();
    for (reading, removed) in [(high - 1, true), (high, false)] {
        let temp = tempfile::tempdir().expect("a tempdir");
        let mut rig = atlas_rig(temp.path(), graceful()).await;
        spawn_in(&mut rig, "ws-1", "agent-1").await;
        archived(&mut rig, "agent-1", 1).await;
        rig.disk.queue(&[low - 1, reading]);
        rig.runtime.scan_workspaces().await;
        assert_eq!(
            !agent_root(&rig, "ws-1", "agent-1").exists(),
            removed,
            "free {reading}"
        );
        if removed {
            assert_eq!(
                touched(&rig, CleanupTrigger::DiskPressure, CleanupTier::Hard).await,
                vec!["agent-1"]
            );
            assert_triggered(&rig, CleanupTrigger::DiskPressure).await;
        }
        rig.runtime.shutdown().await;
    }
}

/// Pressure never forces: an archived checkout holding uncommitted work
/// stays, and its refusal is not journaled as a pressure removal.
#[tokio::test]
async fn pressure_keeps_an_archived_checkout_with_uncommitted_work() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = graceful_rig(temp.path(), &["agent-1"]).await;
    dirty_atlas(&rig, "agent-1");
    archived(&mut rig, "agent-1", 1).await;
    rig.disk.set_free(1);
    rig.runtime.scan_workspaces().await;
    assert_spared_by_pressure(rig, "agent-1").await;
}

/// A busy agent is passed over, and the episode goes on to the next
/// candidate; the oldest activity is reclaimed first.
#[tokio::test]
async fn pressure_passes_over_a_busy_agent_to_the_next() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-a", "agent-b"]).await;
    // agent-b's activity is the older, so it comes first; it is busy.
    agent_a_was_active_last(&rig).await;
    rig.runtime.activity().set_busy("agent-b", true);
    rig.disk.set_free(1);
    assert_eq!(
        pressure_touches(&rig, CleanupTier::Soft).await,
        vec!["agent-a"]
    );
    rig.runtime.shutdown().await;
}

/// Candidates go oldest activity first: relieved after one reclaim, only
/// the agent idle the longest was touched.
#[tokio::test]
async fn pressure_reclaims_the_longest_idle_agent_first() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = graceful_rig(temp.path(), &["agent-a", "agent-b"]).await;
    agent_a_was_active_last(&rig).await;
    let (low, _) = watermarks();
    rig.disk.queue(&[low - 1, low - 1]);
    assert_eq!(
        pressure_touches(&rig, CleanupTier::Soft).await,
        vec!["agent-b"]
    );
    rig.runtime.shutdown().await;
}

/// A checkout already reclaimed is no candidate, and the ones after it
/// still are.
#[tokio::test]
async fn a_reclaimed_checkout_does_not_end_the_candidate_list() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = graceful_rig(temp.path(), &["agent-a", "agent-b"]).await;
    archived(&mut rig, "agent-a", 1).await;
    // agent-a was soft-reclaimed by its archive, so only agent-b, listed
    // after it, is a soft candidate.
    rig.disk.set_free(1);
    assert_eq!(
        pressure_touches(&rig, CleanupTier::Soft).await,
        vec!["agent-b"]
    );
    rig.runtime.shutdown().await;
}

/// Archived checkouts are removed oldest archive first: relieved after one
/// removal, only the earlier archive is gone.
#[tokio::test]
async fn pressure_removes_the_oldest_archive_first() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = graceful_rig(temp.path(), &["agent-a", "agent-b"]).await;
    archived(&mut rig, "agent-b", 1).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    archived(&mut rig, "agent-a", 1).await;
    let (low, _) = watermarks();
    rig.disk.queue(&[low - 1, low - 1]);
    assert_eq!(
        pressure_touches(&rig, CleanupTier::Hard).await,
        vec!["agent-b"]
    );
    assert!(agent_root(&rig, "ws-1", "agent-a").exists());
    rig.runtime.shutdown().await;
}

/// No free-space facts, no episode: a scan with nothing else to do
/// changes nothing.
#[tokio::test]
async fn a_disk_that_cannot_be_read_starts_no_episode() {
    #[derive(Debug)]
    struct Unreadable;
    impl FsStats for Unreadable {
        fn stats(&self, _path: &Path) -> Option<(u64, u64)> {
            None
        }
    }
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = fresh_rig(temp.path());
    let manager = WorkspaceManager::new("worker-a", temp.path().join("workspaces"))
        .with_fs_stats(Arc::new(Unreadable));
    rig.runtime = horde_worker::Runtime::new(rig.journal.clone())
        .with_adapter(Arc::new(horde_adapter_fake::FakeAdapter::new(
            horde_adapter_fake::Script::echoing(),
        )))
        .with_workspaces(manager)
        .with_housekeeping(crate::workspace_rig::manual());
    materialize(
        &mut rig,
        &definition_named(temp.path(), "ws-1", "atlas"),
        graceful(),
    )
    .await;
    assert!(!rig.runtime.scan_workspaces().await);
    rig.runtime.shutdown().await;
}

/// Two Instances, each with an archived checkout inside its grace.
async fn two_instances(temp: &Path) -> Rig {
    let mut rig = fresh_rig(temp);
    for (id, repo) in [("ws-1", "atlas"), ("ws-2", "docs")] {
        materialize(&mut rig, &definition_named(temp, id, repo), graceful()).await;
    }
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    spawn_in(&mut rig, "ws-2", "agent-2").await;
    archived(&mut rig, "agent-1", 1).await;
    archived(&mut rig, "agent-2", 1).await;
    rig
}

/// A purge of every Instance removes each, archived checkouts and all, as
/// a purge; the Instances it removed are reported missing, with no
/// progress.
#[tokio::test]
async fn a_purge_removes_every_instance_as_a_purge() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = two_instances(temp.path()).await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "purge".to_owned(),
            remove_instance: true,
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    assert!(!rig.root.join("ws-1").exists(), "ws-1 is purged");
    assert!(!rig.root.join("ws-2").exists(), "ws-2 is purged");
    let purge: Vec<_> = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "purge")
        .collect();
    assert!(!purge.is_empty());
    assert!(
        purge
            .iter()
            .all(|c| c.trigger == i32::from(CleanupTrigger::Purge)),
        "{purge:?}"
    );
    let missing: Vec<_> = kinds(&rig.journal)
        .await
        .into_iter()
        .filter_map(|kind| match kind {
            Kind::WorkspaceInstanceChanged(changed)
                if changed.report.as_ref().is_some_and(|report| {
                    report.state == i32::from(horde_proto::v1::WorkspaceInstanceState::Missing)
                }) =>
            {
                Some(changed)
            }
            _ => None,
        })
        .collect();
    assert!(
        missing.iter().all(|changed| changed.progress.is_none()),
        "{missing:?}"
    );
    assert_eq!(missing.len(), 2, "{missing:?}");
    rig.runtime.shutdown().await;
}

/// A purge refused for a live agent touches nothing else of the Instance:
/// no soft reclaim rides along.
#[tokio::test]
async fn a_refused_purge_reclaims_nothing() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = graceful_rig(temp.path(), &["agent-1"]).await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "purge".to_owned(),
            remove_instance: true,
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    assert!(agent_root(&rig, "ws-1", "agent-1").exists());
    let soft: Vec<_> = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "purge" && c.tier == i32::from(CleanupTier::Soft))
        .collect();
    assert!(soft.is_empty(), "{soft:?}");
    rig.runtime.shutdown().await;
}

/// A gc naming the second Instance reaches it past the first.
#[tokio::test]
async fn a_gc_naming_the_later_instance_reaches_it() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = two_instances(temp.path()).await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "gc-ws-2".to_owned(),
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-2")),
            level: GcLevel::Hard.into(),
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    assert!(!agent_root(&rig, "ws-2", "agent-2").exists());
    assert!(agent_root(&rig, "ws-1", "agent-1").exists());
    let explicit: Vec<_> = cleanups(&rig.journal)
        .await
        .into_iter()
        .filter(|c| c.gc_request_id == "gc-ws-2")
        .collect();
    assert!(
        !explicit.is_empty()
            && explicit
                .iter()
                .all(|c| c.trigger == i32::from(CleanupTrigger::Explicit)),
        "{explicit:?}"
    );
    rig.runtime.shutdown().await;
}

/// A gc naming agents touches only theirs.
#[tokio::test]
async fn a_gc_naming_an_agent_touches_only_its_checkout() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = graceful_rig(temp.path(), &["agent-1"]).await;
    spawn_in(&mut rig, "ws-1", "agent-2").await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "gc-agent".to_owned(),
            level: GcLevel::Soft.into(),
            agent_ids: vec![horde_proto::v1::AgentId::new("agent-1")],
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    let named: Vec<String> = cleanups(&rig.journal)
        .await
        .iter()
        .filter(|c| c.gc_request_id == "gc-agent")
        .map(agent_of)
        .collect();
    assert_eq!(named, vec!["agent-1"]);
    rig.runtime.shutdown().await;
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_DATE", "2000-01-01T00:00:00Z")
        .env("GIT_COMMITTER_DATE", "2000-01-01T00:00:00Z")
        .output()
        .expect("git runs");
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The scan loop's pin pass is never forced: a pin to a commit only this
/// Worker holds stays, even past the policy's pin TTL, which only a forced
/// gc may apply. A pin origin holds goes, as a trigger's event.
#[tokio::test]
async fn the_scan_loop_never_forces_a_pin_past_its_ttl() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = looping_rig(
        temp.path(),
        Duration::from_millis(20),
        Duration::ZERO,
        Duration::from_secs(3600),
    );
    let ttl = ResolvedWorkspacePolicy {
        archived_grace: Some(wire(0, 0)),
        idle_reclaim_after: Some(wire(0, 0)),
        archived_pin_ttl: Some(wire(1, 0)),
        ..ResolvedWorkspacePolicy::default()
    };
    materialize(
        &mut rig,
        &definition_named(temp.path(), "ws-1", "atlas"),
        ttl,
    )
    .await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    spawn_in(&mut rig, "ws-1", "agent-2").await;
    // agent-1's commit exists only here, dated long before any TTL.
    let checkout = agent_root(&rig, "ws-1", "agent-1").join("atlas");
    git(
        &checkout,
        &[
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "commit",
            "-qm",
            "local only",
            "--allow-empty",
        ],
    );
    archived(&mut rig, "agent-1", 2).await;
    gc(
        &mut rig,
        WorkspaceGcCommand {
            request_id: "forced".to_owned(),
            level: GcLevel::Hard.into(),
            force: true,
            agent_ids: vec![horde_proto::v1::AgentId::new("agent-1")],
            ..WorkspaceGcCommand::default()
        },
    )
    .await;
    let instance_repo = rig.root.join("ws-1/atlas");
    let pins = || archived_pins(&instance_repo);
    assert!(
        pins().contains("agent-1"),
        "the forced removal pinned the commit"
    );
    // agent-2's pin is to the base origin holds: the pass drops it.
    archived(&mut rig, "agent-2", 2).await;
    observed_within("the pass dropping the safe pin", async {
        while pins().contains("agent-2") {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    // Several more passes run before this reading.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(pins().contains("agent-1"), "never forced: {}", pins());
    assert_triggered(&rig, CleanupTrigger::Scan).await;
    rig.runtime.shutdown().await;
}

/// A shutdown stops the scan loop: nothing reports after it.
#[tokio::test]
async fn a_shutdown_stops_the_scan_loop() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut rig = looping_rig(
        temp.path(),
        Duration::from_millis(10),
        Duration::from_secs(3600),
        Duration::ZERO,
    );
    materialize(
        &mut rig,
        &definition_named(temp.path(), "ws-1", "atlas"),
        graceful(),
    )
    .await;
    let reporter = Arc::clone(&rig.reporter);
    observed_within("the loop's reports", async {
        while reporter.count() < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    rig.runtime.shutdown().await;
    let after = reporter.count();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(reporter.count(), after, "the loop ended with the Runtime");
}

async fn pending_of(rig: &Rig) -> Vec<(String, Vec<String>)> {
    horde_worker::workspace::manifest::read(&agent_root(rig, "ws-1", "agent-1"))
        .await
        .expect("a manifest")
        .repos
        .iter()
        .map(|repo| (repo.name.clone(), repo.pending_commits.clone()))
        .collect()
}

async fn echoed(rig: &Rig, needle: &str) -> bool {
    kinds(&rig.journal)
        .await
        .iter()
        .any(|kind| matches!(kind, Kind::AgentOutputChunk(chunk) if chunk.text.contains(needle)))
}

/// A commit for a repo the agent has checked out is the agent's to
/// receive, among several checked-out repos too: nothing is held pending.
#[tokio::test]
async fn a_ref_for_any_checked_out_repo_is_delivered_not_held() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (definition, _atlas, _docs) = harness::two_repo_definition(temp.path());
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, graceful()).await;
    spawn_in(&mut rig, "ws-1", "agent-1").await;
    let docs_head = head(&rig.root.join("ws-1/docs"));
    deliver_refs(&mut rig, &[("docs", docs_head)]).await;
    assert_eq!(
        pending_of(&rig).await,
        vec![
            ("atlas".to_owned(), Vec::new()),
            ("docs".to_owned(), Vec::new())
        ]
    );
    rig.runtime.shutdown().await;
}

/// A lazy receiver's message still arrives when its payload names a repo
/// it has not checked out: that commit is held pending, not delivered.
#[tokio::test]
async fn a_lazy_receivers_message_arrives_with_the_unchecked_ref_held_back() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let (mut definition, _atlas, _docs) = harness::two_repo_definition(temp.path());
    definition.checkout_mode = horde_proto::v1::CheckoutMode::Lazy.into();
    let mut rig = fresh_rig(temp.path());
    materialize(&mut rig, &definition, graceful()).await;
    crate::workspace_rig::spawn_with(
        &mut rig,
        "agent-1",
        horde_proto::v1::AgentSpec {
            checkout_repos: vec!["atlas".to_owned()],
            ..crate::workspace_rig::worktree_in("ws-1")
        },
    )
    .await;
    let atlas_head = head(&rig.root.join("ws-1/atlas"));
    let docs_head = head(&rig.root.join("ws-1/docs"));
    deliver_refs(
        &mut rig,
        &[("atlas", atlas_head), ("docs", docs_head.clone())],
    )
    .await;
    assert_eq!(
        pending_of(&rig).await,
        vec![
            ("atlas".to_owned(), Vec::new()),
            ("docs".to_owned(), vec![docs_head])
        ]
    );
    observed_within("the message reaching the agent", async {
        while !echoed(&rig, "look at these").await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    rig.runtime.shutdown().await;
}

/// A daemon holding no Instance yet still advertises its disk, and its
/// reports are its whole set: an Instance gone from disk is gone from the
/// Server too.
#[tokio::test]
async fn the_daemon_advertises_its_disk_and_reports_its_whole_set() {
    use horde_server::EventLog;
    use horde_server::service::ServerCore;

    let temp = tempfile::tempdir().expect("a tempdir");
    let root = temp.path().join("workspaces");
    std::fs::create_dir_all(&root).expect("the root");
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let server = crate::support::serve(core.clone()).await;
    let config = daemon_config(&server, "w-disk", &temp.path().join("state"), &root);
    let definition = definition_named(temp.path(), "ws-1", "atlas");
    let (advertised, disk, reported, dropped) = daemon_while(&config, async {
        let advertised = eventually(Duration::from_secs(20), || async {
            core.workers.capabilities_of("w-disk").is_some()
        })
        .await;
        let disk = core
            .workers
            .capabilities_of("w-disk")
            .map(|c| (c.workspace_free_bytes, c.workspace_total_bytes));
        WorkspaceManager::new("w-disk", &root)
            .materialize(&definition)
            .await;
        let reported = eventually(Duration::from_secs(20), || async {
            !core.instances.for_definition("ws-1").is_empty()
        })
        .await;
        std::fs::remove_dir_all(root.join("ws-1")).expect("the Instance goes");
        let dropped = eventually(Duration::from_secs(20), || async {
            core.instances.for_definition("ws-1").is_empty()
        })
        .await;
        (advertised, disk, reported, dropped)
    })
    .await;
    assert!(advertised, "registered");
    let (free, total) = disk.expect("capabilities");
    assert!(free > 0 && total >= free, "free {free} of {total}");
    assert!(reported, "the Instance was reported");
    assert!(dropped, "and dropped once gone");
}

/// The pin pass waits out its own interval: a loop ticking every few
/// milliseconds leaves a safe pin alone until its hours are up.
#[tokio::test]
async fn the_pin_pass_keeps_its_own_interval() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let rig = archived_atlas_rig(temp.path()).await;
    let instance_repo = pinned_instance_repo(&rig, "agent-1");
    let pins = || archived_pins(&instance_repo);
    let journal = rig.journal.clone();
    rig.runtime.shutdown().await;

    // Restarted with a loop that ticks fast but passes pins hourly.
    let config = horde_worker::runtime::HousekeepingConfig {
        scan_interval: Duration::from_millis(10),
        pin_scan_interval: Duration::from_secs(3600),
        report_interval: Duration::from_secs(3600),
        ..horde_worker::runtime::HousekeepingConfig::default()
    };
    let mut rig = rig_with(&temp.path().join("workspaces"), journal, config);
    rig.runtime.replay_inbox().await.expect("the replay runs");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(pins().contains("agent-1"), "not yet an hour");
    rig.runtime.shutdown().await;
}
