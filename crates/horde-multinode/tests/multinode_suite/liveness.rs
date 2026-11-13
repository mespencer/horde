//! A worker machine dying (ADR-0011): back inside the lease it resumes its
//! agents; past the lease its started agents are LOST, and the machine that
//! comes back afterwards redials and takes new work.
//!
//! The kill is `docker compose kill -s KILL`: no shutdown path runs, the
//! kernel closes the sockets, and the container's disk (state, journal,
//! workspaces) survives for the restart.

use std::time::Duration;

use crate::common::cluster::{Cluster, about, outputs};

/// SIGKILL and restart well inside the 15 s lease: the restarted worker
/// loads its identity and journal, redials, and the next message revives
/// the agent — nothing re-run, nothing doubled.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn a_killed_worker_restarted_inside_its_lease_resumes_its_agent() {
    let cluster = Cluster::up("resume").await;
    let (session, agent) = cluster.pinned_session("resume", "worker-2").await;
    cluster.echo(&session, &agent, "first life").await;

    let killed = cluster.compose(&["kill", "-s", "KILL", "worker-2"]).await;
    assert!(killed.ok, "{}", killed.stderr);
    let started = cluster.compose(&["start", "worker-2"]).await;
    assert!(started.ok, "{}", started.stderr);
    cluster.await_online("worker-2").await;

    let events = cluster.echo(&session, &agent, "second life").await;
    assert_eq!(
        outputs(&events)
            .iter()
            .filter(|out| *out == "first life")
            .count(),
        1,
        "a restart must not re-run applied work: {events:?}"
    );
    assert!(
        !about(&events, &agent).any(|e| e["kind"] == "placement_changed"
            && e["detail"].as_str().is_some_and(|d| d.starts_with("lost"))),
        "back inside the lease, nothing is lost: {events:?}"
    );
}

/// A worker that stays dead past its lease: its started agent is marked
/// LOST and FAILED in the log and further messages to it are refused. When
/// the machine finally returns it redials, registers, and is placed new
/// work like any other worker.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn a_worker_dead_past_its_lease_loses_its_agents_then_redials() {
    let cluster = Cluster::up("lease").await;
    let (session, agent) = cluster.pinned_session("lease", "worker-3").await;
    cluster.echo(&session, &agent, "before the death").await;

    let killed = cluster.compose(&["kill", "-s", "KILL", "worker-3"]).await;
    assert!(killed.ok, "{}", killed.stderr);

    let events = cluster
        .sync_until_within(
            &session,
            "the lease lapsing",
            Duration::from_secs(120),
            |events| {
                about(events, &agent).any(|e| {
                    e["kind"] == "agent_lifecycle_changed" && e["detail"]["state"] == "failed"
                })
            },
        )
        .await;
    assert!(
        about(&events, &agent).any(|e| e["kind"] == "placement_changed"
            && e["detail"].as_str().is_some_and(|d| d.starts_with("lost"))),
        "the placement is LOST, not silently re-run elsewhere: {events:?}"
    );
    let status = cluster.agent_status(&agent).await;
    assert_eq!(status["state"], "failed", "{status}");
    let refused = cluster
        .horde_in("client", &["send", &agent, "anyone there?"])
        .await;
    assert!(
        !refused.ok,
        "a lost agent refuses messages: {}",
        refused.stdout
    );

    let started = cluster.compose(&["start", "worker-3"]).await;
    assert!(started.ok, "{}", started.stderr);
    cluster.await_online("worker-3").await;
    let (again, fresh) = cluster.pinned_session("after the lease", "worker-3").await;
    cluster.echo(&again, &fresh, "back on the network").await;
}
