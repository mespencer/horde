//! Placement across machines (DESIGN §6): unpinned work spreads over the
//! workers, and each placed agent is started by the worker it landed on —
//! the command crossed the network to that container and its journal came
//! back.

use crate::common::cluster::{Cluster, WORKERS, about, field};

/// Three unpinned sessions over three idle workers: the scheduler's
/// least-loaded preference puts one root agent on each, and every one of
/// them is started where it was placed.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn unpinned_agents_spread_across_the_workers() {
    let cluster = Cluster::up("placement").await;

    let mut placed = Vec::new();
    for title in ["spread-a", "spread-b", "spread-c"] {
        let created = cluster.horde_json(&["session", "new", title]).await;
        let session = field(&created, "session_id");
        let root = field(&created, "root_agent_id");
        // Started where placed: the worker journals the agent's first
        // lifecycle transition only once it holds the agent.
        cluster
            .sync_until(
                &session,
                &format!("{root} starting on its worker"),
                |events| about(events, &root).any(|e| e["kind"] == "agent_lifecycle_changed"),
            )
            .await;
        let status = cluster.agent_status(&root).await;
        placed.push(field(&status, "worker_id"));
    }

    placed.sort();
    assert_eq!(
        placed, WORKERS,
        "one root agent per worker: least loaded wins every tie-break"
    );
}
