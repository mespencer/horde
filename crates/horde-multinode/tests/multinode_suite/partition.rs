//! A network partition between a worker and the server (DESIGN §2): the
//! worker's container leaves the bridge network with its sockets open — no
//! FIN, no RST, packets simply vanish — and rejoins later. Keepalive on both
//! ends must notice, the worker must redial, and work sent during the
//! partition must arrive exactly once.

use std::time::Duration;

use crate::common::cluster::{Cluster, about, outputs};

/// Partitioned for longer than a keepalive round but inside the lease: a
/// message sent while the worker is unreachable is delivered after the heal,
/// once, and the agent is never declared lost.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn a_message_sent_across_a_partition_arrives_once_after_the_heal() {
    let cluster = Cluster::up("partition").await;
    let (session, agent) = cluster.pinned_session("partition", "worker-3").await;
    cluster.echo(&session, &agent, "before the partition").await;

    cluster.partition("worker-3").await;
    cluster
        .horde_json(&["send", &agent, "sent into the void"])
        .await;
    // Long enough for the server's keepalive to give up on the stream, and
    // for the message to sit undelivered, but short of the 15 s lease.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let during = cluster.sync(&session).await;
    assert!(
        !outputs(&during)
            .iter()
            .any(|out| out == "sent into the void"),
        "nothing can be answered across a partition: {during:?}"
    );

    cluster.heal("worker-3").await;
    let events = cluster
        .sync_until_within(
            &session,
            "the message delivered after the heal",
            Duration::from_secs(120),
            |events| {
                outputs(events)
                    .iter()
                    .any(|out| out == "sent into the void")
            },
        )
        .await;
    assert_eq!(
        outputs(&events)
            .iter()
            .filter(|out| *out == "sent into the void")
            .count(),
        1,
        "delivered exactly once: {events:?}"
    );
    assert!(
        !about(&events, &agent).any(|e| e["kind"] == "placement_changed"
            && e["detail"].as_str().is_some_and(|d| d.starts_with("lost"))),
        "a partition inside the lease loses nothing: {events:?}"
    );
    cluster.await_online("worker-3").await;
    cluster.echo(&session, &agent, "after the heal").await;
}
