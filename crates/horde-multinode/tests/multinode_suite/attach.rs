//! A second client machine (DESIGN §3): `client-2` enrolled through a code
//! `client` minted with `horde enroll client`, and attaches to a session
//! from its own container while `client` drives it.

use crate::common::cluster::{Cluster, field};

/// `client-2` follows a session headlessly (`sync --follow`, the scriptable
/// form of attach) and sees the reply to a message `client` sent, produced
/// on a third machine.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn a_client_on_another_machine_attaches_and_sees_the_work() {
    let cluster = Cluster::up("attach").await;
    let (session, agent) = cluster.pinned_session("attach", "worker-2").await;

    // client-2 holds its own identity: its handshake succeeds on the
    // endpoint its join stored.
    let hello = cluster.horde_in("client-2", &["hello"]).await;
    assert!(hello.ok, "client-2 is enrolled: {}", hello.stderr);

    let mut follower = cluster.follow("client-2", &["sync", "--follow", &session]);
    // The follower is subscribed once it has replayed the placement.
    follower
        .until("the follower's replay", |event| {
            event["kind"] == "placement_changed"
        })
        .await;

    cluster
        .horde_json(&["send", &agent, "seen from afar"])
        .await;
    let echoed = follower
        .until("the echo on client-2", |event| {
            event["kind"] == "agent_output_chunk" && event["detail"] == "seen from afar"
        })
        .await;
    assert_eq!(field(&echoed, "agent_id"), agent);
}
