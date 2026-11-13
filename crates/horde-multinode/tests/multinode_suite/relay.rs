//! The git Relay across machines (DESIGN §8): a commit made on one worker's
//! container reaches an agent on another worker's container through the
//! server's mTLS Relay door — the only path between them, since the origin
//! never sees the commit and the workers share no disk.

use crate::common::cluster::{Cluster, about, field, outputs};

/// The `atlas` checkout the worker composes for `agent`.
fn checkout(definition: &str, agent: &str) -> String {
    format!("/work/{definition}/.horde-agents/{agent}/atlas")
}

/// The sender on worker-1 commits and reports; the worker stamps the commit
/// and stages it to the Relay over mTLS. The receiver on worker-2 is sent a
/// payload naming it, and its worker fetches it through the same door
/// before the agent reads the message. A machine with no identity is
/// refused at the same door.
#[tokio::test]
#[ignore = "needs Docker; run with --run-ignored only (docs/TESTING.md)"]
async fn an_agents_commit_reaches_another_worker_through_the_mtls_relay() {
    let cluster = Cluster::up("relay").await;

    // The workspace's one repo lives on the origin container; every worker
    // clones it over git://, and the Relay mirrors it from there.
    let definition = cluster
        .horde_field(
            &[
                "workspace",
                "create",
                "atlas-ws",
                "--repo",
                "atlas=git://origin/atlas.git,path=atlas",
            ],
            "workspace_definition_id",
        )
        .await;

    let created = cluster
        .horde_json(&[
            "session",
            "new",
            "relay",
            "--workspace",
            &definition,
            "--pin-worker",
            "worker-1",
        ])
        .await;
    let session = field(&created, "session_id");
    let sender = field(&created, "root_agent_id");
    let spawned = cluster
        .horde_json(&[
            "spawn",
            &session,
            "--parent",
            &sender,
            "--workspace",
            &definition,
            "--pin-worker",
            "worker-2",
        ])
        .await;
    let receiver = field(&spawned, "agent_id");
    assert_eq!(spawned["placement"], "placed", "{spawned}");
    assert_eq!(spawned["worker_id"], "worker-2", "{spawned}");

    // Both checkouts composed: each worker journals an agent's first
    // lifecycle transition only once its whole checkout exists.
    cluster
        .sync_until(&session, "both checkouts composed", |events| {
            [&sender, &receiver]
                .iter()
                .all(|agent| about(events, agent).any(|e| e["kind"] == "agent_lifecycle_changed"))
        })
        .await;

    // The sender's work: a commit only worker-1's disk holds.
    let sender_checkout = checkout(&definition, &sender);
    let committed = cluster
        .exec(
            "worker-1",
            &[
                "sh",
                "-c",
                "cd \"$1\" && echo 'the atlas moved' > finding.md && git add finding.md \
                 && git commit -q -m finding && git rev-parse HEAD",
                "commit",
                &sender_checkout,
            ],
            None,
        )
        .await;
    assert!(committed.ok, "the sender's commit: {}", committed.stderr);
    let sha = committed.stdout.trim().to_owned();

    // The report: the sender's scripted first turn calls report_result; the
    // worker observes HEAD, stamps it, and pushes it to the Relay.
    cluster
        .horde_json(&["send", &sender, "report your result"])
        .await;
    let events = cluster
        .sync_until(&session, "the sender's result", |events| {
            about(events, &sender).any(|e| e["kind"] == "result_reported")
        })
        .await;
    let result = about(&events, &sender)
        .find(|e| e["kind"] == "result_reported")
        .expect("the result is in the log");
    assert_eq!(
        result["detail"]["git_refs"][0]["commit"], sha,
        "the worker stamped what the checkout committed: {result}"
    );

    // Only the Relay holds it: not the origin, and the share ref is on the
    // server's mirror.
    let at_origin = cluster
        .exec(
            "origin",
            &[
                "git",
                "-C",
                "/srv/git/atlas.git",
                "cat-file",
                "-e",
                &format!("{sha}^{{commit}}"),
            ],
            None,
        )
        .await;
    assert!(!at_origin.ok, "the commit must not reach the origin");
    let mirror = cluster
        .exec(
            "server",
            &["git", "-C", "/state/relay/atlas.git", "show-ref"],
            None,
        )
        .await;
    assert!(
        mirror.stdout.contains(&format!("refs/horde/share/{sha}")),
        "the share ref is on the server's mirror: {} {}",
        mirror.stdout,
        mirror.stderr
    );

    // The payload-carrying delivery to worker-2.
    cluster
        .horde_json(&[
            "send",
            &receiver,
            "integrate the atlas change",
            "--git-ref",
            &format!("atlas:{sha}"),
        ])
        .await;
    cluster
        .sync_until(&session, "the receiver's echo", |events| {
            outputs(events)
                .iter()
                .any(|out| out == "integrate the atlas change")
        })
        .await;
    let present = cluster
        .exec(
            "worker-2",
            &[
                "git",
                "-C",
                &checkout(&definition, &receiver),
                "cat-file",
                "-e",
                &format!("{sha}^{{commit}}"),
            ],
            None,
        )
        .await;
    assert!(
        present.ok,
        "the commit must be present in worker-2's checkout, via the Relay: {}",
        present.stderr
    );

    // The door's other side: a machine with no identity — the origin
    // container holds none — is refused at the handshake, even with server
    // verification switched off, so the shares above passed on the workers'
    // certificates.
    let refused = cluster
        .exec(
            "origin",
            &[
                "git",
                "-c",
                "http.sslVerify=false",
                "ls-remote",
                "https://server:7779/atlas",
            ],
            None,
        )
        .await;
    assert!(!refused.ok, "no certificate, no Relay: {}", refused.stdout);
    let reason = refused.stderr.to_lowercase();
    assert!(
        ["ssl", "tls", "handshake", "certificate"]
            .iter()
            .any(|word| reason.contains(word)),
        "the refusal is the TLS handshake, not a missing repo: {}",
        refused.stderr
    );
}
