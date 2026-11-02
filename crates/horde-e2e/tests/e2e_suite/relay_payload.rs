//! A payload crossing two workers through the Relay (DESIGN §8), with every
//! process real: two worker daemons on separate state, the production Relay
//! served over its own TCP socket, and a commit that becomes present in the
//! receiving agent's checkout *before* the message names it — through the
//! Relay share ref, because it exists nowhere else the receiver can reach.
//!
//! # Nothing is stood in any more
//!
//! The Relay is the server's own (`--serve --relay-bind`, fix a10), its
//! mirror registry fed from the workspace catalog this scenario defines. The
//! sender's commit reaches the Relay because the sender *agent* reports its
//! result through its MCP endpoint (fix a1): the Worker's StampingSink
//! observes HEAD and stages the share ref — no test plumbing touches the
//! relay or the stamp.
//!
//! # Every verb is the CLI's own
//!
//! The workspace definition, the pinned session and spawn, and the
//! payload-carrying send all go through `horde`'s verbs (fix a12) — this
//! scenario is also the end-to-end proof of that surface.

use crate::support::{System, checkout_path, git, outputs, path_appears};
use horde_server_testkit::process::holds_commit;

/// Two workers, one Relay, one commit that only the Relay can carry.
#[tokio::test]
async fn a_payload_crosses_two_workers_via_the_relay() {
    let system = System::launch().await;

    // The origin repo both Instances clone from, with one seed commit.
    let origin = system.seeded_origin().await;

    // The server's own mounted Relay: its registry learns `atlas` from the
    // workspace definition created below, through the catalog follower.
    let relay_url = system.relay.clone();

    // The sender's script: one turn that reports through its MCP endpoint.
    // The stamp is the worker's, observed from the checkout — the script
    // declares no commits, by design.
    let script = system.state.path().join("report.json");
    std::fs::write(
        &script,
        r#"[{"steps": [
            {"use_horde": {"id": "call-r", "tool": "report_result",
                           "input": {"status": "success",
                                     "summary": "the finding is committed"}}}
        ], "text": "reported"}]"#,
    )
    .expect("the script writes");

    // Two daemons, each with its own workspace root and the shared Relay.
    let root_a = system.state.path().join("root-a");
    let root_b = system.state.path().join("root-b");
    let mut worker_a = system
        .spawn_worker(
            "w-a",
            &system.state.path().join("w-a"),
            &[
                "--workspace-root",
                &root_a.to_string_lossy(),
                "--relay",
                &relay_url,
                "--fake-script",
                &script.to_string_lossy(),
            ],
        )
        .await;
    let mut worker_b = system
        .spawn_worker(
            "w-b",
            &system.state.path().join("w-b"),
            &[
                "--workspace-root",
                &root_b.to_string_lossy(),
                "--relay",
                &relay_url,
            ],
        )
        .await;

    // The workspace, defined through the CLI's own verb (fix a12); the full
    // definition rides every MaterializeWorkspace command from here on.
    let repo_flag = format!("atlas={},path=atlas", origin.to_string_lossy());
    let definition_id = system
        .cli_field(
            &["workspace", "create", "atlas-ws", "--repo", &repo_flag],
            "workspace_definition_id",
        )
        .await;

    // The session's root agent is the sender, pinned to worker A with the
    // workspace; placement materializes the Instance on A before the spawn.
    let created = system
        .cli_json(&[
            "session",
            "new",
            "e2e payload",
            "--workspace",
            &definition_id,
            "--pin-worker",
            "w-a",
        ])
        .await;
    let session_id = created["session_id"]
        .as_str()
        .expect("a session id")
        .to_owned();
    let sender = created["root_agent_id"]
        .as_str()
        .expect("a root agent id")
        .to_owned();

    // The receiver, spawned under the sender but pinned to worker B.
    let spawned = system
        .cli_json(&[
            "spawn",
            &session_id,
            "--parent",
            &sender,
            "--workspace",
            &definition_id,
            "--pin-worker",
            "w-b",
        ])
        .await;
    let receiver = spawned["agent_id"]
        .as_str()
        .expect("an agent id")
        .to_owned();
    assert_eq!(
        spawned["placement"], "placed",
        "the pinned spawn must place, not queue: {spawned}"
    );

    // Both daemons materialized their Instances and produced checkouts.
    let sender_checkout = checkout_path(&root_a, &definition_id, &sender);
    let receiver_checkout = checkout_path(&root_b, &definition_id, &receiver);
    path_appears("the receiver's checkout", &receiver_checkout).await;
    system
        .checkout_composed(&session_id, &sender, &sender_checkout)
        .await;

    // The sender's work: a commit that exists only on worker A. Then the
    // real report: a message wakes the sender, whose scripted turn calls
    // `report_result` through its own MCP endpoint — the Worker's
    // StampingSink observes this commit and stages it onto the server's
    // Relay.
    std::fs::write(sender_checkout.join("finding.md"), "the atlas moved\n")
        .expect("the finding writes");
    git(&sender_checkout, &["add", "."]).await;
    git(&sender_checkout, &["commit", "-m", "the finding"]).await;
    let commit_sha = git(&sender_checkout, &["rev-parse", "HEAD"]).await;

    system
        .cli_json(&["send", &sender, "report your result"])
        .await;
    system
        .sync_until(&session_id, "the endpoint-reported result", |events| {
            events.iter().any(|event| {
                event["kind"] == "result_reported"
                    && event["agent_id"].as_str() == Some(sender.as_str())
            })
        })
        .await;
    assert!(
        !holds_commit(&origin, &commit_sha).await,
        "the commit must not have leaked to origin: only the relay may carry it"
    );

    // The payload-carrying delivery. The server folds it into a
    // DeliverMessage for worker B, whose broker must make the commit present
    // before the agent reads the message — named through the CLI's own
    // --git-ref flag (fix a12).
    system
        .cli_json(&[
            "send",
            &receiver,
            "integrate the atlas change",
            "--git-ref",
            &format!("atlas:{commit_sha}"),
        ])
        .await;

    // The delivery reached the agent: its echo is in the log. The obtain
    // precedes the delivery by construction, so the echo is also the receipt
    // for the transfer.
    system
        .sync_until(&session_id, "the receiver's echo", |events| {
            outputs(events)
                .iter()
                .any(|text| text == "integrate the atlas change")
        })
        .await;
    assert!(
        holds_commit(&receiver_checkout, &commit_sha).await,
        "the commit must be present in the receiving checkout, via the relay"
    );

    for worker in [&mut worker_a, &mut worker_b] {
        worker.terminate().await.expect("SIGTERM sends");
    }
    for worker in [worker_a, worker_b] {
        let (status, _, _) = worker.wait().await.expect("the worker exits");
        assert!(status.success());
    }
}
