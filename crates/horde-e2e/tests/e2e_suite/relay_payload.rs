//! A payload crossing two workers through the Relay (DESIGN §8), with every
//! process real: two worker daemons on separate state, the production Relay
//! served over its own TCP socket, and a commit that becomes present in the
//! receiving agent's checkout *before* the message names it — through the
//! Relay share ref, because it exists nowhere else the receiver can reach.
//!
//! # The one stood-in step, named
//!
//! The sender's half of stamping — `horde-mcp`'s `report_result` flowing
//! through the Worker's `StampingSink` — is not wired into the daemon yet
//! (the Runtime's spawn path still passes `horde_endpoint: None`; see
//! `runtime.rs`). Until that lands, this scenario stages the sender's commit
//! onto the Relay with the *same production call* the stamp path makes
//! (`horde_relay::stage_share`, `ViaRelay`), from the sender agent's real
//! checkout on worker A's disk. Everything else — the workspace definition,
//! both materializations, both spawns, the payload-carrying delivery, and
//! the receiver-side obtain through the Relay — is the product, uninvented.
//!
//! # Why some verbs go over the raw client
//!
//! The CLI has no workspace or payload verbs yet, so the workspace
//! definition, the pinned spawns and the payload-carrying send speak the
//! production control-plane RPCs directly, as the operator's enrolled
//! identity. That is a gap in the CLI's surface (flagged in the wave
//! report), not in the system under test.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use horde_proto::v1::control_plane_service_client::ControlPlaneServiceClient;
use horde_proto::v1::{
    AgentId, AgentSpec, Assignment, CreateSessionRequest, CreateWorkspaceDefinitionRequest,
    DeliveryMode, GitRef, Payload, PlacementConstraints, PlacementState, SendMessageRequest,
    SpawnAgentRequest, WorkerId, WorkspaceRepo, send_message_request,
};

use crate::support::{System, git, outputs};

/// A control-plane client speaking as the operator — the production wire
/// surface, over the same mTLS channel the CLI uses.
async fn control_plane(system: &System) -> ControlPlaneServiceClient<tonic::transport::Channel> {
    let identity =
        horde_identity::StoredIdentity::load(&system.operator).expect("the operator identity");
    let channel = tonic::transport::Endpoint::from_shared(system.main.clone())
        .expect("a usable endpoint")
        .tls_config(horde_identity::tls::grpc_client_mtls(
            &identity,
            "localhost",
        ))
        .expect("the identity is usable for TLS")
        .connect()
        .await
        .expect("the operator dials the control plane");
    ControlPlaneServiceClient::new(channel)
}

/// Waits until `path` exists on disk — the observable half of a materialize
/// or checkout that happened inside a worker daemon. Bounded.
async fn path_appears(what: &str, path: &Path) {
    for _ in 0..300_u32 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("{what} never appeared at {}", path.display());
}

use horde_server_testkit::process::holds_commit;

/// The agent-checkout path the worker's WorkspaceManager composes:
/// `<workspace root>/<definition id>/.horde-agents/<agent>/<layout path>`.
fn checkout_path(root: &Path, definition_id: &str, agent_id: &str) -> PathBuf {
    root.join(definition_id)
        .join(".horde-agents")
        .join(agent_id)
        .join("atlas")
}

/// Two workers, one Relay, one commit that only the Relay can carry.
#[tokio::test]
async fn a_payload_crosses_two_workers_via_the_relay() {
    let system = System::launch().await;

    // The origin repo both Instances clone from, with one seed commit.
    let origin = system.state.path().join("origin");
    std::fs::create_dir_all(&origin).expect("the origin dir");
    git(&origin, &["init"]).await;
    std::fs::write(origin.join("README.md"), "atlas\n").expect("the seed file");
    git(&origin, &["add", "."]).await;
    git(&origin, &["commit", "-m", "seed"]).await;

    // The production Relay, served for real on its own socket; the suite is
    // only its host process, the way the server binary will be one day.
    let mirrors = horde_relay::MirrorStore::new(system.state.path().join("mirrors"));
    mirrors
        .register("atlas", &origin.to_string_lossy())
        .expect("the origin registers");
    let relay = horde_relay::RelayServer::bind(
        std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        Arc::new(mirrors),
    )
    .await
    .expect("the relay binds loopback");
    let relay_url = format!("http://{}", relay.addr());

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

    // The workspace, defined once at the control plane; the full definition
    // rides every MaterializeWorkspace command from here on.
    let mut client = control_plane(&system).await;
    let definition = client
        .create_workspace_definition(CreateWorkspaceDefinitionRequest {
            name: "atlas-ws".to_owned(),
            repos: vec![WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: origin.to_string_lossy().into_owned(),
                default_branch: "main".to_owned(),
                layout_path: "atlas".to_owned(),
            }],
            unmanaged_paths: Vec::new(),
        })
        .await
        .expect("the definition creates")
        .into_inner()
        .definition
        .expect("a definition comes back");
    let definition_id = definition
        .workspace_definition_id
        .as_ref()
        .expect("a definition id")
        .to_string();

    // The session's root agent is the sender, pinned to worker A with the
    // workspace; placement materializes the Instance on A before the spawn.
    let pinned = |worker: &str| {
        Some(AgentSpec {
            workspace_definition_id: definition.workspace_definition_id.clone(),
            placement: Some(PlacementConstraints {
                pinned_worker_id: Some(WorkerId::new(worker)),
                ..PlacementConstraints::default()
            }),
            ..AgentSpec::default()
        })
    };
    let created = client
        .create_session(CreateSessionRequest {
            title: "e2e payload".to_owned(),
            workspace_definition_id: definition.workspace_definition_id.clone(),
            root_agent: pinned("w-a"),
            idempotency_key: "e2e-payload".to_owned(),
        })
        .await
        .expect("the session creates")
        .into_inner();
    let summary = created.session.expect("a session summary");
    let session_id = summary
        .session_id
        .as_ref()
        .expect("a session id")
        .to_string();
    let sender = summary
        .root_agent_id
        .as_ref()
        .expect("a root agent id")
        .to_string();

    // The receiver, spawned under the sender but pinned to worker B.
    let spawned = client
        .spawn_agent(SpawnAgentRequest {
            session_id: summary.session_id.clone(),
            parent_agent_id: Some(AgentId::new(sender.clone())),
            spec: pinned("w-b"),
            idempotency_key: "e2e-payload-receiver".to_owned(),
        })
        .await
        .expect("the receiver spawns")
        .into_inner();
    let receiver = spawned.agent_id.as_ref().expect("an agent id").to_string();
    let placement = spawned.placement.expect("a placement answer");
    assert_eq!(
        placement.state,
        i32::from(PlacementState::Placed),
        "the pinned spawn must place, not queue: {}",
        placement.reason
    );

    // Both daemons materialized their Instances and produced checkouts.
    let sender_checkout = checkout_path(&root_a, &definition_id, &sender);
    let receiver_checkout = checkout_path(&root_b, &definition_id, &receiver);
    path_appears("the sender's checkout", &sender_checkout).await;
    path_appears("the receiver's checkout", &receiver_checkout).await;

    // The sender's work: a commit that exists only on worker A. Then the
    // stand-in for the not-yet-wired MCP report: the production staging
    // call pushes it to its content-addressed share ref on the Relay.
    std::fs::write(sender_checkout.join("finding.md"), "the atlas moved\n")
        .expect("the finding writes");
    git(&sender_checkout, &["add", "."]).await;
    git(&sender_checkout, &["commit", "-m", "the finding"]).await;
    let commit_sha = git(&sender_checkout, &["rev-parse", "HEAD"]).await;
    horde_relay::stage_share(
        &sender_checkout,
        &horde_relay::ShareRoute::ViaRelay {
            repo_name: "atlas".to_owned(),
        },
        Some(&relay.location()),
        &commit_sha,
    )
    .await
    .expect("the stamp path's staging call pushes the share ref");
    assert!(
        !holds_commit(&origin, &commit_sha).await,
        "the commit must not have leaked to origin: only the relay may carry it"
    );

    // The payload-carrying delivery. The server folds it into a
    // DeliverMessage for worker B, whose broker must make the commit present
    // before the agent reads the message.
    client
        .send_message(SendMessageRequest {
            to_agent_id: Some(AgentId::new(receiver.clone())),
            body: Some(send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: "integrate the atlas change".to_owned(),
                payload: Some(Payload {
                    git_refs: vec![GitRef {
                        repo_name: "atlas".to_owned(),
                        commit_sha: commit_sha.clone(),
                        branch: String::new(),
                    }],
                    attachments: Vec::new(),
                }),
            })),
            delivery: DeliveryMode::TurnBoundary.into(),
            idempotency_key: "e2e-payload-send".to_owned(),
        })
        .await
        .expect("the payload message sends");

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
