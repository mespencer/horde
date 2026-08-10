//! The contracts the services promise, asserted rather than assumed.
//!
//! Every test here exists because a mutation survived: a stub that fabricated
//! success, a handshake that returned an empty struct, an effect nothing looked
//! at. The pattern in all of them is the same — the previous tests checked that
//! a call *returned* something, never that the something was true.

use horde_proto::v1::{
    AddWorkspaceRepoRequest, AgentId, AgentSpec, ArchiveSessionRequest, Assignment,
    CreateSessionRequest, CreateWorkspaceDefinitionRequest, DeleteWorkspaceDefinitionRequest,
    GetWorkspaceDefinitionRequest, HelloRequest, ListSessionsRequest, ListWorkersRequest,
    ListWorkspaceDefinitionsRequest, MintEnrollmentCodeRequest, PrewarmWorkspaceRequest,
    QueryUsageRequest, RegisterWorkerRequest, ReportCapabilitiesRequest,
    ReportWorkspaceInstancesRequest, RevokeEnrollmentRequest, SendMessageRequest, SessionCursor,
    SessionId, SyncEventsRequest, UpdateWorkspaceDefinitionRequest, WorkerCapabilities, WorkerId,
    control_plane_service_client::ControlPlaneServiceClient,
    control_plane_service_server::ControlPlaneServiceServer, send_message_request,
    sync_events_response, worker_service_client::WorkerServiceClient,
    worker_service_server::WorkerServiceServer,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::transport::{Endpoint, Server, Uri};

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Harness {
    control: ControlPlaneServiceClient<tonic::transport::Channel>,
    worker: WorkerServiceClient<tonic::transport::Channel>,
}

async fn connect() -> Setup<Harness> {
    let core = ServerCore::new(EventLog::in_memory()?);
    Ok(Harness {
        control: ControlPlaneServiceClient::new(channel_to(core.clone(), true).await?),
        worker: WorkerServiceClient::new(channel_to(core, false).await?),
    })
}

async fn channel_to(core: ServerCore, control_plane: bool) -> Setup<tonic::transport::Channel> {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);

    tokio::spawn(async move {
        let mut builder = Server::builder();
        let serve = async move {
            if control_plane {
                builder
                    .add_service(ControlPlaneServiceServer::new(ControlPlane::new(core)))
                    .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_side)))
                    .await
            } else {
                builder
                    .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
                    .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server_side)))
                    .await
            }
        };
        let _ = serve.await;
    });

    let mut client_side = Some(client_side);
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let stream = client_side.take();
            async move {
                stream
                    .map(hyper_util::rt::TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("the duplex was already taken"))
            }
        }))
        .await?;
    Ok(channel)
}

/// Creates a session, returning its id and root agent id.
async fn seed(harness: &mut Harness, title: &str, key: &str) -> Setup<(String, String)> {
    let created = harness
        .control
        .create_session(CreateSessionRequest {
            title: title.to_owned(),
            workspace_definition_id: None,
            root_agent: Some(AgentSpec::default()),
            idempotency_key: key.to_owned(),
        })
        .await?
        .into_inner();

    let session_id = created
        .session
        .and_then(|summary| summary.session_id)
        .map(|id| id.to_string())
        .ok_or("a created session must have an id")?;
    let root = created
        .root_agent_id
        .map(|id| id.to_string())
        .ok_or("a created session must have a root agent")?;
    Ok((session_id, root))
}

/// Every sequence in a session, read back through SyncEvents.
async fn sequences(harness: &mut Harness, session_id: &str) -> Setup<Vec<u64>> {
    let mut stream = harness
        .control
        .sync_events(SyncEventsRequest {
            cursors: vec![SessionCursor {
                session_id: Some(SessionId::new(session_id)),
                from_sequence: 0,
            }],
            all_sessions: false,
            subtree_root: None,
            follow: false,
            max_batch_events: 0,
        })
        .await?
        .into_inner();

    let mut seen = Vec::new();
    while let Some(message) = stream.next().await {
        match message?.update {
            Some(sync_events_response::Update::Batch(batch)) => {
                seen.extend(batch.events.iter().map(|event| event.sequence));
            }
            Some(sync_events_response::Update::CaughtUp(_)) => break,
            _ => {}
        }
    }
    Ok(seen)
}

// -- Honest refusals ---------------------------------------------------------

/// An unbuilt RPC must refuse. Every one of these had a mutant replacing the
/// refusal with `Ok(Default::default())`, which is precisely the fabricated
/// success the design forbids: a client cannot tell an empty answer from an
/// unbuilt feature.
#[tokio::test]
async fn every_unbuilt_control_plane_rpc_refuses_rather_than_fabricating_success() {
    let mut h = connect().await.expect("a harness");

    macro_rules! refuses {
        ($call:expr, $what:literal) => {{
            let status = $call.await.expect_err(concat!($what, " is not built"));
            assert_eq!(
                status.code(),
                Code::Unimplemented,
                concat!($what, " must refuse as Unimplemented, got: {}"),
                status.message()
            );
            assert!(
                !status.message().is_empty(),
                concat!($what, " must say what is not built")
            );
        }};
    }

    refuses!(
        h.control
            .create_workspace_definition(CreateWorkspaceDefinitionRequest::default()),
        "create_workspace_definition"
    );
    refuses!(
        h.control
            .get_workspace_definition(GetWorkspaceDefinitionRequest::default()),
        "get_workspace_definition"
    );
    refuses!(
        h.control
            .list_workspace_definitions(ListWorkspaceDefinitionsRequest::default()),
        "list_workspace_definitions"
    );
    refuses!(
        h.control
            .update_workspace_definition(UpdateWorkspaceDefinitionRequest::default()),
        "update_workspace_definition"
    );
    refuses!(
        h.control
            .add_workspace_repo(AddWorkspaceRepoRequest::default()),
        "add_workspace_repo"
    );
    refuses!(
        h.control
            .delete_workspace_definition(DeleteWorkspaceDefinitionRequest::default()),
        "delete_workspace_definition"
    );
    refuses!(
        h.control
            .prewarm_workspace(PrewarmWorkspaceRequest::default()),
        "prewarm_workspace"
    );
    refuses!(
        h.control
            .mint_enrollment_code(MintEnrollmentCodeRequest::default()),
        "mint_enrollment_code"
    );
    refuses!(
        h.control
            .revoke_enrollment(RevokeEnrollmentRequest::default()),
        "revoke_enrollment"
    );
    refuses!(
        h.control.query_usage(QueryUsageRequest::default()),
        "query_usage"
    );
}

#[tokio::test]
async fn reporting_workspace_instances_refuses_rather_than_pretending_to_record_them() {
    let mut h = connect().await.expect("a harness");

    let status = h
        .worker
        .report_workspace_instances(ReportWorkspaceInstancesRequest::default())
        .await
        .expect_err("workspace instances are not built");
    assert_eq!(status.code(), Code::Unimplemented);
    assert!(status.message().contains("workspace instance"));
}

// -- Handshake ---------------------------------------------------------------

/// A server that will not name its version cannot be version-skewed against,
/// which is the whole point of the handshake (DESIGN.md §2).
#[tokio::test]
async fn the_control_plane_handshake_names_a_real_version() {
    let mut h = connect().await.expect("a harness");

    let server = h
        .control
        .hello(HelloRequest::default())
        .await
        .expect("hello must succeed")
        .into_inner()
        .server
        .expect("a handshake carries a ServerHello");

    assert_eq!(server.semver, env!("CARGO_PKG_VERSION"));
    assert!(!server.min_supported_semver.is_empty());
}

#[tokio::test]
async fn the_worker_handshake_names_the_same_version() {
    let mut h = connect().await.expect("a harness");

    let registered = h
        .worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities::default()),
        })
        .await
        .expect("registration must succeed")
        .into_inner();

    let server = registered
        .server
        .expect("registration carries a ServerHello");
    assert_eq!(server.semver, env!("CARGO_PKG_VERSION"));
    assert!(!server.min_supported_semver.is_empty());
    assert_eq!(
        registered.resume_from_journal_sequence, 1,
        "a fresh worker starts at the first journal position"
    );
}

// -- Effects that nothing was looking at -------------------------------------

/// A registered worker must be visible, or a connected worker is
/// indistinguishable from none.
#[tokio::test]
async fn a_registered_worker_appears_in_the_listing_with_its_capabilities() {
    let mut h = connect().await.expect("a harness");

    assert!(
        h.control
            .list_workers(ListWorkersRequest::default())
            .await
            .expect("list must succeed")
            .into_inner()
            .workers
            .is_empty(),
        "nothing has registered yet"
    );

    h.worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities {
                tags: vec!["gpu".to_owned()],
                ..WorkerCapabilities::default()
            }),
        })
        .await
        .expect("registration");

    let workers = h
        .control
        .list_workers(ListWorkersRequest::default())
        .await
        .expect("list")
        .into_inner()
        .workers;

    assert_eq!(workers.len(), 1);
    assert_eq!(
        workers[0].worker_id.as_ref().map(ToString::to_string),
        Some("worker-a".to_owned())
    );
    assert_eq!(
        workers[0]
            .capabilities
            .as_ref()
            .map(|capabilities| capabilities.tags.clone()),
        Some(vec!["gpu".to_owned()])
    );
}

/// Capabilities are replaced wholesale, so a refresh must be observable —
/// otherwise the scheduler matches on facts that stopped being true.
#[tokio::test]
async fn refreshing_capabilities_replaces_what_the_listing_reports() {
    let mut h = connect().await.expect("a harness");

    h.worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities {
                tags: vec!["gpu".to_owned()],
                ..WorkerCapabilities::default()
            }),
        })
        .await
        .expect("registration");

    h.worker
        .report_capabilities(ReportCapabilitiesRequest {
            worker_id: Some(WorkerId::new("worker-a")),
            capabilities: Some(WorkerCapabilities {
                tags: vec!["macos".to_owned()],
                ..WorkerCapabilities::default()
            }),
        })
        .await
        .expect("a capability refresh must succeed");

    let workers = h
        .control
        .list_workers(ListWorkersRequest::default())
        .await
        .expect("list")
        .into_inner()
        .workers;

    assert_eq!(
        workers[0]
            .capabilities
            .as_ref()
            .map(|capabilities| capabilities.tags.clone()),
        Some(vec!["macos".to_owned()]),
        "a refresh replaces rather than merges, and must actually take effect"
    );
}

#[tokio::test]
async fn archiving_appends_an_event_and_reports_the_archived_session() {
    let mut h = connect().await.expect("a harness");
    let (session_id, _) = seed(&mut h, "temporary", "key-1").await.expect("seed");

    let before = sequences(&mut h, &session_id).await.expect("replay").len();

    let summary = h
        .control
        .archive_session(ArchiveSessionRequest {
            session_id: Some(SessionId::new(session_id.clone())),
            reason: "done".to_owned(),
        })
        .await
        .expect("archive must succeed")
        .into_inner()
        .session
        .expect("archiving reports the session it archived");

    assert_eq!(
        summary.session_id.as_ref().map(ToString::to_string),
        Some(session_id.clone()),
        "an empty summary would tell the caller nothing about what it archived"
    );
    assert_eq!(
        summary.state,
        horde_proto::v1::SessionLifecycleState::Archived as i32
    );
    assert_eq!(
        sequences(&mut h, &session_id).await.expect("replay").len(),
        before + 1,
        "archiving is an event like everything else"
    );
}

#[tokio::test]
async fn sending_a_message_reports_the_position_it_appended_at() {
    let mut h = connect().await.expect("a harness");
    let (session_id, root) = seed(&mut h, "messages", "key-1").await.expect("seed");

    let response = h
        .control
        .send_message(SendMessageRequest {
            to_agent_id: Some(AgentId::new(root)),
            body: Some(send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: "fix the build".to_owned(),
                payload: None,
            })),
            delivery: 0,
            idempotency_key: String::new(),
        })
        .await
        .expect("send must succeed")
        .into_inner();

    assert!(
        response.assignment_id.is_some(),
        "an assignment owes a result, so it must be given an id"
    );
    assert_eq!(
        response.session_id.as_ref().map(ToString::to_string),
        Some(session_id.clone())
    );

    let seen = sequences(&mut h, &session_id).await.expect("replay");
    assert_eq!(
        seen.last().copied(),
        Some(response.sequence),
        "the reported sequence must be where the event actually landed"
    );
}

/// `include_archived` is a filter, and a negation error would either hide
/// everything or hide nothing.
#[tokio::test]
async fn archived_sessions_are_hidden_by_default_and_shown_on_request() {
    let mut h = connect().await.expect("a harness");
    let (archived, _) = seed(&mut h, "old", "key-1").await.expect("seed");
    let (_kept, _) = seed(&mut h, "current", "key-2").await.expect("seed");

    h.control
        .archive_session(ArchiveSessionRequest {
            session_id: Some(SessionId::new(archived.clone())),
            reason: String::new(),
        })
        .await
        .expect("archive");

    let default = h
        .control
        .list_sessions(ListSessionsRequest::default())
        .await
        .expect("list")
        .into_inner()
        .sessions;
    assert_eq!(default.len(), 1, "the archived one is hidden");
    assert_ne!(
        default[0].session_id.as_ref().map(ToString::to_string),
        Some(archived.clone())
    );

    let all = h
        .control
        .list_sessions(ListSessionsRequest {
            include_archived: true,
            ..ListSessionsRequest::default()
        })
        .await
        .expect("list")
        .into_inner()
        .sessions;
    assert_eq!(all.len(), 2, "asking for archived shows both");
}

/// Server-assigned ids must actually distinguish things. A constant id would
/// make two sessions collide in the log.
#[tokio::test]
async fn server_assigned_ids_are_unique_and_non_empty() {
    let mut h = connect().await.expect("a harness");

    let (first, first_root) = seed(&mut h, "one", "").await.expect("seed");
    let (second, second_root) = seed(&mut h, "two", "").await.expect("seed");

    assert!(!first.is_empty() && !second.is_empty());
    assert_ne!(
        first, second,
        "two sessions created without idempotency keys must not share an id"
    );
    assert_ne!(first_root, second_root);

    assert_eq!(
        h.control
            .list_sessions(ListSessionsRequest::default())
            .await
            .expect("list")
            .into_inner()
            .sessions
            .len(),
        2,
        "colliding ids would silently merge the two sessions"
    );
}

/// `recorded_at` is the server's clock, and a client renders and reconciles
/// skew against it. A constant would make every event look simultaneous.
#[tokio::test]
async fn events_are_stamped_with_a_plausible_server_clock() {
    let mut h = connect().await.expect("a harness");
    let (session_id, _) = seed(&mut h, "clock", "key-1").await.expect("seed");

    let mut stream = h
        .control
        .sync_events(SyncEventsRequest {
            cursors: vec![SessionCursor {
                session_id: Some(SessionId::new(session_id)),
                from_sequence: 0,
            }],
            all_sessions: false,
            subtree_root: None,
            follow: false,
            max_batch_events: 0,
        })
        .await
        .expect("sync")
        .into_inner();

    let mut stamps = Vec::new();
    while let Some(message) = stream.next().await {
        match message.expect("sync must not error").update {
            Some(sync_events_response::Update::Batch(batch)) => {
                stamps.extend(batch.events.iter().filter_map(|event| event.recorded_at));
            }
            Some(sync_events_response::Update::CaughtUp(_)) => break,
            _ => {}
        }
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("a clock after the epoch")
        .as_secs() as i64;

    assert!(!stamps.is_empty(), "events must carry a recorded_at");
    for stamp in stamps {
        // A day either side: wide enough not to flake, narrow enough that 0, 1
        // or -1 fail.
        assert!(
            (stamp.seconds - now).abs() < 86_400,
            "recorded_at {} is not a plausible clock reading near {now}",
            stamp.seconds
        );
    }
}
