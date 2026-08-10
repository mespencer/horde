//! The contracts the services promise, asserted rather than assumed.
//!
//! Every test here exists because a mutation survived: a stub that fabricated
//! success, a handshake that returned an empty struct, an effect nothing looked
//! at. The pattern in all of them is the same — the previous tests checked that
//! a call *returned* something, never that the something was true.

use horde_proto::v1::{
    AddWorkspaceRepoRequest, AgentId, ArchiveSessionRequest, Assignment,
    CreateWorkspaceDefinitionRequest, DeleteWorkspaceDefinitionRequest,
    GetWorkspaceDefinitionRequest, HelloRequest, ListSessionsRequest, ListWorkersRequest,
    ListWorkspaceDefinitionsRequest, MintEnrollmentCodeRequest, PrewarmWorkspaceRequest,
    QueryUsageRequest, RegisterWorkerRequest, ReportCapabilitiesRequest,
    ReportWorkspaceInstancesRequest, RevokeEnrollmentRequest, SendMessageRequest, SessionCursor,
    SessionId, SyncEventsRequest, UpdateWorkspaceDefinitionRequest, WorkerCapabilities, WorkerId,
    send_message_request, sync_events_response,
};
use tokio_stream::StreamExt;
use tonic::Code;

mod common;

use common::{Harness, Setup, connect, seed};

/// Every sequence in a session, read back through SyncEvents.
async fn sequences(harness: &mut Harness, session_id: &str) -> Setup<Vec<u64>> {
    drain_sync(harness, session_id, |batch, out| {
        out.extend(batch.events.iter().map(|event| event.sequence));
    })
    .await
}

/// Replays a session to its caught-up marker, keeping whatever `collect` takes.
///
/// One cursor-and-drain loop rather than one per thing a test wants out of the
/// stream: the duplication gate caught the second copy, and a second copy of a
/// stream loop is also a second chance to get the terminator wrong.
async fn drain_sync<T>(
    harness: &mut Harness,
    session_id: &str,
    mut collect: impl FnMut(&horde_proto::v1::EventBatch, &mut Vec<T>),
) -> Setup<Vec<T>> {
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

    let mut out = Vec::new();
    while let Some(message) = stream.next().await {
        match message?.update {
            Some(sync_events_response::Update::Batch(batch)) => collect(&batch, &mut out),
            Some(sync_events_response::Update::CaughtUp(_)) => break,
            _ => {}
        }
    }
    Ok(out)
}

/// Registers a worker with the given tags.
///
/// Three tests need a registered worker before they can assert anything about
/// listings or capability refreshes.
async fn register(h: &mut Harness, worker: &str, tags: &[&str]) -> Setup<()> {
    h.worker
        .register_worker(RegisterWorkerRequest {
            hello: None,
            worker_id: Some(WorkerId::new(worker)),
            capabilities: Some(WorkerCapabilities {
                tags: tags.iter().map(|tag| (*tag).to_owned()).collect(),
                ..WorkerCapabilities::default()
            }),
        })
        .await?;
    Ok(())
}

/// The tags the listing reports for a worker.
async fn listed_tags(h: &mut Harness, worker: &str) -> Setup<Vec<String>> {
    let workers = h
        .control
        .list_workers(ListWorkersRequest::default())
        .await?
        .into_inner()
        .workers;
    Ok(workers
        .into_iter()
        .find(|listed| {
            listed
                .worker_id
                .as_ref()
                .map(ToString::to_string)
                .as_deref()
                == Some(worker)
        })
        .and_then(|listed| listed.capabilities)
        .map(|capabilities| capabilities.tags)
        .unwrap_or_default())
}

/// The `recorded_at` stamps a session's events carry.
///
/// Shares the cursor-and-drain shape with [`sequences`]; both walk one sync
/// stream to its caught-up marker and differ only in what they keep.
async fn recorded_stamps(
    harness: &mut Harness,
    session_id: &str,
) -> Setup<Vec<prost_types::Timestamp>> {
    drain_sync(harness, session_id, |batch, out| {
        out.extend(batch.events.iter().filter_map(|event| event.recorded_at));
    })
    .await
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

    register(&mut h, "worker-a", &["gpu"])
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
        listed_tags(&mut h, "worker-a").await.expect("tags"),
        vec!["gpu".to_owned()]
    );
}

/// Capabilities are replaced wholesale, so a refresh must be observable —
/// otherwise the scheduler matches on facts that stopped being true.
#[tokio::test]
async fn refreshing_capabilities_replaces_what_the_listing_reports() {
    let mut h = connect().await.expect("a harness");

    register(&mut h, "worker-a", &["gpu"])
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

    assert_eq!(
        listed_tags(&mut h, "worker-a").await.expect("tags"),
        vec!["macos".to_owned()],
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

    let stamps = recorded_stamps(&mut h, &session_id)
        .await
        .expect("sync must succeed");

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
