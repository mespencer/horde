//! The in-process server harness, shared by every integration test.
//!
//! Both service test files stood up their own copy of this, which the
//! duplication gate caught. Sharing it is not only about the gate: two copies
//! of a transport harness drift, and a test that fails because its private
//! harness differs from the other one is the worst kind of red.
//!
//! In-process rather than over a socket: the transport is a duplex stream, so
//! there is no port to bind (DESIGN.md §3 forbids binding beyond localhost
//! before auth exists) and no flake from a port race. Everything above the
//! socket — codegen, framing, streaming, status mapping — is the real thing.
//!
//! Not every sibling suite uses every item here, so some items would be
//! dead code without the allow. It is scoped to this module rather than
//! sprinkled at the call sites, which is the conventional shape for an
//! integration-test `common` module.
#![allow(dead_code, unreachable_pub)]

use horde_proto::v1::{
    AgentSpec, CreateSessionRequest, SessionCursor, SessionId, SyncEventsRequest,
    control_plane_service_client::ControlPlaneServiceClient,
    control_plane_service_server::ControlPlaneServiceServer,
    worker_service_client::WorkerServiceClient, worker_service_server::WorkerServiceServer,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tonic::transport::{Endpoint, Server, Uri};

/// Setup failures are returned rather than unwrapped.
///
/// `allow-expect-in-tests` exempts anything under `#[cfg(test)]`, which an
/// integration test file's free functions are not — so `expect` here would trip
/// the deny-list. Threading the error to a `#[test]` also reports *which* setup
/// step failed.
pub type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Both clients, wired to one server over in-process duplex transports.
pub struct Harness {
    pub control: ControlPlaneServiceClient<tonic::transport::Channel>,
    pub worker: WorkerServiceClient<tonic::transport::Channel>,
}

/// A core over a private in-memory log.
pub fn core() -> Setup<ServerCore> {
    Ok(ServerCore::new(EventLog::in_memory()?))
}

/// The versioned Hello every registration must carry since the a6 gate —
/// one helper, so six suites cannot each grow their own copy.
pub fn versioned_hello() -> Option<horde_proto::v1::Hello> {
    Some(horde_proto::v1::Hello {
        semver: "0.1.0".to_owned(),
        ..horde_proto::v1::Hello::default()
    })
}

/// A default-capability registration for `worker`, versioned to pass the
/// gate — the whole request, for the suites that need nothing bespoke.
pub fn plain_registration(worker: &str) -> horde_proto::v1::RegisterWorkerRequest {
    horde_proto::v1::RegisterWorkerRequest {
        hello: versioned_hello(),
        worker_id: Some(horde_proto::v1::WorkerId::new(worker)),
        capabilities: Some(horde_proto::v1::WorkerCapabilities::default()),
    }
}

/// A harness over a fresh core.
pub async fn connect() -> Setup<Harness> {
    connect_to(core()?).await
}

/// A harness over a caller-supplied core, for tests that also hold it directly.
pub async fn connect_to(core: ServerCore) -> Setup<Harness> {
    let plane = ControlPlane::new(core.clone());
    connect_with_plane(plane, core).await
}

/// A harness whose control plane the caller configured — a shortened
/// heartbeat, say — over the same core the worker spine serves.
pub async fn connect_with_plane(plane: ControlPlane, core: ServerCore) -> Setup<Harness> {
    Ok(Harness {
        control: ControlPlaneServiceClient::new(
            duplex_channel(move |server_side| {
                tokio::spawn(async move {
                    let _ = Server::builder()
                        .add_service(ControlPlaneServiceServer::new(plane))
                        .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(
                            server_side,
                        )))
                        .await;
                });
            })
            .await?,
        ),
        worker: WorkerServiceClient::new(
            duplex_channel(move |server_side| {
                tokio::spawn(async move {
                    let _ = Server::builder()
                        .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
                        .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(
                            server_side,
                        )))
                        .await;
                });
            })
            .await?,
        ),
    })
}

/// Stands up one service on a duplex pair and returns a channel to it.
///
/// Each client gets its own transport, which is how they behave in
/// production; `serve` is handed the server half and owns starting it.
async fn duplex_channel(
    serve: impl FnOnce(tokio::io::DuplexStream),
) -> Setup<tonic::transport::Channel> {
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    serve(server_side);

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

/// Opens a sync stream over one session — the resilience primitive every
/// suite reads through, kept here so there is exactly one copy of the cursor
/// arithmetic to get wrong.
pub async fn open_sync(
    control: &mut ControlPlaneServiceClient<tonic::transport::Channel>,
    session_id: &str,
    from_sequence: u64,
    follow: bool,
) -> Setup<tonic::Streaming<horde_proto::v1::SyncEventsResponse>> {
    Ok(control
        .sync_events(SyncEventsRequest {
            cursors: vec![SessionCursor {
                session_id: Some(SessionId::new(session_id)),
                from_sequence,
            }],
            all_sessions: false,
            subtree_root: None,
            follow,
            max_batch_events: 0,
        })
        .await?
        .into_inner())
}

/// Creates a session, returning its id and its root agent's id.
///
/// The preamble most tests need before they can test anything else.
pub async fn seed(harness: &mut Harness, title: &str, key: &str) -> Setup<(String, String)> {
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
