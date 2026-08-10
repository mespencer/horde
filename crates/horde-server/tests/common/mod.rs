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
//! Each test binary compiles its own copy of this module and uses a subset of
//! it, so items unused by one file are dead code there. The allow is scoped to
//! this module rather than sprinkled at the call sites, which is the
//! conventional shape for an integration-test `common` module.
#![allow(dead_code, unreachable_pub)]

use horde_proto::v1::{
    AgentSpec, CreateSessionRequest, control_plane_service_client::ControlPlaneServiceClient,
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

/// A harness over a fresh core.
pub async fn connect() -> Setup<Harness> {
    connect_to(core()?).await
}

/// A harness over a caller-supplied core, for tests that also hold it directly.
pub async fn connect_to(core: ServerCore) -> Setup<Harness> {
    Ok(Harness {
        control: ControlPlaneServiceClient::new(channel_to(core.clone(), true).await?),
        worker: WorkerServiceClient::new(channel_to(core, false).await?),
    })
}

/// Stands up one server on a duplex pair and returns a channel to it.
///
/// Two connections rather than one so each client gets its own transport, which
/// is how they behave in production.
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
