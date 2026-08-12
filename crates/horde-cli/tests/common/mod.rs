//! The server harness every `horde-cli` integration test runs against.
//!
//! All three test files stood up their own copy of this, which the duplication
//! gate caught. Sharing it is not only about the gate: two copies of a
//! transport harness drift, and a test that fails because its private harness
//! differs from its neighbour's is the worst kind of red. `horde-server`'s
//! `tests/common/mod.rs` is the same module, split out for the same reason.
//!
//! **One transport, not two.** An earlier shape had the in-process suites on a
//! `tokio::io::duplex` pair and only the subprocess suite on a socket. A
//! subprocess cannot be handed a duplex stream — it has no way to inherit one —
//! so that split was forced rather than chosen, and it cost a second harness
//! plus a verbatim copy of `horde-server`'s duplex plumbing. A loopback socket
//! serves both, so there is one harness here.
//!
//! Binding is `127.0.0.1:0`: a real socket, loopback only, with the port chosen
//! by the OS so tests running in parallel cannot collide. DESIGN.md §3 forbids
//! binding beyond localhost until auth exists.
//!
//! Each test binary compiles its own copy of this module and uses a subset of
//! it, so items unused by one file are dead code there. The allow is scoped to
//! this module rather than sprinkled at the call sites, which is the
//! conventional shape for an integration-test `common` module.
#![allow(dead_code, unreachable_pub)]

use horde_cli::Client;
use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::worker_service_server::WorkerServiceServer;
use horde_proto::v1::{
    DeliveryMode, Event, JournaledEvent, JournaledEventBatch, PushEventsRequest, PushEventsStart,
    SessionId, WorkerId, push_events_request,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Setup failures are returned rather than unwrapped.
///
/// `allow-expect-in-tests` exempts anything under `#[cfg(test)]`, which an
/// integration test file's free functions are not — so `expect` here would trip
/// the deny-list. Threading the error to a `#[test]` also reports *which* setup
/// step failed.
pub type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Starts a server on its own in-memory log, and returns the endpoint reaching
/// it.
pub async fn serve() -> Setup<String> {
    let core = ServerCore::new(EventLog::in_memory()?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ControlPlaneServiceServer::new(ControlPlane::new(
                core.clone(),
            )))
            // The worker spine too, because some client-facing behaviour has no
            // reachable starting state without it. A permission escalation is
            // the case in point: nothing on the control plane can author one —
            // that is a deliberate design property, not a gap — so a test that
            // wants to *answer* a permission has to journal the request the way
            // a real worker does.
            .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    Ok(format!("http://{address}"))
}

/// Journals one event the way a worker does, and waits for the server's ack.
///
/// The escape hatch for tests whose subject is a client verb but whose
/// precondition is an observed fact — which only the worker's journal can
/// write.
pub async fn journal_event(endpoint: &str, session_id: &str, event: Event) -> Setup<()> {
    let mut worker =
        WorkerServiceClient::connect(tonic::transport::Endpoint::try_from(endpoint.to_owned())?)
            .await?;

    let (to_server, from_client) = tokio::sync::mpsc::channel(8);
    let mut acks = worker
        .push_events(tokio_stream::wrappers::ReceiverStream::new(from_client))
        .await?
        .into_inner();

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Start(PushEventsStart {
                worker_id: Some(WorkerId::new("test-worker")),
                last_local_journal_sequence: 0,
            })),
        })
        .await?;
    // The start is acked before the batch is sent, so a failure to register is
    // reported as itself rather than as a confusing batch rejection.
    acks.message().await?;

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: vec![JournaledEvent {
                    session_id: Some(SessionId::new(session_id)),
                    journal_sequence: 1,
                    event: Some(event),
                }],
                acked_command_sequence: 0,
            })),
        })
        .await?;
    acks.message().await?;

    Ok(())
}

/// A client wired to its own private server, over a channel the caller built
/// rather than one [`Client::connect`] dialled.
///
/// This is the [`Client::with_channel`] seam, and entering through it here is
/// what keeps it exercised: a frontend that owns its transport — an embedded
/// server, a custom connector, a pre-warmed pool — constructs a client this
/// way, and a constructor no test ever calls is one nobody knows still works.
pub async fn client() -> Setup<Client> {
    let endpoint = serve().await?;
    let channel = tonic::transport::Endpoint::try_from(endpoint)?
        .connect()
        .await?;
    Ok(Client::with_channel(channel))
}

/// Creates a session, returning its id and its root agent's id.
///
/// The preamble most tests need before they can test anything else.
pub async fn seed(client: &mut Client, title: &str) -> Setup<(String, String)> {
    let (summary, root) = client.create_session(title, "key-1").await?;
    let session_id = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or("a created session must have an id")?;
    Ok((session_id, root.to_string()))
}

/// A connected client, and a session already created on the server it reaches.
pub async fn seeded(endpoint: &str, title: &str) -> Setup<(Client, String, String)> {
    let mut client = Client::connect(endpoint).await?;
    let (session_id, root) = seed(&mut client, title).await?;
    Ok((client, session_id, root))
}

/// Appends `count` notes to an agent, which is how a test gets a log with
/// enough in it to replay from an offset.
pub async fn send_notes(client: &mut Client, agent_id: &str, count: usize) -> Setup<()> {
    for index in 0..count {
        client
            .send_note(
                agent_id,
                &format!("note {index}"),
                DeliveryMode::TurnBoundary,
                "",
            )
            .await?;
    }
    Ok(())
}
