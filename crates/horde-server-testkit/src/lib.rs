//! The server harness shared by every suite that tests against a real server.
//!
//! `horde-cli`, `horde-mcp` and `horde-server` all arrange the same things
//! before they can test anything: both service spines behind one endpoint, a
//! session to work in, and — when a test's precondition is an observed fact —
//! an event journaled the way a Worker journals it. Each suite owned a private
//! copy of that arrangement, the duplication gate counted four blocks of it,
//! and private copies of a transport harness drift: a test that fails because
//! its harness differs from its neighbour's is the worst kind of red.
//!
//! **Why this is not part of `horde-testkit`.** That crate is the golden
//! transcripts and the adapter conformance suite, and the adapter crates —
//! pure codecs — dev-dep it. This crate drags in `horde-server` and
//! `horde-cli`, and folding it into `horde-testkit`, even behind a feature,
//! would tax every lightweight consumer's test build with a server it never
//! starts. The split follows the dependency weight: `horde-testkit` for what a
//! codec crate can afford, this crate for the suites that already pay for a
//! server.
//!
//! **The dev-dependency cycle is deliberate.** `horde-server`'s tests consume
//! this crate while this crate depends on `horde-server`. Cargo permits that:
//! dev-dependencies are not part of a package's own build graph, so the cycle
//! never reaches the compiler.
//!
//! **This crate carries its own tests.** Not as a copy of its consumers'
//! suites: cargo-mutants runs a mutated package's own tests, so a helper whose
//! only exercise lives in the crates that consume it is a helper whose mutants
//! all survive. `tests/harness.rs` observes each helper's effect through the
//! server — a session fetched back, an event replayed — rather than trusting a
//! returned value.

pub mod process;
pub mod secure;

use horde_cli::Client;
use horde_proto::v1::blob_service_server::BlobServiceServer;
use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::worker_service_server::WorkerServiceServer;
use horde_proto::v1::{
    Event, JournaledEvent, JournaledEventBatch, PushEventsRequest, PushEventsResponse,
    PushEventsStart, SessionId, WorkerId, push_events_request,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Setup failures are returned rather than unwrapped.
///
/// `allow-expect-in-tests` exempts anything under `#[cfg(test)]` and functions
/// carrying `#[test]`, which an integration test file's free helper functions
/// are neither — so `expect` in them would trip the deny-list. Threading the
/// error to a `#[test]` also reports *which* setup step failed.
pub type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Starts a server on its own in-memory log, and returns the endpoint reaching
/// it.
///
/// Both spines are served. The control plane is what clients read and answer
/// through; the worker spine is what makes an *observed fact* reachable at
/// all, since nothing on the control plane can author a permission request or
/// a result — that is the plane split (DESIGN §5), not a gap, so a test whose
/// precondition is an observed fact has to journal it the way a Worker does.
///
/// Binding is `127.0.0.1:0`: a real socket, loopback only, with the port
/// chosen by the OS so tests running in parallel cannot collide. DESIGN.md §3
/// forbids binding beyond localhost until auth exists.
///
/// # Errors
///
/// Any failure to build the log or bind the socket.
pub async fn serve() -> Setup<String> {
    serve_core(ServerCore::new(EventLog::in_memory()?)).await
}

/// [`serve`], with a real blob store rooted at `blob_root` — for suites
/// whose subject is attachment transfer. The caller owns the directory
/// (usually a `tempfile::TempDir` it keeps alive), so the testkit does not
/// grow a tempdir dependency and the test can inspect or corrupt the
/// store's files directly.
///
/// # Errors
///
/// As [`serve`], plus the store refusing to open.
pub async fn serve_with_blob_store(blob_root: &std::path::Path) -> Setup<String> {
    let store = horde_server::blob::BlobStore::open(blob_root)?;
    serve_core(ServerCore::new(EventLog::in_memory()?).with_blob_store(std::sync::Arc::new(store)))
        .await
}

/// [`serve`], with the permission chain (DESIGN §10) running against the same
/// log, configured with `modes` and an optional judge.
///
/// A separate entry point rather than a default of [`serve`], deliberately:
/// the chain appends events of its own (decisions, enriched escalations), and
/// a suite that arranged an exact event sequence should not gain rows because
/// the harness grew an opinion. Tests whose subject *is* the chain opt in.
///
/// The chain task is aborted with the server task when the runtime shuts
/// down, the same lifetime every other harness task has.
///
/// # Errors
///
/// As [`serve`].
pub async fn serve_with_permissions(
    modes: horde_server::permission::PermissionModes,
    judge: Option<std::sync::Arc<dyn horde_server::permission::PolicyJudge>>,
) -> Setup<String> {
    let core = ServerCore::new(EventLog::in_memory()?);
    let _chain = horde_server::permission::spawn(&core.log, std::sync::Arc::new(modes), judge);
    serve_core(core).await
}

/// Serves both spines over a caller-built core.
async fn serve_core(core: ServerCore) -> Setup<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ControlPlaneServiceServer::new(ControlPlane::new(
                core.clone(),
            )))
            // Blobs ride the same endpoint they share in production; a core
            // without a store answers the verbs `Unimplemented`, honestly.
            .add_service(BlobServiceServer::new(horde_server::service::Blobs::new(
                core.blobs.clone(),
            )))
            .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    Ok(format!("http://{address}"))
}

/// Opens a journal stream and announces the worker's journal position.
///
/// Returns the sender and the ack stream with the first ack unread: the caller
/// decides what to make of it, which is the part each test actually cares
/// about — a resume position, an acceptance, a refusal.
///
/// # Errors
///
/// Any failure to open the stream or send the start frame.
pub async fn open_journal(
    worker: &mut WorkerServiceClient<tonic::transport::Channel>,
    worker_id: &str,
    last_local_journal_sequence: u64,
) -> Setup<(
    tokio::sync::mpsc::Sender<PushEventsRequest>,
    tonic::Streaming<PushEventsResponse>,
)> {
    let (to_server, from_client) = tokio::sync::mpsc::channel(8);
    let acks = worker
        .push_events(tokio_stream::wrappers::ReceiverStream::new(from_client))
        .await?
        .into_inner();

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Start(PushEventsStart {
                worker_id: Some(WorkerId::new(worker_id)),
                last_local_journal_sequence,
            })),
        })
        .await?;

    Ok((to_server, acks))
}

/// Journals one event at `journal_sequence` the way a worker does, and waits
/// for the server's acks.
///
/// The escape hatch for tests whose subject is a client-side verb but whose
/// precondition is an observed fact — which only the worker's journal can
/// write. The sequence is caller-chosen so a test journaling several events
/// stays a single honest worker rather than several claiming the same
/// position.
///
/// # Errors
///
/// Any transport failure — and the server closing the stream instead of
/// acking. `message()` answering `Ok(None)` means exactly that close, and
/// treating it as success is how a rejected write becomes a test that hangs
/// somewhere else entirely.
pub async fn journal_event_at(
    endpoint: &str,
    session_id: &str,
    journal_sequence: u64,
    event: Event,
) -> Setup<()> {
    let mut worker =
        WorkerServiceClient::connect(tonic::transport::Endpoint::try_from(endpoint.to_owned())?)
            .await?;

    let (to_server, mut acks) = open_journal(
        &mut worker,
        "test-worker",
        journal_sequence.saturating_sub(1),
    )
    .await?;
    // The start is acked before the batch is sent, so a failure to register is
    // reported as itself rather than as a confusing batch rejection.
    acks.message()
        .await?
        .ok_or("the server closed the journal stream instead of acking the start")?;

    to_server
        .send(PushEventsRequest {
            body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                events: vec![JournaledEvent {
                    session_id: Some(SessionId::new(session_id)),
                    journal_sequence,
                    event: Some(event),
                }],
                acked_command_sequence: 0,
            })),
        })
        .await?;
    acks.message()
        .await?
        .ok_or("the server closed the journal stream instead of acking the batch")?;

    Ok(())
}

/// Journals one event as a worker's first, and waits for the server's ack.
///
/// [`journal_event_at`] with the sequence a fresh worker would use — enough
/// for the common test that needs exactly one observed fact on the log.
///
/// # Errors
///
/// As [`journal_event_at`].
pub async fn journal_event(endpoint: &str, session_id: &str, event: Event) -> Setup<()> {
    journal_event_at(endpoint, session_id, 1, event).await
}

/// Creates a session, returning its id and its root agent's id.
///
/// The preamble most tests need before they can test anything else.
///
/// # Errors
///
/// The create failing, or a created session arriving without an id.
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
///
/// # Errors
///
/// As [`seed`], plus the connection failing.
pub async fn seeded(endpoint: &str, title: &str) -> Setup<(Client, String, String)> {
    let mut client = Client::connect(endpoint).await?;
    let (session_id, root) = seed(&mut client, title).await?;
    Ok((client, session_id, root))
}

/// Runs a product binary with the OTLP environment under the test's control:
/// both variables scrubbed first (nothing leaks in from the harness's own
/// shell), then `environment` injected, and everything the process wrote
/// captured.
///
/// Shared here because every binary's telemetry suite needs exactly this
/// arrangement — `horde-server`'s and `horde`'s tests must not each restate
/// it (the duplication gate agrees). The variable names come from
/// `horde-observe`, the crate that reads them, so the scrub list cannot
/// drift from the config surface it scrubs.
///
/// # Errors
///
/// The process failing to spawn or be waited on.
pub async fn otlp_scrubbed_run(
    program: &str,
    arguments: &[&str],
    environment: &[(&str, &str)],
) -> Setup<std::process::Output> {
    let mut command = tokio::process::Command::new(program);
    command.args(arguments);
    command.env_remove(horde_observe::ENDPOINT_VAR);
    command.env_remove(horde_observe::SAMPLE_VAR);
    for (name, value) in environment {
        command.env(name, value);
    }
    Ok(command.output().await?)
}
