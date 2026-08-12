//! The server harness the endpoint's integration tests run against.
//!
//! Shared rather than copied: two transport harnesses drift, and a test that
//! fails because its private harness differs from its neighbour's is the worst
//! kind of red. `horde-server` and `horde-cli` split theirs out for the same
//! reason.
//!
//! Both spines are served. The control plane is what the endpoint reads and
//! answers through; the worker spine is what makes an *observed fact* reachable
//! at all, since nothing on the control plane can author a permission request
//! or a result — that is the plane split (DESIGN §5), not a gap, so a test
//! whose precondition is an observed fact has to journal it the way a Worker
//! does.
//!
//! Binding is `127.0.0.1:0`: a real socket, loopback only, port chosen by the
//! OS so parallel tests cannot collide.
//!
//! Each test binary compiles its own copy and uses a subset, so items unused by
//! one file are dead code there — hence the module-scoped allow.
#![allow(dead_code, unreachable_pub)]

use horde_cli::client::Client;
use horde_mcp::context::AgentContext;
use horde_mcp::identity::{AgentIdentity, AgentRegistry};
use horde_mcp::journal::{JournalEntry, JournalError, JournalSink};
use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::worker_service_server::WorkerServiceServer;
use horde_proto::v1::{
    JournaledEvent, JournaledEventBatch, PushEventsRequest, PushEventsStart, SessionId, WorkerId,
    push_events_request,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// Setup failures are returned rather than unwrapped.
///
/// `allow-expect-in-tests` exempts `#[test]` functions, which an integration
/// file's free functions are not — so `expect` here would trip the deny-list.
/// Threading the error also reports *which* setup step failed.
pub type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A journal that pushes through the real `WorkerService`.
///
/// The in-memory fake in `journal::testing` proves the endpoint's half of the
/// seam in isolation. This one exists because these tests need the event to
/// actually reach the log: an escalation nobody can see is not an escalation,
/// and a result nobody can read is not a result.
#[derive(Debug, Clone)]
pub struct ServerJournal {
    endpoint: String,
    next: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl JournalSink for ServerJournal {
    async fn record(&self, entry: JournalEntry) -> Result<u64, JournalError> {
        let sequence = self
            .next
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(1);

        self.push(entry, sequence)
            .await
            .map_err(|error| JournalError::Unavailable(error.to_string()))?;
        Ok(sequence)
    }
}

impl ServerJournal {
    #[must_use]
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_owned(),
            next: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    async fn push(&self, entry: JournalEntry, sequence: u64) -> Setup<()> {
        let mut worker = WorkerServiceClient::connect(tonic::transport::Endpoint::try_from(
            self.endpoint.clone(),
        )?)
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
                    last_local_journal_sequence: sequence.saturating_sub(1),
                })),
            })
            .await?;
        // `message()` answering `Ok(None)` means the server closed the stream
        // rather than acking. Treating that as success is how a rejected write
        // becomes a test that hangs somewhere else entirely, so it is an error
        // here.
        acks.message()
            .await?
            .ok_or("the server closed the journal stream instead of acking the start")?;

        to_server
            .send(PushEventsRequest {
                body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                    events: vec![JournaledEvent {
                        session_id: Some(SessionId::new(entry.session_id.clone())),
                        journal_sequence: sequence,
                        event: Some(horde_proto::v1::Event {
                            agent_id: entry.agent_id.as_deref().map(horde_proto::v1::AgentId::new),
                            occurred_at: None,
                            idempotency_key: entry.idempotency_key.clone(),
                            kind: Some(entry.kind.clone()),
                        }),
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
}

/// A running server, a session on it, and the agent the calls speak for.
pub struct Harness {
    pub endpoint: String,
    pub session_id: String,
    pub agent_id: String,
}

impl Harness {
    /// A journal that writes into this harness's server.
    #[must_use]
    pub fn journal(&self) -> std::sync::Arc<dyn JournalSink> {
        std::sync::Arc::new(ServerJournal::new(&self.endpoint))
    }

    /// Another client onto the same server.
    pub async fn client(&self) -> Setup<Client> {
        Ok(Client::connect(&self.endpoint).await?)
    }

    /// A context wired to this harness — the thing every verb needs.
    ///
    /// Each call builds a fresh one, which is what a test wanting two
    /// independent actors (a reporter and a waiter, say) needs.
    pub async fn context(&self) -> Setup<AgentContext> {
        Ok(AgentContext::new(
            self.journal(),
            self.client().await?,
            self.session_id.clone(),
        ))
    }

    /// A context whose journal points nowhere, for testing the unwritable case.
    pub async fn context_with_dead_journal(&self) -> Setup<AgentContext> {
        Ok(AgentContext::new(
            std::sync::Arc::new(ServerJournal::new("http://127.0.0.1:1")),
            self.client().await?,
            self.session_id.clone(),
        ))
    }
}

/// Stands up both spines, creates a session, and returns what tests need.
pub async fn harness() -> Setup<Harness> {
    let core = ServerCore::new(EventLog::in_memory()?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ControlPlaneServiceServer::new(ControlPlane::new(
                core.clone(),
            )))
            .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });

    let endpoint = format!("http://{address}");
    let mut client = Client::connect(&endpoint).await?;
    let (summary, root) = client.create_session("endpoint", "key-1").await?;
    let session_id = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .ok_or("a created session must have an id")?;

    Ok(Harness {
        endpoint,
        session_id,
        agent_id: root.to_string(),
    })
}

/// An authenticated identity for `agent_id`.
///
/// Built through the real [`AgentRegistry`] rather than constructed directly —
/// `AgentIdentity` has no public constructor precisely so that holding one is
/// proof a token matched, and a test that forged one would be testing a type
/// that does not exist in production.
pub fn identity_for(agent_id: &str) -> Setup<AgentIdentity> {
    let mut registry = AgentRegistry::new();
    registry.insert(agent_id, "token");

    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "Bearer token".parse()?);
    headers.insert("x-horde-agent", agent_id.parse()?);
    Ok(registry.authenticate(&headers)?)
}
