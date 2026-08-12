//! The permission chain, end to end, with nothing stubbed between the prompt
//! tool and the log.
//!
//! A real `horde-server` on a real loopback socket; the request journaled
//! through the real `PushEvents` path a Worker uses; the answer given through
//! the real `AnswerPermission` RPC by a client that found the escalation by
//! watching the log, the way a human's client does. The only fake is the
//! harness itself standing in for a Worker process.
//!
//! What each test is really asserting is that a decision made *over there* — by
//! someone who was not in the call — arrives back at the blocked agent intact.

use horde_adapter::permission::PermissionDecision;
use horde_adapter_claude::permission::PromptRequest;
use horde_cli::client::{Client, PermissionAnswer, SyncOptions};
use horde_mcp::identity::{AgentIdentity, AgentRegistry};
use horde_mcp::journal::{JournalEntry, JournalError, JournalSink};
use horde_mcp::permission::PermissionBroker;
use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::worker_service_server::WorkerServiceServer;
use horde_proto::v1::{
    JournaledEvent, JournaledEventBatch, PermissionOutcome, PushEventsRequest, PushEventsStart,
    SessionId, WorkerId, push_events_request,
};
use horde_server::EventLog;
use horde_server::service::{ControlPlane, ServerCore, WorkerFacing};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// A journal that pushes through the real `WorkerService`.
///
/// The in-memory fake in `journal::testing` proves the endpoint's half of the
/// seam. This one is here because these tests need the request to actually
/// reach the log — an escalation nobody can see is not an escalation.
#[derive(Debug, Clone)]
struct ServerJournal {
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
    fn new(endpoint: &str) -> Self {
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
        acks.message().await?;

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
        acks.message().await?;
        Ok(())
    }
}

/// Server, endpoint, a session, and the agent the prompts speak for.
struct Harness {
    endpoint: String,
    session_id: String,
    agent_id: String,
}

async fn harness() -> Setup<Harness> {
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
    let (summary, root) = client.create_session("permissions", "key-1").await?;
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

fn identity_for(agent_id: &str) -> Setup<AgentIdentity> {
    let mut registry = AgentRegistry::new();
    registry.insert(agent_id, "token");

    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "Bearer token".parse()?);
    headers.insert("x-horde-agent", agent_id.parse()?);
    Ok(registry.authenticate(&headers)?)
}

fn prompt(tool: &str, input: serde_json::Value, tool_use_id: &str) -> Setup<PromptRequest> {
    Ok(serde_json::from_value(serde_json::json!({
        "tool_name": tool,
        "input": input,
        "tool_use_id": tool_use_id,
    }))?)
}

/// Waits for the escalation to become visible, the way an attached client does:
/// by watching the log, not by being told.
async fn await_escalation(endpoint: &str, session_id: &str, request_id: &str) -> Setup<String> {
    let mut client = Client::connect(endpoint).await?;
    let mut stream = client
        .follow(&SyncOptions::new(session_id.to_owned()))
        .await?;

    let wanted = request_id.to_owned();
    let found = stream
        .next_matching(move |envelope| {
            let Some(horde_proto::v1::event::Kind::PermissionRequested(requested)) = envelope
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref())
            else {
                return None;
            };
            let request = requested.request.as_ref()?;
            let id = request.request_id.as_ref().map(ToString::to_string)?;
            (id == wanted).then(|| {
                request
                    .tool_call
                    .as_ref()
                    .map(|call| call.rendered.clone())
                    .unwrap_or_default()
            })
        })
        .await?
        .ok_or("the escalation never appeared in the log")?;
    Ok(found)
}

/// Drives one escalation to a decision, returning what the harness would be
/// handed back.
async fn round_trip(
    tool_use_id: &'static str,
    input: serde_json::Value,
    outcome: PermissionOutcome,
    reason: &'static str,
    narrowing: Option<serde_json::Value>,
) -> Setup<serde_json::Value> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;
    let journal = std::sync::Arc::new(ServerJournal::new(&harness.endpoint));
    let client = Client::connect(&harness.endpoint).await?;

    let mut broker = PermissionBroker::new(journal, client, harness.session_id.clone());
    let prompt = prompt("Write", input, tool_use_id)?;

    // The agent's side: blocks until somebody answers.
    let blocked = tokio::spawn(async move {
        let identity = identity;
        broker.respond(&identity, &prompt).await
    });

    // The human's side: sees the escalation, then answers it.
    let rendered = await_escalation(&harness.endpoint, &harness.session_id, tool_use_id).await?;
    assert!(
        !rendered.is_empty(),
        "an approver reads the rendering; an empty one is an unanswerable prompt"
    );

    let mut answering = Client::connect(&harness.endpoint).await?;
    answering
        .answer_permission(PermissionAnswer {
            request_id: tool_use_id,
            outcome,
            reason,
            remember_for_session: false,
            updated_input: narrowing.as_ref(),
        })
        .await?;

    Ok(blocked.await??)
}

/// The decision the model sees is the one the human made, carried across two
/// processes' worth of wire.
#[tokio::test]
async fn an_allow_reaches_the_blocked_agent() {
    let response = round_trip(
        "toolu_allow",
        serde_json::json!({"file_path": "/tmp/x", "content": "hi"}),
        PermissionOutcome::Allow,
        "",
        None,
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "allow");
    assert_eq!(
        decoded["updatedInput"],
        serde_json::json!({"file_path": "/tmp/x", "content": "hi"}),
        "an unmodified allow must echo the proposal back — the harness runs \
         updatedInput, so omitting it would run nothing"
    );
}

/// The case the whole `updated_input` chain exists for: a human clamps an
/// argument, and the harness runs the clamped call rather than the proposed one.
#[tokio::test]
async fn a_narrowing_reaches_the_blocked_agent_as_the_arguments_to_run() {
    let response = round_trip(
        "toolu_narrow",
        serde_json::json!({"file_path": "/etc/hosts", "content": "hi"}),
        PermissionOutcome::Allow,
        "",
        Some(serde_json::json!({"file_path": "/tmp/safe", "content": "hi"})),
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "allow");
    assert_eq!(
        decoded["updatedInput"]["file_path"], "/tmp/safe",
        "the clamp must survive; running the proposed path would be the \
         silently-widened permission this chain exists to prevent"
    );
}

#[tokio::test]
async fn a_denial_reaches_the_blocked_agent_with_its_reason() {
    let response = round_trip(
        "toolu_deny",
        serde_json::json!({"file_path": "/etc/hosts", "content": "hi"}),
        PermissionOutcome::Deny,
        "outside the workspace",
        None,
    )
    .await
    .expect("the round trip must complete");

    let decoded = decode(&response).expect("the harness response must decode");
    assert_eq!(decoded["behavior"], "deny");
    assert_eq!(
        decoded["message"], "outside the workspace",
        "the agent is told why, or it retries the same call forever"
    );
}

/// The harness wants the decision stringified inside a text content block, not
/// as a nested object. Getting this wrong is invisible until a real agent
/// silently ignores every decision.
fn decode(response: &serde_json::Value) -> Setup<serde_json::Value> {
    let text = response["content"][0]["text"]
        .as_str()
        .ok_or("the decision must be a string inside a text block")?;
    Ok(serde_json::from_str(text)?)
}

/// A sanity check that the fake and the real sink agree about the shape of a
/// failure: an unwritable journal is an error to the agent, never a wait.
#[tokio::test]
async fn an_unraisable_request_fails_rather_than_blocking_forever() {
    let harness = harness().await.expect("a harness");
    let identity = identity_for(&harness.agent_id).expect("an identity");
    let client = Client::connect(&harness.endpoint)
        .await
        .expect("a connected client");

    // A journal pointed at nothing: the request cannot be written, so no human
    // will ever see it.
    let journal = std::sync::Arc::new(ServerJournal::new("http://127.0.0.1:1"));
    let mut broker = PermissionBroker::new(journal, client, harness.session_id.clone());
    let prompt = prompt("Write", serde_json::json!({}), "toolu_dead").expect("a prompt");

    let error = broker
        .respond(&identity, &prompt)
        .await
        .expect_err("an unwritable journal must surface, not hang");

    assert!(
        matches!(
            error,
            horde_mcp::permission::PermissionError::NotRaised { .. }
        ),
        "the agent must be told the question was never asked: {error}"
    );
}

/// `PermissionDecision`'s domain type is what the endpoint reasons about, so
/// the allow-with-rewrite distinction has to survive the whole trip rather than
/// only the encoding at the end.
#[test]
fn the_domain_decision_distinguishes_a_narrowing_from_a_bare_allow() {
    assert_eq!(PermissionDecision::allow().updated_input(), None);
    assert_eq!(
        PermissionDecision::allow_with(serde_json::json!({"a": 1})).updated_input(),
        Some(&serde_json::json!({"a": 1}))
    );
}
