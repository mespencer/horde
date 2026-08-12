//! The tool surface over the real HTTP stack, with nothing faked between the
//! JSON-RPC POST and the log.
//!
//! The live smoke proves a real `claude` can drive this endpoint; these tests
//! prove what the endpoint *answers*. The client here is deliberately small —
//! the legacy-era session flow the spike recorded from Claude Code 2.1.228:
//! `initialize`, the session id riding the `Mcp-Session-Id` header,
//! `notifications/initialized`, then `tools/call` — because the subject is
//! the server side of the wire: the identity gate, rmcp's dispatch, the
//! handler's answer, and the event the call left on the log. Each test
//! asserts on both halves — the text handed back *and* the effect observable
//! from another client — since a handler whose answer nobody reads is exactly
//! what a mutated return value fakes.

mod common;

use common::{Setup, harness};
use horde_mcp::endpoint::HordeEndpoint;
use horde_mcp::identity::AgentRegistry;
use horde_mcp::server::{EndpointConfig, serve};

const TOKEN: &str = "endpoint-http-token";

/// A served endpoint over a running harness, kept alive for the test.
struct Served {
    address: std::net::SocketAddr,
    horde: common::Harness,
    _serving: tokio::task::JoinHandle<()>,
}

async fn served() -> Setup<Served> {
    let horde = harness().await?;

    let mut registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, TOKEN);

    let endpoint = HordeEndpoint::new(horde.context().await?);
    let (address, serving) = serve(
        &EndpointConfig::loopback(0),
        std::sync::Arc::new(registry),
        endpoint,
    )
    .await?;

    Ok(Served {
        address,
        horde,
        _serving: serving,
    })
}

/// A legacy-era MCP client, one identity, one session.
struct McpClient {
    address: std::net::SocketAddr,
    agent_id: String,
    session: Option<String>,
}

impl McpClient {
    /// Initializes against the endpoint the way a legacy harness does, and
    /// keeps the session the server assigned.
    async fn connect(served: &Served) -> Setup<Self> {
        let mut client = Self {
            address: served.address,
            agent_id: served.horde.agent_id.clone(),
            session: None,
        };

        let (parts, body) = client
            .post(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 0,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "endpoint-http-test", "version": "0.0.0" },
                },
            }))
            .await?;
        if !parts.status.is_success() {
            return Err(format!("initialize was refused {}: {body}", parts.status).into());
        }
        let session = parts
            .headers
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .ok_or("a legacy-era initialize is assigned a session id")?
            .to_owned();
        client.session = Some(session);

        // The notification the lifecycle requires between initialize and use.
        let (parts, body) = client
            .post(&serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized",
            }))
            .await?;
        if !parts.status.is_success() {
            return Err(format!("initialized was refused {}: {body}", parts.status).into());
        }

        Ok(client)
    }

    /// One JSON-RPC POST, answered with the response head and the raw body.
    async fn post(&self, body: &serde_json::Value) -> Setup<(http::response::Parts, String)> {
        let stream = tokio::net::TcpStream::connect(self.address).await?;
        let (mut sender, connection) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
        // The connection future drives the socket; the request below resolves
        // through it.
        tokio::spawn(connection);

        let mut request = http::Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("host", self.address.to_string())
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("x-horde-agent", self.agent_id.clone());
        if let Some(session) = &self.session {
            request = request.header("mcp-session-id", session.clone());
        }

        let response = sender
            .send_request(
                request.body(http_body_util::Full::new(hyper::body::Bytes::from(
                    body.to_string(),
                )))?,
            )
            .await?;
        let (parts, body) = response.into_parts();
        let bytes = http_body_util::BodyExt::collect(body).await?.to_bytes();
        Ok((parts, String::from_utf8(bytes.to_vec())?))
    }

    /// One `tools/call`, answered with the result's text content.
    async fn call_tool(&self, name: &str, arguments: serde_json::Value) -> Setup<String> {
        let (parts, text) = self
            .post(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "name": name, "arguments": arguments },
            }))
            .await?;
        if !parts.status.is_success() {
            return Err(format!("the endpoint answered {}: {text}", parts.status).into());
        }

        // The session flow answers over SSE; a stateless answer is plain JSON.
        // Same JSON-RPC message either way.
        let payload = match text
            .lines()
            .filter_map(|line| line.strip_prefix("data: "))
            .next_back()
        {
            Some(framed) => framed.to_owned(),
            None => text,
        };
        let rpc: serde_json::Value = serde_json::from_str(&payload)?;
        if let Some(error) = rpc.get("error") {
            return Err(format!("the call was refused: {error}").into());
        }
        let result = rpc.get("result").ok_or("a reply must carry a result")?;
        if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
            return Err(format!("the tool answered an error: {result}").into());
        }
        result["content"][0]["text"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "the result must carry one text content block".into())
    }
}

/// Replays the session and returns the sequence of the first event `matches`.
async fn find_on_log<F>(served: &Served, mut matches: F) -> Setup<Option<u64>>
where
    F: FnMut(&horde_proto::v1::Event) -> bool,
{
    let mut client = served.horde.client().await?;
    let mut found = None;
    client
        .sync(
            &horde_cli::client::SyncOptions::new(served.horde.session_id.clone()),
            |envelope| {
                if found.is_none() && envelope.event.as_ref().is_some_and(&mut matches) {
                    found = Some(envelope.sequence);
                }
            },
            |_| {},
        )
        .await?;
    Ok(found)
}

/// `send_message` answers with the sequence the delivery landed at — the
/// number a caller uses to resume past its own write — and the note it names
/// is really on the log at that sequence.
#[tokio::test]
async fn send_message_answers_with_the_sequence_the_note_landed_at() {
    let served = served().await.expect("the endpoint must serve");
    let mcp = McpClient::connect(&served)
        .await
        .expect("the legacy flow must initialize");
    let to_self = served.horde.agent_id.clone();

    let answer = mcp
        .call_tool(
            "send_message",
            serde_json::json!({ "to_agent_id": to_self, "text": "over http", "note": true }),
        )
        .await
        .expect("the tool call must succeed");

    let named = answer
        .strip_prefix("delivered at ")
        .and_then(|rest| rest.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("the answer names a sequence; got {answer:?}"));

    let landed = find_on_log(&served, |event| {
        matches!(
            event.kind.as_ref(),
            Some(horde_proto::v1::event::Kind::NoteSent(sent))
                if sent.note.as_ref().is_some_and(|note| note.text == "over http")
        )
    })
    .await
    .expect("replay must succeed")
    .expect("the note must be on the log");
    assert_eq!(
        named, landed,
        "the sequence in the answer is the log's, not an invention"
    );
}

/// `report_result` acknowledges with the exact word its contract promises,
/// and the result is readable back off the log.
#[tokio::test]
async fn report_result_answers_recorded_and_lands_on_the_log() {
    let served = served().await.expect("the endpoint must serve");
    let mcp = McpClient::connect(&served)
        .await
        .expect("the legacy flow must initialize");

    let answer = mcp
        .call_tool(
            "report_result",
            serde_json::json!({ "status": "success", "summary": "finished over http" }),
        )
        .await
        .expect("the tool call must succeed");
    assert_eq!(
        answer, "recorded",
        "the acknowledgement is part of the contract"
    );

    find_on_log(&served, |event| {
        matches!(
            event.kind.as_ref(),
            Some(horde_proto::v1::event::Kind::ResultReported(reported))
                if reported
                    .result
                    .as_ref()
                    .is_some_and(|result| result.summary == "finished over http")
        )
    })
    .await
    .expect("replay must succeed")
    .expect("an acked result must be on the log");
}

/// Watches the log for the first escalation and denies it — the other side of
/// the block `approve` sits in.
async fn deny_first_escalation(endpoint: &str, session_id: &str) -> Setup<()> {
    let mut client = horde_cli::client::Client::connect(endpoint).await?;
    let mut stream = client
        .follow(&horde_cli::client::SyncOptions::new(session_id.to_owned()))
        .await?;

    let request_id = stream
        .next_matching(common::escalation_request_id)
        .await?
        .ok_or("the escalation never appeared in the log")?;

    let mut answering = horde_cli::client::Client::connect(endpoint).await?;
    answering
        .answer_permission(horde_cli::client::PermissionAnswer {
            request_id: &request_id,
            outcome: horde_proto::v1::PermissionOutcome::Deny,
            reason: "denied over http",
            remember_for_session: false,
            updated_input: None,
        })
        .await?;
    Ok(())
}

/// `approve` blocks on the human and answers with the decision as a JSON
/// string — the exact shape `--permission-prompt-tool` reads. An answer that
/// is not the decision is indistinguishable from a permission system that
/// invents outcomes.
#[tokio::test]
async fn approve_answers_with_the_decision_that_was_made() {
    let served = served().await.expect("the endpoint must serve");
    let mcp = McpClient::connect(&served)
        .await
        .expect("the legacy flow must initialize");

    let watching_endpoint = served.horde.endpoint.clone();
    let watching_session = served.horde.session_id.clone();
    let answering =
        tokio::spawn(
            async move { deny_first_escalation(&watching_endpoint, &watching_session).await },
        );

    let answer = mcp
        .call_tool(
            "approve",
            serde_json::json!({
                "tool_name": "Write",
                "input": { "file_path": "/tmp/x" },
                "tool_use_id": "toolu_http",
            }),
        )
        .await
        .expect("the prompt tool must answer");

    let decision: serde_json::Value = serde_json::from_str(&answer)
        .expect("the decision is a JSON string, the shape the harness parses");
    assert_eq!(
        decision["behavior"], "deny",
        "the answer is the decision that was made, not an invention"
    );
    answering
        .await
        .expect("the answerer must not panic")
        .expect("the answer must have been sendable");
}
