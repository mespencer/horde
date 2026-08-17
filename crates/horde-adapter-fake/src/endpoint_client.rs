//! The fake harness's MCP client: how a scripted agent reaches its endpoint.
//!
//! A real harness (Claude, Codex) speaks MCP over HTTP at the endpoint the
//! Worker hands it; a scripted agent that could not would leave the
//! orchestrator verbs untestable without burning tokens. This is the legacy
//! session flow the dual-era spike recorded from Claude Code 2.1.228 —
//! `initialize`, the session id riding `Mcp-Session-Id`,
//! `notifications/initialized`, then the call — and deliberately no more:
//! the subject under test is always the *server* side of the wire.
//!
//! Hand-rolled over hyper rather than an MCP client crate because this crate
//! sits below the integration layer: `horde-cli` depends on it, so anything
//! here that reached for `horde-mcp` would close a dependency cycle.

use horde_adapter::McpEndpoint;

/// Calls one tool on the agent's Horde endpoint, answering with the text of
/// the result's first content block.
///
/// # Errors
///
/// A refusal in words the script's output can carry: no HTTP endpoint, an
/// unreachable one, a refused handshake, or a tool-level error.
pub(crate) async fn call_horde_tool(
    endpoint: Option<&McpEndpoint>,
    tool: &str,
    arguments: &serde_json::Value,
) -> Result<String, String> {
    let Some(McpEndpoint::Http { url, headers }) = endpoint else {
        return Err("this agent was spawned with no Horde HTTP endpoint".to_owned());
    };
    let target = Target::parse(url)?;

    // The legacy lifecycle: initialize, learn the session, announce readiness.
    let initialize = post(
        &target,
        headers,
        None,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": { "name": "horde-adapter-fake", "version": "0.1.0" },
            },
        }),
    )
    .await?;
    if !initialize.status.is_success() {
        return Err(format!(
            "initialize was refused {}: {}",
            initialize.status, initialize.body
        ));
    }
    let session = initialize
        .session_id
        .ok_or("the endpoint assigned no Mcp-Session-Id")?;

    let initialized = post(
        &target,
        headers,
        Some(&session),
        &serde_json::json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
    )
    .await?;
    if !initialized.status.is_success() {
        return Err(format!(
            "notifications/initialized was refused {}",
            initialized.status
        ));
    }

    let called = post(
        &target,
        headers,
        Some(&session),
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": { "name": tool, "arguments": arguments },
        }),
    )
    .await?;
    if !called.status.is_success() {
        return Err(format!(
            "tools/call was refused {}: {}",
            called.status, called.body
        ));
    }
    decode_call_reply(&called.body)
}

/// The text a `tools/call` reply carries, or the error it names.
///
/// The session flow answers over SSE (`data: ` frames); a stateless answer is
/// plain JSON. Same JSON-RPC message either way, and both the protocol-level
/// `error` and the tool-level `isError` result are refusals the script's
/// output must carry — a scripted agent that read an error as a result would
/// be testing a happier protocol than the one being served.
fn decode_call_reply(body: &str) -> Result<String, String> {
    let payload = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .next_back()
        .unwrap_or(body);
    let message: serde_json::Value = serde_json::from_str(payload)
        .map_err(|error| format!("the reply is not JSON-RPC ({error}): {payload}"))?;

    if let Some(error) = message.get("error") {
        return Err(format!("the call was refused: {error}"));
    }
    let result = message
        .get("result")
        .ok_or_else(|| format!("the reply carries no result: {message}"))?;
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err(format!("the tool answered an error: {text}"));
    }
    Ok(text)
}

/// Where the endpoint lives, split once so every request agrees.
struct Target {
    authority: String,
    path: String,
}

impl Target {
    fn parse(url: &str) -> Result<Self, String> {
        let uri: http::Uri = url
            .parse()
            .map_err(|error| format!("the endpoint url {url} does not parse: {error}"))?;
        let authority = uri
            .authority()
            .ok_or_else(|| format!("the endpoint url {url} names no host"))?
            .to_string();
        Ok(Self {
            authority,
            path: uri.path().to_owned(),
        })
    }
}

/// One reply, as much of it as the caller reads.
struct Reply {
    status: http::StatusCode,
    session_id: Option<String>,
    body: String,
}

/// One JSON-RPC POST over a fresh connection.
///
/// A fresh TCP connection per request is deliberate: it matches the recorded
/// harness traffic closely enough, and holding a pooled connection open would
/// give this client state the tests would then depend on.
async fn post(
    target: &Target,
    headers: &[(String, String)],
    session: Option<&str>,
    body: &serde_json::Value,
) -> Result<Reply, String> {
    let stream = tokio::net::TcpStream::connect(&target.authority)
        .await
        .map_err(|error| format!("could not reach the endpoint {}: {error}", target.authority))?;
    let (mut sender, connection) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .map_err(|error| format!("the http handshake failed: {error}"))?;
    // The connection future drives the socket; the request below resolves
    // through it.
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let mut request = http::Request::builder()
        .method("POST")
        .uri(&target.path)
        .header("host", &target.authority)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    for (name, value) in headers {
        request = request.header(name, value);
    }
    if let Some(session) = session {
        request = request.header("mcp-session-id", session);
    }
    let request = request
        .body(http_body_util::Full::new(hyper::body::Bytes::from(
            body.to_string(),
        )))
        .map_err(|error| format!("the request would not build: {error}"))?;

    let response = sender
        .send_request(request)
        .await
        .map_err(|error| format!("the endpoint dropped the request: {error}"))?;
    let (parts, body) = response.into_parts();
    let session_id = parts
        .headers
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let bytes = http_body_util::BodyExt::collect(body)
        .await
        .map_err(|error| format!("the reply body failed: {error}"))?
        .to_bytes();
    Ok(Reply {
        status: parts.status,
        session_id,
        body: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::{Target, decode_call_reply};

    #[test]
    fn the_target_splits_authority_and_path() {
        let target = Target::parse("http://127.0.0.1:8931/mcp").expect("a well-formed url");
        assert_eq!(target.authority, "127.0.0.1:8931");
        assert_eq!(target.path, "/mcp");

        assert!(Target::parse("not a url").is_err());
        assert!(
            Target::parse("/mcp").is_err(),
            "a url with no host names nowhere to connect"
        );
    }

    /// Both framings and all three verdicts: plain JSON success, SSE-framed
    /// success, a protocol error, and a tool-level error — each must come
    /// back as what it was, because the script's output is the only place a
    /// test can read the verdict.
    #[test]
    fn call_replies_decode_success_and_both_error_shapes() {
        let plain =
            r#"{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"recorded"}]}}"#;
        assert_eq!(decode_call_reply(plain), Ok("recorded".to_owned()));

        let framed = format!("event: message\ndata: {plain}\n\n");
        assert_eq!(decode_call_reply(&framed), Ok("recorded".to_owned()));

        let refused = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"no"}}"#;
        let refusal = decode_call_reply(refused).expect_err("a protocol error is a refusal");
        assert!(refusal.contains("refused"), "{refusal}");

        let tool_error = r#"{"jsonrpc":"2.0","id":1,"result":{"isError":true,"content":[{"type":"text","text":"permission denied"}]}}"#;
        let refusal = decode_call_reply(tool_error).expect_err("isError is a refusal");
        assert!(refusal.contains("permission denied"), "{refusal}");

        assert!(decode_call_reply("not json").is_err());
    }
}
