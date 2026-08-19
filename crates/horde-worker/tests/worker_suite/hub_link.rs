//! The forwarded hub link against a real server (ADR-0004, 2026-08-19
//! amendment): listing, the round trip through a real upstream child, the
//! refusal mappings, and revocation severing the path.
//!
//! Both ends are real in every test — a `horde-server` on a loopback socket,
//! the hub's upstream a real `mcp_stub_server` child the *server* spawns —
//! because the link's whole behavior is one RPC on each side of a socket.

use horde_mcp::hub::{HubError, HubLink};
use horde_proto::v1::{McpServerConfig, McpStdioTransport, mcp_server_config::Transport};
use horde_worker::hub::ForwardedHubLink;

use crate::support::within;

/// The stub MCP server the hub connects to, as a journaled catalog config.
async fn stub_config(name: &str, allowed: &[&str]) -> McpServerConfig {
    let binary = horde_server_testkit::process::product_binary("horde-mcp", "mcp_stub_server")
        .await
        .expect("the stub server binary exists");
    McpServerConfig {
        name: name.to_owned(),
        transport: Some(Transport::Stdio(McpStdioTransport {
            command: binary.to_string_lossy().into_owned(),
            args: Vec::new(),
            env: [].into(),
        })),
        local: false,
        allowed_agent_types: allowed.iter().map(|s| (*s).to_owned()).collect(),
        etag: String::new(),
    }
}

/// One placed agent, one visible stub server, one scoped-away server — the
/// arrangement every plaintext scenario opens with. Returns the channel the
/// worker's link forwards over.
async fn arranged() -> (horde_server::EventLog, tonic::transport::Channel) {
    let log = horde_server::EventLog::in_memory().expect("an in-memory log");
    let core = horde_server::service::ServerCore::new(log.clone());
    horde_server_testkit::place_agent(&log, "s1", "agent-1", "w1", "go")
        .await
        .expect("the placement seeds");
    horde_server_testkit::seed_mcp_server(&log, stub_config("stub", &[]).await)
        .await
        .expect("the open server seeds");
    horde_server_testkit::seed_mcp_server(&log, stub_config("scoped", &["reviewer"]).await)
        .await
        .expect("the scoped server seeds");

    let endpoint = crate::support::serve(core).await;
    let channel = tonic::transport::Endpoint::try_from(endpoint)
        .expect("a dialable endpoint")
        .connect_lazy();
    (log, channel)
}

/// The text of a result's first content block — what the model would read.
fn first_text(result: &horde_mcp::rmcp::model::CallToolResult) -> String {
    result
        .content
        .first()
        .and_then(|block| block.as_text())
        .map(|text| text.text.clone())
        .unwrap_or_default()
}

/// The whole forwarded path, nothing faked: the link fetches exactly the
/// tools this agent may see (the scoped server invisible), a call round-trips
/// through the server to the stub child it spawned, and every unroutable
/// shape — unadvertised tool, invisible server — comes back as the one
/// protocol-level refusal.
#[tokio::test]
async fn a_forwarded_hub_call_round_trips_and_visibility_fails_closed() {
    let (_log, channel) = arranged().await;

    let link = within(
        "the link fetches the toolset",
        ForwardedHubLink::fetch(channel, "w1", "s1", "agent-1"),
    )
    .await
    .expect("the placed worker fetches its agent's toolset");

    let names: Vec<&str> = link.tools().iter().map(|tool| tool.name.as_ref()).collect();
    assert!(names.contains(&"stub__echo"), "{names:?}");
    assert!(
        !names.iter().any(|name| name.starts_with("scoped__")),
        "a server scoped to another agent type must not be listed: {names:?}"
    );

    let mut arguments = horde_mcp::rmcp::model::JsonObject::new();
    arguments.insert(
        "message".to_owned(),
        serde_json::Value::String("through the forwarded path".to_owned()),
    );
    let answered = within(
        "the call crosses worker channel, server, and stub child",
        link.call("stub__echo", Some(arguments)),
    )
    .await
    .expect("the forwarded call answers");
    assert_ne!(answered.is_error, Some(true), "{answered:?}");
    assert_eq!(first_text(&answered), "echo:through the forwarded path");

    for unroutable in ["stub__no_such_tool", "scoped__echo", "unknown__echo"] {
        let refused = within("the refusal answers", link.call(unroutable, None))
            .await
            .expect_err("must not route");
        assert!(
            matches!(&refused, HubError::UnknownTool { name } if name == unroutable),
            "{unroutable} must be the unroutable refusal, got {refused:?}"
        );
    }
}

/// Attribution: a worker the log does not place the agent on cannot fetch
/// its toolset, and neither can a worker that names nobody.
#[tokio::test]
async fn a_worker_that_does_not_hold_the_agent_cannot_forward_for_it() {
    let (_log, channel) = arranged().await;

    let refused = within(
        "the wrong worker's fetch is refused",
        ForwardedHubLink::fetch(channel.clone(), "w2", "s1", "agent-1"),
    )
    .await
    .expect_err("another worker must not speak for this agent");
    assert!(
        refused.to_string().contains("not placed on worker w2"),
        "the refusal names the attribution failure: {refused}"
    );

    let anonymous = within(
        "the anonymous fetch is refused",
        ForwardedHubLink::fetch(channel, "", "s1", "agent-1"),
    )
    .await
    .expect_err("a caller naming no worker holds nothing");
    assert!(anonymous.to_string().contains("not placed"), "{anonymous}");
}

/// Revocation severs the forwarded hub path with the channel — inherited
/// from the mTLS door, proven rather than assumed: the same call that just
/// round-tripped dies at the next RPC once the worker is revoked, with the
/// door's own reason.
#[tokio::test]
async fn revocation_severs_the_forwarded_hub_path() {
    let secured = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server comes up");
    let identity = secured
        .enroll(horde_identity::PrincipalRole::Worker, "w1")
        .await
        .expect("the production issuance path enrolls the worker");
    horde_server_testkit::place_agent(&secured.log, "s1", "agent-1", "w1", "go")
        .await
        .expect("the placement seeds");
    horde_server_testkit::seed_mcp_server(&secured.log, stub_config("stub", &[]).await)
        .await
        .expect("the catalog seeds");

    let channel = within("the mTLS channel connects", async {
        secured
            .main_endpoint_as(&identity)
            .expect("a dialable endpoint")
            .connect()
            .await
    })
    .await
    .expect("the enrolled worker dials");

    let link = within(
        "the enrolled link fetches",
        ForwardedHubLink::fetch(channel, "w1", "s1", "agent-1"),
    )
    .await
    .expect("an enrolled worker forwards");

    let mut arguments = horde_mcp::rmcp::model::JsonObject::new();
    arguments.insert(
        "message".to_owned(),
        serde_json::Value::String("before revocation".to_owned()),
    );
    let answered = within(
        "the pre-revocation call round-trips",
        link.call("stub__echo", Some(arguments.clone())),
    )
    .await
    .expect("the enrolled worker's call answers");
    assert_eq!(first_text(&answered), "echo:before revocation");

    let principal = horde_identity::Principal::new(horde_identity::PrincipalRole::Worker, "w1")
        .expect("a valid name");
    let revoked = secured
        .authority
        .revoke(
            &principal,
            "test revocation",
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await
        .expect("the revocation journals");
    assert!(!revoked.is_empty(), "the worker held a serial to revoke");

    // The same link, the same call: the per-RPC door checks the deny-list,
    // so the forwarded path dies without waiting for a reconnect.
    let severed = within(
        "the post-revocation call is refused",
        link.call("stub__echo", Some(arguments)),
    )
    .await
    .expect_err("a revoked worker's forwarded hub calls must die");
    assert!(
        matches!(&severed, HubError::Upstream { message, .. } if message.contains("revoked")),
        "the refusal carries the door's reason: {severed:?}"
    );
}
