//! ADR-0004 end to end, with nothing faked at any hop:
//!
//! a legacy-era MCP client (the recorded Claude Code flow) → the Agent's
//! endpoint over real HTTP → the permission chain on a real `horde-server`
//! (the DESIGN §10 placement: every proxied call is adjudicated in the
//! dispatch path) → the hub link over real HTTP to the hub's own endpoint →
//! a real rmcp upstream — plus a `local` server spawned as a real child
//! process beside the agent, the Worker-side pressure valve.

mod common;

use std::sync::Arc;

use common::mcp_client::McpClient;
use common::{Setup, harness_with_permissions, stub_mcp, within};
use horde_mcp::endpoint::HordeEndpoint;
use horde_mcp::hub::{HubEndpoint, HubServerConfig, HubTransport, Placement, ToolProxy, Upstream};
use horde_mcp::identity::AgentRegistry;
use horde_mcp::server::{EndpointConfig, serve};
use horde_server::permission::{PermissionMode, PermissionModes, Rule};

const AGENT_TOKEN: &str = "hub-e2e-agent-token";
const HUB_TOKEN: &str = "hub-e2e-hub-token";

/// The whole stack, held alive for one test.
struct Stack {
    horde: common::Harness,
    client: McpClient,
    _hub_serving: tokio::task::JoinHandle<()>,
    _endpoint_serving: tokio::task::JoinHandle<()>,
}

/// The agent's mode: deterministic rules over the namespaced tool names, so
/// every outcome here is the chain's — no human is attached anywhere.
fn modes() -> PermissionModes {
    PermissionModes::default().with_default(
        PermissionMode::escalate_everything()
            .allow(Rule::tool("linear__echo"))
            .allow(Rule::tool("browser__echo"))
            .deny(Rule::tool("linear__blocked"), "hub tools are gated"),
    )
}

async fn stack() -> Setup<Stack> {
    let horde = harness_with_permissions(modes(), None).await?;

    // -- The hub, at the "server": two user servers, connected once. --------
    let hub_proxy = ToolProxy::new()
        .with_server(stub_mcp::upstream("linear").await?)
        .with_server(stub_mcp::upstream("payments").await?);
    let mut hub_registry = AgentRegistry::new();
    hub_registry.insert(&horde.agent_id, HUB_TOKEN);
    let (hub_address, hub_serving) = serve(
        &EndpointConfig::loopback(0),
        Arc::new(hub_registry),
        HubEndpoint::new(Arc::new(hub_proxy)),
    )
    .await?;

    // -- The agent's endpoint, at the "worker". ------------------------------
    // The hub link rides the agent's own identity pair, the loopback+bearer
    // local mode; `visible` is what the spawn command resolved for this
    // agent — payments is deliberately absent.
    let hub_link = Upstream::dial(
        "hub",
        &format!("http://{hub_address}/mcp"),
        Some(HUB_TOKEN),
        &[("x-horde-agent", &horde.agent_id)],
    )
    .await?;
    // The `local` server: a real child process, spawned per-agent from the
    // journaled config's command and env.
    let local = Upstream::connect(&HubServerConfig {
        name: "browser".to_owned(),
        transport: HubTransport::Stdio {
            command: env!("CARGO_BIN_EXE_mcp_stub_server").to_owned(),
            args: Vec::new(),
            env: [].into(),
        },
        placement: Placement::Local,
    })
    .await?;

    let proxy = ToolProxy::new()
        .with_hub_link(hub_link, ["linear".to_owned()])
        .with_server(local);
    let endpoint = HordeEndpoint::new(horde.context().await?).with_proxy(Arc::new(proxy));

    let mut registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, AGENT_TOKEN);
    let (address, endpoint_serving) =
        serve(&EndpointConfig::loopback(0), Arc::new(registry), endpoint).await?;

    let client = McpClient::connect(address, &horde.agent_id, AGENT_TOKEN).await?;
    Ok(Stack {
        horde,
        client,
        _hub_serving: hub_serving,
        _endpoint_serving: endpoint_serving,
    })
}

/// Whether the log holds a `PermissionRequested` for `tool_name` — the proof
/// the chain was *in the path*, not merely that the outcome matched.
async fn chain_saw(stack: &Stack, tool_name: &str) -> Setup<bool> {
    let mut client = stack.horde.client().await?;
    let mut saw = false;
    client
        .sync(
            &horde_cli::client::SyncOptions::new(stack.horde.session_id.clone()),
            |envelope| {
                let requested =
                    envelope
                        .event
                        .as_ref()
                        .and_then(|event| match event.kind.as_ref() {
                            Some(horde_proto::v1::event::Kind::PermissionRequested(requested)) => {
                                requested.request.as_ref()
                            }
                            _ => None,
                        });
                if requested
                    .and_then(|request| request.tool_call.as_ref())
                    .is_some_and(|call| call.tool_name == tool_name)
                {
                    saw = true;
                }
            },
            |_| {},
        )
        .await?;
    Ok(saw)
}

#[tokio::test]
async fn the_listed_toolset_is_the_verbs_plus_exactly_what_this_agent_may_see() {
    let stack = within("the stack stands up", stack())
        .await
        .expect("every hop must come up");
    let names = stack
        .client
        .list_tool_names()
        .await
        .expect("the listing must answer");

    for verb in ["approve", "spawn_agent", "send_message", "report_result"] {
        assert!(
            names.contains(&verb.to_owned()),
            "{verb} missing: {names:?}"
        );
    }
    assert!(
        names.contains(&"linear__echo".to_owned()),
        "a visible hub server's tools ride the one endpoint: {names:?}"
    );
    assert!(
        names.contains(&"browser__echo".to_owned()),
        "the local server's tools ride the same endpoint: {names:?}"
    );
    assert!(
        !names.iter().any(|name| name.starts_with("payments__")),
        "a hub server outside this agent's visibility must not be listed: {names:?}"
    );
}

#[tokio::test]
async fn an_allowed_hub_call_round_trips_and_the_chain_was_in_its_path() {
    let stack = within("the stack stands up", stack())
        .await
        .expect("every hop must come up");

    let answer = within(
        "the call crosses endpoint, chain, hub, and upstream",
        stack.client.call_tool(
            "linear__echo",
            serde_json::json!({"message": "through the hub"}),
        ),
    )
    .await
    .expect("the deterministic allow must let it through");
    assert_eq!(answer, "echo:through the hub");

    assert!(
        chain_saw(&stack, "linear__echo")
            .await
            .expect("the log is readable"),
        "the call must have been raised as a PermissionRequested — the chain \
         in the path, not just a matching outcome"
    );
}

#[tokio::test]
async fn a_denied_hub_call_is_stopped_at_the_endpoint_with_the_rules_reason() {
    let stack = within("the stack stands up", stack())
        .await
        .expect("every hop must come up");

    let rpc = within(
        "the denial answers",
        stack
            .client
            .call_tool_raw("linear__blocked", serde_json::json!({})),
    )
    .await
    .expect("a denial is an answer, not a transport failure");

    let result = &rpc["result"];
    assert_eq!(
        result["isError"], true,
        "a denial is a readable tool error: {rpc}"
    );
    let text = result["content"][0]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("permission denied") && text.contains("hub tools are gated"),
        "the model reads the rule's own reason: {text}"
    );
}

#[tokio::test]
async fn an_invisible_hub_tool_is_unroutable_not_merely_unlisted() {
    let stack = within("the stack stands up", stack())
        .await
        .expect("every hop must come up");

    let rpc = within(
        "the refusal answers",
        stack
            .client
            .call_tool_raw("payments__echo", serde_json::json!({"message": "sneak"})),
    )
    .await
    .expect("a refusal is an answer");

    let error = rpc
        .get("error")
        .unwrap_or_else(|| panic!("calling past the allowlist must be a protocol error: {rpc}"));
    assert_eq!(
        error["code"], -32601,
        "method not found — the tool does not exist for this agent: {error}"
    );

    assert!(
        !chain_saw(&stack, "payments__echo")
            .await
            .expect("the log is readable"),
        "an unroutable name must be refused before the chain, not escalated \
         to a human who cannot make it exist"
    );
}

#[tokio::test]
async fn a_local_servers_tool_rides_the_same_endpoint_and_the_same_chain() {
    let stack = within("the stack stands up", stack())
        .await
        .expect("every hop must come up");

    let answer = within(
        "the local call crosses endpoint, chain, and child process",
        stack
            .client
            .call_tool("browser__echo", serde_json::json!({"message": "locally"})),
    )
    .await
    .expect("the deterministic allow must let it through");
    // The child fixture echoes verbatim (the in-memory stub prefixes).
    assert_eq!(answer, "locally");

    assert!(
        chain_saw(&stack, "browser__echo")
            .await
            .expect("the log is readable"),
        "local tools are gated by the same chain as hub tools"
    );
}
