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
    /// A clone of the served handler, for the handler-level assertions the
    /// wire cannot make (rmcp answers `tools/call` routing itself, but
    /// `get_tool` is consulted internally for validation).
    endpoint: HordeEndpoint,
    _hub_serving: horde_mcp::server::ServingEndpoint,
    _endpoint_serving: horde_mcp::server::ServingEndpoint,
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

/// Stands the whole stack up in named, individually bounded stages.
///
/// One shared ceiling over all eight hops is what failed here once: a
/// cargo-mutants scratch baseline on a loaded machine starved the stand-up
/// (dominated by the freshly linked stub binary's first-exec validation —
/// see `stub_mcp::warm`) and every test reported only that "the stack" hung,
/// naming no hop. Each hop now carries its own ceiling-bounded stage, so a
/// hang is a named failure, and the one environment-priced cost is paid
/// once, up front, under its own explicit bound.
async fn stack() -> Setup<Stack> {
    let horde = common::stage(
        "the permissioned server stands up",
        harness_with_permissions(modes(), None),
    )
    .await?;

    // The one cost here that belongs to the environment rather than the
    // code: the stub binary's first launch. Paid before any handshake
    // depends on it, once per test process.
    stub_mcp::warm().await?;

    // -- The hub, at the "server": two user servers, connected once. --------
    let hub_proxy = ToolProxy::new()
        .with_server(
            common::stage(
                "the hub's linear upstream connects",
                stub_mcp::upstream("linear"),
            )
            .await?,
        )
        .with_server(
            common::stage(
                "the hub's payments upstream connects",
                stub_mcp::upstream("payments"),
            )
            .await?,
        );
    let hub_registry = AgentRegistry::new();
    hub_registry.insert(&horde.agent_id, HUB_TOKEN);
    let (hub_address, hub_serving) = common::stage("the hub endpoint serves", async {
        Ok(serve(
            &EndpointConfig::loopback(0),
            Arc::new(hub_registry),
            HubEndpoint::new(Arc::new(hub_proxy)),
        )
        .await?)
    })
    .await?;

    // -- The agent's endpoint, at the "worker". ------------------------------
    // The hub link rides the agent's own identity pair, the loopback+bearer
    // local mode; `visible` is what the spawn command resolved for this
    // agent — payments is deliberately absent.
    let hub_link = common::stage("the hub link dials", async {
        Ok(Upstream::dial(
            "hub",
            &format!("http://{hub_address}/mcp"),
            Some(HUB_TOKEN),
            &[("x-horde-agent", &horde.agent_id)],
        )
        .await?)
    })
    .await?;
    // The `local` server: a real child process, spawned per-agent from the
    // journaled config's command and env.
    let local = common::stage(
        "the local child process spawns and completes its handshake",
        async {
            Ok(Upstream::connect(&HubServerConfig {
                name: "browser".to_owned(),
                transport: HubTransport::Stdio {
                    command: stub_mcp::BINARY.to_owned(),
                    args: Vec::new(),
                    env: [].into(),
                },
                placement: Placement::Local,
            })
            .await?)
        },
    )
    .await?;

    let proxy = ToolProxy::new()
        .with_hub_link(hub_link, ["linear".to_owned()])
        .with_server(local);
    // The per-agent registry the Worker's endpoint manager populates in
    // production: this agent's proxy under this agent's id, nobody else's.
    let proxies = Arc::new(horde_mcp::hub::AgentProxies::new());
    proxies.insert(&horde.agent_id, Arc::new(proxy));
    let endpoint = HordeEndpoint::new(horde.context().await?).with_agent_proxies(proxies);

    let registry = AgentRegistry::new();
    registry.insert(&horde.agent_id, AGENT_TOKEN);
    let (address, endpoint_serving) = common::stage("the agent endpoint serves", async {
        Ok(serve(
            &EndpointConfig::loopback(0),
            Arc::new(registry),
            endpoint.clone(),
        )
        .await?)
    })
    .await?;

    let client = common::stage(
        "the MCP client initializes",
        McpClient::connect(address, &horde.agent_id, AGENT_TOKEN),
    )
    .await?;
    Ok(Stack {
        horde,
        client,
        endpoint,
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
    let stack = stack().await.expect("every hop must come up");
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
    let stack = stack().await.expect("every hop must come up");

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
    let stack = stack().await.expect("every hop must come up");

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
    let stack = stack().await.expect("every hop must come up");

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
    let stack = stack().await.expect("every hop must come up");

    let answer = within(
        "the local call crosses endpoint, chain, and child process",
        stack
            .client
            .call_tool("browser__echo", serde_json::json!({"message": "locally"})),
    )
    .await
    .expect("the deterministic allow must let it through");
    assert_eq!(answer, "echo:locally");

    assert!(
        chain_saw(&stack, "browser__echo")
            .await
            .expect("the log is readable"),
        "local tools are gated by the same chain as hub tools"
    );
}

// -- One tool call, one gate -------------------------------------------------
//
// Each harness gates an MCP tool call on its own side before the `tools/call`
// reaches this endpoint, and the endpoint gates proxied calls too. These pin
// that the two gates are one permission event: one request in the log, one
// answer from a human, one tool name a rule has to match.

/// Every distinct permission request in the log, as `(request_id, tool_name)`.
/// Distinct, because the chain's enriched re-raise repeats an escalated
/// request under its own id — that is the same question, not a second one.
async fn requests(stack: &Stack) -> Setup<Vec<(String, String)>> {
    let mut client = stack.horde.client().await?;
    let mut seen: Vec<(String, String)> = Vec::new();
    client
        .sync(
            &horde_cli::client::SyncOptions::new(stack.horde.session_id.clone()),
            |envelope| {
                let Some(horde_proto::v1::event::Kind::PermissionRequested(requested)) = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                else {
                    return;
                };
                let Some(request) = requested.request.as_ref() else {
                    return;
                };
                let id = request
                    .request_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                let tool = request
                    .tool_call
                    .as_ref()
                    .map(|call| call.tool_name.clone())
                    .unwrap_or_default();
                if !seen.iter().any(|(known, _)| *known == id) {
                    seen.push((id, tool));
                }
            },
            |_| {},
        )
        .await?;
    Ok(seen)
}

/// A `tools/call` carrying `meta`, the way a harness sends one.
async fn call_with_meta(
    stack: &Stack,
    name: &str,
    arguments: serde_json::Value,
    meta: serde_json::Value,
) -> Setup<serde_json::Value> {
    stack
        .client
        .rpc(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": { "name": name, "arguments": arguments, "_meta": meta },
        }))
        .await
}

/// The `_meta` codex stamps on every `tools/call` (transcript 17's stub log).
fn codex_meta() -> serde_json::Value {
    serde_json::json!({
        "x-codex-turn-metadata": {"thread_id": "th-1", "turn_id": "tu-1"},
        "progressToken": 1,
    })
}

/// Journals a codex elicitation the way the Worker does — under the item id
/// and `horde/{tool}` — and answers it as a human would.
async fn elicited_and_answered(
    stack: &Stack,
    item_id: &str,
    tool_name: &str,
    arguments: serde_json::Value,
    outcome: horde_proto::v1::PermissionOutcome,
) -> Setup<()> {
    let call = horde_adapter::permission::ProposedToolCall {
        tool_call_id: horde_adapter::ToolCallId::new(item_id),
        tool_name: tool_name.to_owned(),
        input: arguments,
        questions: Vec::new(),
    };
    let agent_id = stack.horde.agent_id.clone();
    stack
        .horde
        .journal()
        .record(horde_mcp::journal::JournalEntry {
            session_id: stack.horde.session_id.clone(),
            agent_id: Some(agent_id.clone()),
            idempotency_key: format!("{agent_id}:{item_id}:requested"),
            kind: horde_proto::v1::event::Kind::PermissionRequested(
                horde_proto::v1::PermissionRequested {
                    request: Some(horde_proto::v1::PermissionRequest {
                        request_id: Some(horde_proto::v1::PermissionRequestId::new(item_id)),
                        agent_id: Some(horde_proto::v1::AgentId::new(agent_id)),
                        tool_call: Some(horde_wire::permission::proposed_tool_call_to_wire(&call)?),
                        ..horde_proto::v1::PermissionRequest::default()
                    }),
                },
            ),
        })
        .await?;
    let mut answering = stack.horde.client().await?;
    answering
        .answer_permission(horde_cli::client::PermissionAnswer {
            request_id: item_id,
            outcome,
            reason: "answered at the elicitation",
            remember_for_session: false,
            updated_input: None,
            answers: horde_adapter::Answers::new(),
        })
        .await?;
    Ok(())
}

/// Answers the first request raised under `tool_name`, found by watching the
/// log the way an approver's client does.
async fn answer_when_raised(stack: &Stack, tool_name: &str) -> Setup<String> {
    let mut client = stack.horde.client().await?;
    let mut stream = client
        .follow(&horde_cli::client::SyncOptions::new(
            stack.horde.session_id.clone(),
        ))
        .await?;
    let wanted = tool_name.to_owned();
    let request_id = within(
        "the endpoint's raise appears in the log",
        stream.next_matching(move |envelope| {
            let Some(horde_proto::v1::event::Kind::PermissionRequested(requested)) =
                envelope.event.as_ref()?.kind.as_ref()
            else {
                return None;
            };
            let request = requested.request.as_ref()?;
            (request.tool_call.as_ref()?.tool_name == wanted)
                .then(|| request.request_id.as_ref().map(ToString::to_string))?
        }),
    )
    .await?
    .ok_or("the raise never appeared")?;
    client
        .answer_permission(horde_cli::client::PermissionAnswer::allow(&request_id))
        .await?;
    Ok(request_id)
}

/// Codex asked first: the elicitation's decision covers the call, so the
/// endpoint forwards it without a second request — the human answered once.
#[tokio::test]
async fn a_codex_call_its_elicitation_already_decided_is_not_gated_again() {
    let stack = stack().await.expect("every hop must come up");
    let arguments = serde_json::json!({"message": "asked once"});
    elicited_and_answered(
        &stack,
        "item-codex-1",
        "horde/linear__echo",
        arguments.clone(),
        horde_proto::v1::PermissionOutcome::Allow,
    )
    .await
    .expect("the elicitation is raised and answered");

    let rpc = within(
        "the elicited call is forwarded",
        call_with_meta(&stack, "linear__echo", arguments, codex_meta()),
    )
    .await
    .expect("the call answers");
    assert_eq!(
        rpc["result"]["content"][0]["text"], "echo:asked once",
        "the elicitation's allow lets the call through: {rpc}"
    );
    assert_eq!(
        requests(&stack).await.expect("the log is readable"),
        vec![("item-codex-1".to_owned(), "horde/linear__echo".to_owned())],
        "one call, one request: the endpoint must not ask again"
    );
}

/// A refusal at the elicitation is the call's refusal too, even if a
/// `tools/call` arrives anyway.
#[tokio::test]
async fn a_codex_call_its_elicitation_denied_stays_denied() {
    let stack = stack().await.expect("every hop must come up");
    let arguments = serde_json::json!({"message": "refused"});
    elicited_and_answered(
        &stack,
        "item-codex-2",
        "horde/linear__echo",
        arguments.clone(),
        horde_proto::v1::PermissionOutcome::Deny,
    )
    .await
    .expect("the elicitation is raised and answered");

    let rpc = within(
        "the refused call answers",
        call_with_meta(&stack, "linear__echo", arguments, codex_meta()),
    )
    .await
    .expect("a denial is an answer");
    assert_eq!(rpc["result"]["isError"], true, "{rpc}");
    assert_eq!(
        requests(&stack).await.expect("the log is readable").len(),
        1,
        "the denial is not re-asked"
    );
}

/// A codex call nobody asked about is gated here — and under the name its
/// elicitation would have carried, so one rule covers the tool either way.
#[tokio::test]
async fn an_unasked_codex_call_is_gated_once_under_the_elicitation_name() {
    let stack = stack().await.expect("every hop must come up");
    let (rpc, answered) = tokio::join!(
        within(
            "the gated call answers once allowed",
            call_with_meta(
                &stack,
                "linear__echo",
                serde_json::json!({"message": "unasked"}),
                codex_meta(),
            ),
        ),
        answer_when_raised(&stack, "horde/linear__echo"),
    );
    let rpc = rpc.expect("the call answers");
    let answered = answered.expect("the raise is answered");
    assert_eq!(rpc["result"]["content"][0]["text"], "echo:unasked", "{rpc}");
    assert_eq!(
        requests(&stack).await.expect("the log is readable"),
        vec![(answered, "horde/linear__echo".to_owned())],
        "gated exactly once, under the canonical codex name"
    );
}

/// Claude's half of the same rule: `approve` decides under the tool-use id,
/// and the `tools/call` stamped with that id is a keyed replay, not a second
/// question.
#[tokio::test]
async fn a_claude_call_its_approve_already_decided_is_not_gated_again() {
    let stack = stack().await.expect("every hop must come up");
    let arguments = serde_json::json!({"message": "approved once"});
    let (approved, answered) = tokio::join!(
        within(
            "approve is decided",
            stack.client.call_tool(
                "approve",
                serde_json::json!({
                    "tool_name": "mcp__horde__linear__echo",
                    "input": arguments.clone(),
                    "tool_use_id": "toolu_once",
                }),
            ),
        ),
        answer_when_raised(&stack, "mcp__horde__linear__echo"),
    );
    answered.expect("the approve-stage raise is answered");
    let approved: serde_json::Value =
        serde_json::from_str(&approved.expect("approve decides")).expect("a JSON decision");
    assert_eq!(approved["behavior"], "allow");

    let rpc = within(
        "the approved call is forwarded",
        call_with_meta(
            &stack,
            "linear__echo",
            arguments,
            serde_json::json!({"claudecode/toolUseId": "toolu_once"}),
        ),
    )
    .await
    .expect("the call answers");
    assert_eq!(
        rpc["result"]["content"][0]["text"], "echo:approved once",
        "{rpc}"
    );
    assert_eq!(
        requests(&stack).await.expect("the log is readable"),
        vec![(
            "toolu_once".to_owned(),
            "mcp__horde__linear__echo".to_owned()
        )],
        "one call, one request"
    );
}

/// `get_tool` is rmcp's own validation hook, and it must agree with the
/// listing: a verb by name, a visible proxied tool by its namespaced name,
/// and nothing for a name outside this agent's surface.
#[tokio::test]
async fn get_tool_answers_for_exactly_the_tools_this_agent_may_see() {
    use rmcp::ServerHandler;

    let stack = stack().await.expect("every hop must come up");

    let verb = stack
        .endpoint
        .get_tool("spawn_agent")
        .expect("the verbs answer by name");
    assert_eq!(verb.name, "spawn_agent");

    let proxied = stack
        .endpoint
        .get_tool("linear__echo")
        .expect("a visible hub tool answers by its namespaced name");
    assert_eq!(
        proxied.name, "linear__echo",
        "the answer is the asked-for tool, not the first one"
    );

    assert!(
        stack.endpoint.get_tool("payments__echo").is_none(),
        "a hub tool outside this agent's visibility must not resolve"
    );
    assert!(
        stack.endpoint.get_tool("no_such_tool").is_none(),
        "an unknown name resolves to nothing, never to a default"
    );
}
