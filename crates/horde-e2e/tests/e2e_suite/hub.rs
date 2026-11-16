//! The MCP Hub across a real deployed stack (ADR-0004, 2026-08-19
//! amendment): the operator journals a hub server through `horde mcp add`,
//! the server connects to it (a real `mcp_stub_server` child at the server),
//! a worker daemon's agent calls its tool through the agent's own endpoint —
//! the call worker-forwarded over the daemon's authenticated channel — and
//! the permission chain sits in the path, answered from the CLI like any
//! other escalation.

use horde_server_testkit::process::product_binary;

use crate::support::{System, has_kind, outputs};

/// One hub tool call, end to end with nothing faked: endpoint → permission
/// chain (a real escalation, approved by id from the CLI) → forwarded over
/// the worker's mTLS channel → the server's hub core → the stub upstream —
/// and the upstream's answer back in the agent's turn. The unroutable shape
/// rides the same turn: a tool the upstream never advertised is refused as
/// unroutable, with no second escalation.
#[tokio::test]
async fn an_agent_calls_a_hub_tool_through_the_deployed_stack() {
    // `ask`: the rule chain that escalates what it does not allow, so the
    // call stops for a human the test plays (ADR-0016's default, bypass,
    // would allow it with no one asked).
    let system = System::launch_with(&["--permission-mode", "ask"]).await;

    // The operator authors the catalog first, through the CLI's own verb:
    // one hub-placed stdio server the *server* will spawn and own.
    let stub = product_binary("horde-mcp", "mcp_stub_server")
        .await
        .expect("the stub MCP server binary exists");
    let added = system
        .cli(&["mcp", "add", "stub", "--command", &stub.to_string_lossy()])
        .await;
    assert!(
        added.status.success(),
        "the operator configures the hub server: {}",
        String::from_utf8_lossy(&added.stderr)
    );

    // The scripted provider: one turn that calls a hub tool (the id is the
    // script's to choose — it rides `_meta` like the real harness's, so the
    // approval below can name the request without scraping a render), then
    // a tool the upstream never advertised.
    let mut worker = system
        .spawn_scripted_worker(
            "w-1",
            r#"[{"steps": [
                {"say": "calling the hub"},
                {"use_horde": {"id": "hub-1", "tool": "stub__echo",
                               "input": {"message": "through the deployed hub"}}},
                {"use_horde": {"id": "hub-2", "tool": "stub__no_such_tool",
                               "input": {}}},
                {"say": "hub answered"}
            ], "text": "done"}]"#,
        )
        .await;

    let (session_id, root) = system.placed_session("e2e hub").await;
    system.cli_json(&["send", &root, "go"]).await;

    // The chain is in the path: the proxied call surfaces as an ordinary
    // escalation (the `ask` mode escalates everything it does not allow),
    // and the agent is blocked on it — not past it.
    let events = system
        .sync_until(&session_id, "the hub call's escalation", |events| {
            has_kind(events, "permission_requested")
                && outputs(events).iter().any(|text| text == "calling the hub")
        })
        .await;
    assert!(
        outputs(&events)
            .iter()
            .any(|text| text == "calling the hub"),
        "the pre-call output arrived before the block: {events:?}"
    );
    assert!(
        !outputs(&events)
            .iter()
            .any(|text| text.starts_with("echo:")),
        "no upstream answer may exist before the decision: {events:?}"
    );

    // The raised request is the script's own call, under the script's own
    // id: `_meta.claudecode/toolUseId` became the request id, which is what
    // lets an approver address it without scraping anything.
    let raised = events
        .iter()
        .find(|event| event["kind"] == "permission_requested")
        .map(|event| event["detail"].clone())
        .unwrap_or_default();
    assert_eq!(raised["tool"], "stub__echo", "{raised}");
    assert_eq!(raised["request_id"], "hub-1", "{raised}");

    // Any attached client may answer, by the id the script chose.
    let approved = system.cli(&["approve", "hub-1"]).await;
    assert!(
        approved.status.success(),
        "the approval must land: {}",
        String::from_utf8_lossy(&approved.stderr)
    );

    // The round trip: the stub's marked answer comes back through server,
    // worker channel, endpoint, and harness into the session log.
    let events = system
        .sync_until(&session_id, "the hub tool's answer", |events| {
            outputs(events)
                .iter()
                .any(|text| text == "echo:through the deployed hub")
                && has_kind(events, "permission_decided")
        })
        .await;
    assert!(
        has_kind(&events, "permission_decided"),
        "the decision is an event of record: {events:?}"
    );

    // The unroutable call refuses without a second escalation: an
    // unadvertised tool is not on the endpoint's menu, so it never reaches
    // the chain — and the turn still finishes.
    let events = system
        .sync_until(&session_id, "the finished turn", |events| {
            let texts = outputs(events);
            texts.iter().any(|text| text == "hub answered")
                && texts
                    .iter()
                    .any(|text| text.contains("no tool named stub__no_such_tool"))
        })
        .await;
    assert!(
        outputs(&events)
            .iter()
            .any(|text| text.contains("no tool named stub__no_such_tool")),
        "the unroutable refusal reaches the agent in words: {events:?}"
    );
    // Only the routable call escalated. Two `permission_requested` events
    // exist — the endpoint's raise and the chain driver's enriched re-raise
    // — but both are the one request, under the script's one id.
    assert!(
        events
            .iter()
            .filter(|event| event["kind"] == "permission_requested")
            .all(|event| event["detail"]["request_id"] == "hub-1"),
        "only the routable call escalates: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}

/// Visibility fails closed across the deployed stack: a hub server scoped to
/// an agent type this agent does not have is unlisted *and* unroutable — the
/// call refuses as unknown, with the chain never consulted.
#[tokio::test]
async fn a_scoped_hub_server_is_unroutable_to_other_agents() {
    let system = System::launch().await;

    let stub = product_binary("horde-mcp", "mcp_stub_server")
        .await
        .expect("the stub MCP server binary exists");
    let added = system
        .cli(&[
            "mcp",
            "add",
            "scoped",
            "--command",
            &stub.to_string_lossy(),
            "--allow-agent-type",
            "reviewer",
        ])
        .await;
    assert!(
        added.status.success(),
        "the operator configures the scoped server: {}",
        String::from_utf8_lossy(&added.stderr)
    );

    let mut worker = system
        .spawn_scripted_worker(
            "w-1",
            r#"[{"steps": [
                {"use_horde": {"id": "hub-1", "tool": "scoped__echo",
                               "input": {"message": "should never arrive"}}},
                {"say": "turn finished"}
            ], "text": "done"}]"#,
        )
        .await;

    let (session_id, root) = system.placed_session("e2e hub scoping").await;
    system.cli_json(&["send", &root, "go"]).await;

    let events = system
        .sync_until(&session_id, "the finished turn", |events| {
            let texts = outputs(events);
            texts.iter().any(|text| text == "turn finished")
                && texts
                    .iter()
                    .any(|text| text.contains("no tool named scoped__echo"))
        })
        .await;
    assert!(
        outputs(&events)
            .iter()
            .any(|text| text.contains("no tool named scoped__echo")),
        "the scoped server must be unroutable to an untyped agent: {events:?}"
    );
    assert!(
        !has_kind(&events, "permission_requested"),
        "an unroutable name never reaches the chain: {events:?}"
    );
    assert!(
        !outputs(&events).iter().any(|text| text.contains("echo:")),
        "nothing may have crossed to the upstream: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}
