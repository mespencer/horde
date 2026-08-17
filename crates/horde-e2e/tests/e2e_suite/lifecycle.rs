//! The headline path, as an operator would drive it: enroll a worker, create
//! a session, watch a spawned agent's output arrive at the CLI, steer it,
//! and answer a permission escalation — every hop a real process on a real
//! socket under a real certificate.

use crate::support::{System, has_kind, outputs};

/// Enrollment → session → spawn → output → steering, end to end.
///
/// The chain this holds is long on purpose, because its length is the test:
/// server bootstrap code → operator identity → minted worker code → worker
/// identity → mTLS registration → scheduler placement → spawn command →
/// fake harness turn → worker journal → push stream → server log → CLI
/// replay. A break anywhere is a break somewhere visible.
#[tokio::test]
async fn the_full_lifecycle_runs_over_real_processes() {
    let system = System::launch().await;
    let mut worker = system
        .spawn_worker("w-1", &system.state.path().join("w-1"), &[])
        .await;

    let (session_id, root) = system.new_session("e2e lifecycle").await;

    // The scheduler answered inside the create: the root agent is placed on
    // the one registered worker before anything is sent to it — a message
    // to an unplaced agent would sit in the log with nobody to deliver to.
    let events = system
        .sync_until(&session_id, "the root agent's placement", |events| {
            has_kind(events, "placement_changed")
        })
        .await;
    assert!(
        events
            .iter()
            .any(|event| event["kind"] == "placement_changed"
                && event["detail"]
                    .as_str()
                    .is_some_and(|d| d.starts_with("placed"))),
        "the root agent must be placed, not queued: {events:?}"
    );

    // First assignment: the echo coming back proves the whole delivery and
    // journaling path, not just the RPC's ack.
    system.cli_json(&["send", &root, "hello horde"]).await;
    let events = system
        .sync_until(&session_id, "the first echo", |events| {
            outputs(events).iter().any(|text| text == "hello horde")
        })
        .await;
    assert!(
        has_kind(&events, "agent_lifecycle_changed"),
        "the spawn's lifecycle transitions travel with the output: {events:?}"
    );
    assert!(
        has_kind(&events, "usage_recorded"),
        "the turn's usage is journaled and replayed: {events:?}"
    );

    // Steering: a second message reaches the same live pump.
    system.cli_json(&["send", &root, "and again"]).await;
    system
        .sync_until(&session_id, "the steered echo", |events| {
            outputs(events).iter().any(|text| text == "and again")
        })
        .await;

    worker.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success(), "a signalled worker shutdown exits zero");
    assert!(
        stdout.contains("shutdown reaped=1"),
        "the agent's pump was reaped on the way out: {stdout}"
    );
}

/// An agent drives the orchestrator through its own MCP endpoint: the worker
/// daemon serves the session's endpoint, mints the agent's identity pair, and
/// the scripted harness dials back over real HTTP — spawning a child under
/// itself and reporting its result. Every hop is production wiring: daemon →
/// endpoint gate → verbs → control plane / stamping sink → server log → CLI.
#[tokio::test]
async fn an_agent_reaches_the_orchestrator_verbs_through_its_endpoint() {
    let system = System::launch().await;

    // One turn: announce, spawn an idle child (no task — a child that got the
    // same script would spawn grandchildren forever), report the result.
    let script = system.state.path().join("delegate.json");
    std::fs::write(
        &script,
        r#"[{"steps": [
            {"say": "delegating"},
            {"use_horde": {"id": "call-1", "tool": "spawn_agent", "input": {"task": ""}}},
            {"use_horde": {"id": "call-2", "tool": "report_result",
                           "input": {"status": "success", "summary": "delegated the work"}}}
        ], "text": "done"}]"#,
    )
    .expect("the script writes");

    let mut worker = system
        .spawn_worker(
            "w-1",
            &system.state.path().join("w-1"),
            &["--fake-script", &script.to_string_lossy()],
        )
        .await;

    let (session_id, root) = system.new_session("e2e endpoint verbs").await;
    system
        .sync_until(&session_id, "the root agent's placement", |events| {
            has_kind(events, "placement_changed")
        })
        .await;

    system.cli_json(&["send", &root, "go"]).await;

    // The child exists because the *agent* asked for it, over its endpoint:
    // a second spawn in the log, placed on the one worker.
    let events = system
        .sync_until(&session_id, "the endpoint-spawned child", |events| {
            events
                .iter()
                .filter(|event| event["kind"] == "agent_spawned")
                .count()
                >= 2
        })
        .await;
    assert!(
        outputs(&events).iter().any(|text| text == "delegating"),
        "the pre-spawn output arrived: {events:?}"
    );

    // And the result the agent reported through the same endpoint is an
    // event of record, attributed to the reporting agent.
    let events = system
        .sync_until(&session_id, "the reported result", |events| {
            has_kind(events, "result_reported")
        })
        .await;
    assert!(
        events
            .iter()
            .any(|event| event["kind"] == "result_reported"
                && event["agent_id"].as_str() == Some(root.as_str())),
        "the result must be the root agent's own: {events:?}"
    );

    // Both pumps — the root's and the endpoint-spawned child's — were real,
    // and shutdown reaps them both.
    worker.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success(), "a signalled worker shutdown exits zero");
    assert!(
        stdout.contains("shutdown reaped=2"),
        "the parent and its endpoint-spawned child were both live pumps: {stdout}"
    );
}

/// A permission escalation crosses the whole system and comes back: the fake
/// harness proposes a tool call and blocks, the request reaches an attached
/// client through the log, `horde approve` answers it, and the turn resumes.
#[tokio::test]
async fn a_permission_is_requested_and_approved_from_the_cli() {
    let system = System::launch().await;

    // The scripted provider: one turn that asks before acting. The ids are
    // the test's to choose, which is how the approval can name the request
    // without scraping it from a render.
    let script = system.state.path().join("ask.json");
    std::fs::write(
        &script,
        r#"[{"steps": [
            {"say": "checking the blast radius"},
            {"ask_permission": {"id": "call-1", "tool": "Bash",
                                "input": {"command": "docker rm horde-test"}}},
            {"say": "approved and done"}
        ], "text": "done"}]"#,
    )
    .expect("the script writes");

    let mut worker = system
        .spawn_worker(
            "w-1",
            &system.state.path().join("w-1"),
            &["--fake-script", &script.to_string_lossy()],
        )
        .await;

    let (session_id, root) = system.new_session("e2e permissions").await;
    system
        .sync_until(&session_id, "the root agent's placement", |events| {
            has_kind(events, "placement_changed")
        })
        .await;

    system.cli_json(&["send", &root, "go"]).await;
    let events = system
        .sync_until(&session_id, "the permission escalation", |events| {
            has_kind(events, "permission_requested")
        })
        .await;
    assert!(
        outputs(&events)
            .iter()
            .any(|text| text == "checking the blast radius"),
        "the pre-ask output arrived before the block: {events:?}"
    );
    assert!(
        !outputs(&events)
            .iter()
            .any(|text| text == "approved and done"),
        "the harness must be blocked on the decision, not past it: {events:?}"
    );

    // Any attached client may answer (DESIGN §10); this one does.
    let approved = system.cli(&["approve", "call-1"]).await;
    assert!(
        approved.status.success(),
        "the approval must land: {}",
        String::from_utf8_lossy(&approved.stderr)
    );

    let events = system
        .sync_until(&session_id, "the resumed turn", |events| {
            outputs(events)
                .iter()
                .any(|text| text == "approved and done")
        })
        .await;
    assert!(
        has_kind(&events, "permission_decided"),
        "the decision is an event of record, not just a delivery: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}
