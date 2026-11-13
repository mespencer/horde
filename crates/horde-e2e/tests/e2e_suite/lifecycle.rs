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
    let mut worker = system.spawn_echo_worker("w-1").await;

    let (session_id, root) = system.new_session("e2e lifecycle").await;

    // The scheduler answered inside the create: the root agent is placed on
    // the one registered worker before anything is sent to it — a message
    // to an unplaced agent would sit in the log with nobody to deliver to.
    let events = system
        .sync_until(&session_id, "the root agent's placement", |events| {
            events.iter().any(|event| {
                event["kind"] == "placement_changed"
                    && event["detail"]
                        .as_str()
                        .is_some_and(|d| d.starts_with("placed"))
            })
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
    // journaling path, not just the RPC's ack. The output chunk and the
    // turn's usage journal as separate transactions, so the wait names every
    // fact the assertions below consume — wait on the fact, not the count.
    system.cli_json(&["send", &root, "hello horde"]).await;
    let events = system
        .sync_until(
            &session_id,
            "the first echo and its turn's facts",
            |events| {
                outputs(events).iter().any(|text| text == "hello horde")
                    && has_kind(events, "agent_lifecycle_changed")
                    && has_kind(events, "usage_recorded")
            },
        )
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
    let mut worker = system
        .spawn_scripted_worker(
            "w-1",
            r#"[{"steps": [
                {"say": "delegating"},
                {"use_horde": {"id": "call-1", "tool": "spawn_agent", "input": {"task": ""}}},
                {"use_horde": {"id": "call-2", "tool": "report_result",
                               "input": {"status": "success", "summary": "delegated the work"}}}
            ], "text": "done"}]"#,
        )
        .await;

    let (session_id, root) = system.placed_session("e2e endpoint verbs").await;
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
                && outputs(events).iter().any(|text| text == "delegating")
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
            events.iter().any(|event| {
                event["kind"] == "result_reported"
                    && event["agent_id"].as_str() == Some(root.as_str())
            })
        })
        .await;
    assert!(
        events.iter().any(|event| event["kind"] == "result_reported"
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
    let mut worker = system
        .spawn_scripted_worker(
            "w-1",
            r#"[{"steps": [
                {"say": "checking the blast radius"},
                {"ask_permission": {"id": "call-1", "tool": "Bash",
                                    "input": {"command": "docker rm horde-test"}}},
                {"say": "approved and done"},
                {"ask_permission": {"id": "call-2", "tool": "Bash",
                                    "input": {"command": "docker rm horde-test"}}},
                {"say": "remembered and done"}
            ], "text": "done"}]"#,
        )
        .await;

    let (session_id, root) = system.placed_session("e2e permissions").await;
    system.cli_json(&["send", &root, "go"]).await;
    let events = system
        .sync_until(&session_id, "the permission escalation", |events| {
            has_kind(events, "permission_requested")
                && outputs(events)
                    .iter()
                    .any(|text| text == "checking the blast radius")
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

    // Any attached client may answer (DESIGN §10); this one does — and asks
    // the chain to remember the answer.
    let approved = system.cli(&["approve", "call-1", "--remember"]).await;
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
                && has_kind(events, "permission_decided")
        })
        .await;
    assert!(
        has_kind(&events, "permission_decided"),
        "the decision is an event of record, not just a delivery: {events:?}"
    );

    // The second, identical ask resolves with no human in the loop: the
    // chain driver the deployed server spawns (wave-7 fix a5) answers it
    // from the remembered decision. Reaching the final output with exactly
    // one `approve` call is the proof.
    let events = system
        .sync_until(&session_id, "the remembered decision", |events| {
            outputs(events)
                .iter()
                .any(|text| text == "remembered and done")
                && events
                    .iter()
                    .filter(|event| event["kind"] == "permission_decided")
                    .count()
                    >= 2
        })
        .await;
    assert_eq!(
        events
            .iter()
            .filter(|event| event["kind"] == "permission_decided")
            .count(),
        2,
        "the remembered layer's answer is an event of record too: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}

/// A harness question crosses the whole system and comes back answered
/// (W12c): the fake harness asks and blocks, the question reaches the log
/// with its options, a bare `horde approve` is refused (only answers settle a
/// question), `horde answer` settles it by position and option number, and the
/// agent receives the canonical answer.
#[tokio::test]
async fn a_question_is_asked_and_answered_from_the_cli() {
    let system = System::launch().await;
    let mut worker = system
        .spawn_scripted_worker(
            "w-1",
            r#"[{"steps": [
                {"ask_question": {"id": "q-1", "tool": "AskUserQuestion",
                                  "questions": [{"id": "colour", "question": "Which colour?",
                                                 "options": ["Red", "Blue"]}]}},
                {"say": "answer received"}
            ], "text": "done"}]"#,
        )
        .await;

    let (session_id, root) = system.placed_session("e2e question").await;
    system.cli_json(&["send", &root, "go"]).await;
    system
        .sync_until(&session_id, "the question", |events| {
            has_kind(events, "permission_requested")
        })
        .await;

    let refused = system.cli(&["approve", "q-1"]).await;
    assert!(
        !refused.status.success(),
        "a question cannot be allowed without answers"
    );
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("is a question"),
        "the refusal says what to do: {}",
        String::from_utf8_lossy(&refused.stderr)
    );

    let answered = system.cli(&["answer", "q-1", "1=2"]).await;
    assert!(
        answered.status.success(),
        "the answer must land: {}",
        String::from_utf8_lossy(&answered.stderr)
    );

    let events = system
        .sync_until(&session_id, "the answered turn", |events| {
            outputs(events).iter().any(|text| text == "answer received")
        })
        .await;
    assert!(
        outputs(&events)
            .iter()
            .any(|text| text == "answered colour=Blue"),
        "the agent received the canonical answer, keyed by id and valued by label: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}

/// One `horde interrupt` is one request and one acknowledgement, and then
/// the log is quiet (ADR-0009).
///
/// The storm this pins: the server folded *every* `TurnInterrupted` into an
/// interrupt command, including the one the worker journals to say it
/// carried the interrupt out — so each ack commanded another interrupt,
/// whose ack commanded another, and the head ran from 20 to 2662 in three
/// seconds. "Settled" is asserted by a fact rather than a sleep: a steering
/// message sent after the ack is delivered on the same ordered command
/// stream, so by the time its echo is in the log, any interrupt the ack had
/// caused would have been applied and acknowledged ahead of it.
#[tokio::test]
async fn an_interrupt_is_requested_once_and_acknowledged_once() {
    let system = System::launch().await;
    let mut worker = system.spawn_echo_worker("w-1").await;
    let (session_id, root) = system.placed_session("e2e interrupt").await;

    let interrupted = |events: &[serde_json::Value]| {
        events
            .iter()
            .filter(|event| event["kind"] == "turn_interrupted")
            .count()
    };

    system
        .cli_json(&["interrupt", &root, "--reason", "stop"])
        .await;
    system
        .sync_until(&session_id, "the worker's acknowledgement", |events| {
            interrupted(events) >= 2
        })
        .await;

    system
        .cli_json(&["send", &root, "after the interrupt"])
        .await;
    let events = system
        .sync_until(&session_id, "the post-interrupt echo", |events| {
            outputs(events)
                .iter()
                .any(|text| text == "after the interrupt")
        })
        .await;
    assert_eq!(
        interrupted(&events),
        2,
        "one request and its one acknowledgement — anything more is the \
         acknowledgement being folded into another request: {events:?}"
    );

    worker.terminate().await.expect("SIGTERM sends");
    let (status, _, _) = worker.wait().await.expect("the worker exits");
    assert!(status.success());
}
