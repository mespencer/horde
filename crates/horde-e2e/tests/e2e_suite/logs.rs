//! Diagnostic logs end to end (ADR-0015), every hop a real process: a real
//! worker daemon's harness stderr and its own log, read back through the
//! server by `horde logs` and by the agent itself through `horde/logs` —
//! redacted wherever a record leaves its machine, unredacted only through
//! `--dir` on the machine that wrote it.

use crate::support::{System, outputs};

/// Turn one writes a secret to stderr and then reads its own logs through
/// the endpoint; turn two writes a second stderr line for the follow to see.
const SCRIPT: &str = r#"[
    {"steps": [
        {"stderr": "harness warning horde://join/h:9/s3cr3t.fp"},
        {"use_horde": {"id": "call-1", "tool": "logs", "input": {"target": "self"}}},
        {"say": "looked"}
    ], "text": "done"},
    {"steps": [
        {"stderr": "second-stderr-line"},
        {"say": "again"}
    ], "text": "done"}
]"#;

fn json_lines(output: &std::process::Output) -> Vec<serde_json::Value> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("each line is one JSON record"))
        .collect()
}

#[tokio::test]
async fn worker_logs_and_agent_stderr_reach_the_cli_and_the_agent_redacted() {
    let system = System::launch().await;
    let mut worker = system.spawn_scripted_worker("w-1", SCRIPT).await;
    let (session_id, root) = system.placed_session("e2e logs").await;
    system.cli_json(&["send", &root, "go"]).await;
    let events = system
        .sync_until(&session_id, "the turn that read its own logs", |events| {
            outputs(events).iter().any(|text| text == "looked")
        })
        .await;

    // The agent read its own stderr through `horde/logs`: the record came
    // back redacted, and the secret is nowhere in the session log.
    let replay = serde_json::to_string(&events).expect("events serialize");
    assert!(
        replay.contains("[REDACTED:join-token]"),
        "the agent's logs call returned its stderr, redacted: {replay}"
    );
    assert!(
        !replay.contains("s3cr3t"),
        "the secret never entered the session log"
    );

    // `horde logs agent` reads the same stderr through the server.
    let stderr = system
        .cli(&["logs", "agent", &root, "--source", "stderr"])
        .await;
    assert!(
        stderr.status.success(),
        "{}",
        String::from_utf8_lossy(&stderr.stderr)
    );
    let lines = json_lines(&stderr);
    assert!(
        lines.iter().any(
            |record| record["line"] == "harness warning [REDACTED:join-token]"
                && record["agent"] == root.as_str()
                && record["stream"] == "stderr"
        ),
        "{lines:?}"
    );
    assert!(!String::from_utf8_lossy(&stderr.stdout).contains("s3cr3t"));

    // The worker's own log, through the server: every record is one JSON
    // object stamped with the worker's boot.
    let worker_log = system.cli(&["logs", "worker", "w-1", "-n", "50"]).await;
    assert!(
        worker_log.status.success(),
        "{}",
        String::from_utf8_lossy(&worker_log.stderr)
    );
    let records = json_lines(&worker_log);
    assert!(!records.is_empty(), "the worker keeps a log");
    assert!(
        records.iter().all(|record| record["boot"].is_string()),
        "{records:?}"
    );
    let grepped = json_lines(
        &system
            .cli(&["logs", "worker", "w-1", "--grep", "^log\\.opened$"])
            .await,
    );
    assert!(
        grepped
            .iter()
            .any(|record| record["fields"]["service"] == "horde-worker"),
        "the file's first record names its service: {grepped:?}"
    );

    // The server's own log reads too.
    let server_log = system.cli(&["logs", "server", "-n", "20"]).await;
    assert!(
        server_log.status.success(),
        "{}",
        String::from_utf8_lossy(&server_log.stderr)
    );
    assert!(
        !json_lines(&server_log).is_empty(),
        "the server keeps a log"
    );

    // `--follow` sees a record written after the stream opened.
    let follow = system.spawn_cli(
        "horde logs --follow",
        &["logs", "agent", &root, "--source", "stderr", "--follow"],
    );
    follow
        .await_stdout("the existing stderr line", |out| {
            out.contains("harness warning")
        })
        .await
        .expect("the follow prints what exists first");
    system.cli_json(&["send", &root, "again please"]).await;
    follow
        .await_stdout("the new stderr line", |out| {
            out.contains("second-stderr-line")
        })
        .await
        .expect("the follow sees a record written after it opened");

    // `--dir` reads the worker's files on its own machine, unredacted.
    let agents = system.state.path().join("w-1").join("logs").join("agents");
    let offline = system
        .cli(&[
            "logs",
            "--dir",
            &agents.to_string_lossy(),
            "--grep",
            "harness",
        ])
        .await;
    assert!(
        offline.status.success(),
        "{}",
        String::from_utf8_lossy(&offline.stderr)
    );
    assert!(
        String::from_utf8_lossy(&offline.stdout).contains("horde://join/h:9/s3cr3t.fp"),
        "the operator's own files are shown as written"
    );

    drop(follow);
    worker.terminate().await.expect("SIGTERM sends");
    let _ = worker.wait().await;
}
