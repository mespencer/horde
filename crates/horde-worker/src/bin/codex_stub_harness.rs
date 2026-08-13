//! A stand-in for `codex app-server`, for deterministic process-level tests.
//!
//! Speaks just enough of the app-server JSON-RPC protocol to drive the live
//! Codex session over real stdio: it answers `initialize` / `thread/start` /
//! `thread/resume`, and replays one scripted block of notifications per
//! `turn/start`, `turn/steer`, or `turn/interrupt`. That makes the session's
//! tests real process tests — actual pipes, actual EOF, actual interleaving —
//! without a login, a network, or a token.
//!
//! Behaviour is driven by env vars so one binary covers every scenario:
//!
//! * `HORDE_CODEX_STUB_SCRIPT` — file of stdout lines, blank-line separated
//!   blocks; each turn request emits the next block. A line starting with
//!   `!ELICIT ` sends the rest as an `mcpServer/elicitation/request` server
//!   request and **waits for the client's reply** before continuing — the
//!   block behind the marker stands in for what the harness does once
//!   answered.
//! * `HORDE_CODEX_STUB_CONTROL_LOG` — append every stdin line to this file.
//!   The client's elicitation answers and steer/interrupt requests are only
//!   observable here, which is the point: a test watching the event stream
//!   alone cannot tell a reply that was sent from one that was never written.
//! * `HORDE_CODEX_STUB_FAIL_THREAD` — answer `thread/start`/`thread/resume`
//!   with this error message instead of a thread.
//! * `HORDE_CODEX_STUB_FAIL_TURN_START` — answer `turn/start` with this error.
//! * `HORDE_CODEX_STUB_STEER=stale` — answer `turn/steer` with the
//!   turn-no-longer-active error, the race `expectedTurnId` exists to detect.

use std::io::{BufRead, Write};

fn main() -> std::process::ExitCode {
    let Some(contents) = script() else {
        return std::process::ExitCode::FAILURE;
    };
    run(&blocks(&contents))
}

/// The script this run replays, or `None` having said why on stderr.
fn script() -> Option<String> {
    let Ok(path) = std::env::var("HORDE_CODEX_STUB_SCRIPT") else {
        eprintln!("HORDE_CODEX_STUB_SCRIPT is required");
        return None;
    };
    match std::fs::read_to_string(&path) {
        Ok(contents) => Some(contents),
        Err(_) => {
            eprintln!("could not read script {path}");
            None
        }
    }
}

/// Blank-line-separated blocks, one per turn request.
fn blocks(contents: &str) -> Vec<Vec<String>> {
    contents
        .split("\n\n")
        .map(|block| {
            block
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(str::to_owned)
                .collect()
        })
        .filter(|block: &Vec<String>| !block.is_empty())
        .collect()
}

fn log_control(line: &str) {
    if let Ok(path) = std::env::var("HORDE_CODEX_STUB_CONTROL_LOG")
        && let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
    {
        let _ = writeln!(file, "{line}");
    }
}

fn emit(line: &str) {
    let mut stdout = std::io::stdout().lock();
    let _ = writeln!(stdout, "{line}");
    let _ = stdout.flush();
}

fn respond_ok(id: &serde_json::Value, result: serde_json::Value) {
    emit(&serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}).to_string());
}

fn respond_error(id: &serde_json::Value, message: &str) {
    emit(
        &serde_json::json!({
            "jsonrpc":"2.0","id":id,
            "error":{"code":-32000,"message":message}
        })
        .to_string(),
    );
}

/// Plays one block: raw lines are emitted; an `!ELICIT` marker sends a server
/// request and blocks until the client answers it.
fn play_block(block: &[String], stdin: &mut impl BufRead, next_server_id: &mut u64) {
    for line in block {
        if let Some(params) = line.strip_prefix("!ELICIT ") {
            *next_server_id += 1;
            let id = *next_server_id;
            let params: serde_json::Value =
                serde_json::from_str(params).unwrap_or(serde_json::Value::Null);
            emit(
                &serde_json::json!({
                    "jsonrpc":"2.0","id":id,
                    "method":"mcpServer/elicitation/request","params":params
                })
                .to_string(),
            );
            await_reply(id, stdin);
        } else {
            emit(line);
        }
    }
}

/// Reads stdin until the reply to `id` arrives, logging everything and
/// acknowledging any interleaved client request so nothing deadlocks.
/// Bounded by count: a client that never answers ends the stub instead of
/// wedging the test suite.
fn await_reply(id: u64, stdin: &mut impl BufRead) {
    for _ in 0..1024 {
        let Some(line) = next_line(stdin) else { return };
        log_control(&line);
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let is_reply_to_us = message.get("id").and_then(serde_json::Value::as_u64) == Some(id)
            && message.get("method").is_none();
        if is_reply_to_us {
            return;
        }
        // An interleaved client request mid-elicitation: acknowledge it so the
        // client is never left waiting on the stub.
        if let (Some(request_id), Some(_)) = (message.get("id"), message.get("method")) {
            respond_ok(request_id, serde_json::json!({}));
        }
    }
}

/// One trimmed, non-empty stdin line, or `None` at EOF.
fn next_line(stdin: &mut impl BufRead) -> Option<String> {
    loop {
        let mut line = String::new();
        match stdin.read_line(&mut line) {
            Ok(0) | Err(_) => return None,
            Ok(_) => {}
        }
        let line = line.trim();
        if !line.is_empty() {
            return Some(line.to_owned());
        }
    }
}

fn run(blocks: &[Vec<String>]) -> std::process::ExitCode {
    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    let mut remaining = blocks.iter();
    let mut next_server_id = 1000_u64;

    loop {
        let Some(line) = next_line(&mut stdin) else {
            return std::process::ExitCode::SUCCESS;
        };
        log_control(&line);
        let Ok(message) = serde_json::from_str::<serde_json::Value>(&line) else {
            continue;
        };
        let (Some(id), Some(method)) = (
            message.get("id").cloned(),
            message.get("method").and_then(serde_json::Value::as_str),
        ) else {
            // Notifications (`initialized`) and stray replies need nothing.
            continue;
        };

        match method {
            "initialize" => respond_ok(&id, serde_json::json!({"userAgent":"codex-stub"})),
            "thread/start" | "thread/resume" => {
                if let Ok(message) = std::env::var("HORDE_CODEX_STUB_FAIL_THREAD") {
                    respond_error(&id, &message);
                    continue;
                }
                let thread_id = message
                    .pointer("/params/threadId")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("stub-thread");
                respond_ok(
                    &id,
                    serde_json::json!({"thread":{"id":thread_id},"model":"stub-model"}),
                );
            }
            "turn/start" => {
                if let Ok(message) = std::env::var("HORDE_CODEX_STUB_FAIL_TURN_START") {
                    respond_error(&id, &message);
                    continue;
                }
                respond_ok(&id, serde_json::json!({"turn":{"id":"turn-accepted"}}));
                if let Some(block) = remaining.next() {
                    play_block(block, &mut stdin, &mut next_server_id);
                }
            }
            "turn/steer" => {
                if std::env::var("HORDE_CODEX_STUB_STEER").as_deref() == Ok("stale") {
                    respond_error(&id, "expectedTurnId does not match the active turn");
                    continue;
                }
                respond_ok(&id, serde_json::json!({"turnId":"turn-steered"}));
                if let Some(block) = remaining.next() {
                    play_block(block, &mut stdin, &mut next_server_id);
                }
            }
            "turn/interrupt" => {
                respond_ok(&id, serde_json::json!({}));
                if let Some(block) = remaining.next() {
                    play_block(block, &mut stdin, &mut next_server_id);
                }
            }
            _ => respond_ok(&id, serde_json::json!({})),
        }
    }
}
