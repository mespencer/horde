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
//!   answered. `!EXEC_APPROVAL ` does the same on
//!   `item/commandExecution/requestApproval`, the exec seam the 2026-08-19
//!   re-probe measured. A `!CLOSE_STDOUT` line closes the stub's stdout and
//!   leaves stdin open (still read, still logged): the client sees the
//!   harness end while its writes would still succeed.
//! * `codex-stub-harness --version` prints `HORDE_CODEX_STUB_VERSION` (default
//!   `codex-cli 0.148.0`) and exits — the version probe's happy path, without
//!   a script.
//! * `HORDE_CODEX_STUB_CONTROL_LOG` — append every stdin line to this file.
//!   The client's elicitation answers and steer/interrupt requests are only
//!   observable here, which is the point: a test watching the event stream
//!   alone cannot tell a reply that was sent from one that was never written.
//! * `HORDE_CODEX_STUB_FAIL_THREAD` — answer `thread/start`/`thread/resume`
//!   with this error message instead of a thread.
//! * `HORDE_CODEX_STUB_FAIL_TURN_START` — answer `turn/start` with this error.
//! * `HORDE_CODEX_STUB_STEER=stale` — answer `turn/steer` with the
//!   turn-no-longer-active error, the race `expectedTurnId` exists to detect.
//! * `HORDE_CODEX_STUB_MUTE=1` — read (and log) everything, answer nothing:
//!   the wedged harness whose silence the adapter's response ceiling exists
//!   to bound.
//! * `HORDE_CODEX_STUB_DIE` — read the first request, then exit nonzero.
//!   `eof` leaves it unanswered, so the client's read sees EOF; `hangup`
//!   closes stdin first and then answers it, so the client's next write is
//!   guaranteed EPIPE. The two orders a mid-handshake death can take, each
//!   forced rather than left to the scheduler. No script needed.
//!
//! The decision logic is pure and unit-tested in this file; only `main`,
//! `run`, and the two byte-shovels touch real stdio.

use std::io::{BufRead, Write};

/// The script line that hangs up the stub's stdout mid-block.
const CLOSE_STDOUT: &str = "!CLOSE_STDOUT";

fn main() -> std::process::ExitCode {
    if std::env::args().any(|argument| argument == "--version") {
        let version = std::env::var("HORDE_CODEX_STUB_VERSION")
            .unwrap_or_else(|_| "codex-cli 0.148.0".to_owned());
        println!("{version}");
        return std::process::ExitCode::SUCCESS;
    }
    if let Ok(death) = std::env::var("HORDE_CODEX_STUB_DIE") {
        return die_mid_handshake(death == "hangup");
    }
    let Some(contents) = script() else {
        return std::process::ExitCode::FAILURE;
    };
    run(&blocks(&contents), &StubConfig::from_env())
}

/// Dies after the client's first request, in the order a test asked for.
///
/// Reading that request first is what makes the order certain: the client's
/// write has landed before anything closes. Then either the stub exits with
/// the request unanswered (the client's next read sees EOF), or it closes
/// stdin *before* answering — so a client that has read the answer is
/// guaranteed that its next write meets EPIPE.
fn die_mid_handshake(hang_up: bool) -> std::process::ExitCode {
    let stdin = std::io::stdin();
    let message = next_message(&mut stdin.lock());
    if hang_up {
        close_descriptor(0);
        if let Some(reply) = message.and_then(|m| reply_for(&m, &StubConfig::default())) {
            emit(&reply.line);
        }
    }
    eprintln!("the stub died mid-handshake, as HORDE_CODEX_STUB_DIE told it to");
    std::process::ExitCode::FAILURE
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

/// The scenario switches, read from the environment once.
#[derive(Debug, Default)]
struct StubConfig {
    fail_thread: Option<String>,
    fail_turn_start: Option<String>,
    steer_stale: bool,
    mute: bool,
}

impl StubConfig {
    fn from_env() -> Self {
        Self {
            fail_thread: std::env::var("HORDE_CODEX_STUB_FAIL_THREAD").ok(),
            fail_turn_start: std::env::var("HORDE_CODEX_STUB_FAIL_TURN_START").ok(),
            steer_stale: std::env::var("HORDE_CODEX_STUB_STEER").as_deref() == Ok("stale"),
            mute: std::env::var("HORDE_CODEX_STUB_MUTE").is_ok(),
        }
    }
}

/// What one client request gets back.
#[derive(Debug, PartialEq)]
struct Reply {
    /// The JSON-RPC reply line to emit.
    line: String,
    /// Whether the next scripted block plays after the reply.
    plays_block: bool,
}

fn ok_line(id: &serde_json::Value, result: serde_json::Value) -> String {
    serde_json::json!({"jsonrpc":"2.0","id":id,"result":result}).to_string()
}

fn error_line(id: &serde_json::Value, message: &str) -> String {
    serde_json::json!({
        "jsonrpc":"2.0","id":id,
        "error":{"code":-32000,"message":message}
    })
    .to_string()
}

/// The whole request->reply decision, pure so its truth table is testable.
fn reply_for(message: &serde_json::Value, config: &StubConfig) -> Option<Reply> {
    if config.mute {
        return None;
    }
    let id = message.get("id")?.clone();
    let method = message.get("method")?.as_str()?;

    let reply = match method {
        "initialize" => Reply {
            line: ok_line(&id, serde_json::json!({"userAgent":"codex-stub"})),
            plays_block: false,
        },
        "thread/start" | "thread/resume" => thread_reply(&id, message, config),
        "turn/start" => match &config.fail_turn_start {
            Some(refusal) => Reply {
                line: error_line(&id, refusal),
                plays_block: false,
            },
            None => Reply {
                line: ok_line(&id, serde_json::json!({"turn":{"id":"turn-accepted"}})),
                plays_block: true,
            },
        },
        "turn/steer" if config.steer_stale => Reply {
            line: error_line(&id, "expectedTurnId does not match the active turn"),
            plays_block: false,
        },
        "turn/steer" => Reply {
            line: ok_line(&id, serde_json::json!({"turnId":"turn-steered"})),
            plays_block: true,
        },
        "turn/interrupt" => Reply {
            line: ok_line(&id, serde_json::json!({})),
            plays_block: true,
        },
        _ => Reply {
            line: ok_line(&id, serde_json::json!({})),
            plays_block: false,
        },
    };
    Some(reply)
}

/// `thread/start` and `thread/resume` share a reply: the resumed thread keeps
/// the id the client asked for, so revival identity is observable end to end.
fn thread_reply(id: &serde_json::Value, message: &serde_json::Value, config: &StubConfig) -> Reply {
    if let Some(refusal) = &config.fail_thread {
        return Reply {
            line: error_line(id, refusal),
            plays_block: false,
        };
    }
    let thread_id = message
        .pointer("/params/threadId")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("stub-thread");
    Reply {
        line: ok_line(
            id,
            serde_json::json!({"thread":{"id":thread_id},"model":"stub-model"}),
        ),
        plays_block: false,
    }
}

/// Whether `message` is the reply to server request `id` (as opposed to a new
/// client request or something unparseable).
fn is_reply_to(message: &serde_json::Value, id: u64) -> bool {
    message.get("id").and_then(serde_json::Value::as_u64) == Some(id)
        && message.get("method").is_none()
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

/// One trimmed, non-empty, logged stdin line as JSON, or `None` at EOF.
/// Unparseable lines are skipped — they were logged, which is all a test
/// needs from them.
fn next_message(stdin: &mut impl BufRead) -> Option<serde_json::Value> {
    loop {
        let mut line = String::new();
        let read = stdin.read_line(&mut line).ok()?;
        if read == 0 {
            return None;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        log_control(line);
        if let Ok(message) = serde_json::from_str::<serde_json::Value>(line) {
            return Some(message);
        }
    }
}

/// Plays one block: raw lines are emitted; an `!ELICIT` or `!EXEC_APPROVAL`
/// marker sends a server request on its method and blocks until the client
/// answers it; `!CLOSE_STDOUT` hangs up the output alone.
fn play_block(block: &[String], stdin: &mut impl BufRead, next_server_id: &mut u64) {
    for line in block {
        if line == CLOSE_STDOUT {
            // The client's read meets EOF while its writes still land: the
            // only state in which a session can tell "ended" from "unwritable".
            let _ = std::io::stdout().flush();
            close_descriptor(1);
            continue;
        }
        match server_request_marker(line) {
            Some((method, params)) => {
                *next_server_id += 1;
                emit(&server_request_line(*next_server_id, method, params));
                await_reply(*next_server_id, stdin);
            }
            None => emit(line),
        }
    }
}

/// The method a marker line asks for, with its params — or `None` for a raw
/// line.
fn server_request_marker(line: &str) -> Option<(&'static str, &str)> {
    if let Some(params) = line.strip_prefix("!ELICIT ") {
        return Some(("mcpServer/elicitation/request", params));
    }
    line.strip_prefix("!EXEC_APPROVAL ")
        .map(|params| ("item/commandExecution/requestApproval", params))
}

fn server_request_line(id: u64, method: &str, params: &str) -> String {
    let params: serde_json::Value = serde_json::from_str(params).unwrap_or(serde_json::Value::Null);
    serde_json::json!({
        "jsonrpc":"2.0","id":id,
        "method":method,"params":params
    })
    .to_string()
}

/// Reads stdin until the reply to `id` arrives, acknowledging any interleaved
/// client request so nothing deadlocks. Bounded by count: a client that never
/// answers ends the stub instead of wedging the test suite.
fn await_reply(id: u64, stdin: &mut impl BufRead) {
    for _ in 0..1024 {
        let Some(message) = next_message(stdin) else {
            return;
        };
        if is_reply_to(&message, id) {
            return;
        }
        acknowledge(&message);
    }
}

/// Acknowledges an interleaved client request mid-elicitation, so the client
/// is never left waiting on the stub.
fn acknowledge(message: &serde_json::Value) {
    if let (Some(request_id), Some(_)) = (message.get("id"), message.get("method")) {
        emit(&ok_line(request_id, serde_json::json!({})));
    }
}

/// Answers one client message: emit the reply, play the next block when the
/// reply calls for one. Notifications and stray replies need nothing.
fn handle<'b>(
    message: &serde_json::Value,
    config: &StubConfig,
    remaining: &mut impl Iterator<Item = &'b Vec<String>>,
    stdin: &mut impl BufRead,
    next_server_id: &mut u64,
) {
    let Some(reply) = reply_for(message, config) else {
        return;
    };
    emit(&reply.line);
    if reply.plays_block
        && let Some(block) = remaining.next()
    {
        play_block(block, stdin, next_server_id);
    }
}

fn run(blocks: &[Vec<String>], config: &StubConfig) -> std::process::ExitCode {
    let stdin = std::io::stdin();
    let mut stdin = stdin.lock();
    let mut remaining = blocks.iter();
    let mut next_server_id = 1000_u64;

    while let Some(message) = next_message(&mut stdin) {
        handle(
            &message,
            config,
            &mut remaining,
            &mut stdin,
            &mut next_server_id,
        );
    }
    std::process::ExitCode::SUCCESS
}

/// Closes a standard descriptor — the hang-ups the supervision suites stage.
/// Unix only: Windows has no safe way to close a standard handle, and the
/// suites that stage hang-ups run on Unix alone (ADR-0017).
fn close_descriptor(descriptor: i32) {
    #[cfg(unix)]
    {
        let _ = nix::unistd::close(descriptor);
    }
    #[cfg(not(unix))]
    {
        let _ = descriptor;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{StubConfig, blocks, handle, is_reply_to, next_message, reply_for};

    fn request(method: &str) -> serde_json::Value {
        json!({"jsonrpc":"2.0","id":1,"method":method,"params":{}})
    }

    /// The dispatch truth table: which method gets which reply shape, and
    /// which replies pull the next scripted block onto the wire.
    #[test]
    fn each_method_gets_its_reply_and_only_turn_requests_play_blocks() {
        let config = StubConfig::default();

        for (method, plays) in [
            ("initialize", false),
            ("thread/start", false),
            ("thread/resume", false),
            ("turn/start", true),
            ("turn/steer", true),
            ("turn/interrupt", true),
            ("account/rateLimits/read", false),
        ] {
            let reply = reply_for(&request(method), &config)
                .unwrap_or_else(|| panic!("{method} must be answered"));
            assert_eq!(reply.plays_block, plays, "{method}");
            assert!(
                reply.line.contains("\"result\""),
                "{method}: {}",
                reply.line
            );
        }

        assert_eq!(
            reply_for(&json!({"jsonrpc":"2.0","method":"initialized"}), &config),
            None,
            "a notification needs no reply"
        );
        let mute = StubConfig {
            mute: true,
            ..StubConfig::default()
        };
        assert_eq!(
            reply_for(&request("initialize"), &mute),
            None,
            "a muted stub answers nothing at all"
        );
        assert_eq!(
            reply_for(&json!({"jsonrpc":"2.0","id":7,"result":{}}), &config),
            None,
            "a stray reply needs no reply either"
        );
    }

    /// The failure switches: a configured refusal answers with an error and
    /// never plays a block.
    #[test]
    fn configured_refusals_answer_with_errors() {
        let failing_thread = StubConfig {
            fail_thread: Some("not logged in".to_owned()),
            ..StubConfig::default()
        };
        let refused = reply_for(&request("thread/start"), &failing_thread)
            .unwrap_or_else(|| panic!("answered"));
        assert!(refused.line.contains("not logged in") && refused.line.contains("error"));
        assert!(!refused.plays_block);

        let failing_turn = StubConfig {
            fail_turn_start: Some("archived".to_owned()),
            ..StubConfig::default()
        };
        let refused =
            reply_for(&request("turn/start"), &failing_turn).unwrap_or_else(|| panic!("answered"));
        assert!(refused.line.contains("archived"));
        assert!(!refused.plays_block);

        let stale = StubConfig {
            steer_stale: true,
            ..StubConfig::default()
        };
        let refused =
            reply_for(&request("turn/steer"), &stale).unwrap_or_else(|| panic!("answered"));
        assert!(refused.line.contains("expectedTurnId"));
        assert!(!refused.plays_block);
    }

    /// The resumed thread keeps the id the client asked for — revival
    /// identity must be observable end to end.
    #[test]
    fn a_resume_echoes_the_thread_it_was_asked_to_revive() {
        let resume = json!({"jsonrpc":"2.0","id":2,"method":"thread/resume",
                             "params":{"threadId":"t-revived"}});
        let reply =
            reply_for(&resume, &StubConfig::default()).unwrap_or_else(|| panic!("answered"));
        assert!(reply.line.contains("t-revived"), "{}", reply.line);
    }

    /// Reply detection: same id without a method is the answer; anything with
    /// a method is a new request, whatever its id.
    #[test]
    fn a_reply_is_the_matching_id_without_a_method() {
        assert!(is_reply_to(&json!({"id":5,"result":{}}), 5));
        assert!(!is_reply_to(&json!({"id":6,"result":{}}), 5));
        assert!(!is_reply_to(&json!({"id":5,"method":"turn/interrupt"}), 5));
        assert!(!is_reply_to(&json!({"result":{}}), 5));
    }

    /// The byte shovel: skips blanks and garbage, returns parsed JSON, `None`
    /// at EOF.
    #[test]
    fn next_message_skips_noise_and_ends_at_eof() {
        let mut input = std::io::Cursor::new("\n\nnot json\n{\"id\":1}\n");
        let message = next_message(&mut input).unwrap_or_else(|| panic!("a message"));
        assert_eq!(message, json!({"id":1}));
        assert_eq!(next_message(&mut input), None, "EOF ends the stub");
    }

    /// The whole handle path with a scripted block: the reply plays the
    /// block, an `!ELICIT` marker inside it waits for (and gets) its answer,
    /// and an interleaved request is acknowledged rather than deadlocked on.
    #[test]
    fn handle_plays_a_block_and_an_elicitation_waits_for_its_answer() {
        let blocks = [vec![
            "{\"method\":\"turn/started\"}".to_owned(),
            "!ELICIT {\"serverName\":\"spike\"}".to_owned(),
            "{\"method\":\"turn/completed\"}".to_owned(),
        ]];
        // The client answers the elicitation (id 1001) after one interleaved
        // request the stub must acknowledge instead of hanging on.
        let mut client = std::io::Cursor::new(
            "{\"id\":42,\"method\":\"turn/interrupt\"}\n{\"id\":1001,\"result\":{\"action\":\"accept\"}}\n",
        );
        let mut remaining = blocks.iter();
        let mut next_server_id = 1000_u64;

        handle(
            &request("turn/start"),
            &StubConfig::default(),
            &mut remaining,
            &mut client,
            &mut next_server_id,
        );

        assert_eq!(next_server_id, 1001, "the elicitation minted a server id");
        assert!(
            remaining.next().is_none(),
            "the turn request consumed its block"
        );
        assert_eq!(
            client.position() as usize,
            client.get_ref().len(),
            "the elicitation answer (and the interleaved request before it) \
             were both consumed"
        );
    }

    /// A notification consumes no block — the guard the plays_block flag and
    /// the None reply protect together.
    #[test]
    fn handle_ignores_notifications_without_consuming_blocks() {
        let blocks = [vec!["never emitted".to_owned()]];
        let mut client = std::io::Cursor::new("");
        let mut remaining = blocks.iter();
        let mut next_server_id = 1000_u64;

        handle(
            &json!({"jsonrpc":"2.0","method":"initialized"}),
            &StubConfig::default(),
            &mut remaining,
            &mut client,
            &mut next_server_id,
        );
        assert!(remaining.next().is_some(), "the block is still unplayed");
    }

    /// The marker table: each directive names its own method, and a raw line
    /// is no directive at all.
    #[test]
    fn markers_name_their_methods_and_raw_lines_are_raw() {
        assert_eq!(
            super::server_request_marker("!ELICIT {\"serverName\":\"s\"}"),
            Some(("mcpServer/elicitation/request", "{\"serverName\":\"s\"}"))
        );
        assert_eq!(
            super::server_request_marker("!EXEC_APPROVAL {\"command\":\"true\"}"),
            Some((
                "item/commandExecution/requestApproval",
                "{\"command\":\"true\"}"
            ))
        );
        assert_eq!(
            super::server_request_marker("{\"method\":\"turn/started\"}"),
            None
        );
    }

    #[test]
    fn blocks_split_on_blank_lines_and_drop_empties() {
        let script = "a\nb\n\n\nc\n";
        assert_eq!(
            blocks(script),
            vec![vec!["a".to_owned(), "b".to_owned()], vec!["c".to_owned()]]
        );
    }
}
