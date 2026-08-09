//! Drives the **real** `codex app-server` through the codec. Manual only.
//!
//! Everything else in this crate is proven against recorded bytes, which is how
//! it should be: the automated suite must never spend a subscription's quota,
//! need a ChatGPT login, or depend on a machine's CLI version. But recordings
//! can only ever prove the codec matches codex-cli 0.145.0 as it was on
//! 2026-08-09. This test is what catches the protocol moving.
//!
//! ```sh
//! cargo test -p horde-adapter-codex --test live_smoke -- --ignored --nocapture
//! ```
//!
//! It costs a handful of tokens — one turn, a nine-character reply. Run it when
//! bumping the supported CLI version, or when a transcript-backed test starts
//! disagreeing with reality. If it fails, capture a fresh transcript into
//! `docs/spikes/codex-app-server/transcripts/` rather than loosening the parser
//! to accept both shapes.
//!
//! It deliberately does **not** re-test the three traps. Interrupt needs a long
//! turn, steer needs a mid-flight race, and resume needs a thread from a prior
//! process; all three burn real quota and are already pinned against the
//! recordings that captured them. This checks only that the protocol still
//! looks the way the codec expects.

use std::process::Stdio;

use horde_adapter::{AgentEvent, TurnStatus};
use horde_adapter_codex::codec::{decode_line, encode_turn_start};
use horde_adapter_codex::turn::TurnAssembler;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// Starts the app-server on stdio.
///
/// No feature flags and no config overrides: the point is to see what an
/// ordinary operator's Codex does, including whatever their `config.toml` says.
fn app_server() -> Command {
    let mut command = Command::new("codex");
    command
        .args(["app-server", "--stdio"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command
}

#[tokio::test]
#[ignore = "spends real subscription quota and needs a ChatGPT login"]
async fn the_real_app_server_still_speaks_what_the_codec_reads() {
    let mut child = app_server().spawn().expect("codex must be on PATH");
    let mut stdin = child.stdin.take().expect("stdin was piped");
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout was piped")).lines();

    // initialize / initialized, then a thread, then one turn.
    let handshake = serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "clientInfo": { "name": "horde-live-smoke", "version": "0.1.0" } }
    });
    stdin
        .write_all(format!("{handshake}\n").as_bytes())
        .await
        .expect("the app-server must accept a handshake");

    // Read until the handshake is answered, then announce readiness.
    while let Ok(Some(line)) = stdout.next_line().await {
        if let Ok(frame) = decode_line(&line)
            && matches!(
                frame,
                horde_adapter_codex::wire::Frame::Response { id: 1, .. }
            )
        {
            break;
        }
    }
    let initialized = serde_json::json!({ "jsonrpc": "2.0", "method": "initialized" });
    stdin
        .write_all(format!("{initialized}\n").as_bytes())
        .await
        .expect("writes");

    let start_thread = serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "thread/start",
        "params": { "approvalPolicy": "untrusted", "sandbox": "read-only" }
    });
    stdin
        .write_all(format!("{start_thread}\n").as_bytes())
        .await
        .expect("writes");

    let mut thread_id = None;
    while let Ok(Some(line)) = stdout.next_line().await {
        if let Ok(horde_adapter_codex::wire::Frame::Response { id: 2, result, .. }) =
            decode_line(&line)
        {
            thread_id = result
                .as_ref()
                .and_then(|r| r.pointer("/thread/id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            break;
        }
    }
    let thread_id = thread_id.expect("thread/start must return a thread id");

    stdin
        .write_all(
            format!(
                "{}\n",
                encode_turn_start(
                    3,
                    &thread_id,
                    "Reply with exactly the token HORDE_LIVE and nothing else."
                )
            )
            .as_bytes(),
        )
        .await
        .expect("writes");

    let mut assembler = TurnAssembler::new();
    let mut streamed = String::new();
    let mut outcome = None;

    while let Ok(Some(line)) = stdout.next_line().await {
        let Ok(frame) = decode_line(&line) else {
            continue;
        };
        for event in assembler.feed(&frame) {
            match event {
                AgentEvent::Output { text, .. } => streamed.push_str(&text),
                AgentEvent::TurnEnded(ended) => outcome = Some(ended),
                _ => {}
            }
        }
        if outcome.is_some() {
            break;
        }
    }

    let _ = child.kill().await;

    let outcome = outcome.expect("the turn must end with a TurnEnded");
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert!(
        streamed.contains("HORDE_LIVE"),
        "deltas must reassemble into the answer; got {streamed:?}"
    );
    assert!(
        outcome.usage.tokens.input_tokens > 0,
        "a real turn must report usage — if this is zero the usage frame moved"
    );
}
