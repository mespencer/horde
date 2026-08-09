//! Drives the **real** `claude` binary through the codec. Manual only.
//!
//! Everything else in this crate is proven against recorded bytes, which is how
//! it should be: the automated suite must never spend tokens, need a login, or
//! depend on a machine's CLI version. But recordings can only ever prove the
//! codec matches `claude` as it was on 2026-08-09. This test is what catches
//! the protocol moving.
//!
//! ```sh
//! cargo test -p horde-adapter-claude --test live_smoke -- --ignored --nocapture
//! ```
//!
//! It costs a few hundred haiku tokens. Run it when bumping the supported CLI
//! version, or when a transcript-backed test starts disagreeing with reality.
//! If it fails, capture a fresh transcript into `docs/spikes/transcripts/` —
//! the conformance suite picks up new files automatically — rather than
//! loosening the parser to accept both shapes.

use std::process::Stdio;

use horde_adapter::{AgentEvent, TurnStatus};
use horde_adapter_claude::codec::{decode_line, encode_user_message};
use horde_adapter_claude::turn::TurnAssembler;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// The flags spike W1c established as the minimum for a usable stream:
/// `--verbose` is mandatory alongside `-p`, and the setting/MCP suppression
/// keeps an operator's hooks and servers out of the protocol.
fn claude_command() -> Command {
    let mut command = Command::new("claude");
    command
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            "haiku",
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--tools",
            "",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
}

#[tokio::test]
#[ignore = "spawns the real claude binary and spends tokens; run manually"]
async fn the_codec_still_understands_a_live_claude() {
    let mut child = claude_command()
        .spawn()
        .expect("the `claude` binary must be on PATH for this manual test");

    let mut stdin = child.stdin.take().expect("stdin was piped");
    let stdout = child.stdout.take().expect("stdout was piped");

    let message = encode_user_message("Reply with exactly: HORDE_LIVE_OK");
    stdin
        .write_all(format!("{message}\n").as_bytes())
        .await
        .expect("writing the user message must succeed");
    stdin.flush().await.expect("flush must succeed");

    let mut assembler = TurnAssembler::new();
    let mut lines = BufReader::new(stdout).lines();
    let mut outcome = None;
    let mut unmodelled = Vec::new();

    while let Some(line) = lines
        .next_line()
        .await
        .expect("reading stdout must succeed")
    {
        if line.trim().is_empty() {
            continue;
        }
        let envelope = decode_line(&line)
            .unwrap_or_else(|error| panic!("the live protocol no longer decodes: {error}"));

        if let horde_adapter_claude::wire::Event::Unknown { event_type } = &envelope.event
            && !unmodelled.contains(event_type)
        {
            unmodelled.push(event_type.clone());
        }

        for event in assembler.feed(&envelope) {
            if let AgentEvent::TurnEnded(ended) = event {
                outcome = Some(ended);
            }
        }
        if outcome.is_some() {
            break;
        }
    }

    drop(stdin);
    let _ = child.wait().await;

    let outcome = outcome.expect("a live turn must close with a TurnEnded");

    assert_eq!(
        outcome.status,
        TurnStatus::Completed,
        "live turn did not complete: {outcome:?}"
    );
    assert_eq!(
        outcome.text.as_deref().map(str::trim),
        Some("HORDE_LIVE_OK")
    );
    assert!(
        !outcome.usage.tokens.is_zero(),
        "a live turn must report usage; zero means the accounting fields moved"
    );
    assert!(
        assembler.provider_session_id().is_some(),
        "a live turn must name its session, or the agent could not be revived"
    );

    // Not a failure — new event types are tolerated by design — but worth
    // seeing, because it is the earliest signal the protocol has grown.
    if !unmodelled.is_empty() {
        println!("note: unmodelled event types from this CLI: {unmodelled:?}");
    }
}
