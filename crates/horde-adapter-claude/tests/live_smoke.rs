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
use horde_adapter_claude::launch::IsolationFlags;
use horde_adapter_claude::turn::TurnAssembler;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;

/// The flags spike W1c established as the minimum for a usable stream:
/// `--verbose` is mandatory alongside `-p`, and the isolation flags — from
/// the same builder the Worker's production launch uses (ADR-0010) — keep an
/// operator's settings, hooks, and servers out of the protocol.
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
            "--tools",
            "",
        ])
        .args(IsolationFlags::default().argv())
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

/// Spawns the real `claude` on the stdio permission route (W12c) in `cwd`,
/// offering only `tools`.
fn stdio_claude(cwd: &std::path::Path, tools: &str) -> Command {
    let mut command = Command::new("claude");
    command
        .current_dir(cwd)
        .args([
            "-p",
            "--input-format",
            "stream-json",
            "--output-format",
            "stream-json",
            "--verbose",
            "--model",
            "haiku",
            "--tools",
            tools,
        ])
        .args(
            IsolationFlags {
                mcp_config: None,
                permission_prompt_tool: Some(horde_adapter_claude::permission::STDIO_PROMPT_TOOL),
            }
            .argv(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
}

/// One turn on the stdio route: every `can_use_tool` prompt is answered by
/// `decide` through the production encoders, and the turn's outcome and the
/// prompts seen come back.
async fn stdio_turn(
    cwd: &std::path::Path,
    tools: &str,
    prompt: &str,
    decide: impl Fn(&horde_adapter::ProposedToolCall) -> horde_adapter::PermissionDecision,
) -> (horde_adapter::TurnOutcome, Vec<horde_adapter::ProposedToolCall>) {
    use horde_adapter_claude::permission::{encode_control_response, encode_decision};
    use horde_adapter_claude::wire::{ControlRequestKind, Event};

    let mut child = stdio_claude(cwd, tools)
        .spawn()
        .expect("the `claude` binary must be on PATH for this manual test");
    let mut stdin = child.stdin.take().expect("stdin was piped");
    let stdout = child.stdout.take().expect("stdout was piped");
    stdin
        .write_all(format!("{}\n", encode_user_message(prompt)).as_bytes())
        .await
        .expect("writes");
    stdin.flush().await.expect("flushes");

    let mut assembler = TurnAssembler::new();
    let mut lines = BufReader::new(stdout).lines();
    let mut prompts = Vec::new();
    let mut outcome = None;
    while let Some(line) = lines.next_line().await.expect("reads stdout") {
        if line.trim().is_empty() {
            continue;
        }
        let envelope = decode_line(&line)
            .unwrap_or_else(|error| panic!("the live protocol no longer decodes: {error}"));
        let events = assembler.feed(&envelope);
        if let Event::ControlRequest(request) = &envelope.event
            && let ControlRequestKind::CanUseTool(can_use) = &request.request
        {
            let call = can_use.to_proposed_call();
            let body = encode_decision(&decide(&call), &can_use.tool_name, &can_use.input)
                .expect("the decision is expressible");
            stdin
                .write_all(format!("{}\n", encode_control_response(&request.request_id, &body)).as_bytes())
                .await
                .expect("answers");
            stdin.flush().await.expect("flushes");
            prompts.push(call);
        }
        for event in events {
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
    (outcome.expect("a live turn must close"), prompts)
}

/// The stdio route against the live CLI: the Write prompt arrives as a
/// `can_use_tool` control request before anything is written, and the
/// harness writes the **narrowed** content Horde answered with.
#[tokio::test]
#[ignore = "spawns the real claude binary and spends tokens; run manually"]
async fn a_live_stdio_prompt_is_answered_and_the_narrowing_runs() {
    let scratch = tempfile::tempdir().expect("a scratch cwd");
    let (outcome, prompts) = stdio_turn(
        scratch.path(),
        "Write",
        "Use the Write tool to create the file probe.txt containing exactly: ORIGINAL",
        |call| {
            let mut narrowed = call.input.clone();
            narrowed["content"] = serde_json::json!("NARROWED_BY_HORDE");
            horde_adapter::PermissionDecision::allow_with(narrowed)
        },
    )
    .await;
    assert!(outcome.status.is_completed(), "{outcome:?}");
    assert_eq!(prompts.len(), 1, "one Write, one prompt: {prompts:?}");
    assert_eq!(prompts[0].tool_name, "Write");
    let written = std::fs::read_to_string(scratch.path().join("probe.txt"))
        .expect("the allowed write ran");
    assert_eq!(written.trim(), "NARROWED_BY_HORDE");
}

/// `AskUserQuestion` against the live CLI: the question arrives as a prompt
/// carrying questions, and the answer reaches the model.
#[tokio::test]
#[ignore = "spawns the real claude binary and spends tokens; run manually"]
async fn a_live_question_is_answered_through_the_prompt() {
    let scratch = tempfile::tempdir().expect("a scratch cwd");
    let (outcome, prompts) = stdio_turn(
        scratch.path(),
        "AskUserQuestion",
        "Before doing anything, use the AskUserQuestion tool to ask me which colour I prefer, \
         offering exactly two options: Red and Blue. Then reply with just the colour I chose.",
        |call| {
            let question = call.questions.first().expect("the prompt is a question");
            horde_adapter::PermissionDecision::answer(horde_adapter::Answers::from([(
                question.id.clone(),
                vec!["Blue".to_owned()],
            )]))
        },
    )
    .await;
    assert!(outcome.status.is_completed(), "{outcome:?}");
    assert!(prompts.iter().all(horde_adapter::ProposedToolCall::is_question));
    assert!(
        outcome.text.as_deref().is_some_and(|text| text.contains("Blue")),
        "{outcome:?}"
    );
}

/// The installed CLI's version, against the measured table.
#[test]
#[ignore = "runs the real claude binary; run manually"]
fn the_installed_claude_version_is_classified() {
    let output = std::process::Command::new("claude")
        .arg("--version")
        .output()
        .expect("claude is on PATH");
    let report =
        horde_adapter_claude::version::SUPPORT.classify(&String::from_utf8_lossy(&output.stdout));
    println!("{}", report.describe());
    assert!(!report.is_refused(), "{}", report.describe());
}
