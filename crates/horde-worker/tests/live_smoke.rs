//! Drives the **real** provider binaries through the supervisor. Manual only.
//!
//! Every other test in this crate runs against the stub harnesses, which is
//! how it should be: the automated suite must never spend tokens, need a
//! login, or depend on a machine's CLI version. But the stubs only prove the
//! supervisor matches what the transcripts recorded. This proves it still
//! matches the binaries.
//!
//! ```sh
//! cargo test -p horde-worker --test live_smoke -- --ignored --nocapture
//! ```
//!
//! It costs a few hundred tokens per provider. Run it when bumping a supported
//! CLI version, or when a stub-backed test starts disagreeing with reality.

use horde_adapter::{
    Adapter, AgentEvent, AgentSession, AgentSpec, PermissionDecision, TurnInput, TurnStatus,
};
use horde_worker::{ClaudeAdapter, CodexAdapter, ExecInterception};

/// A codex session rooted in its own scratch cwd — the shape every codex
/// live test shares, so the sandbox tier judges a throwaway directory.
async fn codex_session_in_scratch(
    adapter: &CodexAdapter,
) -> Result<(tempfile::TempDir, Box<dyn AgentSession>), String> {
    let scratch = tempfile::tempdir().map_err(|error| format!("no scratch cwd: {error}"))?;
    let spec = AgentSpec {
        cwd: Some(scratch.path().to_path_buf()),
        ..AgentSpec::default()
    };
    let session = adapter
        .spawn(&spec)
        .await
        .map_err(|error| format!("codex must be on PATH: {error}"))?;
    Ok((scratch, session))
}

/// Drains one turn: everything streamed (assistant and diagnostics apart),
/// and the outcome that closed it.
async fn stream_turn(
    session: &mut Box<dyn AgentSession>,
) -> (String, String, Option<horde_adapter::TurnOutcome>) {
    let mut streamed = String::new();
    let mut diagnostics = String::new();
    while let Ok(Some(event)) = session.next_event().await {
        match event {
            AgentEvent::Output {
                channel: horde_adapter::OutputChannel::HarnessDiagnostic,
                text,
                ..
            } => diagnostics.push_str(&text),
            AgentEvent::Output { text, .. } => streamed.push_str(&text),
            AgentEvent::TurnEnded(ended) => return (streamed, diagnostics, Some(ended)),
            _ => {}
        }
    }
    (streamed, diagnostics, None)
}

#[tokio::test]
#[ignore = "spends real tokens and needs a claude login"]
async fn the_real_binary_still_streams_a_turn_through_the_supervisor() {
    let adapter = ClaudeAdapter::new();
    let spec = AgentSpec {
        model: Some("haiku".to_owned()),
        append_system_prompt: Some("Answer with exactly the token asked for.".to_owned()),
        ..AgentSpec::default()
    };

    let mut session = adapter.spawn(&spec).await.expect("claude must be on PATH");
    session
        .send(TurnInput::queued(
            "Reply with exactly the token HORDE_WORKER_OK and nothing else.",
        ))
        .await
        .expect("sends");

    let (_streamed, _diagnostics, outcome) = stream_turn(&mut session).await;

    let outcome = outcome.expect("the turn must end with a TurnEnded");
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert!(
        session.provider_session_id().is_some(),
        "the revival identifier must arrive, or the agent cannot be revived"
    );
    assert!(
        outcome.usage.tokens.total() > 0,
        "a real turn must report usage — if this is zero the result shape moved"
    );

    session.shutdown().await.expect("shutdown kills by handle");
}

/// The full production codex path — spawn, handshake, sandbox tier, one
/// streamed turn — against the real `codex app-server`.
///
/// Also the natural place to verify the tier-choice diagnostic reaches the
/// event stream on a real process, since it rides the first turn.
#[tokio::test]
#[ignore = "spends real subscription quota and needs a ChatGPT login"]
async fn the_real_codex_app_server_still_streams_a_turn_through_the_adapter() {
    let adapter = CodexAdapter::new();
    let (_scratch, mut session) = codex_session_in_scratch(&adapter)
        .await
        .expect("a codex session in a scratch cwd");
    assert!(
        session.provider_session_id().is_some(),
        "the thread id arrives at spawn, before any turn"
    );

    session
        .send(TurnInput::queued(
            "Reply with exactly the token HORDE_CODEX_LIVE and nothing else.",
        ))
        .await
        .expect("sends");

    let (streamed, diagnostics, outcome) = stream_turn(&mut session).await;

    let outcome = outcome.expect("the turn must end with a TurnEnded");
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert!(
        streamed.contains("HORDE_CODEX_LIVE"),
        "deltas must reassemble into the answer; got {streamed:?}"
    );
    assert!(
        diagnostics.contains("read-only"),
        "the tier choice must be journaled on the first turn; got {diagnostics:?}"
    );
    assert!(
        outcome.usage.tokens.total() > 0,
        "a real turn must report usage — if this is zero the usage frame moved"
    );

    session.shutdown().await.expect("shutdown kills by handle");
}

/// The exec-approval round trip against the real codex: an in-cwd write under
/// the read-only tier forces `item/commandExecution/requestApproval` (re-probe
/// transcripts 25/33), the request surfaces through the production session as
/// an `exec` proposal, the decline blocks the write, and — on a second agent —
/// the accept lets it run.
///
/// Requires codex-cli 0.147+; on an older CLI the probe claims the floor and
/// this test skips itself rather than park on a request that will never come.
#[tokio::test]
#[ignore = "spends real subscription quota and needs a ChatGPT login"]
async fn the_real_codex_routes_an_exec_approval_and_honors_both_verdicts() {
    let adapter = CodexAdapter::new().probe_exec_interception().await;
    let ExecInterception::Routed { version } = adapter.exec_interception.clone() else {
        eprintln!("SKIP: this codex does not route exec approvals; nothing to smoke");
        return;
    };
    eprintln!("probed {version}");

    // One agent per verdict: an accept after a decline in the same thread
    // would be measuring codex's retry behavior, not the decision encoding.
    for (decision, expect_file) in [
        (PermissionDecision::deny("live smoke decline"), false),
        (PermissionDecision::allow(), true),
    ] {
        let (scratch, mut session) = codex_session_in_scratch(&adapter)
            .await
            .expect("a codex session in a scratch cwd");
        session
            .send(TurnInput::queued(
                "Create a file named horde-live-approval.txt in the current directory \
                 containing the word LIVE. Use a shell command.",
            ))
            .await
            .expect("sends");

        // Drain to the approval, answer it (and any retry the model makes
        // after a decline), then run the turn out.
        let mut answered = false;
        while let Ok(Some(event)) = session.next_event().await {
            match event {
                AgentEvent::PermissionRequested { request, .. } => {
                    if !answered {
                        assert_eq!(request.tool_name, "exec");
                        assert!(
                            request.input.get("command").is_some(),
                            "the chain must see the command: {request:?}"
                        );
                    }
                    session
                        .answer_permission(&request.tool_call_id, decision.clone())
                        .await
                        .expect("the decision encodes and writes");
                    answered = true;
                }
                AgentEvent::TurnEnded(_) => break,
                _ => {}
            }
        }
        assert!(answered, "the approval must surface on a 0.147+ codex");

        let written = scratch.path().join("horde-live-approval.txt").exists();
        assert_eq!(
            written, expect_file,
            "decline blocks the write and accept runs it (transcripts 20/29)"
        );
        session.shutdown().await.expect("shutdown kills by handle");
    }
}
