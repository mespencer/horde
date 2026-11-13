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
//!
//! The adapters run under a Worker-owned [`HarnessProfile`] exactly as in
//! production (ADR-0010), so the profile's config directories must be signed
//! in first — the operator's own login is deliberately not inherited:
//!
//! ```sh
//! export HORDE_LIVE_HARNESS_ROOT=$HOME/.horde-live-harness
//! CLAUDE_CONFIG_DIR=$HORDE_LIVE_HARNESS_ROOT/claude/default claude login
//! CODEX_HOME=$HORDE_LIVE_HARNESS_ROOT/codex/default codex login
//! ```

use horde_adapter::{
    Adapter, AgentEvent, AgentSession, AgentSpec, PermissionDecision, TurnInput, TurnStatus,
};
use horde_worker::{ClaudeAdapter, CodexAdapter, HarnessProfile};

/// The Worker-owned profile one provider's live smoke runs under: the
/// production launch shape, rooted where the operator signed it in.
fn live_profile(provider: &str) -> HarnessProfile {
    let root = std::env::var_os("HORDE_LIVE_HARNESS_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("horde-live-harness"));
    HarnessProfile::new(root.join(provider))
}

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
async fn the_real_claude_still_streams_a_turn_through_the_supervisor() {
    let adapter = ClaudeAdapter::new(live_profile("claude"));
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

/// The stdio permission route through the production session (W12c): the
/// real claude asks on stdout, the session surfaces a permission event, the
/// narrowed allow goes back through `answer_permission`, and the harness
/// writes what Horde said.
#[tokio::test]
#[ignore = "spends real tokens and needs a claude login"]
async fn the_real_claude_asks_on_stdio_and_runs_the_narrowed_call() {
    let mut adapter = ClaudeAdapter::new(live_profile("claude")).probe_version().await;
    eprintln!("probed {}", adapter.version.describe());
    assert!(!adapter.version.is_refused(), "{}", adapter.version.describe());
    // A chain is wired by the prompt tool alone here: no endpoint, so no
    // MCP config, but the stdio route and the settings hardening apply.
    adapter.permission_prompt_tool = Some("stdio".to_owned());

    let scratch = tempfile::tempdir().expect("a scratch cwd");
    let spec = AgentSpec {
        model: Some("haiku".to_owned()),
        cwd: Some(scratch.path().to_path_buf()),
        ..AgentSpec::default()
    };
    let mut session = adapter.spawn(&spec).await.expect("claude must be on PATH");
    session
        .send(TurnInput::queued(
            "Use the Write tool to create the file probe.txt containing exactly: ORIGINAL",
        ))
        .await
        .expect("sends");

    let mut asked = 0;
    let outcome = loop {
        match session.next_event().await {
            Ok(Some(AgentEvent::PermissionRequested { request, .. })) => {
                asked += 1;
                let mut narrowed = request.input.clone();
                narrowed["content"] = serde_json::json!("NARROWED_BY_HORDE");
                session
                    .answer_permission(&request.tool_call_id, PermissionDecision::allow_with(narrowed))
                    .await
                    .expect("the prompt is answerable");
            }
            Ok(Some(AgentEvent::TurnEnded(ended))) => break ended,
            Ok(Some(_)) => {}
            other => panic!("the turn ended without a TurnEnded: {other:?}"),
        }
    };
    assert!(outcome.status.is_completed(), "{outcome:?}");
    assert!(asked >= 1, "the Write must have been asked about");
    let written =
        std::fs::read_to_string(scratch.path().join("probe.txt")).expect("the write ran");
    assert_eq!(written.trim(), "NARROWED_BY_HORDE");
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
    let adapter = CodexAdapter::new(live_profile("codex")).probe_version().await;
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
    let adapter = CodexAdapter::new(live_profile("codex")).probe_version().await;
    if !adapter
        .version
        .at_least(horde_adapter_codex::version::FIRST_EXEC_ROUTING)
    {
        eprintln!("SKIP: this codex does not route exec approvals; nothing to smoke");
        return;
    }
    eprintln!("probed {}", adapter.version.describe());

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
