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

use horde_adapter::{Adapter, AgentEvent, AgentSpec, TurnInput, TurnStatus};
use horde_worker::{ClaudeAdapter, CodexAdapter};

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

    let mut streamed = String::new();
    let mut outcome = None;
    while let Ok(Some(event)) = session.next_event().await {
        match event {
            AgentEvent::Output { text, .. } => streamed.push_str(&text),
            AgentEvent::TurnEnded(ended) => {
                outcome = Some(ended);
                break;
            }
            _ => {}
        }
    }

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
    let scratch = tempfile::tempdir().expect("a scratch cwd");
    let spec = AgentSpec {
        cwd: Some(scratch.path().to_path_buf()),
        ..AgentSpec::default()
    };

    let mut session = adapter.spawn(&spec).await.expect("codex must be on PATH");
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

    let mut streamed = String::new();
    let mut diagnostics = String::new();
    let mut outcome = None;
    while let Ok(Some(event)) = session.next_event().await {
        match event {
            AgentEvent::Output {
                channel: horde_adapter::OutputChannel::HarnessDiagnostic,
                text,
                ..
            } => diagnostics.push_str(&text),
            AgentEvent::Output { text, .. } => streamed.push_str(&text),
            AgentEvent::TurnEnded(ended) => {
                outcome = Some(ended);
                break;
            }
            _ => {}
        }
    }

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
