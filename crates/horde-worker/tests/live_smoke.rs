//! Drives the **real** `claude` binary through the supervisor. Manual only.
//!
//! Every other test in this crate runs against the stub harness, which is how
//! it should be: the automated suite must never spend tokens, need a login, or
//! depend on a machine's CLI version. But the stub only proves the supervisor
//! matches what the transcripts recorded. This proves it still matches the
//! binary.
//!
//! ```sh
//! cargo test -p horde-worker --test live_smoke -- --ignored --nocapture
//! ```
//!
//! It costs a few hundred haiku tokens. Run it when bumping the supported CLI
//! version, or when a stub-backed test starts disagreeing with reality.

use horde_adapter::{Adapter, AgentEvent, AgentSpec, TurnInput, TurnStatus};
use horde_worker::ClaudeAdapter;

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
