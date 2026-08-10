//! Holds the FakeAdapter to the same contract as a real one.
//!
//! docs/TESTING.md §3: the fake is only worth building orchestration tests on
//! if it behaves like a real harness. The checks in `horde_testkit::conformance`
//! that the Claude codec passes against recorded bytes are run here against the
//! script-backed backend, so the two cannot drift apart unnoticed.

use core::time::Duration;
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSpec, Capabilities, Capability, OutputChannel,
    PermissionDecision, ProviderSessionId, TokenUsage, TurnInput, TurnStatus,
};

use horde_adapter_fake::{FakeAdapter, Misbehaviour, Script, ScriptedTurn, Step};
use horde_testkit::conformance::{
    TurnRecord, check_capability_honesty, check_session, check_session_with, run_turn,
};
use serde_json::json;

/// The `#[cfg(test)]` wrapper is what tells clippy this is test code, which is
/// where `expect` is permitted (`clippy.toml`); an integration test's free
/// functions are not otherwise recognised as such. See docs/QUALITY_GATES.md.
#[cfg(test)]
mod harness {
    use horde_adapter::{Adapter, AgentSpec};

    use super::{FakeAdapter, Script, ScriptedTurn, TurnRecord, check_session};

    /// Runs one message through a scripted adapter and returns the turn records.
    ///
    /// Several tests need exactly this — script, spawn, drive the conformance
    /// suite with one message — and differ only in the script and in what they
    /// assert.
    pub(crate) async fn conforming_records(turns: Vec<ScriptedTurn>) -> Vec<TurnRecord> {
        let adapter = FakeAdapter::new(Script::of(turns));
        let mut session = adapter
            .spawn(&AgentSpec::default())
            .await
            .expect("the fake always spawns");
        check_session(session.as_mut(), &["go"])
            .await
            .expect("a scripted turn must satisfy the adapter contract")
    }
}

use harness::conforming_records;

fn asking_permission() -> Script {
    Script::of(vec![ScriptedTurn::saying("working on it").with_step(
        Step::AskPermission {
            tool_call_id: "call-1".to_owned(),
            tool_name: "Write".to_owned(),
            input: json!({"path": "/tmp/x", "content": "ORIGINAL"}),
        },
    )])
}

#[tokio::test]
async fn the_fake_satisfies_the_same_contract_as_a_real_adapter() {
    let adapter = FakeAdapter::new(Script::of(vec![
        ScriptedTurn::saying("first"),
        ScriptedTurn::saying("second"),
    ]));
    let mut session = adapter
        .spawn(&AgentSpec::default())
        .await
        .expect("spawn must succeed");

    let records = check_session(session.as_mut(), &["a", "b"])
        .await
        .expect("the fake must satisfy the adapter contract");

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].assistant_text(), "first");
    assert_eq!(records[1].assistant_text(), "second");
}

#[tokio::test]
async fn a_scripted_permission_request_blocks_the_turn_until_answered() {
    let adapter = FakeAdapter::new(asking_permission());
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    session
        .send(TurnInput::queued("go"))
        .await
        .expect("send must succeed");

    // Drain to the permission request.
    let mut request_id = None;
    while request_id.is_none() {
        let event = session
            .next_event()
            .await
            .expect("draining to the request must not fail")
            .expect("the script asks for permission before ending");
        if let AgentEvent::PermissionRequested { request, .. } = event {
            request_id = Some(request.tool_call_id);
        }
    }

    // Asking for more without answering must say so rather than hang or lie.
    let error = session
        .next_event()
        .await
        .expect_err("the turn cannot advance while a decision is outstanding");
    assert!(matches!(error, AdapterError::AwaitingDecision { .. }));

    let request_id = request_id.expect("checked above");
    session
        .answer_permission(&request_id, PermissionDecision::allow())
        .await
        .expect("answering must unblock the turn");

    let mut ended = false;
    while let Some(event) = session.next_event().await.expect("draining must not fail") {
        if matches!(event, AgentEvent::TurnEnded(_)) {
            ended = true;
            break;
        }
    }
    assert!(ended, "the turn must close once the decision arrives");
}

#[tokio::test]
async fn a_denied_call_lands_in_the_turn_outcome_as_an_audit_record() {
    let adapter = FakeAdapter::new(asking_permission());
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    let records = check_session_with(session.as_mut(), &["go"], &|_| {
        PermissionDecision::deny("blocked by policy")
    })
    .await
    .expect("a denied turn still conforms");

    let outcome = &records[0].outcome;
    assert_eq!(outcome.denials.len(), 1);
    assert!(
        !outcome.is_clean_success(),
        "a turn that was refused work must not read as clean"
    );
    assert!(
        records[0]
            .events
            .iter()
            .any(|event| matches!(event, AgentEvent::ToolCompleted { is_error: true, .. })),
        "a refusal is a tool failure from the agent's point of view"
    );
}

#[tokio::test]
async fn a_narrowing_decision_is_refused_by_an_adapter_that_cannot_rewrite() {
    let adapter = FakeAdapter::new(asking_permission()).with_capabilities(
        Capabilities::none()
            .with(Capability::PreAuthorizeToolCalls)
            .with(Capability::InterruptibleTurn),
    );
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");
    session.send(TurnInput::queued("go")).await.expect("send");

    let mut request_id = None;
    while request_id.is_none() {
        if let Some(AgentEvent::PermissionRequested { request, .. }) =
            session.next_event().await.expect("draining must not fail")
        {
            request_id = Some(request.tool_call_id);
        }
    }

    let error = session
        .answer_permission(
            &request_id.expect("checked above"),
            PermissionDecision::allow_with(json!({"content": "NARROWED"})),
        )
        .await
        .expect_err("rewriting must be refused, not silently dropped");

    assert!(matches!(
        error,
        AdapterError::Unsupported {
            capability: Capability::RewritesToolInput,
            ..
        }
    ));
}

#[tokio::test]
async fn an_interrupt_ends_the_turn_and_leaves_the_session_usable() {
    let adapter = FakeAdapter::new(Script::of(vec![
        ScriptedTurn::saying("one")
            .with_step(Step::Say("two".to_owned()))
            .with_step(Step::Say("three".to_owned())),
        ScriptedTurn::saying("STILL_ALIVE"),
    ]));
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    session.send(TurnInput::queued("go")).await.expect("send");
    assert!(session.next_event().await.expect("first event").is_some());
    session.interrupt().await.expect("interrupt must succeed");

    let mut status = None;
    while let Some(event) = session.next_event().await.expect("draining must not fail") {
        if let AgentEvent::TurnEnded(outcome) = event {
            status = Some(outcome.status);
            break;
        }
    }
    assert_eq!(
        status,
        Some(TurnStatus::Interrupted),
        "an interrupt is a way for a turn to end, not a way to skip ending it"
    );

    let revived = run_turn(session.as_mut(), TurnInput::queued("still there?"))
        .await
        .expect("the session must survive its own interrupt");
    assert_eq!(revived.assistant_text(), "STILL_ALIVE");
}

#[tokio::test]
async fn an_adapter_without_interruptible_turns_refuses_to_interrupt() {
    let adapter = FakeAdapter::default()
        .with_capabilities(Capabilities::none().with(Capability::PreAuthorizeToolCalls));
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    let error = session
        .interrupt()
        .await
        .expect_err("an unclaimed capability must not silently no-op");
    assert!(matches!(
        error,
        AdapterError::Unsupported {
            capability: Capability::InterruptibleTurn,
            ..
        }
    ));
}

#[tokio::test]
async fn a_non_resumable_adapter_refuses_revival_rather_than_faking_it() {
    let adapter = FakeAdapter::default().with_capabilities(Capabilities::none());

    check_capability_honesty(&adapter)
        .await
        .expect("a limited adapter must refuse what it does not claim");

    match adapter
        .resume(&AgentSpec::default(), &ProviderSessionId::new("whatever"))
        .await
    {
        Err(AdapterError::Unsupported {
            capability: Capability::Resumable,
            ..
        }) => {}
        Err(other) => panic!("expected Unsupported(Resumable), got {other}"),
        Ok(_) => panic!("a revival that silently lost context is worse than a refusal"),
    }
}

#[tokio::test]
async fn resuming_keeps_the_session_identity_it_was_given() {
    let adapter = FakeAdapter::default();
    let recorded = ProviderSessionId::new("fake-session-42");

    let mut session = adapter
        .resume(&AgentSpec::default(), &recorded)
        .await
        .expect("the default fake is resumable");
    run_turn(session.as_mut(), TurnInput::queued("hello"))
        .await
        .expect("a revived session takes messages");

    assert_eq!(
        session.provider_session_id(),
        Some(&recorded),
        "revival must target the conversation it was asked for"
    );
}

/// The fake accumulates the script's per-turn tokens into a running total and
/// differences them, exactly as a real harness forces its adapter to. Without
/// that, the fake would exercise none of the usage normalization the Claude
/// adapter depends on.
#[tokio::test]
async fn usage_is_reported_per_turn_despite_being_accumulated_internally() {
    let tokens = |output: u64| TokenUsage {
        input_tokens: 5,
        output_tokens: output,
        ..TokenUsage::default()
    };
    let adapter = FakeAdapter::new(Script::of(vec![
        ScriptedTurn::saying("a").with_usage(tokens(100), 0.01),
        ScriptedTurn::saying("b").with_usage(tokens(30), 0.02),
    ]));
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    let records = check_session(session.as_mut(), &["a", "b"])
        .await
        .expect("must conform");

    let outputs: Vec<u64> = records
        .iter()
        .map(|record| record.outcome.usage.tokens.output_tokens)
        .collect();
    assert_eq!(
        outputs,
        vec![100, 30],
        "each turn reports only its own cost"
    );

    let total: f64 = records
        .iter()
        .map(|record| record.outcome.usage.cost_usd)
        .sum();
    assert!((total - 0.03).abs() < 1e-9, "deltas must sum to the total");
}

#[tokio::test]
async fn an_exhausted_script_can_end_the_harness() {
    let adapter = FakeAdapter::new(
        Script::of(vec![ScriptedTurn::saying("only one")]).ending_after_the_script(),
    );
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    run_turn(session.as_mut(), TurnInput::queued("first"))
        .await
        .expect("the scripted turn runs");

    let error = session
        .send(TurnInput::queued("second"))
        .await
        .expect_err("the script is spent and the harness is gone");
    assert!(matches!(error, AdapterError::HarnessExited { .. }));
    assert!(
        error.is_retryable_here(),
        "a dead harness is worth retrying; a missing capability is not"
    );
}

/// Chunk indices exist so a client can detect a gap in a turn's output. They
/// only do that if a turn actually emits several chunks on one channel, which
/// the single-utterance scripts above never do — this is the case that gives
/// the conformance suite's index check something to bite on.
#[tokio::test]
async fn several_chunks_on_one_channel_are_numbered_in_order() {
    let records = conforming_records(vec![
        ScriptedTurn::saying("one")
            .with_step(Step::Think("pondering".to_owned()))
            .with_step(Step::Say("two".to_owned()))
            .with_step(Step::Say("three".to_owned())),
    ])
    .await;

    let assistant: Vec<u64> = records[0]
        .events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Output {
                channel: OutputChannel::Assistant,
                chunk_index,
                ..
            } => Some(*chunk_index),
            _ => None,
        })
        .collect();
    assert_eq!(
        assistant,
        vec![0, 1, 2],
        "indices must advance within a channel"
    );

    let reasoning: Vec<u64> = records[0]
        .events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Output {
                channel: OutputChannel::Reasoning,
                chunk_index,
                ..
            } => Some(*chunk_index),
            _ => None,
        })
        .collect();
    assert_eq!(
        reasoning,
        vec![0],
        "each channel is numbered independently of the others"
    );
}

// ---------------------------------------------------------------------------
// Positive controls: the conformance suite must reject a misbehaving adapter.
//
// docs/TESTING.md §1 — a check that has never been shown to fail is not known
// to work. Each of these deliberately breaks one contract clause and asserts
// the suite catches it, so the checks stay honest as the suite grows.
// ---------------------------------------------------------------------------

/// Drives a misbehaving agent and returns the violation the suite reported.
async fn violation_from(misbehaviour: Misbehaviour, messages: &[&str]) -> String {
    let adapter = FakeAdapter::new(
        Script::of(vec![
            ScriptedTurn::saying("one").with_step(Step::Say("two".to_owned())),
            ScriptedTurn::saying("three"),
        ])
        .misbehaving(misbehaviour),
    );
    let Ok(mut session) = adapter.spawn(&AgentSpec::default()).await else {
        return "spawn failed".to_owned();
    };

    match check_session(session.as_mut(), messages).await {
        Ok(_) => String::new(),
        Err(failure) => failure.to_string(),
    }
}

#[tokio::test]
async fn output_before_the_session_is_named_is_rejected() {
    let reported = violation_from(Misbehaviour::SkipSessionEstablished, &["go"]).await;
    assert!(
        reported.contains("before any SessionEstablished"),
        "the suite must reject unattributable output, got: {reported:?}"
    );
}

#[tokio::test]
async fn a_turn_that_never_ends_is_rejected() {
    let reported = violation_from(Misbehaviour::NeverEndTurn, &["go"]).await;
    assert!(
        reported.contains("without a TurnEnded"),
        "every message owes exactly one turn ending, got: {reported:?}"
    );
}

#[tokio::test]
async fn a_repeated_chunk_index_is_rejected() {
    let reported = violation_from(Misbehaviour::RepeatChunkIndex, &["go"]).await;
    assert!(
        reported.contains("chunk index"),
        "indices exist for gap detection and must advance, got: {reported:?}"
    );
}

#[tokio::test]
async fn renaming_the_session_mid_conversation_is_rejected() {
    let reported = violation_from(
        Misbehaviour::ChangeSessionIdMidConversation,
        &["go", "again"],
    )
    .await;
    assert!(
        reported.contains("changed mid-conversation"),
        "revival would target the wrong conversation, got: {reported:?}"
    );
}

#[tokio::test]
async fn closing_the_same_turn_twice_is_rejected() {
    let reported = violation_from(Misbehaviour::EndTurnTwice, &["go", "again"]).await;
    assert!(
        !reported.is_empty(),
        "a doubled turn ending must not pass as two clean turns"
    );
}

/// A stalling agent still conforms; the clock is paused so the wait is free.
#[tokio::test(start_paused = true)]
async fn a_stalling_turn_still_conforms() {
    let adapter = FakeAdapter::new(Script::of(vec![
        ScriptedTurn::saying("thinking").with_step(Step::Stall(Duration::from_secs(300))),
    ]));
    let mut session = adapter.spawn(&AgentSpec::default()).await.expect("spawn");

    let started = tokio::time::Instant::now();
    let records = check_session(session.as_mut(), &["go"])
        .await
        .expect("a slow agent is still a conforming agent");

    assert_eq!(records.len(), 1);
    assert!(
        started.elapsed() >= Duration::from_secs(300),
        "the stall must actually advance the clock, or timeout tests would be vacuous"
    );
}

/// Horde's own verbs reach the adapter as ordinary tool calls, which is how a
/// script says "here the agent spawns a child".
#[tokio::test]
async fn a_horde_verb_is_reported_as_a_tool_call_without_an_approval_round_trip() {
    let records = conforming_records(vec![ScriptedTurn::saying("delegating").with_step(
        Step::UseHordeTool {
            tool_call_id: "call-9".to_owned(),
            tool_name: "spawn_agent".to_owned(),
            input: json!({ "agent_type": "reviewer", "tier": "deep" }),
        },
    )])
    .await;

    let called = records[0].events.iter().any(|event| {
        matches!(event, AgentEvent::Output { channel: OutputChannel::ToolCall, text, .. }
            if text.starts_with("spawn_agent "))
    });
    assert!(called, "the spawn intent must reach the event stream");
    assert!(
        records[0].outcome.is_clean_success(),
        "a Horde verb needs no approval and must not register as a denial"
    );
}
