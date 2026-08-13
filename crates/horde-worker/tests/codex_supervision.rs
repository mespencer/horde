//! Process-level tests for the live Codex session, against a scripted
//! app-server child.
//!
//! Real `fork`/`exec`, real pipes, real request/response interleaving — no
//! login, no network, no quota. The stub (`codex-stub-harness`) answers the
//! handshake and replays one block of recorded-shape notifications per turn
//! request, so everything above the transport is the code a live `codex
//! app-server` session runs.

#[cfg(test)]
mod harness {
    use std::path::PathBuf;

    use horde_adapter::{Adapter, AgentEvent, AgentSession, AgentSpec, TurnOutcome};
    use horde_worker::CodexAdapter;

    /// The stub binary cargo built alongside this test.
    pub(crate) fn stub_binary() -> PathBuf {
        let mut path = std::env::current_exe().unwrap_or_default();
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path.join("codex-stub-harness")
    }

    /// One scripted turn: the thread announces itself, a turn opens, streams
    /// one delta, reports usage, and completes.
    pub(crate) const BASIC_TURN: &str = concat!(
        r#"{"method":"thread/started","params":{"thread":{"id":"stub-thread","model":"stub-model"}}}"#,
        "\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}"#,
        "\n",
        r#"{"method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"i1","delta":"HORDE_OK"}}"#,
        "\n",
        r#"{"method":"thread/tokenUsage/updated","params":{"threadId":"stub-thread","turnId":"turn-1","tokenUsage":{"last":{"inputTokens":10,"outputTokens":2}}}}"#,
        "\n",
        r#"{"method":"item/completed","params":{"turnId":"turn-1","item":{"type":"agentMessage","id":"i1","text":"HORDE_OK","phase":"final_answer"}}}"#,
        "\n",
        r#"{"method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed","durationMs":5}}}"#,
    );

    /// An adapter pointed at the stub, with `script` and `extra` env riding to
    /// the child. The `TempDir` must travel with the session: dropping it
    /// deletes the script out from under a child still reading it.
    pub(crate) fn adapter_for(
        script: &str,
        extra: &[(&str, &str)],
    ) -> (CodexAdapter, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let file = dir.path().join("script.jsonl");
        std::fs::write(&file, script).expect("writes");

        let mut adapter = CodexAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = vec![(
            "HORDE_CODEX_STUB_SCRIPT".to_owned(),
            file.to_string_lossy().into_owned(),
        )];
        for (key, value) in extra {
            adapter
                .extra_env
                .push(((*key).to_owned(), (*value).to_owned()));
        }
        (adapter, dir)
    }

    /// A live session on the stub, its spawn already completed.
    pub(crate) async fn session_on(
        script: &str,
        extra: &[(&str, &str)],
    ) -> (Box<dyn AgentSession>, tempfile::TempDir) {
        let (adapter, dir) = adapter_for(script, extra);
        let session = adapter
            .spawn(&AgentSpec::default())
            .await
            .expect("the stub spawns");
        (session, dir)
    }

    /// How long any wait in this file may take before it is a failure.
    pub(crate) const CEILING: std::time::Duration = std::time::Duration::from_secs(5);

    /// Runs `future` with a ceiling, failing the test if it does not finish —
    /// an unbounded await turns a defect into a hang, and a hang into a
    /// mutant nobody evaluated (docs/QUALITY_GATES.md).
    pub(crate) async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
        match tokio::time::timeout(CEILING, future).await {
            Ok(value) => value,
            Err(_) => panic!("{what} must finish rather than hang"),
        }
    }

    /// Drains `session` until its turn ends, under the ceiling, returning the
    /// outcome and everything that preceded it.
    pub(crate) async fn events_through_turn_end(
        session: &mut Box<dyn AgentSession>,
    ) -> (Vec<AgentEvent>, Option<TurnOutcome>) {
        within("the turn ends", async {
            let mut events = Vec::new();
            loop {
                match session.next_event().await {
                    Ok(Some(AgentEvent::TurnEnded(outcome))) => return (events, Some(outcome)),
                    Ok(Some(event)) => events.push(event),
                    Ok(None) | Err(_) => return (events, None),
                }
            }
        })
        .await
    }

    /// Drains events until assistant output arrives — the proof the turn is
    /// genuinely open on the session's side, since output can only follow the
    /// `turn/started` frame. A mid-turn send before this point would honestly
    /// (and correctly) start a turn rather than steer one.
    pub(crate) async fn drain_until_output(session: &mut Box<dyn AgentSession>) {
        within("output arrives", async {
            loop {
                match session.next_event().await {
                    Ok(Some(AgentEvent::Output { .. })) => return,
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => panic!("the stream ended before the turn opened"),
                }
            }
        })
        .await;
    }

    /// Polls the control log, bounded by count, until `expected` holds.
    pub(crate) async fn control_log_shows(
        log_path: &std::path::Path,
        expected: impl Fn(&str) -> bool,
    ) -> String {
        let mut recorded = String::new();
        for _ in 0..250 {
            recorded = std::fs::read_to_string(log_path).unwrap_or_default();
            if expected(&recorded) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        recorded
    }
}

use harness::{
    BASIC_TURN, adapter_for, control_log_shows, events_through_turn_end, session_on, within,
};
use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSpec, OutputChannel, PermissionDecision,
    ProviderSessionId, ToolCallId, TurnInput, TurnStatus,
};
use serde_json::json;

/// A whole turn, from spawn through the handshake to `TurnEnded`, against a
/// real child process.
#[tokio::test]
async fn a_turn_round_trips_through_the_handshake_and_a_real_child() {
    let (mut session, _dir) = session_on(BASIC_TURN, &[]).await;

    assert_eq!(
        session.provider_session_id().map(ToString::to_string),
        Some("stub-thread".to_owned()),
        "the revival identifier comes from the thread response, before any turn"
    );

    session
        .send(TurnInput::queued("hello"))
        .await
        .expect("sends");
    let (events, outcome) = events_through_turn_end(&mut session).await;

    let established_first = matches!(events.first(), Some(AgentEvent::SessionEstablished { .. }));
    assert!(
        established_first,
        "SessionEstablished precedes any output: {events:?}"
    );
    let streamed: String = events
        .iter()
        .filter_map(|event| match event {
            AgentEvent::Output {
                channel: OutputChannel::Assistant,
                text,
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(streamed, "HORDE_OK");

    let outcome = outcome.expect("the turn ends");
    assert_eq!(outcome.status, TurnStatus::Completed);
    assert_eq!(outcome.text.as_deref(), Some("HORDE_OK"));
    assert_eq!(outcome.usage.tokens.input_tokens, 10);

    session.shutdown().await.expect("shutdown kills by handle");
}

/// The sandbox tier and its reason reach the journalable event stream — the
/// tier is the enforcement, so its choice must be auditable (ADR-0005).
#[tokio::test]
async fn the_sandbox_tier_choice_is_journaled_on_the_first_turn() {
    let (mut adapter, dir) = adapter_for(BASIC_TURN, &[]);
    adapter.sandbox_policy = horde_adapter_codex::SandboxPolicy::read_only()
        .with_mode("builder", horde_adapter_codex::SandboxTier::WorkspaceWrite);
    let spec = AgentSpec {
        permission_mode: Some("builder".to_owned()),
        ..AgentSpec::default()
    };
    let mut session = adapter.spawn(&spec).await.expect("spawns");
    session.send(TurnInput::queued("go")).await.expect("sends");

    let (events, _) = events_through_turn_end(&mut session).await;
    let diagnostic = events.iter().find_map(|event| match event {
        AgentEvent::Output {
            channel: OutputChannel::HarnessDiagnostic,
            text,
            ..
        } => Some(text.clone()),
        _ => None,
    });
    let text = diagnostic.expect("the tier choice must be journaled");
    assert!(
        text.contains("workspace-write") && text.contains("builder"),
        "the reason names the tier and the mode that chose it: {text}"
    );

    session.shutdown().await.expect("shuts down");
    drop(dir);
}

/// The elicitation flow end to end over a real process: the request surfaces
/// with the tool's identity, the reply reaches the child in the elicitation
/// vocabulary, and the turn finishes.
#[tokio::test]
async fn an_elicitation_surfaces_and_the_accept_reaches_the_wire() {
    let log = tempfile::tempdir().expect("a scratch dir");
    let log_path = log.path().join("control.log");

    let script = format!(
        concat!(
            r#"{{"method":"thread/started","params":{{"thread":{{"id":"stub-thread","model":"stub-model"}}}}}}"#,
            "\n",
            r#"{{"method":"turn/started","params":{{"turn":{{"id":"turn-1"}}}}}}"#,
            "\n",
            r#"{{"method":"item/started","params":{{"turnId":"turn-1","item":{{"type":"mcpToolCall","id":"exec-1","server":"horde","tool":"spawn_agent","status":"inProgress","arguments":{{"task":"review"}}}}}}}}"#,
            "\n",
            "!ELICIT {}",
            "\n",
            r#"{{"method":"item/completed","params":{{"turnId":"turn-1","item":{{"type":"mcpToolCall","id":"exec-1","server":"horde","tool":"spawn_agent","status":"completed","arguments":{{"task":"review"}}}}}}}}"#,
            "\n",
            r#"{{"method":"turn/completed","params":{{"turn":{{"id":"turn-1","status":"completed"}}}}}}"#,
        ),
        json!({
            "threadId": "stub-thread",
            "turnId": "turn-1",
            "serverName": "horde",
            "mode": "form",
            "_meta": {
                "codex_approval_kind": "mcp_tool_call",
                "tool_params": {"task": "review"}
            },
            "message": "Allow?",
            "requestedSchema": {"type": "object", "properties": {}}
        })
    );

    let (mut session, _dir) = session_on(
        &script,
        &[("HORDE_CODEX_STUB_CONTROL_LOG", &log_path.to_string_lossy())],
    )
    .await;
    session.send(TurnInput::queued("go")).await.expect("sends");

    // Drain to the permission request.
    let request = within("the request surfaces", async {
        loop {
            match session.next_event().await {
                Ok(Some(AgentEvent::PermissionRequested { request, .. })) => return Some(request),
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => return None,
            }
        }
    })
    .await
    .expect("the elicitation must surface as a permission request");

    assert_eq!(request.tool_name, "horde/spawn_agent");
    assert_eq!(request.input, json!({"task": "review"}));

    session
        .answer_permission(&request.tool_call_id, PermissionDecision::allow())
        .await
        .expect("the answer is writable");

    // The reply must reach the child in the elicitation vocabulary: `action`,
    // never `decision` — the wrong shape fails to deserialize server-side.
    let recorded = control_log_shows(&log_path, |log| log.contains("\"action\":\"accept\"")).await;
    assert!(
        recorded.contains("\"action\":\"accept\""),
        "the accept must reach the wire as an elicitation result: {recorded}"
    );
    assert!(
        !recorded.contains("\"decision\""),
        "the approval vocabulary would deserialize to nothing: {recorded}"
    );

    let (_, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("the turn finishes after the allow");
    assert!(outcome.denials.is_empty(), "an allow is not a refusal");
    session.shutdown().await.expect("shuts down");
}

/// A narrowing decision must be refused loudly — the elicitation reply cannot
/// carry replacement arguments, and a narrowing that silently widened into a
/// plain allow is the failure the capability advertisement exists to prevent.
#[tokio::test]
async fn a_narrowing_decision_is_refused_not_widened() {
    let (mut session, _dir) = session_on(BASIC_TURN, &[]).await;

    let refused = session
        .answer_permission(
            &ToolCallId::new("exec-1"),
            PermissionDecision::allow_with(json!({"task": "narrower"})),
        )
        .await;
    assert!(
        matches!(refused, Err(AdapterError::Unsupported { .. })),
        "got {refused:?}"
    );

    // And an answer for a call nobody proposed is an error, never a no-op.
    let stray = session
        .answer_permission(&ToolCallId::new("exec-1"), PermissionDecision::allow())
        .await;
    assert!(
        matches!(stray, Err(AdapterError::UnknownPermissionRequest { .. })),
        "got {stray:?}"
    );
    session.shutdown().await.expect("shuts down");
}

/// Steering: a message sent mid-turn goes out as `turn/steer` carrying the
/// active turn as its precondition, and the amended turn still ends once.
#[tokio::test]
async fn a_mid_turn_message_steers_the_active_turn() {
    let log = tempfile::tempdir().expect("a scratch dir");
    let log_path = log.path().join("control.log");

    // Block 1 opens a turn and stalls (no terminal frame); block 2 — played on
    // the steer — carries the second final answer and the turn's end, the
    // transcript-15 shape.
    let script = concat!(
        r#"{"method":"thread/started","params":{"thread":{"id":"stub-thread","model":"stub-model"}}}"#,
        "\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}"#,
        "\n",
        r#"{"method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"i1","delta":"FIRST"}}"#,
        "\n",
        r#"{"method":"item/completed","params":{"turnId":"turn-1","item":{"type":"agentMessage","id":"i1","text":"FIRST","phase":"final_answer"}}}"#,
        "\n\n",
        r#"{"method":"item/completed","params":{"turnId":"turn-1","item":{"type":"agentMessage","id":"i2","text":"STEERED","phase":"final_answer"}}}"#,
        "\n",
        r#"{"method":"turn/completed","params":{"turn":{"id":"turn-1","status":"completed"}}}"#,
    );

    let (mut session, _dir) = session_on(
        script,
        &[("HORDE_CODEX_STUB_CONTROL_LOG", &log_path.to_string_lossy())],
    )
    .await;
    session
        .send(TurnInput::queued("start"))
        .await
        .expect("sends");
    harness::drain_until_output(&mut session).await;

    session
        .send(TurnInput::queued("actually, do this instead"))
        .await
        .expect("a mid-turn send steers");

    let recorded = control_log_shows(&log_path, |log| log.contains("turn/steer")).await;
    assert!(
        recorded.contains("\"expectedTurnId\":\"turn-1\""),
        "the steer names the turn it amends: {recorded}"
    );

    let (_, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("the amended turn ends exactly once");
    assert_eq!(
        outcome.text.as_deref(),
        Some("STEERED"),
        "the turn's answer is the answer to the steer"
    );
    session.shutdown().await.expect("shuts down");
}

/// The steer race: `expectedTurnId` no longer active means the harness
/// refuses the steer, and the message falls back to an ordinary `turn/start`
/// rather than being dropped.
#[tokio::test]
async fn a_steer_that_lost_its_race_falls_back_to_a_fresh_turn() {
    let log = tempfile::tempdir().expect("a scratch dir");
    let log_path = log.path().join("control.log");

    // Block 1: a turn that opens and never closes on its own (the steer will
    // be sent against it). Block 2: the fresh turn the fallback starts.
    let script = concat!(
        r#"{"method":"thread/started","params":{"thread":{"id":"stub-thread","model":"stub-model"}}}"#,
        "\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}"#,
        "\n",
        r#"{"method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"i1","delta":"working"}}"#,
        "\n\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-2"}}}"#,
        "\n",
        r#"{"method":"turn/completed","params":{"turn":{"id":"turn-2","status":"completed"}}}"#,
    );

    let (mut session, _dir) = session_on(
        script,
        &[
            ("HORDE_CODEX_STUB_CONTROL_LOG", &log_path.to_string_lossy()),
            ("HORDE_CODEX_STUB_STEER", "stale"),
        ],
    )
    .await;
    session
        .send(TurnInput::queued("start"))
        .await
        .expect("sends");
    harness::drain_until_output(&mut session).await;

    session
        .send(TurnInput::queued("the raced message"))
        .await
        .expect("the steer goes out");

    // The refusal comes back interleaved; reading events is what routes it,
    // and the fallback turn/start must then reach the wire with the text.
    let _ = within("the fallback settles", session.next_event()).await;
    let recorded = control_log_shows(&log_path, |log| {
        log.contains("turn/steer") && log.matches("turn/start").count() >= 2
    })
    .await;
    assert!(
        recorded.contains("the raced message"),
        "the raced message is delivered as a fresh turn, not dropped: {recorded}"
    );
    session.shutdown().await.expect("shuts down");
}

/// An interrupt-delivery message cancels the active turn and is delivered
/// only after that turn actually closes — starting it earlier would race the
/// idle that ends the old turn.
#[tokio::test]
async fn an_interrupt_delivery_cancels_then_delivers_after_the_turn_closes() {
    let log = tempfile::tempdir().expect("a scratch dir");
    let log_path = log.path().join("control.log");

    // Block 1: a turn that opens and stalls. Block 2 — played on the
    // interrupt — the delta tail and the idle that closes the turn
    // (transcript 11's shape). Block 3 — played on the deferred turn/start —
    // the replacement turn.
    let script = concat!(
        r#"{"method":"thread/started","params":{"thread":{"id":"stub-thread","model":"stub-model"}}}"#,
        "\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-1"}}}"#,
        "\n",
        r#"{"method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"i1","delta":"working"}}"#,
        "\n\n",
        r#"{"method":"item/agentMessage/delta","params":{"turnId":"turn-1","itemId":"i1","delta":"tail"}}"#,
        "\n",
        r#"{"method":"thread/status/changed","params":{"threadId":"stub-thread","status":{"type":"idle"}}}"#,
        "\n\n",
        r#"{"method":"turn/started","params":{"turn":{"id":"turn-2"}}}"#,
        "\n",
        r#"{"method":"item/completed","params":{"turnId":"turn-2","item":{"type":"agentMessage","id":"i2","text":"REPLACEMENT","phase":"final_answer"}}}"#,
        "\n",
        r#"{"method":"turn/completed","params":{"turn":{"id":"turn-2","status":"completed"}}}"#,
    );

    let (mut session, _dir) = session_on(
        script,
        &[("HORDE_CODEX_STUB_CONTROL_LOG", &log_path.to_string_lossy())],
    )
    .await;
    session
        .send(TurnInput::queued("start"))
        .await
        .expect("sends");
    harness::drain_until_output(&mut session).await;

    session
        .send(TurnInput::interrupting("do this instead"))
        .await
        .expect("the interrupt goes out");

    // First ending: the cancelled turn, closed by idle, with no answer.
    let (_, first) = events_through_turn_end(&mut session).await;
    let first = first.expect("the interrupted turn closes on idle");
    assert_eq!(first.status, TurnStatus::Interrupted);
    assert_eq!(first.text, None, "a cancelled turn has no answer");

    // Second ending: the deferred message's own turn.
    let (_, second) = events_through_turn_end(&mut session).await;
    let second = second.expect("the deferred message runs as its own turn");
    assert_eq!(second.status, TurnStatus::Completed);
    assert_eq!(second.text.as_deref(), Some("REPLACEMENT"));

    let recorded = control_log_shows(&log_path, |log| log.contains("do this instead")).await;
    let interrupt_at = recorded.find("turn/interrupt");
    let deferred_at = recorded.find("do this instead");
    match (interrupt_at, deferred_at) {
        (Some(interrupt), Some(deferred)) => assert!(
            interrupt < deferred,
            "the cancel precedes the delivery: {recorded}"
        ),
        other => panic!("both requests must reach the wire, got {other:?}: {recorded}"),
    }
    session.shutdown().await.expect("shuts down");
}

/// A `thread/start` refusal that names authentication maps to
/// `Unauthenticated` — the scheduler's cue to try another account, not
/// another Worker. Anything else is a spawn failure.
#[tokio::test]
async fn an_unauthenticated_thread_start_names_the_account_problem() {
    let (adapter, _dir) = adapter_for(
        BASIC_TURN,
        &[(
            "HORDE_CODEX_STUB_FAIL_THREAD",
            "401 Unauthorized: not logged in",
        )],
    );
    let spec = AgentSpec {
        account: Some(horde_adapter::ProviderAccount::new("work")),
        ..AgentSpec::default()
    };

    let error = adapter
        .spawn(&spec)
        .await
        .err()
        .expect("an unauthenticated spawn must fail");
    match &error {
        AdapterError::Unauthenticated { account, .. } => {
            assert_eq!(account.as_deref(), Some("work"));
        }
        other => panic!("expected Unauthenticated, got {other}"),
    }
    assert!(
        !error.is_retryable_here(),
        "retrying the same account here only burns time"
    );
}

/// A thread refusal that is not about auth is a spawn failure, and stays
/// retryable-nowhere too — but under its own name, so the operator's log
/// tells the two apart.
#[tokio::test]
async fn an_ordinary_thread_refusal_is_a_spawn_failure() {
    let (adapter, _dir) = adapter_for(
        BASIC_TURN,
        &[("HORDE_CODEX_STUB_FAIL_THREAD", "rollout file is corrupt")],
    );
    let error = adapter
        .spawn(&AgentSpec::default())
        .await
        .err()
        .expect("the spawn must fail");
    assert!(
        matches!(&error, AdapterError::Spawn { message, .. } if message.contains("corrupt")),
        "got {error}"
    );
}

/// A refused `turn/start` surfaces through `next_event` — where the pump
/// listens — rather than hanging a turn that never opened.
#[tokio::test]
async fn a_refused_turn_start_fails_loudly_instead_of_hanging() {
    let (mut session, _dir) = session_on(
        BASIC_TURN,
        &[("HORDE_CODEX_STUB_FAIL_TURN_START", "thread is archived")],
    )
    .await;

    session
        .send(TurnInput::queued("hello"))
        .await
        .expect("send itself succeeds; the refusal arrives on the stream");

    let error = within("the refusal surfaces", async {
        loop {
            match session.next_event().await {
                Ok(Some(_)) => continue,
                Ok(None) => return None,
                Err(error) => return Some(error),
            }
        }
    })
    .await
    .expect("the refusal must surface as an error");
    assert!(
        error.to_string().contains("thread is archived"),
        "the reason reaches the lifecycle: {error}"
    );
    session.shutdown().await.expect("shuts down");
}

/// Revival: the thread id learned from one process revives in the next, and
/// the resume request carries it to the harness.
#[tokio::test]
async fn a_thread_id_from_one_process_revives_in_the_next() {
    let log = tempfile::tempdir().expect("a scratch dir");
    let log_path = log.path().join("control.log");

    let (adapter, _dir) = adapter_for(
        BASIC_TURN,
        &[("HORDE_CODEX_STUB_CONTROL_LOG", &log_path.to_string_lossy())],
    );

    let mut first = adapter.spawn(&AgentSpec::default()).await.expect("spawns");
    let revival_id = first
        .provider_session_id()
        .cloned()
        .expect("the id reaches the caller at spawn");
    first.shutdown().await.expect("shuts down");

    let mut revived = adapter
        .resume(&AgentSpec::default(), &revival_id)
        .await
        .expect("resume spawns a fresh process");
    assert_eq!(
        revived.provider_session_id().map(ToString::to_string),
        Some(revival_id.to_string()),
        "the revived session keeps the identity it was revived from"
    );

    let recorded = control_log_shows(&log_path, |log| log.contains("thread/resume")).await;
    assert!(
        recorded.contains(&format!("\"threadId\":\"{revival_id}\"")),
        "the resume names the thread it revives: {recorded}"
    );
    revived.shutdown().await.expect("shuts down");
}

/// Resuming a thread the harness cannot find fails loudly rather than
/// silently starting a fresh conversation that has lost its context.
#[tokio::test]
async fn reviving_an_unknown_thread_does_not_silently_start_fresh() {
    let (adapter, _dir) = adapter_for(
        BASIC_TURN,
        &[(
            "HORDE_CODEX_STUB_FAIL_THREAD",
            "no rollout found for thread",
        )],
    );
    let refused = adapter
        .resume(&AgentSpec::default(), &ProviderSessionId::new("t-missing"))
        .await;
    assert!(
        refused.is_err(),
        "a revival that lost its context must be refused, not faked"
    );
}

/// A dead harness reads as end-of-stream and a refused send, never a hang.
#[tokio::test]
async fn a_dead_harness_surfaces_instead_of_hanging() {
    let (mut session, _dir) = session_on(BASIC_TURN, &[]).await;
    session.shutdown().await.expect("shuts down");

    assert!(
        session
            .send(TurnInput::queued("anyone home?"))
            .await
            .is_err(),
        "a shut-down session refuses work"
    );
    assert!(
        within("the closed stream answers", session.next_event())
            .await
            .expect("no error")
            .is_none(),
        "and produces nothing further"
    );
}
