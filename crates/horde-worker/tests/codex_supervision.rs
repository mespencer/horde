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

    use horde_adapter::{Adapter, AgentEvent, AgentSession, AgentSpec, TurnInput, TurnOutcome};
    use horde_worker::CodexAdapter;
    use serde_json::json;

    /// The stub binary cargo built alongside this test.
    pub(crate) fn stub_binary() -> PathBuf {
        crate::support::built_binary("codex-stub-harness")
    }

    // -- Script lines, one builder per frame kind ------------------------------
    //
    // The scripts differ per test in which frames they replay and in what
    // order; the frames themselves are the same recorded shapes. Builders keep
    // each test's script an explicit composition instead of twenty copies of
    // the same JSON.

    pub(crate) fn thread_started() -> String {
        json!({"method":"thread/started",
               "params":{"thread":{"id":"stub-thread","model":"stub-model"}}})
        .to_string()
    }

    pub(crate) fn turn_started(id: &str) -> String {
        json!({"method":"turn/started","params":{"turn":{"id":id}}}).to_string()
    }

    pub(crate) fn delta(turn: &str, text: &str) -> String {
        json!({"method":"item/agentMessage/delta",
               "params":{"turnId":turn,"itemId":"i1","delta":text}})
        .to_string()
    }

    pub(crate) fn usage(turn: &str, input_tokens: u64, output_tokens: u64) -> String {
        json!({"method":"thread/tokenUsage/updated",
               "params":{"threadId":"stub-thread","turnId":turn,
                          "tokenUsage":{"last":{"inputTokens":input_tokens,
                                                 "outputTokens":output_tokens}}}})
        .to_string()
    }

    pub(crate) fn final_answer(turn: &str, item: &str, text: &str) -> String {
        json!({"method":"item/completed",
               "params":{"turnId":turn,
                          "item":{"type":"agentMessage","id":item,"text":text,
                                  "phase":"final_answer"}}})
        .to_string()
    }

    pub(crate) fn turn_completed(id: &str) -> String {
        json!({"method":"turn/completed",
               "params":{"turn":{"id":id,"status":"completed","durationMs":5}}})
        .to_string()
    }

    pub(crate) fn idle() -> String {
        json!({"method":"thread/status/changed",
               "params":{"threadId":"stub-thread","status":{"type":"idle"}}})
        .to_string()
    }

    /// One scripted turn: the thread announces itself, a turn opens, streams
    /// one delta, reports usage, and completes.
    pub(crate) fn basic_turn() -> String {
        [
            thread_started(),
            turn_started("turn-1"),
            delta("turn-1", "HORDE_OK"),
            usage("turn-1", 10, 2),
            final_answer("turn-1", "i1", "HORDE_OK"),
            turn_completed("turn-1"),
        ]
        .join("\n")
    }

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

        let mut adapter = CodexAdapter::new().with_program(stub_binary().to_string_lossy());
        // A stub that answers nothing must fail the test in seconds, not
        // stall the suite until a mutants timeout nobody evaluates.
        adapter.response_ceiling = std::time::Duration::from_secs(2);
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

    /// Bounded waiting, shared with the claude suite — see `support::within`.
    pub(crate) use crate::support::within;

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

    /// The opening block every steering-shaped script starts with: the
    /// thread announces itself, turn-1 opens, and a delta proves it open.
    pub(crate) fn opening_block() -> String {
        [
            thread_started(),
            turn_started("turn-1"),
            delta("turn-1", "working"),
        ]
        .join("\n")
    }

    /// A session already mid-turn: spawned on `script` (whose first block must
    /// stream a delta), its opening message sent, and drained until output
    /// proved the turn open — the precondition every steering-shaped test
    /// shares. Returns the control-log path the stub records to; the tempdirs
    /// ride along because dropping them deletes the files under the child.
    pub(crate) async fn mid_turn(
        script: &str,
        steer_mode: Option<&str>,
    ) -> (
        Box<dyn AgentSession>,
        std::path::PathBuf,
        (tempfile::TempDir, tempfile::TempDir),
    ) {
        let log = tempfile::tempdir().expect("a scratch dir");
        let log_path = log.path().join("control.log");
        let log_value = log_path.to_string_lossy().into_owned();
        let mut extra: Vec<(&str, &str)> = vec![("HORDE_CODEX_STUB_CONTROL_LOG", &log_value)];
        if let Some(mode) = steer_mode {
            extra.push(("HORDE_CODEX_STUB_STEER", mode));
        }
        let (mut session, dir) = session_on(script, &extra).await;
        session
            .send(TurnInput::queued("start"))
            .await
            .expect("sends");
        drain_until_output(&mut session).await;
        (session, log_path, (log, dir))
    }

    /// Bounded control-log polling, shared with the claude suite — see
    /// `support::control_log_reaches`.
    pub(crate) use crate::support::control_log_reaches as control_log_shows;
}

#[cfg(test)]
mod support;

use harness::{
    adapter_for, basic_turn, control_log_shows, events_through_turn_end, mid_turn, session_on,
    within,
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
    let (mut session, _dir) = session_on(&basic_turn(), &[]).await;

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
    let mut streamed = String::new();
    for event in &events {
        if let AgentEvent::Output {
            channel: OutputChannel::Assistant,
            text,
            ..
        } = event
        {
            streamed.push_str(text);
        }
    }
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
    let (mut adapter, dir) = adapter_for(&basic_turn(), &[]);
    adapter.sandbox_policy = horde_adapter_codex::SandboxPolicy::default()
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

    let elicitation = json!({
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
    });
    let mcp_item = |status: &str| {
        json!({"method": if status == "inProgress" { "item/started" } else { "item/completed" },
               "params":{"turnId":"turn-1",
                          "item":{"type":"mcpToolCall","id":"exec-1","server":"horde",
                                  "tool":"spawn_agent","status":status,
                                  "arguments":{"task":"review"}}}})
        .to_string()
    };
    let script = [
        harness::thread_started(),
        harness::turn_started("turn-1"),
        mcp_item("inProgress"),
        format!("!ELICIT {elicitation}"),
        mcp_item("completed"),
        harness::turn_completed("turn-1"),
    ]
    .join("\n");

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
    let (mut session, _dir) = session_on(&basic_turn(), &[]).await;

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
    // Block 1 opens a turn and stalls (no terminal frame); block 2 — played on
    // the steer — carries the second final answer and the turn's end, the
    // transcript-15 shape.
    let script = [
        [
            harness::thread_started(),
            harness::turn_started("turn-1"),
            harness::delta("turn-1", "FIRST"),
            harness::final_answer("turn-1", "i1", "FIRST"),
        ]
        .join("\n"),
        [
            harness::final_answer("turn-1", "i2", "STEERED"),
            harness::turn_completed("turn-1"),
        ]
        .join("\n"),
    ]
    .join("\n\n");

    let (mut session, log_path, _dirs) = mid_turn(&script, None).await;

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
    // Block 1: a turn that opens and never closes on its own (the steer will
    // be sent against it). Block 2: the fresh turn the fallback starts.
    let script = [
        harness::opening_block(),
        [
            harness::turn_started("turn-2"),
            harness::turn_completed("turn-2"),
        ]
        .join("\n"),
    ]
    .join("\n\n");

    let (mut session, log_path, _dirs) = mid_turn(&script, Some("stale")).await;

    session
        .send(TurnInput::queued("the raced message"))
        .await
        .expect("the steer goes out");

    // The refusal comes back interleaved; reading events is what routes it.
    // Only the fallback turn/start can make turn-2 run, so draining to a turn
    // ending IS the proof the fallback happened — a session that dropped the
    // raced message would stall here and fail on the ceiling.
    let (_, outcome) = events_through_turn_end(&mut session).await;
    assert!(outcome.is_some(), "the fallback turn must run to its end");
    let recorded = control_log_shows(&log_path, |log| {
        log.contains("turn/steer") && log.matches("\"method\":\"turn/start\"").count() >= 2
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
    // Block 1: a turn that opens and stalls. Block 2 — played on the
    // interrupt — the delta tail and the idle that closes the turn
    // (transcript 11's shape). Block 3 — played on the deferred turn/start —
    // the replacement turn.
    let script = [
        harness::opening_block(),
        [harness::delta("turn-1", "tail"), harness::idle()].join("\n"),
        [
            harness::turn_started("turn-2"),
            harness::final_answer("turn-2", "i2", "REPLACEMENT"),
            harness::turn_completed("turn-2"),
        ]
        .join("\n"),
    ]
    .join("\n\n");

    let (mut session, log_path, _dirs) = mid_turn(&script, None).await;

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

/// `interrupt()` itself — not just interrupt-delivery — must put
/// `turn/interrupt` on the wire and read the idle that follows as the
/// cancellation. An interrupt that silently does nothing is indistinguishable
/// from one that worked, right up until a user cannot stop a runaway agent.
#[tokio::test]
async fn interrupt_puts_the_cancel_on_the_wire_and_idle_closes_the_turn() {
    let script = [
        harness::opening_block(),
        [harness::delta("turn-1", "tail"), harness::idle()].join("\n"),
    ]
    .join("\n\n");

    let (mut session, log_path, _dirs) = mid_turn(&script, None).await;
    session.interrupt().await.expect("the cancel is writable");

    let recorded = control_log_shows(&log_path, |log| log.contains("turn/interrupt")).await;
    assert!(
        recorded.contains("turn/interrupt"),
        "the cancel must actually reach the child: {recorded}"
    );

    let (_, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("idle closes the interrupted turn");
    assert_eq!(outcome.status, TurnStatus::Interrupted);
    assert_eq!(outcome.text, None, "a cancelled turn has no answer");
    session.shutdown().await.expect("shuts down");
}

/// A harness that answers nothing must become a spawn error on the response
/// ceiling — the wedged-child case that would otherwise hang the pump
/// forever. The ceiling is legitimate here: each read genuinely awaits the
/// pipe, so the clock is bounding an arrival, not a loop.
#[tokio::test]
async fn a_mute_harness_fails_the_spawn_on_the_response_ceiling() {
    let (mut adapter, _dir) = adapter_for(&basic_turn(), &[("HORDE_CODEX_STUB_MUTE", "1")]);
    adapter.response_ceiling = std::time::Duration::from_millis(300);

    let error = adapter
        .spawn(&AgentSpec::default())
        .await
        .err()
        .expect("silence must become an error, not a hang");
    assert!(
        error.to_string().contains("no reply"),
        "the failure names what never arrived: {error}"
    );
}

/// A harness that exits during the handshake reads as its death, not as a
/// protocol accident — EOF is how a dead child announces itself.
#[tokio::test]
async fn a_harness_that_dies_mid_handshake_reads_as_its_death() {
    // No script in the environment: the stub prints why to stderr and exits
    // nonzero, which is exactly a binary that starts and immediately dies.
    let adapter =
        horde_worker::CodexAdapter::new().with_program(harness::stub_binary().to_string_lossy());

    let error = adapter
        .spawn(&AgentSpec::default())
        .await
        .err()
        .expect("a dead child must fail the spawn");
    assert!(
        matches!(error, AdapterError::HarnessExited { .. }),
        "EOF during the handshake is the child's death: {error:?}"
    );
}

/// A `thread/start` refusal that names authentication maps to
/// `Unauthenticated` — the scheduler's cue to try another account, not
/// another Worker. Anything else is a spawn failure.
#[tokio::test]
async fn an_unauthenticated_thread_start_names_the_account_problem() {
    let (adapter, _dir) = adapter_for(
        &basic_turn(),
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
        &basic_turn(),
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
        &basic_turn(),
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
        &basic_turn(),
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
        &basic_turn(),
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
    let (mut session, _dir) = session_on(&basic_turn(), &[]).await;
    session.shutdown().await.expect("shuts down");

    let refused = session.send(TurnInput::queued("anyone home?")).await;
    assert!(
        matches!(refused, Err(AdapterError::HarnessExited { .. })),
        "a shut-down session refuses work as the death it is — before touching \
         the wire, so the refusal is deterministic, never a pipe race: {refused:?}"
    );
    let cancel = session.interrupt().await;
    assert!(
        matches!(cancel, Err(AdapterError::HarnessExited { .. })),
        "and an interrupt after the end is refused the same way: {cancel:?}"
    );
    assert!(
        within("the closed stream answers", session.next_event())
            .await
            .expect("no error")
            .is_none(),
        "and produces nothing further"
    );
}
