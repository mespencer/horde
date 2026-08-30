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

    /// The stub binary cargo built alongside this test, its first-exec
    /// admission paid (`support::admitted_binary`) so the adapter's 2s
    /// response ceiling never absorbs the OS's first-launch validation.
    /// `--version` is the stub's documented prompt-exit probe.
    pub(crate) fn stub_binary() -> PathBuf {
        crate::support::admitted_binary("codex-stub-harness", &["--version"])
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

    /// A session on `script` with the stub's control log wired and its
    /// opening message sent — the arrangement the steering-shaped and
    /// approval-shaped tests both build on. Returns the control-log path the
    /// stub records to; the tempdirs ride along because dropping them deletes
    /// the files under the child.
    async fn logged_session(
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
        (session, log_path, (log, dir))
    }

    /// A session already mid-turn: [`logged_session`], drained until output
    /// proved the turn open — the precondition every steering-shaped test
    /// shares. A mid-turn send before that point would honestly (and
    /// correctly) start a turn rather than steer one.
    pub(crate) async fn mid_turn(
        script: &str,
        steer_mode: Option<&str>,
    ) -> (
        Box<dyn AgentSession>,
        std::path::PathBuf,
        (tempfile::TempDir, tempfile::TempDir),
    ) {
        let (mut session, log_path, dirs) = logged_session(script, steer_mode).await;
        drain_until_output(&mut session).await;
        (session, log_path, dirs)
    }

    /// Bounded control-log polling, shared with the claude suite — see
    /// `support::control_log_reaches`.
    pub(crate) use crate::support::control_log_reaches as control_log_shows;

    /// The exec-approval params the re-probe recorded (transcript 20's
    /// shape), pointed at a stub item.
    pub(crate) fn exec_approval_params() -> serde_json::Value {
        json!({
            "threadId": "stub-thread",
            "turnId": "turn-1",
            "itemId": "exec-9",
            "reason": "Allow me to write probe.txt?",
            "command": "/bin/zsh -lc \"printf ESCALATED > probe.txt\"",
            "cwd": "/work",
            "commandActions": [{"type": "unknown", "command": "printf ESCALATED > probe.txt"}],
            "proposedExecpolicyAmendment": ["/bin/zsh", "-lc", "printf ESCALATED > probe.txt"],
            "availableDecisions": ["accept", "cancel"]
        })
    }

    /// One scripted turn whose exec item asks for approval and then completes
    /// as `completed_status`, exit `exit_code` — the stub parks on the marker
    /// until the session answers, exactly as codex does.
    pub(crate) fn exec_approval_script(completed_status: &str, exit_code: Option<i64>) -> String {
        let mut completed_item = json!({
            "type": "commandExecution", "id": "exec-9",
            "command": "/bin/zsh -lc \"printf ESCALATED > probe.txt\"",
            "status": completed_status,
        });
        if let Some(code) = exit_code {
            completed_item["exitCode"] = json!(code);
        }
        [
            thread_started(),
            turn_started("turn-1"),
            json!({"method":"item/started","params":{"turnId":"turn-1","item":{
                "type":"commandExecution","id":"exec-9",
                "command":"/bin/zsh -lc \"printf ESCALATED > probe.txt\"",
                "status":"inProgress"}}})
            .to_string(),
            format!("!EXEC_APPROVAL {}", exec_approval_params()),
            json!({"method":"item/completed",
                   "params":{"turnId":"turn-1","item": completed_item}})
            .to_string(),
            turn_completed("turn-1"),
        ]
        .join("\n")
    }

    /// A session on `script` with a control log, its opening message already
    /// sent — the arrangement every approval-shaped test shares. Exactly
    /// [`logged_session`] without a steer mode; named for what the callers
    /// mean rather than for what the plumbing does.
    pub(crate) async fn approval_session(
        script: &str,
    ) -> (
        Box<dyn AgentSession>,
        std::path::PathBuf,
        (tempfile::TempDir, tempfile::TempDir),
    ) {
        logged_session(script, None).await
    }

    /// Drains to the permission request, under the ceiling.
    pub(crate) async fn surfaced_request(
        session: &mut Box<dyn AgentSession>,
    ) -> horde_adapter::ProposedToolCall {
        within("the request surfaces", async {
            loop {
                match session.next_event().await {
                    Ok(Some(AgentEvent::PermissionRequested { request, .. })) => {
                        return Some(request);
                    }
                    Ok(Some(_)) => continue,
                    Ok(None) | Err(_) => return None,
                }
            }
        })
        .await
        .expect("the exec approval must surface as a permission request")
    }
}

use harness::{
    adapter_for, basic_turn, control_log_shows, events_through_turn_end, exec_approval_params,
    exec_approval_script, mid_turn, session_on, surfaced_request, within,
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

    let (mut session, log_path, _dirs) = harness::approval_session(&script).await;

    let request = surfaced_request(&mut session).await;
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

/// The boundary of `is_adjudicable_method`, held over a real process: a
/// server request on a method outside the chain's two seams takes the refusal
/// path, never adjudication. The two paths are observably different — the
/// refusal is a JSON-RPC method-not-found error naming the method, while a
/// request wrongly fed to the adjudicator would come back as a `decision`
/// decline (its empty feed falls back to that vocabulary) — so a guard that
/// quietly widened to everything changes the bytes on the wire.
#[tokio::test]
async fn a_non_adjudicable_server_request_is_refused_not_adjudicated() {
    let script = [
        harness::thread_started(),
        harness::turn_started("turn-1"),
        json!({"id": 900, "method": "attestation/generate", "params": {}}).to_string(),
        harness::final_answer("turn-1", "i1", "done"),
        harness::turn_completed("turn-1"),
    ]
    .join("\n");

    let (mut session, log_path, _dirs) = harness::approval_session(&script).await;
    let (_, outcome) = events_through_turn_end(&mut session).await;
    assert!(
        outcome.is_some(),
        "a stray server request must not derail the turn"
    );

    // The refusal names the method inside the error reply — the only line in
    // this exchange that carries the method back toward the stub.
    let recorded = control_log_shows(&log_path, |log| log.contains("attestation/generate")).await;
    assert!(
        recorded.contains("-32601"),
        "the refusal is a method-not-found error: {recorded}"
    );
    assert!(
        !recorded.contains("\"decision\""),
        "adjudication's decline vocabulary must never answer an unknown \
         method: {recorded}"
    );
    session.shutdown().await.expect("shuts down");
}

/// The exec-approval flow end to end over a real process: the request
/// surfaces with the command and cwd the chain's rules match on, the allow
/// reaches the child as `{"decision":"accept"}` — never the elicitation
/// `action` — and the turn finishes clean.
#[tokio::test]
async fn an_exec_approval_surfaces_and_the_accept_reaches_the_wire() {
    let (mut session, log_path, _dirs) =
        harness::approval_session(&exec_approval_script("completed", Some(0))).await;

    let request = surfaced_request(&mut session).await;
    assert_eq!(request.tool_name, "exec");
    assert_eq!(request.tool_call_id.as_str(), "exec-9");
    assert_eq!(request.input["command"], "printf ESCALATED > probe.txt");
    assert_eq!(request.input["cwd"], "/work");

    session
        .answer_permission(&request.tool_call_id, PermissionDecision::allow())
        .await
        .expect("the answer is writable");

    let recorded =
        control_log_shows(&log_path, |log| log.contains("\"decision\":\"accept\"")).await;
    assert!(
        !recorded.contains("\"action\""),
        "the elicitation vocabulary would deserialize to nothing here: {recorded}"
    );

    let (events, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("the turn finishes after the allow");
    assert!(outcome.denials.is_empty(), "an allow is not a refusal");
    assert!(
        events.iter().any(|event| matches!(
            event,
            AgentEvent::Output { text, .. }
                if text.contains("chain allow") && text.contains("accept")
        )),
        "the decision is journaled with its codex encoding: {events:?}"
    );
    session.shutdown().await.expect("shuts down");
}

/// The decline path: the chain's deny reaches the child as
/// `{"decision":"decline"}` and the refusal is the turn's denial record.
#[tokio::test]
async fn a_denied_exec_approval_declines_on_the_wire_and_records_the_denial() {
    let (mut session, log_path, _dirs) =
        harness::approval_session(&exec_approval_script("declined", None)).await;

    let request = surfaced_request(&mut session).await;
    session
        .answer_permission(
            &request.tool_call_id,
            PermissionDecision::deny("not in this mode"),
        )
        .await
        .expect("the answer is writable");

    let recorded =
        control_log_shows(&log_path, |log| log.contains("\"decision\":\"decline\"")).await;
    assert!(recorded.contains("\"decision\":\"decline\""), "{recorded}");

    let (_, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("the turn finishes after the decline");
    assert_eq!(outcome.denials.len(), 1, "the refusal is the audit record");
    assert_eq!(outcome.denials[0].tool_name, "exec");
    assert!(!outcome.is_clean_success());
    session.shutdown().await.expect("shuts down");
}

/// A remembering allow reaches the child as `acceptForSession` — the
/// session-scoped grant in codex's own vocabulary, mirroring the Server's
/// remembered-allow layer.
#[tokio::test]
async fn a_remembering_allow_becomes_accept_for_session_on_the_wire() {
    let (mut session, log_path, _dirs) =
        harness::approval_session(&exec_approval_script("completed", Some(0))).await;

    let request = surfaced_request(&mut session).await;
    session
        .answer_permission(
            &request.tool_call_id,
            PermissionDecision::allow().remembered(),
        )
        .await
        .expect("the answer is writable");

    let recorded = control_log_shows(&log_path, |log| {
        log.contains("\"decision\":\"acceptForSession\"")
    })
    .await;
    assert!(
        recorded.contains("\"decision\":\"acceptForSession\""),
        "{recorded}"
    );

    let (_, outcome) = events_through_turn_end(&mut session).await;
    assert!(outcome.is_some(), "the turn finishes after the grant");
    session.shutdown().await.expect("shuts down");
}

/// An interrupt that finds an approval parked answers it with `cancel` —
/// before the `turn/interrupt` — because the dying turn does not release the
/// harness from waiting, and the withdrawn question is not a denial.
#[tokio::test]
async fn an_interrupt_cancels_a_parked_exec_approval_before_the_turn_dies() {
    // Block 1 parks on the approval; block 2 — played on the interrupt — is
    // the idle that closes the cancelled turn (transcript 11's shape).
    let script = [
        [
            harness::thread_started(),
            harness::turn_started("turn-1"),
            format!("!EXEC_APPROVAL {}", exec_approval_params()),
        ]
        .join("\n"),
        harness::idle(),
    ]
    .join("\n\n");

    let (mut session, log_path, _dirs) = harness::approval_session(&script).await;
    let _request = surfaced_request(&mut session).await;

    session.interrupt().await.expect("the cancel is writable");

    let recorded = control_log_shows(&log_path, |log| log.contains("turn/interrupt")).await;
    let cancel_at = recorded.find("\"decision\":\"cancel\"");
    let interrupt_at = recorded.find("turn/interrupt");
    match (cancel_at, interrupt_at) {
        (Some(cancel), Some(interrupt)) => assert!(
            cancel < interrupt,
            "the parked approval is answered before the turn is torn down: {recorded}"
        ),
        other => panic!("both frames must reach the wire, got {other:?}: {recorded}"),
    }

    let (_, outcome) = events_through_turn_end(&mut session).await;
    let outcome = outcome.expect("idle closes the interrupted turn");
    assert_eq!(outcome.status, TurnStatus::Interrupted);
    assert!(
        outcome.denials.is_empty(),
        "a withdrawal is not a chain refusal: {:?}",
        outcome.denials
    );
    session.shutdown().await.expect("shuts down");
}

/// A narrowing decision must be refused loudly — neither reply vocabulary can
/// carry replacement arguments, and a narrowing that silently widened into a
/// plain allow is the failure the capability advertisement exists to prevent.
/// The refusal must also leave the request outstanding: a consumed-but-
/// unanswered approval would park the harness forever.
#[tokio::test]
async fn a_narrowing_decision_is_refused_not_widened_and_stays_answerable() {
    let (mut session, _log_path, _dirs) =
        harness::approval_session(&exec_approval_script("declined", None)).await;
    let request = surfaced_request(&mut session).await;

    // A real rewrite: the chain narrowed to a different command, which the
    // decision vocabulary cannot express.
    let refused = session
        .answer_permission(
            &request.tool_call_id,
            PermissionDecision::allow_with(json!({"command": "true", "cwd": "/work"})),
        )
        .await;
    assert!(
        matches!(refused, Err(AdapterError::Unsupported { .. })),
        "got {refused:?}"
    );

    // The refusal consumed nothing: the same request still takes an
    // expressible answer, and the turn completes on it.
    session
        .answer_permission(
            &request.tool_call_id,
            PermissionDecision::deny("narrowing could not apply"),
        )
        .await
        .expect("the request must still be answerable after the refusal");
    let (_, outcome) = events_through_turn_end(&mut session).await;
    assert!(outcome.is_some(), "the turn ends once a real answer landed");

    // And an answer for a call nobody proposed is an error, never a no-op.
    let stray = session
        .answer_permission(&ToolCallId::new("exec-nobody"), PermissionDecision::allow())
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

/// The full permission loop, every hop real: the daemon binary spawns the
/// codex-shaped stub, the stub asks an exec approval, the worker journals a
/// `PermissionRequested` that rides to the server log, the *actual*
/// `ChainDriver` rules on the command prefix, the decision commands its way
/// back to the daemon, the encoded `decision` reaches the stub's stdin, and
/// the outcome lands in the server log.
#[cfg(test)]
mod full_loop {
    use std::sync::Arc;

    use horde_proto::v1::{PermissionLayer, PermissionOutcome, event::Kind};
    use horde_server::EventLog;
    use horde_server::permission::{PermissionMode, PermissionModes, Rule};
    use horde_server::service::ServerCore;
    use horde_server_testkit::process::SpawnedProcess;

    use crate::support;

    /// A directory whose `codex` is the stub binary, for the daemon's `PATH`.
    fn stub_on_path() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("a scratch dir");
        std::os::unix::fs::symlink(super::harness::stub_binary(), dir.path().join("codex"))
            .expect("the stub links as `codex`");
        let path = format!(
            "{}:{}",
            dir.path().display(),
            std::env::var("PATH").unwrap_or_default()
        );
        (dir, path)
    }

    /// Bounded wait for a session-log predicate, notification-driven.
    async fn log_satisfying(
        log: &EventLog,
        what: &str,
        holds: impl Fn(&[horde_proto::v1::Event]) -> bool,
    ) -> Vec<horde_proto::v1::Event> {
        let mut notifications = log.subscribe();
        for _ in 0_u32..256 {
            let events = support::decoded_session_log(log, "s-cx").await;
            if holds(&events) {
                return events;
            }
            let _ = tokio::time::timeout(core::time::Duration::from_secs(5), notifications.recv())
                .await
                .unwrap_or_else(|_| panic!("{what}: the server log stalled"));
        }
        panic!("{what}: never appeared in the session log");
    }

    /// One whole loop under `modes`, returning the session events once the
    /// chain's decision and the turn's ending are both in the log, plus the
    /// stub's control log for the wire-level assertion.
    async fn run_loop(
        modes: PermissionModes,
        script: &str,
        decided: impl Fn(&[horde_proto::v1::Event]) -> bool,
    ) -> (Vec<horde_proto::v1::Event>, String) {
        let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
        let endpoint = support::serve(core.clone()).await;
        // The real chain, over the real log — the seam under test.
        let chain = horde_server::permission::spawn(&core.log, Arc::new(modes), None);
        support::place_agent(
            &core.log,
            "s-cx",
            "agent-cx",
            "w-cx",
            "write the probe file",
        )
        .await;

        let state = tempfile::tempdir().expect("a temp dir");
        let script_file = state.path().join("stub-script.jsonl");
        std::fs::write(&script_file, script).expect("the script writes");
        let control_log = state.path().join("control.log");
        let (_stub_dir, path) = stub_on_path();

        // First-exec admission before the watched spawn, so a fresh tree's
        // validation never lands inside this scenario's bounded waits.
        let daemon_binary = std::path::Path::new(env!("CARGO_BIN_EXE_horde-worker"));
        support::admit(daemon_binary, &["--help"]);
        let mut daemon = SpawnedProcess::spawn(
            "horde-worker",
            daemon_binary,
            &[
                "--insecure",
                "--server",
                &endpoint,
                "--name",
                "w-cx",
                "--adapter",
                "codex",
                "--state-dir",
                &state.path().to_string_lossy(),
            ],
            &[
                ("PATH", path.as_str()),
                ("HORDE_CODEX_STUB_SCRIPT", &script_file.to_string_lossy()),
                (
                    "HORDE_CODEX_STUB_CONTROL_LOG",
                    &control_log.to_string_lossy(),
                ),
            ],
        )
        .expect("the daemon spawns");
        daemon
            .await_stdout("the registration line", |out| {
                out.contains("registered worker=w-cx")
            })
            .await
            .expect("the plaintext dial registers");

        let events = log_satisfying(
            &core.log,
            "the chain's decision and the outcome",
            |events| decided(events),
        )
        .await;

        daemon.terminate().await.expect("SIGTERM sends");
        let _ = daemon.wait().await;
        chain.abort();

        let recorded = std::fs::read_to_string(&control_log).unwrap_or_default();
        (events, recorded)
    }

    fn requested_in(
        events: &[horde_proto::v1::Event],
    ) -> Option<&horde_proto::v1::ProposedToolCall> {
        events.iter().find_map(|event| match &event.kind {
            Some(Kind::PermissionRequested(requested)) => requested
                .request
                .as_ref()
                .and_then(|r| r.tool_call.as_ref()),
            _ => None,
        })
    }

    fn decision_in(
        events: &[horde_proto::v1::Event],
    ) -> Option<&horde_proto::v1::PermissionDecision> {
        events.iter().find_map(|event| match &event.kind {
            Some(Kind::PermissionDecided(decided)) => decided.decision.as_ref(),
            _ => None,
        })
    }

    fn turn_ended(events: &[horde_proto::v1::Event]) -> bool {
        events.iter().any(|event| {
            matches!(&event.kind, Some(Kind::UsageRecorded(usage))
                if usage.record.as_ref().is_some_and(|r| !r.turn_id.is_empty()))
        })
    }

    /// The deterministic-deny layer refuses the command by prefix, and the
    /// refusal reaches the stub as `{"decision":"decline"}`.
    #[tokio::test]
    async fn a_deny_rule_blocks_the_exec_command_end_to_end() {
        let modes =
            PermissionModes::default().with_default(PermissionMode::escalate_everything().deny(
                Rule::tool("exec").where_input_starts_with("command", "printf"),
                "probe writes are refused in this mode",
            ));
        let (events, recorded) = run_loop(
            modes,
            &super::harness::exec_approval_script("declined", None),
            |events| decision_in(events).is_some() && turn_ended(events),
        )
        .await;

        let requested = requested_in(&events).expect("the escalation reached the server log");
        assert_eq!(requested.tool_name, "exec");
        assert!(
            requested.rendered.contains("printf"),
            "the approver-facing rendering names the command: {}",
            requested.rendered
        );

        let decision = decision_in(&events).expect("the chain decided");
        assert_eq!(decision.outcome, i32::from(PermissionOutcome::Deny));
        assert_eq!(
            decision.decided_by,
            i32::from(PermissionLayer::DeterministicDeny),
            "the rule decided — no judge, no human"
        );

        assert!(
            recorded.contains("\"decision\":\"decline\""),
            "the deny must reach the stub in the decision vocabulary: {recorded}"
        );
        assert!(
            !recorded.contains("\"decision\":\"accept\""),
            "and nothing may have accepted it: {recorded}"
        );
    }

    /// The deterministic-allow layer passes the command by prefix, and the
    /// grant reaches the stub as `{"decision":"accept"}`.
    #[tokio::test]
    async fn an_allow_rule_passes_the_exec_command_end_to_end() {
        let modes = PermissionModes::default().with_default(
            PermissionMode::escalate_everything()
                .allow(Rule::tool("exec").where_input_starts_with("command", "printf")),
        );
        let (events, recorded) = run_loop(
            modes,
            &super::harness::exec_approval_script("completed", Some(0)),
            |events| decision_in(events).is_some() && turn_ended(events),
        )
        .await;

        let decision = decision_in(&events).expect("the chain decided");
        assert_eq!(decision.outcome, i32::from(PermissionOutcome::Allow));
        assert_eq!(
            decision.decided_by,
            i32::from(PermissionLayer::DeterministicAllow)
        );

        assert!(
            recorded.contains("\"decision\":\"accept\""),
            "the allow must reach the stub in the decision vocabulary: {recorded}"
        );
    }
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
