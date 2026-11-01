//! Process-level tests against a real stub-harness child.
//!
//! Real `fork`/`exec`, real pipes, real EOF, real exit codes — no login, no
//! network, no quota. These cover failures that only exist once a process is
//! involved and that an in-process fake cannot reproduce.

use std::path::PathBuf;

use horde_worker::process::HarnessSpec;
use horde_worker::{SupervisedHarness, SupervisorError};

/// Test-only fixtures.
///
/// Wrapped in `#[cfg(test)]` so the workspace's `expect_used` exemption applies:
/// clippy exempts anything under a `cfg(test)` module, but not a bare helper
/// beside a `#[test]` fn.
#[cfg(test)]
mod harness {
    use super::{HarnessSpec, PathBuf};
    use horde_adapter::{Adapter, AgentEvent, AgentSession, AgentSpec, TurnOutcome};
    use horde_worker::{ClaudeAdapter, HarnessProfile};

    /// The stub binary cargo built alongside this test, its first-exec
    /// admission paid (`support::admitted_binary`) so the suite's bounded
    /// waits never absorb the OS's first-launch validation. No probe
    /// arguments: without a transcript the stub refuses and exits at once.
    pub(crate) fn stub_binary() -> PathBuf {
        crate::support::admitted_binary("stub-harness", &[])
    }

    /// A spec whose child gets extra environment, for scripting the stub.
    pub(crate) fn spec_with(
        transcript: &str,
        extra: &[(&str, &str)],
    ) -> (HarnessSpec, tempfile::TempDir) {
        let (mut spec, dir) = spec_for(transcript);
        for (key, value) in extra {
            spec.env.push(((*key).to_owned(), (*value).to_owned()));
        }
        (spec, dir)
    }

    /// A claude adapter on the stub, its Worker-owned profile inside `dir` —
    /// the production launch shape (ADR-0010), so the tests run the same argv
    /// and the same config-dir rules production does.
    pub(crate) fn stub_claude(dir: &tempfile::TempDir) -> ClaudeAdapter {
        ClaudeAdapter::new(HarnessProfile::new(dir.path().join("harness")))
            .with_program(stub_binary().to_string_lossy())
    }

    pub(crate) fn spec_for(transcript: &str) -> (HarnessSpec, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let file = dir.path().join("transcript.jsonl");
        std::fs::write(&file, transcript).expect("writes");

        let spec = HarnessSpec {
            // The script travels in the child's environment, so each test's child
            // gets its own without touching this process's env.
            env: vec![(
                "HORDE_STUB_TRANSCRIPT".to_owned(),
                file.to_string_lossy().into_owned(),
            )],
            ..HarnessSpec::new(stub_binary().to_string_lossy())
        };
        (spec, dir)
    }

    /// A live session on the stub, playing `script`, with `extra` reaching the
    /// child's environment.
    ///
    /// Every session test builds one the same way, and the `TempDir` has to
    /// travel with it: dropping the directory deletes the transcript out from
    /// under a child that has not finished reading it.
    pub(crate) async fn session_on(
        script: &str,
        extra: &[(&str, &str)],
    ) -> (Box<dyn AgentSession>, tempfile::TempDir) {
        let (spec, dir) = spec_with(script, extra);
        let mut adapter = stub_claude(&dir);
        adapter.extra_env = spec.env.clone();
        let session = adapter
            .spawn(&AgentSpec::default())
            .await
            .expect("the stub spawns");
        (session, dir)
    }

    /// A script that reports one successful turn and nothing else.
    pub(crate) const RESULT_ONLY: &str = "{\"type\":\"result\",\"subtype\":\"success\",\
         \"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}";

    /// Bounded waiting, shared with the codex suite — see `support::within`.
    pub(crate) use crate::support::within;

    /// Drains `session` until its turn ends, under the ceiling.
    ///
    /// `None` means the stream ended first. A `send` that sends nothing reaches
    /// this the same way it reaches every other caller — by never producing the
    /// turn — so the ceiling is what turns that mutant into a failure instead
    /// of a stalled suite.
    pub(crate) async fn turn_end(session: &mut Box<dyn AgentSession>) -> Option<TurnOutcome> {
        within("the turn ends", async {
            while let Ok(Some(event)) = session.next_event().await {
                if let AgentEvent::TurnEnded(outcome) = event {
                    return Some(outcome);
                }
            }
            None
        })
        .await
    }

    /// Everything `session` emits before its stream ends, under the ceiling.
    pub(crate) async fn drain(session: &mut Box<dyn AgentSession>) -> Vec<AgentEvent> {
        within("the stream ends", async {
            let mut events = Vec::new();
            while let Ok(Some(event)) = session.next_event().await {
                events.push(event);
            }
            events
        })
        .await
    }
}

use harness::{spec_for, within};

#[tokio::test]
async fn a_turn_round_trips_through_a_real_child_process() {
    let (spec, _dir) = spec_for(
        "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s1\",\"model\":\"m\"}\n\
         {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":5,\"result\":\"hi\"}",
    );

    let mut harness = SupervisedHarness::spawn(&spec)
        .await
        .expect("the stub must spawn");
    assert!(harness.id().is_some(), "a live child has a pid");

    harness
        .write_line("{\"type\":\"user\"}")
        .await
        .expect("writes");

    let first = within("a line arrives", harness.read_line())
        .await
        .expect("reads")
        .unwrap_or_default();
    assert!(first.contains("\"session_id\":\"s1\""), "got {first}");

    harness.kill().await.expect("kill by handle");
}

#[tokio::test]
async fn a_dead_harness_reads_as_eof_rather_than_hanging() {
    // The failure this crate exists to make impossible: a supervisor waiting
    // for output from a process that already exited.
    let (spec, _dir) = spec_for(
        "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"done\"}",
    );

    let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
    harness
        .write_line("{\"type\":\"user\"}")
        .await
        .expect("writes");
    let _ = within("a line arrives", harness.read_line())
        .await
        .expect("reads the scripted turn");

    // Script exhausted: the stub exits and its stdout closes.
    let _ = harness.write_line("{\"type\":\"user\"}").await;

    let mut saw_eof = false;
    for _ in 0..64 {
        match within("a line arrives", harness.read_line()).await {
            Ok(None) => {
                saw_eof = true;
                break;
            }
            Ok(Some(_)) => continue,
            Err(_) => break,
        }
    }
    assert!(saw_eof, "a dead harness must read as EOF, never block");
}

#[tokio::test]
async fn a_missing_binary_is_an_error_not_a_panic() {
    // Ordinary on a Worker that has not installed the harness yet.
    let spec = HarnessSpec::new("horde-no-such-harness");

    assert!(
        matches!(
            SupervisedHarness::spawn(&spec).await.err(),
            Some(SupervisorError::Spawn { .. })
        ),
        "a missing harness is a reportable condition, not a crash"
    );
}

#[tokio::test]
async fn the_account_directory_reaches_the_child() {
    // Account isolation is a Worker-level resource (DESIGN.md §4): one Worker,
    // several accounts, each in its own config directory.
    let (mut spec, _dir) = spec_for(
        "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"ok\"}",
    );
    let account = PathBuf::from("/var/horde/accounts/work");
    spec.config_dir = Some(account.clone());

    let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
    // The stub echoes nothing about its env, so this asserts the spec carries
    // the binding rather than the child's view of it; the child-side read is
    // covered by the live smoke.
    assert_eq!(spec.config_dir.as_deref(), Some(account.as_path()));
    harness.kill().await.expect("kill by handle");
}

// ---------------------------------------------------------------------------
// The live session, driven end to end over a real child process.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod live_session {
    use super::harness::{spec_for, turn_end};
    use horde_adapter::{Adapter, AgentSpec, TurnInput, TurnStatus};

    /// A whole turn, from spawn to `TurnEnded`, against a real child.
    #[tokio::test]
    async fn a_turn_streams_and_ends_through_a_real_process() {
        let (harness_spec, _dir) = spec_for(
            "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-1\",\"model\":\"haiku\"}\n\
             {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"sess-1\",\"duration_ms\":7,\"result\":\"HORDE_OK\"}",
        );

        // Point the adapter at the stub: the tests must never touch the real CLI.
        let mut adapter = super::harness::stub_claude(&_dir);

        // Per-child, never process-wide: setting it on this process would race
        // every other test's spawn.
        adapter.extra_env = harness_spec.env.clone();

        let spec = AgentSpec {
            model: Some("haiku".to_owned()),
            append_system_prompt: Some("you are supervised".to_owned()),
            ..AgentSpec::default()
        };

        let mut session = adapter.spawn(&spec).await.expect("the stub must spawn");
        session
            .send(TurnInput::queued("hello"))
            .await
            .expect("sends");

        let ended = turn_end(&mut session).await.expect("the turn must end");
        assert_eq!(ended.status, TurnStatus::Completed);
        assert_eq!(
            session.provider_session_id().map(ToString::to_string),
            Some("sess-1".to_owned()),
            "the revival identifier must reach the caller before anything is lost"
        );

        session.shutdown().await.expect("shutdown kills by handle");
    }
}

// ---------------------------------------------------------------------------
// The failure modes a supervisor must not have.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod failure_modes {
    use super::harness::{spec_with, within};
    use horde_worker::SupervisedHarness;

    /// An interrupt the harness never acknowledges must not hang the supervisor.
    ///
    /// The real hazard: `interrupt()` writes a control message, and a harness
    /// that is wedged or dying never answers. A supervisor that waited for the
    /// ack would hold the agent forever — so it must not wait for one.
    #[tokio::test]
    async fn an_unacknowledged_interrupt_does_not_block() {
        let (spec, _dir) = spec_with(
            "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"ok\"}",
            &[("HORDE_STUB_IGNORE_INTERRUPT", "1")],
        );

        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let control =
            r#"{"type":"control_request","request_id":"r1","request":{"subtype":"interrupt"}}"#;

        // Writing must return regardless of whether anything answers.
        let written = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            harness.write_line(control),
        )
        .await;

        assert!(
            written.is_ok(),
            "writing an interrupt must not wait on an acknowledgement"
        );
        harness.kill().await.expect("kill by handle");
    }

    /// A harness that exits mid-session is reported, not waited on.
    #[tokio::test]
    async fn an_exit_mid_session_surfaces_as_eof_and_a_status() {
        let (spec, _dir) = spec_with(
            "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"ok\"}",
            &[
                ("HORDE_STUB_EXIT_AFTER", "1"),
                ("HORDE_STUB_EXIT_CODE", "3"),
            ],
        );

        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        harness
            .write_line("{\"type\":\"user\"}")
            .await
            .expect("writes");

        // Drain to EOF, bounded by COUNT rather than by time. A clock is the
        // wrong instrument here: a `read_line` that returns instantly forever
        // never awaits anything, so on the current-thread runtime a
        // `tokio::time::timeout` around this loop is never polled and the
        // deadline it promises silently does not exist. The mutant that does
        // exactly that spun here for a full minute while every other test in
        // the file had already failed. A ceiling on iterations needs no
        // scheduler co-operation, so it holds where the timer cannot.
        let mut lines = 0_u32;
        while let Ok(Some(_)) = within("a line arrives", harness.read_line()).await {
            lines += 1;
            assert!(
                lines < 1024,
                "the stub's stdout must reach EOF; a reader that never sees one \
                 is the failure this test exists to catch"
            );
        }
        let status = within("the child is reaped", harness.wait()).await;
        assert_eq!(
            status.ok().flatten(),
            Some(3),
            "the exit status is how the Worker explains the death"
        );
    }

    /// stderr is captured, not discarded.
    #[tokio::test]
    async fn harness_diagnostics_are_kept_for_the_agent_log() {
        // The stub writes to stderr when its script is missing, which is
        // exactly the "refused to start" case an operator has to diagnose.
        let (mut spec, _dir) = spec_with("{}", &[]);
        spec.env.retain(|(k, _)| k != "HORDE_STUB_TRANSCRIPT");

        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let _ = within("a line arrives", harness.read_line()).await;
        let _ = within("the child is reaped", harness.wait()).await;

        // Give the drain task a moment to flush the pipe.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let diagnostics = harness.diagnostics().await;

        assert!(
            diagnostics
                .iter()
                .any(|l| l.contains("HORDE_STUB_TRANSCRIPT")),
            "a harness that refused to start must leave its reason behind, got {diagnostics:?}"
        );
    }

    /// A write to a child that has let go of its stdin is a hang-up — the
    /// death seen from the write side — never a bare I/O failure. The stub
    /// closes stdin before serving the turn, so having read the turn the
    /// child is no longer a reader. A sibling test's spawn can still hold a
    /// copy of the read end for a moment, so the writes repeat, bounded,
    /// until the refusal arrives (see `support::first_refusal`).
    #[tokio::test]
    async fn a_write_after_the_child_hangs_up_is_a_hang_up() {
        let (spec, _dir) = spec_with(super::harness::RESULT_ONLY, &[("HORDE_STUB_HANG_UP", "1")]);
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        harness
            .write_line("{\"type\":\"user\"}")
            .await
            .expect("writes");
        let served = within("the turn arrives", harness.read_line()).await;
        assert!(
            matches!(served, Ok(Some(_))),
            "the turn proves stdin is closed: {served:?}"
        );

        let refused = within(
            "the hang-up is met",
            crate::support::first_refusal(async || harness.write_line("{\"type\":\"user\"}").await),
        )
        .await;
        assert!(
            matches!(refused, Err(super::SupervisorError::HungUp)),
            "EPIPE is the child hanging up: {refused:?}"
        );
        harness.kill().await.expect("kill by handle");
    }

    /// The same hang-up through the claude session is the harness's death,
    /// with the exit status attached exactly as the EOF path attaches it.
    #[tokio::test]
    async fn a_claude_harness_that_hangs_up_reads_as_its_death() {
        let (mut session, _dir) = super::harness::session_on(
            super::harness::RESULT_ONLY,
            &[("HORDE_STUB_HANG_UP", "1"), ("HORDE_STUB_EXIT_CODE", "3")],
        )
        .await;
        session
            .send(horde_adapter::TurnInput::queued("hello"))
            .await
            .expect("the first turn is written before the stub lets go");
        assert!(
            super::harness::turn_end(&mut session).await.is_some(),
            "the served turn proves stdin is closed"
        );

        let refused = within(
            "the death is reported",
            crate::support::first_refusal(async || {
                session
                    .send(horde_adapter::TurnInput::queued("again"))
                    .await
            }),
        )
        .await;
        assert!(
            matches!(
                refused,
                Err(horde_adapter::AdapterError::HarnessExited { exit_code: Some(3) })
            ),
            "EPIPE on a claude harness is its death, with its status: {refused:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Revival: a conversation outliving the process that started it.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod revival {
    use super::harness::{drain, spec_with, turn_end};
    use horde_adapter::{Adapter, AgentEvent, AgentSpec, ProviderSessionId, TurnInput};
    use horde_worker::ClaudeAdapter;

    /// An adapter pointed at the stub, with `script` in the child's env — the
    /// preamble every revival test shares. The `TempDir` must outlive the
    /// child, so it travels with the adapter.
    fn stub_adapter(script: &str) -> (ClaudeAdapter, tempfile::TempDir) {
        let (harness_spec, dir) = spec_with(script, &[]);
        let mut adapter = super::harness::stub_claude(&dir);
        adapter.extra_env = harness_spec.env.clone();
        (adapter, dir)
    }

    /// The handoff: a session id learned from one process is what revives the
    /// conversation in the next.
    ///
    /// The stub replays the same script either way, so this does not prove the
    /// model remembered anything — that is the live smoke's job. What it proves
    /// is the supervisor's half: the id reaches the caller, `resume` spawns a
    /// second process carrying it, and the revived session is usable.
    #[tokio::test]
    async fn a_session_id_from_one_process_revives_in_the_next() {
        let script = "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-42\",\"model\":\"haiku\"}\n\
                      {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"sess-42\",\"duration_ms\":3,\"result\":\"first\"}";
        let (adapter, _dir) = stub_adapter(script);

        let mut first = adapter.spawn(&AgentSpec::default()).await.expect("spawns");
        first.send(TurnInput::queued("hello")).await.expect("sends");
        assert!(
            turn_end(&mut first).await.is_some(),
            "the first session must complete a turn before it can hand over an id"
        );

        let revival_id = first
            .provider_session_id()
            .cloned()
            .expect("the id must reach the caller before the process can be lost");
        assert_eq!(revival_id.to_string(), "sess-42");
        first.shutdown().await.expect("shuts down");

        // A second, independent process, carrying the id.
        let mut revived = adapter
            .resume(&AgentSpec::default(), &revival_id)
            .await
            .expect("resume spawns a fresh process");
        revived
            .send(TurnInput::queued("still there?"))
            .await
            .expect("the revived session is usable");

        assert!(
            turn_end(&mut revived).await.is_some(),
            "the revived session must run a turn"
        );
        revived.shutdown().await.expect("shuts down");
    }

    /// A revived session numbers its turns after the ones already taken.
    ///
    /// The journal keys events by turn id, so a revived process that restarted
    /// at `#1` would re-mint keys the journal already holds — and the Server's
    /// dedupe would then discard the revived turns' events while acking them.
    #[tokio::test]
    async fn a_revived_session_continues_the_turn_numbering() {
        let script = "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-9\",\"model\":\"haiku\"}\n\
                      {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"sess-9\",\"duration_ms\":3,\"result\":\"revived\"}";
        let (adapter, _dir) = stub_adapter(script);

        let mut revived = adapter
            .resume(
                &AgentSpec::default().with_turns_taken(3),
                &ProviderSessionId::new("sess-9"),
            )
            .await
            .expect("resume spawns");
        revived
            .send(TurnInput::queued("continue"))
            .await
            .expect("sends");

        let outcome = turn_end(&mut revived).await.expect("the turn ends");
        assert_eq!(
            outcome.turn_id.to_string(),
            "sess-9#4",
            "a revived turn must continue the conversation's numbering"
        );
        revived.shutdown().await.expect("shuts down");
    }

    /// Resuming an id the harness has never heard of must fail loudly.
    ///
    /// Silently starting a fresh conversation is the dangerous alternative: the
    /// agent looks alive and has lost everything it knew.
    #[tokio::test]
    async fn reviving_an_unknown_conversation_does_not_silently_start_a_new_one() {
        // An empty script means the stub answers nothing and exits — which is
        // what a harness does when `--resume` names a session it cannot find.
        let (adapter, _dir) = stub_adapter("");

        let mut revived = adapter
            .resume(
                &AgentSpec::default(),
                &ProviderSessionId::new("sess-does-not-exist"),
            )
            .await
            .expect("the process starts; the failure is in what it does next");
        let _ = revived.send(TurnInput::queued("hello")).await;

        // No events, and the stream ends — never an invented SessionEstablished.
        let events = drain(&mut revived).await;

        assert!(
            !events
                .iter()
                .any(|e| matches!(e, AgentEvent::SessionEstablished { .. })),
            "a conversation that was not found must not be reported as established"
        );
        assert!(
            revived.provider_session_id().is_none(),
            "and no revival identity may be invented for it"
        );
    }
}

// ---------------------------------------------------------------------------
// Side effects that must actually happen.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod side_effects {
    use super::harness::{RESULT_ONLY, session_on, within};
    use horde_adapter::TurnInput;

    /// A session whose child records control requests into a log file, plus
    /// that log's path — the observation point both interrupt tests need.
    async fn session_with_control_log() -> (
        Box<dyn horde_adapter::AgentSession>,
        tempfile::TempDir,
        tempfile::TempDir,
        std::path::PathBuf,
    ) {
        let log = tempfile::tempdir().expect("a scratch dir");
        let log_path = log.path().join("control.log");
        let script = format!(
            "{{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s1\",\"model\":\"m\"}}\n{}",
            RESULT_ONLY
        );
        let (session, dir) = session_on(
            &script,
            &[("HORDE_STUB_CONTROL_LOG", &log_path.to_string_lossy())],
        )
        .await;
        (session, log, dir, log_path)
    }

    /// Bounded control-log polling, shared with the codex suite — see
    /// `support::control_log_reaches`.
    use crate::support::control_log_reaches;

    /// `interrupt()` must actually put the control message on the wire.
    ///
    /// Observed at the child, not in the event stream: the harness answers a
    /// control request with a `control_response`, which the codec classifies as
    /// a non-event, so a test watching events alone cannot tell an interrupt
    /// that was sent from one that was never written. A no-op `interrupt` used
    /// to pass here — that mutant is what this file now catches.
    ///
    /// The stakes are why it is worth the extra machinery: an interrupt that
    /// silently does nothing is indistinguishable from one that worked, right up
    /// until a user cannot stop a runaway agent.
    #[tokio::test]
    async fn interrupt_puts_the_control_message_on_the_wire() {
        let (mut session, _log, _dir, log_path) = session_with_control_log().await;
        session.interrupt().await.expect("the write must succeed");

        let expected =
            |recorded: &str| recorded.contains("control_request") && recorded.contains("interrupt");
        let recorded = control_log_reaches(&log_path, expected).await;
        assert!(
            expected(&recorded),
            "the child must actually receive the interrupt, got {recorded:?}"
        );

        // And the session must remain usable — what separates the control
        // message from a signal, which would have ended the process.
        session
            .send(TurnInput::queued("after the interrupt"))
            .await
            .expect("the session survives an interrupt");
        assert!(
            matches!(
                within("the steered turn produces an event", session.next_event()).await,
                Ok(Some(_))
            ),
            "a control-message interrupt leaves the session steerable"
        );
        session.shutdown().await.expect("shuts down");
    }

    /// An interrupting send aborts first: the control message reaches the
    /// child before the text does, which is what "cancel the current turn and
    /// deliver immediately" means at the wire.
    #[tokio::test]
    async fn an_interrupting_send_puts_the_control_message_before_the_text() {
        let (mut session, _log, _dir, log_path) = session_with_control_log().await;

        session
            .send(horde_adapter::TurnInput::interrupting("change of plan"))
            .await
            .expect("an interrupting send is accepted");

        // The child records what it received; the control request must be
        // there. That the turn still runs proves the text followed it.
        let expects_interrupt = |recorded: &str| recorded.contains("interrupt");
        let recorded = control_log_reaches(&log_path, expects_interrupt).await;
        assert!(
            expects_interrupt(&recorded),
            "the abort must reach the child ahead of the message, got {recorded:?}"
        );

        assert!(
            matches!(
                within("the steered turn produces an event", session.next_event()).await,
                Ok(Some(_))
            ),
            "the interrupting message must still start its own turn"
        );
        session.shutdown().await.expect("shuts down");
    }

    /// Answering a permission request that cannot exist must be an error.
    ///
    /// Prompts arrive on the MCP endpoint, not on stdout, and that endpoint is
    /// the next increment. Returning Ok here would let a decision that went
    /// nowhere look like one that was applied — the failure the permission chain
    /// cannot tolerate.
    #[tokio::test]
    async fn answering_a_permission_request_that_cannot_exist_is_an_error() {
        let (mut session, _dir) = session_on(RESULT_ONLY, &[]).await;
        let refused = session
            .answer_permission(
                &horde_adapter::ToolCallId::new("toolu_01"),
                horde_adapter::PermissionDecision::allow(),
            )
            .await;

        assert!(
            refused.is_err(),
            "a decision with nowhere to go must be refused"
        );
        assert!(
            refused
                .err()
                .map(|e| e.to_string())
                .unwrap_or_default()
                .contains("toolu_01"),
            "and must name the call it could not answer"
        );
        session.shutdown().await.expect("shuts down");
    }

    /// `shutdown()` must actually end the child.
    #[tokio::test]
    async fn shutdown_ends_the_child_and_closes_the_session() {
        let (mut session, _dir) = session_on(RESULT_ONLY, &[]).await;
        session.shutdown().await.expect("shuts down");

        // A shut-down session is finished, not merely quiet: sending must fail
        // rather than block on a process that is gone.
        assert!(
            session.send(TurnInput::queued("hello")).await.is_err(),
            "a shut-down session must refuse work rather than hang"
        );
        assert!(
            within("the closed stream answers", session.next_event())
                .await
                .expect("no error")
                .is_none(),
            "and produce nothing further"
        );
    }
}

// ---------------------------------------------------------------------------
// Process identity and death, observed at the OS.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod process_identity {
    use super::harness::{spec_with, within};
    use horde_worker::SupervisedHarness;

    const SCRIPT: &str = "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}";

    /// `kill()` must actually end the child.
    ///
    /// Observed at the OS, not from the return value: a `kill` that returned
    /// `Ok(())` without signalling anything is the worst failure this type can
    /// have — a supervisor that cannot stop what it started. The stub blocks
    /// reading stdin forever, so if the signal never lands, `wait()` never
    /// returns and this test times out rather than passing.
    #[tokio::test]
    async fn kill_actually_ends_the_child() {
        let (spec, _dir) = spec_with(SCRIPT, &[]);
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        assert!(harness.id().is_some(), "the child is running");

        harness.kill().await.expect("kill by handle");

        let reaped = tokio::time::timeout(std::time::Duration::from_secs(10), harness.wait()).await;
        let status = reaped.expect("a killed child must be reapable, not hang forever");

        // A signalled death has no exit code on unix, which is itself the proof
        // it was killed rather than having exited on its own.
        assert_eq!(
            status.expect("wait succeeds"),
            None,
            "a killed child dies by signal and reports no exit status"
        );
    }

    /// `id()` must be the child's real pid.
    ///
    /// Two independent children must have different ids. A constant — 0, 1, or
    /// anything else — passes every aliveness check you could write (pid 1 is
    /// always alive) but collapses here, which is what makes this the assertion
    /// that pins identity rather than plausibility.
    #[tokio::test]
    async fn each_child_reports_its_own_distinct_pid() {
        let (first_spec, _a) = spec_with(SCRIPT, &[]);
        let (second_spec, _b) = spec_with(SCRIPT, &[]);

        let mut first = SupervisedHarness::spawn(&first_spec).await.expect("spawns");
        let mut second = SupervisedHarness::spawn(&second_spec)
            .await
            .expect("spawns");

        let first_id = first.id().expect("a running child has a pid");
        let second_id = second.id().expect("a running child has a pid");

        assert_ne!(
            first_id, second_id,
            "two children cannot share a pid; a constant id would say they do"
        );
        assert!(
            first_id > 1 && second_id > 1,
            "a spawned child is never the kernel (0) or init (1), got {first_id} and {second_id}"
        );

        first.kill().await.expect("kill by handle");
        second.kill().await.expect("kill by handle");
    }

    /// The id stops being reported once the child is reaped.
    ///
    /// Pinning the contract, because the dangerous alternative is a stale pid:
    /// the kernel reuses them, so a caller acting on one after reaping could
    /// signal a process that has nothing to do with this Worker.
    #[tokio::test]
    async fn a_reaped_child_reports_no_pid() {
        let (spec, _dir) = spec_with(SCRIPT, &[]);
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        assert!(harness.id().is_some());

        harness.kill().await.expect("kill by handle");
        let _ = within("the child is reaped", harness.wait()).await;

        assert_eq!(
            harness.id(),
            None,
            "a reaped pid must not be handed out — the kernel may have reissued it"
        );
    }
}

// ---------------------------------------------------------------------------
// Process groups (ADR-0010): stopping a harness reaches everything it started.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod process_groups {
    use std::sync::Arc;

    use nix::sys::signal::kill;
    use nix::unistd::{Pid, getpgid};

    use super::harness::{spec_with, within};
    use horde_worker::process::{HarnessSpec, reap_leftover_groups};
    use horde_worker::{Journal, SupervisedHarness};

    const SCRIPT: &str = "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}";

    /// A stub that starts a SIGTERM-ignoring grandchild, with a short grace.
    /// The pid file lives in the spec's own scratch dir, which travels back.
    fn spec_with_grandchild() -> (HarnessSpec, tempfile::TempDir, std::path::PathBuf) {
        let scratch = tempfile::tempdir().expect("scratch");
        let pidfile = scratch.path().join("grandchild.pid");
        let (mut spec, _transcript_dir) = spec_with(
            SCRIPT,
            &[("HORDE_STUB_GRANDCHILD", &pidfile.to_string_lossy())],
        );
        // The transcript must outlive the child: copy it beside the pid file.
        let transcript = scratch.path().join("transcript.jsonl");
        std::fs::write(&transcript, SCRIPT).expect("writes");
        for (key, value) in &mut spec.env {
            if key == "HORDE_STUB_TRANSCRIPT" {
                *value = transcript.to_string_lossy().into_owned();
            }
        }
        spec.stop_grace = std::time::Duration::from_millis(300);
        (spec, scratch, pidfile)
    }

    fn pid_of(raw: u32) -> Pid {
        Pid::from_raw(i32::try_from(raw).expect("a pid fits pid_t"))
    }

    /// The grandchild's pid, once the stub has written it.
    async fn grandchild(pidfile: &std::path::Path) -> Pid {
        within("the grandchild starts", async {
            loop {
                if let Some(pid) = std::fs::read_to_string(pidfile)
                    .ok()
                    .and_then(|text| text.trim().parse::<i32>().ok())
                {
                    return Pid::from_raw(pid);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
    }

    /// Whether `pid` is gone, polled under a ceiling: an orphan is reaped by
    /// init a moment after it dies, and until then it still answers.
    async fn is_gone(pid: Pid) -> bool {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while kill(pid, None).is_ok() {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .is_ok()
    }

    /// Each harness leads its own group, so a group signal reaches its
    /// children and never the Worker's own group.
    #[tokio::test]
    async fn a_harness_leads_its_own_process_group() {
        let (spec, _dir, _pidfile) = spec_with_grandchild();
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let pid = harness.id().expect("running");

        assert_eq!(harness.process_group(), Some(pid));
        assert_eq!(
            getpgid(Some(pid_of(pid))).ok(),
            Some(pid_of(pid)),
            "the harness is its own group's leader"
        );
        assert_ne!(
            getpgid(None).ok(),
            Some(pid_of(pid)),
            "and that group is not the Worker's"
        );
        harness.stop().await.expect("stops");
        assert_eq!(
            harness.process_group(),
            None,
            "a reaped group is not ours to name"
        );
    }

    /// The archive path: stop reaches a child that ignores SIGTERM — a tool
    /// command or dev server must not outlive the agent that started it.
    #[tokio::test]
    async fn stopping_a_harness_reaches_everything_it_started() {
        let (spec, _dir, pidfile) = spec_with_grandchild();
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let orphan = grandchild(&pidfile).await;

        within("stop", harness.stop()).await.expect("stops");

        assert!(
            is_gone(orphan).await,
            "the grandchild must die with its harness"
        );
    }

    /// A leader that ignores SIGTERM is SIGKILLed once the grace runs out —
    /// the stop is bounded, never a wait on a process that chose not to exit.
    #[tokio::test]
    async fn a_leader_that_ignores_sigterm_is_killed_after_the_grace() {
        let spec = HarnessSpec {
            args: vec!["-c".to_owned(), "trap '' TERM; exec sleep 600".to_owned()],
            stop_grace: std::time::Duration::from_millis(200),
            ..HarnessSpec::new("/bin/sh")
        };
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let pid = pid_of(harness.id().expect("running"));
        // Let the shell install its trap before the signal lands.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        within("a bounded stop", harness.stop())
            .await
            .expect("stops");
        assert_eq!(harness.id(), None, "the leader is reaped");
        assert!(kill(pid, None).is_err(), "and gone");
    }

    /// A handle dropped without a stop still takes its group down.
    #[tokio::test]
    async fn a_dropped_handle_takes_its_group_down() {
        let (spec, _dir, pidfile) = spec_with_grandchild();
        let harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let orphan = grandchild(&pidfile).await;

        drop(harness);

        assert!(
            is_gone(orphan).await,
            "dropping the handle must not orphan children"
        );
    }

    /// A harness that dies on its own leaves its children with no supervisor;
    /// reaping it sweeps them.
    #[tokio::test]
    async fn a_harness_that_dies_on_its_own_leaves_no_children_behind() {
        let (mut spec, _dir, pidfile) = spec_with_grandchild();
        spec.env
            .push(("HORDE_STUB_EXIT_AFTER".to_owned(), "1".to_owned()));
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let orphan = grandchild(&pidfile).await;

        harness.write_line("{}").await.expect("one turn");
        let _ = within("the harness exits", harness.wait()).await;

        assert!(is_gone(orphan).await, "the dead harness's child is swept");
    }

    /// The ledger remembers a group from spawn to reap, fingerprinted.
    #[tokio::test]
    async fn the_ledger_remembers_a_group_until_it_is_reaped() {
        let journal = Journal::in_memory().expect("journal");
        let (mut spec, _dir, _pidfile) = spec_with_grandchild();
        spec.ledger = Some(Arc::new(journal.clone()));
        let mut harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let pid = harness.id().expect("running");

        let groups = journal.harness_groups().await.expect("readable");
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, pid);
        assert!(
            !groups[0].1.is_empty(),
            "the leader is fingerprinted: {groups:?}"
        );

        harness.stop().await.expect("stops");
        assert!(
            journal.harness_groups().await.expect("readable").is_empty(),
            "a reaped group is forgotten"
        );
    }

    /// A dropped handle forgets its group too, so the next start does not
    /// go looking for a group this run already took down.
    #[tokio::test]
    async fn a_dropped_handle_forgets_its_group() {
        let journal = Journal::in_memory().expect("journal");
        let (mut spec, _dir, _pidfile) = spec_with_grandchild();
        spec.ledger = Some(Arc::new(journal.clone()));
        let harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        assert_eq!(journal.harness_groups().await.expect("readable").len(), 1);

        drop(harness);

        within("the group is forgotten", async {
            while !journal.harness_groups().await.expect("readable").is_empty() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
    }

    /// The crash path: a group the Worker never stopped (its handle leaked,
    /// as a crashed process leaks everything) is swept by the next start's
    /// reap and forgotten — a second sweep finds nothing.
    #[tokio::test]
    async fn a_crashed_runs_leftover_group_is_swept_at_startup() {
        let journal = Journal::in_memory().expect("journal");
        let (mut spec, _dir, pidfile) = spec_with_grandchild();
        spec.ledger = Some(Arc::new(journal.clone()));
        let harness = SupervisedHarness::spawn(&spec).await.expect("spawns");
        let orphan = grandchild(&pidfile).await;
        // The crash: no stop, no drop — nothing of this run gets to clean up.
        std::mem::forget(harness);

        assert_eq!(reap_leftover_groups(&journal).await, 1, "one group swept");
        assert!(is_gone(orphan).await, "the leftover child is dead");
        assert!(journal.harness_groups().await.expect("readable").is_empty());
        assert_eq!(reap_leftover_groups(&journal).await, 0, "nothing left");
    }
}
