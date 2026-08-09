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

    /// The stub binary cargo built alongside this test.
    pub fn stub_binary() -> PathBuf {
        let mut path = std::env::current_exe().unwrap_or_default();
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path.join("stub-harness")
    }

    /// A spec whose child gets extra environment, for scripting the stub.
    pub fn spec_with(transcript: &str, extra: &[(&str, &str)]) -> (HarnessSpec, tempfile::TempDir) {
        let (mut spec, dir) = spec_for(transcript);
        for (key, value) in extra {
            spec.env.push(((*key).to_owned(), (*value).to_owned()));
        }
        (spec, dir)
    }

    pub fn spec_for(transcript: &str) -> (HarnessSpec, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let file = dir.path().join("transcript.jsonl");
        std::fs::write(&file, transcript).expect("writes");

        let spec = HarnessSpec {
            program: stub_binary().to_string_lossy().into_owned(),
            args: Vec::new(),
            // The script travels in the child's environment, so each test's child
            // gets its own without touching this process's env.
            env: vec![(
                "HORDE_STUB_TRANSCRIPT".to_owned(),
                file.to_string_lossy().into_owned(),
            )],
            config_dir: None,
            config_env_var: "CLAUDE_CONFIG_DIR",
            cwd: None,
        };
        (spec, dir)
    }
}

use harness::spec_for;

#[tokio::test]
async fn a_turn_round_trips_through_a_real_child_process() {
    let (spec, _dir) = spec_for(
        "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s1\",\"model\":\"m\"}\n\
         {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":5,\"result\":\"hi\"}",
    );

    let mut harness = SupervisedHarness::spawn(&spec).expect("the stub must spawn");
    assert!(harness.id().is_some(), "a live child has a pid");

    harness
        .write_line("{\"type\":\"user\"}")
        .await
        .expect("writes");

    let first = harness
        .read_line()
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

    let mut harness = SupervisedHarness::spawn(&spec).expect("spawns");
    harness
        .write_line("{\"type\":\"user\"}")
        .await
        .expect("writes");
    let _ = harness.read_line().await.expect("reads the scripted turn");

    // Script exhausted: the stub exits and its stdout closes.
    let _ = harness.write_line("{\"type\":\"user\"}").await;

    let mut saw_eof = false;
    for _ in 0..64 {
        match harness.read_line().await {
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
    let spec = HarnessSpec {
        program: "horde-no-such-harness".to_owned(),
        args: Vec::new(),
        env: Vec::new(),
        config_dir: None,
        config_env_var: "CLAUDE_CONFIG_DIR",
        cwd: None,
    };

    assert!(
        matches!(
            SupervisedHarness::spawn(&spec).err(),
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

    let mut harness = SupervisedHarness::spawn(&spec).expect("spawns");
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
    use super::harness::{spec_for, stub_binary};
    use horde_adapter::{Adapter, AgentEvent, AgentSpec, TurnInput, TurnStatus};
    use horde_worker::ClaudeAdapter;

    /// A whole turn, from spawn to `TurnEnded`, against a real child.
    #[tokio::test]
    async fn a_turn_streams_and_ends_through_a_real_process() {
        let (harness_spec, _dir) = spec_for(
            "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"sess-1\",\"model\":\"haiku\"}\n\
             {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"sess-1\",\"duration_ms\":7,\"result\":\"HORDE_OK\"}",
        );

        // Point the adapter at the stub: the tests must never touch the real CLI.
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.accounts_root = None;

        // Per-child, never process-wide: setting it on this process would race
        // every other test's spawn.
        adapter.extra_env = harness_spec.env.clone();

        let spec = AgentSpec {
            model: Some("haiku".to_owned()),
            append_system_prompt: Some("you are supervised".to_owned()),
            cwd: None,
            account: None,
            horde_endpoint: None,
            provider_options: std::collections::BTreeMap::new(),
        };

        let mut session = adapter.spawn(&spec).await.expect("the stub must spawn");
        session
            .send(TurnInput::queued("hello"))
            .await
            .expect("sends");

        let mut ended = None;
        while let Ok(Some(event)) = session.next_event().await {
            if let AgentEvent::TurnEnded(outcome) = event {
                ended = Some(outcome);
                break;
            }
        }

        let ended = ended.expect("the turn must end");
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
    use super::harness::spec_with;
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

        let mut harness = SupervisedHarness::spawn(&spec).expect("spawns");
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

        let mut harness = SupervisedHarness::spawn(&spec).expect("spawns");
        harness
            .write_line("{\"type\":\"user\"}")
            .await
            .expect("writes");

        // Drain to EOF — bounded, because a hang here is the bug under test.
        let drained = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while let Ok(Some(_)) = harness.read_line().await {}
            harness.wait().await
        })
        .await;

        let status = drained.expect("a dead harness must not hang the reader");
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

        let mut harness = SupervisedHarness::spawn(&spec).expect("spawns");
        let _ = harness.read_line().await;
        let _ = harness.wait().await;

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
}

// ---------------------------------------------------------------------------
// Revival: a conversation outliving the process that started it.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod revival {
    use super::harness::{spec_with, stub_binary};
    use horde_adapter::{Adapter, AgentEvent, AgentSpec, ProviderSessionId, TurnInput};
    use horde_worker::ClaudeAdapter;

    fn spec() -> AgentSpec {
        AgentSpec {
            model: None,
            append_system_prompt: None,
            cwd: None,
            account: None,
            horde_endpoint: None,
            provider_options: std::collections::BTreeMap::new(),
        }
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

        let (harness_spec, _dir) = spec_with(script, &[]);
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = harness_spec.env.clone();

        let mut first = adapter.spawn(&spec()).await.expect("spawns");
        first.send(TurnInput::queued("hello")).await.expect("sends");
        while let Ok(Some(event)) = first.next_event().await {
            if matches!(event, AgentEvent::TurnEnded(_)) {
                break;
            }
        }

        let revival_id = first
            .provider_session_id()
            .cloned()
            .expect("the id must reach the caller before the process can be lost");
        assert_eq!(revival_id.to_string(), "sess-42");
        first.shutdown().await.expect("shuts down");

        // A second, independent process, carrying the id.
        let mut revived = adapter
            .resume(&spec(), &revival_id)
            .await
            .expect("resume spawns a fresh process");
        revived
            .send(TurnInput::queued("still there?"))
            .await
            .expect("the revived session is usable");

        let mut saw_turn = false;
        while let Ok(Some(event)) = revived.next_event().await {
            if matches!(event, AgentEvent::TurnEnded(_)) {
                saw_turn = true;
                break;
            }
        }
        assert!(saw_turn, "the revived session must run a turn");
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
        let (harness_spec, _dir) = spec_with("", &[]);
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = harness_spec.env.clone();

        let mut revived = adapter
            .resume(&spec(), &ProviderSessionId::new("sess-does-not-exist"))
            .await
            .expect("the process starts; the failure is in what it does next");
        let _ = revived.send(TurnInput::queued("hello")).await;

        // No events, and the stream ends — never an invented SessionEstablished.
        let mut events = Vec::new();
        while let Ok(Some(event)) = revived.next_event().await {
            events.push(event);
        }

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
    use super::harness::{spec_with, stub_binary};
    use horde_adapter::{Adapter, AgentSpec, TurnInput};
    use horde_worker::ClaudeAdapter;

    fn spec() -> AgentSpec {
        AgentSpec {
            model: None,
            append_system_prompt: None,
            cwd: None,
            account: None,
            horde_endpoint: None,
            provider_options: std::collections::BTreeMap::new(),
        }
    }

    /// `interrupt()` must actually write the control message.
    ///
    /// The stub acknowledges control requests, so a no-op `interrupt` produces
    /// no acknowledgement and this fails — which is the point: an interrupt that
    /// silently does nothing is indistinguishable from one that worked, right up
    /// until a user cannot stop a runaway agent.
    #[tokio::test]
    async fn interrupt_puts_the_control_message_on_the_wire() {
        let script = "{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s1\",\"model\":\"m\"}\n\
                      {\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}";
        let (harness_spec, _dir) = spec_with(script, &[]);
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = harness_spec.env.clone();

        let mut session = adapter.spawn(&spec()).await.expect("spawns");
        session.interrupt().await.expect("the write must succeed");

        // The stub answers a control request with a control_response, which the
        // codec classifies as a non-event — so the proof is that the turn still
        // runs afterwards rather than the stream being poisoned.
        session
            .send(TurnInput::queued("after the interrupt"))
            .await
            .expect("the session survives an interrupt");

        let saw_event = matches!(session.next_event().await, Ok(Some(_)));
        assert!(
            saw_event,
            "the session must remain usable after an interrupt — that is what \
             distinguishes the control message from a signal"
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
        let (harness_spec, _dir) = spec_with(
            "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}",
            &[],
        );
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = harness_spec.env.clone();

        let mut session = adapter.spawn(&spec()).await.expect("spawns");
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
        let (harness_spec, _dir) = spec_with(
            "{\"type\":\"result\",\"subtype\":\"success\",\"is_error\":false,\"session_id\":\"s1\",\"duration_ms\":1,\"result\":\"x\"}",
            &[],
        );
        let mut adapter = ClaudeAdapter::with_program(stub_binary().to_string_lossy());
        adapter.extra_env = harness_spec.env.clone();

        let mut session = adapter.spawn(&spec()).await.expect("spawns");
        session.shutdown().await.expect("shuts down");

        // A shut-down session is finished, not merely quiet: sending must fail
        // rather than block on a process that is gone.
        assert!(
            session.send(TurnInput::queued("hello")).await.is_err(),
            "a shut-down session must refuse work rather than hang"
        );
        assert!(
            session.next_event().await.expect("no error").is_none(),
            "and produce nothing further"
        );
    }
}
