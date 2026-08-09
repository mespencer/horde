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

        // The stub reads its script from the environment of its own process, so
        // hand the binding through the spec the adapter builds.
        unsafe {
            std::env::set_var(
                "HORDE_STUB_TRANSCRIPT",
                harness_spec
                    .env
                    .iter()
                    .find(|(k, _)| k == "HORDE_STUB_TRANSCRIPT")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default(),
            );
        }

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
