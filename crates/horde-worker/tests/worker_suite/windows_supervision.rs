//! Supervision on Windows (ADR-0017): a real stub-harness child whose tree
//! is a kill-on-close Job Object.
//!
//! The stub starts a long-lived grandchild (`ping -n 600`) — the tool
//! command or dev server a real harness leaves running — and each test
//! watches whether stopping the harness reaches it. On Unix the same
//! questions are asked of process groups in `supervision.rs`.

use horde_server_testkit::process::windows::{gone_soon, running};

use rig::{CEILING, Spawned, spawned};

/// Test-only fixtures, under `cfg(test)` so the workspace's `expect_used`
/// exemption applies (the same shape as `supervision.rs`'s `harness`).
#[cfg(test)]
mod rig {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use horde_worker::SupervisedHarness;
    use horde_worker::process::HarnessSpec;

    pub(crate) const CEILING: Duration = Duration::from_secs(30);

    /// A running stub, the pid of the grandchild it started, and the
    /// scratch directory its files live in (kept for the test's length).
    pub(crate) struct Spawned {
        pub(crate) harness: SupervisedHarness,
        pub(crate) grandchild: u32,
        _dir: tempfile::TempDir,
    }

    /// The stub binary cargo built alongside this test.
    fn stub_binary() -> PathBuf {
        let mut path = std::env::current_exe().expect("the test binary's path");
        path.pop();
        if path.ends_with("deps") {
            path.pop();
        }
        path.join("stub-harness.exe")
    }

    /// A spec for the stub replaying one turn, starting a grandchild whose
    /// pid it writes to `<dir>/grandchild.pid`.
    fn spec_in(dir: &Path) -> HarnessSpec {
        let transcript = dir.join("transcript.jsonl");
        std::fs::write(&transcript, "{\"type\":\"result\"}\n").expect("writes the transcript");
        HarnessSpec {
            env: vec![
                (
                    "HORDE_STUB_TRANSCRIPT".to_owned(),
                    transcript.display().to_string(),
                ),
                (
                    "HORDE_STUB_GRANDCHILD".to_owned(),
                    dir.join("grandchild.pid").display().to_string(),
                ),
            ],
            stop_grace: Duration::from_secs(3),
            ..HarnessSpec::new(stub_binary().display().to_string())
        }
    }

    /// The stub, spawned, once it has written its grandchild's pid.
    pub(crate) async fn spawned() -> Spawned {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let harness = SupervisedHarness::spawn(&spec_in(dir.path()))
            .await
            .expect("the stub spawns");
        let file = dir.path().join("grandchild.pid");
        let grandchild = tokio::time::timeout(CEILING, async {
            loop {
                if let Some(pid) = std::fs::read_to_string(&file)
                    .ok()
                    .and_then(|text| text.trim().parse().ok())
                {
                    return pid;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the stub writes its grandchild's pid");
        Spawned {
            harness,
            grandchild,
            _dir: dir,
        }
    }
}

/// A stop closes the harness's stdin — EOF ends the stub, as it ends a
/// stream-json session — and the job's sweep after the reap takes the
/// grandchild the harness left behind.
#[tokio::test]
async fn a_stop_ends_the_harness_by_eof_and_sweeps_its_job() {
    let mut spawned = spawned().await;
    let grandchild = spawned.grandchild;
    assert!(running(grandchild).await, "the grandchild started");
    assert_eq!(spawned.harness.process_group(), None, "a job, not a group");

    tokio::time::timeout(CEILING, spawned.harness.stop())
        .await
        .expect("a stop is bounded")
        .expect("the stop succeeds");
    assert!(spawned.harness.id().is_none(), "the leader is reaped");
    assert!(
        gone_soon(grandchild).await,
        "{grandchild} outlived its harness's job"
    );
}

/// A kill terminates the whole job at once.
#[tokio::test]
async fn a_kill_terminates_the_whole_job() {
    let mut spawned = spawned().await;
    tokio::time::timeout(CEILING, spawned.harness.kill())
        .await
        .expect("a kill is bounded")
        .expect("the kill succeeds");
    let grandchild = spawned.grandchild;
    assert!(gone_soon(grandchild).await, "{grandchild} outlived a kill");
}

/// A handle dropped without a stop closes the job's last handle, which
/// kills every member — the property that leaves a crashed Worker nothing
/// to sweep on Windows.
#[tokio::test]
async fn a_dropped_handle_kills_the_job() {
    let spawned: Spawned = spawned().await;
    let grandchild = spawned.grandchild;
    drop(spawned.harness);
    assert!(
        gone_soon(grandchild).await,
        "{grandchild} outlived its handle"
    );
}

/// Writing after a stop closed stdin is the harness's death seen from the
/// write side — a hang-up, never a protocol accident.
#[tokio::test]
async fn a_write_after_a_stop_is_a_hang_up() {
    let mut spawned = spawned().await;
    spawned.harness.stop().await.expect("the stop succeeds");
    assert!(matches!(
        spawned.harness.write_line("{}").await,
        Err(horde_worker::SupervisorError::HungUp)
    ));
}
