//! Direct probes of the process harness itself.
//!
//! The harness is a product this suite tests, not fixture: an injector
//! whose kill answers `Ok` over a process still breathing makes every
//! kill scenario vacuous, because recovery from a fault that never
//! happened is indistinguishable from no fault. `kill_on_drop` makes that
//! failure mode subtle — a no-op kill's child still dies when the handle
//! drops — so these probes hold the harness to what a drop cannot fake:
//! the reported pid names the runner itself, and `kill_now` returns only
//! once the child is reaped, with SIGKILL as the recorded cause of death.

use std::os::unix::process::ExitStatusExt;

use horde_fault::{WorkerProcess, WorkerRunnerConfig};

/// A runner that parks in its redial loop: the endpoint is a port nothing
/// serves, so the process stays alive and killable for the probe's whole
/// life without needing a server.
fn parked_runner(dir: &tempfile::TempDir) -> WorkerRunnerConfig {
    WorkerRunnerConfig {
        endpoint: "http://127.0.0.1:1".to_owned(),
        worker_id: "probe".to_owned(),
        journal: dir.path().join("journal.sqlite"),
        turns: Vec::new(),
        identity_dir: None,
    }
}

/// What `ps` calls `pid`, or empty when the pid does not exist at all —
/// which is also the distinction the probes turn on: a zombie still has a
/// `ps` row, a reaped process has nothing.
fn process_name_of(pid: u32) -> String {
    let output = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "comm="])
        .output()
        .expect("ps runs");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[tokio::test]
async fn the_reported_pid_names_the_runner_itself() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let worker = WorkerProcess::spawn(&parked_runner(&dir)).expect("the runner spawns");
    let pid = worker.id().expect("a live child has a pid");

    let name = process_name_of(pid);
    assert!(
        name.ends_with("worker-runner"),
        "id() must name the spawned runner, not somebody else's pid: {name:?}"
    );
    let _ = worker.kill_now().await;
}

#[tokio::test]
async fn kill_now_delivers_sigkill_and_reaps_before_returning() {
    let dir = tempfile::tempdir().expect("a tempdir");
    let worker = WorkerProcess::spawn(&parked_runner(&dir)).expect("the runner spawns");
    let pid = worker.id().expect("a live child has a pid");

    let ended = worker.kill_now().await.expect("the kill lands");
    assert_eq!(
        ended.signal(),
        Some(9),
        "SIGKILL is the contract: no shutdown, no goodbye — got {ended:?}"
    );

    // Reaped, not merely signalled: by the time kill_now answers, the pid
    // is gone entirely. A kill that only signalled would leave a zombie
    // with a `ps` row; a kill that never happened would leave the runner.
    let name = process_name_of(pid);
    assert!(
        name.is_empty(),
        "the child must be reaped before kill_now returns, found {name:?}"
    );
}
