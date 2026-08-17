//! The `horde-worker` daemon, run for real.
//!
//! `bin/horde_worker.rs` is reachable only by running the binary, and what it
//! decides — exit codes, operator lines, signal behavior — is only observable
//! from outside the process. These tests spawn the binary cargo just built
//! against in-process servers (secured and plaintext), which is the same
//! shape `horde-server`'s and `horde`'s binary suites use. The startup
//! *decisions* (flag table, script form, identity plan, fate classifier)
//! have their own unit tests in `startup.rs`; what lives here is the wiring
//! no unit can see: enrollment over a real TLS listener, registration over
//! mTLS, a placed agent served end to end, SIGTERM reaping the pumps, and a
//! revoked identity stopping the daemon instead of spinning on the door.

#[cfg(test)]
mod harness {
    use horde_server_testkit::process::SpawnedProcess;

    /// The daemon binary cargo built alongside this test — never a stale
    /// copy on `PATH`.
    pub(crate) const DAEMON: &str = env!("CARGO_BIN_EXE_horde-worker");

    /// Spawns the daemon with `args`, watched.
    pub(crate) fn daemon(args: &[&str]) -> SpawnedProcess {
        SpawnedProcess::spawn("horde-worker", std::path::Path::new(DAEMON), args, &[])
            .expect("the daemon spawns")
    }
}

use horde_server::EventLog;
use horde_server::service::ServerCore;

use crate::support;

/// A mistyped flag must reach the exit code as a refusal that names it —
/// the positive control that the parser's verdicts survive `main`.
#[tokio::test]
async fn wrong_flags_refuse_by_name_from_the_binary() {
    let output = tokio::process::Command::new(harness::DAEMON)
        .arg("--bogus")
        .output()
        .await
        .expect("the binary runs");
    assert!(
        !output.status.success(),
        "a refused parse must exit nonzero"
    );
    let refusal = String::from_utf8_lossy(&output.stderr);
    assert!(
        refusal.contains("--bogus"),
        "the refusal names the flag to fix: {refusal}"
    );
}

/// A missing identity is a startup refusal that says how to enroll, not a
/// silent plaintext fallback and not a retry loop.
#[tokio::test]
async fn a_missing_identity_refuses_startup_naming_the_fix() {
    let state = tempfile::tempdir().expect("a temp dir");
    let output = tokio::process::Command::new(harness::DAEMON)
        .args(["--state-dir", &state.path().to_string_lossy()])
        .output()
        .await
        .expect("the binary runs");
    assert!(!output.status.success());
    let refusal = String::from_utf8_lossy(&output.stderr);
    assert!(
        refusal.contains("--join"),
        "the refusal tells the operator how to enroll: {refusal}"
    );
}

/// The daemon's headline path: redeem a one-time code over the real
/// enrollment listener, persist the identity, register over mTLS under the
/// certificate's name, and shut down cleanly on SIGTERM.
#[tokio::test]
async fn the_daemon_joins_registers_and_a_sigterm_ends_it_cleanly() {
    let secured = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server comes up");
    let principal = horde_identity::Principal::new(horde_identity::PrincipalRole::Worker, "w-1")
        .expect("a valid name");
    let minted = secured
        .authority
        .mint(
            &principal,
            0,
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await
        .expect("the code mints");

    let state = tempfile::tempdir().expect("a temp dir");
    let mut daemon = harness::daemon(&[
        "--server",
        &secured.main_endpoint(),
        "--enrollment",
        &secured.enrollment_endpoint(),
        "--join",
        &minted.join_code(),
        "--state-dir",
        &state.path().to_string_lossy(),
        "--adapter",
        "fake",
    ]);

    daemon
        .await_stdout("the enrollment line", |out| {
            out.contains("enrolled principal=worker:w-1")
        })
        .await
        .expect("the daemon enrolls through the production listener");
    assert!(
        horde_identity::StoredIdentity::exists(&state.path().join("identity")),
        "the identity must persist, or every restart needs a fresh code"
    );
    daemon
        .await_stdout("the registration line", |out| {
            out.contains("registered worker=w-1")
        })
        .await
        .expect("the enrolled daemon registers over mTLS");

    daemon.terminate().await.expect("SIGTERM sends");
    let (status, stdout, stderr) = daemon.wait().await.expect("the daemon exits");
    assert!(
        status.success(),
        "a signalled shutdown is the ordinary case and must exit zero: {stderr}"
    );
    assert!(
        stdout.contains("shutdown reaped=0"),
        "shutdown reports what it reaped — zero agents, honestly: {stdout}"
    );
}

/// The whole worker, as a process, over plaintext loopback: a placed agent's
/// spawn command reaches the daemon, the fake harness runs the turn, the
/// four events arrive in the server's log, and SIGTERM reaps the one pump.
#[tokio::test]
async fn an_insecure_daemon_serves_a_placed_agent_and_reaps_it() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = support::serve(core.clone()).await;
    support::place_agent(&core.log, "s1", "agent-1", "w-x", "say hello").await;

    let state = tempfile::tempdir().expect("a temp dir");
    let mut daemon = harness::daemon(&[
        "--insecure",
        "--server",
        &endpoint,
        "--name",
        "w-x",
        "--adapter",
        "fake",
        "--state-dir",
        &state.path().to_string_lossy(),
    ]);
    daemon
        .await_stdout("the registration line", |out| {
            out.contains("registered worker=w-x resume=")
        })
        .await
        .expect("the plaintext dial registers");

    // Two seed events, then the worker's four: starting, established,
    // one output chunk, one usage record.
    let head = support::server_log_at_least(&core.log, "s1", 6).await;
    assert_eq!(head, 6, "exactly the four worker events, no duplicates");

    daemon.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = daemon.wait().await.expect("the daemon exits");
    assert!(status.success());
    assert!(
        stdout.contains("shutdown reaped=1"),
        "the running agent's pump must be reaped, and said so: {stdout}"
    );
}

/// Revocation closes the door for good: a daemon holding a revoked identity
/// must stop with a nonzero exit instead of redialing forever — the fate
/// split that separates a partition from a refusal, held over a real
/// process.
#[tokio::test]
async fn a_revoked_worker_stops_fatally_instead_of_redialing() {
    let secured = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server comes up");
    let identity = secured
        .enroll(horde_identity::PrincipalRole::Worker, "w-2")
        .await
        .expect("the worker enrolls");
    let state = tempfile::tempdir().expect("a temp dir");
    identity
        .save(&state.path().join("identity"))
        .expect("the identity persists");

    let principal = horde_identity::Principal::new(horde_identity::PrincipalRole::Worker, "w-2")
        .expect("a valid name");
    let revoked = secured
        .authority
        .revoke(
            &principal,
            "the e2e closes this door",
            horde_server::service::enrollment::wall_clock_ms(),
        )
        .await
        .expect("the revocation journals");
    assert!(!revoked.is_empty(), "the worker held a serial to revoke");

    let daemon = harness::daemon(&[
        "--server",
        &secured.main_endpoint(),
        "--state-dir",
        &state.path().to_string_lossy(),
        "--adapter",
        "fake",
    ]);
    // No terminate: the daemon must decide to stop by itself.
    let (status, stdout, stderr) = daemon.wait().await.expect("the daemon stops on its own");
    assert!(
        !status.success(),
        "a revoked identity is a refusal, not weather: {stdout}"
    );
    assert!(
        stderr.contains("refused"),
        "the fatal reason reaches stderr: {stderr}"
    );
    assert!(
        stdout.contains("shutdown reaped=0"),
        "even a fatal end reaps before exiting: {stdout}"
    );
}

/// The certificate outranks the flag: an enrolled identity names the worker,
/// and the state directory yields the principal for telemetry stamping.
/// In-process (no daemon), because the subject is the naming decision over a
/// production-issued certificate.
#[tokio::test]
async fn an_enrolled_identity_names_the_worker() {
    let secured = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server comes up");
    let identity = secured
        .enroll(horde_identity::PrincipalRole::Worker, "w-7")
        .await
        .expect("the worker enrolls");

    assert_eq!(
        horde_worker::startup::worker_name(Some(&identity), Some("flag-name")),
        "w-7",
        "the certificate's principal outranks the flag"
    );

    let state = tempfile::tempdir().expect("a temp dir");
    identity
        .save(&state.path().join("identity"))
        .expect("the identity persists");
    assert_eq!(
        horde_worker::startup::enrolled_principal(state.path()).as_deref(),
        Some("worker:w-7"),
        "telemetry is stamped with who this daemon is"
    );
    assert_eq!(
        horde_worker::startup::enrolled_principal(&state.path().join("empty")),
        None,
        "an absent identity enriches nothing rather than refusing"
    );
}

/// The reconnect loop, in-process: a dead endpoint is weather, announced and
/// redialed, and shutdown wins the pause — the loop's whole contract without
/// a server anywhere.
#[tokio::test]
async fn the_run_loop_redials_dial_failures_until_shutdown() {
    let state = tempfile::tempdir().expect("a temp dir");
    let config = horde_worker::startup::invocation(
        [
            "--insecure",
            "--server",
            "http://127.0.0.1:1",
            "--name",
            "w-dead",
            "--adapter",
            "fake",
            "--state-dir",
            &state.path().to_string_lossy(),
        ]
        .into_iter()
        .map(ToOwned::to_owned),
    )
    .expect("the shape parses");

    let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut stop = Some(stop);
    let recorded = std::sync::Arc::clone(&lines);
    let mut announce = move |line: String| {
        // The first dial failure requests shutdown: the test then asserts
        // the loop honored it instead of redialing forever.
        if line.starts_with("dial-failed")
            && let Some(stop) = stop.take()
        {
            let _ = stop.send(());
        }
        recorded.lock().expect("an unpoisoned recorder").push(line);
    };

    let prepared = horde_worker::startup::prepare(&config, &mut announce)
        .await
        .expect("an insecure daemon prepares without a server");
    support::within(
        "the run loop must stop when shutdown wins the redial pause",
        horde_worker::startup::run(
            prepared,
            async move {
                let _ = stopped.await;
            },
            &mut announce,
        ),
    )
    .await
    .expect("a signalled stop is clean, not an error");

    let lines = lines.lock().expect("an unpoisoned recorder").clone();
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("dial-failed reason=")),
        "the loss is announced with its reason: {lines:?}"
    );
    assert_eq!(
        lines.last().map(String::as_str),
        Some("shutdown reaped=0"),
        "the last word is the reap count: {lines:?}"
    );
}
