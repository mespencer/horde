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

    /// [`DAEMON`], its first-exec admission paid (`support::admitted_binary`
    /// explains the mechanism): the refusal tests below run the binary under
    /// the suite's 5s ceiling, and a fresh tree's first launch pays macOS
    /// validation that must not land inside that bound. `--help` prints
    /// and exits at once.
    pub(crate) fn admitted_daemon() -> &'static str {
        crate::support::admit(std::path::Path::new(DAEMON), &["--help"]);
        DAEMON
    }

    /// Spawns the daemon with `args`, watched.
    pub(crate) fn daemon(args: &[&str]) -> SpawnedProcess {
        SpawnedProcess::spawn(
            "horde-worker",
            std::path::Path::new(admitted_daemon()),
            args,
            &[],
        )
        .expect("the daemon spawns")
    }

    /// A one-time worker join token for `name`, minted by the harness's
    /// production authority and in the token form a mint prints.
    pub(crate) async fn worker_token(
        secured: &horde_server_testkit::secure::SecureHarness,
        name: &str,
    ) -> String {
        let principal = horde_identity::Principal::new(horde_identity::PrincipalRole::Worker, name)
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
        secured.join_token(&minted).to_string()
    }

    /// The insecure fake-adapter shape both in-process loop tests drive:
    /// plaintext to `server`, named, stateful under `state`.
    pub(crate) fn insecure_config(
        server: &str,
        name: &str,
        state: &std::path::Path,
    ) -> horde_worker::startup::WorkerConfig {
        horde_worker::startup::invocation(
            [
                "--insecure",
                "--server",
                server,
                "--name",
                name,
                "--adapter",
                "fake",
                "--state-dir",
                &state.to_string_lossy(),
            ]
            .into_iter()
            .map(ToOwned::to_owned),
        )
        .expect("the shape parses")
    }
}

use horde_server::EventLog;
use horde_server::service::ServerCore;

use crate::support;

/// A mistyped flag must reach the exit code as a refusal that names it —
/// the positive control that the parser's verdicts survive `main`.
///
/// Bounded: a refusal exits in milliseconds, and a daemon that instead
/// *ran* would make an unbounded `output().await` a hang — the shape
/// docs/QUALITY_GATES.md warns turns a defect into a TIMEOUT nobody
/// evaluated (the resolve-identity mutant found this the hard way).
#[tokio::test]
async fn wrong_flags_refuse_by_name_from_the_binary() {
    let output = support::within(
        "a refused parse must exit, not run",
        tokio::process::Command::new(harness::admitted_daemon())
            .arg("--bogus")
            .output(),
    )
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
/// silent plaintext fallback and not a retry loop. Bounded for the same
/// reason as the flag refusal above: a daemon that wrongly resolved "no
/// identity" into "run without one" would never exit.
#[tokio::test]
async fn a_missing_identity_refuses_startup_naming_the_fix() {
    let state = tempfile::tempdir().expect("a temp dir");
    let output = support::within(
        "a refused startup must exit, not run",
        tokio::process::Command::new(harness::admitted_daemon())
            .args(["--state-dir", &state.path().to_string_lossy()])
            .output(),
    )
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
    let state = tempfile::tempdir().expect("a temp dir");
    // One token, one flag (ADR-0007): the listener rides inside it.
    let token = harness::worker_token(&secured, "w-1").await;
    let mut daemon = harness::daemon(&[
        "--server",
        &secured.main_endpoint(),
        "--join",
        &token,
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

/// The token is the only input (ADR-0007, 2026-10-31 amendment): a daemon
/// joined from a token *file* with no `--server` stores the server the
/// token taught and dials it; while it runs, a second daemon on the same
/// state refuses naming the lock; and a restart with no flags at all finds
/// the stored server again. `--help` is output and a success.
#[tokio::test]
async fn a_token_joined_daemon_remembers_its_server_and_holds_its_state() {
    let secured = horde_server_testkit::secure::serve_secured()
        .await
        .expect("a secured server comes up");
    let state = tempfile::tempdir().expect("a temp dir");
    let token_file = state.path().join("token");
    std::fs::write(
        &token_file,
        format!("{}\n", harness::worker_token(&secured, "w-9").await),
    )
    .expect("the token file writes");
    let state_dir = state.path().join("worker");
    let state_arg = state_dir.to_string_lossy().into_owned();

    let mut first = harness::daemon(&[
        "--join-file",
        &token_file.to_string_lossy(),
        "--state-dir",
        &state_arg,
    ]);
    first
        .await_stdout("the registration line", |out| {
            out.contains("registered worker=w-9")
        })
        .await
        .expect("a token-file join registers with no --server");
    assert_eq!(
        horde_identity::store::load_endpoint(&state_dir.join("identity")),
        Some(secured.main_endpoint()),
        "the join stored the server the token taught"
    );

    let second = support::within(
        "a held state refuses at once",
        tokio::process::Command::new(harness::admitted_daemon())
            .args(["--state-dir", &state_arg])
            .output(),
    )
    .await
    .expect("the binary runs");
    assert!(!second.status.success(), "one daemon per state dir");
    let refusal = String::from_utf8_lossy(&second.stderr);
    assert!(refusal.contains("worker.lock"), "{refusal}");

    first.terminate().await.expect("SIGTERM sends");
    first.wait().await.expect("the daemon exits");

    let mut again = harness::daemon(&["--state-dir", &state_arg]);
    again
        .await_stdout("the re-registration", |out| {
            out.contains("registered worker=w-9")
        })
        .await
        .expect("a bare restart dials the stored server");
    again.terminate().await.expect("SIGTERM sends");
    again.wait().await.expect("the daemon exits");

    let help = support::within(
        "help exits at once",
        tokio::process::Command::new(harness::admitted_daemon())
            .arg("--help")
            .output(),
    )
    .await
    .expect("the binary runs");
    assert!(help.status.success(), "help is not a failure");
    assert!(String::from_utf8_lossy(&help.stdout).contains("--join-file"));
}

/// The whole worker, as a process, over plaintext loopback: a placed agent's
/// spawn command reaches the daemon, the *scripted* fake harness plays its
/// turn (not an echo — the proof the `--fake-script` file was read and
/// parsed), the four events arrive in the server's log, and SIGTERM reaps
/// the one pump.
#[tokio::test]
async fn an_insecure_daemon_serves_a_placed_agent_and_reaps_it() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = support::serve(core.clone()).await;
    support::place_agent(&core.log, "s1", "agent-1", "w-x", "say hello").await;

    let state = tempfile::tempdir().expect("a temp dir");
    let script = state.path().join("script.json");
    std::fs::write(
        &script,
        r#"[{"steps": [{"say": "scripted, not an echo"}], "text": "done"}]"#,
    )
    .expect("the script writes");
    let mut daemon = harness::daemon(&[
        "--insecure",
        "--server",
        &endpoint,
        "--name",
        "w-x",
        "--adapter",
        "fake",
        "--fake-script",
        &script.to_string_lossy(),
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

    // The output is the script's first turn, not an echo of the assignment:
    // a daemon that quietly fell back to the echo script would still pass
    // every count above while ignoring the operator's script file.
    let events = support::decoded_session_log(&core.log, "s1").await;
    assert!(
        events.iter().any(|event| matches!(&event.kind,
            Some(horde_proto::v1::event::Kind::AgentOutputChunk(chunk))
                if chunk.text == "scripted, not an echo")),
        "the scripted turn must be what streamed: {events:?}"
    );

    daemon.terminate().await.expect("SIGTERM sends");
    let (status, stdout, _) = daemon.wait().await.expect("the daemon exits");
    assert!(status.success());
    assert!(
        stdout.contains("shutdown reaped=1"),
        "the running agent's pump must be reaped, and said so: {stdout}"
    );
}

/// DESIGN §2's partition behavior, held over the daemon's own reconnect
/// loop in-process: a severed transport is announced and *survived* — the
/// loop redials through the healed link and registers again — and only a
/// requested shutdown ends it. This is the run-loop half the fault tier
/// exercises against the binary; here it pins the loop's decisions.
#[tokio::test]
async fn the_run_loop_survives_a_partition_and_registers_again() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let upstream = support::serve(core.clone()).await;
    let proxy = horde_testkit::FaultProxy::front(&upstream)
        .await
        .expect("the fault proxy binds");

    let state = tempfile::tempdir().expect("a temp dir");
    let config = harness::insecure_config(&proxy.endpoint(), "w-r", state.path());

    let lines = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let recorded = std::sync::Arc::clone(&lines);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let mut announce =
        move |line: String| recorded.lock().expect("an unpoisoned recorder").push(line);
    let mut sink = |_: String| {};
    let prepared = horde_worker::startup::prepare(&config, &mut sink)
        .await
        .expect("an insecure daemon prepares");

    // The Debug view an operator's crash report will carry: the facts, and
    // never the key material.
    let rendered = format!("{prepared:?}");
    assert!(
        rendered.contains("w-r") && rendered.contains("enrolled: false"),
        "the prepared worker debugs its facts: {rendered}"
    );

    // `run`'s announce parameter is a plain `&mut dyn FnMut`, so the future
    // is not `Send` and cannot be spawned; the injector runs beside it on
    // the same task instead, which changes nothing observed.
    fn registered(lines: &[String], minimum: usize) -> bool {
        lines
            .iter()
            .filter(|line| line.starts_with("registered worker=w-r"))
            .count()
            >= minimum
    }
    async fn reached(
        lines: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        what: &str,
        holds: impl Fn(&[String]) -> bool,
    ) {
        for _ in 0_u32..250 {
            if holds(&lines.lock().expect("an unpoisoned recorder")) {
                return;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        panic!(
            "{what} never happened: {:?}",
            lines.lock().expect("an unpoisoned recorder")
        );
    }
    let injector = async {
        reached(&lines, "the first registration", |lines| {
            registered(lines, 1)
        })
        .await;

        // The cable is yanked: the loss is announced, not fatal.
        proxy.cut();
        reached(&lines, "the disconnect announcement", |lines| {
            lines
                .iter()
                .any(|line| line.starts_with("disconnected") || line.starts_with("dial-failed"))
        })
        .await;

        // Healed: the loop must come back by itself and register again.
        proxy.heal();
        reached(&lines, "the re-registration", |lines| registered(lines, 2)).await;
        let _ = stop.send(());
    };

    let (outcome, ()) = support::within(
        "the partition round-trip",
        futures_join(
            horde_worker::startup::run(
                prepared,
                async move {
                    let _ = stopped.await;
                },
                &mut announce,
            ),
            injector,
        ),
    )
    .await;
    assert!(outcome.is_ok(), "a signalled stop is clean: {outcome:?}");
    assert_eq!(
        lines
            .lock()
            .expect("an unpoisoned recorder")
            .last()
            .map(String::as_str),
        Some("shutdown reaped=0"),
        "the last word is the reap count"
    );
    assert_eq!(
        lines
            .lock()
            .expect("an unpoisoned recorder")
            .first()
            .map(String::as_str),
        Some("inbox-replayed commands=0"),
        "the first word is the inbox replay, before any dial"
    );
}

/// `tokio::join!` as a function, so the select-free test body stays
/// readable where the futures are built.
async fn futures_join<A, B>(
    a: impl std::future::Future<Output = A>,
    b: impl std::future::Future<Output = B>,
) -> (A, B) {
    tokio::join!(a, b)
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
    let config = harness::insecure_config("http://127.0.0.1:1", "w-dead", state.path());

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
    assert!(
        !lines.iter().any(|line| line.starts_with("swept")),
        "a clean state dir has nothing to sweep, and says nothing: {lines:?}"
    );
}

/// Startup sweeps the harness groups a crashed previous run left in the
/// journal (ADR-0010), before anything spawns, and says how many.
#[tokio::test]
async fn prepare_sweeps_a_crashed_runs_harness_groups() {
    let state = tempfile::tempdir().expect("a temp dir");
    let config = harness::insecure_config("http://127.0.0.1:1", "w-crashed", state.path());

    // The previous run: a harness group journaled and never stopped.
    let journal =
        horde_worker::Journal::open(&state.path().join(horde_worker::startup::JOURNAL_FILE))
            .expect("the journal opens");
    let spec = horde_worker::process::HarnessSpec {
        args: vec!["-c".to_owned(), "exec sleep 600".to_owned()],
        ledger: Some(std::sync::Arc::new(journal)),
        ..horde_worker::process::HarnessSpec::new("/bin/sh")
    };
    let crashed = horde_worker::SupervisedHarness::spawn(&spec)
        .await
        .expect("spawns");
    std::mem::forget(crashed);

    let mut lines = Vec::new();
    let prepared = horde_worker::startup::prepare(&config, &mut |line| lines.push(line))
        .await
        .expect("an insecure daemon prepares without a server");

    assert!(
        lines
            .iter()
            .any(|line| line == "swept 1 harness process group(s) a previous run left behind"),
        "the sweep is announced with its count: {lines:?}"
    );
    assert!(
        prepared
            .journal
            .harness_groups()
            .await
            .expect("readable")
            .is_empty(),
        "and the journal forgets what it swept"
    );
}
