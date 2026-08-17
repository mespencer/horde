//! The `horde-server` binary, run for real.
//!
//! `main` is reachable only by running the binary, so nothing else in this
//! crate can hold it to anything: a mutation replacing its body with a
//! successful exit code survived every other test. What it decides — the exit
//! code — is only observable from outside the process.

use tokio::process::Command;

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Runs the real binary. `CARGO_BIN_EXE_horde-server` is the one cargo just
/// built, so this can never test a stale copy on `PATH`.
///
/// Async because a blocking `std::process::Command::output` would stall the
/// runtime this test is running on.
async fn horde_server(argument: Option<&str>) -> Setup<std::process::ExitStatus> {
    Ok(horde_server_with_env(argument, &[]).await?.status)
}

/// Same, through the testkit's scrubbed runner, with the whole output kept:
/// the telemetry tests read the refusal off stderr, not just the exit code.
async fn horde_server_with_env(
    argument: Option<&str>,
    environment: &[(&str, &str)],
) -> Setup<std::process::Output> {
    let arguments: Vec<&str> = argument.into_iter().collect();
    horde_server_testkit::otlp_scrubbed_run(
        env!("CARGO_BIN_EXE_horde-server"),
        &arguments,
        environment,
    )
    .await
}

#[tokio::test]
async fn opening_a_usable_database_exits_successfully() {
    let directory = tempfile::tempdir().expect("a temp dir");
    let path = directory.path().join("horde.db");

    let status = horde_server(Some(&path.to_string_lossy()))
        .await
        .expect("the binary must run");

    assert!(
        status.success(),
        "opening a fresh database is the ordinary case and must exit zero"
    );
    assert!(
        path.exists(),
        "startup must create the database it was given"
    );
}

/// The case that makes `main`'s body matter: a startup failure must reach the
/// exit code, or a supervisor restarting on failure would never restart.
#[tokio::test]
async fn a_database_that_cannot_be_opened_exits_nonzero() {
    let directory = tempfile::tempdir().expect("a temp dir");

    // A directory is not a database file.
    let status = horde_server(Some(&directory.path().to_string_lossy()))
        .await
        .expect("the binary must run");

    assert!(
        !status.success(),
        "a server that cannot open its log must not report success"
    );
}

/// One check-only run against a fresh database with the given OTLP
/// environment — the arrangement all three telemetry tests share.
async fn check_only_with_otlp(environment: &[(&str, &str)]) -> Setup<std::process::Output> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("horde.db");
    horde_server_with_env(Some(&path.to_string_lossy()), environment).await
}

/// OTLP misconfiguration is a startup refusal (DESIGN §12 wiring): a sample
/// ratio with no endpoint is the shape a mistyped variable takes, and a
/// server that silently exported nothing would be worse than one that says
/// which variable to fix.
#[tokio::test]
async fn a_sample_ratio_without_an_endpoint_refuses_startup() {
    let output = check_only_with_otlp(&[("HORDE_OTLP_SAMPLE", "0.5")])
        .await
        .expect("the binary must run");

    assert!(!output.status.success(), "misconfiguration must refuse");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("HORDE_OTLP_SAMPLE"),
        "the refusal must name the variable to fix: {stderr}"
    );
}

/// An endpoint the exporter cannot even parse is the same kind of refusal.
#[tokio::test]
async fn an_unparseable_otlp_endpoint_refuses_startup() {
    let output = check_only_with_otlp(&[("HORDE_OTLP_ENDPOINT", "not a uri")])
        .await
        .expect("the binary must run");

    assert!(!output.status.success(), "an unusable endpoint must refuse");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not a uri"),
        "the refusal must show what it could not use: {stderr}"
    );
}

/// The graceful-degradation contract at the binary boundary: a well-formed
/// endpoint nobody listens on costs telemetry, never the server. Same
/// invocation as `opening_a_usable_database_exits_successfully`, plus a dead
/// collector — the exit code must not change.
#[tokio::test]
async fn a_dead_collector_does_not_change_the_exit_code() {
    let output = check_only_with_otlp(&[("HORDE_OTLP_ENDPOINT", "http://127.0.0.1:1")])
        .await
        .expect("the binary must run");

    assert!(
        output.status.success(),
        "an unreachable collector must never take the server down with it: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A mistyped flag must refuse loudly, not silently serve with defaults.
#[tokio::test]
async fn an_unknown_flag_exits_nonzero() {
    let status = horde_server(Some("--bnid"))
        .await
        .expect("the binary must run");
    assert!(!status.success(), "an unknown flag must be a refusal");
}

/// `--serve` for real: the networked server comes up, prints its operator
/// interface — addresses, fingerprint, and the bootstrap code (DESIGN §3's
/// first-client answer) — and keeps serving until killed.
#[tokio::test]
async fn serve_prints_the_operator_interface_and_stays_up() -> Setup<()> {
    use tokio::io::AsyncBufReadExt;

    let directory = tempfile::tempdir()?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_horde-server"));
    command
        .arg(directory.path().join("horde.db").as_os_str())
        .args([
            "--serve",
            "--bind",
            "127.0.0.1:0",
            "--enrollment-bind",
            "127.0.0.1:0",
        ])
        .arg("--state-dir")
        .arg(directory.path().join("state").as_os_str())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().ok_or("stdout must be piped")?;

    // Bounded by line count and by clock: four known lines are owed, and a
    // server that prints something else first has already failed.
    let mut lines = tokio::io::BufReader::new(stdout).lines();
    let mut seen = Vec::new();
    for _ in 0_u8..4 {
        let line = tokio::time::timeout(std::time::Duration::from_secs(30), lines.next_line())
            .await
            .map_err(|_| "timed out waiting for the operator interface")??
            .ok_or("stdout ended before the operator interface was printed")?;
        seen.push(line);
    }

    assert!(seen[0].starts_with("listening main=127.0.0.1:"), "{seen:?}");
    assert!(
        seen[1].starts_with("listening enrollment=127.0.0.1:"),
        "{seen:?}"
    );
    let fingerprint = seen[2]
        .strip_prefix("server-fingerprint ")
        .ok_or_else(|| format!("line 3 must be the fingerprint: {seen:?}"))?;
    assert_eq!(fingerprint.len(), 64, "{seen:?}");
    let code = seen[3]
        .strip_prefix("bootstrap-client-code ")
        .ok_or_else(|| format!("line 4 must be the bootstrap code: {seen:?}"))?;
    assert!(
        code.ends_with(fingerprint),
        "the code embeds the fingerprint the joiner pins: {seen:?}"
    );

    // Still serving, not exited: printing the interface must not be the
    // end. A ceilinged wait on the exit that must NOT come — a server that
    // returns from its shutdown wait early exits here and fails loudly.
    let exited = tokio::time::timeout(std::time::Duration::from_secs(2), child.wait()).await;
    assert!(
        exited.is_err(),
        "the server must keep serving after printing its interface; it exited: {exited:?}"
    );
    child.kill().await?;
    Ok(())
}
