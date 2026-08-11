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
    let mut command = Command::new(env!("CARGO_BIN_EXE_horde-server"));
    if let Some(argument) = argument {
        command.arg(argument);
    }
    Ok(command.output().await?.status)
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
