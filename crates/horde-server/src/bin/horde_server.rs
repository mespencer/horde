//! The `horde-server` binary.
//!
//! Thin by design: runtime and subscriber policy live here, at the application
//! edge, and every decision lives in [`horde_server::startup`] or in
//! `horde-observe`, where it can be tested. A binary's own functions are
//! reachable only by running the binary, which coverage cannot see — which is
//! also why `tests/binary.rs` runs it for real.

use std::process::ExitCode;

use horde_server::startup::{Invocation, invocation, open_log, serve};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(refusal) => {
            eprintln!("{refusal}");
            ExitCode::FAILURE
        }
    }
}

/// Everything between the arguments and the exit code. A `String` error is a
/// startup refusal, printed before any subscriber exists to log it.
fn run() -> Result<ExitCode, String> {
    let parsed = invocation(std::env::args().skip(1))?;

    // The runtime exists before telemetry because the OTLP exporter's gRPC
    // channel lives on it; `CheckOnly` pays for an idle runtime, which is
    // nothing, to keep one startup order for both invocations.
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("could not build a runtime: {error}"))?;
    let telemetry = {
        let _guard = runtime.enter();
        horde_observe::init_from_env(
            horde_observe::ServiceIdentity::new("horde-server"),
            |name| std::env::var(name).ok(),
        )
        .map_err(|error| error.to_string())?
    };

    // Composed once, at the edge. `RUST_LOG` controls the console; the
    // default is quiet enough to run in a terminal and loud enough to be
    // useful. Logs go to stderr: stdout is the operator interface (`--serve`
    // prints the addresses, the fingerprint, and the bootstrap code there),
    // and a log line interleaved into it breaks the scripts that read it.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,horde_server=debug"));
    tracing::dispatcher::set_global_default(horde_observe::stderr_subscriber(
        filter,
        telemetry.as_ref(),
    ))
    .map_err(|error| format!("could not install the subscriber: {error}"))?;

    let code = match parsed {
        Invocation::CheckOnly { database } => check_only(&database),
        Invocation::Serve(config) => runtime.block_on(serve_until_interrupted(&config)),
    };

    if let Some(telemetry) = telemetry {
        // From main's own thread, with the runtime still alive for the flush
        // to travel its channel. The verdict is discarded deliberately: a
        // collector that is down must not change an exit code.
        let _ = telemetry.shutdown();
    }
    Ok(code)
}

/// The health-check invocation: open the log, report by exit code.
fn check_only(database: &str) -> ExitCode {
    match open_log(database) {
        Ok(_log) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "horde-server failed to start");
            ExitCode::FAILURE
        }
    }
}

/// Serves until ctrl-c. What is printed is decided in
/// [`horde_server::startup::operator_lines`], where it is tested; this stays
/// plumbing an exit code, like `main`.
async fn serve_until_interrupted(config: &horde_server::startup::ServeConfig) -> ExitCode {
    match serve(config).await {
        Err(error) => {
            tracing::error!(%error, "horde-server failed to start");
            ExitCode::FAILURE
        }
        Ok(running) => announce_and_wait(running).await,
    }
}

/// Prints the operator interface, then holds the server up until shutdown.
async fn announce_and_wait(running: horde_server::startup::RunningServer) -> ExitCode {
    for line in horde_server::startup::operator_lines(&running) {
        println!("{line}");
    }
    wait_for_shutdown().await
}

/// Blocks on ctrl-c; the running server lives exactly as long as this does.
async fn wait_for_shutdown() -> ExitCode {
    match tokio::signal::ctrl_c().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "could not listen for shutdown");
            ExitCode::FAILURE
        }
    }
}
