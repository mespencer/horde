//! The `horde-worker` binary.
//!
//! Thin by design, like `horde-server`'s: runtime, subscriber and signal
//! policy live here at the application edge, and every decision lives in
//! [`horde_worker::startup`] where it can be tested. A binary's own functions
//! are reachable only by running the binary — which is what
//! `tests/worker_suite/daemon.rs` does.

use std::process::ExitCode;

use horde_worker::startup;
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
    let config = startup::invocation(std::env::args().skip(1))?;

    // The runtime exists before telemetry because the OTLP exporter's gRPC
    // channel lives on it — the same startup order as the other binaries.
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("could not build a runtime: {error}"))?;
    let telemetry = {
        let _guard = runtime.enter();
        horde_observe::init_from_env(
            horde_observe::ServiceIdentity::new("horde-worker")
                .with_principal(startup::enrolled_principal(&config.state_dir)),
            |name| std::env::var(name).ok(),
        )
        .map_err(|error| error.to_string())?
    };

    // Logs go to stderr: stdout is the operator interface (`registered
    // worker=…`, `shutdown reaped=…`), and a log line interleaved into it
    // breaks the scripts that read it.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,horde_worker=debug"));
    tracing::dispatcher::set_global_default(horde_observe::stderr_subscriber(
        filter,
        telemetry.as_ref(),
    ))
    .map_err(|error| format!("could not install the subscriber: {error}"))?;

    let code = runtime.block_on(serve_until_terminated(&config));

    if let Some(telemetry) = telemetry {
        // From main's own thread, with the runtime still alive for the flush
        // to travel its channel. The verdict is discarded deliberately: a
        // collector that is down must not change an exit code.
        let _ = telemetry.shutdown();
    }
    Ok(code)
}

/// Prepares the daemon and serves until SIGTERM/ctrl-c ends it, or a fatal
/// refusal does. All the decisions are in `startup`; this is plumbing an
/// exit code, like `main`.
async fn serve_until_terminated(config: &startup::WorkerConfig) -> ExitCode {
    let mut announce = |line: String| println!("{line}");
    let prepared = match startup::prepare(config, &mut announce).await {
        Ok(prepared) => prepared,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };
    match startup::run(prepared, termination(), &mut announce).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(reason) => {
            eprintln!("{reason}");
            ExitCode::FAILURE
        }
    }
}

/// Resolves when the process is asked to stop: SIGTERM (how a supervisor
/// stops a daemon) or ctrl-c (how an operator does).
async fn termination() {
    let mut sigterm = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        Ok(sigterm) => sigterm,
        Err(error) => {
            // Without a SIGTERM handler the default disposition kills the
            // process anyway; ctrl-c still shuts down cleanly.
            tracing::warn!(%error, "could not listen for SIGTERM");
            wait_for_ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
}

/// Blocks on ctrl-c alone — the fallback when SIGTERM cannot be listened for.
async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "could not listen for shutdown");
        // Pending forever: with no signal to hear, ending would stop a
        // healthy daemon; the operator still holds SIGKILL.
        std::future::pending::<()>().await;
    }
}
