//! The `horde-worker` binary.
//!
//! Thin by design, like `horde-server`'s: runtime, subscriber and signal
//! policy live here at the application edge, and every decision lives in
//! [`horde_worker::startup`] where it can be tested. A binary's own functions
//! are reachable only by running the binary — which is what
//! `tests/worker_suite/daemon.rs` does.

use std::process::ExitCode;

use horde_worker::startup;

fn main() -> ExitCode {
    // A `String` error is a startup refusal, printed before any subscriber
    // exists to log it.
    let parsed = horde_worker::flags::parse(
        std::env::args().skip(1),
        |name| std::env::var(name).ok(),
        std::env::var_os("HOME").map(std::path::PathBuf::from),
    );
    let outcome = parsed.and_then(|parsed| match parsed {
        horde_worker::flags::Parsed::Print(text) => {
            print!("{text}");
            Ok(ExitCode::SUCCESS)
        }
        horde_worker::flags::Parsed::Run(config) => {
            startup::with_observability(&config.state_dir, |runtime| {
                runtime.block_on(serve_until_terminated(&config))
            })
        }
    });
    outcome.unwrap_or_else(|refusal| {
        eprintln!("{refusal}");
        ExitCode::FAILURE
    })
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
/// stops a daemon) or ctrl-c (how an operator does). The racing itself is
/// [`startup::first_of`], where it is tested; failing to even listen for
/// SIGTERM degrades to ctrl-c alone — the default disposition would kill the
/// process on SIGTERM anyway.
async fn termination() {
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => {
            startup::first_of(wait_for_ctrl_c(), async move {
                let _ = sigterm.recv().await;
            })
            .await;
        }
        Err(error) => {
            tracing::warn!(%error, "could not listen for SIGTERM; ctrl-c only");
            wait_for_ctrl_c().await;
        }
    }
}

/// Parks on ctrl-c. A registration failure parks forever instead: with no
/// signal to hear, returning would stop a healthy daemon, and the operator
/// still holds SIGKILL.
async fn wait_for_ctrl_c() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(%error, "could not listen for shutdown");
        std::future::pending::<()>().await;
    }
}
