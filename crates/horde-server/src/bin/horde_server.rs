//! The `horde-server` binary.
//!
//! Thin by design: runtime and subscriber policy live here, at the application
//! edge, and every decision lives in [`horde_server::startup`], where it can be
//! tested. A binary's own functions are reachable only by running the binary,
//! which coverage cannot see.

use std::process::ExitCode;

use horde_server::startup::{Invocation, invocation, open_log, serve};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    // Composed once, at the edge. `RUST_LOG` controls it; the default is quiet
    // enough to run in a terminal and loud enough to be useful.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,horde_server=debug"));
    // Logs on stderr: stdout is the operator interface (`--serve` prints the
    // addresses, the fingerprint, and the bootstrap code there), and a log
    // line interleaved into it breaks the scripts that read it.
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let parsed = match invocation(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };

    match parsed {
        Invocation::CheckOnly { database } => match open_log(&database) {
            Ok(_log) => ExitCode::SUCCESS,
            Err(error) => {
                tracing::error!(%error, "horde-server failed to start");
                ExitCode::FAILURE
            }
        },
        Invocation::Serve(config) => {
            let runtime = match tokio::runtime::Runtime::new() {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!(%error, "could not build a runtime");
                    return ExitCode::FAILURE;
                }
            };
            runtime.block_on(serve_until_interrupted(&config))
        }
    }
}

/// Serves until ctrl-c. The stdout lines are the operator interface: stable,
/// greppable keys — the addresses, the fingerprint every join pins, and the
/// bootstrap code when one was minted.
async fn serve_until_interrupted(config: &horde_server::startup::ServeConfig) -> ExitCode {
    let running = match serve(config).await {
        Ok(running) => running,
        Err(error) => {
            tracing::error!(%error, "horde-server failed to start");
            return ExitCode::FAILURE;
        }
    };

    println!("listening main={}", running.main.addr);
    println!("listening enrollment={}", running.enrollment.addr);
    println!("server-fingerprint {}", running.server_fingerprint);
    if let Some(code) = &running.bootstrap_code {
        println!("bootstrap-client-code {}", code.join_code());
    }

    match tokio::signal::ctrl_c().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "could not listen for shutdown");
            ExitCode::FAILURE
        }
    }
}
