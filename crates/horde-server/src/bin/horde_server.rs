//! The `horde-server` binary.
//!
//! Thin by design: runtime and subscriber policy live here, at the application
//! edge, and everything else is library code that merely emits `tracing` data.

use std::process::ExitCode;

use horde_server::EventLog;
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    // Composed once, at the edge. `RUST_LOG` controls it; the default is quiet
    // enough to run in a terminal and loud enough to be useful.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,horde_server=debug"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "horde-server failed to start");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), horde_server::ServerError> {
    // DESIGN.md §3 forbids binding beyond localhost before auth exists, so the
    // networked entrypoint lands with enrollment in wave 5. Until then this
    // binary proves the log opens and reports what it found.
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "horde.db".to_owned());
    let log = EventLog::open(&path)?;
    tracing::info!(%path, "event log opened");
    drop(log);
    Ok(())
}
