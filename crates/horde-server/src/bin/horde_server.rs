//! The `horde-server` binary.
//!
//! Thin by design: runtime and subscriber policy live here, at the application
//! edge, and every decision lives in [`horde_server::startup`], where it can be
//! tested. A binary's own functions are reachable only by running the binary,
//! which coverage cannot see.

use std::process::ExitCode;

use horde_server::startup::{database_path, open_log};
use tracing_subscriber::EnvFilter;

fn main() -> ExitCode {
    // Composed once, at the edge. `RUST_LOG` controls it; the default is quiet
    // enough to run in a terminal and loud enough to be useful.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,horde_server=debug"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    match open_log(&database_path(std::env::args().skip(1))) {
        Ok(_log) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "horde-server failed to start");
            ExitCode::FAILURE
        }
    }
}
