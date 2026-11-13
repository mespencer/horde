//! The fault-server: the production server startup path as a killable
//! process the fault suite can always find.
//!
//! Test fixture, not product (`gamma.toml` excludes it under the
//! same criterion as the worker-runner). The shipped `horde-server` binary
//! is deliberately thin — argument parsing, a subscriber, an exit code —
//! with every decision in `horde_server::startup`, and this fixture
//! composes exactly those pieces: `invocation` parses the same command
//! line, `serve` stands up the same log, CA, blob store and mTLS
//! listeners, `operator_lines` prints the same stdout contract. What a
//! `kill -9` interrupts here is the production serve stack.
//!
//! Why not spawn the real binary: another package's `[[bin]]` is not built
//! when this package's tests build (cargo-mutants' scratch trees proved it
//! by baseline failure), and a suite that skips when infrastructure is
//! missing is forbidden outright (TESTING.md §1). A fixture binary of this
//! package is built everywhere its tests are.

use std::convert::Infallible;
use std::process::ExitCode;

use horde_fault::fixture_serve_config;
use horde_server::startup::{ServeConfig, operator_lines, serve};

fn main() -> ExitCode {
    match boot() {
        Ok(never) => match never {},
        Err(refusal) => {
            eprintln!("fault-server: {refusal}");
            ExitCode::FAILURE
        }
    }
}

/// Parse, serve, and hold the process up until a signal ends it. The
/// parsing lives in the library (`fixture_serve_config`), under its tests —
/// a binary's own functions are reachable only by running the binary.
fn boot() -> Result<Infallible, String> {
    let config = fixture_serve_config(std::env::args().skip(1))?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("could not build a runtime: {error}"))?;
    runtime.block_on(serve_forever(&config))
}

/// Serves and parks. The suite ends this process with SIGKILL — that is
/// its purpose — so nothing here listens for a polite shutdown.
async fn serve_forever(config: &ServeConfig) -> Result<Infallible, String> {
    let running = serve(config).await.map_err(|error| error.to_string())?;
    for line in operator_lines(&running) {
        println!("{line}");
    }
    Ok(std::future::pending().await)
}
