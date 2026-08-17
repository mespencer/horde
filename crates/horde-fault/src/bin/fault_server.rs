//! The fault-server: the production server startup path as a killable
//! process the fault suite can always find.
//!
//! Test fixture, not product (`.cargo/mutants.toml` excludes it under the
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

use horde_server::startup::{Invocation, ServeConfig, invocation, operator_lines, serve};

fn main() -> ExitCode {
    match boot() {
        Ok(never) => match never {},
        Err(refusal) => {
            eprintln!("fault-server: {refusal}");
            ExitCode::FAILURE
        }
    }
}

/// Parse, serve, and hold the process up until a signal ends it.
fn boot() -> Result<Infallible, String> {
    let config = serve_config(std::env::args().skip(1))?;
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|error| format!("could not build a runtime: {error}"))?;
    runtime.block_on(serve_forever(&config))
}

/// The `--serve` invocation, through the production parser.
fn serve_config(arguments: impl Iterator<Item = String>) -> Result<ServeConfig, String> {
    match invocation(arguments)? {
        Invocation::Serve(config) => Ok(*config),
        Invocation::CheckOnly { .. } => Err("this fixture only serves; pass --serve".to_owned()),
    }
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
