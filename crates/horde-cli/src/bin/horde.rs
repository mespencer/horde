//! The `horde` binary: parse arguments, run the verb, choose an exit code.
//!
//! Everything else is library code in [`horde_cli::cli`] or `horde-observe`,
//! so the verbs can be tested against a real server without a subprocess and
//! the telemetry wiring can be tested without a terminal.

use std::process::ExitCode;

use clap::Parser;
use horde_cli::Format;
use horde_cli::cli::{Cli, run};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = Format::from_flag(cli.json);

    // OTLP export opts in via the environment (DESIGN §12); off installs no
    // subscriber at all, exactly as before. This binary owns its terminal —
    // stdout is the TUI, stderr the refusal channel — so the subscriber is
    // export-only, stamped with the enrolled principal when `--identity`
    // holds one.
    let identity = horde_observe::ServiceIdentity::new("horde")
        .with_principal(horde_cli::cli::enrolled_principal(cli.identity.as_deref()));
    let telemetry = match horde_observe::init_from_env(identity, |name| std::env::var(name).ok()) {
        Ok(telemetry) => telemetry,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(telemetry) = &telemetry {
        let installed =
            tracing::dispatcher::set_global_default(horde_observe::silent_subscriber(telemetry));
        if installed.is_err() {
            eprintln!("could not install the telemetry subscriber: one is already set");
            return ExitCode::FAILURE;
        }
    }

    let code = match run(&cli, format).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Decided in the library (`ClientError::report`), where it is
            // tested: the refusal, and the diagnosed cause beneath it.
            eprintln!("{}", error.report());
            ExitCode::FAILURE
        }
    };

    if let Some(telemetry) = telemetry {
        // The flush blocks, so it runs off the runtime's worker threads. The
        // verdict is discarded deliberately: a collector that is down must
        // not change an exit code.
        let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    }
    code
}
