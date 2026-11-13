//! The `horde` binary: parse arguments, run the verb, choose an exit code.
//!
//! Everything else is library code in [`horde_cli::cli`] or `horde-observe`,
//! so the verbs can be tested against a real server without a subprocess and
//! the telemetry wiring can be tested without a terminal.

use std::process::ExitCode;

use clap::Parser as _;
use horde_cli::Format;
use horde_cli::cli::{Cli, run};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            // Under `--json` a usage mistake is a JSON object on stderr like
            // every other refusal; help and version still print as clap says.
            match horde_cli::cli::argument_refusal(&error, std::env::args()) {
                Some(refusal) => {
                    eprintln!("{refusal}");
                    return ExitCode::from(2);
                }
                None => error.exit(),
            }
        }
    };
    let format = Format::from_flag(cli.json);

    // OTLP export opts in via the environment (DESIGN §12); off installs no
    // subscriber at all, exactly as before. This binary owns its terminal —
    // stdout is the TUI, stderr the refusal channel — so the subscriber is
    // export-only, stamped with the enrolled principal when `--identity`
    // holds one.
    let principal = horde_cli::cli::enrolled_principal(cli.identity.as_deref());
    let identity = horde_observe::ServiceIdentity::new("horde").with_principal(principal.clone());
    let telemetry = match horde_observe::init_from_env(identity, |name| std::env::var(name).ok()) {
        Ok(telemetry) => telemetry,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };
    // The CLI's own diagnostic log is opt-in (ADR-0015): `--log-file` or
    // `HORDE_LOG_FILE=1`. Its stdout is the TUI and its stderr the refusal
    // channel, so nothing is written to the terminal either way.
    let file = match horde_cli::logs::client_file_logging(
        horde_cli::logs::wants_log_file(
            cli.log_file,
            std::env::var(horde_observe::settings::CLIENT_FILE_VAR)
                .ok()
                .as_deref(),
        ),
        std::env::var_os("HOME").map(Into::into),
        principal,
        |name| std::env::var(name).ok(),
    ) {
        Ok(file) => file,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(dispatch) = horde_observe::silent_subscriber(telemetry.as_ref(), file.as_ref()) {
        let installed = tracing::dispatcher::set_global_default(dispatch);
        if installed.is_err() {
            eprintln!("could not install the telemetry subscriber: one is already set");
            return ExitCode::FAILURE;
        }
    }

    // Never the arguments: a join token rides argv.
    tracing::info!(target: "horde_cli", version = env!("CARGO_PKG_VERSION"), "invocation started");
    let code = match run(&cli, format).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::warn!(target: "horde_cli", %error, "invocation refused");
            // Decided in the library (`ClientError::report`), where it is
            // tested: the refusal, and the diagnosed cause beneath it.
            eprintln!("{}", error.report_for(format));
            ExitCode::FAILURE
        }
    };

    if let Some(file) = file {
        let _ = tokio::task::spawn_blocking(move || file.flush(std::time::Duration::from_secs(2)))
            .await;
    }
    if let Some(telemetry) = telemetry {
        // The flush blocks, so it runs off the runtime's worker threads. The
        // verdict is discarded deliberately: a collector that is down must
        // not change an exit code.
        let _ = tokio::task::spawn_blocking(move || telemetry.shutdown()).await;
    }
    code
}
