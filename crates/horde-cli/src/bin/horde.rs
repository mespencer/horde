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
    let (telemetry, file) = match horde_cli::logs::observability(
        cli.log_file,
        horde_cli::cli::enrolled_principal(cli.identity.as_deref()),
        |name| std::env::var(name).ok(),
        std::env::var_os("HOME").map(Into::into),
    ) {
        Ok(installed) => installed,
        Err(refusal) => {
            eprintln!("{refusal}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(dispatch) = horde_observe::silent_subscriber(telemetry.as_ref(), file.as_ref())
        && tracing::dispatcher::set_global_default(dispatch).is_err()
    {
        eprintln!("could not install the telemetry subscriber: one is already set");
        return ExitCode::FAILURE;
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

    // Off the runtime's worker threads: both flushes block.
    let _ = tokio::task::spawn_blocking(move || {
        horde_cli::logs::finish_observability(
            telemetry,
            file.as_ref(),
            std::time::Duration::from_secs(2),
        );
    })
    .await;
    code
}
