//! The `horde` binary: parse arguments, run the verb, choose an exit code.
//!
//! Everything else is library code in [`horde_cli::cli`], so the verbs can be
//! tested against a real server without a subprocess.

use std::process::ExitCode;

use clap::Parser;
use horde_cli::Format;
use horde_cli::cli::{Cli, run};

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let format = Format::from_flag(cli.json);

    match run(&cli, format).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // An unbuilt verb is a roadmap fact, not a fault. Saying "not built
            // yet" beats printing a raw status a user would file a bug about.
            if error.is_unimplemented() {
                eprintln!("not built yet: {error}");
            } else {
                eprintln!("error: {error}");
            }
            ExitCode::FAILURE
        }
    }
}
