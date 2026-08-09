//! The `horde` binary.
//!
//! A thin argument parser over [`horde_cli::Client`]. Every verb here is one
//! library call and one render; nothing is computed in this file that a second
//! frontend would have to reimplement. That is what keeps DESIGN.md §11's
//! CLI-parity rule enforceable rather than aspirational.

use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use horde_cli::client::SyncOptions;
use horde_cli::{Client, ClientError, Format, render};
use horde_proto::v1::{DeliveryMode, PermissionOutcome};

/// Where a server is expected when none is named.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7777";

#[derive(Parser, Debug)]
#[command(
    name = "horde",
    about = "Drive the Horde control plane.",
    version,
    disable_help_subcommand = true
)]
struct Cli {
    /// Server address.
    #[arg(long, global = true, default_value = DEFAULT_ENDPOINT, env = "HORDE_ENDPOINT")]
    endpoint: String,

    /// Emit JSON instead of prose.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Handshake with the server and print what it is.
    Hello,
    /// Session lifecycle.
    #[command(subcommand)]
    Session(SessionCommand),
    /// Replay a session's events, optionally following.
    Sync(SyncArgs),
    /// Send an assignment or a note to an agent.
    Send(SendArgs),
    /// Cancel an agent's turn in flight.
    Interrupt(InterruptArgs),
    /// Answer a permission escalation from any attached client.
    Approve(ApproveArgs),
    /// Spawn an agent under an existing one.
    Spawn(SpawnArgs),
}

#[derive(Subcommand, Debug)]
enum SessionCommand {
    /// Create a session and its top-level agent.
    New {
        title: String,
        /// Makes a retry after a lost response safe.
        #[arg(long, default_value = "")]
        idempotency_key: String,
    },
    /// List sessions.
    List {
        #[arg(long)]
        include_archived: bool,
    },
    /// Show one session.
    Get { session_id: String },
    /// Archive a session. This, not a result, is what ends its agents.
    Archive {
        session_id: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
}

#[derive(Args, Debug)]
struct SyncArgs {
    session_id: String,
    /// First sequence wanted; 0 replays from the beginning.
    #[arg(long, default_value_t = 0)]
    from: u64,
    /// Keep listening after the replay instead of exiting.
    #[arg(long)]
    follow: bool,
}

#[derive(Args, Debug)]
struct SendArgs {
    agent_id: String,
    text: String,
    /// Send a note (owes no result) instead of an assignment.
    #[arg(long)]
    note: bool,
    /// Cancel the turn in flight rather than waiting for the turn boundary.
    #[arg(long)]
    interrupt: bool,
    #[arg(long, default_value = "")]
    idempotency_key: String,
}

#[derive(Args, Debug)]
struct InterruptArgs {
    agent_id: String,
    #[arg(long, default_value = "")]
    reason: String,
}

#[derive(Args, Debug)]
struct ApproveArgs {
    request_id: String,
    /// Refuse the call instead of permitting it.
    #[arg(long)]
    deny: bool,
    /// Shown to the agent when denying; recorded either way.
    #[arg(long, default_value = "")]
    reason: String,
    /// Widen the deterministic-allow layer for the rest of the session.
    #[arg(long)]
    remember: bool,
}

#[derive(Args, Debug)]
struct SpawnArgs {
    session_id: String,
    /// The spawner. Defaults to the session's top-level agent.
    #[arg(long)]
    parent: Option<String>,
    #[arg(long, default_value = "")]
    idempotency_key: String,
}

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

async fn run(cli: &Cli, format: Format) -> Result<(), ClientError> {
    let mut client = Client::connect(&cli.endpoint).await?;

    match &cli.command {
        Command::Hello => {
            let server = client.hello(env!("CARGO_PKG_VERSION")).await?;
            println!("{}", render::hello(&server, format));
        }

        Command::Session(SessionCommand::New {
            title,
            idempotency_key,
        }) => {
            let (summary, root) = client.create_session(title, idempotency_key).await?;
            println!("{}", render::created(&summary, &root, format));
        }
        Command::Session(SessionCommand::List { include_archived }) => {
            let summaries = client.list_sessions(*include_archived).await?;
            println!("{}", render::sessions(&summaries, format));
        }
        Command::Session(SessionCommand::Get { session_id }) => {
            let summary = client.get_session(session_id).await?;
            println!("{}", render::session(&summary, format));
        }
        Command::Session(SessionCommand::Archive { session_id, reason }) => {
            let summary = client.archive_session(session_id, reason).await?;
            println!("{}", render::session(&summary, format));
        }

        Command::Sync(args) => {
            let options = SyncOptions::new(args.session_id.clone())
                .from_sequence(args.from)
                .following(args.follow);

            // Printed as they arrive rather than collected: `--follow` must
            // stream, and a session that never ends must not be buffered.
            client
                .sync(
                    &options,
                    |envelope| println!("{}", render::event(envelope, format)),
                    |head| {
                        if !args.follow {
                            return;
                        }
                        // Only meaningful when following; otherwise the stream
                        // ending already says the replay is done.
                        eprintln!("-- caught up at {head} --");
                    },
                )
                .await?;
        }

        Command::Send(args) => {
            let delivery = if args.interrupt {
                DeliveryMode::Interrupt
            } else {
                DeliveryMode::TurnBoundary
            };
            let response = if args.note {
                client
                    .send_note(&args.agent_id, &args.text, delivery, &args.idempotency_key)
                    .await?
            } else {
                client
                    .send_assignment(&args.agent_id, &args.text, delivery, &args.idempotency_key)
                    .await?
            };
            println!("{}", render::sent(&response, format));
        }

        Command::Interrupt(args) => {
            let sequence = client.interrupt(&args.agent_id, &args.reason).await?;
            println!("{}", render::sequence("interrupted", sequence, format));
        }

        Command::Approve(args) => {
            let outcome = if args.deny {
                PermissionOutcome::Deny
            } else {
                PermissionOutcome::Allow
            };
            let sequence = client
                .answer_permission(&args.request_id, outcome, &args.reason, args.remember)
                .await?;
            let verb = if args.deny { "denied" } else { "allowed" };
            println!("{}", render::sequence(verb, sequence, format));
        }

        Command::Spawn(args) => {
            let response = client
                .spawn_agent(
                    &args.session_id,
                    args.parent.as_deref(),
                    &args.idempotency_key,
                )
                .await?;
            println!("{}", render::spawned(&response, format));
        }
    }

    Ok(())
}
