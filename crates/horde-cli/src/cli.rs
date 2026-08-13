//! The command model and its dispatch.
//!
//! The clap types live here rather than in `bin/horde.rs` so the verbs are
//! ordinary library code: a test can build a `Cli` and run it against a real
//! server without a subprocess, and the future TUI can reuse the same command
//! model instead of restating it. The binary is left as argument parsing and an
//! exit code.

use clap::{Args, Parser, Subcommand};
use horde_proto::v1::{DeliveryMode, PermissionOutcome};

use crate::client::{PermissionAnswer, SyncOptions};
use crate::{Client, ClientError, Format, render};

/// Where a server is expected when none is named.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7777";

#[derive(Parser, Debug)]
#[command(
    name = "horde",
    about = "Drive the Horde control plane.",
    version,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Server address.
    #[arg(long, global = true, default_value = DEFAULT_ENDPOINT, env = "HORDE_ENDPOINT")]
    pub endpoint: String,

    /// Emit JSON instead of prose.
    #[arg(long, global = true)]
    pub json: bool,

    /// No verb opens the interactive multi-session overview (DESIGN.md §11:
    /// two zoom levels, one binary — `horde` is the overview, `horde attach`
    /// the per-session view).
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
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
    /// Attach to a session interactively. `sync --follow` is the headless,
    /// scriptable form of the same subscription.
    Attach(AttachArgs),
}

#[derive(Subcommand, Debug)]
pub enum SessionCommand {
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
pub struct SyncArgs {
    pub session_id: String,
    /// First sequence wanted; 0 replays from the beginning.
    #[arg(long, default_value_t = 0)]
    pub from: u64,
    /// Keep listening after the replay instead of exiting.
    #[arg(long)]
    pub follow: bool,
}

#[derive(Args, Debug)]
pub struct SendArgs {
    pub agent_id: String,
    pub text: String,
    /// Send a note (owes no result) instead of an assignment.
    #[arg(long)]
    pub note: bool,
    /// Cancel the turn in flight rather than waiting for the turn boundary.
    #[arg(long)]
    pub interrupt: bool,
    #[arg(long, default_value = "")]
    pub idempotency_key: String,
}

#[derive(Args, Debug)]
pub struct InterruptArgs {
    pub agent_id: String,
    #[arg(long, default_value = "")]
    pub reason: String,
}

#[derive(Args, Debug)]
pub struct ApproveArgs {
    pub request_id: String,
    /// Refuse the call instead of permitting it.
    #[arg(long)]
    pub deny: bool,
    /// Shown to the agent when denying; recorded either way.
    #[arg(long, default_value = "")]
    pub reason: String,
    /// Widen the deterministic-allow layer for the rest of the session.
    #[arg(long)]
    pub remember: bool,
    /// Allow the call with these arguments instead of the proposed ones, as a
    /// JSON object — clamping a path, dropping a flag.
    ///
    /// The harness runs what this says, so it narrows the call rather than
    /// annotating it. Meaningless with `--deny`, and refused rather than
    /// ignored there.
    #[arg(long, value_name = "JSON")]
    pub update_input: Option<String>,
}

#[derive(Args, Debug)]
pub struct AttachArgs {
    pub session_id: String,
    /// Scope the view to this agent's subtree instead of the whole session.
    /// Prompts from elsewhere show only as a passive badge.
    #[arg(long)]
    pub agent: Option<String>,
}

#[derive(Args, Debug)]
pub struct SpawnArgs {
    pub session_id: String,
    /// The spawner. Defaults to the session's top-level agent.
    #[arg(long)]
    pub parent: Option<String>,
    #[arg(long, default_value = "")]
    pub idempotency_key: String,
}

/// Dispatches to one handler per verb.
///
/// Every arm is a single call and nothing is computed here, which keeps this
/// function's branching equal to the number of verbs. An earlier version
/// inlined each verb's body and scored 164 on CRAP — readable enough in a diff,
/// impossible to bring under the complexity bar however well it was covered.
pub async fn run(cli: &Cli, format: Format) -> Result<(), ClientError> {
    let mut client = Client::connect(&cli.endpoint).await?;
    let client = &mut client;

    match &cli.command {
        None => crate::tui::launch(client.clone(), crate::tui::Launch::Overview).await,
        Some(Command::Attach(args)) => {
            crate::tui::launch(
                client.clone(),
                crate::tui::Launch::Attach {
                    session_id: args.session_id.clone(),
                    agent_id: args.agent.clone(),
                },
            )
            .await
        }
        Some(Command::Hello) => run_hello(client, format).await,
        Some(Command::Session(command)) => run_session(client, command, format).await,
        Some(Command::Sync(args)) => run_sync(client, args, format).await,
        Some(Command::Send(args)) => run_send(client, args, format).await,
        Some(Command::Interrupt(args)) => run_interrupt(client, args, format).await,
        Some(Command::Approve(args)) => run_approve(client, args, format).await,
        Some(Command::Spawn(args)) => run_spawn(client, args, format).await,
    }
}

/// The delivery mode a `send` asks for.
///
/// Extracted so the choice is testable without a server; it is the kind of
/// one-line decision that is silently inverted in a refactor.
#[must_use]
pub fn delivery_for(interrupt: bool) -> DeliveryMode {
    if interrupt {
        DeliveryMode::Interrupt
    } else {
        DeliveryMode::TurnBoundary
    }
}

/// The outcome an `approve` records, and the word used to report it.
#[must_use]
pub fn outcome_for(deny: bool) -> (PermissionOutcome, &'static str) {
    if deny {
        (PermissionOutcome::Deny, "denied")
    } else {
        (PermissionOutcome::Allow, "allowed")
    }
}

async fn run_hello(client: &mut Client, format: Format) -> Result<(), ClientError> {
    let server = client.hello(env!("CARGO_PKG_VERSION")).await?;
    println!("{}", render::hello(&server, format));
    Ok(())
}

async fn run_interrupt(
    client: &mut Client,
    args: &InterruptArgs,
    format: Format,
) -> Result<(), ClientError> {
    let sequence = client.interrupt(&args.agent_id, &args.reason).await?;
    println!("{}", render::sequence("interrupted", sequence, format));
    Ok(())
}

async fn run_spawn(
    client: &mut Client,
    args: &SpawnArgs,
    format: Format,
) -> Result<(), ClientError> {
    let response = client
        .spawn_agent(
            &args.session_id,
            args.parent.as_deref(),
            &args.idempotency_key,
        )
        .await?;
    println!("{}", render::spawned(&response, format));
    Ok(())
}

async fn run_session(
    client: &mut Client,
    command: &SessionCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        SessionCommand::New {
            title,
            idempotency_key,
        } => {
            let (summary, root) = client.create_session(title, idempotency_key).await?;
            println!("{}", render::created(&summary, &root, format));
        }
        SessionCommand::List { include_archived } => {
            let summaries = client.list_sessions(*include_archived).await?;
            println!("{}", render::sessions(&summaries, format));
        }
        SessionCommand::Get { session_id } => {
            let summary = client.get_session(session_id).await?;
            println!("{}", render::session(&summary, format));
        }
        SessionCommand::Archive { session_id, reason } => {
            let summary = client.archive_session(session_id, reason).await?;
            println!("{}", render::session(&summary, format));
        }
    }
    Ok(())
}

async fn run_sync(client: &mut Client, args: &SyncArgs, format: Format) -> Result<(), ClientError> {
    let options = SyncOptions::new(args.session_id.clone())
        .from_sequence(args.from)
        .following(args.follow);
    let follow = args.follow;

    // Printed as they arrive rather than collected: `--follow` must stream, and
    // a session that never ends must not be buffered.
    client
        .sync(
            &options,
            |envelope| println!("{}", render::event(envelope, format)),
            |head| {
                // Only meaningful when following; otherwise the stream ending
                // already says the replay is done. Stderr, so it cannot be
                // mistaken for an event by a script reading stdout.
                if follow {
                    eprintln!("-- caught up at {head} --");
                }
            },
        )
        .await?;
    Ok(())
}

async fn run_send(client: &mut Client, args: &SendArgs, format: Format) -> Result<(), ClientError> {
    let delivery = delivery_for(args.interrupt);
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
    Ok(())
}

async fn run_approve(
    client: &mut Client,
    args: &ApproveArgs,
    format: Format,
) -> Result<(), ClientError> {
    let (outcome, verb) = outcome_for(args.deny);

    // Parsed before the RPC so a typo in the JSON is a local complaint rather
    // than a round trip that comes back as a server-side refusal.
    let narrowing: Option<serde_json::Value> = match &args.update_input {
        Some(raw) => Some(
            serde_json::from_str(raw).map_err(|source| ClientError::NarrowingJson { source })?,
        ),
        None => None,
    };

    let sequence = client
        .answer_permission(PermissionAnswer {
            request_id: &args.request_id,
            outcome,
            reason: &args.reason,
            remember_for_session: args.remember,
            updated_input: narrowing.as_ref(),
        })
        .await?;
    println!("{}", render::sequence(verb, sequence, format));
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use horde_proto::v1::{DeliveryMode, PermissionOutcome};

    use super::{Cli, Command, SessionCommand, delivery_for, outcome_for};

    #[test]
    fn delivery_defaults_to_the_turn_boundary_and_flips_only_when_asked() {
        assert_eq!(delivery_for(false), DeliveryMode::TurnBoundary);
        assert_eq!(delivery_for(true), DeliveryMode::Interrupt);
    }

    #[test]
    fn approving_allows_and_denying_denies() {
        assert_eq!(outcome_for(false), (PermissionOutcome::Allow, "allowed"));
        assert_eq!(outcome_for(true), (PermissionOutcome::Deny, "denied"));
    }

    /// The arguments a script depends on must survive parsing. Each of these
    /// would otherwise parse cleanly and be silently ignored.
    #[test]
    fn flags_reach_the_command_they_belong_to() {
        let cli = Cli::try_parse_from(["horde", "--json", "sync", "s1", "--from", "7", "--follow"])
            .expect("valid arguments");
        assert!(cli.json);
        match cli.command {
            Some(Command::Sync(args)) => {
                assert_eq!(args.session_id, "s1");
                assert_eq!(args.from, 7);
                assert!(args.follow);
            }
            other => panic!("expected sync, got {other:?}"),
        }
    }

    #[test]
    fn send_defaults_to_an_assignment_and_notes_are_opt_in() {
        let assignment =
            Cli::try_parse_from(["horde", "send", "a1", "do the thing"]).expect("valid arguments");
        match assignment.command {
            Some(Command::Send(args)) => assert!(!args.note && !args.interrupt),
            other => panic!("expected send, got {other:?}"),
        }

        let note = Cli::try_parse_from(["horde", "send", "a1", "fyi", "--note", "--interrupt"])
            .expect("valid arguments");
        match note.command {
            Some(Command::Send(args)) => assert!(args.note && args.interrupt),
            other => panic!("expected send, got {other:?}"),
        }
    }

    #[test]
    fn archived_sessions_are_hidden_unless_requested() {
        let cli = Cli::try_parse_from(["horde", "session", "list", "--include-archived"])
            .expect("valid arguments");
        match cli.command {
            Some(Command::Session(SessionCommand::List { include_archived })) => {
                assert!(include_archived);
            }
            other => panic!("expected session list, got {other:?}"),
        }
    }

    #[test]
    fn the_endpoint_has_a_localhost_default() {
        let cli = Cli::try_parse_from(["horde", "hello"]).expect("valid arguments");
        assert!(cli.endpoint.starts_with("http://127.0.0.1"));
        assert!(!cli.json, "human output is the default");
    }

    /// clap's own invariants: a typo must fail rather than silently selecting
    /// something else.
    #[test]
    fn an_unknown_verb_is_rejected() {
        assert!(Cli::try_parse_from(["horde", "sessions"]).is_err());
        assert!(
            Cli::try_parse_from(["horde", "sync"]).is_err(),
            "sync needs a session"
        );
    }

    /// `horde` with no verb is the interactive overview, not a usage error —
    /// DESIGN.md §11's top-level zoom.
    #[test]
    fn no_verb_means_the_interactive_overview() {
        let cli = Cli::try_parse_from(["horde"]).expect("bare horde is valid");
        assert!(cli.command.is_none());
    }

    #[test]
    fn attach_takes_a_session_and_optionally_an_agent_scope() {
        let cli = Cli::try_parse_from(["horde", "attach", "s1", "--agent", "a9"])
            .expect("valid arguments");
        match cli.command {
            Some(Command::Attach(args)) => {
                assert_eq!(args.session_id, "s1");
                assert_eq!(args.agent.as_deref(), Some("a9"));
            }
            other => panic!("expected attach, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from(["horde", "attach"]).is_err(),
            "attach needs a session"
        );
    }
}
