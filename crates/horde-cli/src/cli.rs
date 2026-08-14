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

    /// Directory holding this machine's enrolled identity (DESIGN §3).
    /// When set, every connection is mTLS; `enroll join` writes here.
    #[arg(long, global = true, env = "HORDE_IDENTITY")]
    pub identity: Option<std::path::PathBuf>,

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
    /// The MCP Hub catalog: user MCP servers proxied onto every agent's
    /// endpoint (ADR-0004).
    #[command(subcommand)]
    Mcp(McpCommand),
    /// Worker administration: enrollment codes and revocation (DESIGN §3).
    #[command(subcommand)]
    Worker(WorkerCommand),
    /// This machine's own identity: joining, and client codes for others.
    #[command(subcommand)]
    Enroll(EnrollCommand),
}

#[derive(Subcommand, Debug)]
pub enum McpCommand {
    /// Create or update one server in the catalog. The name becomes the
    /// namespace prefix on every proxied tool (`{name}__{tool}`), so it is
    /// lowercase [a-z0-9-].
    Add(McpAddArgs),
    /// List every live server.
    List,
    /// Remove one server. Needs the etag `list` shows, so a concurrent edit
    /// aborts instead of being silently deleted.
    Remove {
        name: String,
        #[arg(long)]
        etag: String,
    },
}

#[derive(Args, Debug)]
pub struct McpAddArgs {
    pub name: String,
    /// Run a stdio server: the command to spawn. Exactly one of this and
    /// `--url`.
    #[arg(long, conflicts_with = "url")]
    pub command: Option<String>,
    /// Argument for the stdio command; repeatable, in order. Values may
    /// themselves start with `--` (`--arg --stdio` is the common case).
    #[arg(long = "arg", requires = "command", allow_hyphen_values = true)]
    pub args: Vec<String>,
    /// Environment for the stdio child, as KEY=VALUE; repeatable. NOTE:
    /// journaled through the event log, which attached clients replay.
    #[arg(long = "env", requires = "command", value_name = "KEY=VALUE")]
    pub env: Vec<String>,
    /// Reach a running server over streamable HTTP at this url.
    #[arg(long)]
    pub url: Option<String>,
    /// Run worker-side, one instance per agent, instead of at the server —
    /// the pressure valve for machine-bound or stateful servers.
    #[arg(long)]
    pub local: bool,
    /// Restrict to agents of this type; repeatable. Absent means every
    /// agent; present fails closed for untyped agents.
    #[arg(long = "allow-agent-type")]
    pub allowed_agent_types: Vec<String>,
    /// Update the version this etag names, instead of creating.
    #[arg(long, default_value = "")]
    pub etag: String,
}

#[derive(Subcommand, Debug)]
pub enum WorkerCommand {
    /// Mint a one-time enrollment code for a new worker. Run
    /// `horde-worker`'s join (or `horde enroll join`) with it on the new
    /// machine before it expires.
    Enroll {
        /// The worker's principal name, fixed here at mint time.
        name: String,
        /// Code lifetime; the server clamps it (default 10, cap 60).
        #[arg(long)]
        ttl_minutes: Option<u32>,
    },
    /// Revoke every certificate a worker holds, by name.
    Revoke {
        name: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum EnrollCommand {
    /// Redeem a one-time code against a server's enrollment listener and
    /// store the identity in the `--identity` directory. The code embeds the
    /// server fingerprint this machine pins — nothing else is trusted yet.
    Join {
        /// The enrollment listener, e.g. `https://host:7778`.
        enrollment_endpoint: String,
        /// The code exactly as the server printed it: `secret.fingerprint`.
        code: String,
    },
    /// Mint a one-time enrollment code for another client machine.
    Client {
        name: String,
        #[arg(long)]
        ttl_minutes: Option<u32>,
    },
    /// Revoke every certificate a client holds, by name.
    RevokeClient {
        name: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
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
    // The one verb that must work *before* any identity exists: joining is
    // how this machine gets one, so it dials the enrollment listener rather
    // than the control plane.
    if let Some(Command::Enroll(EnrollCommand::Join {
        enrollment_endpoint,
        code,
    })) = &cli.command
    {
        return run_join(cli, enrollment_endpoint, code, format).await;
    }

    let mut client = connect(cli).await?;
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
        Some(Command::Mcp(command)) => run_mcp(client, command, format).await,
        Some(Command::Worker(command)) => run_worker(client, command, format).await,
        Some(Command::Enroll(command)) => run_enroll(client, command, format).await,
    }
}

/// Connects as the configured identity, or plaintext when none is set.
///
/// No silent fallback in either direction: `--identity` that does not load
/// is an error naming the directory, never a plaintext connection wearing an
/// enrolled flag.
async fn connect(cli: &Cli) -> Result<Client, ClientError> {
    match &cli.identity {
        Some(directory) => {
            let identity = horde_identity::StoredIdentity::load(directory).map_err(|source| {
                ClientError::Identity {
                    directory: directory.clone(),
                    source,
                }
            })?;
            Client::connect_with_identity(&cli.endpoint, &identity).await
        }
        None => Client::connect(&cli.endpoint).await,
    }
}

/// Where this machine's identity lives when `--identity` does not say.
#[must_use]
pub fn default_identity_dir(home: Option<std::path::PathBuf>) -> std::path::PathBuf {
    home.map_or_else(
        || std::path::PathBuf::from(".horde-identity"),
        |home| home.join(".horde").join("identity"),
    )
}

async fn run_join(
    cli: &Cli,
    enrollment_endpoint: &str,
    code: &str,
    format: Format,
) -> Result<(), ClientError> {
    let code = horde_identity::JoinCode::parse(code).ok_or_else(|| {
        ClientError::Enrollment(
            "the code is malformed; paste it exactly as the server printed it \
             (secret.fingerprint)"
                .to_owned(),
        )
    })?;

    let enrolled = horde_identity::join(
        enrollment_endpoint,
        &code,
        horde_proto::v1::Hello {
            semver: env!("CARGO_PKG_VERSION").to_owned(),
            capability_flags: Vec::new(),
            role: horde_proto::v1::Role::Unspecified as i32,
            display_name: "horde".to_owned(),
        },
    )
    .await
    .map_err(|error| ClientError::Enrollment(error.to_string()))?;

    let directory = cli
        .identity
        .clone()
        .unwrap_or_else(|| default_identity_dir(std::env::var_os("HOME").map(Into::into)));
    enrolled
        .identity
        .save(&directory)
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;

    println!(
        "{}",
        render::joined(&enrolled.principal.to_string(), &directory, format)
    );
    Ok(())
}

/// The wire config an `mcp add` describes.
///
/// # Errors
///
/// A malformed `--env` entry, or neither/both transports named — refused
/// here, where the message can talk about flags, rather than sent for the
/// server to refuse in wire vocabulary.
pub fn mcp_config_from(args: &McpAddArgs) -> Result<horde_proto::v1::McpServerConfig, ClientError> {
    let transport =
        match (&args.command, &args.url) {
            (Some(command), None) => {
                let mut env = std::collections::HashMap::new();
                for pair in &args.env {
                    let (key, value) = parse_env_pair(pair).ok_or_else(|| {
                        ClientError::Rpc(tonic::Status::invalid_argument(format!(
                            "--env takes KEY=VALUE, got {pair:?}"
                        )))
                    })?;
                    env.insert(key.to_owned(), value.to_owned());
                }
                horde_proto::v1::mcp_server_config::Transport::Stdio(
                    horde_proto::v1::McpStdioTransport {
                        command: command.clone(),
                        args: args.args.clone(),
                        env,
                    },
                )
            }
            (None, Some(url)) => horde_proto::v1::mcp_server_config::Transport::Http(
                horde_proto::v1::McpHttpTransport { url: url.clone() },
            ),
            _ => {
                return Err(ClientError::Rpc(tonic::Status::invalid_argument(
                    "name exactly one transport: --command for stdio, or --url for http",
                )));
            }
        };

    Ok(horde_proto::v1::McpServerConfig {
        name: args.name.clone(),
        transport: Some(transport),
        local: args.local,
        allowed_agent_types: args.allowed_agent_types.clone(),
        etag: args.etag.clone(),
    })
}

/// Splits one `--env` entry on its first `=`.
///
/// The first, because values contain `=` routinely (base64 padding, URLs);
/// an empty key is refused, an empty value is a real value.
#[must_use]
pub fn parse_env_pair(pair: &str) -> Option<(&str, &str)> {
    let (key, value) = pair.split_once('=')?;
    if key.is_empty() {
        return None;
    }
    Some((key, value))
}

async fn run_mcp(
    client: &mut Client,
    command: &McpCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        McpCommand::Add(args) => {
            let stamped = client.configure_mcp_server(mcp_config_from(args)?).await?;
            println!("{}", render::mcp_configured(&stamped, format));
        }
        McpCommand::List => {
            let servers = client.list_mcp_servers().await?;
            println!("{}", render::mcp_servers(&servers, format));
        }
        McpCommand::Remove { name, etag } => {
            client.delete_mcp_server(name, etag).await?;
            println!("{}", render::mcp_removed(name, format));
        }
    }
    Ok(())
}

async fn run_worker(
    client: &mut Client,
    command: &WorkerCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        WorkerCommand::Enroll { name, ttl_minutes } => {
            let minted = client
                .mint_enrollment_code(horde_proto::v1::Role::Worker, name, *ttl_minutes)
                .await?;
            println!("{}", render::minted(&minted, format));
        }
        WorkerCommand::Revoke { name, reason } => {
            let serials = client
                .revoke_enrollment(
                    horde_proto::v1::revoke_enrollment_request::Principal::WorkerId(
                        horde_proto::v1::WorkerId::new(name),
                    ),
                    reason,
                )
                .await?;
            println!("{}", render::revoked(name, &serials, format));
        }
    }
    Ok(())
}

async fn run_enroll(
    client: &mut Client,
    command: &EnrollCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        EnrollCommand::Join { .. } => {
            // Handled before any connection exists; reaching here is a bug in
            // `run`'s early return, and refusing loudly beats joining twice.
            Err(ClientError::Enrollment(
                "join must not reach the connected dispatch".to_owned(),
            ))
        }
        EnrollCommand::Client { name, ttl_minutes } => {
            let minted = client
                .mint_enrollment_code(horde_proto::v1::Role::Client, name, *ttl_minutes)
                .await?;
            println!("{}", render::minted(&minted, format));
            Ok(())
        }
        EnrollCommand::RevokeClient { name, reason } => {
            let serials = client
                .revoke_enrollment(
                    horde_proto::v1::revoke_enrollment_request::Principal::ClientId(
                        horde_proto::v1::ClientId::new(name),
                    ),
                    reason,
                )
                .await?;
            println!("{}", render::revoked(name, &serials, format));
            Ok(())
        }
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

    /// Values contain `=` routinely (base64 padding, URLs), so the split must
    /// be on the first — and a keyless entry is a mistake, not an empty key.
    #[test]
    fn env_pairs_split_on_the_first_equals_and_refuse_a_missing_key() {
        assert_eq!(
            super::parse_env_pair("TOKEN=abc=="),
            Some(("TOKEN", "abc=="))
        );
        assert_eq!(super::parse_env_pair("EMPTY="), Some(("EMPTY", "")));
        assert_eq!(super::parse_env_pair("=value"), None);
        assert_eq!(super::parse_env_pair("no-equals"), None);
    }

    /// The `mcp add` flags become exactly one wire transport, with everything
    /// they said riding along.
    #[test]
    fn an_mcp_add_becomes_the_wire_config_it_described() {
        let cli = Cli::try_parse_from([
            "horde",
            "mcp",
            "add",
            "linear",
            "--command",
            "linear-mcp",
            "--arg",
            "--stdio",
            "--env",
            "LINEAR_TOKEN=tok",
            "--local",
            "--allow-agent-type",
            "reviewer",
            "--etag",
            "3",
        ])
        .expect("valid arguments");
        let Some(Command::Mcp(super::McpCommand::Add(args))) = &cli.command else {
            panic!("expected mcp add, got {:?}", cli.command);
        };

        let config = super::mcp_config_from(args).expect("a valid config");
        assert_eq!(config.name, "linear");
        assert!(config.local);
        assert_eq!(config.allowed_agent_types, vec!["reviewer".to_owned()]);
        assert_eq!(config.etag, "3");
        match config.transport {
            Some(horde_proto::v1::mcp_server_config::Transport::Stdio(stdio)) => {
                assert_eq!(stdio.command, "linear-mcp");
                assert_eq!(stdio.args, vec!["--stdio".to_owned()]);
                assert_eq!(
                    stdio.env.get("LINEAR_TOKEN").map(String::as_str),
                    Some("tok")
                );
            }
            other => panic!("expected stdio, got {other:?}"),
        }
    }

    /// Neither transport, and a malformed env entry: refused here, in flag
    /// vocabulary, before anything reaches the wire.
    #[test]
    fn an_mcp_add_without_a_transport_or_with_bad_env_is_refused_locally() {
        let no_transport = super::McpAddArgs {
            name: "linear".to_owned(),
            command: None,
            args: Vec::new(),
            env: Vec::new(),
            url: None,
            local: false,
            allowed_agent_types: Vec::new(),
            etag: String::new(),
        };
        let refused = super::mcp_config_from(&no_transport).expect_err("no transport");
        assert!(
            refused.to_string().contains("--command"),
            "the refusal talks in flags: {refused}"
        );

        let bad_env = super::McpAddArgs {
            command: Some("linear-mcp".to_owned()),
            env: vec!["NOEQUALS".to_owned()],
            ..no_transport
        };
        let refused = super::mcp_config_from(&bad_env).expect_err("bad env");
        assert!(refused.to_string().contains("NOEQUALS"), "{refused}");
    }

    /// An http `mcp add` and the both-transports refusal (clap owns it: the
    /// flags conflict, so the mistake never reaches the converter).
    #[test]
    fn an_http_mcp_add_converts_and_both_transports_cannot_parse() {
        let cli = Cli::try_parse_from([
            "horde",
            "mcp",
            "add",
            "grafana",
            "--url",
            "http://127.0.0.1:9931/mcp",
        ])
        .expect("valid arguments");
        let Some(Command::Mcp(super::McpCommand::Add(args))) = &cli.command else {
            panic!("expected mcp add, got {:?}", cli.command);
        };
        let config = super::mcp_config_from(args).expect("a valid config");
        assert!(matches!(
            config.transport,
            Some(horde_proto::v1::mcp_server_config::Transport::Http(_))
        ));

        assert!(
            Cli::try_parse_from([
                "horde",
                "mcp",
                "add",
                "x",
                "--command",
                "c",
                "--url",
                "http://y",
            ])
            .is_err(),
            "--command and --url conflict at parse time"
        );
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

#[cfg(test)]
mod enrollment_tests {
    use clap::Parser;

    use super::{Cli, Command, EnrollCommand, WorkerCommand, default_identity_dir};

    #[test]
    fn the_worker_verbs_parse_as_design_names_them() {
        let cli = Cli::try_parse_from(["horde", "worker", "enroll", "gpu-box"])
            .expect("DESIGN §3 names this verb");
        match cli.command {
            Some(Command::Worker(WorkerCommand::Enroll { name, ttl_minutes })) => {
                assert_eq!(name, "gpu-box");
                assert_eq!(ttl_minutes, None, "the server owns the default ttl");
            }
            other => panic!("expected worker enroll, got {other:?}"),
        }

        let cli =
            Cli::try_parse_from(["horde", "worker", "revoke", "gpu-box", "--reason", "stolen"])
                .expect("DESIGN §3 names this verb");
        match cli.command {
            Some(Command::Worker(WorkerCommand::Revoke { name, reason })) => {
                assert_eq!(name, "gpu-box");
                assert_eq!(reason, "stolen");
            }
            other => panic!("expected worker revoke, got {other:?}"),
        }
    }

    #[test]
    fn join_takes_the_enrollment_endpoint_and_the_code() {
        let cli = Cli::try_parse_from([
            "horde",
            "--identity",
            "/tmp/id",
            "enroll",
            "join",
            "https://host:7778",
            "s.fp",
        ])
        .expect("a join line parses");
        assert_eq!(
            cli.identity.as_deref(),
            Some(std::path::Path::new("/tmp/id"))
        );
        match cli.command {
            Some(Command::Enroll(EnrollCommand::Join {
                enrollment_endpoint,
                code,
            })) => {
                assert_eq!(enrollment_endpoint, "https://host:7778");
                assert_eq!(code, "s.fp");
            }
            other => panic!("expected enroll join, got {other:?}"),
        }
    }

    #[test]
    fn the_default_identity_dir_is_under_home_when_there_is_one() {
        assert_eq!(
            default_identity_dir(Some(std::path::PathBuf::from("/Users/me"))),
            std::path::PathBuf::from("/Users/me/.horde/identity")
        );
        assert_eq!(
            default_identity_dir(None),
            std::path::PathBuf::from(".horde-identity"),
            "no HOME still yields a deterministic, local place"
        );
    }
}
