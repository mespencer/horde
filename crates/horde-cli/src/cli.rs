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

/// The last-resort plaintext endpoint, used only when no identity exists
/// anywhere (ADR-0007): a machine that has joined a server never needs it,
/// and against a secured server it could never connect.
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:7777";

#[derive(Parser, Debug)]
#[command(
    name = "horde",
    about = "Drive the Horde control plane.",
    version,
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Server address. An override, not a requirement (ADR-0007): absent,
    /// `HORDE_ENDPOINT` is read, then the endpoint the identity dir stored
    /// at join — so after one join a bare `horde hello` just works.
    #[arg(long, global = true, env = "HORDE_ENDPOINT")]
    pub endpoint: Option<String>,

    /// Directory holding this machine's enrolled identity (DESIGN §3).
    /// When set, every connection is mTLS; `enroll join` writes here.
    #[arg(long, global = true, env = "HORDE_IDENTITY")]
    pub identity: Option<std::path::PathBuf>,

    /// Emit JSON instead of prose.
    #[arg(long, global = true)]
    pub json: bool,

    /// No verb is the front door (ADR-0008): no live sessions creates one,
    /// auto-named, and attaches; exactly one attaches it; several open the
    /// picker. Headless or `--json`, it fails fast with the candidates.
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Handshake with the server and print what it is.
    Hello,
    /// Create a session and attach to it — the explicit spelling of the
    /// front door. `session new` remains the headless create.
    New(NewArgs),
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
    /// Move attachment content through the server's blob store (DESIGN §8).
    #[command(subcommand)]
    Blob(BlobCommand),
    /// The MCP Hub catalog: user MCP servers proxied onto every agent's
    /// endpoint (ADR-0004).
    #[command(subcommand)]
    Mcp(McpCommand),
    /// Usage and headroom, folded from the journaled usage events.
    Usage(UsageArgs),
    /// Workspace definitions: the fixed repo sets agents check out (DESIGN §7).
    #[command(subcommand)]
    Workspace(WorkspaceCommand),
    /// Ask a worker to materialize a workspace instance ahead of any spawn.
    Prewarm(PrewarmArgs),
    /// Worker administration: enrollment codes and revocation (DESIGN §3).
    #[command(subcommand)]
    Worker(WorkerCommand),
    /// This machine's own identity: joining, and client codes for others.
    #[command(subcommand)]
    Enroll(EnrollCommand),
    /// Stand up this machine (ADR-0007): start a server if none is running,
    /// enroll the local client if none exists, mint-and-join a local
    /// worker, and hold everything until ctrl-c — which tears down only
    /// what this invocation started.
    Up(crate::up::UpArgs),
    /// Port forwards (ADR-0006): expose worker ports here, inject local
    /// ports there.
    #[command(subcommand)]
    Forward(ForwardCommand),
}

#[derive(Subcommand, Debug)]
pub enum ForwardCommand {
    /// Declare an expose and realize it here: the agent's worker port
    /// appears on this machine's localhost until ctrl-c. The requested local
    /// port falls back to an ephemeral one, with the mapping printed.
    Expose(ForwardExposeArgs),
    /// Declare an inject and back it here: a local port appears on the
    /// agent's worker loopback, served by this process until ctrl-c. The
    /// initiator is the backer.
    Inject(ForwardInjectArgs),
    /// Live forwards, with worker reachability and backing.
    Ls {
        /// Restrict to one session.
        #[arg(long)]
        session: Option<String>,
    },
    /// Close a forward intent for good.
    Close {
        session_id: String,
        forward_id: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Realize every live forward in scope: bind a listener per expose, and
    /// back the injects this client's principal owns. Reattach = rerun.
    Serve {
        /// Restrict to one session.
        #[arg(long)]
        session: Option<String>,
    },
}

#[derive(Args, Debug)]
pub struct ForwardExposeArgs {
    pub session_id: String,
    /// The owning agent; the intent lives and dies with its lifecycle.
    pub agent_id: String,
    /// The port on the agent's worker.
    pub port: u16,
    /// The forward's name — forwards are named, not numbered.
    #[arg(long = "as", default_value = "")]
    pub name: String,
    /// The local port to bind; defaults to the worker port.
    #[arg(long)]
    pub local_port: Option<u16>,
}

#[derive(Args, Debug)]
pub struct ForwardInjectArgs {
    pub session_id: String,
    pub agent_id: String,
    /// The local port whose service the agent should reach.
    pub port: u16,
    /// The forward's name — forwards are named, not numbered.
    #[arg(long = "as", default_value = "")]
    pub name: String,
    /// The worker-loopback port where the service appears; defaults to the
    /// local port.
    #[arg(long)]
    pub worker_port: Option<u16>,
}

#[derive(Subcommand, Debug)]
pub enum BlobCommand {
    /// Upload a file to the blob store and print its content address.
    /// Payload attachments carry this address; events never carry the bulk.
    Stash {
        /// The file whose bytes become the blob.
        file: std::path::PathBuf,
    },
    /// Download a blob by content address into a file, verified against the
    /// address before anything is kept.
    Fetch {
        /// The address `stash` printed: `sha256:<64 hex>`.
        address: String,
        /// Where to write the verified bytes.
        #[arg(long, short)]
        out: std::path::PathBuf,
    },
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
pub enum WorkspaceCommand {
    /// Define a workspace from one or more repos.
    Create {
        name: String,
        /// One repo, as name=URL with optional ,branch= and ,path= parts;
        /// repeatable. Example: --repo atlas=https://…/atlas.git,branch=main
        #[arg(
            long = "repo",
            value_name = "NAME=URL[,branch=B][,path=P]",
            required = true
        )]
        repos: Vec<String>,
        /// A path agents may use but Horde never syncs; repeatable.
        #[arg(long = "unmanaged")]
        unmanaged_paths: Vec<String>,
    },
    /// List every live definition.
    List,
    /// Show one definition (and its known instances), by id or name.
    Get { reference: String },
    /// Widen a definition's repo set by one repo — the one deliberate way a
    /// repo set changes after creation.
    AddRepo {
        workspace_definition_id: String,
        /// The repo, in `create`'s --repo form.
        #[arg(long = "repo", value_name = "NAME=URL[,branch=B][,path=P]")]
        repo: String,
        /// The etag `list` shows, so a concurrent edit aborts.
        #[arg(long)]
        etag: String,
    },
    /// Remove a definition at the version `--etag` names.
    Delete {
        workspace_definition_id: String,
        #[arg(long)]
        etag: String,
    },
}

#[derive(Args, Debug)]
pub struct PrewarmArgs {
    pub workspace_definition_id: String,
    pub worker_id: String,
    /// The session whose log carries the progress events.
    #[arg(long)]
    pub session: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum WorkerCommand {
    /// List the connected workers.
    List,
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
    /// Sign a provider's CLI into an account on a worker: the provider's
    /// login command runs in a PTY there with this terminal attached, and
    /// any OAuth-callback port the provider needs is relayed from this
    /// machine's localhost for the login's duration (DESIGN §4).
    Login {
        /// The worker to sign in on.
        worker: String,
        /// The provider whose adapter carries the login recipe, e.g.
        /// `claude`.
        provider: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum EnrollCommand {
    /// Redeem a one-time join token and store the identity in the
    /// `--identity` directory (default `~/.horde/id`). The token carries the
    /// enrollment listener and the server fingerprint this machine pins —
    /// nothing else is trusted yet — and the join stores the server's
    /// endpoint beside the credential, so later invocations need no flag.
    Join {
        /// The token exactly as the server printed it:
        /// `horde://join/<host:port>/<secret>.<fingerprint>`.
        token: String,
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

/// `horde new [title]`: create, then attach.
#[derive(Args, Debug)]
pub struct NewArgs {
    /// The session's title. Omitted, a thematic `adjective-noun` name is
    /// drawn, collision-checked against the live sessions.
    pub title: Option<String>,
    /// What the root agent is: type, tier, workspace, mode, first task.
    #[command(flatten)]
    pub spec: SpecArgs,
    /// Create without attaching — the scripting form.
    #[arg(long)]
    pub detach: bool,
}

#[derive(Subcommand, Debug)]
pub enum SessionCommand {
    /// Create a session and its top-level agent.
    New {
        /// The session's title; omitted draws a thematic name.
        title: Option<String>,
        /// What the root agent is: type, tier, workspace, mode, first task.
        #[command(flatten)]
        spec: SpecArgs,
        /// Makes a retry after a lost response safe.
        #[arg(long, default_value = "")]
        idempotency_key: String,
    },
    /// List sessions.
    List {
        #[arg(long)]
        include_archived: bool,
    },
    /// Show one session, by name, id, or unique id prefix.
    Get {
        session_id: String,
        /// Resolve against archived sessions too.
        #[arg(long)]
        all: bool,
    },
    /// Archive a session. This, not a result, is what ends its agents.
    Archive {
        session_id: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
}

#[derive(Args, Debug)]
pub struct SyncArgs {
    /// The session, by name, id, or unique id prefix.
    pub session_id: String,
    /// First sequence wanted; 0 replays from the beginning.
    #[arg(long, default_value_t = 0)]
    pub from: u64,
    /// Keep listening after the replay instead of exiting.
    #[arg(long)]
    pub follow: bool,
    /// Resolve against archived sessions too.
    #[arg(long)]
    pub all: bool,
}

#[derive(Args, Debug)]
pub struct SendArgs {
    /// The agent, by id — or a session name, which addresses that session's
    /// root agent (ADR-0008: "talk to the session" needs no second id).
    pub agent_id: String,
    pub text: String,
    /// Send a note (owes no result) instead of an assignment.
    #[arg(long)]
    pub note: bool,
    /// Cancel the turn in flight rather than waiting for the turn boundary.
    #[arg(long)]
    pub interrupt: bool,
    /// A commit the message names, as REPO:COMMIT[:BRANCH]; repeatable. The
    /// receiving worker makes it present in the agent's checkout before the
    /// text is read — transport is never the agent's concern (DESIGN §5).
    #[arg(long = "git-ref", value_name = "REPO:COMMIT[:BRANCH]")]
    pub git_refs: Vec<String>,
    /// An attachment by content address, as NAME:sha256:<64 hex>:SIZE;
    /// repeatable. `horde blob stash` prints the address half.
    #[arg(long = "attach", value_name = "NAME:ADDRESS:SIZE")]
    pub attachments: Vec<String>,
    #[arg(long, default_value = "")]
    pub idempotency_key: String,
}

/// The wire payload a send's flags describe, or `None` when they name none.
///
/// # Errors
///
/// A malformed `--git-ref` or `--attach` entry, refused naming the flag and
/// the shape it takes.
pub fn payload_from(args: &SendArgs) -> Result<Option<horde_proto::v1::Payload>, ClientError> {
    if args.git_refs.is_empty() && args.attachments.is_empty() {
        return Ok(None);
    }
    let invalid = |flag: &str, shape: &str, got: &str| {
        ClientError::Rpc(tonic::Status::invalid_argument(format!(
            "{flag} takes {shape}, got {got:?}"
        )))
    };

    let mut git_refs = Vec::new();
    for entry in &args.git_refs {
        let mut parts = entry.splitn(3, ':');
        let (repo, commit) = match (parts.next(), parts.next()) {
            (Some(repo), Some(commit)) if !repo.is_empty() && !commit.is_empty() => (repo, commit),
            _ => return Err(invalid("--git-ref", "REPO:COMMIT[:BRANCH]", entry)),
        };
        git_refs.push(horde_proto::v1::GitRef {
            repo_name: repo.to_owned(),
            commit_sha: commit.to_owned(),
            branch: parts.next().unwrap_or_default().to_owned(),
        });
    }

    let mut attachments = Vec::new();
    for entry in &args.attachments {
        // NAME:sha256:<hex>:SIZE — the address itself contains a colon, so
        // split from both ends rather than blindly on every colon.
        let (name, rest) = entry
            .split_once(':')
            .ok_or_else(|| invalid("--attach", "NAME:ADDRESS:SIZE", entry))?;
        let (address, size) = rest
            .rsplit_once(':')
            .ok_or_else(|| invalid("--attach", "NAME:ADDRESS:SIZE", entry))?;
        let size_bytes: u64 = size
            .parse()
            .map_err(|_| invalid("--attach", "NAME:ADDRESS:SIZE (SIZE in bytes)", entry))?;
        if name.is_empty() || address.is_empty() {
            return Err(invalid("--attach", "NAME:ADDRESS:SIZE", entry));
        }
        attachments.push(horde_proto::v1::Attachment {
            name: name.to_owned(),
            blob_digest: address.to_owned(),
            size_bytes,
            media_type: String::new(),
            inline: Vec::new(),
        });
    }

    Ok(Some(horde_proto::v1::Payload {
        git_refs,
        attachments,
    }))
}

#[derive(Args, Debug)]
pub struct InterruptArgs {
    /// The agent, by id — or a session name, which addresses that session's
    /// root agent.
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
    /// The session, by name, id, or unique id prefix.
    pub session_id: String,
    /// Scope the view to this agent's subtree instead of the whole session.
    /// Prompts from elsewhere show only as a passive badge. A session name
    /// here scopes to that session's root agent.
    #[arg(long)]
    pub agent: Option<String>,
    /// Resolve against archived sessions too.
    #[arg(long)]
    pub all: bool,
}

#[derive(Args, Debug)]
pub struct UsageArgs {
    /// Restrict to this session; repeatable. Default: every session.
    #[arg(long = "session")]
    pub sessions: Vec<String>,
    /// Restrict to this agent's subtree — tree-level attribution.
    #[arg(long)]
    pub subtree: Option<String>,
    /// How to group the rows.
    #[arg(long, value_enum, default_value_t = GroupChoice::Account)]
    pub by: GroupChoice,
}

/// The groupings `--by` offers, in CLI words.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupChoice {
    Agent,
    Session,
    Tree,
    Account,
    Model,
}

/// The wire grouping one CLI choice asks for.
///
/// Extracted so the mapping is testable without a server: a swapped arm here
/// would silently answer a different question than the flag named.
#[must_use]
pub fn group_by_for(choice: GroupChoice) -> horde_proto::v1::UsageGroupBy {
    match choice {
        GroupChoice::Agent => horde_proto::v1::UsageGroupBy::Agent,
        GroupChoice::Session => horde_proto::v1::UsageGroupBy::Session,
        GroupChoice::Tree => horde_proto::v1::UsageGroupBy::TreePath,
        GroupChoice::Account => horde_proto::v1::UsageGroupBy::ProviderAccount,
        GroupChoice::Model => horde_proto::v1::UsageGroupBy::Model,
    }
}

/// The renderer's grouping for the same choice — the renderer owns its own
/// vocabulary rather than importing this module's (which would close a
/// module cycle), so the flag maps into both, here, testably.
#[must_use]
pub fn rendering_for(choice: GroupChoice) -> render::UsageGrouping {
    match choice {
        GroupChoice::Agent => render::UsageGrouping::Agent,
        GroupChoice::Session => render::UsageGrouping::Session,
        GroupChoice::Tree => render::UsageGrouping::Tree,
        GroupChoice::Account => render::UsageGrouping::Account,
        GroupChoice::Model => render::UsageGrouping::Model,
    }
}

#[derive(Args, Debug)]
pub struct SpawnArgs {
    pub session_id: String,
    /// The spawner. Defaults to the session's top-level agent.
    #[arg(long)]
    pub parent: Option<String>,
    #[command(flatten)]
    pub spec: SpecArgs,
    #[arg(long, default_value = "")]
    pub idempotency_key: String,
}

/// What an agent is, everywhere an agent is made (DESIGN §5's `AgentSpec`).
///
/// Shared between `spawn` and `session new` — the root agent is an agent —
/// so the two verbs cannot drift apart in what they let a client say.
#[derive(Args, Debug, Default)]
pub struct SpecArgs {
    /// Agent type name, e.g. "reviewer". Hub MCP allowlists key on it, and
    /// an allowlisted server is invisible to untyped agents.
    #[arg(long = "type")]
    pub agent_type: Option<String>,
    /// Model tier: "fast", "standard", or "deep" (plus whatever the catalog
    /// grows). The server resolves it to a concrete provider and model.
    #[arg(long, conflicts_with = "model")]
    pub tier: Option<String>,
    /// An explicit provider:model, e.g. "claude:opus". Subject to permission
    /// policy — tiers are the everyday vocabulary.
    #[arg(long, value_name = "PROVIDER:MODEL")]
    pub model: Option<String>,
    /// The workspace definition the agent works in.
    #[arg(long)]
    pub workspace: Option<String>,
    /// How the agent's checkout relates to the workspace instance.
    #[arg(long, value_enum)]
    pub checkout: Option<CheckoutChoice>,
    /// The named permission mode the agent runs under.
    #[arg(long)]
    pub permission_mode: Option<String>,
    /// Pin the agent to this worker instead of letting the scheduler choose.
    #[arg(long, value_name = "WORKER_ID")]
    pub pin_worker: Option<String>,
    /// The implicit first assignment: the agent's first turn input, delivered
    /// the moment it is placed. Absent spawns an idle agent.
    #[arg(long)]
    pub task: Option<String>,
}

/// The checkout policies, in CLI words.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutChoice {
    /// Work directly in the shared instance.
    Shared,
    /// A private worktree of the instance's clone (the default server-side).
    Worktree,
    /// A fully separate clone.
    FreshClone,
}

/// The wire policy one CLI choice asks for.
#[must_use]
pub fn checkout_for(choice: CheckoutChoice) -> horde_proto::v1::CheckoutPolicy {
    match choice {
        CheckoutChoice::Shared => horde_proto::v1::CheckoutPolicy::Shared,
        CheckoutChoice::Worktree => horde_proto::v1::CheckoutPolicy::Worktree,
        CheckoutChoice::FreshClone => horde_proto::v1::CheckoutPolicy::FreshClone,
    }
}

/// The wire `AgentSpec` these flags describe.
///
/// # Errors
///
/// A `--model` value without its `provider:` half — refused here, where the
/// message can talk about the flag, rather than sent for the server to
/// refuse in wire vocabulary.
pub fn spec_from(args: &SpecArgs) -> Result<horde_proto::v1::AgentSpec, ClientError> {
    let model = match (&args.tier, &args.model) {
        (Some(tier), _) => Some(horde_proto::v1::ModelSelection {
            selection: Some(horde_proto::v1::model_selection::Selection::Tier(
                tier.clone(),
            )),
        }),
        (None, Some(concrete)) => {
            let (provider, model) = concrete.split_once(':').ok_or_else(|| {
                ClientError::Rpc(tonic::Status::invalid_argument(format!(
                    "--model takes PROVIDER:MODEL (e.g. claude:opus), got {concrete:?}"
                )))
            })?;
            if provider.is_empty() {
                return Err(ClientError::Rpc(tonic::Status::invalid_argument(
                    "--model needs a provider before the colon (e.g. claude:opus)",
                )));
            }
            Some(horde_proto::v1::ModelSelection {
                selection: Some(horde_proto::v1::model_selection::Selection::Concrete(
                    horde_proto::v1::ConcreteModel {
                        provider: provider.to_owned(),
                        model: model.to_owned(),
                        provider_account: String::new(),
                    },
                )),
            })
        }
        (None, None) => None,
    };

    Ok(horde_proto::v1::AgentSpec {
        agent_type: args.agent_type.clone().unwrap_or_default(),
        model,
        workspace_definition_id: args
            .workspace
            .as_deref()
            .map(horde_proto::v1::WorkspaceDefinitionId::new),
        checkout_policy: args
            .checkout
            .map(checkout_for)
            .unwrap_or(horde_proto::v1::CheckoutPolicy::Unspecified)
            as i32,
        placement: args
            .pin_worker
            .as_ref()
            .map(|worker| horde_proto::v1::PlacementConstraints {
                pinned_worker_id: Some(horde_proto::v1::WorkerId::new(worker)),
                ..horde_proto::v1::PlacementConstraints::default()
            }),
        permission_mode: args.permission_mode.clone().unwrap_or_default(),
        budget: None,
        initial_assignment: args.task.as_ref().map(|task| horde_proto::v1::Assignment {
            assignment_id: None,
            text: task.clone(),
            payload: None,
        }),
    })
}

/// Runs one invocation: the pre-connection special case, then the connected
/// dispatch.
///
/// Split from [`dispatch`] because the verb count only ever grows: the match
/// alone already carries one branch per verb, and anything else in the same
/// function walks the whole thing back over the complexity ceiling.
pub async fn run(cli: &Cli, format: Format) -> Result<(), ClientError> {
    // The one verb that must work *before* any identity exists: joining is
    // how this machine gets one, so it dials the enrollment listener rather
    // than the control plane.
    if let Some(Command::Enroll(EnrollCommand::Join { token })) = &cli.command {
        return run_join(cli, token, format).await;
    }
    // `up` orchestrates its own enrollments and connections, starting from
    // a machine that may hold nothing yet.
    if let Some(Command::Up(args)) = &cli.command {
        return crate::up::run(args).await;
    }

    let resolved = resolve_connection(
        cli.endpoint.as_deref(),
        cli.identity.as_deref(),
        std::env::var_os("HOME").map(Into::into),
    )?;
    let mut client = connect(&resolved).await?;
    dispatch(&mut client, cli, &resolved, format).await
}

/// Dispatches to one handler per verb.
///
/// Every arm is a single call and nothing is computed here, which keeps this
/// function's branching equal to the number of verbs. An earlier version
/// inlined each verb's body and scored 164 on CRAP — readable enough in a diff,
/// impossible to bring under the complexity bar however well it was covered.
async fn dispatch(
    client: &mut Client,
    cli: &Cli,
    resolved: &ResolvedConnection,
    format: Format,
) -> Result<(), ClientError> {
    match &cli.command {
        None => crate::front_door::enter(client, interactive(cli)).await,
        Some(Command::New(args)) => {
            crate::front_door::new_session(
                client,
                args.title.as_deref(),
                spec_from(&args.spec)?,
                args.detach,
                format,
                interactive(cli),
            )
            .await
        }
        Some(Command::Attach(args)) => run_attach(client, args).await,
        Some(Command::Hello) => run_hello(client, format).await,
        Some(Command::Session(command)) => run_session(client, command, format).await,
        Some(Command::Sync(args)) => run_sync(client, args, format).await,
        Some(Command::Send(args)) => run_send(client, args, format).await,
        Some(Command::Interrupt(args)) => run_interrupt(client, args, format).await,
        Some(Command::Approve(args)) => run_approve(client, args, format).await,
        Some(Command::Spawn(args)) => run_spawn(client, args, format).await,
        // Routed here rather than through the admin table because serving
        // needs the invocation's identity: backing is keyed to the enrolled
        // principal, which only the flags know.
        Some(Command::Forward(command)) => {
            run_forward(
                client,
                command,
                Some(resolved.identity_dir.as_path()),
                format,
                ctrl_c_ends(),
            )
            .await
        }
        Some(command) => dispatch_admin(client, command, resolved, format).await,
    }
}

/// The administrative half of the verb table: catalogs, stores, and
/// identity. Split from [`dispatch`] by subject so neither half outgrows the
/// complexity bar as the surface grows — same rule the worker's flag parser
/// follows.
async fn dispatch_admin(
    client: &mut Client,
    command: &Command,
    resolved: &ResolvedConnection,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        Command::Blob(command) => run_blob(client, command, format).await,
        Command::Mcp(command) => run_mcp(client, command, format).await,
        Command::Usage(args) => run_usage(client, args, format).await,
        Command::Workspace(command) => run_workspace(client, command, format).await,
        Command::Prewarm(args) => run_prewarm(client, args, format).await,
        Command::Worker(command) => run_worker(client, command, &resolved.endpoint, format).await,
        Command::Enroll(command) => run_enroll(client, command, &resolved.endpoint, format).await,
        // Unreachable while `dispatch` and this match agree; refusing loudly
        // (rather than a silent no-op) is what keeps a drift between them
        // diagnosable.
        other => Err(ClientError::Rpc(tonic::Status::internal(format!(
            "verb dispatched to the wrong table: {other:?}"
        )))),
    }
}

/// Whether this invocation may prompt (ADR-0008): stdin a TTY, `--json`
/// absent. Everything that would have prompted fails fast otherwise.
fn interactive(cli: &Cli) -> bool {
    use std::io::IsTerminal;
    !cli.json && std::io::stdin().is_terminal()
}

/// Attaches, with the reference resolved: a name, id, or unique id prefix
/// names the session, and a session name under `--agent` scopes to that
/// session's root agent.
async fn run_attach(client: &mut Client, args: &AttachArgs) -> Result<(), ClientError> {
    let session_id = crate::resolve::session_id(client, &args.session_id, args.all).await?;
    let agent_id = match &args.agent {
        Some(reference) => Some(crate::resolve::agent_id(client, reference).await?),
        None => None,
    };
    crate::tui::launch(
        client.clone(),
        crate::tui::Launch::Attach {
            session_id,
            agent_id,
        },
    )
    .await
}

/// One invocation's connection facts, decided before any dial.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedConnection {
    /// The endpoint this invocation dials.
    pub endpoint: String,
    /// The identity directory in effect: the flag's, or the default.
    pub identity_dir: std::path::PathBuf,
    /// Whether the directory was named explicitly (`--identity`).
    pub explicit_identity: bool,
    /// Whether the directory holds a complete identity.
    pub enrolled: bool,
}

/// Resolves where to dial and as whom (ADR-0007).
///
/// Endpoint precedence: explicit flag, then environment (clap merges those
/// two before this runs), then the endpoint the identity dir stored at
/// join. The plaintext localhost default survives only as the last resort
/// for a machine with no identity at all — an enrolled machine with no
/// endpoint from any source is a refusal that says where endpoints come
/// from, because the old always-there default could never connect to the
/// server it implied.
///
/// # Errors
///
/// The enrolled-but-endpointless refusal.
pub fn resolve_connection(
    endpoint: Option<&str>,
    identity: Option<&std::path::Path>,
    home: Option<std::path::PathBuf>,
) -> Result<ResolvedConnection, ClientError> {
    let identity_dir = identity.map_or_else(|| default_identity_dir(home), |dir| dir.to_path_buf());
    let enrolled = horde_identity::StoredIdentity::exists(&identity_dir);
    let endpoint = match endpoint {
        Some(endpoint) => endpoint.to_owned(),
        None => match horde_identity::store::load_endpoint(&identity_dir) {
            Some(stored) => stored,
            None if enrolled => {
                return Err(ClientError::NoEndpoint {
                    directory: identity_dir,
                });
            }
            None => DEFAULT_ENDPOINT.to_owned(),
        },
    };
    Ok(ResolvedConnection {
        endpoint,
        identity_dir,
        explicit_identity: identity.is_some(),
        enrolled,
    })
}

/// How one resolved invocation authenticates — a decision, as data, so the
/// truth table is testable without sockets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// Load the identity dir and speak mTLS.
    Mtls,
    /// Dial plaintext — an `http://` endpoint asked for exactly that.
    Plaintext,
    /// An `https://` endpoint with no identity anywhere: refused with the
    /// fix, because dialing would fail with a TLS mystery instead.
    RefusedUnenrolled,
}

/// The transport a resolution implies.
///
/// An explicit `--identity` always speaks mTLS (refusing to load is louder
/// and better than a plaintext connection wearing an enrolled flag); the
/// *defaulted* dir is used only against an `https://` endpoint, so an
/// explicit plaintext endpoint stays plaintext whatever `~/.horde/id`
/// happens to hold.
#[must_use]
pub fn transport_for(resolved: &ResolvedConnection) -> Transport {
    if resolved.explicit_identity {
        return Transport::Mtls;
    }
    if !resolved.endpoint.starts_with("https://") {
        return Transport::Plaintext;
    }
    if resolved.enrolled {
        Transport::Mtls
    } else {
        Transport::RefusedUnenrolled
    }
}

/// Connects as the resolution says.
async fn connect(resolved: &ResolvedConnection) -> Result<Client, ClientError> {
    match transport_for(resolved) {
        Transport::Mtls => {
            let identity =
                horde_identity::StoredIdentity::load(&resolved.identity_dir).map_err(|source| {
                    ClientError::Identity {
                        directory: resolved.identity_dir.clone(),
                        source,
                    }
                })?;
            Client::connect_with_identity(&resolved.endpoint, &identity).await
        }
        Transport::Plaintext => Client::connect(&resolved.endpoint).await,
        Transport::RefusedUnenrolled => Err(ClientError::Unenrolled {
            endpoint: resolved.endpoint.clone(),
            directory: resolved.identity_dir.clone(),
        }),
    }
}

/// Where this machine's identity lives when `--identity` does not say:
/// `~/.horde/id` (ADR-0007).
#[must_use]
pub fn default_identity_dir(home: Option<std::path::PathBuf>) -> std::path::PathBuf {
    home.map_or_else(
        || std::path::PathBuf::from(".horde-identity"),
        |home| home.join(".horde").join("id"),
    )
}

/// The host component of a dialed endpoint URI: `https://h:1` → `h`.
///
/// The one seam where "the server I dialed" becomes "the host a token
/// names": minting composes the token from this host and the port the
/// server's answer carried, because the server cannot know the name its
/// clients dial it by.
#[must_use]
pub fn host_of_endpoint(endpoint: &str) -> Option<&str> {
    let rest = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    if host.is_empty() { None } else { Some(host) }
}

/// The one-token form of a mint answer (ADR-0007): the host this invocation
/// dialed around the code and port the server named. `None` when the answer
/// named no enrollment port — composing a token would mean inventing a
/// listener.
#[must_use]
pub fn token_from_mint(
    dialed_endpoint: &str,
    response: &horde_proto::v1::MintEnrollmentCodeResponse,
) -> Option<horde_identity::JoinToken> {
    let port = u16::try_from(response.enrollment_port)
        .ok()
        .filter(|port| *port != 0)?;
    let host = host_of_endpoint(dialed_endpoint)?;
    let code = horde_identity::JoinCode::parse(&response.code)?;
    Some(horde_identity::JoinToken::new(
        format!("{host}:{port}"),
        code,
    ))
}

/// The principal held by `--identity`'s directory, as its common name
/// (`client:laptop`), for stamping telemetry with who this invocation is.
///
/// Best-effort by design, unlike [`connect`]: enrichment must never turn an
/// absent or unreadable identity into a refusal — the connection path
/// already refuses loudly, once, where it matters.
#[must_use]
pub fn enrolled_principal(identity: Option<&std::path::Path>) -> Option<String> {
    let stored = horde_identity::StoredIdentity::load(identity?).ok()?;
    horde_identity::principal_of_pem(&stored.certificate_pem)
        .ok()
        .map(|principal| principal.common_name())
}

/// Redeems a parsed client token: the join dial, introduced with this
/// CLI's `Hello`. Shared with `horde up`'s bootstrap-file enrollment so
/// the two joins cannot drift in what they claim to be.
///
/// # Errors
///
/// The join refusing, wrapped as an enrollment failure.
pub(crate) async fn redeem_client_token(
    token: &horde_identity::JoinToken,
    display_name: &str,
) -> Result<horde_identity::EnrolledIdentity, ClientError> {
    horde_identity::join(
        &token.enrollment_endpoint(),
        &token.code,
        horde_proto::v1::Hello {
            semver: env!("CARGO_PKG_VERSION").to_owned(),
            capability_flags: Vec::new(),
            role: horde_proto::v1::Role::Unspecified as i32,
            display_name: display_name.to_owned(),
        },
    )
    .await
    .map_err(|error| ClientError::Enrollment(error.to_string()))
}

async fn run_join(cli: &Cli, token: &str, format: Format) -> Result<(), ClientError> {
    // The token's own parse error names the expected horde://join/… shape:
    // the refusal diagnoses the mistake, not the paste (ADR-0007).
    let token = horde_identity::JoinToken::parse(token)
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;

    let enrolled = redeem_client_token(&token, "horde").await?;

    let directory = cli
        .identity
        .clone()
        .unwrap_or_else(|| default_identity_dir(std::env::var_os("HOME").map(Into::into)));
    enrolled
        .identity
        .save(&directory)
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;

    // The join teaches the machine its server (ADR-0007): the token's host,
    // the answer's control-plane port, stored beside the credential so a
    // bare `horde hello` works from here on. An answer without the port
    // (an older server) stores nothing rather than guessing.
    if let Some(port) = enrolled.control_plane_port {
        let endpoint = format!("https://{}:{port}", token.host());
        horde_identity::store::save_endpoint(&directory, &endpoint)
            .map_err(|error| ClientError::Enrollment(error.to_string()))?;
    }

    println!(
        "{}",
        render::joined(&enrolled.principal.to_string(), &directory, format)
    );
    Ok(())
}

async fn run_blob(
    client: &mut Client,
    command: &BlobCommand,
    format: Format,
) -> Result<(), ClientError> {
    let file_error = |path: &std::path::Path| {
        let path = path.to_owned();
        move |source| ClientError::File { path, source }
    };
    match command {
        BlobCommand::Stash { file } => {
            let bytes = std::fs::read(file).map_err(file_error(file))?;
            let stored = client.stash_blob(&bytes).await?;
            println!("{}", render::stashed(&stored, format));
        }
        BlobCommand::Fetch { address, out } => {
            let bytes = client.fetch_blob(address).await?;
            std::fs::write(out, &bytes).map_err(file_error(out))?;
            println!(
                "{}",
                render::fetched(address, out, bytes.len() as u64, format)
            );
        }
    }
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

/// One `--repo` value as the wire repo it describes.
///
/// `NAME=URL` first, then optional comma parts `branch=` and `path=` in any
/// order. The URL may itself contain commas only in the rare case; anyone
/// bitten passes branch/path explicitly, which bounds the URL.
///
/// # Errors
///
/// A refusal naming the flag and the shape it takes.
pub fn repo_from(value: &str) -> Result<horde_proto::v1::WorkspaceRepo, ClientError> {
    let invalid = || {
        ClientError::Rpc(tonic::Status::invalid_argument(format!(
            "--repo takes NAME=URL[,branch=B][,path=P], got {value:?}"
        )))
    };
    let (name, rest) = value.split_once('=').ok_or_else(invalid)?;
    if name.is_empty() || rest.is_empty() {
        return Err(invalid());
    }

    let mut url = rest;
    let mut branch = String::new();
    let mut path = String::new();
    // Options are comma parts *after* the URL; scan from the right so a URL
    // containing `,branch=` literally would still need to be last to bite.
    while let Some((head, tail)) = url.rsplit_once(',') {
        if let Some(value) = tail.strip_prefix("branch=") {
            branch = value.to_owned();
        } else if let Some(value) = tail.strip_prefix("path=") {
            path = value.to_owned();
        } else {
            break;
        }
        url = head;
    }
    if url.is_empty() {
        return Err(invalid());
    }

    Ok(horde_proto::v1::WorkspaceRepo {
        name: name.to_owned(),
        remote_url: url.to_owned(),
        default_branch: branch,
        layout_path: path,
    })
}

async fn run_workspace(
    client: &mut Client,
    command: &WorkspaceCommand,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        WorkspaceCommand::Create {
            name,
            repos,
            unmanaged_paths,
        } => {
            let repos = repos
                .iter()
                .map(|value| repo_from(value))
                .collect::<Result<Vec<_>, _>>()?;
            let definition = client
                .create_workspace_definition(name, repos, unmanaged_paths.clone())
                .await?;
            println!("{}", render::workspace(&definition, format));
        }
        WorkspaceCommand::List => {
            let definitions = client.list_workspace_definitions().await?;
            println!("{}", render::workspaces(&definitions, format));
        }
        WorkspaceCommand::Get { reference } => {
            let response = client.get_workspace_definition(reference).await?;
            println!("{}", render::workspace_with_instances(&response, format));
        }
        WorkspaceCommand::AddRepo {
            workspace_definition_id,
            repo,
            etag,
        } => {
            let definition = client
                .add_workspace_repo(workspace_definition_id, repo_from(repo)?, etag)
                .await?;
            println!("{}", render::workspace(&definition, format));
        }
        WorkspaceCommand::Delete {
            workspace_definition_id,
            etag,
        } => {
            client
                .delete_workspace_definition(workspace_definition_id, etag)
                .await?;
            println!(
                "{}",
                render::workspace_deleted(workspace_definition_id, format)
            );
        }
    }
    Ok(())
}

async fn run_prewarm(
    client: &mut Client,
    args: &PrewarmArgs,
    format: Format,
) -> Result<(), ClientError> {
    let report = client
        .prewarm_workspace(
            &args.workspace_definition_id,
            &args.worker_id,
            args.session.as_deref(),
        )
        .await?;
    println!("{}", render::prewarmed(report.as_ref(), format));
    Ok(())
}

async fn run_worker(
    client: &mut Client,
    command: &WorkerCommand,
    endpoint: &str,
    format: Format,
) -> Result<(), ClientError> {
    match command {
        WorkerCommand::List => {
            let workers = client.list_workers().await?;
            println!("{}", render::workers(&workers, format));
        }
        WorkerCommand::Enroll { name, ttl_minutes } => {
            let minted = client
                .mint_enrollment_code(horde_proto::v1::Role::Worker, name, *ttl_minutes)
                .await?;
            let token = token_from_mint(endpoint, &minted);
            println!("{}", render::minted(&minted, token.as_ref(), format));
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
        WorkerCommand::Login { worker, provider } => {
            crate::login::run_interactive(client, worker, provider, format).await?;
        }
    }
    Ok(())
}

async fn run_enroll(
    client: &mut Client,
    command: &EnrollCommand,
    endpoint: &str,
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
            let token = token_from_mint(endpoint, &minted);
            println!("{}", render::minted(&minted, token.as_ref(), format));
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

// -- Port forwards (ADR-0006) -------------------------------------------------

/// The foreground verbs' lifetime: until ctrl-c. Injected as a future —
/// exactly the worker daemon's shutdown shape — so the flows are testable
/// without a signal.
async fn ctrl_c_ends() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn run_forward(
    client: &mut Client,
    command: &ForwardCommand,
    identity: Option<&std::path::Path>,
    format: Format,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    match command {
        ForwardCommand::Expose(args) => {
            let session_id = crate::resolve::session_id(client, &args.session_id, false).await?;
            run_forward_expose(client, args, &session_id, format, shutdown).await
        }
        ForwardCommand::Inject(args) => {
            let session_id = crate::resolve::session_id(client, &args.session_id, false).await?;
            run_forward_inject(client, args, &session_id, format, shutdown).await
        }
        ForwardCommand::Ls { session } => {
            let session = resolved_filter(client, session.as_deref()).await?;
            let forwards = client.list_forwards(session.as_deref()).await?;
            println!("{}", render::forwards(&forwards, format));
            Ok(())
        }
        ForwardCommand::Close {
            session_id,
            forward_id,
            reason,
        } => {
            let session_id = crate::resolve::session_id(client, session_id, false).await?;
            let sequence = client
                .close_forward(&session_id, forward_id, reason)
                .await?;
            println!("{}", render::sequence("closed", sequence, format));
            Ok(())
        }
        ForwardCommand::Serve { session } => {
            let session = resolved_filter(client, session.as_deref()).await?;
            run_forward_serve(client, session.as_deref(), identity, format, shutdown)
                .await
                .map(drop)
        }
    }
}

/// An optional `--session` filter, resolved when present.
async fn resolved_filter(
    client: &mut Client,
    session: Option<&str>,
) -> Result<Option<String>, ClientError> {
    match session {
        Some(reference) => Ok(Some(
            crate::resolve::session_id(client, reference, false).await?,
        )),
        None => Ok(None),
    }
}

/// The forward's display name: what `--as` said, or `port-<port>` — a
/// default rather than a refusal, because the CLI mirrors the agent verb
/// where the name is likewise the declarer's choice.
#[must_use]
pub fn forward_name_or_default(name: &str, port: u16) -> String {
    if name.is_empty() {
        format!("port-{port}")
    } else {
        name.to_owned()
    }
}

/// Journals one declaration and returns the id the log minted — the shared
/// opening move of both foreground forward verbs.
async fn declare(
    client: &mut Client,
    session_id: &str,
    agent_id: &str,
    spec: horde_proto::v1::ForwardSpec,
) -> Result<String, ClientError> {
    let opened = client.open_forward(session_id, agent_id, spec, "").await?;
    Ok(opened
        .forward_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default())
}

async fn run_forward_expose(
    client: &mut Client,
    args: &ForwardExposeArgs,
    session_id: &str,
    format: Format,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    let name = forward_name_or_default(&args.name, args.port);
    let local_port = u32::from(args.local_port.unwrap_or(0));
    let spec = horde_proto::v1::ForwardSpec {
        direction: horde_proto::v1::ForwardDirection::Expose as i32,
        name: name.clone(),
        worker_port: u32::from(args.port),
        client_port: local_port,
    };
    let forward_id = declare(client, session_id, &args.agent_id, spec).await?;

    let realized = crate::forward::realize_expose(
        client,
        &forward_id,
        &name,
        u32::from(args.port),
        local_port,
    )
    .await
    .map_err(|source| ClientError::File {
        path: std::path::PathBuf::from("127.0.0.1"),
        source,
    })?;
    println!(
        "{}",
        render::forward_realized(
            &name,
            &forward_id,
            realized.local_addr,
            realized.fell_back,
            format
        )
    );

    // Foreground until the operator says otherwise — a listener is only as
    // alive as the process holding it, and exiting silently would be a
    // realization lying about itself.
    shutdown.await;
    realized.stop().await;
    Ok(())
}

async fn run_forward_inject(
    client: &mut Client,
    args: &ForwardInjectArgs,
    session_id: &str,
    format: Format,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    let name = forward_name_or_default(&args.name, args.port);
    let worker_port = args.worker_port.unwrap_or(args.port);
    let spec = horde_proto::v1::ForwardSpec {
        direction: horde_proto::v1::ForwardDirection::Inject as i32,
        name: name.clone(),
        worker_port: u32::from(worker_port),
        client_port: u32::from(args.port),
    };
    let forward_id = declare(client, session_id, &args.agent_id, spec).await?;

    println!(
        "{}",
        render::forward_backing(&name, &forward_id, args.port, worker_port, format)
    );

    let targets: crate::forward::InjectTargets = std::sync::Arc::default();
    if let Ok(mut map) = targets.write() {
        map.insert(
            forward_id,
            crate::forward::InjectTarget {
                name,
                client_port: args.port,
            },
        );
    }
    serve_until_interrupted(client, targets, shutdown).await
}

/// Serves until shutdown, returning how many exposes were realized — the
/// one observable fact a headless caller (and a test) can hold this whole
/// foreground verb to.
async fn run_forward_serve(
    client: &mut Client,
    session: Option<&str>,
    identity: Option<&std::path::Path>,
    format: Format,
    shutdown: impl Future<Output = ()>,
) -> Result<usize, ClientError> {
    let statuses = client.list_forwards(session).await?;
    let principal = enrolled_principal(identity);
    let plan = crate::forward::ServePlan::from_statuses(&statuses, principal.as_deref());

    let realized = realize_planned(client, &plan.exposes, format).await;
    for line in backing_lines(&plan.injects, format) {
        println!("{line}");
    }
    let outcome = serve_until_interrupted(client, plan.injects.clone(), shutdown).await;
    let count = realized.len();
    stop_all(realized).await;
    outcome.map(|()| count)
}

/// Realizes every planned expose; a bind failure costs that one forward,
/// loudly, never the rest.
async fn realize_planned(
    client: &Client,
    exposes: &[crate::forward::PlannedExpose],
    format: Format,
) -> Vec<crate::forward::RealizedExpose> {
    let mut realized = Vec::new();
    for expose in exposes {
        if let Some(listener) = realize_one(client, expose, format).await {
            realized.push(listener);
        }
    }
    realized
}

/// One planned expose realized and announced, or a loud `None`.
async fn realize_one(
    client: &Client,
    expose: &crate::forward::PlannedExpose,
    format: Format,
) -> Option<crate::forward::RealizedExpose> {
    match crate::forward::realize_expose(
        client,
        &expose.forward_id,
        &expose.name,
        expose.worker_port,
        expose.client_port,
    )
    .await
    {
        Ok(listener) => {
            println!(
                "{}",
                render::forward_realized(
                    &expose.name,
                    &expose.forward_id,
                    listener.local_addr,
                    listener.fell_back,
                    format
                )
            );
            Some(listener)
        }
        Err(error) => {
            eprintln!("could not realize {}: {error}", expose.name);
            None
        }
    }
}

/// What this invocation backs, rendered — returned rather than printed so
/// the lines are assertable; the caller owns the printing.
fn backing_lines(targets: &crate::forward::InjectTargets, format: Format) -> Vec<String> {
    read_targets(targets)
        .into_iter()
        .map(|(forward_id, target)| {
            render::forward_backing(&target.name, &forward_id, target.client_port, 0, format)
        })
        .collect()
}

/// Backs `targets` until ctrl-c or the serve stream dies — serving IS
/// backing, so returning is the moment the server counts this client
/// unbacked.
async fn serve_until_interrupted(
    client: &Client,
    targets: crate::forward::InjectTargets,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    tokio::select! {
        outcome = crate::forward::serve_backing(client, targets) => outcome,
        () = shutdown => Ok(()),
    }
}

/// Tears every realization down, waiting for each accept loop to be gone.
async fn stop_all(realized: Vec<crate::forward::RealizedExpose>) {
    for listener in realized {
        listener.stop().await;
    }
}

/// A snapshot of the inject targets, for printing without holding the lock.
fn read_targets(
    targets: &crate::forward::InjectTargets,
) -> Vec<(String, crate::forward::InjectTarget)> {
    match targets.read() {
        Ok(map) => map
            .iter()
            .map(|(id, target)| (id.clone(), target.clone()))
            .collect(),
        Err(_) => Vec::new(),
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
    let agent_id = crate::resolve::agent_id(client, &args.agent_id).await?;
    let sequence = client.interrupt(&agent_id, &args.reason).await?;
    println!("{}", render::sequence("interrupted", sequence, format));
    Ok(())
}

async fn run_spawn(
    client: &mut Client,
    args: &SpawnArgs,
    format: Format,
) -> Result<(), ClientError> {
    let session_id = crate::resolve::session_id(client, &args.session_id, false).await?;
    let response = client
        .spawn_agent_with_spec(
            &session_id,
            args.parent.as_deref(),
            spec_from(&args.spec)?,
            &args.idempotency_key,
        )
        .await?;
    println!("{}", render::spawned(&response, format));
    Ok(())
}

async fn run_usage(
    client: &mut Client,
    args: &UsageArgs,
    format: Format,
) -> Result<(), ClientError> {
    let options = crate::client::UsageOptions::grouped_by(group_by_for(args.by))
        .sessions(args.sessions.clone())
        .scoped_to_subtree(args.subtree.clone());
    let response = client.query_usage(&options).await?;
    println!(
        "{}",
        render::usage(&response, rendering_for(args.by), format)
    );
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
            spec,
            idempotency_key,
        } => {
            let (summary, root) = crate::front_door::create(
                client,
                title.as_deref(),
                spec_from(spec)?,
                idempotency_key,
            )
            .await?;
            println!("{}", render::created(&summary, &root, format));
        }
        SessionCommand::List { include_archived } => {
            let summaries = client.list_sessions(*include_archived).await?;
            println!("{}", render::sessions(&summaries, format));
        }
        SessionCommand::Get { session_id, all } => {
            let session_id = crate::resolve::session_id(client, session_id, *all).await?;
            let summary = client.get_session(&session_id).await?;
            println!("{}", render::session(&summary, format));
        }
        SessionCommand::Archive { session_id, reason } => {
            let session_id = crate::resolve::session_id(client, session_id, false).await?;
            let summary = client.archive_session(&session_id, reason).await?;
            println!("{}", render::session(&summary, format));
        }
    }
    Ok(())
}

async fn run_sync(client: &mut Client, args: &SyncArgs, format: Format) -> Result<(), ClientError> {
    let session_id = crate::resolve::session_id(client, &args.session_id, args.all).await?;
    let options = SyncOptions::new(session_id)
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
    let agent_id = crate::resolve::agent_id(client, &args.agent_id).await?;
    let response = client
        .send_with_payload(
            &agent_id,
            args.note,
            &args.text,
            payload_from(args)?,
            delivery_for(args.interrupt),
            &args.idempotency_key,
        )
        .await?;
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

    use super::{AttachArgs, Cli, Command, SessionCommand, SyncArgs, delivery_for, outcome_for};

    #[test]
    fn delivery_defaults_to_the_turn_boundary_and_flips_only_when_asked() {
        assert_eq!(delivery_for(false), DeliveryMode::TurnBoundary);
        assert_eq!(delivery_for(true), DeliveryMode::Interrupt);
    }

    /// The spec flags become the wire `AgentSpec` field for field — the whole
    /// point of fix a12: before this, every client sent the default and no
    /// agent could be typed, tiered, homed, or given a first task.
    #[test]
    fn spawn_spec_flags_become_the_wire_spec() {
        let cli = Cli::try_parse_from([
            "horde",
            "spawn",
            "s1",
            "--type",
            "reviewer",
            "--tier",
            "deep",
            "--workspace",
            "ws-1",
            "--checkout",
            "fresh-clone",
            "--permission-mode",
            "cautious",
            "--task",
            "review the diff",
        ])
        .expect("valid arguments");
        let Some(Command::Spawn(args)) = &cli.command else {
            panic!("expected spawn, got {:?}", cli.command);
        };
        let spec = super::spec_from(&args.spec).expect("a valid spec");

        assert_eq!(spec.agent_type, "reviewer");
        assert_eq!(
            spec.model
                .as_ref()
                .and_then(|model| model.selection.clone()),
            Some(horde_proto::v1::model_selection::Selection::Tier(
                "deep".to_owned()
            ))
        );
        assert_eq!(
            spec.workspace_definition_id.map(|id| id.to_string()),
            Some("ws-1".to_owned())
        );
        assert_eq!(
            spec.checkout_policy,
            horde_proto::v1::CheckoutPolicy::FreshClone as i32
        );
        assert_eq!(spec.permission_mode, "cautious");
        assert_eq!(
            spec.initial_assignment.map(|assignment| assignment.text),
            Some("review the diff".to_owned())
        );
    }

    /// `--model` is an explicit provider:model pair; half a pair is refused
    /// naming the flag, and `--tier` beside it is a clap conflict.
    #[test]
    fn the_model_flag_parses_a_provider_model_pair_or_refuses() {
        let concrete = super::spec_from(&super::SpecArgs {
            model: Some("claude:opus".to_owned()),
            ..super::SpecArgs::default()
        })
        .expect("a valid pair");
        assert_eq!(
            concrete.model.and_then(|model| model.selection),
            Some(horde_proto::v1::model_selection::Selection::Concrete(
                horde_proto::v1::ConcreteModel {
                    provider: "claude".to_owned(),
                    model: "opus".to_owned(),
                    provider_account: String::new(),
                }
            ))
        );

        for wrong in ["opus", ":opus"] {
            let refused = super::spec_from(&super::SpecArgs {
                model: Some(wrong.to_owned()),
                ..super::SpecArgs::default()
            })
            .expect_err("half a pair must be refused");
            assert!(
                refused.to_string().to_lowercase().contains("provider"),
                "{wrong}: {refused}"
            );
        }

        assert!(
            Cli::try_parse_from([
                "horde",
                "spawn",
                "s1",
                "--tier",
                "deep",
                "--model",
                "claude:opus"
            ])
            .is_err(),
            "--tier and --model are one choice, not two flags"
        );

        // No flags at all is no selection — the worker's default, honestly.
        let bare = super::spec_from(&super::SpecArgs::default()).expect("a bare spec");
        assert_eq!(bare, horde_proto::v1::AgentSpec::default());
    }

    /// One `--repo` value becomes one wire repo: name and URL split on the
    /// first `=`, branch and path as trailing comma parts in any order, and
    /// every malformed shape refused naming the flag.
    #[test]
    fn repo_values_parse_name_url_branch_and_path_or_refuse() {
        let bare = super::repo_from("atlas=https://git.example/atlas.git").expect("a bare pair");
        assert_eq!(bare.name, "atlas");
        assert_eq!(bare.remote_url, "https://git.example/atlas.git");
        assert_eq!(bare.default_branch, "");
        assert_eq!(bare.layout_path, "");

        let full = super::repo_from("atlas=https://git.example/a.git,path=src/atlas,branch=main")
            .expect("options in any order");
        assert_eq!(full.remote_url, "https://git.example/a.git");
        assert_eq!(full.default_branch, "main");
        assert_eq!(full.layout_path, "src/atlas");

        for wrong in ["atlas", "=url", "atlas=", "atlas=,branch=main"] {
            assert!(
                super::repo_from(wrong).is_err(),
                "{wrong:?} must be refused"
            );
        }
    }

    /// `--pin-worker` becomes a placement constraint; its absence stays no
    /// constraint at all rather than an empty one.
    #[test]
    fn pinning_a_worker_becomes_the_placement_constraint() {
        let pinned = super::spec_from(&super::SpecArgs {
            pin_worker: Some("w-1".to_owned()),
            ..super::SpecArgs::default()
        })
        .expect("a pinned spec");
        assert_eq!(
            pinned
                .placement
                .and_then(|placement| placement.pinned_worker_id)
                .map(|id| id.to_string()),
            Some("w-1".to_owned())
        );
    }

    /// The payload flags: refs and attachments parse into the wire payload,
    /// nothing named is no payload (not an empty one), and malformed entries
    /// refuse naming the flag and its shape.
    #[test]
    fn send_payload_flags_parse_refs_and_attachments_or_refuse() {
        let args = |git_refs: &[&str], attachments: &[&str]| super::SendArgs {
            agent_id: "a1".to_owned(),
            text: "integrate".to_owned(),
            note: false,
            interrupt: false,
            git_refs: git_refs.iter().map(|s| (*s).to_owned()).collect(),
            attachments: attachments.iter().map(|s| (*s).to_owned()).collect(),
            idempotency_key: String::new(),
        };

        assert_eq!(
            super::payload_from(&args(&[], &[])).expect("no flags"),
            None,
            "nothing named is no payload, not an empty one"
        );

        let payload = super::payload_from(&args(
            &["atlas:abc123:main", "codex:def456"],
            &["log.txt:sha256:aa:120"],
        ))
        .expect("well-formed flags")
        .expect("a payload");
        assert_eq!(payload.git_refs.len(), 2);
        assert_eq!(payload.git_refs[0].repo_name, "atlas");
        assert_eq!(payload.git_refs[0].commit_sha, "abc123");
        assert_eq!(payload.git_refs[0].branch, "main");
        assert_eq!(
            payload.git_refs[1].branch, "",
            "the branch is optional and advisory"
        );
        assert_eq!(payload.attachments.len(), 1);
        assert_eq!(payload.attachments[0].name, "log.txt");
        assert_eq!(
            payload.attachments[0].blob_digest, "sha256:aa",
            "the address keeps its own colon — split from both ends"
        );
        assert_eq!(payload.attachments[0].size_bytes, 120);

        for (git_refs, attachments) in [
            (&["atlas"][..], &[][..]),
            (&[":abc"][..], &[][..]),
            (&[][..], &["log.txt"][..]),
            (&[][..], &["log.txt:sha256:aa:lots"][..]),
            (&[][..], &[":sha256:aa:1"][..]),
        ] {
            assert!(
                super::payload_from(&args(git_refs, attachments)).is_err(),
                "{git_refs:?} {attachments:?} must be refused"
            );
        }
    }

    /// Each CLI checkout word maps to its own wire policy.
    #[test]
    fn every_checkout_choice_maps_to_its_wire_policy() {
        use super::{CheckoutChoice, checkout_for};
        assert_eq!(
            checkout_for(CheckoutChoice::Shared),
            horde_proto::v1::CheckoutPolicy::Shared
        );
        assert_eq!(
            checkout_for(CheckoutChoice::Worktree),
            horde_proto::v1::CheckoutPolicy::Worktree
        );
        assert_eq!(
            checkout_for(CheckoutChoice::FreshClone),
            horde_proto::v1::CheckoutPolicy::FreshClone
        );
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

    /// Parses one `mcp add` invocation through the real command model and
    /// converts it — the arrangement both transport tests share.
    fn parsed_mcp_add(argv: &[&str]) -> horde_proto::v1::McpServerConfig {
        let cli = Cli::try_parse_from(argv).expect("valid arguments");
        let Some(Command::Mcp(super::McpCommand::Add(args))) = &cli.command else {
            panic!("expected mcp add, got {:?}", cli.command);
        };
        super::mcp_config_from(args).expect("a valid config")
    }

    /// The `mcp add` flags become exactly one wire transport, with everything
    /// they said riding along.
    #[test]
    fn an_mcp_add_becomes_the_wire_config_it_described() {
        let config = parsed_mcp_add(&[
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
        ]);
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
        let config = parsed_mcp_add(&[
            "horde",
            "mcp",
            "add",
            "grafana",
            "--url",
            "http://127.0.0.1:9931/mcp",
        ]);
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

    /// Both halves of the flag's meaning, since the flag maps twice: the wire
    /// grouping the server folds by, and the renderer's label vocabulary. A
    /// swapped arm in either would answer a different question than the flag
    /// named.
    #[test]
    fn every_group_choice_maps_to_its_wire_and_render_groupings() {
        use super::{GroupChoice, group_by_for, rendering_for};
        use horde_proto::v1::UsageGroupBy;

        let cases = [
            (
                GroupChoice::Agent,
                UsageGroupBy::Agent,
                crate::render::UsageGrouping::Agent,
            ),
            (
                GroupChoice::Session,
                UsageGroupBy::Session,
                crate::render::UsageGrouping::Session,
            ),
            (
                GroupChoice::Tree,
                UsageGroupBy::TreePath,
                crate::render::UsageGrouping::Tree,
            ),
            (
                GroupChoice::Account,
                UsageGroupBy::ProviderAccount,
                crate::render::UsageGrouping::Account,
            ),
            (
                GroupChoice::Model,
                UsageGroupBy::Model,
                crate::render::UsageGrouping::Model,
            ),
        ];
        for (choice, wire, rendering) in cases {
            assert_eq!(group_by_for(choice), wire, "wire mapping for {choice:?}");
            assert_eq!(
                rendering_for(choice),
                rendering,
                "render mapping for {choice:?}"
            );
        }
    }

    #[test]
    fn usage_defaults_to_accounts_and_carries_its_filters() {
        use super::GroupChoice;

        let bare = Cli::try_parse_from(["horde", "usage"]).expect("bare usage parses");
        match bare.command {
            Some(Command::Usage(args)) => {
                assert_eq!(
                    args.by,
                    GroupChoice::Account,
                    "accounts are the default view"
                );
                assert!(args.sessions.is_empty(), "default is every session");
                assert_eq!(args.subtree, None);
            }
            other => panic!("expected usage, got {other:?}"),
        }

        let scoped = Cli::try_parse_from([
            "horde",
            "usage",
            "--by",
            "tree",
            "--session",
            "s1",
            "--session",
            "s2",
            "--subtree",
            "agent-9",
        ])
        .expect("a scoped usage parses");
        match scoped.command {
            Some(Command::Usage(args)) => {
                assert_eq!(args.by, GroupChoice::Tree);
                assert_eq!(args.sessions, vec!["s1".to_owned(), "s2".to_owned()]);
                assert_eq!(args.subtree.as_deref(), Some("agent-9"));
            }
            other => panic!("expected usage, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from(["horde", "usage", "--by", "bogus"]).is_err(),
            "an unknown grouping is a parse error, not a guess"
        );
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

    /// The endpoint is an override, not a requirement (ADR-0007): a bare
    /// invocation names none, and resolution decides from there.
    #[test]
    fn the_endpoint_flag_is_optional_and_explicit_when_given() {
        let cli = Cli::try_parse_from(["horde", "hello"]).expect("valid arguments");
        assert_eq!(cli.endpoint, None, "no flag means resolution decides");
        assert!(!cli.json, "human output is the default");

        let cli = Cli::try_parse_from(["horde", "--endpoint", "https://h:1", "hello"])
            .expect("valid arguments");
        assert_eq!(cli.endpoint.as_deref(), Some("https://h:1"));
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
                assert!(!args.all, "live-only resolution is the default");
            }
            other => panic!("expected attach, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from(["horde", "attach"]).is_err(),
            "attach needs a session"
        );
    }

    /// `horde new [title]` is the explicit front door: the title is
    /// optional, the root spec rides along, and `--detach` is the scripting
    /// form (ADR-0008).
    #[test]
    fn new_takes_an_optional_title_a_spec_and_detach() {
        let bare = Cli::try_parse_from(["horde", "new"]).expect("titleless new parses");
        match bare.command {
            Some(Command::New(args)) => {
                assert_eq!(args.title, None, "an omitted title draws a name");
                assert!(!args.detach, "attach is the default");
            }
            other => panic!("expected new, got {other:?}"),
        }

        let full = Cli::try_parse_from([
            "horde", "new", "ci fix", "--type", "reviewer", "--tier", "deep", "--detach",
        ])
        .expect("a full new parses");
        match full.command {
            Some(Command::New(args)) => {
                assert_eq!(args.title.as_deref(), Some("ci fix"));
                assert_eq!(args.spec.agent_type.as_deref(), Some("reviewer"));
                assert_eq!(args.spec.tier.as_deref(), Some("deep"));
                assert!(args.detach);
            }
            other => panic!("expected new, got {other:?}"),
        }
    }

    /// `session new`'s title is optional everywhere now — the headless
    /// create draws the same thematic name the front door does.
    #[test]
    fn session_new_no_longer_requires_a_title() {
        let cli = Cli::try_parse_from(["horde", "session", "new"]).expect("titleless create");
        match cli.command {
            Some(Command::Session(SessionCommand::New { title, .. })) => {
                assert_eq!(title, None);
            }
            other => panic!("expected session new, got {other:?}"),
        }
    }

    /// `--all` widens resolution to archived sessions on the verbs that can
    /// sensibly reach one; each flag must land on its own struct.
    #[test]
    fn the_all_flag_reaches_attach_sync_and_get() {
        let attach = Cli::try_parse_from(["horde", "attach", "s1", "--all"]).expect("parses");
        assert!(matches!(
            attach.command,
            Some(Command::Attach(AttachArgs { all: true, .. }))
        ));

        let sync = Cli::try_parse_from(["horde", "sync", "s1", "--all"]).expect("parses");
        assert!(matches!(
            sync.command,
            Some(Command::Sync(SyncArgs { all: true, .. }))
        ));

        let get = Cli::try_parse_from(["horde", "session", "get", "s1", "--all"]).expect("parses");
        assert!(matches!(
            get.command,
            Some(Command::Session(SessionCommand::Get { all: true, .. }))
        ));
    }
}

#[cfg(test)]
mod enrollment_tests {
    use clap::Parser;

    use super::{Cli, Command, EnrollCommand, WorkerCommand, default_identity_dir};

    /// Writes a complete, CA-signed `client:laptop` identity into `dir` —
    /// the production `StoredIdentity` layout, for the tests that need a
    /// real enrolled directory without a server.
    fn seeded_identity(dir: &std::path::Path) {
        let ca = horde_identity::CertificateAuthority::generate(1_754_956_800_000)
            .expect("a CA generates");
        let (key_pem, csr_pem) = horde_identity::enroll::generate_csr().expect("a CSR generates");
        let principal =
            horde_identity::Principal::new(horde_identity::PrincipalRole::Client, "laptop")
                .expect("a valid name");
        let issued = ca
            .sign_join(&csr_pem, &principal, 1_754_956_800_000)
            .expect("the CA signs");
        horde_identity::StoredIdentity {
            certificate_pem: issued.certificate_pem,
            key_pem,
            ca_pem: ca.certificate_pem().to_owned(),
        }
        .save(dir)
        .expect("the identity saves");
    }

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

    /// Join takes exactly the token — one argument, nothing left to swap
    /// (ADR-0007). Two positionals is the old ceremony and must not parse.
    #[test]
    fn join_takes_the_token_and_nothing_else() {
        let cli = Cli::try_parse_from([
            "horde",
            "--identity",
            "/tmp/id",
            "enroll",
            "join",
            "horde://join/host:7778/s.fp",
        ])
        .expect("a join line parses");
        assert_eq!(
            cli.identity.as_deref(),
            Some(std::path::Path::new("/tmp/id"))
        );
        match cli.command {
            Some(Command::Enroll(EnrollCommand::Join { token })) => {
                assert_eq!(token, "horde://join/host:7778/s.fp");
            }
            other => panic!("expected enroll join, got {other:?}"),
        }

        assert!(
            Cli::try_parse_from(["horde", "enroll", "join", "https://host:7778", "s.fp"]).is_err(),
            "the old endpoint-and-code pair must be a parse error, not a guess"
        );
    }

    #[test]
    fn the_default_identity_dir_is_under_home_when_there_is_one() {
        assert_eq!(
            default_identity_dir(Some(std::path::PathBuf::from("/Users/me"))),
            std::path::PathBuf::from("/Users/me/.horde/id"),
            "ADR-0007 names ~/.horde/id"
        );
        assert_eq!(
            default_identity_dir(None),
            std::path::PathBuf::from(".horde-identity"),
            "no HOME still yields a deterministic, local place"
        );
    }

    /// The `up` verb's flags reach it (each would otherwise parse cleanly
    /// and be silently ignored), and a bare `horde up` is valid.
    #[test]
    fn up_flags_reach_the_command() {
        let cli = Cli::try_parse_from([
            "horde",
            "up",
            "--state-dir",
            "/tmp/h",
            "--adapter",
            "claude",
            "--adapter",
            "fake",
            "--name",
            "den",
            "--bind",
            "127.0.0.1:0",
            "--enrollment-bind",
            "127.0.0.1:0",
        ])
        .expect("a full up line parses");
        let Some(Command::Up(args)) = &cli.command else {
            panic!("expected up, got {:?}", cli.command);
        };
        assert_eq!(
            args.state_dir.as_deref(),
            Some(std::path::Path::new("/tmp/h"))
        );
        assert_eq!(args.adapters, vec!["claude", "fake"]);
        assert_eq!(args.name.as_deref(), Some("den"));
        assert_eq!(args.bind, "127.0.0.1:0");
        assert_eq!(args.enrollment_bind, "127.0.0.1:0");

        let bare = Cli::try_parse_from(["horde", "up"]).expect("bare up parses");
        let Some(Command::Up(args)) = &bare.command else {
            panic!("expected up, got {:?}", bare.command);
        };
        assert_eq!(args.bind, "127.0.0.1:7777", "the day-one default port");
        assert_eq!(args.enrollment_bind, "127.0.0.1:7778");
        assert!(args.adapters.is_empty() && args.state_dir.is_none());
    }

    /// ADR-0007's endpoint precedence, whole: explicit beats stored beats
    /// the last-resort default, the stored endpoint only exists after a
    /// join, and an enrolled-but-endpointless invocation is a refusal that
    /// says where endpoints come from.
    #[test]
    fn endpoint_resolution_prefers_explicit_then_stored_then_refuses_or_defaults() {
        let home = tempfile::tempdir().expect("a temp dir");
        let home_path = || Some(home.path().to_path_buf());
        let id_dir = super::default_identity_dir(home_path());

        // No identity anywhere: the plaintext default is the last resort.
        let fresh = super::resolve_connection(None, None, home_path()).expect("resolves");
        assert_eq!(fresh.endpoint, "http://127.0.0.1:7777");
        assert_eq!(fresh.identity_dir, id_dir);
        assert!(!fresh.enrolled && !fresh.explicit_identity);

        // An explicit endpoint outranks everything, stored included.
        horde_identity::store::save_endpoint(&id_dir, "https://stored:7777").expect("saves");
        let explicit =
            super::resolve_connection(Some("https://flag:1"), None, home_path()).expect("resolves");
        assert_eq!(explicit.endpoint, "https://flag:1");

        // Nothing explicit: the identity dir's stored endpoint answers.
        let stored = super::resolve_connection(None, None, home_path()).expect("resolves");
        assert_eq!(stored.endpoint, "https://stored:7777");

        // Enrolled with no endpoint from any source: a refusal naming the
        // sources, never the unusable plaintext default.
        let enrolled_dir = home.path().join("enrolled");
        seeded_identity(&enrolled_dir);
        let refused = super::resolve_connection(None, Some(&enrolled_dir), home_path())
            .expect_err("an enrolled, endpointless invocation must refuse");
        let refusal = refused.to_string();
        assert!(
            refusal.contains("--endpoint") && refusal.contains("HORDE_ENDPOINT"),
            "the refusal says where endpoints come from: {refusal}"
        );

        // The same dir with a stored endpoint resolves, marked explicit.
        horde_identity::store::save_endpoint(&enrolled_dir, "https://joined:7777").expect("saves");
        let joined =
            super::resolve_connection(None, Some(&enrolled_dir), home_path()).expect("resolves");
        assert_eq!(joined.endpoint, "https://joined:7777");
        assert!(joined.enrolled && joined.explicit_identity);
    }

    /// The transport table (ADR-0007): explicit identity always speaks mTLS,
    /// an `http://` endpoint is plaintext whatever the default dir holds,
    /// and `https://` needs an identity — enrolled speaks, unenrolled is
    /// refused with the fix rather than dialed into a TLS mystery.
    #[test]
    fn the_transport_table_separates_mtls_plaintext_and_the_refusal() {
        let resolved = |endpoint: &str, explicit: bool, enrolled: bool| super::ResolvedConnection {
            endpoint: endpoint.to_owned(),
            identity_dir: std::path::PathBuf::from("/tmp/id"),
            explicit_identity: explicit,
            enrolled,
        };
        use super::{Transport, transport_for};
        assert_eq!(
            transport_for(&resolved("http://h:1", true, false)),
            Transport::Mtls,
            "an explicit --identity is never quietly ignored"
        );
        assert_eq!(
            transport_for(&resolved("http://h:1", false, true)),
            Transport::Plaintext,
            "an explicit plaintext endpoint stays plaintext whatever ~/.horde/id holds"
        );
        assert_eq!(
            transport_for(&resolved("https://h:1", false, true)),
            Transport::Mtls,
            "the defaulted dir speaks for an mTLS endpoint"
        );
        assert_eq!(
            transport_for(&resolved("https://h:1", false, false)),
            Transport::RefusedUnenrolled
        );
    }

    /// The host seam minting composes tokens from: scheme off, port off,
    /// path off, and an empty host is nothing rather than an empty token.
    #[test]
    fn the_endpoint_host_extracts_or_declines() {
        use super::host_of_endpoint;
        assert_eq!(
            host_of_endpoint("https://horde.example:7777"),
            Some("horde.example")
        );
        assert_eq!(
            host_of_endpoint("http://127.0.0.1:7777/x"),
            Some("127.0.0.1")
        );
        assert_eq!(host_of_endpoint("localhost:7777"), Some("localhost"));
        assert_eq!(host_of_endpoint("localhost"), Some("localhost"));
        assert_eq!(host_of_endpoint("https://"), None);
        assert_eq!(host_of_endpoint(""), None);
    }

    /// A mint answer becomes one self-contained token — dialed host, served
    /// port, unchanged code — and every honest reason not to compose one
    /// (no port, an unparseable code) is `None`, not a guessed token.
    #[test]
    fn a_mint_answer_composes_the_token_or_declines() {
        let response = |code: &str, port: u32| horde_proto::v1::MintEnrollmentCodeResponse {
            code: code.to_owned(),
            server_cert_fingerprint: "a".repeat(64),
            expires_at: None,
            enrollment_port: port,
        };
        let code = format!("00ff.{}", "a".repeat(64));

        let token = super::token_from_mint("https://horde.example:7777", &response(&code, 7778))
            .expect("a full answer composes");
        assert_eq!(
            token.to_string(),
            format!("horde://join/horde.example:7778/{code}")
        );

        assert_eq!(
            super::token_from_mint("https://horde.example:7777", &response(&code, 0)),
            None,
            "no port means no listener to name"
        );
        assert_eq!(
            super::token_from_mint("https://horde.example:7777", &response("garbled", 7778)),
            None,
            "a code that does not parse must not ride a token"
        );
        assert_eq!(
            super::token_from_mint("https://", &response(&code, 7778)),
            None,
            "no host means no token"
        );
    }

    /// The identity read back for telemetry is the one enrollment wrote: a
    /// directory holding a CA-signed certificate yields its common name, and
    /// every failure shape yields `None` rather than a refusal.
    #[test]
    fn the_enrolled_principal_is_read_best_effort_from_the_identity_dir() {
        let directory = tempfile::tempdir().expect("a temp dir");
        seeded_identity(directory.path());

        assert_eq!(
            super::enrolled_principal(Some(directory.path())).as_deref(),
            Some("client:laptop"),
            "an enrolled directory names its principal"
        );
        assert_eq!(
            super::enrolled_principal(None),
            None,
            "no --identity, no principal"
        );
        assert_eq!(
            super::enrolled_principal(Some(std::path::Path::new("/nonexistent/iddir"))),
            None,
            "an unreadable directory enriches nothing and refuses nothing"
        );
    }
}

#[cfg(test)]
mod forward_tests {
    use clap::Parser;

    use super::{Cli, Command, ForwardCommand, forward_name_or_default};

    /// Forwards are named, not numbered — but the CLI defaults rather than
    /// refuses, mirroring the agent verb where the name is the declarer's
    /// choice.
    #[test]
    fn a_nameless_forward_defaults_to_its_port() {
        assert_eq!(forward_name_or_default("vite", 5173), "vite");
        assert_eq!(forward_name_or_default("", 5173), "port-5173");
    }

    /// The verb surface, as ADR-0006 names it: `expose <port> --as <name>`,
    /// `inject` with the worker port defaulted, `ls`, `close`, `serve`.
    #[test]
    fn the_forward_verbs_parse_as_the_adr_names_them() {
        let cli = Cli::try_parse_from([
            "horde", "forward", "expose", "s1", "agent-1", "5173", "--as", "vite",
        ])
        .expect("the expose verb parses");
        match cli.command {
            Some(Command::Forward(ForwardCommand::Expose(args))) => {
                assert_eq!(args.session_id, "s1");
                assert_eq!(args.agent_id, "agent-1");
                assert_eq!(args.port, 5173);
                assert_eq!(args.name, "vite");
                assert_eq!(
                    args.local_port, None,
                    "the local port is the client's choice"
                );
            }
            other => panic!("expected forward expose, got {other:?}"),
        }

        let cli = Cli::try_parse_from(["horde", "forward", "inject", "s1", "agent-1", "5432"])
            .expect("the inject verb parses");
        match cli.command {
            Some(Command::Forward(ForwardCommand::Inject(args))) => {
                assert_eq!(args.port, 5432);
                assert_eq!(
                    args.worker_port, None,
                    "the worker port defaults to the local one"
                );
            }
            other => panic!("expected forward inject, got {other:?}"),
        }

        assert!(matches!(
            Cli::try_parse_from(["horde", "forward", "ls"])
                .expect("ls parses")
                .command,
            Some(Command::Forward(ForwardCommand::Ls { session: None }))
        ));
        assert!(matches!(
            Cli::try_parse_from(["horde", "forward", "close", "s1", "fwd-1"])
                .expect("close parses")
                .command,
            Some(Command::Forward(ForwardCommand::Close { .. }))
        ));
        assert!(matches!(
            Cli::try_parse_from(["horde", "forward", "serve", "--session", "s1"])
                .expect("serve parses")
                .command,
            Some(Command::Forward(ForwardCommand::Serve { session: Some(_) }))
        ));
    }
}

#[cfg(test)]
mod forward_flow_tests {
    //! The foreground forward verbs, driven whole against a real server —
    //! the shutdown is an injected future (the worker daemon's own shape),
    //! which is what makes "until ctrl-c" testable without a signal.

    use horde_proto::v1::{ForwardDirection, ForwardSpec};

    use crate::forward::testing::within;

    use super::{Format, ForwardCommand, run_forward};

    /// One foreground verb driven to completion with an instant shutdown —
    /// the shared shape of every flow case here.
    async fn run_headless(
        client: &mut crate::Client,
        command: ForwardCommand,
    ) -> Result<(), crate::ClientError> {
        within(
            "the flow completes",
            run_forward(
                client,
                &command,
                None,
                Format::Human,
                std::future::ready(()),
            ),
        )
        .await
    }

    async fn placed_server() -> (horde_server::service::ServerCore, crate::Client) {
        let (core, client) = crate::forward::testing::served().await;
        horde_server_testkit::place_agent(&core.log, "s1", "agent-1", "w1", "serve")
            .await
            .expect("the placement seeds");
        (core, client)
    }

    fn named(forwards: &[horde_proto::v1::ForwardStatus]) -> Vec<String> {
        forwards
            .iter()
            .filter_map(|status| status.intent.as_ref())
            .filter_map(|intent| intent.spec.as_ref())
            .map(|spec| spec.name.clone())
            .collect()
    }

    /// The dispatch reports refusals rather than swallowing them: closing a
    /// forward nobody opened must come back as the server's error.
    #[tokio::test]
    async fn the_forward_dispatch_reports_refusals() {
        let (_core, mut client) = placed_server().await;
        let refused = run_headless(
            &mut client,
            ForwardCommand::Close {
                session_id: "s1".to_owned(),
                forward_id: "fwd-none".to_owned(),
                reason: String::new(),
            },
        )
        .await;
        assert!(refused.is_err(), "a refused verb must not report success");
    }

    /// The whole expose verb: declared on the server, realized locally, and
    /// torn down at shutdown — the journaled intent is the proof the flow
    /// ran, and it must carry the name the arguments chose.
    #[tokio::test]
    async fn the_expose_verb_declares_and_realizes_until_shutdown() {
        let (_core, mut client) = placed_server().await;
        run_headless(
            &mut client,
            ForwardCommand::Expose(super::ForwardExposeArgs {
                session_id: "s1".to_owned(),
                agent_id: "agent-1".to_owned(),
                port: 4242,
                name: "vite".to_owned(),
                local_port: None,
            }),
        )
        .await
        .expect("the flow succeeds");

        let listed = client.list_forwards(Some("s1")).await.expect("the list");
        assert_eq!(
            named(&listed),
            vec!["vite".to_owned()],
            "the verb's whole point is the journaled intent"
        );
    }

    /// The inject verb under a real identity: the initiator is journaled as
    /// the backer — the acceptance-and-backing-are-one-act rule, spelled by
    /// the CLI.
    #[tokio::test]
    async fn the_inject_verb_backs_under_its_own_principal() {
        let harness = horde_server_testkit::secure::serve_secured()
            .await
            .expect("the secured server starts");
        let identity = harness
            .enroll(horde_identity::PrincipalRole::Client, "laptop")
            .await
            .expect("the client enrolls");
        horde_server_testkit::place_agent(&harness.log, "s1", "agent-1", "w1", "reach")
            .await
            .expect("the placement seeds");
        // Dialed with this crate's own client type: the testkit's helper
        // returns its *own* horde-cli dependency's `Client`, which is a
        // different crate instance inside this crate's tests.
        let mut client = crate::Client::connect_with_identity(&harness.main_endpoint(), &identity)
            .await
            .expect("the enrolled client dials");

        run_headless(
            &mut client,
            ForwardCommand::Inject(super::ForwardInjectArgs {
                session_id: "s1".to_owned(),
                agent_id: "agent-1".to_owned(),
                port: 5432,
                name: "db".to_owned(),
                worker_port: None,
            }),
        )
        .await
        .expect("the flow succeeds");

        let listed = client.list_forwards(Some("s1")).await.expect("the list");
        assert_eq!(named(&listed), vec!["db".to_owned()]);
        assert_eq!(
            listed[0]
                .intent
                .as_ref()
                .and_then(|intent| intent.backer_client_id.as_ref())
                .map(ToString::to_string),
            Some("laptop".to_owned()),
            "the initiator is the backer"
        );
    }

    /// Serve realizes exactly the live exposes and says how many — the one
    /// observable a headless caller can hold the verb to.
    #[tokio::test]
    async fn serve_counts_what_it_realized() {
        let (_core, mut client) = placed_server().await;
        client
            .open_forward(
                "s1",
                "agent-1",
                ForwardSpec {
                    direction: ForwardDirection::Expose as i32,
                    name: "vite".to_owned(),
                    worker_port: 4242,
                    client_port: 0,
                },
                "open-1",
            )
            .await
            .expect("the intent journals");

        let realized = within(
            "the serve flow completes",
            super::run_forward_serve(
                &mut client,
                Some("s1"),
                None,
                Format::Human,
                std::future::ready(()),
            ),
        )
        .await
        .expect("the flow succeeds");
        assert_eq!(realized, 1, "one live expose, one realization");
    }

    /// Backing over a plaintext channel is refused by the server, and the
    /// verb reports it — a serve that cannot back must not claim it did.
    #[tokio::test]
    async fn an_unbackable_serve_reports_the_refusal() {
        let (_core, mut client) = placed_server().await;
        let outcome = within(
            "the serve returns",
            super::run_forward_serve(
                &mut client,
                Some("s1"),
                None,
                Format::Human,
                std::future::pending(),
            ),
        )
        .await;
        assert!(
            outcome.is_err(),
            "the unauthenticated serve stream's refusal must surface"
        );
    }

    /// The `--session` filter resolves like every other session reference
    /// (ADR-0008): a name becomes the id, absence stays absent, and an
    /// unmatched reference passes through for the server to refuse.
    #[tokio::test]
    async fn the_session_filter_resolves_names_and_absence_stays_absent() {
        let (_core, mut client) = crate::forward::testing::served().await;
        let (summary, _root) = client
            .create_session("Fwd Home", "key-9")
            .await
            .expect("a session creates");
        let id = summary.session_id.map(|id| id.to_string()).expect("an id");

        assert_eq!(
            super::resolved_filter(&mut client, Some("fwd-home"))
                .await
                .expect("a name resolves"),
            Some(id)
        );
        assert_eq!(
            super::resolved_filter(&mut client, None)
                .await
                .expect("no filter is no filter"),
            None
        );
        assert_eq!(
            super::resolved_filter(&mut client, Some("session-nope"))
                .await
                .expect("unmatched passes through"),
            Some("session-nope".to_owned())
        );
    }

    /// The backing lines are the operator's map of what this process is
    /// serving; returned rather than printed so they are assertable.
    #[test]
    fn backing_lines_render_what_is_backed() {
        let targets: crate::forward::InjectTargets = std::sync::Arc::default();
        assert!(super::backing_lines(&targets, Format::Human).is_empty());

        targets.write().expect("writable").insert(
            "fwd-db".to_owned(),
            crate::forward::InjectTarget {
                name: "db".to_owned(),
                client_port: 5432,
            },
        );
        assert_eq!(
            super::read_targets(&targets).len(),
            1,
            "the snapshot carries the map"
        );
        let lines = super::backing_lines(&targets, Format::Human);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("db"), "{}", lines[0]);
        assert!(lines[0].contains("5432"), "{}", lines[0]);
    }

    /// Stopping every realization is provable, twice over: the port stops
    /// answering, and — through an owned witness — the accept loop is
    /// already gone when `stop_all` returns. The second half is what
    /// separates a real stop from `Drop`'s fire-and-forget abort: on a
    /// current-thread runtime, an un-awaited abort has not run when the
    /// assertion looks.
    #[tokio::test]
    async fn stop_all_reaps_every_listener() {
        let (_core, client) = placed_server().await;
        let realized = crate::forward::realize_expose(&client, "fwd-x", "x", 4242, 0)
            .await
            .expect("the listener binds");
        let local_addr = realized.local_addr;

        super::stop_all(vec![realized]).await;
        assert!(
            tokio::net::TcpStream::connect(local_addr).await.is_err(),
            "a stopped listener must not keep its port"
        );

        let (guarded, mut reaped) = crate::forward::testing::guarded_realization();
        super::stop_all(vec![guarded]).await;
        assert!(
            matches!(
                reaped.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ),
            "stop_all must return only after every loop is gone"
        );
    }

    /// The foreground lifetime ends on the signal and nothing else: a wait
    /// that completed with nobody sending one would turn every foreground
    /// verb into an instant exit. Biased order makes the probe
    /// deterministic — the wait is polled first, and only its pending-ness
    /// lets the ready arm win.
    #[tokio::test]
    async fn the_foreground_wait_outlives_a_ready_future() {
        tokio::select! {
            biased;
            () = super::ctrl_c_ends() => {
                panic!("nobody sent a signal, so the wait must not end")
            }
            () = std::future::ready(()) => {}
        }
    }
}
