//! What the binary does, in a form that can be tested.
//!
//! `bin/horde_server.rs` is left as subscriber setup and an exit code. Anything
//! with a decision in it lives here, because a binary's functions are reachable
//! only by running the binary — which coverage cannot see and mutation testing
//! therefore reports as untested however carefully it was written.
//!
//! Two invocations exist, and one of them must be asked for by name (a bare
//! `horde-server` is a refusal, ADR-0007's 2026-10-31 amendment).
//! `--check [db]` opens the log and exits — the health check. With
//! `--serve`, the networked entrypoint DESIGN §3 sequenced into wave 5 comes
//! up: the CA is loaded or created, both TLS listeners bind, and — the
//! bootstrap answer to "who mints the first code with no client enrolled to
//! mint it" — a first client enrollment code is minted and printed exactly
//! when no client holds an active certificate. Possession of the server's
//! stdout is the root of trust, which is the same trust the disk already
//! grants.

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use crate::EventLog;
use crate::error::ServerError;
use crate::service::ServerCore;
use crate::service::enrollment::{EnrollmentAuthority, MintedCode, wall_clock_ms};
use crate::transport::BoundListener;
use horde_identity::{CertificateAuthority, Principal, PrincipalRole};

/// The log's file name inside the default state directory.
pub const DATABASE_FILE: &str = "horde.db";

/// The server's state directory under `$HOME` when the operator does not
/// say — the same `server/` that `horde up` lays out under `~/.horde`, and
/// never the working directory, where a stray `horde.db` would land in
/// whatever checkout the server happened to start from.
pub const DEFAULT_STATE_SUBDIR: &str = ".horde/server";

/// The lock a serving server holds on its state directory, so a second
/// server on the same state refuses instead of sharing a CA and a log.
pub const STATE_LOCK_FILE: &str = "server.lock";

/// The principal name of the bootstrap client code.
pub const BOOTSTRAP_CLIENT_NAME: &str = "operator";

/// Opens the log the server will serve from.
///
/// Startup proves the log opens whether or not it goes on to serve: a corrupt
/// or unwritable database fails here rather than on the first append.
pub fn open_log(path: &str) -> Result<EventLog, ServerError> {
    let log = EventLog::open(path)?;
    tracing::info!(%path, "event log opened");
    Ok(log)
}

/// How to serve: addresses, identity directory, and extra names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServeConfig {
    pub database: String,
    /// Where the CA persists. Losing it orphans every issued certificate.
    pub state_dir: PathBuf,
    /// The mTLS listener both spines share.
    pub bind: SocketAddr,
    /// The TLS-only enrollment listener.
    pub enrollment_bind: SocketAddr,
    /// Extra names the listener credential must answer to, beyond
    /// `localhost` and the loopback addresses — the names remote peers dial.
    pub subject_names: Vec<String>,
    /// The blob store's per-blob cap; `None` takes the documented default
    /// ([`crate::blob::DEFAULT_MAX_BLOB_BYTES`]). This is DESIGN §8's
    /// "server-configurable size cap".
    pub blob_max_bytes: Option<u64>,
    /// Serve the git Relay here (DESIGN §8, fix a10). `None` serves no
    /// relay; loopback only — see [`crate::relay::mount`].
    pub relay_bind: Option<SocketAddr>,
    /// The worker-lost lease (ADR-0011); `None` takes
    /// [`crate::liveness::DEFAULT_WORKER_LEASE`].
    pub worker_lease: Option<std::time::Duration>,
}

/// What one command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Invocation {
    /// `--check`: open the log, report, exit — the health check.
    CheckOnly { database: String },
    /// `--serve`: the networked server.
    Serve(Box<ServeConfig>),
    /// `--remint`: have the server on this state re-mint its bootstrap token.
    Remint { state_dir: PathBuf },
    /// `--help` or `--version`: text for stdout, then a successful exit.
    Print(String),
}

/// The command line, as clap reads it. Every serve-time flag requires
/// `--serve`, and one of `--serve` or `--check` is required: a bare
/// `horde-server` used to open (and create) `horde.db` in whatever
/// directory it ran from, which is how stray databases landed in
/// checkouts. Values may not start with `-`, so a forgotten value never
/// swallows the next flag as a path.
#[derive(clap::Parser, Debug)]
#[command(
    name = "horde-server",
    version,
    about = "The Horde control plane: the event log, the mTLS listeners, and the enrollment CA.",
    group = clap::ArgGroup::new("mode").required(true).args(["serve", "check", "remint"]),
    group = clap::ArgGroup::new("stateful").multiple(false).args(["serve", "remint"])
)]
struct ServerArgs {
    /// The event log's SQLite file. Defaults to `~/.horde/server/horde.db`
    /// (inside the default state directory).
    #[arg(conflicts_with = "remint")]
    database: Option<PathBuf>,

    /// Run the networked server.
    #[arg(long)]
    serve: bool,

    /// Open the log and exit: the health check.
    #[arg(long)]
    check: bool,

    /// Ask the server running on this state directory to re-mint its
    /// bootstrap client token (it was printed at start and may have
    /// expired), and print the fresh one. Same-machine only: the state
    /// directory is the trust.
    #[arg(long)]
    remint: bool,

    /// Where the CA, blobs, and bootstrap token live. Defaults to
    /// `~/.horde/server`, created owner-only.
    #[arg(long, requires = "stateful")]
    state_dir: Option<PathBuf>,

    /// The mTLS listener both spines share.
    #[arg(long, requires = "serve", default_value = "127.0.0.1:7777")]
    bind: SocketAddr,

    /// The TLS-only enrollment listener.
    #[arg(long, requires = "serve", default_value = "127.0.0.1:7778")]
    enrollment_bind: SocketAddr,

    /// Serve the git Relay here (loopback only).
    #[arg(long, requires = "serve")]
    relay_bind: Option<SocketAddr>,

    /// An extra name the listener certificate answers to — the host other
    /// machines dial. Repeatable. Join tokens name the first one when the
    /// listener binds the unspecified address. Must be an IP literal or a
    /// well-formed host name: an obvious typo is refused here rather than
    /// minted into the certificate every joiner pins.
    #[arg(long = "san", requires = "serve", value_parser = subject_name)]
    subject_names: Vec<String>,

    /// The blob store's per-blob cap in bytes.
    #[arg(long, requires = "serve")]
    blob_max_bytes: Option<u64>,

    /// Seconds a disconnected worker may stay away before its agents are
    /// declared lost.
    #[arg(long, requires = "serve", value_parser = clap::value_parser!(u64).range(1..))]
    worker_lease: Option<u64>,
}

/// `--san`'s parser: the name, once it passes the certificate-name checks.
fn subject_name(value: &str) -> Result<String, String> {
    horde_identity::check_subject_name(value).map(|()| value.to_owned())
}

/// Parses the command line, defaulting under `$HOME`.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong.
pub fn invocation(arguments: impl IntoIterator<Item = String>) -> Result<Invocation, String> {
    invocation_in(arguments, std::env::var_os("HOME").map(PathBuf::from))
}

/// [`invocation`] with `$HOME` passed in, so the defaults are testable.
///
/// Pure so every flag combination is testable without a process. Unknown
/// flags are errors rather than ignored: a mistyped `--bnid` silently
/// falling back to a default would serve on the wrong interface.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong, or no `$HOME`
/// to default under when nothing named the state.
pub fn invocation_in(
    arguments: impl IntoIterator<Item = String>,
    home: Option<PathBuf>,
) -> Result<Invocation, String> {
    use clap::Parser;
    let program = std::iter::once("horde-server".to_owned());
    let args = match ServerArgs::try_parse_from(program.chain(arguments)) {
        Ok(args) => args,
        Err(error) if !error.use_stderr() => return Ok(Invocation::Print(error.to_string())),
        Err(error) => return Err(error.to_string()),
    };
    let state_dir = match (args.state_dir, home) {
        (Some(dir), _) => dir,
        (None, Some(home)) => home.join(DEFAULT_STATE_SUBDIR),
        (None, None) if args.database.is_some() && args.check => PathBuf::new(),
        (None, None) => {
            return Err("HOME is unset, so pass --state-dir (and a database path)".to_owned());
        }
    };
    if args.remint {
        return Ok(Invocation::Remint { state_dir });
    }
    let database = args
        .database
        .unwrap_or_else(|| state_dir.join(DATABASE_FILE))
        .to_string_lossy()
        .into_owned();
    if args.check {
        return Ok(Invocation::CheckOnly { database });
    }
    Ok(Invocation::Serve(Box::new(ServeConfig {
        database,
        state_dir,
        // Loopback defaults: binding wider is a deliberate act, exactly as
        // before mTLS existed — the difference is that now it works.
        bind: args.bind,
        enrollment_bind: args.enrollment_bind,
        subject_names: args.subject_names,
        blob_max_bytes: args.blob_max_bytes,
        relay_bind: args.relay_bind,
        worker_lease: args.worker_lease.map(std::time::Duration::from_secs),
    })))
}

/// The listener credential's names: the defaults plus whatever the operator
/// asked for, split into the DNS and IP forms certificates distinguish.
#[must_use]
pub fn credential_names(subject_names: &[String]) -> (Vec<String>, Vec<IpAddr>) {
    let mut dns = vec!["localhost".to_owned()];
    let mut ips: Vec<IpAddr> = vec![
        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
    ];
    for name in subject_names {
        match name.parse::<IpAddr>() {
            Ok(ip) if !ips.contains(&ip) => ips.push(ip),
            Ok(_) => {}
            Err(_) if !dns.contains(name) => dns.push(name.clone()),
            Err(_) => {}
        }
    }
    (dns, ips)
}

/// A serving server: both listeners up, and the bootstrap code if one was
/// minted.
#[derive(Debug)]
pub struct RunningServer {
    pub main: BoundListener,
    pub enrollment: BoundListener,
    pub server_fingerprint: String,
    /// Present exactly when no client held an active certificate at startup.
    pub bootstrap_code: Option<MintedCode>,
    /// The enrollment listener as join tokens name it: `host:port` on a
    /// host the listener certificate answers to ([`advertised_host`]).
    pub advertised_enrollment: String,
    /// This server's hold on its state directory: a second server on the
    /// same state refuses while this lives.
    pub state_lock: horde_identity::StateLock,
    /// The mounted git Relay, when `--relay-bind` asked for one.
    pub relay: Option<crate::relay::MountedRelay>,
    /// One line per hub-placed MCP server the startup warm-up touched
    /// (ADR-0004): connected with its tool count, or unavailable with the
    /// cause. On the operator interface rather than only in tracing, because
    /// a misconfigured upstream must surface where the operator is looking.
    pub hub_lines: Vec<String>,
    /// The permission chain driver (DESIGN §10, wave-7 fix a5): layers 1–2
    /// live in a deployed server, not only in tests. Held so its lifetime is
    /// the server's.
    pub permission_chain: ChainTask,
    /// The enrollment authority both listeners share, kept so a hang-up can
    /// re-mint the bootstrap code without a restart ([`Self::remint_bootstrap`]).
    authority: Arc<EnrollmentAuthority>,
    /// Where the bootstrap-token file lives.
    state_dir: PathBuf,
}

/// The operator line a re-mint that found the bootstrap door closed prints.
pub const BOOTSTRAP_CLOSED_LINE: &str = "bootstrap-closed a client is enrolled; mint a code from it \
     with `horde enroll client <name>`";

impl RunningServer {
    /// Re-mints the bootstrap code on a running server (ADR-0007's deferred
    /// re-mint): a fresh code and token file while no client is enrolled —
    /// the earlier code stays redeemable until its own expiry — or, once
    /// one is, the file retired and the closed door named. Returns the
    /// operator line to print.
    ///
    /// # Errors
    ///
    /// The mint, the log, or the token file failing.
    pub async fn remint_bootstrap(&mut self) -> Result<String, ServerError> {
        let code = bootstrap_code_if_unclaimed(&self.authority, wall_clock_ms()).await?;
        sync_bootstrap_token_file(&self.state_dir, code.as_ref(), &self.advertised_enrollment)?;
        self.bootstrap_code = code;
        Ok(self
            .bootstrap_line()
            .unwrap_or_else(|| BOOTSTRAP_CLOSED_LINE.to_owned()))
    }

    /// The `bootstrap-client-token` line, while a bootstrap code is open.
    fn bootstrap_line(&self) -> Option<String> {
        self.bootstrap_code.as_ref().map(|code| {
            format!(
                "bootstrap-client-token {}",
                join_token_of(code, &self.advertised_enrollment)
            )
        })
    }
}

/// Holds a running server up until `shutdown` resolves, re-minting its
/// bootstrap code on every `hangups` message (the binary forwards SIGHUP
/// there) and handing each resulting operator line to `announce`. A closed
/// `hangups` only stops the re-minting; the server stays up.
pub async fn hold_until(
    running: &mut RunningServer,
    shutdown: impl std::future::Future<Output = ()>,
    hangups: &mut tokio::sync::mpsc::Receiver<()>,
    announce: &mut (dyn FnMut(String) + Send),
) {
    tokio::pin!(shutdown);
    let mut listening = true;
    loop {
        tokio::select! {
            () = &mut shutdown => return,
            got = hangups.recv(), if listening => match got {
                Some(()) => announce(match running.remint_bootstrap().await {
                    Ok(line) => line,
                    Err(error) => format!("bootstrap-remint-failed {error}"),
                }),
                None => listening = false,
            },
        }
    }
}

/// The chain driver's task, stopped when the server is dropped — the same
/// lifetime [`BoundListener`] gives the listeners.
#[derive(Debug)]
pub struct ChainTask {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ChainTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Brings the networked server up (DESIGN §3, activated in wave 5).
///
/// The CA persists in the state directory and is created on first start; the
/// listener credential is issued fresh each start (it never leaves the
/// process, and peers verify it against the CA — only an in-flight join
/// pins its fingerprint, and codes outlive it by minutes, not restarts).
///
/// # Errors
///
/// The log, the CA, the credential, or either bind failing.
pub async fn serve(config: &ServeConfig) -> Result<RunningServer, ServerError> {
    // Held before anything opens: a second server on this state would share
    // a CA, a log, and a bootstrap file with this one.
    let state_lock = horde_identity::StateLock::acquire(&config.state_dir, STATE_LOCK_FILE)
        .map_err(identity_startup)?;
    let log = open_log(&config.database)?;
    let now = wall_clock_ms();

    let (ca, credential, server_fingerprint) = listener_identity(config, now)?;

    // Both listeners bind before either serves (ADR-0007): each one's
    // service teaches the other's real port, and `:0` only has one after
    // the OS chose it.
    let main_listener = crate::transport::prebind(config.bind).await?;
    let enrollment_listener = crate::transport::prebind(config.enrollment_bind).await?;

    let authority = Arc::new(
        advertising_authority(
            &log,
            ca,
            credential,
            (main_listener.addr.port(), enrollment_listener.addr.port()),
            &config.state_dir,
        )
        .await?,
    );
    let bootstrap_code = bootstrap_code_if_unclaimed(&authority, now).await?;
    let advertised_enrollment = format!(
        "{}:{}",
        advertised_host(enrollment_listener.addr.ip(), &config.subject_names),
        enrollment_listener.addr.port()
    );
    sync_bootstrap_token_file(
        &config.state_dir,
        bootstrap_code.as_ref(),
        &advertised_enrollment,
    )?;

    let core = reconciled_core(config, log).await?;

    // The permission chain driver (fix a5): the deterministic layers run in
    // the deployed server, over the shipped registry (ADR-0009) — an agent's
    // own orchestrator verbs allowed, everything else escalated — until §13's
    // authoring layer exists. The judge stays a seam; a missing judge
    // already escalates.
    let permission_chain = ChainTask {
        task: crate::permission::spawn(
            &core.log,
            Arc::new(crate::permission::PermissionModes::shipped()),
            None,
        ),
    };

    let relay = mounted_relay(config, &core).await?;
    let hub_lines = warm_hub(&core).await?;

    let main = crate::transport::serve_main(main_listener, core, Arc::clone(&authority)).await?;
    let enrollment =
        crate::transport::serve_enrollment(enrollment_listener, Arc::clone(&authority)).await?;

    Ok(RunningServer {
        authority,
        state_dir: config.state_dir.clone(),
        main,
        enrollment,
        server_fingerprint,
        bootstrap_code,
        advertised_enrollment,
        state_lock,
        relay,
        hub_lines,
        permission_chain,
    })
}

/// The host a join token names for a listener bound at `bound`
/// (ADR-0007, 2026-10-31 amendment). A token must name a host the
/// listener certificate answers to, because the joiner stores
/// `https://<host>:<port>` and dials it over mTLS forever after — and
/// `0.0.0.0` is a bind address no peer can dial. So: a loopback bind or a
/// bound address that is itself a `--san` names itself; otherwise the
/// first `--san` (the name other machines were told to dial); with no
/// `--san`, a specific address names itself and the unspecified address
/// falls back to loopback — this machine only, which is honest.
#[must_use]
pub fn advertised_host(bound: IpAddr, subject_names: &[String]) -> String {
    let own = || match bound {
        IpAddr::V6(v6) => format!("[{v6}]"),
        IpAddr::V4(v4) => v4.to_string(),
    };
    if bound.is_loopback() || subject_names.contains(&bound.to_string()) {
        return own();
    }
    if let Some(first) = subject_names.first() {
        return match first.parse::<std::net::Ipv6Addr>() {
            Ok(v6) => format!("[{v6}]"),
            Err(_) => first.clone(),
        };
    }
    if bound.is_unspecified() {
        return "127.0.0.1".to_owned();
    }
    own()
}

/// The serving core: blob store and worker lease from `config`, and the log
/// reconciled at boot (ADR-0011) — this process holds no registrations, so
/// every agent the log places on a worker is on a worker that is away until
/// it dials in, treated exactly as a disconnect, lease and all, rather than
/// left placed forever on a machine nobody may ever see again. The session
/// projection is rebuilt from the log first, so the reconcile and the first
/// client read find it warm.
///
/// # Errors
///
/// The blob store being unusable, or the reconcile failing to read or append.
async fn reconciled_core(config: &ServeConfig, log: EventLog) -> Result<ServerCore, ServerError> {
    let core = ServerCore::new(log)
        .with_blob_store(Arc::new(blob_store(config)?))
        .with_worker_lease(
            config
                .worker_lease
                .unwrap_or(crate::liveness::DEFAULT_WORKER_LEASE),
        );
    // The one cold fold of the whole log (ADR-0013); every read after it
    // folds only what was appended since.
    core.sessions.rebuild().await?;
    crate::liveness::reconcile_at_boot(&core).await?;
    Ok(core)
}

/// The CA (loaded, or created on first start) and the listener credential
/// issued fresh from it for this start, with the fingerprint every join pins.
///
/// # Errors
///
/// The CA failing to load or persist, or the credential failing to issue.
fn listener_identity(
    config: &ServeConfig,
    now: i64,
) -> Result<
    (
        CertificateAuthority,
        horde_identity::ServerCredential,
        String,
    ),
    ServerError,
> {
    let ca = ca_for(&config.state_dir.join("ca"), now)?;
    let (dns, ips) = credential_names(&config.subject_names);
    let credential = ca.issue_server(now, &dns, &ips).map_err(identity_startup)?;
    let fingerprint = credential.fingerprint().map_err(identity_startup)?;
    Ok((ca, credential, fingerprint))
}

/// The enrollment authority `serve` stands up (ADR-0007): it advertises the
/// ports both listeners actually bound, and knows where the bootstrap token
/// rides the state dir so redeeming it can retire the file.
///
/// # Errors
///
/// The authority failing to read its state from the log.
async fn advertising_authority(
    log: &EventLog,
    ca: CertificateAuthority,
    credential: horde_identity::ServerCredential,
    (main_port, enrollment_port): (u16, u16),
    state_dir: &std::path::Path,
) -> Result<EnrollmentAuthority, ServerError> {
    Ok(EnrollmentAuthority::new(log.clone(), ca, credential)
        .await?
        .advertising_ports(main_port, enrollment_port)
        .with_bootstrap_token_file(bootstrap_token_path(state_dir)))
}

/// The blob store, beside the CA in the state directory. Failing here rather
/// than on the first upload matches the log's own rule: an unusable store
/// must fail at startup, not mid-payload.
///
/// # Errors
///
/// The store's directory being unusable.
fn blob_store(config: &ServeConfig) -> Result<crate::blob::BlobStore, ServerError> {
    let blobs = crate::blob::BlobStore::open(&config.state_dir.join("blobs"))
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    Ok(match config.blob_max_bytes {
        Some(max) => blobs.with_max_blob_bytes(max),
        None => blobs,
    })
}

/// The Relay, when `--relay-bind` asked for one (DESIGN §8, fix a10) —
/// mounted before the gRPC listeners so a bad relay bind fails startup whole
/// rather than leaving a half-up server.
///
/// # Errors
///
/// The mount failing: the bind, or the relay's own state directory.
async fn mounted_relay(
    config: &ServeConfig,
    core: &ServerCore,
) -> Result<Option<crate::relay::MountedRelay>, ServerError> {
    match config.relay_bind {
        Some(bind) => Ok(Some(
            crate::relay::mount(bind, &config.state_dir, &core.log).await?,
        )),
        None => Ok(None),
    }
}

/// Warms the MCP Hub's upstreams (ADR-0004, 2026-08-19 amendment) from the
/// journaled catalog — there is deliberately no flag for this: which servers
/// exist is `horde mcp add`'s to say, and the catalog rides the same log as
/// everything else. Warmed at startup so a misconfigured upstream surfaces
/// where the operator is looking; a failure is a reported line, not fatal,
/// because a dead user server must not hold the control plane down, and the
/// connect is retried at the next forwarded call anyway.
///
/// The lines come back (for [`operator_lines`]) as well as being traced: an
/// effect that only tracing sees is one no test can hold.
///
/// # Errors
///
/// Only the log failing to yield the catalog.
async fn warm_hub(core: &ServerCore) -> Result<Vec<String>, ServerError> {
    let lines = crate::hub::warm(core).await?;
    for line in &lines {
        tracing::info!("{line}");
    }
    Ok(lines)
}

/// The persisted CA, or a fresh one persisted on first start.
fn ca_for(ca_dir: &std::path::Path, now: i64) -> Result<CertificateAuthority, ServerError> {
    if CertificateAuthority::exists(ca_dir) {
        return CertificateAuthority::load(ca_dir).map_err(identity_startup);
    }
    let ca = CertificateAuthority::generate(now).map_err(identity_startup)?;
    ca.persist(ca_dir).map_err(identity_startup)?;
    Ok(ca)
}

/// The bootstrap answer (DESIGN §3 gap, decided here): a first client code,
/// minted exactly when no client holds an active certificate.
async fn bootstrap_code_if_unclaimed(
    authority: &EnrollmentAuthority,
    now: i64,
) -> Result<Option<MintedCode>, ServerError> {
    if authority.any_client_enrolled().await? {
        return Ok(None);
    }
    let operator =
        Principal::new(PrincipalRole::Client, BOOTSTRAP_CLIENT_NAME).map_err(identity_startup)?;
    Ok(Some(authority.mint(&operator, 0, now).await?))
}

/// Where the bootstrap token rides the filesystem (ADR-0007): the server's
/// state dir, which already carries the CA — same trust, same disk.
#[must_use]
pub fn bootstrap_token_path(state_dir: &std::path::Path) -> PathBuf {
    state_dir.join("bootstrap-token")
}

/// The one-token form of a minted code (ADR-0007): the enrollment listener
/// this server actually bound, as [`advertised_host`] names it, around the
/// unchanged `secret.fingerprint`.
#[must_use]
pub fn join_token_of(code: &MintedCode, advertised_enrollment: &str) -> horde_identity::JoinToken {
    horde_identity::JoinToken::new(
        advertised_enrollment,
        horde_identity::JoinCode {
            secret: code.secret.clone(),
            server_fingerprint: code.server_fingerprint.clone(),
        },
    )
}

/// Keeps the bootstrap-token file truthful at startup (ADR-0007): rewritten
/// with the fresh token while no client exists, removed when one does — a
/// stale token on disk after enrollment would be a mintable path the stdout
/// line no longer shows. User-read-only, through the same secret writer the
/// identity key uses.
fn sync_bootstrap_token_file(
    state_dir: &std::path::Path,
    code: Option<&MintedCode>,
    advertised_enrollment: &str,
) -> Result<(), ServerError> {
    let path = bootstrap_token_path(state_dir);
    match code {
        Some(code) => {
            let token = join_token_of(code, advertised_enrollment);
            horde_identity::store::write_secret(&path, &format!("{token}\n"))
                .map_err(identity_startup)
        }
        None => crate::service::enrollment::retire_bootstrap_token(&path).map_err(|error| {
            ServerError::Internal(format!(
                "could not remove the stale bootstrap token {}: {error}",
                path.display()
            ))
        }),
    }
}

/// The stdout lines `--serve` owes the operator: stable, greppable keys —
/// the addresses, the fingerprint every join pins, and the bootstrap code
/// when one was minted.
#[must_use]
pub fn operator_lines(running: &RunningServer) -> Vec<String> {
    let mut lines = vec![
        format!("listening main={}", running.main.addr),
        format!("listening enrollment={}", running.enrollment.addr),
        format!("server-fingerprint {}", running.server_fingerprint),
    ];
    if let Some(relay) = &running.relay {
        lines.push(format!("listening relay={}", relay.server.addr()));
    }
    lines.extend(running.hub_lines.iter().cloned());
    // The same token the state dir's bootstrap-token file carries: the
    // stdout line and the file must never disagree about what is mintable
    // (ADR-0007).
    lines.extend(running.bootstrap_line());
    lines
}

fn identity_startup(error: horde_identity::IdentityError) -> ServerError {
    ServerError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::open_log;

    #[test]
    fn opening_creates_a_usable_log() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let path = directory.path().join("horde.db");
        let path = path.to_string_lossy().into_owned();

        let log = open_log(&path).expect("opening must succeed");
        assert!(
            std::path::Path::new(&path).exists(),
            "opening must create the database"
        );
        drop(log);

        // Re-opening an existing database is the same code path as creating one.
        open_log(&path).expect("re-opening must succeed");
    }

    /// A path that cannot be opened must fail at startup rather than on the
    /// first append, when an agent is already mid-turn.
    #[test]
    fn an_unopenable_path_fails_at_startup() {
        let directory = tempfile::tempdir().expect("a temp dir");
        // A directory is not a database file.
        let path = directory.path().to_string_lossy().into_owned();

        open_log(&path).expect_err("a directory is not a database");
    }

    fn parsed(arguments: &[&str]) -> Result<super::Invocation, String> {
        super::invocation_in(
            arguments.iter().map(|s| (*s).to_owned()),
            Some("/home/me".into()),
        )
    }

    /// A mode is asked for by name: a bare invocation (or a bare database
    /// path, the old health check) is a refusal naming both modes, and
    /// `--check` is the health check — defaulting under `$HOME`, never the
    /// working directory.
    #[test]
    fn a_mode_must_be_named_and_check_is_the_health_check() {
        for bare in [&[][..], &["/tmp/x.db"][..]] {
            let refused = parsed(bare).expect_err("no mode named");
            assert!(
                refused.contains("--serve") && refused.contains("--check"),
                "{bare:?}: {refused}"
            );
        }
        assert_eq!(
            parsed(&["--check", "/tmp/x.db"]),
            Ok(super::Invocation::CheckOnly {
                database: "/tmp/x.db".to_owned()
            })
        );
        assert_eq!(
            parsed(&["--check"]),
            Ok(super::Invocation::CheckOnly {
                database: "/home/me/.horde/server/horde.db".to_owned()
            })
        );
        assert!(
            parsed(&["--check", "--serve"]).is_err(),
            "one mode at a time"
        );
    }

    /// Help and version are output, not refusals; with no `$HOME` and no
    /// named state there is nowhere honest to default to, unless the check
    /// names its own database.
    #[test]
    fn help_prints_and_a_homeless_default_refuses() {
        for flag in ["--help", "-h", "--version"] {
            let Ok(super::Invocation::Print(text)) = parsed(&[flag]) else {
                panic!("{flag} must print");
            };
            assert!(text.contains("horde-server"), "{flag}: {text}");
        }
        let homeless = |arguments: &[&str]| {
            super::invocation_in(arguments.iter().map(|s| (*s).to_owned()), None)
        };
        let refused = homeless(&["--serve"]).expect_err("nowhere to default to");
        assert!(refused.contains("--state-dir"), "{refused}");
        assert!(homeless(&["--check"]).is_err());
        assert_eq!(
            homeless(&["--check", "/tmp/x.db"]),
            Ok(super::Invocation::CheckOnly {
                database: "/tmp/x.db".to_owned()
            })
        );
        let Ok(super::Invocation::Serve(named)) =
            homeless(&["--serve", "--state-dir", "/srv/horde"])
        else {
            panic!("a named state dir needs no HOME");
        };
        assert_eq!(named.database, "/srv/horde/horde.db");
    }

    #[test]
    fn serve_parses_its_flags_and_defaults_the_rest() {
        let Ok(super::Invocation::Serve(config)) = parsed(&[
            "/tmp/x.db",
            "--serve",
            "--bind",
            "0.0.0.0:9000",
            "--enrollment-bind",
            "0.0.0.0:9001",
            "--state-dir",
            "/tmp/state",
            "--san",
            "horde.example",
            "--san",
            "10.0.0.7",
            "--blob-max-bytes",
            "1048576",
            "--relay-bind",
            "127.0.0.1:7420",
            "--worker-lease",
            "45",
        ]) else {
            panic!("a full serve line must parse");
        };
        assert_eq!(
            config.worker_lease,
            Some(std::time::Duration::from_secs(45))
        );
        assert_eq!(
            config.relay_bind.map(|addr| addr.to_string()),
            Some("127.0.0.1:7420".to_owned())
        );
        assert_eq!(config.database, "/tmp/x.db");
        assert_eq!(config.bind.to_string(), "0.0.0.0:9000");
        assert_eq!(config.enrollment_bind.to_string(), "0.0.0.0:9001");
        assert_eq!(config.state_dir, std::path::PathBuf::from("/tmp/state"));
        assert_eq!(config.subject_names, vec!["horde.example", "10.0.0.7"]);
        assert_eq!(config.blob_max_bytes, Some(1_048_576));

        let Ok(super::Invocation::Serve(defaulted)) = parsed(&["--serve"]) else {
            panic!("--serve alone must parse");
        };
        assert_eq!(defaulted.database, "/home/me/.horde/server/horde.db");
        assert_eq!(
            defaulted.bind.to_string(),
            "127.0.0.1:7777",
            "binding wider than loopback stays a deliberate act"
        );
        assert_eq!(defaulted.enrollment_bind.to_string(), "127.0.0.1:7778");
        assert_eq!(
            defaulted.state_dir,
            std::path::PathBuf::from("/home/me/.horde/server"),
            "state defaults under HOME, never the working directory"
        );
        assert_eq!(
            defaulted.blob_max_bytes, None,
            "unset lets the store keep its documented default"
        );
        assert_eq!(defaulted.worker_lease, None);
    }

    /// A mistyped flag silently falling back to a default would serve on the
    /// wrong interface; every wrong shape must be a refusal that names it.
    #[test]
    fn wrong_flags_are_refused_by_name() {
        for (arguments, needle) in [
            (&["--serve", "--bnid", "1.2.3.4:1"][..], "--bnid"),
            (&["--serve", "--bind"][..], "--bind"),
            (
                &["--serve", "--bind", "not-an-address"][..],
                "not-an-address",
            ),
            (&["--serve", "--enrollment-bind", "7777"][..], "7777"),
            // A forgotten value never swallows the next flag as a path, and
            // a dash-prefixed word is never the database.
            (&["--serve", "--state-dir", "--san", "x"][..], "--state-dir"),
            (&["--serve", "-rf"][..], "-r"),
            (&["--bind", "127.0.0.1:1"][..], "--serve"),
            // Each serve-only flag alone must trip the rule by itself: the
            // refusal is an OR over them, not a conjunction.
            (&["--enrollment-bind", "127.0.0.1:1"][..], "--serve"),
            (&["--state-dir", "/tmp/x"][..], "--serve"),
            (&["--blob-max-bytes", "1"][..], "--serve"),
            (&["--relay-bind", "127.0.0.1:1"][..], "--serve"),
            (
                &["--serve", "--relay-bind", "not-an-address"][..],
                "not-an-address",
            ),
            (&["--serve", "--blob-max-bytes", "lots"][..], "lots"),
            (&["--serve", "--blob-max-bytes"][..], "--blob-max-bytes"),
            (&["--worker-lease", "30"][..], "--serve"),
            (&["--serve", "--worker-lease", "soon"][..], "soon"),
            (&["--serve", "--worker-lease", "0"][..], "--worker-lease"),
            (&["--serve", "a.db", "b.db"][..], "b.db"),
            // An obviously invalid certificate name is refused at mint time,
            // naming the name and what is wrong with it.
            (
                &["--serve", "--san", "https://box.lan"][..],
                "\"https://box.lan\" is not a usable certificate name",
            ),
            (&["--serve", "--san", "box.lan:7777"][..], "port"),
        ] {
            let refused = parsed(arguments).expect_err("must refuse");
            assert!(
                refused.contains(needle),
                "{arguments:?} must be refused naming {needle}: {refused}"
            );
        }
    }

    #[test]
    fn credential_names_carry_the_loopbacks_and_split_operator_names() {
        let (dns, ips) = super::credential_names(&[
            "horde.example".to_owned(),
            "10.0.0.7".to_owned(),
            "horde.example".to_owned(), // duplicates collapse
            "127.0.0.1".to_owned(),
        ]);
        assert_eq!(dns, vec!["localhost", "horde.example"]);
        assert_eq!(
            ips.iter().map(ToString::to_string).collect::<Vec<_>>(),
            vec!["127.0.0.1", "::1", "10.0.0.7"],
            "IP literals must become IP SANs — a DNS SAN of `10.0.0.7` never matches"
        );
    }

    /// What a join token names, as a table (ADR-0007, 2026-10-31
    /// amendment): never the unspecified address, always a host the
    /// listener certificate answers to when one exists.
    #[test]
    fn tokens_name_a_host_the_certificate_answers_to() {
        let ip = |text: &str| text.parse::<std::net::IpAddr>().expect("an ip");
        let sans = |names: &[&str]| names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        for (bound, names, expected) in [
            ("127.0.0.1", &[][..], "127.0.0.1"),
            ("127.0.0.1", &["box.lan"][..], "127.0.0.1"),
            ("::1", &[][..], "[::1]"),
            ("0.0.0.0", &["box.lan", "10.0.0.7"][..], "box.lan"),
            ("0.0.0.0", &["fe80::1"][..], "[fe80::1]"),
            ("0.0.0.0", &[][..], "127.0.0.1"),
            ("::", &[][..], "127.0.0.1"),
            ("10.0.0.7", &["box.lan", "10.0.0.7"][..], "10.0.0.7"),
            ("10.0.0.7", &["box.lan"][..], "box.lan"),
            ("10.0.0.7", &[][..], "10.0.0.7"),
            ("fe80::2", &[][..], "[fe80::2]"),
        ] {
            assert_eq!(
                super::advertised_host(ip(bound), &sans(names)),
                expected,
                "{bound} with {names:?}"
            );
        }
    }

    /// A serve config rooted in `directory`: OS-chosen loopback ports, the
    /// relay bound only when a bind is given — the shape every serving test
    /// opens with.
    fn config_in(
        directory: &tempfile::TempDir,
        relay_bind: Option<std::net::SocketAddr>,
    ) -> super::ServeConfig {
        super::ServeConfig {
            database: directory
                .path()
                .join("horde.db")
                .to_string_lossy()
                .into_owned(),
            state_dir: directory.path().join("state"),
            bind: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            enrollment_bind: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            subject_names: Vec::new(),
            blob_max_bytes: None,
            relay_bind,
            worker_lease: None,
        }
    }

    /// The hub warm-up's effect is on the operator interface: a server
    /// started over a catalog whose upstream cannot spawn reports the
    /// unavailable line on stdout — not only in tracing, where no operator
    /// (and no test) would hold it.
    #[tokio::test]
    async fn serve_reports_the_hub_warm_up_on_the_operator_interface() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let database = directory
            .path()
            .join("horde.db")
            .to_string_lossy()
            .into_owned();

        // The catalog exists before this start — journaled by an earlier
        // `horde mcp add`, replayed here by opening the same log first.
        let log = super::open_log(&database).expect("the log opens");
        crate::hub::testfix::seed_catalog(
            &log,
            horde_proto::v1::McpServerConfig {
                name: "ghost".to_owned(),
                transport: Some(horde_proto::v1::mcp_server_config::Transport::Stdio(
                    horde_proto::v1::McpStdioTransport {
                        command: "horde-no-such-binary".to_owned(),
                        args: Vec::new(),
                        env: [].into(),
                    },
                )),
                local: false,
                allowed_agent_types: Vec::new(),
                etag: String::new(),
            },
        )
        .await;
        drop(log);

        let config = config_in(&directory, None);
        let running = super::serve(&config).await.expect("the server comes up");
        assert!(
            super::operator_lines(&running)
                .iter()
                .any(|line| line.starts_with("hub-server ghost unavailable:")),
            "the warm-up's outcome must reach the operator: {:?}",
            super::operator_lines(&running)
        );
    }

    /// The serving path, end to end: CA created and persisted, both
    /// listeners bound, the bootstrap code minted exactly when no client is
    /// enrolled. The certificates it issues are proven against real
    /// handshakes in `tests/enrollment.rs`; this holds the startup wiring.
    #[tokio::test]
    async fn serve_stands_up_and_bootstraps_exactly_once() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let config = config_in(
            &directory,
            Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        );

        let running = super::serve(&config).await.expect("the server comes up");
        assert_ne!(running.main.addr.port(), 0, "the OS chose a real port");
        let relay = running
            .relay
            .as_ref()
            .expect("--relay-bind mounts the relay");
        assert_ne!(relay.server.addr().port(), 0, "the relay bound a real port");
        assert!(
            super::operator_lines(&running)
                .iter()
                .any(|line| line.starts_with("listening relay=")),
            "the operator learns where the relay listens"
        );
        assert_ne!(running.enrollment.addr.port(), 0);
        assert_eq!(running.server_fingerprint.len(), 64);
        let code = running
            .bootstrap_code
            .as_ref()
            .expect("a fresh server has no client, so it must mint the first code");
        assert_eq!(
            running.advertised_enrollment,
            running.enrollment.addr.to_string(),
            "a loopback listener advertises itself"
        );
        let token = super::join_token_of(code, &running.advertised_enrollment);
        assert_eq!(
            token.code.server_fingerprint, running.server_fingerprint,
            "the printed token embeds the fingerprint the joiner pins"
        );
        assert_eq!(
            token.enrollment_endpoint(),
            format!("https://{}", running.enrollment.addr),
            "the token names the listener that actually bound"
        );
        assert!(
            horde_identity::CertificateAuthority::exists(&config.state_dir.join("ca")),
            "the CA must persist, or a restart orphans every certificate"
        );
        assert!(
            config.state_dir.join("blobs").join("sha256").is_dir(),
            "the blob store must come up beside the CA, not on the first upload"
        );

        // The operator interface, exactly as the binary will print it.
        let lines = super::operator_lines(&running);
        assert_eq!(lines.len(), 5, "{lines:?}");
        assert_eq!(lines[0], format!("listening main={}", running.main.addr));
        assert_eq!(
            lines[1],
            format!("listening enrollment={}", running.enrollment.addr)
        );
        assert_eq!(
            lines[2],
            format!("server-fingerprint {}", running.server_fingerprint)
        );
        assert_eq!(
            lines[3],
            format!(
                "listening relay={}",
                running.relay.as_ref().expect("mounted").server.addr()
            )
        );
        assert_eq!(lines[4], format!("bootstrap-client-token {token}"));

        // The same token rides the state dir, user-read-only (ADR-0007):
        // `horde up` reads it where the operator's eyes read stdout.
        let token_file = super::bootstrap_token_path(&config.state_dir);
        assert_eq!(
            std::fs::read_to_string(&token_file)
                .expect("the bootstrap token file exists")
                .trim(),
            token.to_string(),
            "the stdout line and the file must carry the same token"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&token_file)
                .expect("the token file exists")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "the token file is user-read-only");
        }

        // A second server on the same state refuses while this one lives,
        // naming the lock; the state dir itself is owner-only.
        let refused = super::serve(&config)
            .await
            .expect_err("one server per state dir");
        assert!(
            refused.to_string().contains("server.lock"),
            "the refusal names the held lock: {refused}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&config.state_dir)
                .expect("the state dir exists")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o700, "the state dir is owner-only");
        }

        // Dropping the server takes the chain driver with it: a leaked
        // driver would keep folding the (closed) log forever.
        let chain = running.permission_chain.task.abort_handle();
        drop(running);
        let mut chain_finished = false;
        for _ in 0..100_u32 {
            if chain.is_finished() {
                chain_finished = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            chain_finished,
            "drop must abort the permission chain driver"
        );

        // Second start, same state: the CA loads, and no client is enrolled
        // yet (a minted code is not an enrollment), so bootstrap re-offers.
        // This restart also drops `--relay-bind`: no relay is mounted and no
        // relay line is owed — the other half of `mounted_relay`'s table.
        let mut without_relay = config.clone();
        without_relay.relay_bind = None;
        let again = super::serve(&without_relay)
            .await
            .expect("a restart comes up");
        assert!(
            again.bootstrap_code.is_some(),
            "an unredeemed bootstrap code must not close the bootstrap door"
        );
        assert!(
            again.relay.is_none(),
            "no --relay-bind mounts no relay, quietly"
        );
        let rewritten = std::fs::read_to_string(super::bootstrap_token_path(&config.state_dir))
            .expect("the restart rewrites the bootstrap token file");
        assert_ne!(
            rewritten.trim(),
            token.to_string(),
            "each unclaimed boot mints (and files) a fresh token"
        );
    }

    /// The bootstrap file's other half: a server starting over a state that
    /// already has an enrolled client removes any stale token file rather
    /// than leaving a mintable path on disk (ADR-0007).
    #[tokio::test]
    async fn a_client_holding_restart_retires_a_stale_bootstrap_token_file() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let config = config_in(&directory, None);

        let running = super::serve(&config).await.expect("the server comes up");
        let token_file = super::bootstrap_token_path(&config.state_dir);
        assert!(token_file.is_file(), "a fresh boot files its token");

        // Enroll a client through the real listener, using the filed token —
        // exactly what `horde up` does — and watch the file retire.
        let token = std::fs::read_to_string(&token_file).expect("the token reads");
        let enrolled = enroll_client(&token).await;
        assert_eq!(
            enrolled.control_plane_port,
            Some(running.main.addr.port()),
            "the join answer teaches the control plane beside the token's host"
        );
        assert!(
            !token_file.exists(),
            "the file must retire the moment a client exists"
        );

        // Plant a stale file and restart: an enrolled client means no
        // bootstrap line and no file, however the file got there.
        drop(running);
        std::fs::write(&token_file, "stale\n").expect("the stale file plants");
        let again = super::serve(&config).await.expect("a restart comes up");
        assert!(
            again.bootstrap_code.is_none(),
            "an enrolled client closes the bootstrap door"
        );
        assert!(
            !token_file.exists(),
            "a client-holding restart removes the stale file"
        );
    }

    #[test]
    fn remint_names_its_state_and_takes_no_database() {
        assert_eq!(
            parsed(&["--remint", "--state-dir", "/srv/horde"]),
            Ok(super::Invocation::Remint {
                state_dir: "/srv/horde".into()
            })
        );
        assert_eq!(
            parsed(&["--remint"]),
            Ok(super::Invocation::Remint {
                state_dir: "/home/me/.horde/server".into()
            })
        );
        for refused in [
            &["--remint", "x.db"][..],
            &["--remint", "--serve"][..],
            &["--check", "--state-dir", "/x"][..],
        ] {
            assert!(parsed(refused).is_err(), "{refused:?}");
        }
    }

    /// Enrolls a client through the real listener with `token` — exactly
    /// what `horde up` does — asserting it became the bootstrap operator.
    async fn enroll_client(token: &str) -> horde_identity::EnrolledIdentity {
        let token = horde_identity::JoinToken::parse(token.trim()).expect("the token parses");
        let enrolled = horde_identity::join(
            &token.enrollment_endpoint(),
            &token.code,
            horde_proto::v1::Hello {
                semver: "0.1.0".to_owned(),
                capability_flags: Vec::new(),
                role: horde_proto::v1::Role::Client as i32,
                display_name: "up".to_owned(),
            },
        )
        .await
        .expect("the token enrolls");
        assert_eq!(enrolled.principal.to_string(), "client:operator");
        enrolled
    }

    /// The re-mint end to end, minus only the signal: `--remint`'s side
    /// finds this server by its state lock and asks through the channel the
    /// binary feeds from SIGHUP; the held server mints, files, and
    /// announces a fresh token, which redeems — and once a client exists,
    /// the next ask is refused naming the closed door.
    #[tokio::test]
    async fn a_running_server_remints_its_bootstrap_token_on_request() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let config = config_in(&directory, None);
        let mut running = super::serve(&config).await.expect("the server comes up");
        let first = running.bootstrap_line().expect("a first token");
        let pid = std::process::id();

        let (ask, mut hangups) = tokio::sync::mpsc::channel(1);
        let (stop, shutdown) = tokio::sync::oneshot::channel::<()>();
        let (announce, mut announced) = tokio::sync::mpsc::unbounded_channel();
        let held = tokio::spawn(async move {
            super::hold_until(
                &mut running,
                async move {
                    let _ = shutdown.await;
                },
                &mut hangups,
                &mut |line| {
                    let _ = announce.send(line);
                },
            )
            .await;
            running
        });
        let mut next_line = async || {
            tokio::time::timeout(std::time::Duration::from_secs(10), announced.recv())
                .await
                .expect("an announcement")
                .expect("the hold is alive")
        };

        let asked = ask.clone();
        let fresh = crate::remint::remint_running(
            &config.state_dir,
            move |signalled| {
                assert_eq!(signalled, pid, "the lock names this server");
                asked.try_send(()).map_err(std::io::Error::other)
            },
            std::time::Duration::from_secs(10),
        )
        .await
        .expect("a fresh token");
        assert_ne!(format!("bootstrap-client-token {fresh}"), first);
        assert_eq!(next_line().await, format!("bootstrap-client-token {fresh}"));
        enroll_client(&fresh).await;

        let closed = crate::remint::remint_running(
            &config.state_dir,
            |_| panic!("a closed door is refused before any signal"),
            std::time::Duration::from_secs(10),
        )
        .await
        .expect_err("a client exists");
        assert!(closed.contains("horde enroll client"), "{closed}");

        // Asked directly, the held server retires the door and says so.
        ask.send(()).await.expect("the hold listens");
        assert_eq!(next_line().await, super::BOOTSTRAP_CLOSED_LINE);
        // A closed hang-up channel stops only the re-minting: the hold
        // stays up until shutdown, which then ends it.
        drop(ask);
        tokio::task::yield_now().await;
        assert!(!held.is_finished(), "the server outlives its SIGHUP feed");
        let _ = stop.send(());
        let running = tokio::time::timeout(std::time::Duration::from_secs(10), held)
            .await
            .expect("the hold ends on shutdown")
            .expect("the hold task");
        assert!(running.bootstrap_code.is_none());
    }

    /// A re-mint that cannot file its token says so on the operator
    /// interface instead of ending the server.
    #[tokio::test]
    async fn a_failed_remint_is_announced_and_the_server_holds() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let config = config_in(&directory, None);
        let mut running = super::serve(&config).await.expect("the server comes up");
        let token_file = super::bootstrap_token_path(&config.state_dir);
        std::fs::remove_file(&token_file).expect("the token file goes");
        std::fs::create_dir(&token_file).expect("a directory squats its path");

        let (ask, mut hangups) = tokio::sync::mpsc::channel(1);
        ask.send(()).await.expect("queued");
        drop(ask);
        let mut announced = Vec::new();
        let shutdown = tokio::time::sleep(std::time::Duration::from_millis(300));
        super::hold_until(&mut running, shutdown, &mut hangups, &mut |line| {
            announced.push(line);
        })
        .await;
        assert_eq!(announced.len(), 1, "{announced:?}");
        assert!(
            announced[0].starts_with("bootstrap-remint-failed "),
            "{announced:?}"
        );
    }

    #[tokio::test]
    async fn remint_refuses_without_a_server_and_bounds_its_wait() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let refused = crate::remint::remint_running(
            directory.path(),
            |_| panic!("nobody to signal"),
            std::time::Duration::from_secs(10),
        )
        .await
        .expect_err("no server");
        assert!(refused.contains("no server is running"), "{refused}");

        // A held lock and a token that never changes: the wait gives up.
        let _held = horde_identity::StateLock::acquire(directory.path(), super::STATE_LOCK_FILE)
            .expect("the lock");
        std::fs::write(super::bootstrap_token_path(directory.path()), "same\n").expect("token");
        let silent = crate::remint::remint_running(
            directory.path(),
            |_| Ok(()),
            std::time::Duration::from_millis(100),
        )
        .await
        .expect_err("nothing re-minted");
        assert!(silent.contains("did not re-mint"), "{silent}");
        let unsignalled = crate::remint::remint_running(
            directory.path(),
            |_| Err(std::io::Error::other("no such process")),
            std::time::Duration::from_millis(100),
        )
        .await
        .expect_err("the signal failed");
        assert!(unsignalled.contains("could not signal"), "{unsignalled}");
    }

    #[tokio::test]
    async fn a_hangup_reaches_a_real_process_and_a_dead_pid_is_refused() {
        let mut sleeper = tokio::process::Command::new("sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()
            .expect("sleep spawns");
        let pid = sleeper.id().expect("a pid");
        // Delivery to a live pid succeeds. Whether the hang-up *ends* the
        // child is not asserted: a test run under `nohup` hands every child
        // an ignored SIGHUP, and `sleep` keeps it ignored.
        crate::remint::send_hangup(pid).expect("the hang-up sends");
        sleeper.start_kill().expect("the sleeper is killed");
        let ended = tokio::time::timeout(std::time::Duration::from_secs(10), sleeper.wait())
            .await
            .expect("a killed sleep ends")
            .expect("a status");
        assert!(!ended.success());
        assert!(
            crate::remint::send_hangup(pid).is_err(),
            "a reaped pid refuses"
        );
    }
}
