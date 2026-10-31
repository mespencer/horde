//! What the binary does, in a form that can be tested.
//!
//! `bin/horde_server.rs` is left as subscriber setup and an exit code. Anything
//! with a decision in it lives here, because a binary's functions are reachable
//! only by running the binary — which coverage cannot see and mutation testing
//! therefore reports as untested however carefully it was written.
//!
//! Two invocations exist. Bare `horde-server [db]` opens the log and exits —
//! the pre-wave-5 behavior, kept as the health check it always was. With
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

/// Where the log lives when the operator does not say.
pub const DEFAULT_DATABASE: &str = "horde.db";

/// Where identity material lives when the operator does not say.
pub const DEFAULT_STATE_DIR: &str = "horde-state";

/// The principal name of the bootstrap client code.
pub const BOOTSTRAP_CLIENT_NAME: &str = "operator";

/// Chooses the database path from the command line.
///
/// Separated from opening it so the choice is testable without touching a
/// filesystem: an empty argument is not a path, and treating it as one would
/// create a file named after nothing.
#[must_use]
pub fn database_path(arguments: impl IntoIterator<Item = String>) -> String {
    arguments
        .into_iter()
        .find(|argument| !argument.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_DATABASE.to_owned())
}

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
    /// Open the log, report, exit — the health check.
    CheckOnly { database: String },
    /// The networked server.
    Serve(Box<ServeConfig>),
}

/// What the flags said, before the shape rules are applied.
#[derive(Debug, Default)]
struct ParsedFlags {
    database: Option<String>,
    serve: bool,
    state_dir: Option<PathBuf>,
    bind: Option<SocketAddr>,
    enrollment_bind: Option<SocketAddr>,
    subject_names: Vec<String>,
    blob_max_bytes: Option<u64>,
    relay_bind: Option<SocketAddr>,
    worker_lease_secs: Option<u64>,
}

impl ParsedFlags {
    /// Folds one argument (consuming its value from `arguments` if it takes
    /// one) into the parse: the mode flag, then the serve-time flags, then
    /// whatever is left.
    fn apply(
        &mut self,
        argument: &str,
        arguments: &mut impl Iterator<Item = String>,
    ) -> Result<(), String> {
        let mut value_of = |flag: &str| {
            arguments
                .next()
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| format!("{flag} needs a value"))
        };
        if argument == "--serve" {
            self.serve = true;
            return Ok(());
        }
        if self.apply_serve_flag(argument, &mut value_of)? {
            return Ok(());
        }
        self.apply_bare(argument)
    }

    /// Applies one of the flags that configure how `--serve` serves.
    /// `Ok(false)` means the argument is not one of them. Split from the
    /// bind-address flags by subject so neither half outgrows the
    /// complexity bar as listeners accumulate.
    fn apply_serve_flag(
        &mut self,
        argument: &str,
        value_of: &mut impl FnMut(&str) -> Result<String, String>,
    ) -> Result<bool, String> {
        if self.apply_bind_flag(argument, value_of)? {
            return Ok(true);
        }
        match argument {
            "--state-dir" => self.state_dir = Some(PathBuf::from(value_of("--state-dir")?)),
            "--san" => self.subject_names.push(value_of("--san")?),
            "--blob-max-bytes" => {
                let value = value_of("--blob-max-bytes")?;
                self.blob_max_bytes = Some(
                    value
                        .parse()
                        .map_err(|_| format!("--blob-max-bytes {value} is not a byte count"))?,
                );
            }
            "--worker-lease" => {
                let value = value_of("--worker-lease")?;
                self.worker_lease_secs = Some(
                    value
                        .parse()
                        .ok()
                        .filter(|secs: &u64| *secs > 0)
                        .ok_or_else(|| {
                            format!("--worker-lease {value} is not a positive number of seconds")
                        })?,
                );
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Applies one of the three listener-address flags.
    fn apply_bind_flag(
        &mut self,
        argument: &str,
        value_of: &mut impl FnMut(&str) -> Result<String, String>,
    ) -> Result<bool, String> {
        match argument {
            "--bind" => self.bind = Some(parse_bind("--bind", &value_of("--bind")?)?),
            "--enrollment-bind" => {
                self.enrollment_bind = Some(parse_bind(
                    "--enrollment-bind",
                    &value_of("--enrollment-bind")?,
                )?);
            }
            "--relay-bind" => {
                self.relay_bind = Some(parse_bind("--relay-bind", &value_of("--relay-bind")?)?);
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// An argument that is not a known flag: the one positional (the
    /// database path), or a refusal naming what it was.
    fn apply_bare(&mut self, argument: &str) -> Result<(), String> {
        if argument.starts_with("--") {
            return Err(format!("unknown flag {argument}"));
        }
        if self.database.is_none() && !argument.trim().is_empty() {
            self.database = Some(argument.to_owned());
            return Ok(());
        }
        Err(format!("unexpected argument {argument}"))
    }

    /// Applies the shape rules and defaults, yielding the invocation.
    fn conclude(self) -> Result<Invocation, String> {
        let database = self.database.unwrap_or_else(|| DEFAULT_DATABASE.to_owned());
        if !self.serve {
            if self.bind.is_some()
                || self.enrollment_bind.is_some()
                || self.state_dir.is_some()
                || self.blob_max_bytes.is_some()
                || self.relay_bind.is_some()
                || self.worker_lease_secs.is_some()
            {
                return Err("--bind, --enrollment-bind, --state-dir, --blob-max-bytes, \
                     --relay-bind and --worker-lease need --serve"
                    .to_owned());
            }
            return Ok(Invocation::CheckOnly { database });
        }
        Ok(Invocation::Serve(Box::new(ServeConfig {
            database,
            state_dir: self
                .state_dir
                .unwrap_or_else(|| PathBuf::from(DEFAULT_STATE_DIR)),
            // Loopback defaults: binding wider is a deliberate act, exactly
            // as before mTLS existed — the difference is that now it works.
            bind: self
                .bind
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 7777))),
            enrollment_bind: self
                .enrollment_bind
                .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 7778))),
            subject_names: self.subject_names,
            blob_max_bytes: self.blob_max_bytes,
            relay_bind: self.relay_bind,
            worker_lease: self.worker_lease_secs.map(std::time::Duration::from_secs),
        })))
    }
}

/// A bind flag's value as an address, refused with the flag's own name.
fn parse_bind(flag: &str, value: &str) -> Result<SocketAddr, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} {value} is not an address:port"))
}

/// Parses the command line.
///
/// Pure so every flag combination is testable without a process. Unknown
/// flags are errors rather than ignored: a mistyped `--bnid` silently
/// falling back to a default would serve on the wrong interface.
///
/// # Errors
///
/// A human-readable refusal naming the flag that was wrong.
pub fn invocation(arguments: impl IntoIterator<Item = String>) -> Result<Invocation, String> {
    let mut flags = ParsedFlags::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        flags.apply(&argument, &mut arguments)?;
    }
    flags.conclude()
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
    sync_bootstrap_token_file(
        &config.state_dir,
        bootstrap_code.as_ref(),
        enrollment_listener.addr,
    )?;

    let core = ServerCore::new(log)
        .with_blob_store(Arc::new(blob_store(config)?))
        .with_worker_lease(
            config
                .worker_lease
                .unwrap_or(crate::liveness::DEFAULT_WORKER_LEASE),
        );
    // Boot reconciliation (ADR-0011): this process holds no registrations,
    // so every agent the log places on a worker is on a worker that is away
    // until it dials in — treated exactly as a disconnect, lease and all,
    // rather than left placed forever on a machine nobody may ever see again.
    let away = crate::liveness::reconcile_at_boot(&core).await?;
    if !away.is_empty() {
        tracing::info!(?away, "awaiting workers the log places agents on");
    }

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
    let enrollment = crate::transport::serve_enrollment(enrollment_listener, authority).await?;

    Ok(RunningServer {
        main,
        enrollment,
        server_fingerprint,
        bootstrap_code,
        relay,
        hub_lines,
        permission_chain,
    })
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
/// this server actually bound, around the unchanged `secret.fingerprint`.
#[must_use]
pub fn join_token_of(code: &MintedCode, enrollment_addr: SocketAddr) -> horde_identity::JoinToken {
    horde_identity::JoinToken::new(
        enrollment_addr.to_string(),
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
    enrollment_addr: SocketAddr,
) -> Result<(), ServerError> {
    let path = bootstrap_token_path(state_dir);
    match code {
        Some(code) => {
            let token = join_token_of(code, enrollment_addr);
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
    if let Some(code) = &running.bootstrap_code {
        // The same token the state dir's bootstrap-token file carries: the
        // stdout line and the file must never disagree about what is
        // mintable (ADR-0007).
        lines.push(format!(
            "bootstrap-client-token {}",
            join_token_of(code, running.enrollment.addr)
        ));
    }
    lines
}

fn identity_startup(error: horde_identity::IdentityError) -> ServerError {
    ServerError::Internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_DATABASE, database_path, open_log};

    #[test]
    fn the_first_argument_names_the_database() {
        assert_eq!(
            database_path(vec!["/tmp/horde-test.db".to_owned()]),
            "/tmp/horde-test.db"
        );
    }

    #[test]
    fn no_argument_falls_back_to_the_default() {
        assert_eq!(database_path(Vec::new()), DEFAULT_DATABASE);
    }

    /// An empty argument is not a path. Taking it literally would create a file
    /// named after nothing, which is worse than falling back.
    #[test]
    fn a_blank_argument_falls_back_rather_than_naming_a_file_after_nothing() {
        assert_eq!(database_path(vec![String::new()]), DEFAULT_DATABASE);
        assert_eq!(database_path(vec!["   ".to_owned()]), DEFAULT_DATABASE);
    }

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
        super::invocation(arguments.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn a_bare_invocation_is_the_health_check_it_always_was() {
        assert_eq!(
            parsed(&["/tmp/x.db"]),
            Ok(super::Invocation::CheckOnly {
                database: "/tmp/x.db".to_owned()
            })
        );
        assert_eq!(
            parsed(&[]),
            Ok(super::Invocation::CheckOnly {
                database: DEFAULT_DATABASE.to_owned()
            })
        );
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
        assert_eq!(defaulted.database, DEFAULT_DATABASE);
        assert_eq!(
            defaulted.bind.to_string(),
            "127.0.0.1:7777",
            "binding wider than loopback stays a deliberate act"
        );
        assert_eq!(defaulted.enrollment_bind.to_string(), "127.0.0.1:7778");
        assert_eq!(
            defaulted.state_dir,
            std::path::PathBuf::from(super::DEFAULT_STATE_DIR)
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
            (&["--bnid", "1.2.3.4:1"][..], "--bnid"),
            (&["--serve", "--bind"][..], "--bind"),
            (
                &["--serve", "--bind", "not-an-address"][..],
                "not-an-address",
            ),
            (
                &["--serve", "--enrollment-bind", "7777"][..],
                "--enrollment-bind 7777",
            ),
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
            (
                &["--serve", "--blob-max-bytes", "lots"][..],
                "--blob-max-bytes lots",
            ),
            (&["--serve", "--blob-max-bytes"][..], "--blob-max-bytes"),
            (&["--worker-lease", "30"][..], "--serve"),
            (
                &["--serve", "--worker-lease", "soon"][..],
                "--worker-lease soon",
            ),
            (&["--serve", "--worker-lease", "0"][..], "--worker-lease 0"),
            (&["a.db", "b.db"][..], "b.db"),
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
        let token = super::join_token_of(code, running.enrollment.addr);
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
        let token = horde_identity::JoinToken::parse(token.trim()).expect("the filed token parses");
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
        .expect("the filed token enrolls");
        assert_eq!(enrolled.principal.to_string(), "client:operator");
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
}
