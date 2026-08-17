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
    /// `Ok(false)` means the argument is not one of them.
    fn apply_serve_flag(
        &mut self,
        argument: &str,
        value_of: &mut impl FnMut(&str) -> Result<String, String>,
    ) -> Result<bool, String> {
        match argument {
            "--state-dir" => self.state_dir = Some(PathBuf::from(value_of("--state-dir")?)),
            "--bind" => self.bind = Some(parse_bind("--bind", &value_of("--bind")?)?),
            "--enrollment-bind" => {
                self.enrollment_bind = Some(parse_bind(
                    "--enrollment-bind",
                    &value_of("--enrollment-bind")?,
                )?);
            }
            "--san" => self.subject_names.push(value_of("--san")?),
            "--relay-bind" => {
                self.relay_bind = Some(parse_bind("--relay-bind", &value_of("--relay-bind")?)?);
            }
            "--blob-max-bytes" => {
                let value = value_of("--blob-max-bytes")?;
                self.blob_max_bytes = Some(
                    value
                        .parse()
                        .map_err(|_| format!("--blob-max-bytes {value} is not a byte count"))?,
                );
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
            {
                return Err(
                    "--bind, --enrollment-bind, --state-dir, --blob-max-bytes and \
                     --relay-bind need --serve"
                        .to_owned(),
                );
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

    let ca = ca_for(&config.state_dir.join("ca"), now)?;
    let (dns, ips) = credential_names(&config.subject_names);
    let credential = ca.issue_server(now, &dns, &ips).map_err(identity_startup)?;
    let server_fingerprint = credential.fingerprint().map_err(identity_startup)?;

    let authority = Arc::new(EnrollmentAuthority::new(log.clone(), ca, credential).await?);
    let bootstrap_code = bootstrap_code_if_unclaimed(&authority, now).await?;

    // The blob store lives beside the CA in the state directory. Failing
    // here rather than on the first upload matches the log's own rule: an
    // unusable store must fail at startup, not mid-payload.
    let mut blobs = crate::blob::BlobStore::open(&config.state_dir.join("blobs"))
        .map_err(|error| ServerError::Internal(error.to_string()))?;
    if let Some(max) = config.blob_max_bytes {
        blobs = blobs.with_max_blob_bytes(max);
    }

    let core = ServerCore::new(log).with_blob_store(Arc::new(blobs));

    // The permission chain driver (fix a5): the deterministic layers run in
    // the deployed server, over an escalate-everything default registry —
    // exactly what an unconfigured server should do with every request until
    // §13's authoring layer exists. The judge stays a seam; a missing judge
    // already escalates.
    let permission_chain = ChainTask {
        task: crate::permission::spawn(
            &core.log,
            Arc::new(crate::permission::PermissionModes::default()),
            None,
        ),
    };

    // The Relay (fix a10), before the gRPC listeners so a bad relay bind
    // fails startup whole rather than leaving a half-up server.
    let relay = match config.relay_bind {
        Some(bind) => Some(crate::relay::mount(bind, &config.state_dir, &core.log).await?),
        None => None,
    };

    let main = crate::transport::serve_main(config.bind, core, Arc::clone(&authority)).await?;
    let enrollment = crate::transport::serve_enrollment(config.enrollment_bind, authority).await?;

    Ok(RunningServer {
        main,
        enrollment,
        server_fingerprint,
        bootstrap_code,
        relay,
        permission_chain,
    })
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
    if let Some(code) = &running.bootstrap_code {
        lines.push(format!("bootstrap-client-code {}", code.join_code()));
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
        ]) else {
            panic!("a full serve line must parse");
        };
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

    /// The serving path, end to end: CA created and persisted, both
    /// listeners bound, the bootstrap code minted exactly when no client is
    /// enrolled. The certificates it issues are proven against real
    /// handshakes in `tests/enrollment.rs`; this holds the startup wiring.
    #[tokio::test]
    async fn serve_stands_up_and_bootstraps_exactly_once() {
        let directory = tempfile::tempdir().expect("a temp dir");
        let config = super::ServeConfig {
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
            relay_bind: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))),
        };

        let running = super::serve(&config).await.expect("the server comes up");
        assert_ne!(running.main.addr.port(), 0, "the OS chose a real port");
        let relay = running.relay.as_ref().expect("--relay-bind mounts the relay");
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
        assert!(
            code.join_code().ends_with(&running.server_fingerprint),
            "the printed code embeds the fingerprint the joiner pins"
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
        assert_eq!(
            lines[4],
            format!("bootstrap-client-code {}", code.join_code())
        );
        drop(running);

        // Second start, same state: the CA loads, and no client is enrolled
        // yet (a minted code is not an enrollment), so bootstrap re-offers.
        let again = super::serve(&config).await.expect("a restart comes up");
        assert!(
            again.bootstrap_code.is_some(),
            "an unredeemed bootstrap code must not close the bootstrap door"
        );
    }
}
