//! `horde up`: stand up this machine (ADR-0007, amended 2026-10-31).
//!
//! One command covers the single-machine day-one case by composing the
//! pieces that already exist — `horde-server --serve`, the token join, the
//! worker daemon — rather than growing a parallel path: start the server if
//! none is running, enroll the local client if none exists, prove the pair
//! with a real `Hello`, mint-and-join a local worker, print one ready line.
//! Same-machine trust rides the filesystem: the server's user-read-only
//! bootstrap-token file stands in for the token ceremony — read only from a
//! server this invocation started itself.
//!
//! What "already running" means is a fact the OS keeps, not a port that
//! happens to answer: each daemon holds a lock on its state directory, and
//! `up` holds one on its root, so a second `up` refuses and a running
//! server or worker is found rather than double-started.
//!
//! Ctrl-C (or SIGTERM) — during bring-up or after — tears down exactly what
//! this invocation started; a daemon it found running is left running. A
//! started daemon that dies is noticed, reported, and everything else torn
//! down.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::ClientError;

/// The bootstrap-token file's name inside the server's state dir — the
/// contract `horde-server`'s startup writes (its `bootstrap_token_path`).
/// Names rather than shared symbols because the CLI deliberately has no
/// dependency on the server or worker crates.
const BOOTSTRAP_TOKEN_FILE: &str = "bootstrap-token";

/// The lock `horde-server --serve` holds on its state dir (its
/// `STATE_LOCK_FILE`).
const SERVER_LOCK: &str = "server.lock";

/// The lock `horde-worker` holds on its state dir (its `STATE_LOCK_FILE`).
const WORKER_LOCK: &str = "worker.lock";

/// The lock this command holds on its root for its whole life.
const UP_LOCK: &str = "up.lock";

/// Where the server persists its CA inside its state dir.
const CA_DIR: &str = "ca";

/// The ambient join token the worker daemon redeems when it holds no
/// identity (its `JOIN_TOKEN_VAR`) — how the minted token reaches it
/// without appearing in the process list.
const WORKER_JOIN_TOKEN_VAR: &str = "HORDE_JOIN_TOKEN";

/// How long any single wait against a spawned daemon may take. Generous
/// against a loaded machine; a healthy start never spends it.
const STARTUP_CEILING: std::time::Duration = std::time::Duration::from_secs(60);

/// How long the readiness `Hello` may keep meeting a server that is still
/// coming up (connection refused, timed out) before giving up. Certificate
/// failures are never retried.
const HELLO_CEILING: std::time::Duration = std::time::Duration::from_secs(20);

/// How long a torn-down child may take to exit after SIGTERM before it is
/// killed outright.
const TEARDOWN_CEILING: std::time::Duration = std::time::Duration::from_secs(10);

/// How much of a daemon's stdout is kept for the waits to read. The lines
/// they wait on come first; a chatty daemon must not grow `up` without
/// bound.
const STDOUT_CAP: usize = 64 * 1024;

/// The longest forwarded line; longer ones are clipped with an ellipsis.
const LINE_CAP: usize = 2048;

/// The cadence every poll in this module waits at.
const POLL: std::time::Duration = std::time::Duration::from_millis(20);

/// What `horde up` takes: everything else is composition of defaults.
#[derive(clap::Args, Debug)]
pub struct UpArgs {
    /// Root for everything `up` owns: `server/`, `id/`, `worker/`.
    /// Defaults to `~/.horde`. The client identity follows the global
    /// `--identity` (or `HORDE_IDENTITY`) when one is given.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// A harness for the local worker (`fake`, `claude`, `codex`);
    /// repeatable, in order. The worker daemon owns the vocabulary and
    /// refuses anything it does not know.
    #[arg(long = "adapter")]
    pub adapters: Vec<String>,

    /// Where the server listens (and where a running one is dialed).
    #[arg(long, default_value = "127.0.0.1:7777")]
    pub bind: String,

    /// Where the server's enrollment listener binds when `up` starts one.
    #[arg(long, default_value = "127.0.0.1:7778")]
    pub enrollment_bind: String,

    /// The local worker's name. Defaults to this machine's hostname.
    #[arg(long)]
    pub name: Option<String>,

    /// A name other machines dial this server by; repeatable. Passed to a
    /// server `up` starts as `--san`, and advertised on the ready line.
    /// Needs a non-loopback `--bind` and `--enrollment-bind`.
    #[arg(long = "san")]
    pub subject_names: Vec<String>,
}

/// Where everything `up` owns lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpPaths {
    pub root: PathBuf,
    pub server_state: PathBuf,
    pub server_db: PathBuf,
    pub client_identity: PathBuf,
    pub worker_state: PathBuf,
}

/// The layout under a root: `server/` (state and db), `id/` (unless an
/// explicit identity dir says otherwise), `worker/`.
#[must_use]
pub fn paths_under(root: &Path, identity: Option<&Path>) -> UpPaths {
    UpPaths {
        root: root.to_path_buf(),
        server_state: root.join("server"),
        server_db: root.join("server").join("horde.db"),
        client_identity: identity.map_or_else(|| root.join("id"), Path::to_path_buf),
        worker_state: root.join("worker"),
    }
}

/// The root `up` works under: the flag's, or `~/.horde`.
///
/// # Errors
///
/// No flag and no `HOME` — there is nowhere honest to default to.
pub fn resolve_root(flag: Option<PathBuf>, home: Option<PathBuf>) -> Result<PathBuf, ClientError> {
    flag.or_else(|| home.map(|home| home.join(".horde")))
        .ok_or_else(|| ClientError::Up("needs --state-dir when HOME is unset".to_owned()))
}

/// A worker name a certificate can carry: the hostname with anything a
/// principal name refuses mapped to `-`, clipped to the 64-character cap;
/// a hostname that yields nothing falls back to a fixed name.
#[must_use]
pub fn worker_name_from(hostname: Option<String>) -> String {
    let sanitized: String = hostname
        .unwrap_or_default()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .take(64)
        .collect();
    if sanitized.is_empty() || sanitized.chars().all(|c| c == '-') {
        "this-machine".to_owned()
    } else {
        sanitized
    }
}

/// This machine's hostname, best effort: `up` must not fail over a name.
fn hostname() -> Option<String> {
    let output = std::process::Command::new("hostname").output().ok()?;
    let name = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if name.is_empty() { None } else { Some(name) }
}

/// A product binary expected beside the running `horde` binary.
///
/// # Errors
///
/// The sibling not existing — named, with where it was looked for.
pub fn sibling_binary(own_binary: &Path, name: &str) -> Result<PathBuf, ClientError> {
    let dir = own_binary.parent().unwrap_or(Path::new("."));
    let candidate = dir.join(name);
    if candidate.is_file() {
        return Ok(candidate);
    }
    Err(ClientError::Up(format!(
        "runs {name} but found none at {}; install it beside the horde binary",
        candidate.display()
    )))
}

/// Where forwarded daemon lines go once the machine is up.
pub type Forward = Arc<dyn Fn(String) + Send + Sync>;

/// Everything `stand_up` composes, resolved so the flow itself is testable
/// with substitute binaries and scratch directories.
#[derive(Debug)]
pub struct UpEnvironment {
    pub server_binary: PathBuf,
    pub worker_binary: PathBuf,
    pub paths: UpPaths,
    pub bind: String,
    pub enrollment_bind: String,
    pub adapters: Vec<String>,
    pub worker_name: String,
    pub subject_names: Vec<String>,
    /// Whether the client identity sits somewhere a bare `horde` would not
    /// look — then the ready line is followed by the `export` that fixes it.
    pub export_identity: bool,
}

/// The `horde up` verb: install the stop signals, resolve the environment,
/// stand the machine up, hold it until stopped, tear down what was started.
///
/// `--json` deliberately does not change this verb: its announcements are
/// operator lines, and re-encoding them would be a second vocabulary.
///
/// # Errors
///
/// Any startup refusal, in operator words.
pub async fn run(args: &UpArgs, identity: Option<&Path>) -> Result<(), ClientError> {
    // First of all: a ctrl-c that lands during bring-up must reach the
    // teardown, not the default disposition that would orphan the daemons.
    let stop = stop_signals()?;
    let home = std::env::var_os("HOME").map(PathBuf::from);
    let root = resolve_root(args.state_dir.clone(), home.clone())?;
    let paths = paths_under(&root, identity);
    let export_identity = paths.client_identity != crate::cli::default_identity_dir(home);
    let own = std::env::current_exe()
        .map_err(|error| ClientError::Up(format!("cannot locate its own binary: {error}")))?;
    let environment = UpEnvironment {
        server_binary: sibling_binary(&own, "horde-server")?,
        worker_binary: sibling_binary(&own, "horde-worker")?,
        paths,
        bind: args.bind.clone(),
        enrollment_bind: args.enrollment_bind.clone(),
        adapters: args.adapters.clone(),
        worker_name: args
            .name
            .clone()
            .unwrap_or_else(|| worker_name_from(hostname())),
        subject_names: args.subject_names.clone(),
        export_identity,
    };
    let forward: Forward = Arc::new(|line| println!("{line}"));
    stand_up(&environment, &mut |line| println!("{line}"), forward, stop).await
}

/// The stop signals, registered now (registration happens at creation for
/// unix signal streams), resolving on SIGINT or SIGTERM.
fn stop_signals() -> Result<impl Future<Output = ()>, ClientError> {
    use tokio::signal::unix::{SignalKind, signal};
    let install = |kind| {
        signal(kind).map_err(|error| ClientError::Up(format!("cannot listen for signals: {error}")))
    };
    let mut interrupt = install(SignalKind::interrupt())?;
    let mut terminate = install(SignalKind::terminate())?;
    Ok(async move {
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    })
}

/// Stands the machine up, announces the ready line, holds until `shutdown`
/// or a started daemon's death, and tears down only what it started.
///
/// # Errors
///
/// Any refusal along the way, or a started daemon dying; whatever was
/// started is torn down before the error is returned.
pub async fn stand_up(
    environment: &UpEnvironment,
    announce: &mut dyn FnMut(String),
    forward: Forward,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    let _held = horde_identity::StateLock::acquire(&environment.paths.root, UP_LOCK)
        .map_err(|error| ClientError::Up(format!("another `horde up` is running: {error}")))?;
    let mut shutdown = std::pin::pin!(shutdown);
    let mut owned: Vec<WatchedChild> = Vec::new();
    let brought = tokio::select! {
        outcome = bring_up(environment, &mut owned, &forward, announce) => Some(outcome),
        () = &mut shutdown => None,
    };
    let outcome = match brought {
        Some(Ok(())) => hold(&mut owned, shutdown).await,
        Some(Err(error)) => Err(error),
        None => {
            announce("stopped during bring-up".to_owned());
            Ok(())
        }
    };
    tear_down(owned, announce).await;
    outcome
}

/// The bring-up half: server, client, readiness, worker, ready line.
async fn bring_up(
    environment: &UpEnvironment,
    owned: &mut Vec<WatchedChild>,
    forward: &Forward,
    announce: &mut dyn FnMut(String),
) -> Result<(), ClientError> {
    check_advertising(environment)?;
    let server = ensure_server(environment, owned, forward, announce).await?;
    ensure_client(environment, &server, announce).await?;
    await_hello(environment, &server, owned).await?;
    let worker = ensure_worker(environment, &server, owned, forward, announce).await?;
    announce_ready(environment, &server, &worker, announce);
    Ok(())
}

/// Holds the machine: forwarding on, until the stop signal or the first
/// started daemon to die.
async fn hold(
    owned: &mut [WatchedChild],
    shutdown: std::pin::Pin<&mut impl Future<Output = ()>>,
) -> Result<(), ClientError> {
    for child in owned.iter() {
        child.forwarding.store(true, Ordering::SeqCst);
    }
    tokio::select! {
        () = shutdown => Ok(()),
        (name, status) = first_exit(owned) => Err(ClientError::Up(format!(
            "{name} exited ({status}); tearing down what this invocation started"
        ))),
    }
}

/// The first owned child to exit, polled. With nothing owned it never
/// resolves — the stop signal is then the only way out, as it should be.
async fn first_exit(owned: &mut [WatchedChild]) -> (String, std::process::ExitStatus) {
    loop {
        for child in owned.iter_mut() {
            if let Ok(Some(status)) = child.child.try_wait() {
                return (child.name.clone(), status);
            }
        }
        tokio::time::sleep(POLL).await;
    }
}

/// `--san` promises other machines a way in; refused when the binds would
/// break that promise, or when a name is no name another machine can dial.
fn check_advertising(environment: &UpEnvironment) -> Result<(), ClientError> {
    if environment.subject_names.is_empty() {
        return Ok(());
    }
    for name in &environment.subject_names {
        if horde_identity::token::host_scope(name) != horde_identity::HostScope::Routable {
            return Err(ClientError::Up(format!(
                "--san {name} is not a name another machine can dial"
            )));
        }
    }
    for (flag, bind) in [
        ("--bind", &environment.bind),
        ("--enrollment-bind", &environment.enrollment_bind),
    ] {
        if socket_of(flag, bind)?.ip().is_loopback() {
            return Err(ClientError::Up(format!(
                "--san names how other machines dial this server, but {flag} {bind} accepts \
                 only this machine; bind a routable address too (e.g. --bind 0.0.0.0:7777 \
                 --enrollment-bind 0.0.0.0:7778)"
            )));
        }
    }
    Ok(())
}

/// A bind flag's value as an address.
fn socket_of(flag: &str, value: &str) -> Result<std::net::SocketAddr, ClientError> {
    value
        .parse()
        .map_err(|_| ClientError::Up(format!("{flag} {value} is not an address:port")))
}

/// How this machine dials a server listening at `ip:port`: loopback for a
/// loopback or unspecified bind (both reach it, and the listener
/// certificate always covers loopback), the address itself otherwise.
#[must_use]
pub fn local_endpoint(ip: std::net::IpAddr, port: u16) -> String {
    match ip {
        ip if ip.is_unspecified() => format!("https://127.0.0.1:{port}"),
        std::net::IpAddr::V6(v6) => format!("https://[{v6}]:{port}"),
        std::net::IpAddr::V4(v4) => format!("https://{v4}:{port}"),
    }
}

/// What `up` knows about the server it will talk to.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ServerFacts {
    /// How this machine dials it.
    endpoint: String,
    port: u16,
    /// The fingerprint it announced, when this invocation started it.
    fingerprint: Option<String>,
    /// Whether this invocation started it (and so may read its bootstrap
    /// token file).
    started: bool,
}

/// A server to talk to: the one holding the server state's lock, or a
/// freshly started one.
async fn ensure_server(
    environment: &UpEnvironment,
    owned: &mut Vec<WatchedChild>,
    forward: &Forward,
    announce: &mut dyn FnMut(String),
) -> Result<ServerFacts, ClientError> {
    let state = &environment.paths.server_state;
    if horde_identity::StateLock::is_held(state, SERVER_LOCK) {
        // Found running: not ours to stop on the way out.
        let bind = socket_of("--bind", &environment.bind)?;
        let endpoint = local_endpoint(bind.ip(), bind.port());
        announce(format!(
            "server already-running state={} endpoint={endpoint}",
            state.display()
        ));
        return Ok(ServerFacts {
            endpoint,
            port: bind.port(),
            fingerprint: None,
            started: false,
        });
    }

    let db = environment.paths.server_db.display().to_string();
    let state_arg = state.display().to_string();
    let mut args: Vec<String> = [
        db.as_str(),
        "--serve",
        "--bind",
        &environment.bind,
        "--enrollment-bind",
        &environment.enrollment_bind,
        "--state-dir",
        &state_arg,
    ]
    .map(str::to_owned)
    .to_vec();
    for name in &environment.subject_names {
        args.extend(["--san".to_owned(), name.clone()]);
    }
    // Owned before it is awaited on, so a bring-up failure past this point
    // still tears it down deliberately rather than by drop.
    owned.push(WatchedChild::spawn(
        "horde-server",
        &environment.server_binary,
        &args,
        &[],
        Arc::clone(forward),
    )?);
    let stdout = await_on_last(owned, "the server's listening lines", announced_server).await?;
    started_server_facts(&stdout, announce)
}

/// The facts a started server announced. The announced address, not the
/// flag: a `:0` bind is only real after the OS chose it.
fn started_server_facts(
    stdout: &str,
    announce: &mut dyn FnMut(String),
) -> Result<ServerFacts, ClientError> {
    let main = line_value(stdout, "listening main=")
        .and_then(|value| value.parse::<std::net::SocketAddr>().ok())
        .ok_or_else(|| ClientError::Up("the server announced no usable main address".to_owned()))?;
    let endpoint = local_endpoint(main.ip(), main.port());
    announce(format!("server started endpoint={endpoint}"));
    Ok(ServerFacts {
        endpoint,
        port: main.port(),
        fingerprint: line_value(stdout, "server-fingerprint ").map(str::to_owned),
        started: true,
    })
}

/// A local client identity: the one already in the id dir (checked against
/// the server's CA), or one enrolled through the bootstrap-token file of a
/// server this invocation started — same user, same machine, no code on
/// any screen (ADR-0007).
async fn ensure_client(
    environment: &UpEnvironment,
    server: &ServerFacts,
    announce: &mut dyn FnMut(String),
) -> Result<(), ClientError> {
    let id_dir = &environment.paths.client_identity;
    if horde_identity::StoredIdentity::exists(id_dir) {
        check_same_ca(id_dir, &environment.paths.server_state)?;
        announce(format!(
            "client already-enrolled identity={}",
            id_dir.display()
        ));
        return Ok(());
    }
    if !server.started {
        return Err(ClientError::Up(format!(
            "a server is already running from {} and {} holds no client identity; `up` reads \
             the bootstrap token only from a server it started itself — join with `horde \
             enroll join <token>` (minted by an enrolled client), or pass --identity <dir>",
            environment.paths.server_state.display(),
            id_dir.display()
        )));
    }

    let token = bootstrap_token(environment, server)?;
    let enrolled = crate::cli::redeem_client_token(&token, "horde up")
        .await
        .map_err(|error| {
            ClientError::Up(format!(
                "{error} (the bootstrap token is minted at server start and expires; stop \
                 and re-run `horde up` to start a fresh server and re-mint it)"
            ))
        })?;
    let persist = |error: horde_identity::IdentityError| ClientError::Up(error.to_string());
    enrolled.identity.save(id_dir).map_err(persist)?;
    // Stored (validated first) so every later bare `horde` invocation
    // finds its server — the persistence `enroll join` performs.
    horde_identity::store::save_endpoint(id_dir, &server.endpoint).map_err(persist)?;
    announce(format!(
        "client enrolled principal={} identity={}",
        enrolled.principal.common_name(),
        id_dir.display()
    ));
    Ok(())
}

/// Refuses an identity whose CA is not the CA of the server in `up`'s own
/// state dir — the two would only ever fail a handshake. A server state
/// with no readable CA is not judged here; the readiness `Hello` is.
fn check_same_ca(id_dir: &Path, server_state: &Path) -> Result<(), ClientError> {
    let Ok(server_ca) =
        horde_identity::CertificateAuthority::persisted_certificate_pem(&server_state.join(CA_DIR))
    else {
        return Ok(());
    };
    let identity =
        horde_identity::StoredIdentity::load(id_dir).map_err(|source| ClientError::Identity {
            directory: id_dir.to_path_buf(),
            source,
        })?;
    match (
        horde_identity::fingerprint_of_pem(&identity.ca_pem),
        horde_identity::fingerprint_of_pem(&server_ca),
    ) {
        (Ok(held), Ok(served)) if held == served => Ok(()),
        _ => Err(ClientError::Up(format!(
            "the identity in {} trusts a different CA than the server in {}: it was joined to \
             another server (or this server's state was recreated); move it aside, or point \
             --identity at this server's identity",
            id_dir.display(),
            server_state.display()
        ))),
    }
}

/// The bootstrap token a just-started server filed, refused when it is
/// missing or was written by an earlier server (its fingerprint is not the
/// one this server announced).
fn bootstrap_token(
    environment: &UpEnvironment,
    server: &ServerFacts,
) -> Result<horde_identity::JoinToken, ClientError> {
    let token_file = environment.paths.server_state.join(BOOTSTRAP_TOKEN_FILE);
    let raw = std::fs::read_to_string(&token_file).map_err(|error| {
        ClientError::Up(format!(
            "no client identity in {} and no bootstrap token at {} ({error}); the server has \
             an enrolled client elsewhere — join with `horde enroll join <token>` instead",
            environment.paths.client_identity.display(),
            token_file.display()
        ))
    })?;
    let token = horde_identity::JoinToken::parse(raw.trim())
        .map_err(|error| ClientError::Up(error.to_string()))?;
    match &server.fingerprint {
        Some(announced)
            if !horde_identity::fingerprint::fingerprints_match(
                announced,
                &token.code.server_fingerprint,
            ) =>
        {
            Err(ClientError::Up(format!(
                "the bootstrap token at {} names another server's fingerprint (an earlier \
                 run left it); stop and re-run `horde up`",
                token_file.display()
            )))
        }
        _ => Ok(token),
    }
}

/// Readiness, proven by a real `Hello` over mTLS as the local client — a
/// TCP connect would pass for any listener, a CA mismatch included. A
/// server still coming up is retried under [`HELLO_CEILING`]; a
/// certificate failure fails at once, diagnosed; a started daemon dying
/// meanwhile fails with what it said.
async fn await_hello(
    environment: &UpEnvironment,
    server: &ServerFacts,
    owned: &mut [WatchedChild],
) -> Result<(), ClientError> {
    let id_dir = &environment.paths.client_identity;
    let identity =
        horde_identity::StoredIdentity::load(id_dir).map_err(|source| ClientError::Identity {
            directory: id_dir.clone(),
            source,
        })?;
    let deadline = tokio::time::Instant::now() + HELLO_CEILING;
    loop {
        let error = match hello_once(&server.endpoint, &identity).await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let transient =
            horde_identity::diagnosis::cause(&error).is_none_or(|cause| cause.is_transient());
        if !transient || tokio::time::Instant::now() >= deadline {
            return Err(ClientError::Up(format!(
                "the server at {} did not answer a Hello: {}",
                server.endpoint,
                error.report()
            )));
        }
        if let Some(died) = owned.iter_mut().find_map(WatchedChild::exit_report) {
            return Err(died);
        }
        tokio::time::sleep(POLL).await;
    }
}

/// One bounded `Hello` as `identity`.
async fn hello_once(
    endpoint: &str,
    identity: &horde_identity::StoredIdentity,
) -> Result<(), ClientError> {
    let attempt = async {
        let mut client = crate::Client::connect_with_identity(endpoint, identity).await?;
        client.hello(env!("CARGO_PKG_VERSION")).await.map(|_| ())
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), attempt)
        .await
        .unwrap_or_else(|_| {
            Err(ClientError::Up(format!(
                "the Hello to {endpoint} timed out"
            )))
        })
}

/// A local worker: one already holding the worker state's lock, or one
/// started over its existing identity, or mint-and-joined through the
/// enrolled client. Returns the name it registered under.
async fn ensure_worker(
    environment: &UpEnvironment,
    server: &ServerFacts,
    owned: &mut Vec<WatchedChild>,
    forward: &Forward,
    announce: &mut dyn FnMut(String),
) -> Result<String, ClientError> {
    let state = &environment.paths.worker_state;
    if horde_identity::StateLock::is_held(state, WORKER_LOCK) {
        announce(format!("worker already-running state={}", state.display()));
        return Ok(environment.worker_name.clone());
    }
    let mut args: Vec<String> = vec![
        "--server".to_owned(),
        server.endpoint.clone(),
        "--state-dir".to_owned(),
        state.display().to_string(),
        "--name".to_owned(),
        environment.worker_name.clone(),
    ];
    for adapter in &environment.adapters {
        args.extend(["--adapter".to_owned(), adapter.clone()]);
    }
    let mut envs: Vec<(String, String)> = Vec::new();
    if !horde_identity::StoredIdentity::exists(&state.join("identity")) {
        let token = mint_worker_token(environment, &server.endpoint).await?;
        envs.push((WORKER_JOIN_TOKEN_VAR.to_owned(), token));
    }
    owned.push(WatchedChild::spawn(
        "horde-worker",
        &environment.worker_binary,
        &args,
        &envs,
        Arc::clone(forward),
    )?);
    let stdout = await_on_last(owned, "the worker's registration line", |out| {
        registered_name(out).is_some()
    })
    .await?;
    let name = registered_name(&stdout)
        .unwrap_or(&environment.worker_name)
        .to_owned();
    announce(format!("worker registered name={name}"));
    Ok(name)
}

/// The name on the first complete `registered worker=<name> …` line — any
/// name, because an existing identity's principal outranks `--name`.
fn registered_name(out: &str) -> Option<&str> {
    line_value(out, "registered worker=")?
        .split_whitespace()
        .next()
}

/// The ready line, the advertised endpoint for other machines when `--san`
/// named one, and the `export` that makes a bare `horde` find this identity.
fn announce_ready(
    environment: &UpEnvironment,
    server: &ServerFacts,
    worker: &str,
    announce: &mut dyn FnMut(String),
) {
    announce(format!(
        "ready worker={worker} adapters={} endpoint={}",
        if environment.adapters.is_empty() {
            "none".to_owned()
        } else {
            environment.adapters.join(",")
        },
        server.endpoint
    ));
    if let Some(name) = environment.subject_names.first() {
        let host = match name.parse::<std::net::Ipv6Addr>() {
            Ok(v6) => format!("[{v6}]"),
            Err(_) => name.clone(),
        };
        let advertised = format!("https://{host}:{}", server.port);
        announce(format!(
            "advertise endpoint={advertised} (another machine joins with a token minted \
             through it: `horde worker enroll <name> --endpoint {advertised}`)"
        ));
    }
    if environment.export_identity {
        announce(format!(
            "export HORDE_IDENTITY={}",
            environment.paths.client_identity.display()
        ));
    }
}

/// Waits on the most recently owned child's stdout — the push-then-wait
/// shape both spawns share, so a failed wait still leaves the child owned
/// for teardown.
async fn await_on_last(
    owned: &mut [WatchedChild],
    what: &str,
    holds: impl Fn(&str) -> bool,
) -> Result<String, ClientError> {
    match owned.last_mut() {
        Some(child) => child.await_stdout(what, holds).await,
        None => Err(ClientError::Up(format!(
            "nothing was spawned to wait for {what}"
        ))),
    }
}

/// Mints the local worker's join token through the enrolled client — the
/// same mint-and-compose `horde worker enroll` performs. An answer this
/// cannot compose a token from is a refusal: the server beside this flow
/// always names its listener.
async fn mint_worker_token(
    environment: &UpEnvironment,
    endpoint: &str,
) -> Result<String, ClientError> {
    let identity = horde_identity::StoredIdentity::load(&environment.paths.client_identity)
        .map_err(|source| ClientError::Identity {
            directory: environment.paths.client_identity.clone(),
            source,
        })?;
    let mut client = crate::Client::connect_with_identity(endpoint, &identity).await?;
    let minted = client
        .mint_enrollment_code(
            horde_proto::v1::Role::Worker,
            &environment.worker_name,
            None,
        )
        .await?;
    match crate::cli::token_from_mint(endpoint, &minted) {
        Some(token) => Ok(token.to_string()),
        None => Err(ClientError::Up(
            "the mint answer named no enrollment listener to compose a worker token from"
                .to_owned(),
        )),
    }
}

/// Whether a server's startup stdout has announced everything `up` reads:
/// both listeners and the fingerprint (which proves the bootstrap token
/// file fresh). All, not any: the address is parsed out of the same
/// snapshot, and half an announcement is not yet an address to dial.
fn announced_server(out: &str) -> bool {
    line_value(out, "listening main=").is_some()
        && line_value(out, "listening enrollment=").is_some()
        && line_value(out, "server-fingerprint ").is_some()
}

/// The value after `key` on the first *complete* line of `text` that starts
/// with it. Only newline-terminated lines count, because the buffer fills
/// in raw chunks and a value read off a half-arrived line would be a
/// truncated address that parses.
fn line_value<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let complete = match text.rfind('\n') {
        Some(newline) => &text[..newline],
        None => "",
    };
    complete
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .map(str::trim)
}

/// One daemon line as forwarded: tagged with its daemon, clipped to
/// [`LINE_CAP`] bytes on a character boundary.
fn forwarded_line(name: &str, line: &str) -> String {
    if line.len() <= LINE_CAP {
        return format!("[{name}] {line}");
    }
    let mut end = LINE_CAP;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    format!("[{name}] {}…", &line[..end])
}

/// Appends to the kept stdout without passing [`STDOUT_CAP`] bytes.
fn push_capped(buffer: &mut String, text: &str) {
    let room = STDOUT_CAP.saturating_sub(buffer.len());
    let mut end = text.len().min(room);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    buffer.push_str(&text[..end]);
}

/// Ends every owned child, newest first (the worker before the server it
/// dials): SIGTERM, a bounded wait, and only then a kill.
async fn tear_down(owned: Vec<WatchedChild>, announce: &mut dyn FnMut(String)) {
    for child in owned.into_iter().rev() {
        let name = child.name.clone();
        child.stop().await;
        announce(format!("stopped {name}"));
    }
}

/// A child process this invocation owns, with its stdout watched.
///
/// Kill-on-drop as the last resort; the deliberate path is [`Self::stop`].
struct WatchedChild {
    name: String,
    child: tokio::process::Child,
    stdout: Arc<Mutex<String>>,
    /// Off during bring-up (the server's first lines carry the bootstrap
    /// token, which must not reach a screen), on once the machine is up.
    forwarding: Arc<AtomicBool>,
}

impl WatchedChild {
    fn spawn(
        name: &str,
        binary: &Path,
        args: &[String],
        envs: &[(String, String)],
        forward: Forward,
    ) -> Result<Self, ClientError> {
        let spawn_failure = |error: &dyn std::fmt::Display| {
            ClientError::Up(format!(
                "could not start {name} ({}): {error}",
                binary.display()
            ))
        };
        let mut child = tokio::process::Command::new(binary)
            .args(args)
            .envs(envs.iter().map(|(key, value)| (key, value)))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| spawn_failure(&error))?;
        let out = child
            .stdout
            .take()
            .ok_or_else(|| spawn_failure(&"no stdout pipe"))?;
        let stdout = Arc::new(Mutex::new(String::new()));
        let forwarding = Arc::new(AtomicBool::new(false));
        tokio::spawn(pump(
            out,
            name.to_owned(),
            Arc::clone(&stdout),
            Arc::clone(&forwarding),
            forward,
        ));
        Ok(Self {
            name: name.to_owned(),
            child,
            stdout,
            forwarding,
        })
    }

    fn stdout_so_far(&self) -> String {
        self.stdout.lock().map(|b| b.clone()).unwrap_or_default()
    }

    /// The refusal for a child that has exited, carrying what it printed —
    /// `None` while it runs.
    fn exit_report(&mut self) -> Option<ClientError> {
        let status = self.child.try_wait().ok().flatten()?;
        Some(ClientError::Up(format!(
            "{} exited ({status}) during bring-up; its output:\n{}",
            self.name,
            self.stdout_so_far()
        )))
    }

    /// Waits under [`STARTUP_CEILING`] until stdout satisfies `holds`,
    /// failing at once (with the output) if the child exits first.
    async fn await_stdout(
        &mut self,
        what: &str,
        holds: impl Fn(&str) -> bool,
    ) -> Result<String, ClientError> {
        let deadline = tokio::time::Instant::now() + STARTUP_CEILING;
        loop {
            let snapshot = self.stdout_so_far();
            if holds(&snapshot) {
                return Ok(snapshot);
            }
            if let Some(died) = self.exit_report() {
                return Err(died);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Up(format!(
                    "{} never printed {what}; its output so far:\n{snapshot}",
                    self.name
                )));
            }
            tokio::time::sleep(POLL).await;
        }
    }

    /// SIGTERM, a bounded wait, then SIGKILL — a clean shutdown offered,
    /// never depended on.
    async fn stop(mut self) {
        if let Some(pid) = self.child.id() {
            let _best_effort_term = tokio::process::Command::new("kill")
                .args(["-TERM", &pid.to_string()])
                .status()
                .await;
        }
        if tokio::time::timeout(TEARDOWN_CEILING, self.child.wait())
            .await
            .is_err()
        {
            let _kill_a_stuck_child = self.child.kill().await;
        }
    }
}

/// Reads a child's stdout to its end: kept (capped) for the waits, and
/// forwarded line by line once forwarding is on.
async fn pump(
    mut out: tokio::process::ChildStdout,
    name: String,
    buffer: Arc<Mutex<String>>,
    forwarding: Arc<AtomicBool>,
    forward: Forward,
) {
    use tokio::io::AsyncReadExt;
    let mut chunk = [0_u8; 4096];
    let mut pending = String::new();
    while let Ok(read) = out.read(&mut chunk).await {
        if read == 0 {
            break;
        }
        let text = String::from_utf8_lossy(&chunk[..read]);
        if let Ok(mut buffer) = buffer.lock() {
            push_capped(&mut buffer, &text);
        }
        if !forwarding.load(Ordering::SeqCst) {
            continue;
        }
        pending.push_str(&text);
        while let Some(newline) = pending.find('\n') {
            let line: String = pending.drain(..=newline).collect();
            forward(forwarded_line(&name, line.trim_end()));
        }
        if pending.len() > LINE_CAP {
            forward(forwarded_line(&name, &pending));
            pending.clear();
        }
    }
}

#[cfg(test)]
mod tests;
