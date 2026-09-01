//! `horde up`: stand up this machine (ADR-0007).
//!
//! One command covers the single-machine day-one case by composing the
//! pieces that already exist — `horde-server --serve`, the token join, the
//! worker daemon — rather than growing a parallel path: start the server if
//! none is running, enroll the local client if none exists, mint-and-join a
//! local worker, print one ready line. Same-machine trust rides the
//! filesystem: the server's user-read-only bootstrap-token file stands in
//! for the token ceremony, which surfaces only when a second machine joins.
//!
//! Ctrl-C tears down exactly what this invocation started: a server it
//! found running is left running on exit.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tokio::io::AsyncBufReadExt;

use crate::ClientError;

/// The bootstrap-token file's name inside the server's state dir — the
/// contract `horde-server`'s startup writes (its `bootstrap_token_path`)
/// and this command reads. A name rather than a shared symbol because the
/// CLI deliberately has no dependency on the server crate.
const BOOTSTRAP_TOKEN_FILE: &str = "bootstrap-token";

/// How long any single wait against a spawned daemon may take. Generous
/// against a loaded machine; a healthy start never spends it.
const STARTUP_CEILING: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a torn-down child may take to exit after SIGTERM before it is
/// killed outright.
const TEARDOWN_CEILING: std::time::Duration = std::time::Duration::from_secs(10);

/// What `horde up` takes: everything else is composition of defaults.
#[derive(clap::Args, Debug)]
pub struct UpArgs {
    /// Root for everything `up` owns: `server/`, `id/`, `worker/`.
    /// Defaults to `~/.horde`.
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// A harness for the local worker (`fake`, `claude`, `codex`);
    /// repeatable, in order. The worker daemon owns the vocabulary and
    /// refuses anything it does not know.
    #[arg(long = "adapter")]
    pub adapters: Vec<String>,

    /// Where the server listens (and where a running one is looked for).
    #[arg(long, default_value = "127.0.0.1:7777")]
    pub bind: String,

    /// Where the server's enrollment listener binds when `up` starts one.
    #[arg(long, default_value = "127.0.0.1:7778")]
    pub enrollment_bind: String,

    /// The local worker's name. Defaults to this machine's hostname.
    #[arg(long)]
    pub name: Option<String>,
}

/// Where everything `up` owns lives, under one root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpPaths {
    pub server_state: PathBuf,
    pub server_db: PathBuf,
    pub client_identity: PathBuf,
    pub worker_state: PathBuf,
}

/// The layout under a root: `server/` (state and db), `id/` (the same
/// default the rest of the CLI resolves), `worker/`.
#[must_use]
pub fn paths_under(root: &Path) -> UpPaths {
    UpPaths {
        server_state: root.join("server"),
        server_db: root.join("server").join("horde.db"),
        client_identity: root.join("id"),
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
        .ok_or_else(|| {
            ClientError::Enrollment("horde up needs --state-dir when HOME is unset".to_owned())
        })
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
    Err(ClientError::Enrollment(format!(
        "horde up runs {name} but found none at {}; install it beside the horde binary",
        candidate.display()
    )))
}

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
}

/// The `horde up` verb: resolve the environment, stand the machine up, hold
/// it until ctrl-c, tear down what was started.
///
/// `--json` deliberately does not change this verb: its announcements are
/// the operator lines of the daemons it composes, and re-encoding lines
/// already defined elsewhere would be a second vocabulary.
///
/// # Errors
///
/// Any startup refusal, in operator words.
pub async fn run(args: &UpArgs) -> Result<(), ClientError> {
    let root = resolve_root(
        args.state_dir.clone(),
        std::env::var_os("HOME").map(Into::into),
    )?;
    let own = std::env::current_exe().map_err(|error| {
        ClientError::Enrollment(format!("horde up cannot locate its own binary: {error}"))
    })?;
    let environment = UpEnvironment {
        server_binary: sibling_binary(&own, "horde-server")?,
        worker_binary: sibling_binary(&own, "horde-worker")?,
        paths: paths_under(&root),
        bind: args.bind.clone(),
        enrollment_bind: args.enrollment_bind.clone(),
        adapters: args.adapters.clone(),
        worker_name: args
            .name
            .clone()
            .unwrap_or_else(|| worker_name_from(hostname())),
    };
    stand_up(
        &environment,
        &mut |line| println!("{line}"),
        until_stopped(),
    )
    .await
}

/// Resolves when the operator says stop: ctrl-c, or the SIGTERM a
/// supervisor sends. Both matter here specifically — a signal death would
/// skip teardown, and `up`'s whole promise is tearing down what it started.
async fn until_stopped() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
        Ok(mut sigterm) => {
            tokio::select! {
                () = ctrl_c => {}
                _ = sigterm.recv() => {}
            }
        }
        Err(_) => ctrl_c.await,
    }
}

/// Stands the machine up, announces one ready line, parks on `shutdown`,
/// and tears down only what it started (ADR-0007).
///
/// # Errors
///
/// Any refusal along the way; whatever was already started is torn down
/// before the error is returned.
pub async fn stand_up(
    environment: &UpEnvironment,
    announce: &mut dyn FnMut(String),
    shutdown: impl Future<Output = ()>,
) -> Result<(), ClientError> {
    let mut owned: Vec<WatchedChild> = Vec::new();
    let outcome = bring_up(environment, &mut owned, announce).await;
    match outcome {
        Ok(()) => {
            shutdown.await;
            tear_down(owned, announce).await;
            Ok(())
        }
        Err(error) => {
            tear_down(owned, announce).await;
            Err(error)
        }
    }
}

/// The bring-up half: server, client, worker, ready line. Split from
/// [`stand_up`] so teardown runs on every exit path without repeating it.
async fn bring_up(
    environment: &UpEnvironment,
    owned: &mut Vec<WatchedChild>,
    announce: &mut dyn FnMut(String),
) -> Result<(), ClientError> {
    let main_addr = ensure_server(environment, owned, announce).await?;
    let endpoint = format!("https://{main_addr}");

    ensure_client(environment, &endpoint, announce).await?;
    ensure_worker(environment, &endpoint, owned, announce).await?;

    announce(format!(
        "ready worker={} adapters={} endpoint={endpoint}",
        environment.worker_name,
        if environment.adapters.is_empty() {
            "none".to_owned()
        } else {
            environment.adapters.join(",")
        },
    ));
    Ok(())
}

/// A server to talk to: the one already answering on the bind address, or a
/// freshly started one. Returns the main address.
async fn ensure_server(
    environment: &UpEnvironment,
    owned: &mut Vec<WatchedChild>,
    announce: &mut dyn FnMut(String),
) -> Result<String, ClientError> {
    if port_answers(&environment.bind).await {
        // Found running: not ours to stop on the way out.
        announce(format!("server already-running main={}", environment.bind));
        return Ok(environment.bind.clone());
    }

    // The database lives inside the state dir, and SQLite will not invent
    // the directory around its own file.
    std::fs::create_dir_all(&environment.paths.server_state).map_err(|error| {
        ClientError::Enrollment(format!(
            "could not create {}: {error}",
            environment.paths.server_state.display()
        ))
    })?;
    let db = environment.paths.server_db.display().to_string();
    let state = environment.paths.server_state.display().to_string();
    // Owned before it is awaited on, so a bring-up failure past this point
    // still tears it down deliberately rather than by drop.
    owned.push(WatchedChild::spawn(
        "horde-server",
        &environment.server_binary,
        &[
            &db,
            "--serve",
            "--bind",
            &environment.bind,
            "--enrollment-bind",
            &environment.enrollment_bind,
            "--state-dir",
            &state,
        ],
    )?);
    let stdout = await_on_last(owned, "the server's listening lines", |out| {
        line_value(out, "listening main=").is_some()
            && line_value(out, "listening enrollment=").is_some()
    })
    .await?;

    // The announced addresses, not the flags: a `:0` bind is only real
    // after the OS chose it.
    let main = line_value(&stdout, "listening main=")
        .ok_or_else(|| ClientError::Enrollment("the server announced no main address".to_owned()))?
        .to_owned();
    announce(format!("server started main={main}"));
    Ok(main)
}

/// A local client identity: the one already in the id dir, or one enrolled
/// through the server's bootstrap-token file — same user, same machine, no
/// code on any screen (ADR-0007).
async fn ensure_client(
    environment: &UpEnvironment,
    endpoint: &str,
    announce: &mut dyn FnMut(String),
) -> Result<(), ClientError> {
    let id_dir = &environment.paths.client_identity;
    if horde_identity::StoredIdentity::exists(id_dir) {
        announce(format!(
            "client already-enrolled identity={}",
            id_dir.display()
        ));
        return Ok(());
    }

    let token_file = environment.paths.server_state.join(BOOTSTRAP_TOKEN_FILE);
    let raw = std::fs::read_to_string(&token_file).map_err(|error| {
        ClientError::Enrollment(format!(
            "no client identity in {} and no bootstrap token at {} ({error}); \
             the server has an enrolled client elsewhere — join with \
             `horde enroll join <token>` instead",
            id_dir.display(),
            token_file.display()
        ))
    })?;
    let token = horde_identity::JoinToken::parse(raw.trim())
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;

    let enrolled = horde_identity::join(
        &token.enrollment_endpoint(),
        &token.code,
        horde_proto::v1::Hello {
            semver: env!("CARGO_PKG_VERSION").to_owned(),
            capability_flags: Vec::new(),
            role: horde_proto::v1::Role::Unspecified as i32,
            display_name: "horde up".to_owned(),
        },
    )
    .await
    .map_err(|error| ClientError::Enrollment(error.to_string()))?;
    enrolled
        .identity
        .save(id_dir)
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;
    // Stored so every later bare `horde` invocation finds its server — the
    // same persistence `enroll join` performs, against the endpoint this
    // flow already dialed.
    horde_identity::store::save_endpoint(id_dir, endpoint)
        .map_err(|error| ClientError::Enrollment(error.to_string()))?;
    announce(format!(
        "client enrolled principal={} identity={}",
        enrolled.principal.common_name(),
        id_dir.display()
    ));
    Ok(())
}

/// A local worker: started over its existing identity, or mint-and-joined
/// through the enrolled client — the same mint `horde worker enroll` runs.
async fn ensure_worker(
    environment: &UpEnvironment,
    endpoint: &str,
    owned: &mut Vec<WatchedChild>,
    announce: &mut dyn FnMut(String),
) -> Result<(), ClientError> {
    let state = environment.paths.worker_state.display().to_string();
    let mut args: Vec<String> = vec![
        "--server".to_owned(),
        endpoint.to_owned(),
        "--state-dir".to_owned(),
        state,
        "--name".to_owned(),
        environment.worker_name.clone(),
    ];
    for adapter in &environment.adapters {
        args.extend(["--adapter".to_owned(), adapter.clone()]);
    }

    let identity_dir = environment.paths.worker_state.join("identity");
    if !horde_identity::StoredIdentity::exists(&identity_dir) {
        let token = mint_worker_token(environment, endpoint).await?;
        args.extend(["--join".to_owned(), token]);
    }

    let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
    owned.push(WatchedChild::spawn(
        "horde-worker",
        &environment.worker_binary,
        &borrowed,
    )?);
    let registered = format!("registered worker={} ", environment.worker_name);
    await_on_last(owned, "the worker's registration line", |out| {
        line_value(out, &registered).is_some()
    })
    .await?;
    announce(format!(
        "worker registered name={}",
        environment.worker_name
    ));
    Ok(())
}

/// Waits on the most recently owned child's stdout — the push-then-wait
/// shape both spawns share, so a failed wait still leaves the child owned
/// for teardown.
async fn await_on_last(
    owned: &[WatchedChild],
    what: &str,
    holds: impl Fn(&str) -> bool,
) -> Result<String, ClientError> {
    match owned.last() {
        Some(child) => child.await_stdout(what, holds).await,
        None => Err(ClientError::Enrollment(format!(
            "nothing was spawned to wait for {what}"
        ))),
    }
}

/// Mints the local worker's join token through the enrolled client — the
/// same mint-and-compose `horde worker enroll` performs. An answer this
/// cannot compose a token from (no port, an unparseable code) is a refusal:
/// the server beside this flow always names its listener, so there is
/// nothing honest to fall back to.
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
        None => Err(ClientError::Enrollment(
            "the mint answer named no enrollment listener to compose a worker token from"
                .to_owned(),
        )),
    }
}

/// Whether something is listening at `addr` — ADR-0007's "server already
/// running" probe. A connect refused or timed out both mean "none".
async fn port_answers(addr: &str) -> bool {
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            tokio::net::TcpStream::connect(addr),
        )
        .await,
        Ok(Ok(_))
    )
}

/// The value after `key` on the first line of `text` that starts with it.
fn line_value<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(key))
        .map(str::trim)
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
#[derive(Debug)]
struct WatchedChild {
    name: String,
    child: tokio::process::Child,
    stdout: Arc<Mutex<String>>,
}

impl WatchedChild {
    fn spawn(name: &str, binary: &Path, args: &[&str]) -> Result<Self, ClientError> {
        let spawn_failure = |error: &dyn std::fmt::Display| {
            ClientError::Enrollment(format!(
                "could not start {name} ({}): {error}",
                binary.display()
            ))
        };
        let mut child = tokio::process::Command::new(binary)
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| spawn_failure(&error))?;
        let stdout = Arc::new(Mutex::new(String::new()));
        let out = child
            .stdout
            .take()
            .ok_or_else(|| spawn_failure(&"no stdout pipe"))?;
        let buffer = Arc::clone(&stdout);
        tokio::spawn(async move {
            let mut lines = tokio::io::BufReader::new(out).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if let Ok(mut buffer) = buffer.lock() {
                    buffer.push_str(&line);
                    buffer.push('\n');
                }
            }
        });
        Ok(Self {
            name: name.to_owned(),
            child,
            stdout,
        })
    }

    fn stdout_so_far(&self) -> String {
        self.stdout.lock().map(|b| b.clone()).unwrap_or_default()
    }

    /// Waits under [`STARTUP_CEILING`] until stdout satisfies `holds`. The
    /// failure carries the output so far — a daemon that never said the
    /// line usually said why.
    async fn await_stdout(
        &self,
        what: &str,
        holds: impl Fn(&str) -> bool,
    ) -> Result<String, ClientError> {
        let deadline = tokio::time::Instant::now() + STARTUP_CEILING;
        loop {
            let snapshot = self.stdout_so_far();
            if holds(&snapshot) {
                return Ok(snapshot);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(ClientError::Enrollment(format!(
                    "{} never printed {what}; its output so far:\n{snapshot}",
                    self.name
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
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

#[cfg(test)]
mod tests {
    use super::{UpEnvironment, UpPaths, line_value, paths_under, port_answers, worker_name_from};

    #[test]
    fn the_layout_under_a_root_is_server_id_worker() {
        let root = std::path::Path::new("/home/me/.horde");
        assert_eq!(
            paths_under(root),
            UpPaths {
                server_state: root.join("server"),
                server_db: root.join("server/horde.db"),
                client_identity: root.join("id"),
                worker_state: root.join("worker"),
            },
            "id/ must be the same default the rest of the CLI resolves"
        );
    }

    #[test]
    fn the_root_is_the_flag_or_home_or_a_refusal() {
        assert_eq!(
            super::resolve_root(Some("/x".into()), Some("/home/me".into())).expect("resolves"),
            std::path::PathBuf::from("/x")
        );
        assert_eq!(
            super::resolve_root(None, Some("/home/me".into())).expect("resolves"),
            std::path::PathBuf::from("/home/me/.horde")
        );
        let refused = super::resolve_root(None, None).expect_err("nowhere honest to default to");
        assert!(refused.to_string().contains("--state-dir"), "{refused}");
    }

    /// The hostname becomes a principal-safe worker name: valid characters
    /// survive, everything else maps to `-`, 64 caps it, and a hostname
    /// that yields nothing usable falls back to a fixed name rather than a
    /// dash-soup principal.
    #[test]
    fn worker_names_are_principal_safe_hostnames_or_the_fallback() {
        assert_eq!(
            worker_name_from(Some("Myless-MacBook-Pro.local".to_owned())),
            "Myless-MacBook-Pro.local"
        );
        assert_eq!(
            worker_name_from(Some("études box".to_owned())),
            "-tudes-box",
            "anything a certificate name refuses maps to a dash"
        );
        assert_eq!(worker_name_from(Some("é é".to_owned())), "this-machine");
        assert_eq!(worker_name_from(Some("a".repeat(80))), "a".repeat(64));
        assert_eq!(worker_name_from(None), "this-machine");
        assert_eq!(worker_name_from(Some(String::new())), "this-machine");
    }

    #[test]
    fn sibling_binaries_are_found_beside_the_own_binary_or_named_missing() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let own = dir.path().join("horde");
        std::fs::write(&own, b"").expect("writes");
        std::fs::write(dir.path().join("horde-server"), b"").expect("writes");

        assert_eq!(
            super::sibling_binary(&own, "horde-server").expect("found"),
            dir.path().join("horde-server")
        );
        let refused = super::sibling_binary(&own, "horde-worker").expect_err("missing");
        assert!(
            refused.to_string().contains("horde-worker")
                && refused.to_string().contains(&*dir.path().to_string_lossy()),
            "the refusal names the binary and where it looked: {refused}"
        );
    }

    #[test]
    fn line_values_read_the_operator_line_convention() {
        let out = "listening main=127.0.0.1:7777\nregistered worker=w resume=3\n";
        assert_eq!(line_value(out, "listening main="), Some("127.0.0.1:7777"));
        assert_eq!(line_value(out, "registered worker=w "), Some("resume=3"));
        assert_eq!(line_value(out, "bootstrap-client-token "), None);
    }

    #[tokio::test]
    async fn the_probe_tells_a_listener_from_silence() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("binds");
        let addr = listener.local_addr().expect("an addr").to_string();
        assert!(port_answers(&addr).await, "a bound port answers");
        drop(listener);
        assert!(
            !port_answers("127.0.0.1:1").await,
            "an unbound port is no server"
        );
    }

    /// The port of an `https://host:PORT` endpoint string.
    fn port_of(endpoint: &str) -> u16 {
        endpoint
            .rsplit(':')
            .next()
            .and_then(|port| port.parse().ok())
            .expect("an endpoint carries a port")
    }

    /// A `/bin/sh` stand-in daemon: runs `prelude`, prints `lines`, marks
    /// its SIGTERM in `<name>.stopped`, and parks. What `stand_up` watches
    /// and tears down is stdout and signals, which is exactly what these
    /// fake; the daemons' own behavior has its own suites and the e2e.
    fn fake_daemon(
        dir: &std::path::Path,
        name: &str,
        prelude: &str,
        lines: &[String],
    ) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        // Scripts live under `bin/`: the root's `server/` and `worker/`
        // names belong to the layout under test.
        let bin = dir.join("bin");
        std::fs::create_dir_all(&bin).expect("the bin dir");
        let mark = bin.join(format!("{name}.stopped"));
        let path = bin.join(name);
        let printed: String = lines
            .iter()
            .map(|line| format!("echo '{line}'\n"))
            .collect();
        let script = format!(
            "#!/bin/sh\ntrap 'echo stopped > {}; exit 0' TERM\n{prelude}\n{printed}while true; do sleep 1; done\n",
            mark.display()
        );
        std::fs::write(&path, script).expect("the script writes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmods");
        path
    }

    fn stopped(dir: &std::path::Path, name: &str) -> bool {
        dir.join("bin").join(format!("{name}.stopped")).is_file()
    }

    /// The whole first boot, in process: a fresh machine, the bootstrap
    /// token riding the filesystem, the client enrolled for real against a
    /// secured server, a worker token minted through the real control plane
    /// — and the token handed to the worker actually redeems, which is what
    /// separates a minted token from a printed guess. Teardown is proven by
    /// the SIGTERM marks both scripted daemons leave.
    #[tokio::test(flavor = "multi_thread")]
    async fn stand_up_boots_enrolls_mints_and_tears_down_what_it_started() {
        let harness = horde_server_testkit::secure::serve_secured()
            .await
            .expect("a secured server");
        let main_port = port_of(&harness.main_endpoint());
        let enrollment_port = port_of(&harness.enrollment_endpoint());

        let root = tempfile::tempdir().expect("a temp dir");
        let paths = paths_under(root.path());
        std::fs::create_dir_all(&paths.server_state).expect("the server state dir");

        // The server-side arrangement: the harness IS the server; the
        // scripted binary only performs its stdout. The bootstrap token
        // file carries a real minted code, exactly as startup writes it.
        let operator =
            horde_identity::Principal::new(horde_identity::PrincipalRole::Client, "operator")
                .expect("a valid name");
        let minted = harness
            .authority
            .mint(
                &operator,
                0,
                horde_server::service::enrollment::wall_clock_ms(),
            )
            .await
            .expect("the bootstrap mint");
        std::fs::write(
            paths.server_state.join("bootstrap-token"),
            format!("{}\n", harness.join_token(&minted)),
        )
        .expect("the token files");

        let args_file = root.path().join("worker-args");
        let server_binary = fake_daemon(
            root.path(),
            "server",
            "",
            &[
                format!("listening main=127.0.0.1:{main_port}"),
                format!("listening enrollment=127.0.0.1:{enrollment_port}"),
            ],
        );
        let worker_binary = fake_daemon(
            root.path(),
            "worker",
            &format!("printf '%s\\n' \"$@\" > {}", args_file.display()),
            &["registered worker=up-test resume=0".to_owned()],
        );

        let environment = UpEnvironment {
            server_binary,
            worker_binary,
            paths: paths.clone(),
            // Nobody listens on port 1, so `up` must start "the server".
            bind: "127.0.0.1:1".to_owned(),
            enrollment_bind: "127.0.0.1:2".to_owned(),
            adapters: vec!["fake".to_owned()],
            worker_name: "up-test".to_owned(),
        };
        let mut lines = Vec::new();
        super::stand_up(
            &environment,
            &mut |line| lines.push(line),
            std::future::ready(()),
        )
        .await
        .expect("the machine stands up");

        // The ready line, and the steps it summarizes.
        let endpoint = format!("https://127.0.0.1:{main_port}");
        assert!(
            lines.contains(&format!(
                "ready worker=up-test adapters=fake endpoint={endpoint}"
            )),
            "one ready line names the worker and its harnesses: {lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.starts_with("server started main=")),
            "{lines:?}"
        );
        assert!(
            lines.contains(&format!(
                "client enrolled principal=client:operator identity={}",
                paths.client_identity.display()
            )),
            "{lines:?}"
        );

        // The client enrollment is real and remembered (ADR-0007).
        assert!(horde_identity::StoredIdentity::exists(
            &paths.client_identity
        ));
        assert_eq!(
            horde_identity::store::load_endpoint(&paths.client_identity),
            Some(endpoint.clone()),
            "the join stores the endpoint `up` dialed"
        );

        // The worker was handed a token that actually redeems: parse it out
        // of the daemon's argv and enroll with it — the proof a body-swapped
        // mint could not fake.
        let argv = std::fs::read_to_string(&args_file).expect("the worker saw its argv");
        let token_text = argv
            .lines()
            .skip_while(|line| *line != "--join")
            .nth(1)
            .expect("a --join token was passed");
        let token = horde_identity::JoinToken::parse(token_text).expect("the token parses");
        assert_eq!(token.host_port, format!("127.0.0.1:{enrollment_port}"));
        let enrolled = horde_identity::join(
            &token.enrollment_endpoint(),
            &token.code,
            horde_server_testkit::secure::hello(horde_identity::PrincipalRole::Worker, "up-test"),
        )
        .await
        .expect("the minted token enrolls");
        assert_eq!(enrolled.principal.to_string(), "worker:up-test");

        // Teardown reached both children it started, deliberately.
        assert!(stopped(root.path(), "server"), "the server got SIGTERM");
        assert!(stopped(root.path(), "worker"), "the worker got SIGTERM");
    }

    /// The other half of ownership (ADR-0007): a server found running is
    /// left running, an enrolled client and worker are not re-enrolled, and
    /// teardown stops only the worker this invocation started.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_found_server_and_existing_identities_are_left_alone() {
        let running = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a stand-in server binds");
        let bind = running.local_addr().expect("an addr").to_string();

        let root = tempfile::tempdir().expect("a temp dir");
        let paths = paths_under(root.path());
        for dir in [
            paths.client_identity.clone(),
            paths.worker_state.join("identity"),
        ] {
            std::fs::create_dir_all(&dir).expect("the dir");
            for file in ["identity.pem", "identity.key", "ca.pem"] {
                std::fs::write(dir.join(file), "seeded").expect("writes");
            }
        }

        let server_binary = fake_daemon(root.path(), "server", "", &[]);
        let worker_binary = fake_daemon(
            root.path(),
            "worker",
            "",
            &["registered worker=up-test resume=7".to_owned()],
        );
        let environment = UpEnvironment {
            server_binary,
            worker_binary,
            paths,
            bind: bind.clone(),
            enrollment_bind: "127.0.0.1:2".to_owned(),
            adapters: Vec::new(),
            worker_name: "up-test".to_owned(),
        };
        let mut lines = Vec::new();
        super::stand_up(
            &environment,
            &mut |line| lines.push(line),
            std::future::ready(()),
        )
        .await
        .expect("the machine stands up");

        assert!(
            lines.contains(&format!("server already-running main={bind}")),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("client already-enrolled identity=")),
            "{lines:?}"
        );
        assert!(
            lines.contains(&format!(
                "ready worker=up-test adapters=none endpoint=https://{bind}"
            )),
            "{lines:?}"
        );
        assert!(
            !stopped(root.path(), "server"),
            "a server nobody here started was never spawned, so never stopped"
        );
        assert!(stopped(root.path(), "worker"), "the started worker stops");
    }

    /// A machine with no client identity and no bootstrap-token file is a
    /// refusal that names the file and the remote-join alternative — and
    /// the server this invocation had already started is torn down on the
    /// way out, not leaked.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_missing_bootstrap_token_refuses_and_tears_down_the_started_server() {
        let root = tempfile::tempdir().expect("a temp dir");
        let paths = paths_under(root.path());
        let server_binary = fake_daemon(
            root.path(),
            "server",
            "",
            &[
                "listening main=127.0.0.1:4242".to_owned(),
                "listening enrollment=127.0.0.1:4243".to_owned(),
            ],
        );
        let worker_binary = fake_daemon(root.path(), "worker", "", &[]);
        let environment = UpEnvironment {
            server_binary,
            worker_binary,
            paths: paths.clone(),
            bind: "127.0.0.1:1".to_owned(),
            enrollment_bind: "127.0.0.1:2".to_owned(),
            adapters: Vec::new(),
            worker_name: "up-test".to_owned(),
        };
        let mut lines = Vec::new();
        let refused = super::stand_up(
            &environment,
            &mut |line| lines.push(line),
            std::future::ready(()),
        )
        .await
        .expect_err("no token file must refuse");
        let refusal = refused.to_string();
        assert!(
            refusal.contains("bootstrap-token") && refusal.contains("enroll join"),
            "the refusal names the file and the alternative: {refusal}"
        );
        assert!(
            stopped(root.path(), "server"),
            "the error path tears down what was already started"
        );
        assert!(
            !stopped(root.path(), "worker"),
            "nothing ever started the worker"
        );
    }
}
