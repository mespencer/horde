//! The whole-system harness: a real server process, real worker daemons,
//! and the real `horde` CLI, arranged the way an operator would arrange them.
//!
//! Everything trust-bearing goes through the production paths: the operator
//! identity comes from the bootstrap code on the server's own stdout, worker
//! identities from codes the operator mints with `horde worker enroll`, and
//! every connection after that is mTLS. The only parsing this harness does is
//! of the interfaces the products commit to — operator lines on stdout and
//! `--json` renders — which is the point: a scenario that needed more than
//! those would be evidence the operator interface is missing something.

use std::path::{Path, PathBuf};

use horde_server_testkit::process::{SpawnedProcess, product_binary, value_after};

/// One bounded poll interval for cross-process waits.
const POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// How many polls a cross-process wait may spend before it is a failure.
const POLLS: u32 = 300;

/// A running Horde: the server process, its endpoints, and an enrolled
/// operator identity.
pub(crate) struct System {
    /// Held only for kill-on-drop: the server dies with the test. Nothing
    /// reads it back — the scenarios talk to the server over its sockets.
    _server: SpawnedProcess,
    /// The main mTLS endpoint, as `https://localhost:PORT`.
    pub(crate) main: String,
    /// The TLS-only enrollment endpoint, as `https://localhost:PORT`.
    pub(crate) enrollment: String,
    /// The server-mounted git Relay, as `http://127.0.0.1:PORT` — the
    /// loopback listener `--relay-bind` stands up (fix a10).
    pub(crate) relay: String,
    /// Owns every state directory this system uses.
    pub(crate) state: tempfile::TempDir,
    /// The operator's enrolled identity directory.
    pub(crate) operator: PathBuf,
    horde: PathBuf,
    worker_bin: PathBuf,
}

impl System {
    /// The worker daemon binary this system spawns workers from.
    pub(crate) fn worker_binary(&self) -> PathBuf {
        self.worker_bin.clone()
    }
}

/// The `https://localhost:PORT` form of a printed `IP:PORT` listener line.
///
/// `localhost` rather than the printed IP because it is the DNS name the
/// server credential always carries, and the name a client dials is the name
/// the certificate must answer for.
fn endpoint_of(listen: &str) -> String {
    let port = listen.rsplit(':').next().unwrap_or_default();
    format!("https://localhost:{port}")
}

impl System {
    /// Builds the three product binaries (if needed), starts a fresh server
    /// on OS-chosen loopback ports, and enrolls the operator through the
    /// bootstrap code — the exact first-run ceremony DESIGN §3 describes.
    pub(crate) async fn launch() -> System {
        let server_bin = product_binary("horde-server", "horde-server")
            .await
            .expect("the server binary exists");
        let horde = product_binary("horde-cli", "horde")
            .await
            .expect("the CLI binary exists");
        let worker_bin = product_binary("horde-worker", "horde-worker")
            .await
            .expect("the worker binary exists");

        let state = tempfile::tempdir().expect("a state directory");
        let db = state.path().join("horde.db");
        let server_state = state.path().join("server-state");
        let server = SpawnedProcess::spawn(
            "horde-server",
            &server_bin,
            &[
                &db.to_string_lossy(),
                "--serve",
                "--bind",
                "127.0.0.1:0",
                "--enrollment-bind",
                "127.0.0.1:0",
                "--relay-bind",
                "127.0.0.1:0",
                "--state-dir",
                &server_state.to_string_lossy(),
            ],
            &[],
        )
        .expect("the server spawns");

        // A fresh server has no client, so it must mint the bootstrap code;
        // its stdout is the root of trust for everything that follows.
        let stdout = server
            .await_stdout("the operator lines", |out| {
                out.contains("bootstrap-client-code ")
            })
            .await
            .expect("the server announces itself");
        let main = endpoint_of(value_after(&stdout, "listening main=").expect("a main address"));
        let enrollment = endpoint_of(
            value_after(&stdout, "listening enrollment=").expect("an enrollment address"),
        );
        let code = value_after(&stdout, "bootstrap-client-code ")
            .expect("the bootstrap code")
            .to_owned();
        let relay = format!(
            "http://{}",
            value_after(&stdout, "listening relay=").expect("a relay address")
        );

        let system = System {
            _server: server,
            main,
            enrollment,
            relay,
            operator: state.path().join("operator-identity"),
            state,
            horde,
            worker_bin,
        };

        // `horde enroll join` — the CLI's own verb, as a real process.
        let joined = system
            .cli(&["enroll", "join", &system.enrollment.clone(), &code])
            .await;
        assert!(
            joined.status.success(),
            "the operator must enroll through the bootstrap code: {}",
            String::from_utf8_lossy(&joined.stderr)
        );
        system
    }

    /// Runs one `horde` invocation as the operator, JSON output, bounded.
    pub(crate) async fn cli(&self, args: &[&str]) -> std::process::Output {
        let mut command = tokio::process::Command::new(&self.horde);
        command
            .args(["--endpoint", &self.main, "--json"])
            .args(["--identity", &self.operator.to_string_lossy()])
            .args(args);
        tokio::time::timeout(std::time::Duration::from_secs(30), command.output())
            .await
            .unwrap_or_else(|_| panic!("horde {args:?} must finish rather than hang"))
            .expect("the CLI runs")
    }

    /// Runs a verb that must succeed and prints one JSON line, parsed.
    pub(crate) async fn cli_json(&self, args: &[&str]) -> serde_json::Value {
        let output = self.cli(args).await;
        assert!(
            output.status.success(),
            "horde {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        let line = stdout
            .lines()
            .find(|line| !line.trim().is_empty())
            .unwrap_or_else(|| panic!("horde {args:?} printed nothing"));
        serde_json::from_str(line)
            .unwrap_or_else(|error| panic!("horde {args:?} printed non-JSON ({error}): {line}"))
    }

    /// One string field of a JSON render, by key.
    pub(crate) async fn cli_field(&self, args: &[&str], field: &str) -> String {
        let value = self.cli_json(args).await;
        value[field]
            .as_str()
            .unwrap_or_else(|| panic!("horde {args:?} carried no {field}: {value}"))
            .to_owned()
    }

    /// Creates a session, returning `(session_id, root_agent_id)`.
    pub(crate) async fn new_session(&self, title: &str) -> (String, String) {
        let created = self.cli_json(&["session", "new", title]).await;
        let field = |name: &str| {
            created[name]
                .as_str()
                .unwrap_or_else(|| panic!("session new carried no {name}: {created}"))
                .to_owned()
        };
        (field("session_id"), field("root_agent_id"))
    }

    /// One full replay of a session's log, as the CLI renders it.
    pub(crate) async fn sync(&self, session_id: &str) -> Vec<serde_json::Value> {
        let output = self.cli(&["sync", session_id]).await;
        assert!(
            output.status.success(),
            "sync {session_id} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(|line| serde_json::from_str(line).expect("sync prints JSON lines"))
            .collect()
    }

    /// Replays the session until `holds`, bounded; the events that satisfied
    /// it come back for further assertions.
    pub(crate) async fn sync_until(
        &self,
        session_id: &str,
        what: &str,
        holds: impl Fn(&[serde_json::Value]) -> bool,
    ) -> Vec<serde_json::Value> {
        let mut last = Vec::new();
        for _ in 0..POLLS {
            last = self.sync(session_id).await;
            if holds(&last) {
                return last;
            }
            tokio::time::sleep(POLL).await;
        }
        panic!("{what} never appeared in the session log; last replay: {last:?}");
    }

    /// Mints a one-time worker enrollment code, as the operator would.
    pub(crate) async fn mint_worker_code(&self, name: &str) -> String {
        self.cli_field(&["worker", "enroll", name], "code").await
    }

    /// Writes `script_json` beside the state and starts a worker named
    /// `name` running it — the arrangement every scripted scenario opens
    /// with.
    pub(crate) async fn spawn_scripted_worker(
        &self,
        name: &str,
        script_json: &str,
    ) -> SpawnedProcess {
        let script = self.state.path().join(format!("{name}-script.json"));
        std::fs::write(&script, script_json).expect("the script writes");
        self.spawn_worker(
            name,
            &self.state.path().join(name),
            &["--fake-script", &script.to_string_lossy()],
        )
        .await
    }

    /// A fresh session whose root agent has been placed — the point from
    /// which a scenario can start sending.
    pub(crate) async fn placed_session(&self, title: &str) -> (String, String) {
        let (session_id, root) = self.new_session(title).await;
        self.sync_until(&session_id, "the root agent's placement", |events| {
            has_kind(events, "placement_changed")
        })
        .await;
        (session_id, root)
    }

    /// Starts a worker daemon named `name` over `state_dir`, enrolling it
    /// first when the directory holds no identity yet, and waits for its
    /// registration line. `extra` carries scenario flags (`--fake-script`,
    /// `--workspace-root`, `--relay`).
    pub(crate) async fn spawn_worker(
        &self,
        name: &str,
        state_dir: &Path,
        extra: &[&str],
    ) -> SpawnedProcess {
        let state = state_dir.to_string_lossy().into_owned();
        let mut args: Vec<String> = [
            "--server",
            &self.main,
            "--state-dir",
            &state,
            "--adapter",
            "fake",
        ]
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
        if !state_dir.join("identity").join("identity.pem").is_file() {
            let code = self.mint_worker_code(name).await;
            for argument in ["--join", &code, "--enrollment", &self.enrollment] {
                args.push(argument.to_owned());
            }
        }
        args.extend(extra.iter().map(|s| (*s).to_owned()));

        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        let daemon = SpawnedProcess::spawn(
            &format!("horde-worker[{name}]"),
            &self.worker_bin,
            &borrowed,
            &[],
        )
        .expect("the worker daemon spawns");
        daemon
            .await_stdout("the registration line", |out| {
                out.contains(&format!("registered worker={name}"))
            })
            .await
            .expect("the worker registers");
        daemon
    }
}

/// The texts of every output chunk in a replay, in log order.
pub(crate) fn outputs(events: &[serde_json::Value]) -> Vec<String> {
    events
        .iter()
        .filter(|event| event["kind"] == "agent_output_chunk")
        .filter_map(|event| event["detail"].as_str().map(ToOwned::to_owned))
        .collect()
}

/// Whether any event in a replay has this kind.
pub(crate) fn has_kind(events: &[serde_json::Value], kind: &str) -> bool {
    events.iter().any(|event| event["kind"] == kind)
}

/// The shared git driver, panicking on the failures an arrangement cannot
/// survive — the production git runs live inside `horde-relay` and the
/// worker daemons.
pub(crate) async fn git(dir: &Path, args: &[&str]) -> String {
    horde_server_testkit::process::git(dir, args)
        .await
        .expect("the git arrangement succeeds")
}
