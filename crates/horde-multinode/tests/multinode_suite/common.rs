//! The multi-node harness: one compose project per scenario, brought up and
//! joined through the operator's real flows, torn down on drop.
//!
//! Wrapped in `#[cfg(test)]` so the workspace's `expect_used` exemption
//! applies (clippy exempts a `cfg(test)` module, not a bare helper), and
//! `dead_code` is allowed module-wide because the real-harness binary
//! includes this file too (by path) and uses a subset of it — the same
//! arrangement as the relay suite's shared fixtures. It lives inside this
//! suite's directory so cargo-machete, which scans each target's own
//! directory, sees the dependencies it uses.
#![allow(dead_code)]

#[cfg(test)]
pub(crate) mod cluster {
    use std::path::PathBuf;
    use std::process::Stdio;
    use std::time::Duration;

    use serde_json::Value;
    use tokio::io::{AsyncBufReadExt, BufReader};

    /// How long one `docker` invocation may take before it is a hang. The
    /// image build is the slow one and gets [`BUILD_CEILING`] instead.
    const CEILING: Duration = Duration::from_secs(120);

    /// A cold image build compiles the three binaries in release.
    const BUILD_CEILING: Duration = Duration::from_secs(1800);

    /// One bounded poll interval for cross-container waits.
    const POLL: Duration = Duration::from_millis(500);

    /// The workers every topology runs, by service name — which is also the
    /// name each one enrolls under, so it is its worker id.
    pub(crate) const WORKERS: [&str; 3] = ["worker-1", "worker-2", "worker-3"];

    /// The compose file, relative to this crate.
    fn compose_file() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docker/multinode/compose.yaml")
    }

    /// One finished command: status, stdout, stderr.
    #[derive(Debug)]
    pub(crate) struct Ran {
        pub(crate) ok: bool,
        pub(crate) stdout: String,
        pub(crate) stderr: String,
    }

    /// Runs `program args`, feeding `stdin` when given, bounded by `ceiling`.
    async fn run(program: &str, args: &[String], stdin: Option<&str>, ceiling: Duration) -> Ran {
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().unwrap_or_else(|error| {
            panic!("{program} could not start ({error}): is Docker installed?")
        });
        if let Some(input) = stdin {
            let mut pipe = child.stdin.take().expect("stdin was piped");
            tokio::io::AsyncWriteExt::write_all(&mut pipe, input.as_bytes())
                .await
                .expect("stdin takes the input");
            drop(pipe);
        }
        let output = tokio::time::timeout(ceiling, child.wait_with_output())
            .await
            .unwrap_or_else(|_| panic!("{program} {args:?} must finish within {ceiling:?}"))
            .expect("the command runs");
        Ran {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// A running topology: one compose project, every service its own
    /// container on the project's bridge network.
    pub(crate) struct Cluster {
        project: String,
        compose: PathBuf,
        profiles: Vec<String>,
    }

    impl Cluster {
        /// Builds the image (a cache hit after the first time), starts the
        /// topology under a project named for `scenario`, and joins it the
        /// way an operator would: `client` through the server's bootstrap
        /// token, `client-2` through a code `client` mints, each worker
        /// through a code minted with `horde worker enroll`. Returns once
        /// every worker is online.
        pub(crate) async fn up(scenario: &str) -> Cluster {
            Self::up_with(scenario, &[], &WORKERS).await
        }

        /// [`Self::up`] with extra compose profiles and the workers to join.
        pub(crate) async fn up_with(
            scenario: &str,
            profiles: &[&str],
            workers: &[&str],
        ) -> Cluster {
            let cluster = Cluster {
                project: format!("hmn-{scenario}-{}", std::process::id()),
                compose: compose_file(),
                profiles: profiles.iter().map(|p| (*p).to_owned()).collect(),
            };
            let built = cluster
                .compose_bounded(&["build", "server"], BUILD_CEILING)
                .await;
            assert!(built.ok, "the image must build: {}", built.stderr);
            let up = cluster.compose(&["up", "-d", "--no-build", "--wait"]).await;
            assert!(up.ok, "the topology must come up: {}", up.stderr);

            cluster.enroll_clients().await;
            for worker in workers {
                cluster.join_worker(worker).await;
            }
            for worker in workers {
                cluster.await_online(worker).await;
            }
            cluster
        }

        /// The compose argv prefix every invocation shares.
        fn base_args(&self) -> Vec<String> {
            let mut args = vec![
                "compose".to_owned(),
                "-f".to_owned(),
                self.compose.to_string_lossy().into_owned(),
                "-p".to_owned(),
                self.project.clone(),
            ];
            for profile in &self.profiles {
                args.extend(["--profile".to_owned(), profile.clone()]);
            }
            args
        }

        async fn compose_bounded(&self, args: &[&str], ceiling: Duration) -> Ran {
            let mut full = self.base_args();
            full.extend(args.iter().map(|a| (*a).to_owned()));
            run("docker", &full, None, ceiling).await
        }

        /// One `docker compose` invocation against this project.
        pub(crate) async fn compose(&self, args: &[&str]) -> Ran {
            self.compose_bounded(args, CEILING).await
        }

        /// Runs `args` inside `service` as its image user, with `stdin`.
        pub(crate) async fn exec(&self, service: &str, args: &[&str], stdin: Option<&str>) -> Ran {
            let mut full = self.base_args();
            full.extend(["exec".to_owned(), "-T".to_owned(), service.to_owned()]);
            full.extend(args.iter().map(|a| (*a).to_owned()));
            run("docker", &full, stdin, CEILING).await
        }

        /// One `horde --json` invocation inside the client container
        /// `client`, against the endpoint its join stored.
        pub(crate) async fn horde_in(&self, client: &str, args: &[&str]) -> Ran {
            let mut full = vec!["horde", "--json"];
            full.extend_from_slice(args);
            self.exec(client, &full, None).await
        }

        /// A `horde` verb in `client` that must succeed and print one JSON
        /// line, parsed.
        pub(crate) async fn horde_json(&self, args: &[&str]) -> Value {
            let ran = self.horde_in("client", args).await;
            assert!(ran.ok, "horde {args:?} failed: {}", ran.stderr);
            first_json_line(&ran.stdout, args)
        }

        /// One string field of a `horde` verb's JSON render.
        pub(crate) async fn horde_field(&self, args: &[&str], field: &str) -> String {
            let value = self.horde_json(args).await;
            value[field]
                .as_str()
                .unwrap_or_else(|| panic!("horde {args:?} carried no {field}: {value}"))
                .to_owned()
        }

        /// `client` joins with the bootstrap token the server filed in its
        /// state directory; `client-2` with a code `client` mints. Both read
        /// the token from stdin, so it never rides an argv.
        async fn enroll_clients(&self) {
            let token = self.bootstrap_token().await;
            let joined = self
                .exec(
                    "client",
                    &["horde", "enroll", "join", "--token-file", "/dev/stdin"],
                    Some(&token),
                )
                .await;
            assert!(
                joined.ok,
                "client must enroll with the bootstrap token: {}",
                joined.stderr
            );

            let code = self
                .horde_field(&["enroll", "client", "client-2"], "token")
                .await;
            let joined = self
                .exec(
                    "client-2",
                    &["horde", "enroll", "join", "--token-file", "/dev/stdin"],
                    Some(&code),
                )
                .await;
            assert!(
                joined.ok,
                "client-2 must enroll with a minted code: {}",
                joined.stderr
            );
        }

        /// The server's bootstrap token, once it has filed it.
        async fn bootstrap_token(&self) -> String {
            for _ in 0..120_u32 {
                let ran = self
                    .exec("server", &["cat", "/state/bootstrap-token"], None)
                    .await;
                if ran.ok && ran.stdout.trim().starts_with("horde://join/") {
                    return ran.stdout.trim().to_owned();
                }
                tokio::time::sleep(POLL).await;
            }
            panic!("the server never filed its bootstrap token");
        }

        /// Mints `worker`'s join token from `client` and hands it to the
        /// worker container, whose entrypoint is waiting for it.
        pub(crate) async fn join_worker(&self, worker: &str) {
            let token = self
                .horde_field(&["worker", "enroll", worker], "token")
                .await;
            let handed = self
                .exec(
                    worker,
                    &[
                        "sh",
                        "-c",
                        "umask 077 && cat > /join/token.part && mv /join/token.part /join/token",
                    ],
                    Some(&token),
                )
                .await;
            assert!(handed.ok, "{worker} must take its token: {}", handed.stderr);
        }

        /// The connected-worker list, as `client` sees it.
        pub(crate) async fn workers(&self) -> Vec<Value> {
            self.horde_json(&["worker", "list"]).await["workers"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        }

        /// Waits until `worker` is listed online.
        pub(crate) async fn await_online(&self, worker: &str) {
            self.await_worker_state(worker, |state| state == Some("online"))
                .await;
        }

        /// Waits until `worker`'s listed state satisfies `holds` (`None` when
        /// it is not listed at all).
        pub(crate) async fn await_worker_state(
            &self,
            worker: &str,
            holds: impl Fn(Option<&str>) -> bool,
        ) {
            let mut last = Vec::new();
            for _ in 0..240_u32 {
                last = self.workers().await;
                let state = last
                    .iter()
                    .find(|w| {
                        w["worker_id"].as_str() == Some(worker)
                            || w["name"].as_str() == Some(worker)
                    })
                    .and_then(|w| w["state"].as_str());
                if holds(state) {
                    return;
                }
                tokio::time::sleep(POLL).await;
            }
            panic!("{worker} never reached the awaited state; last list: {last:?}");
        }

        /// Creates a session pinned to `worker`, returning
        /// `(session_id, root_agent_id)` once the root agent is placed.
        pub(crate) async fn pinned_session(&self, title: &str, worker: &str) -> (String, String) {
            let created = self
                .horde_json(&["session", "new", title, "--pin-worker", worker])
                .await;
            let session = field(&created, "session_id");
            let root = field(&created, "root_agent_id");
            self.sync_until(&session, &format!("{root}'s placement"), |events| {
                events.iter().any(|e| e["kind"] == "placement_changed")
            })
            .await;
            (session, root)
        }

        /// One full replay of a session's log, as the CLI renders it.
        pub(crate) async fn sync(&self, session: &str) -> Vec<Value> {
            let ran = self.horde_in("client", &["sync", session]).await;
            assert!(ran.ok, "sync {session} failed: {}", ran.stderr);
            json_lines(&ran.stdout)
        }

        /// Replays the session until `holds`, bounded; the satisfying replay
        /// comes back.
        pub(crate) async fn sync_until(
            &self,
            session: &str,
            what: &str,
            holds: impl Fn(&[Value]) -> bool,
        ) -> Vec<Value> {
            self.sync_until_within(session, what, Duration::from_secs(90), holds)
                .await
        }

        /// [`Self::sync_until`] with an explicit bound.
        pub(crate) async fn sync_until_within(
            &self,
            session: &str,
            what: &str,
            bound: Duration,
            holds: impl Fn(&[Value]) -> bool,
        ) -> Vec<Value> {
            let deadline = tokio::time::Instant::now() + bound;
            let mut last = Vec::new();
            while tokio::time::Instant::now() < deadline {
                last = self.sync(session).await;
                if holds(&last) {
                    return last;
                }
                tokio::time::sleep(POLL).await;
            }
            panic!(
                "{what} never appeared in the session log within {bound:?}; last replay: {last:?}"
            );
        }

        /// One agent's status as the server folds it.
        pub(crate) async fn agent_status(&self, agent: &str) -> Value {
            self.horde_json(&["agent", "status", agent]).await
        }

        /// Sends `text` to `agent` from `client` and waits for its echo.
        pub(crate) async fn echo(&self, session: &str, agent: &str, text: &str) -> Vec<Value> {
            self.horde_json(&["send", agent, text]).await;
            self.sync_until(session, &format!("the echo {text:?}"), |events| {
                outputs(events).iter().any(|out| out == text)
            })
            .await
        }

        /// The container id of `service`.
        pub(crate) async fn container(&self, service: &str) -> String {
            let ran = self.compose(&["ps", "-q", service]).await;
            let id = ran.stdout.trim().to_owned();
            assert!(
                ran.ok && !id.is_empty(),
                "{service} has a container: {}",
                ran.stderr
            );
            id
        }

        /// The project's bridge network.
        fn network(&self) -> String {
            format!("{}_horde", self.project)
        }

        /// Cuts `service` off the network: its sockets stay open and its
        /// packets go nowhere — a partition, not a disconnect it can see.
        pub(crate) async fn partition(&self, service: &str) {
            let container = self.container(service).await;
            let ran = run(
                "docker",
                &[
                    "network".to_owned(),
                    "disconnect".to_owned(),
                    self.network(),
                    container,
                ],
                None,
                CEILING,
            )
            .await;
            assert!(ran.ok, "{service} must leave the network: {}", ran.stderr);
        }

        /// Heals [`Self::partition`].
        pub(crate) async fn heal(&self, service: &str) {
            let container = self.container(service).await;
            let ran = run(
                "docker",
                &[
                    "network".to_owned(),
                    "connect".to_owned(),
                    "--alias".to_owned(),
                    service.to_owned(),
                    self.network(),
                    container,
                ],
                None,
                CEILING,
            )
            .await;
            assert!(ran.ok, "{service} must rejoin the network: {}", ran.stderr);
        }

        /// Starts `horde --json args` in `client` as a long-running process
        /// whose stdout lines the caller reads — the shape of a follower.
        pub(crate) fn follow(&self, client: &str, args: &[&str]) -> Follower {
            let mut full = self.base_args();
            full.extend(["exec".to_owned(), "-T".to_owned(), client.to_owned()]);
            full.extend(["horde".to_owned(), "--json".to_owned()]);
            full.extend(args.iter().map(|a| (*a).to_owned()));
            let mut child = tokio::process::Command::new("docker")
                .args(&full)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()
                .expect("docker starts the follower");
            let stdout = child.stdout.take().expect("stdout was piped");
            Follower {
                _child: child,
                lines: BufReader::new(stdout).lines(),
            }
        }

        /// The last lines of every service's log, for a failure report.
        fn logs(&self) -> String {
            let mut args = self.base_args();
            args.extend(["logs", "--no-color", "--tail", "80"].map(ToOwned::to_owned));
            std::process::Command::new("docker")
                .args(&args)
                .output()
                .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
                .unwrap_or_default()
        }
    }

    impl Drop for Cluster {
        /// Tears the project down with its volumes. A failing scenario dumps
        /// the services' logs first; `HORDE_MULTINODE_KEEP=1` leaves the
        /// project running for a post-mortem.
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("---- {} logs ----\n{}", self.project, self.logs());
            }
            if std::env::var_os("HORDE_MULTINODE_KEEP").is_some() {
                eprintln!("leaving {} up (HORDE_MULTINODE_KEEP)", self.project);
                return;
            }
            let mut args = self.base_args();
            args.extend(
                ["down", "-v", "--remove-orphans", "--timeout", "5"].map(ToOwned::to_owned),
            );
            let _ = std::process::Command::new("docker")
                .args(&args)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
    }

    /// A long-running `horde` whose JSON lines arrive as they are printed.
    pub(crate) struct Follower {
        _child: tokio::process::Child,
        lines: tokio::io::Lines<BufReader<tokio::process::ChildStdout>>,
    }

    impl Follower {
        /// Reads lines until one satisfies `holds`, bounded.
        pub(crate) async fn until(&mut self, what: &str, holds: impl Fn(&Value) -> bool) -> Value {
            let read = async {
                while let Some(line) = self
                    .lines
                    .next_line()
                    .await
                    .expect("the follower's stdout reads")
                {
                    if let Ok(value) = serde_json::from_str::<Value>(&line)
                        && holds(&value)
                    {
                        return Some(value);
                    }
                }
                None
            };
            match tokio::time::timeout(Duration::from_secs(90), read).await {
                Ok(Some(value)) => value,
                Ok(None) => panic!("the follower ended before {what}"),
                Err(_) => panic!("{what} never reached the follower"),
            }
        }
    }

    /// A string field of a JSON render.
    pub(crate) fn field(value: &Value, name: &str) -> String {
        value[name]
            .as_str()
            .unwrap_or_else(|| panic!("no {name} in {value}"))
            .to_owned()
    }

    /// The first JSON line a `horde` verb printed.
    fn first_json_line(stdout: &str, args: &[&str]) -> Value {
        json_lines(stdout)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("horde {args:?} printed no JSON line"))
    }

    /// Every non-blank line of a `--json` render, parsed; a line that is not
    /// JSON fails the scenario naming it.
    fn json_lines(stdout: &str) -> Vec<Value> {
        let mut parsed = Vec::new();
        for line in stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
        {
            match serde_json::from_str(line) {
                Ok(value) => parsed.push(value),
                Err(error) => panic!("horde printed a non-JSON line ({error}): {line}"),
            }
        }
        parsed
    }

    /// The text of every output chunk in a replay, oldest first.
    pub(crate) fn outputs(events: &[Value]) -> Vec<String> {
        let mut texts = Vec::new();
        for event in events {
            if let (Some("agent_output_chunk"), Some(text)) =
                (event["kind"].as_str(), event["detail"].as_str())
            {
                texts.push(text.to_owned());
            }
        }
        texts
    }

    /// The events in a replay about `agent`.
    pub(crate) fn about<'a>(
        events: &'a [Value],
        agent: &'a str,
    ) -> impl Iterator<Item = &'a Value> {
        events
            .iter()
            .filter(move |event| event["agent_id"].as_str() == Some(agent))
    }
}
