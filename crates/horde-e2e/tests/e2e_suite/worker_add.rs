//! `horde worker add` (DESIGN §3, ADR-0007 2026-11-13 amendment) against a
//! real server and the real `horde-worker` binary.
//!
//! The hermetic scenarios stand in for the remote machine with two small
//! shell programs passed as `--ssh` and `--scp`: the ssh stand-in runs the
//! remote command under `sh -c` with a throwaway `$HOME` (and `$PATH`), the
//! scp stand-in copies into that home. Everything the verb sends — the
//! scripts, the token on stdin, the copy — runs exactly as it would on a
//! remote machine; only the network hop is missing.
//!
//! The Docker scenario puts the hop back: a real sshd in a container,
//! reached by the system ssh and scp with key auth and a pinned host key.
//! It needs Docker, so it is `#[ignore]`d and also gated on
//! `HORDE_SSH_DOCKER_TEST=1`; run it with
//! `HORDE_SSH_DOCKER_TEST=1 cargo test -p horde-e2e --test e2e_suite worker_add -- --ignored`.

use std::path::{Path, PathBuf};

use crate::support::{System, bounded_output};

/// The ssh stand-in: the last argument is the remote command.
const SSH_SHIM: &str = "#!/bin/sh\n\
    for last; do :; done\n\
    HOME=\"$FAKE_HOME\" PATH=\"$FAKE_PATH\" exec sh -c \"$last\"\n";

/// The scp stand-in: `… <local> <target>:<remote>`, the remote path
/// relative to the stand-in home, as scp's is to the remote home.
const SCP_SHIM: &str = "#!/bin/sh\n\
    for last; do :; done\n\
    prev=\n\
    for a; do [ \"$a\" = \"$last\" ] && src=$prev; prev=$a; done\n\
    cd \"$FAKE_HOME\" && exec cp \"$src\" \"${last#*:}\"\n";

fn executable(path: &Path, content: &str) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, content).expect("the script writes");
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
        .expect("the script is executable");
}

/// A stand-in remote machine: its home, its PATH (with a `claude` that
/// answers `--version`), and the two shims.
struct Remote {
    home: PathBuf,
    path: String,
    ssh: PathBuf,
    scp: PathBuf,
}

fn remote(root: &Path) -> Remote {
    let home = root.join("remote-home");
    let bin = root.join("remote-path");
    std::fs::create_dir_all(&home).expect("the remote home");
    std::fs::create_dir_all(&bin).expect("the remote PATH dir");
    executable(
        &bin.join("claude"),
        "#!/bin/sh\necho '9.9.9 (Fake Claude)'\n",
    );
    let ssh = root.join("ssh-shim");
    let scp = root.join("scp-shim");
    executable(&ssh, SSH_SHIM);
    executable(&scp, SCP_SHIM);
    Remote {
        home,
        path: format!("{}:/usr/bin:/bin", bin.display()),
        ssh,
        scp,
    }
}

/// Runs `horde worker add` as the operator, against `remote`.
async fn worker_add(system: &System, remote: &Remote, extra: &[&str]) -> std::process::Output {
    let worker_dir = system
        .worker_binary()
        .parent()
        .expect("the worker binary has a directory")
        .to_path_buf();
    let mut command = tokio::process::Command::new(system.horde_binary());
    command
        .args(system.operator_args())
        .args(["--json", "worker", "add", "box-ssh", "me@remote"])
        .args(["--ssh", &remote.ssh.to_string_lossy()])
        .args(["--scp", &remote.scp.to_string_lossy()])
        .args(["--binary-dir", &worker_dir.to_string_lossy()])
        .args(["--adapter", "fake", "--timeout-secs", "20"])
        .args(extra)
        .env("FAKE_HOME", &remote.home)
        .env("FAKE_PATH", &remote.path);
    bounded_output("horde worker add", command).await
}

/// Stops a worker the verb started detached, through the testkit's guard
/// (never pid 0, 1, this process or its ancestry).
fn stop(pid: u64) {
    let Ok(pid) = u32::try_from(pid) else { return };
    if horde_server_testkit::reap::guard(pid).is_ok() {
        let _ = std::process::Command::new("kill")
            .arg(pid.to_string())
            .status();
    }
}

/// The whole flow: probe, copy, install, mint, token over stdin, start,
/// registration, harness report — and the remote left with an identity,
/// a launcher, and no token file. A second run refuses: the machine is
/// already a worker.
#[tokio::test]
async fn worker_add_bootstraps_a_machine_over_ssh_and_refuses_a_second_time() {
    let system = System::launch().await;
    let remote = remote(system.state.path());
    let output = worker_add(&system, &remote, &[]).await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "worker add failed: {}\n{stdout}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_str(stdout.trim()).expect("one JSON report");
    let pid = report["pid"].as_u64().expect("a detached worker pid");

    assert_eq!(report["worker"], "box-ssh");
    assert_eq!(report["registered"], true);
    assert_eq!(report["mode"], "foreground");
    assert_eq!(
        report["binary"],
        remote
            .home
            .join(".horde/bin/horde-worker")
            .display()
            .to_string()
    );
    assert!(
        report["version"]
            .as_str()
            .is_some_and(|v| v.starts_with("horde-worker ")),
        "{report}"
    );
    let claude = &report["harnesses"][0];
    assert_eq!(claude["name"], "claude");
    assert_eq!(claude["version"], "9.9.9 (Fake Claude)");
    assert_eq!(claude["required"], false);
    assert!(
        report["warnings"][0]
            .as_str()
            .is_some_and(|w| w.contains("names localhost")),
        "the loopback token is called out: {report}"
    );

    // The roster agrees, and the remote holds what a join leaves behind.
    let listed = system.cli_json(&["worker", "list"]).await;
    assert!(listed.to_string().contains("box-ssh"), "{listed}");
    assert!(
        remote
            .home
            .join(".horde/worker/identity/identity.pem")
            .is_file()
    );
    assert!(remote.home.join(".horde/bin/horde-worker-start").is_file());
    assert!(
        !remote.home.join(".horde/join-token").exists(),
        "the token file is gone"
    );

    let again = worker_add(&system, &remote, &[]).await;
    stop(pid);
    assert!(!again.status.success());
    let refusal: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&again.stderr).trim())
            .expect("a JSON refusal");
    assert_eq!(refusal["error"]["kind"], "worker_add");
    assert!(
        refusal["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("already holds a worker identity")),
        "{refusal}"
    );
}

/// `--dry-run` connects to nothing: with no identity and no server it
/// still prints the plan, and the stand-in remote is never touched.
#[tokio::test]
async fn a_dry_run_prints_the_plan_and_touches_nothing() {
    let horde = horde_server_testkit::process::product_binary("horde-cli", "horde")
        .await
        .expect("the CLI binary exists");
    let root = tempfile::tempdir().expect("a root");
    let remote = remote(root.path());
    let mut command = tokio::process::Command::new(&horde);
    command
        .args(["--identity", &root.path().join("no-id").to_string_lossy()])
        .args(["--json", "worker", "add", "box", "me@remote", "--dry-run"])
        .args(["--ssh", &remote.ssh.to_string_lossy()])
        .env_remove("HORDE_ENDPOINT")
        .env("FAKE_HOME", &remote.home);
    let output = bounded_output("horde worker add --dry-run", command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim()).expect("JSON");
    assert_eq!(plan["dry_run"], true);
    assert_eq!(
        plan["steps"][0]["argv"][0],
        remote.ssh.display().to_string()
    );
    assert_eq!(
        std::fs::read_dir(&remote.home)
            .expect("the home lists")
            .count(),
        0,
        "nothing ran remotely"
    );
}

/// Runs a command that must succeed, returning stdout.
fn run_ok(program: &str, args: &[&str]) -> String {
    let output = std::process::Command::new(program)
        .args(args)
        .output()
        .unwrap_or_else(|error| panic!("{program} runs: {error}"));
    assert!(
        output.status.success(),
        "{program} {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// The image: the host's own Ubuntu release (so the locally built,
/// dynamically linked `horde-worker` finds its C library), with sshd and a
/// key-only user.
const DOCKERFILE: &str = "FROM ubuntu:24.04\n\
    RUN apt-get update && apt-get install -y --no-install-recommends openssh-server \
    && rm -rf /var/lib/apt/lists/* && mkdir -p /run/sshd \
    && useradd -m -s /bin/bash horde && ssh-keygen -A\n\
    COPY authorized_keys /home/horde/.ssh/authorized_keys\n\
    RUN chown -R horde:horde /home/horde/.ssh && chmod 700 /home/horde/.ssh \
    && chmod 600 /home/horde/.ssh/authorized_keys\n";

/// Removes the container however the test ends.
struct Container(String);

impl Drop for Container {
    fn drop(&mut self) {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", &self.0])
            .output();
    }
}

/// The real thing: system ssh and scp, key auth, a pinned host key, an
/// sshd in a container on the host network (so the loopback token reaches
/// this test's server).
#[tokio::test]
#[ignore = "needs Docker; set HORDE_SSH_DOCKER_TEST=1 and pass --ignored"]
async fn worker_add_bootstraps_a_container_over_real_ssh() {
    if std::env::var("HORDE_SSH_DOCKER_TEST").as_deref() != Ok("1") {
        eprintln!("skipped: HORDE_SSH_DOCKER_TEST is not 1");
        return;
    }
    let system = System::launch().await;
    let root = system.state.path().join("docker");
    std::fs::create_dir_all(&root).expect("the docker dir");
    let key = root.join("id_ed25519");
    run_ok(
        "ssh-keygen",
        &[
            "-q",
            "-t",
            "ed25519",
            "-N",
            "",
            "-f",
            &key.to_string_lossy(),
        ],
    );
    std::fs::copy(root.join("id_ed25519.pub"), root.join("authorized_keys"))
        .expect("the authorized key");
    std::fs::write(root.join("Dockerfile"), DOCKERFILE).expect("the Dockerfile");
    run_ok(
        "docker",
        &[
            "build",
            "-q",
            "-t",
            "horde-ssh-test:w12i",
            &root.to_string_lossy(),
        ],
    );

    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
        listener.local_addr().expect("an address").port()
    };
    let id = run_ok(
        "docker",
        &[
            "run",
            "-d",
            "--network",
            "host",
            "horde-ssh-test:w12i",
            "/usr/sbin/sshd",
            "-D",
            "-p",
            &port.to_string(),
            "-o",
            "ListenAddress=127.0.0.1",
        ],
    );
    let container = Container(id.trim().to_owned());
    let host_key = run_ok(
        "docker",
        &[
            "exec",
            &container.0,
            "cat",
            "/etc/ssh/ssh_host_ed25519_key.pub",
        ],
    );
    let known_hosts = root.join("known_hosts");
    std::fs::write(&known_hosts, format!("[127.0.0.1]:{port} {host_key}"))
        .expect("the pinned host key");
    let config = root.join("ssh_config");
    std::fs::write(
        &config,
        format!(
            "Host horde-container\n\
             \x20 HostName 127.0.0.1\n\
             \x20 Port {port}\n\
             \x20 User horde\n\
             \x20 IdentityFile {key}\n\
             \x20 IdentitiesOnly yes\n\
             \x20 UserKnownHostsFile {known}\n\
             \x20 StrictHostKeyChecking yes\n\
             \x20 BatchMode yes\n",
            key = key.display(),
            known = known_hosts.display()
        ),
    )
    .expect("the ssh config");
    // sshd takes a moment to listen.
    for _ in 0..50 {
        if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    let worker_dir = system
        .worker_binary()
        .parent()
        .expect("a directory")
        .to_path_buf();
    let mut command = tokio::process::Command::new(system.horde_binary());
    command
        .args(system.operator_args())
        .args(["--json", "worker", "add", "box-docker", "horde-container"])
        .args(["--ssh-config", &config.to_string_lossy()])
        .args(["--binary-dir", &worker_dir.to_string_lossy()])
        .args(["--adapter", "fake", "--timeout-secs", "30"]);
    let output = tokio::time::timeout(std::time::Duration::from_secs(300), command.output())
        .await
        .expect("worker add finishes")
        .expect("worker add runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "worker add failed: {}\n{stdout}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_str(stdout.trim()).expect("one JSON report");
    assert_eq!(report["registered"], true);
    assert_eq!(report["binary"], "/home/horde/.horde/bin/horde-worker");
    assert_eq!(report["harnesses"][0]["name"], "claude");
    assert_eq!(report["harnesses"][0]["path"], serde_json::Value::Null);
    let listed = system.cli_json(&["worker", "list"]).await;
    assert!(listed.to_string().contains("box-docker"), "{listed}");
    let leftover = std::process::Command::new("docker")
        .args([
            "exec",
            &container.0,
            "test",
            "-e",
            "/home/horde/.horde/join-token",
        ])
        .status()
        .expect("docker exec runs");
    assert!(!leftover.success(), "the token file is gone remotely");
}
