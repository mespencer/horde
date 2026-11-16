//! `horde worker add` (DESIGN §3, ADR-0007 2026-11-13 amendment) over the
//! network hop the CLI suite's stand-ins leave out
//! (`crates/horde-cli/tests/cli_suite/worker_add.rs`): a real sshd in a container,
//! reached by the system ssh and scp with key auth and a pinned host key.
//! It needs Docker, so it is `#[ignore]`d and also gated on
//! `HORDE_SSH_DOCKER_TEST=1`; run it with
//! `HORDE_SSH_DOCKER_TEST=1 cargo test -p horde-e2e --test e2e_suite worker_add -- --ignored`.

use crate::support::System;

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
