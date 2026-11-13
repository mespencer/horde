//! `horde worker add` (ADR-0007, 2026-11-13 amendment) as the real binary,
//! against a real secured server, joining the real `horde-worker`.
//!
//! The remote machine is stood in for by two small shell programs passed as
//! `--ssh` and `--scp`: the ssh stand-in runs the remote command under
//! `sh -c` with a throwaway `$HOME` and `$PATH`, and the scp stand-in copies
//! into that home. Everything the verb sends — every script, the token on
//! stdin, the copy — runs exactly as it would on a remote machine; only the
//! network hop is missing. (The hop itself, over real ssh into an sshd
//! container, is the e2e suite's Docker scenario.)

use std::path::{Path, PathBuf};

use horde_identity::PrincipalRole;
use horde_server_testkit::secure::{SecureHarness, serve_secured, within};

#[cfg(test)]
mod scripts;

type Setup<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn executable(path: &Path, content: &str) -> Setup<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, content)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))?;
    Ok(())
}

/// A stand-in remote machine.
struct Remote {
    root: tempfile::TempDir,
}

impl Remote {
    /// A fresh home, a PATH holding a `claude` that answers `--version`,
    /// and the two shims with both baked in.
    fn new() -> Setup<Self> {
        let root = tempfile::tempdir()?;
        let home = root.path().join("home");
        let bin = root.path().join("path");
        std::fs::create_dir_all(&home)?;
        std::fs::create_dir_all(&bin)?;
        executable(
            &bin.join("claude"),
            "#!/bin/sh\necho '9.9.9 (Fake Claude)'\n",
        )?;
        executable(
            &root.path().join("ssh"),
            &format!(
                "#!/bin/sh\n\
                 for last; do :; done\n\
                 HOME='{home}' PATH='{bin}:/usr/bin:/bin' exec sh -c \"$last\"\n",
                home = home.display(),
                bin = bin.display(),
            ),
        )?;
        executable(
            &root.path().join("scp"),
            &format!(
                "#!/bin/sh\n\
                 for last; do :; done\n\
                 prev=\n\
                 for a; do [ \"$a\" = \"$last\" ] && src=$prev; prev=$a; done\n\
                 cd '{home}' && exec cp \"$src\" \"${{last#*:}}\"\n",
                home = home.display(),
            ),
        )?;
        Ok(Self { root })
    }

    fn home(&self) -> PathBuf {
        self.root.path().join("home")
    }

    fn program(&self, name: &str) -> String {
        self.root.path().join(name).display().to_string()
    }

    /// The pid in the worker's state lock, if a worker holds it.
    fn worker_pid(&self) -> Option<u32> {
        std::fs::read_to_string(self.home().join(".horde/worker/worker.lock"))
            .ok()?
            .trim()
            .parse()
            .ok()
    }

    /// Stops the worker the verb started detached, through the testkit's
    /// guard (never pid 0, 1, this process or its ancestry).
    fn stop_worker(&self) {
        if let Some(pid) = self.worker_pid()
            && horde_server_testkit::reap::guard(pid).is_ok()
        {
            let _ = std::process::Command::new("kill")
                .arg(pid.to_string())
                .status();
        }
    }
}

impl Drop for Remote {
    fn drop(&mut self) {
        self.stop_worker();
    }
}

/// A secured server and an operator identity on disk.
struct Operator {
    harness: SecureHarness,
    identity: tempfile::TempDir,
}

async fn operator() -> Setup<Operator> {
    let harness = serve_secured().await?;
    let identity = tempfile::tempdir()?;
    harness
        .enroll(PrincipalRole::Client, "operator")
        .await?
        .save(identity.path())?;
    Ok(Operator { harness, identity })
}

/// What one `horde` run printed.
struct Run {
    stdout: String,
    stderr: String,
    success: bool,
}

/// Runs the real `horde worker add <name> me@remote` as the operator,
/// `--json` only when `extra` asks for it.
/// Async: the server runs on this test's runtime.
async fn worker_add(
    operator: &Operator,
    remote: &Remote,
    name: &str,
    extra: &[&str],
) -> Setup<Run> {
    let worker =
        horde_server_testkit::process::product_binary("horde-worker", "horde-worker").await?;
    let worker_dir = worker.parent().ok_or("the worker binary has a directory")?;
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_horde"))
        .args(["--endpoint", &operator.harness.main_endpoint()])
        .args(["--identity", &operator.identity.path().to_string_lossy()])
        .args(["worker", "add", name, "me@remote"])
        .args(["--ssh", &remote.program("ssh")])
        .args(["--scp", &remote.program("scp")])
        .args(["--binary-dir", &worker_dir.to_string_lossy()])
        .args(["--adapter", "fake", "--timeout-secs", "20"])
        .args(extra)
        .output();
    let output = within("horde worker add", output).await??;
    Ok(Run {
        stdout: String::from_utf8(output.stdout)?,
        stderr: String::from_utf8(output.stderr)?,
        success: output.status.success(),
    })
}

/// The whole flow: probe, copy, install, mint, the token over stdin, start,
/// registration, harness report — and the remote left with an identity, a
/// launcher and no token file. A second run refuses: the machine is
/// already a worker.
#[tokio::test(flavor = "multi_thread")]
async fn worker_add_bootstraps_a_machine_and_refuses_a_second_time() -> Setup<()> {
    let operator = operator().await?;
    let remote = Remote::new()?;
    let run = worker_add(&operator, &remote, "box-ssh", &["--json"]).await?;
    assert!(
        run.success,
        "worker add failed: {}\n{}",
        run.stderr, run.stdout
    );
    let report: serde_json::Value = serde_json::from_str(run.stdout.trim())?;

    assert_eq!(report["worker"], "box-ssh");
    assert_eq!(report["target"], "me@remote");
    assert_eq!(report["registered"], true);
    assert_eq!(report["mode"], "foreground");
    let home = remote.home();
    assert_eq!(
        report["pid"].as_u64(),
        remote.worker_pid().map(u64::from),
        "the reported pid is the worker holding the state lock"
    );
    assert_eq!(
        report["binary"],
        home.join(".horde/bin/horde-worker").display().to_string()
    );
    assert_eq!(
        report["log"],
        home.join(".horde/worker.log").display().to_string()
    );
    assert!(
        report["version"]
            .as_str()
            .is_some_and(|v| v.starts_with("horde-worker ")),
        "{report}"
    );
    assert_eq!(report["harnesses"][0]["name"], "claude");
    assert_eq!(report["harnesses"][0]["version"], "9.9.9 (Fake Claude)");
    assert_eq!(report["harnesses"][1]["name"], "codex");
    assert!(
        report["warnings"][0]
            .as_str()
            .is_some_and(|w| w.contains("names localhost")),
        "the loopback token is called out: {report}"
    );

    // The server's roster agrees, and the remote holds what a join leaves.
    let identity = horde_identity::StoredIdentity::load(operator.identity.path())?;
    let mut client = operator.harness.client(&identity).await?;
    let workers = client.list_workers().await?;
    assert!(
        workers.iter().any(|worker| worker.name == "box-ssh"),
        "{workers:?}"
    );
    assert!(home.join(".horde/worker/identity/identity.pem").is_file());
    assert!(home.join(".horde/bin/horde-worker-start").is_file());
    assert!(
        !home.join(".horde/join-token").exists(),
        "the token file is gone"
    );

    let again = worker_add(&operator, &remote, "box-ssh", &["--json"]).await?;
    assert!(!again.success);
    let refusal: serde_json::Value = serde_json::from_str(again.stderr.trim())?;
    assert_eq!(refusal["error"]["kind"], "worker_add");
    assert!(
        refusal["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("already holds a worker identity")),
        "{refusal}"
    );
    Ok(())
}

/// A worker that cannot start — its state dir is taken by a file — ends
/// the wait at once, and the refusal carries its log; nothing registers
/// and the token file is still removed.
#[tokio::test(flavor = "multi_thread")]
async fn a_worker_that_cannot_join_is_refused_with_its_log() -> Setup<()> {
    let operator = operator().await?;
    let remote = Remote::new()?;
    std::fs::create_dir_all(remote.home().join(".horde"))?;
    std::fs::write(remote.home().join(".horde/worker"), "not a directory")?;
    let run = worker_add(&operator, &remote, "box-broken", &[]).await?;
    assert!(!run.success, "{}", run.stdout);
    // A terminal run narrates its steps on stderr, then the refusal.
    let progress: Vec<&str> = run.stderr.lines().take(3).collect();
    assert_eq!(
        progress,
        [
            "probing me@remote",
            "installing into .horde/bin",
            "starting the worker"
        ],
        "{}",
        run.stderr
    );
    assert!(
        run.stderr.contains(
            "error: worker add: the worker exited before its join was redeemed; its log ends:"
        ),
        "{}",
        run.stderr
    );
    assert!(!remote.home().join(".horde/join-token").exists());
    let identity = horde_identity::StoredIdentity::load(operator.identity.path())?;
    let workers = operator
        .harness
        .client(&identity)
        .await?
        .list_workers()
        .await?;
    assert!(workers.is_empty(), "{workers:?}");
    Ok(())
}

/// The registration wait gives up with a refusal, not a hang, and a name
/// on the roster is the only thing that satisfies it.
#[tokio::test(flavor = "multi_thread")]
async fn the_registration_wait_is_bounded() -> Setup<()> {
    let operator = operator().await?;
    let identity = horde_identity::StoredIdentity::load(operator.identity.path())?;
    let mut client = operator.harness.client(&identity).await?;
    let waited = within(
        "the registration wait",
        horde_cli::worker_add::await_registration(
            &mut client,
            "nobody",
            std::time::Duration::from_millis(600),
        ),
    )
    .await?;
    let refused = waited.err().ok_or("an unregistered name is refused")?;
    assert!(
        refused
            .to_string()
            .contains("nobody joined but did not register within 600ms"),
        "{refused}"
    );
    Ok(())
}

/// `--dry-run` connects to nothing: with no identity and no server it
/// still prints the plan, and the stand-in remote is never touched.
#[tokio::test]
async fn a_dry_run_prints_the_plan_and_touches_nothing() -> Setup<()> {
    let remote = Remote::new()?;
    let nowhere = tempfile::tempdir()?;
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_horde"))
        .args(["--identity", &nowhere.path().join("id").to_string_lossy()])
        .args(["--json", "worker", "add", "box", "me@remote", "--dry-run"])
        .args(["--ssh", &remote.program("ssh")])
        .env_remove("HORDE_ENDPOINT")
        .output()
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(plan["dry_run"], true);
    assert_eq!(plan["steps"][0]["argv"][0], remote.program("ssh"));
    assert_eq!(plan["steps"][0]["step"], "probe");
    let horde_dir = std::path::Path::new(env!("CARGO_BIN_EXE_horde"))
        .parent()
        .ok_or("the horde binary has a directory")?;
    assert_eq!(plan["steps"][2]["step"], "copy");
    assert_eq!(
        plan["steps"][2]["argv"][2],
        horde_dir.join("horde-worker").display().to_string(),
        "by default the worker binary beside this horde is copied"
    );
    assert_eq!(
        std::fs::read_dir(remote.home())?.count(),
        0,
        "nothing ran remotely"
    );

    // --build-locally builds into CARGO_TARGET_DIR, when it is set.
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_horde"))
        .args(["--json", "worker", "add", "box", "me@remote", "--dry-run"])
        .arg("--build-locally")
        .env("CARGO_TARGET_DIR", "/elsewhere")
        .output()
        .await?;
    let plan: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let copied = plan["steps"][2]["argv"][2].as_str().unwrap_or_default();
    assert!(
        copied.starts_with("/elsewhere/") && copied.ends_with("/release/horde-worker"),
        "{plan}"
    );
    assert_eq!(plan["build"][0], "cargo");
    Ok(())
}
