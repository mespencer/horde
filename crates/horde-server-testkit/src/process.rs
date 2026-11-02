//! Real-process drivers for the multi-process suites.
//!
//! The end-to-end tier (docs/TESTING.md §2) runs `horde-server`,
//! `horde-worker` and `horde` as separate OS processes, and the fault tier
//! kills and restarts them mid-scenario. Both need the same three abilities —
//! find a product binary, watch a live process's stdout for an operator line,
//! and end a process deliberately (SIGTERM for a clean shutdown, SIGKILL for
//! a crash) — so they live here rather than in two drifting copies.
//!
//! # Why the watching is polling, not streaming
//!
//! A spawned daemon writes operator lines at its own pace, and a suite's only
//! question is "has line X appeared yet". Reader tasks append everything into
//! one buffer and the wait polls it under a ceiling; the buffer also makes
//! post-mortem assertions ("stderr named the refusal") free, because nothing
//! was consumed to answer the first question.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::AsyncBufReadExt;

use crate::Setup;

/// How long any single wait against a real process may take. Generous
/// against a loaded machine; a passing run never spends it.
pub const CEILING: std::time::Duration = std::time::Duration::from_secs(30);

/// Where a compiled binary may be, given the running test executable.
///
/// Integration tests run from `target/<profile>/deps/…`, so the profile
/// directory is the parent (skipping `deps`), and product binaries land
/// directly in it. The `debug` directory is the second candidate because a
/// suite running under another profile may have built its products with a
/// plain `cargo build`.
#[must_use]
pub fn binary_candidates(test_executable: &Path, name: &str) -> Vec<PathBuf> {
    let mut profile_dir = test_executable.to_path_buf();
    profile_dir.pop();
    if profile_dir.ends_with("deps") {
        profile_dir.pop();
    }
    let mut candidates = vec![profile_dir.join(name)];
    if let Some(target_root) = profile_dir.parent() {
        let debug = target_root.join("debug").join(name);
        if !candidates.contains(&debug) {
            candidates.push(debug);
        }
    }
    candidates
}

/// Serializes fallback builds: two suites racing `cargo build` would fight
/// over the target-directory lock and one would report the other's wait.
static BUILD_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// The path of product binary `name` from package `package`, building it if
/// no compiled copy exists yet.
///
/// A whole-workspace test run has already built every product binary, so the
/// fallback build only runs for a narrowly-scoped invocation (`cargo test -p
/// horde-e2e`), where failing for a missing sibling binary would punish
/// exactly the person trying to run the suite.
///
/// # Errors
///
/// The build failing, or the binary still missing after it.
pub async fn product_binary(package: &str, name: &str) -> Setup<PathBuf> {
    let candidates = binary_candidates(&std::env::current_exe()?, name);
    match existing(&candidates) {
        Some(found) => Ok(found),
        None => built(package, name, &candidates).await,
    }
}

/// The first candidate that is a real file.
fn existing(candidates: &[PathBuf]) -> Option<PathBuf> {
    candidates
        .iter()
        .find(|candidate| candidate.is_file())
        .cloned()
}

/// Builds the binary under the lock and locates it.
async fn built(package: &str, name: &str, candidates: &[PathBuf]) -> Setup<PathBuf> {
    let _build = BUILD_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    // Re-checked under the lock: the build another suite just finished is
    // this suite's binary too.
    if let Some(found) = existing(candidates) {
        return Ok(found);
    }
    // `--offline` deliberately: a workspace that runs this suite has been
    // built before, so every dependency is already local — and the registry
    // lock an online build takes is contended machine-wide (a cargo-mutants
    // sweep at --jobs 4 held this exact wait long enough to turn a caught
    // mutant into a timeout).
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(600),
        tokio::process::Command::new("cargo")
            .args(["build", "--offline", "--package", package, "--bin", name])
            .output(),
    )
    .await
    .map_err(|_| format!("building {name} from {package} timed out"))??;
    build_verdict(package, name, output.status.success(), &output.stderr)?;
    existing(candidates)
        .ok_or_else(|| format!("{name} was built but not found beside the test binary").into())
}

/// Turns a finished `cargo build` into a verdict, carrying the compiler's
/// own words on failure — "the build failed" names nothing anyone can fix.
fn build_verdict(package: &str, name: &str, success: bool, stderr: &[u8]) -> Setup<()> {
    if success {
        return Ok(());
    }
    Err(format!(
        "cargo build -p {package} --bin {name} failed:\n{}",
        String::from_utf8_lossy(stderr)
    )
    .into())
}

/// Runs one git command in `dir`, returning stdout, with identity and
/// default-branch pinned so no machine config leaks into a test.
///
/// The suites' own git, for arranging repos and reading results — the
/// production git runs live inside `horde-relay` and the worker. Shared here
/// because the payload suites on both sides of the process boundary arrange
/// repos the same way, and two copies of a git driver drift.
///
/// # Errors
///
/// The command failing, with git's own stderr.
pub async fn git(dir: &Path, args: &[&str]) -> Setup<String> {
    let mut command = tokio::process::Command::new("git");
    command.current_dir(dir);
    for pin in [
        "user.email=test@horde",
        "user.name=horde-test",
        "init.defaultBranch=main",
        // A commit otherwise leaves a detached `git maintenance run --auto`
        // churning `objects/` after it returns, racing the clone a suite
        // takes next.
        "maintenance.auto=false",
        "gc.auto=0",
    ] {
        command.arg("-c").arg(pin);
    }
    let output = command.args(args).output().await?;
    if !output.status.success() {
        return Err(format!(
            "git {args:?} in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Whether `repo` holds `commit_sha` as a commit — the observable fact the
/// payload suites assert after a share travelled (or refuse to find after
/// one must not have).
pub async fn holds_commit(repo: &Path, commit_sha: &str) -> bool {
    tokio::process::Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["cat-file", "-e", &format!("{commit_sha}^{{commit}}")])
        .status()
        .await
        .is_ok_and(|status| status.success())
}

/// The remainder of the first line of `text` that starts with `key`.
///
/// The operator-line convention every binary shares (`listening main=…`,
/// `registered worker=…`) makes this the whole parser a suite needs.
#[must_use]
pub fn value_after<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(key))
        .map(str::trim)
}

/// A live product process with its output watched.
#[derive(Debug)]
pub struct SpawnedProcess {
    name: String,
    child: tokio::process::Child,
    stdout: Arc<Mutex<String>>,
    stderr: Arc<Mutex<String>>,
}

/// Appends every line of `source` into `buffer` until EOF.
fn watch_lines<R>(source: R, buffer: Arc<Mutex<String>>)
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = tokio::io::BufReader::new(source).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if let Ok(mut buffer) = buffer.lock() {
                buffer.push_str(&line);
                buffer.push('\n');
            }
        }
    });
}

impl SpawnedProcess {
    /// Spawns `program` with `args` and `envs`, watching both output streams.
    ///
    /// `name` labels every failure this handle reports. The child is
    /// kill-on-drop, its descendants with it: a panicking test must not
    /// leak a live daemon. The first spawn in each test process also reaps
    /// the orphans an earlier, killed test process left behind
    /// ([`crate::reap`]).
    ///
    /// # Errors
    ///
    /// The spawn failing, or a pipe missing.
    pub fn spawn(name: &str, program: &Path, args: &[&str], envs: &[(&str, &str)]) -> Setup<Self> {
        crate::reap::reap_orphans_once();
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        for (key, value) in envs {
            command.env(key, value);
        }
        let mut child = command.spawn()?;

        let stdout = Arc::new(Mutex::new(String::new()));
        let stderr = Arc::new(Mutex::new(String::new()));
        let out = child
            .stdout
            .take()
            .ok_or_else(|| format!("{name} has no stdout pipe"))?;
        let err = child
            .stderr
            .take()
            .ok_or_else(|| format!("{name} has no stderr pipe"))?;
        watch_lines(out, Arc::clone(&stdout));
        watch_lines(err, Arc::clone(&stderr));

        Ok(Self {
            name: name.to_owned(),
            child,
            stdout,
            stderr,
        })
    }

    /// Everything the process has written to stdout so far.
    #[must_use]
    pub fn stdout_so_far(&self) -> String {
        self.stdout.lock().map(|b| b.clone()).unwrap_or_default()
    }

    /// Everything the process has written to stderr so far.
    #[must_use]
    pub fn stderr_so_far(&self) -> String {
        self.stderr.lock().map(|b| b.clone()).unwrap_or_default()
    }

    /// Waits until stdout satisfies `holds`, returning the snapshot that did.
    ///
    /// Bounded by [`CEILING`]; the failure carries both streams, because a
    /// process that never said the line usually said *why* on stderr.
    ///
    /// # Errors
    ///
    /// The ceiling elapsing, named with `what`.
    pub async fn await_stdout(&self, what: &str, holds: impl Fn(&str) -> bool) -> Setup<String> {
        let deadline = tokio::time::Instant::now() + CEILING;
        loop {
            let snapshot = self.stdout_so_far();
            if holds(&snapshot) {
                return Ok(snapshot);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "{} never reached: {what}\n--- stdout ---\n{snapshot}\n--- stderr ---\n{}",
                    self.name,
                    self.stderr_so_far()
                )
                .into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// Sends SIGTERM — the clean-shutdown request a supervisor would send.
    ///
    /// # Errors
    ///
    /// The process id being gone, or the signal failing to send.
    pub async fn terminate(&mut self) -> Setup<()> {
        let pid = self
            .child
            .id()
            .ok_or_else(|| format!("{} has already exited", self.name))?;
        let status = tokio::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .await?;
        if !status.success() {
            return Err(format!("kill -TERM {pid} failed for {}", self.name).into());
        }
        Ok(())
    }

    /// Kills the process without ceremony — the crash the fault tier injects.
    ///
    /// # Errors
    ///
    /// The kill failing.
    pub async fn kill(&mut self) -> Setup<()> {
        self.child.kill().await?;
        Ok(())
    }

    /// Waits for the process to exit, bounded by [`CEILING`], and returns its
    /// status with both captured streams.
    ///
    /// # Errors
    ///
    /// The ceiling elapsing — a daemon asked to stop that will not is a
    /// worse finding than any assertion downstream of it.
    pub async fn wait(mut self) -> Setup<(std::process::ExitStatus, String, String)> {
        let status = tokio::time::timeout(CEILING, self.child.wait())
            .await
            .map_err(|_| {
                format!(
                    "{} did not exit within {CEILING:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
                    self.name,
                    self.stdout_so_far(),
                    self.stderr_so_far()
                )
            })??;
        // The reader tasks may still be flushing the final lines; give them
        // one bounded beat so the captured streams are complete.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        Ok((status, self.stdout_so_far(), self.stderr_so_far()))
    }
}

impl Drop for SpawnedProcess {
    /// Kills the child's whole tree before kill-on-drop takes the child: a
    /// dropped `horde up` must not leave its server and worker to init.
    fn drop(&mut self) {
        if let Some(pid) = self.child.id() {
            crate::reap::reap_descendants(pid);
        }
    }
}

/// How long the OS may take to admit one freshly linked binary. An OS
/// bound, deliberately distinct from every scenario's own wait ceiling: the
/// cost it bounds is real and was measured — macOS validates a freshly
/// linked binary on its first-ever exec (code-signature and XProtect
/// assessment, cached per file afterwards), 327ms against 8ms warm on an
/// idle machine and multi-second when the validation service is contended
/// by concurrent builds minting new binaries, exactly what a cargo-mutants
/// run does.
const FIRST_LAUNCH_CEILING: std::time::Duration = std::time::Duration::from_secs(60);

/// Pays a binary's first-exec cost once per test process, before any
/// scenario timing exists to eat it.
///
/// Every cargo-mutants scratch tree links its binaries brand-new, so a
/// suite that spawns them inside ceilinged waits pays the OS's validation
/// *inside* those ceilings — the mechanism that failed the hub e2e baseline
/// first and the lifecycle baseline after it, each in its own copy of this
/// fix until it earned one home. Warming is serialized by one lock and
/// deduplicated per binary path: the first launch pays, the rest find the
/// OS's per-file cache.
///
/// `args` is the probe that makes the binary exit promptly — `--help` for
/// the product binaries, nothing for stubs that exit on stdin's EOF. The
/// exit status is deliberately ignored: that the process *executed* is the
/// whole payment.
///
/// # Errors
///
/// The binary failing to execute at all, or the OS not admitting it within
/// [`FIRST_LAUNCH_CEILING`] — both named, neither a hang.
pub async fn admit_binary(binary: &Path, args: &[&str]) -> Setup<()> {
    let binary = binary.to_path_buf();
    let args: Vec<String> = args.iter().map(|&arg| arg.to_owned()).collect();
    tokio::task::spawn_blocking(move || {
        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        admit_binary_blocking(&binary, &borrowed)
    })
    .await?
}

/// [`admit_binary`] for a caller without an async context — the worker
/// suite's synchronous binary locators run before any adapter exists to
/// await anything. Same lock, same per-binary dedup, same admission bound;
/// the async form delegates here through `spawn_blocking`, so the two entry
/// points cannot drift into two mechanisms.
///
/// # Errors
///
/// The binary failing to execute at all, or the OS not admitting it within
/// [`FIRST_LAUNCH_CEILING`] — both named, neither a hang.
pub fn admit_binary_blocking(binary: &Path, args: &[&str]) -> Setup<()> {
    use std::collections::BTreeSet;
    static WARMED: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
    let mut warmed = WARMED
        .get_or_init(|| Mutex::new(BTreeSet::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if warmed.contains(binary) {
        return Ok(());
    }
    let mut probe = std::process::Command::new(binary)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    let deadline = std::time::Instant::now() + FIRST_LAUNCH_CEILING;
    loop {
        if let Some(_exit_of_a_probe) = probe.try_wait()? {
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _kill_a_stuck_probe = probe.kill();
            return Err(format!(
                "{}'s first launch did not finish within {FIRST_LAUNCH_CEILING:?}; the OS \
                 admits a freshly built binary only after validating it, and that validation \
                 is starved or stuck",
                binary.display()
            )
            .into());
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    warmed.insert(binary.to_path_buf());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{SpawnedProcess, admit_binary, binary_candidates, value_after};

    /// The admission, observed through its probe's own side effect: a
    /// `touch` probe leaves its file, so a warm-up hollowed to nothing
    /// leaves none — and the second call on the same binary is deduplicated,
    /// so its differently-named file must never appear.
    #[tokio::test]
    async fn admission_executes_the_probe_once_per_binary() {
        let scratch = tempfile::tempdir().expect("a scratch directory");
        let first = scratch.path().join("first-admission");
        let second = scratch.path().join("second-admission");
        let touch = std::path::Path::new("/usr/bin/touch");

        admit_binary(touch, &[&first.to_string_lossy()])
            .await
            .expect("the probe executes");
        assert!(
            first.is_file(),
            "the admission must actually run the binary"
        );

        admit_binary(touch, &[&second.to_string_lossy()])
            .await
            .expect("a deduplicated admission still answers Ok");
        assert!(
            !second.is_file(),
            "one payment per binary: the second call must not execute"
        );
    }

    /// A binary that cannot execute is a named error, not a hang.
    #[tokio::test]
    async fn an_unexecutable_binary_refuses_admission() {
        let missing = std::path::Path::new("/nonexistent/never-a-binary");
        assert!(
            admit_binary(missing, &[]).await.is_err(),
            "executing nothing is not a payment"
        );
    }

    #[test]
    fn candidates_cover_the_profile_dir_and_the_debug_fallback() {
        let exe = std::path::Path::new("/w/target/mutants/deps/suite-abc123");
        let candidates = binary_candidates(exe, "horde-server");
        assert_eq!(
            candidates,
            vec![
                std::path::PathBuf::from("/w/target/mutants/horde-server"),
                std::path::PathBuf::from("/w/target/debug/horde-server"),
            ]
        );

        // Under the debug profile the two candidates coincide and the list
        // must not repeat itself — a duplicated path would double every
        // existence probe's failure message.
        let exe = std::path::Path::new("/w/target/debug/deps/suite-abc123");
        assert_eq!(
            binary_candidates(exe, "horde"),
            vec![std::path::PathBuf::from("/w/target/debug/horde")]
        );
    }

    /// The build verdict's truth table: success is silent, failure carries
    /// the compiler's words and the target's name.
    #[test]
    fn the_build_verdict_carries_the_compilers_own_words() {
        assert!(super::build_verdict("horde-cli", "horde", true, b"ignored").is_ok());
        let refused = super::build_verdict("horde-cli", "horde", false, b"E0599: nope")
            .expect_err("a failed build is a failure");
        let rendered = refused.to_string();
        assert!(
            rendered.contains("horde-cli") && rendered.contains("E0599"),
            "the failure names the package and quotes the compiler: {rendered}"
        );
    }

    /// The fast path: a binary that already exists is returned without any
    /// build. The package name is deliberately unbuildable, so reaching the
    /// fallback would fail loudly instead of quietly making the test slow.
    #[tokio::test]
    async fn an_existing_binary_is_found_without_building() {
        let exe = std::env::current_exe().expect("a test executable");
        let name = format!("horde-testkit-probe-{}", std::process::id());
        let candidates = super::binary_candidates(&exe, &name);
        let planted = candidates.first().expect("at least one candidate");
        std::fs::write(planted, b"not really a binary").expect("the probe file writes");

        let found = super::product_binary("horde-no-such-package", &name).await;
        let removed = std::fs::remove_file(planted);
        assert_eq!(
            found.expect("an existing binary needs no build"),
            *planted,
            "the compiled copy is what comes back"
        );
        removed.expect("the probe file cleans up");
    }

    /// The fallback path, driven to its honest failure: a package that does
    /// not exist cannot build, and the error must say so rather than time
    /// out or report a missing file.
    #[tokio::test]
    async fn a_missing_package_fails_the_fallback_build_loudly() {
        let error = super::product_binary("horde-no-such-package", "no-such-bin")
            .await
            .expect_err("an unbuildable package must fail");
        assert!(
            error.to_string().contains("horde-no-such-package"),
            "the failure names what could not build: {error}"
        );
    }

    /// The git driver and the commit probe, over a real repository: a made
    /// commit is held, an unmade one is not, and a failing git command
    /// reports git's own words.
    #[tokio::test]
    async fn git_runs_and_the_commit_probe_tells_present_from_absent() {
        let dir = tempfile::tempdir().expect("a temp dir");
        super::git(dir.path(), &["init"]).await.expect("init");
        std::fs::write(dir.path().join("a.txt"), "a\n").expect("a file");
        super::git(dir.path(), &["add", "."]).await.expect("add");
        super::git(dir.path(), &["commit", "-m", "seed"])
            .await
            .expect("commit");
        let sha = super::git(dir.path(), &["rev-parse", "HEAD"])
            .await
            .expect("rev-parse");

        assert!(super::holds_commit(dir.path(), &sha).await);
        assert!(
            !super::holds_commit(dir.path(), &"0".repeat(40)).await,
            "an unmade commit must not be reported present"
        );

        let refused = super::git(dir.path(), &["rev-parse", "not-a-ref"])
            .await
            .expect_err("a failing git command is an error");
        assert!(
            refused.to_string().contains("not-a-ref"),
            "the failure quotes what was asked: {refused}"
        );
    }

    #[test]
    fn value_after_reads_the_operator_line_convention() {
        let stdout = "listening main=127.0.0.1:7777\nserver-fingerprint abc\n";
        assert_eq!(
            value_after(stdout, "listening main="),
            Some("127.0.0.1:7777")
        );
        assert_eq!(value_after(stdout, "server-fingerprint "), Some("abc"));
        assert_eq!(value_after(stdout, "bootstrap-client-code "), None);
    }

    /// The whole handle against a real (non-product) process: output arrives
    /// through the watcher, and a clean wait reports the real exit status.
    #[tokio::test]
    async fn a_spawned_process_is_watched_and_reaped() {
        let mut process = SpawnedProcess::spawn(
            "echoer",
            std::path::Path::new("/bin/sh"),
            &["-c", "echo ready marker=42; echo oops >&2"],
            &[],
        )
        .expect("sh spawns");

        let seen = process
            .await_stdout("the marker line", |out| out.contains("marker=42"))
            .await
            .expect("the marker must arrive");
        assert_eq!(value_after(&seen, "ready marker="), Some("42"));

        let terminate_a_finished_process = process.terminate().await;
        drop(terminate_a_finished_process); // it may have exited already; either is fine

        let (status, stdout, stderr) = process.wait().await.expect("sh exits");
        assert!(status.success());
        assert!(stdout.contains("marker=42"));
        assert!(stderr.contains("oops"), "stderr is captured too: {stderr}");
    }

    /// SIGTERM actually ends a process that ignores nothing — the lever the
    /// clean-shutdown scenarios pull must be shown to work by itself.
    #[tokio::test]
    async fn terminate_ends_a_parked_process() {
        let mut process = SpawnedProcess::spawn(
            "sleeper",
            std::path::Path::new("/bin/sh"),
            &["-c", "echo up; sleep 600"],
            &[],
        )
        .expect("sh spawns");
        process
            .await_stdout("the up line", |out| out.contains("up"))
            .await
            .expect("the process is running");

        process.terminate().await.expect("SIGTERM sends");
        let (status, _, _) = process.wait().await.expect("the sleep dies");
        assert!(
            !status.success(),
            "a SIGTERM'd shell reports the signal, not success"
        );
    }
}
