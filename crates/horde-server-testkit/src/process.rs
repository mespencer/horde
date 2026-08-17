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
    let current = std::env::current_exe()?;
    let candidates = binary_candidates(&current, name);
    if let Some(found) = candidates.iter().find(|candidate| candidate.is_file()) {
        return Ok(found.clone());
    }

    let _build = BUILD_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;
    // Re-checked under the lock: the build another suite just finished is
    // this suite's binary too.
    if let Some(found) = candidates.iter().find(|candidate| candidate.is_file()) {
        return Ok(found.clone());
    }
    let output = tokio::time::timeout(
        std::time::Duration::from_secs(600),
        tokio::process::Command::new("cargo")
            .args(["build", "--package", package, "--bin", name])
            .output(),
    )
    .await
    .map_err(|_| format!("building {name} from {package} timed out"))??;
    if !output.status.success() {
        return Err(format!(
            "cargo build -p {package} --bin {name} failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    candidates
        .into_iter()
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| format!("{name} was built but not found beside the test binary").into())
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
    /// kill-on-drop: a panicking test must not leak a live daemon.
    ///
    /// # Errors
    ///
    /// The spawn failing, or a pipe missing.
    pub fn spawn(name: &str, program: &Path, args: &[&str], envs: &[(&str, &str)]) -> Setup<Self> {
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

#[cfg(test)]
mod tests {
    use super::{SpawnedProcess, binary_candidates, value_after};

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
