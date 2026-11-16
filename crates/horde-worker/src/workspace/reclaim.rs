//! Soft reclaim: run a repo's reclaim command in its checkout, and leave
//! the worktree, the branch and every commit in place (ADR-0014).
//!
//! Commands come only from the Definition or Server config, never from the
//! repo or an agent. The Worker cannot verify what an operator's command
//! deletes; it guarantees only that it runs in the repo, without a shell,
//! with a cleared environment, in its own process group, for a bounded time.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use horde_proto::v1::ReclaimPolicy;
use horde_proto::v1::workspace_cleanup::RepoAction;
use horde_proto::v1::workspace_cleanup::repo_action::Outcome;
use tokio::io::AsyncReadExt;

use crate::process_tree::{ProcessTree, Stop};

use super::policy::{CommandSpec, WorkspacePolicy};

/// How much of a command's combined output a `RepoAction` keeps.
pub const OUTPUT_TAIL_BYTES: usize = 4096;

/// How long a timed-out command's group has between SIGTERM and SIGKILL.
#[cfg(unix)]
const KILL_GRACE: Duration = Duration::from_secs(5);

/// How long a finished command's output pipes may stay open: a process it
/// left in the background can hold them forever, and a reclaim must end.
const DRAIN_GRACE: Duration = Duration::from_secs(2);

/// How long a size walk may run before its answer is left unset.
pub const SIZE_WALK_CAP: Duration = Duration::from_secs(30);

/// The environment a reclaim command — and the cargo guard ahead of it —
/// keeps; `CARGO_TARGET_DIR` is deliberately absent.
#[cfg(unix)]
const KEPT_ENVIRONMENT: &[&str] = &["PATH", "HOME", "LANG", "CARGO_HOME", "RUSTUP_HOME"];

/// The environment a reclaim command keeps on Windows: the Unix set, plus
/// what a Windows program cannot start without (`SystemRoot` above all —
/// sockets and the CRT fail without it) and where a user's profile and
/// temporary files are. `CARGO_TARGET_DIR` is deliberately absent.
#[cfg(windows)]
const KEPT_ENVIRONMENT: &[&str] = &[
    "PATH",
    "HOME",
    "LANG",
    "CARGO_HOME",
    "RUSTUP_HOME",
    "SystemRoot",
    "SystemDrive",
    "windir",
    "ComSpec",
    "PATHEXT",
    "USERPROFILE",
    "LOCALAPPDATA",
    "APPDATA",
    "TEMP",
    "TMP",
];

/// What a repo's reclaim resolves to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// Nothing runs, for this reason.
    Skip(String),
    /// This command runs; its output paths name what it reclaims.
    Run(CommandSpec),
}

/// Resolves a repo's reclaim: `disabled` skips, an explicit command runs,
/// and auto takes the first rule whose marker is at the repo root — the
/// built-in `Cargo.toml` rule guarded by `cargo metadata`, which also names
/// the target directory as the command's output.
pub async fn resolve(
    repo_dir: &Path,
    reclaim: Option<&ReclaimPolicy>,
    policy: &WorkspacePolicy,
) -> Resolved {
    match WorkspacePolicy::explicit_command(reclaim) {
        Err(reason) => return Resolved::Skip(reason),
        Ok(Some(command)) => return Resolved::Run(command),
        Ok(None) => {}
    }
    for rule in &policy.auto_rules {
        if !repo_dir.join(&rule.marker_file).is_file() {
            continue;
        }
        if !is_cargo(&rule.command) {
            return Resolved::Run(rule.command.clone());
        }
        return match cargo_target_inside(repo_dir, policy.reclaim_timeout).await {
            Ok(target) => Resolved::Run(CommandSpec {
                output_paths: vec![target],
                ..rule.command.clone()
            }),
            Err(reason) => Resolved::Skip(reason),
        };
    }
    Resolved::Skip("no reclaim command matches this repo".to_owned())
}

/// Whether a rule runs cargo, and so needs the target-directory guard.
fn is_cargo(command: &CommandSpec) -> bool {
    command.argv.first().is_some_and(|program| {
        Path::new(program).file_name().and_then(|n| n.to_str()) == Some("cargo")
    })
}

/// The part of `cargo metadata` the guard reads.
#[derive(serde::Deserialize)]
struct CargoMetadata {
    target_directory: String,
}

/// The cargo target directory relative to the repo, when it lies inside it.
///
/// `cargo clean` on a shared `CARGO_TARGET_DIR` or a `build.target-dir`
/// outside the checkout would wipe what other checkouts are using, so that
/// is a skip with the reason, never a blind clean. Cargo is asked in the
/// environment the clean itself would run in, and is bounded like it.
async fn cargo_target_inside(repo_dir: &Path, timeout: Duration) -> Result<String, String> {
    let ran = tokio::time::timeout(
        timeout,
        confined("cargo", repo_dir)
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--offline",
            ])
            .output(),
    )
    .await
    .map_err(|_| "could not locate target dir: cargo metadata timed out".to_owned())?
    .map_err(|error| format!("could not locate target dir: {error}"))?;
    if !ran.status.success() {
        return Err(format!(
            "could not locate target dir: {}",
            String::from_utf8_lossy(&ran.stderr).trim()
        ));
    }
    let metadata: CargoMetadata = serde_json::from_slice(&ran.stdout)
        .map_err(|error| format!("could not locate target dir: {error}"))?;
    let target = metadata.target_directory;
    relative_inside(repo_dir, Path::new(&target)).ok_or_else(|| {
        format!("the cargo target dir {target} is outside this checkout; cargo clean would wipe a shared directory")
    })
}

/// A command as reclaim runs one: in `cwd`, with only the kept
/// environment, no stdin, and killed if it is dropped unfinished.
fn confined(program: &str, cwd: &Path) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(program);
    command
        .current_dir(cwd)
        .env_clear()
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    for name in KEPT_ENVIRONMENT {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
}

/// `inner` relative to `outer`, when it lies strictly inside it.
fn relative_inside(outer: &Path, inner: &Path) -> Option<String> {
    let outer = std::fs::canonicalize(outer).ok()?;
    // The target dir may not exist yet; canonicalize its nearest parent.
    let inner = match std::fs::canonicalize(inner) {
        Ok(inner) => inner,
        Err(_) => std::fs::canonicalize(inner.parent()?)
            .ok()?
            .join(inner.file_name()?),
    };
    let relative = inner.strip_prefix(&outer).ok()?;
    let relative = relative.to_str()?.to_owned();
    (!relative.is_empty()).then_some(relative)
}

/// Runs one resolved reclaim in `repo_dir`, measuring what it freed.
pub async fn run(
    repo_name: &str,
    repo_dir: &Path,
    resolved: &Resolved,
    default_timeout: Duration,
) -> RepoAction {
    let command = match resolved {
        Resolved::Skip(reason) => {
            return RepoAction {
                repo_name: repo_name.to_owned(),
                outcome: Outcome::Skipped.into(),
                reason: reason.clone(),
                ..RepoAction::default()
            };
        }
        Resolved::Run(command) => command,
    };
    let before = dir_size(repo_dir).await;
    let timeout = command.timeout.unwrap_or(default_timeout);
    let ran = execute(&command.argv, repo_dir, timeout).await;
    let after = dir_size(repo_dir).await;
    let bytes_freed = freed(before, after);
    RepoAction {
        repo_name: repo_name.to_owned(),
        outcome: if ran.succeeded {
            Outcome::Done
        } else {
            Outcome::Failed
        }
        .into(),
        reason: ran.reason,
        argv: command.argv.clone(),
        exit_code: ran.exit_code,
        output_tail: ran.output_tail,
        bytes_freed,
        ..RepoAction::default()
    }
}

/// What a run freed: the size it shrank by, or 0 when either walk was
/// left unset.
fn freed(before: Option<u64>, after: Option<u64>) -> u64 {
    match (before, after) {
        (Some(before), Some(after)) => before.saturating_sub(after),
        _ => 0,
    }
}

/// What running a command came to.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ran {
    pub succeeded: bool,
    pub exit_code: i32,
    pub reason: String,
    pub output_tail: String,
}

/// Runs `argv` in `cwd` without a shell, bounded by `timeout`; on timeout
/// the whole process group gets SIGTERM, then SIGKILL after a grace (on
/// Windows its job is terminated at once — there is no SIGTERM to offer).
pub async fn execute(argv: &[String], cwd: &Path, timeout: Duration) -> Ran {
    let Some(program) = argv.first() else {
        return refused("an empty reclaim command");
    };
    if !is_runnable_program(program) {
        return refused(&format!(
            "{program:?} is neither a bare program name nor an absolute path"
        ));
    }
    let mut command = confined(program, cwd);
    command
        .args(&argv[1..])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    // Its own tree (a process group, or a job), killed on drop.
    let mut child = match ProcessTree::spawn(command) {
        Ok(child) => child,
        Err(error) => return refused(&format!("could not run {program}: {error}")),
    };
    let tail = Arc::new(Mutex::new(Vec::new()));
    let pipes = child.take_pipes();
    let readers = [
        pipes.stdout.map(|out| collect(out, Arc::clone(&tail))),
        pipes.stderr.map(|err| collect(err, Arc::clone(&tail))),
    ];
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => status.ok(),
        Err(_) => {
            terminate(&mut child).await;
            for reader in readers.into_iter().flatten() {
                reader.abort();
            }
            return Ran {
                succeeded: false,
                exit_code: -1,
                reason: format!("timed out after {timeout:?}; its process group was killed"),
                output_tail: tail_text(&tail),
            };
        }
    };
    let output_tail = drain(readers, &mut child, &tail).await;
    let exit_code = status.and_then(|status| status.code()).unwrap_or(-1);
    let succeeded = status.is_some_and(|status| status.success());
    Ran {
        succeeded,
        exit_code,
        reason: if succeeded {
            String::new()
        } else {
            format!("{program} exited with status {exit_code}")
        },
        output_tail,
    }
}

/// Waits for the output readers of a command that has exited, for at most
/// [`DRAIN_GRACE`]; a pipe still open past it belongs to a process the
/// command left behind, so its group is killed and the readers dropped.
async fn drain(
    readers: [Option<tokio::task::JoinHandle<()>>; 2],
    tree: &mut ProcessTree,
    tail: &Arc<Mutex<Vec<u8>>>,
) -> String {
    let mut readers: Vec<_> = readers.into_iter().flatten().collect();
    let drained = tokio::time::timeout(DRAIN_GRACE, async {
        for reader in &mut readers {
            let _ = reader.await;
        }
    })
    .await;
    if drained.is_err() {
        tree.signal(Stop::Kill);
        for reader in &readers {
            reader.abort();
        }
    }
    tail_text(tail)
}

/// SIGTERM to the group, a grace for the leader, then SIGKILL to the group
/// whatever the leader did — a member that ignores SIGTERM outlives a
/// leader that honours it, and still holds the group id — then the reap.
/// On Windows the job is terminated at once: a console-less command has no
/// SIGTERM.
async fn terminate(tree: &mut ProcessTree) {
    #[cfg(unix)]
    {
        tree.signal(Stop::Terminate);
        let _ = tokio::time::timeout(KILL_GRACE, tree.wait()).await;
    }
    tree.signal(Stop::Kill);
    let _ = tree.kill_root().await;
}

fn refused(reason: &str) -> Ran {
    Ran {
        succeeded: false,
        exit_code: -1,
        reason: reason.to_owned(),
        output_tail: String::new(),
    }
}

/// A bare program name (resolved on `PATH`) or an absolute path; a relative
/// path would run whatever the checkout — agent-written content — holds.
#[must_use]
pub fn is_runnable_program(program: &str) -> bool {
    let path = Path::new(program);
    path.is_absolute() || (!program.is_empty() && !program.contains('/') && !program.contains('\\'))
}

/// Reads one pipe into the shared tail, keeping only its last bytes.
fn collect<R>(mut pipe: R, tail: Arc<Mutex<Vec<u8>>>) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        // #[gamma::skip(literal.int_increment, reason = "the buffer's initial bytes are never read: only `buffer[..read]`, which `read` has just written, is used")]
        let mut buffer = [0_u8; 4096];
        while let Ok(read) = pipe.read(&mut buffer).await {
            if read == 0 {
                break;
            }
            let mut tail = tail.lock().unwrap_or_else(PoisonError::into_inner);
            tail.extend_from_slice(&buffer[..read]);
            let excess = tail.len().saturating_sub(OUTPUT_TAIL_BYTES);
            tail.drain(..excess);
        }
    })
}

fn tail_text(tail: &Arc<Mutex<Vec<u8>>>) -> String {
    String::from_utf8_lossy(&tail.lock().unwrap_or_else(PoisonError::into_inner)).into_owned()
}

/// The bytes under `path`, symlinks not followed, or `None` when the walk
/// could not finish within [`SIZE_WALK_CAP`] — unset rather than guessed.
pub async fn dir_size(path: &Path) -> Option<u64> {
    let path = path.to_path_buf();
    let deadline = std::time::Instant::now() + SIZE_WALK_CAP;
    tokio::task::spawn_blocking(move || walk_size(&path, deadline))
        .await
        .ok()
        .flatten()
}

/// The blocking walk behind [`dir_size`].
fn walk_size(root: &Path, deadline: std::time::Instant) -> Option<u64> {
    let mut total = 0_u64;
    let mut pending: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if deadline
            .saturating_duration_since(std::time::Instant::now())
            .is_zero()
        {
            return None;
        }
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&path) {
                pending.extend(entries.filter_map(|entry| entry.ok().map(|entry| entry.path())));
            }
        } else {
            total = total.saturating_add(metadata.len());
        }
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workspace::policy::cargo_rule;

    #[test]
    fn only_a_bare_name_or_an_absolute_path_may_run() {
        // An absolute program as this platform spells one: `/usr/bin/make`
        // is only rooted on Windows, where absolute needs a drive.
        #[cfg(unix)]
        const ABSOLUTE_MAKE: &str = "/usr/bin/make";
        #[cfg(windows)]
        const ABSOLUTE_MAKE: &str = r"C:\tools\make.exe";
        assert!(is_runnable_program("cargo"));
        assert!(is_runnable_program(ABSOLUTE_MAKE));
        assert!(!is_runnable_program("./clean.sh"));
        assert!(!is_runnable_program("scripts/clean"));
        assert!(!is_runnable_program(""));
    }

    #[test]
    fn the_cargo_guard_applies_to_cargo_by_any_path() {
        assert!(is_cargo(&cargo_rule().command));
        assert!(is_cargo(&CommandSpec {
            argv: vec!["/usr/local/bin/cargo".to_owned()],
            timeout: None,
            output_paths: Vec::new()
        }));
        assert!(!is_cargo(&CommandSpec {
            argv: vec!["make".to_owned()],
            timeout: None,
            output_paths: Vec::new()
        }));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_command_runs_in_its_directory_with_a_cleared_environment() {
        let dir = tempfile::tempdir().expect("tempdir");
        // SAFETY of the claim: a variable the Worker has is not passed on
        // unless it is on the kept list.
        let ran = execute(
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                "pwd; echo \"[$CARGO_TARGET_DIR]\"; env | wc -l".to_owned(),
            ],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(ran.succeeded, "{ran:?}");
        assert_eq!(ran.exit_code, 0);
        assert_eq!(ran.reason, "", "a success has no reason");
        let canonical = std::fs::canonicalize(dir.path()).expect("canonical");
        assert!(
            ran.output_tail.contains(&canonical.display().to_string()),
            "{ran:?}"
        );
        assert!(ran.output_tail.contains("[]"), "{ran:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_failing_command_reports_its_status_and_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ran = execute(
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                "echo boom >&2; exit 3".to_owned(),
            ],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(!ran.succeeded);
        assert_eq!(ran.exit_code, 3);
        assert_eq!(ran.reason, "sh exited with status 3");
        assert!(ran.reason.contains("status 3"), "{ran:?}");
        assert!(ran.output_tail.contains("boom"), "{ran:?}");
    }

    /// On Windows a timed-out command's job is terminated: the grandchild
    /// it started (a `ping` standing in for a build daemon) dies with it,
    /// and the command keeps the environment a Windows program needs.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_timed_out_command_is_killed_with_its_job() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("grandchild.pid");
        let script = format!(
            "$p = Start-Process -PassThru -WindowStyle Hidden ping.exe \
             -ArgumentList '-n','600','127.0.0.1'; \
             Set-Content -Path '{}' -Value $p.Id; Start-Sleep -Seconds 600",
            marker.display()
        );
        let ran = execute(
            &[
                "powershell.exe".to_owned(),
                "-NoProfile".to_owned(),
                "-Command".to_owned(),
                script,
            ],
            dir.path(),
            Duration::from_secs(15),
        )
        .await;
        assert!(!ran.succeeded);
        assert_eq!(ran.exit_code, -1, "a killed command has no exit status");
        assert!(ran.reason.contains("timed out"), "{ran:?}");
        let pid: u32 = std::fs::read_to_string(&marker)
            .expect("the command wrote its grandchild's pid")
            .trim()
            .parse()
            .expect("a pid");
        assert!(
            horde_server_testkit::windows::gone_soon(pid).await,
            "the grandchild {pid} survived its job's termination"
        );
    }

    /// On Windows a reclaim command runs in its directory and keeps
    /// `SystemRoot` through the cleared environment.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_windows_command_keeps_what_windows_needs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ran = execute(
            &[
                "cmd.exe".to_owned(),
                "/C".to_owned(),
                "cd & echo root=%SystemRoot%".to_owned(),
            ],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(ran.succeeded, "{ran:?}");
        assert!(!ran.output_tail.contains("root=%SystemRoot%"), "{ran:?}");
        let canonical = dir.path().canonicalize().expect("canonical");
        let shown = canonical.display().to_string();
        let shown = shown.trim_start_matches(r"\\?\");
        assert!(ran.output_tail.contains(shown), "{ran:?}");
    }

    #[tokio::test]
    async fn a_missing_or_relative_program_is_refused_without_running() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = execute(
            &["horde-no-such-program".to_owned()],
            dir.path(),
            Duration::from_secs(5),
        )
        .await;
        assert!(
            !missing.succeeded && missing.reason.contains("could not run"),
            "{missing:?}"
        );
        let relative = execute(&["./x".to_owned()], dir.path(), Duration::from_secs(5)).await;
        assert!(
            !relative.succeeded && relative.reason.contains("neither"),
            "{relative:?}"
        );
        let empty = execute(&[], dir.path(), Duration::from_secs(5)).await;
        assert!(
            !empty.succeeded && empty.reason.contains("empty"),
            "{empty:?}"
        );
        assert_eq!(empty.exit_code, -1, "a refusal has no exit status");
    }

    /// `sh -c script` in `dir` under a one-second timeout, which it outlives.
    async fn timed_out_sh(dir: &Path, script: String) -> Ran {
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir,
            Duration::from_secs(1),
        )
        .await;
        assert!(ran.reason.contains("timed out"), "{ran:?}");
        ran
    }

    /// A command past its timeout is killed with its whole group: a child it
    /// forked into the background dies too.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_timed_out_command_is_killed_with_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("child.pid");
        let script = format!("sleep 600 & echo $! > {}; wait", marker.display());
        let ran = timed_out_sh(dir.path(), script).await;
        assert!(!ran.succeeded);
        assert_eq!(ran.exit_code, -1, "a killed command has no exit status");
        // The background sleep was in the group: it is gone (or a zombie
        // already reparented and reaped).
        let pid = written_pid(&marker);
        assert!(
            gone_soon(pid).await,
            "the grandchild {pid} survived its group's kill"
        );
    }

    /// The pid a test's shell wrote to `marker`.
    #[cfg(unix)]
    fn written_pid(marker: &Path) -> i32 {
        std::fs::read_to_string(marker)
            .expect("the child wrote its pid")
            .trim()
            .parse()
            .expect("a pid")
    }

    /// Whether `pid` is gone, or only a zombie, within five seconds.
    #[cfg(unix)]
    async fn gone_soon(pid: i32) -> bool {
        for _ in 0..50 {
            let alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
                && !is_zombie(pid);
            if !alive {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[cfg(unix)]
    fn is_zombie(pid: i32) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
            .map(|stat| stat.split_whitespace().nth(2) == Some("Z"))
            .unwrap_or(false)
    }

    #[tokio::test]
    async fn a_skip_reports_its_reason_and_runs_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let action = run(
            "atlas",
            dir.path(),
            &Resolved::Skip("no reclaim command".to_owned()),
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(action.outcome, i32::from(Outcome::Skipped));
        assert_eq!(action.repo_name, "atlas");
        assert_eq!(action.reason, "no reclaim command");
        assert!(action.argv.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_run_measures_what_it_freed() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("build")).expect("dir");
        std::fs::write(dir.path().join("build/out.bin"), vec![0_u8; 10_000]).expect("file");
        std::fs::write(dir.path().join("keep.txt"), b"keep").expect("file");
        let resolved = Resolved::Run(CommandSpec {
            argv: vec!["rm".to_owned(), "-rf".to_owned(), "build".to_owned()],
            timeout: None,
            output_paths: vec!["build".to_owned()],
        });
        let action = run("atlas", dir.path(), &resolved, Duration::from_secs(10)).await;
        assert_eq!(action.outcome, i32::from(Outcome::Done), "{action:?}");
        assert_eq!(action.bytes_freed, 10_000);
        assert!(dir.path().join("keep.txt").exists());
    }

    /// A failed run records which repo, why, the status and the output.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_failed_run_records_its_repo_status_and_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let resolved = Resolved::Run(CommandSpec {
            argv: vec![
                "sh".to_owned(),
                "-c".to_owned(),
                "echo out; echo err >&2; exit 4".to_owned(),
            ],
            timeout: None,
            output_paths: Vec::new(),
        });
        let action = run("atlas", dir.path(), &resolved, Duration::from_secs(10)).await;
        assert_eq!(action.outcome, i32::from(Outcome::Failed), "{action:?}");
        assert_eq!(action.repo_name, "atlas");
        assert_eq!(action.reason, "sh exited with status 4");
        assert_eq!(action.exit_code, 4);
        assert!(
            action.output_tail.contains("out") && action.output_tail.contains("err"),
            "{action:?}"
        );
    }

    /// A command killed by a signal has no exit code: it reads as -1.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_command_killed_by_a_signal_reports_no_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), "kill -KILL $$".to_owned()],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(!ran.succeeded);
        assert_eq!(ran.exit_code, -1);
        assert_eq!(ran.reason, "sh exited with status -1");
    }

    /// Output on either stream, or both, is kept whole.
    #[cfg(unix)]
    #[tokio::test]
    async fn output_on_each_stream_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        for script in ["echo only-out", "echo only-err >&2", "echo a; echo b >&2"] {
            let ran = execute(
                &["sh".to_owned(), "-c".to_owned(), script.to_owned()],
                dir.path(),
                Duration::from_secs(10),
            )
            .await;
            assert!(ran.succeeded, "{ran:?}");
            let mut lines: Vec<&str> = ran.output_tail.lines().collect();
            lines.sort_unstable();
            let mut wanted: Vec<&str> = script
                .split("; ")
                .map(|part| part.trim_start_matches("echo ").trim_end_matches(" >&2"))
                .collect();
            wanted.sort_unstable();
            assert_eq!(lines, wanted, "{script}");
        }
    }

    /// A process the command left in the background holding its output
    /// open does not hold the reclaim: it ends shortly after the command,
    /// with what was written, and the leftover is killed with the group.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_background_process_holding_the_output_does_not_hold_the_reclaim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("child.pid");
        let script = format!("sleep 600 & echo $! > {}; echo done", marker.display());
        let started = std::time::Instant::now();
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir.path(),
            Duration::from_secs(60),
        )
        .await;
        assert!(started.elapsed() < Duration::from_secs(30), "{ran:?}");
        assert!(ran.succeeded, "{ran:?}");
        assert_eq!(ran.output_tail.trim(), "done");
        let pid = written_pid(&marker);
        assert!(gone_soon(pid).await, "the leftover {pid} survived");
    }

    #[test]
    fn a_walk_past_its_deadline_leaves_the_size_unset() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a"), vec![0_u8; 100]).expect("file");
        assert_eq!(walk_size(dir.path(), std::time::Instant::now()), None);
        assert_eq!(
            walk_size(
                dir.path(),
                std::time::Instant::now() + Duration::from_secs(60)
            ),
            Some(100)
        );
    }

    #[tokio::test]
    async fn auto_resolution_follows_markers_and_explicit_policies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let policy = WorkspacePolicy::default();
        assert_eq!(
            resolve(dir.path(), None, &policy).await,
            Resolved::Skip("no reclaim command matches this repo".to_owned())
        );
        let disabled = ReclaimPolicy {
            policy: Some(horde_proto::v1::reclaim_policy::Policy::Disabled(
                horde_proto::v1::reclaim_policy::Disabled {},
            )),
        };
        assert_eq!(
            resolve(dir.path(), Some(&disabled), &policy).await,
            Resolved::Skip("no reclaim command".to_owned())
        );
        let custom = WorkspacePolicy {
            auto_rules: vec![crate::workspace::policy::AutoRule {
                marker_file: "Makefile".to_owned(),
                command: CommandSpec {
                    argv: vec!["make".to_owned(), "clean".to_owned()],
                    timeout: None,
                    output_paths: vec!["build".to_owned()],
                },
            }],
            ..WorkspacePolicy::default()
        };
        std::fs::write(dir.path().join("Makefile"), b"clean:\n").expect("makefile");
        let Resolved::Run(command) = resolve(dir.path(), None, &custom).await else {
            panic!("the Makefile rule applies");
        };
        assert_eq!(command.argv, vec!["make", "clean"]);
    }

    #[test]
    fn a_path_inside_is_relative_and_one_outside_is_none() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("target")).expect("dir");
        assert_eq!(
            relative_inside(dir.path(), &dir.path().join("target")),
            Some("target".to_owned())
        );
        assert_eq!(
            relative_inside(dir.path(), &dir.path().join("not-yet")),
            Some("not-yet".to_owned()),
            "a target dir that does not exist yet still resolves"
        );
        let other = tempfile::tempdir().expect("tempdir");
        assert_eq!(relative_inside(dir.path(), other.path()), None);
        assert_eq!(
            relative_inside(dir.path(), dir.path()),
            None,
            "the repo itself is not inside it"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_size_walk_counts_files_and_not_symlink_targets() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a"), vec![0_u8; 100]).expect("file");
        std::fs::create_dir(dir.path().join("sub")).expect("dir");
        std::fs::write(dir.path().join("sub/b"), vec![0_u8; 50]).expect("file");
        let outside = tempfile::tempdir().expect("tempdir");
        std::fs::write(outside.path().join("big"), vec![0_u8; 10_000]).expect("file");
        std::os::unix::fs::symlink(outside.path(), dir.path().join("link")).expect("symlink");
        let size = dir_size(dir.path()).await.expect("a size");
        // The link itself is a few bytes; its target's 10 000 are not counted.
        assert!((150..1_000).contains(&size), "{size}");
    }
    /// The first rule whose marker is present applies, however many rules
    /// before it do not.
    #[tokio::test]
    async fn a_rule_without_its_marker_passes_to_the_next() {
        let dir = tempfile::tempdir().expect("tempdir");
        let rule = |marker: &str, program: &str| crate::workspace::policy::AutoRule {
            marker_file: marker.to_owned(),
            command: CommandSpec {
                argv: vec![program.to_owned()],
                timeout: None,
                output_paths: Vec::new(),
            },
        };
        let policy = WorkspacePolicy {
            auto_rules: vec![
                rule("absent.marker", "first"),
                rule("present.marker", "second"),
            ],
            ..WorkspacePolicy::default()
        };
        std::fs::write(dir.path().join("present.marker"), b"").expect("marker");
        let Resolved::Run(command) = resolve(dir.path(), None, &policy).await else {
            panic!("the second rule applies");
        };
        assert_eq!(command.argv, vec!["second"]);
    }

    fn cargo_crate(dir: &Path, manifest: &str) {
        std::fs::write(dir.join("Cargo.toml"), manifest).expect("manifest");
        std::fs::create_dir_all(dir.join("src")).expect("src");
        std::fs::write(dir.join("src/lib.rs"), b"").expect("lib");
    }

    /// The cargo rule cleans the target dir cargo names inside the repo.
    #[tokio::test]
    async fn the_cargo_rule_names_the_target_dir_inside_the_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        cargo_crate(
            dir.path(),
            "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        let Resolved::Run(command) = resolve(dir.path(), None, &WorkspacePolicy::default()).await
        else {
            panic!("the cargo rule applies");
        };
        assert_eq!(command.argv, vec!["cargo", "clean"]);
        assert_eq!(command.output_paths, vec!["target"]);
    }

    /// A manifest cargo cannot read skips with cargo's own words.
    #[tokio::test]
    async fn an_unreadable_manifest_skips_with_cargos_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        cargo_crate(dir.path(), "this is not toml [");
        let Resolved::Skip(reason) = resolve(dir.path(), None, &WorkspacePolicy::default()).await
        else {
            panic!("an unreadable manifest skips");
        };
        assert!(
            reason.starts_with("could not locate target dir: ") && reason.contains("Cargo.toml"),
            "{reason}"
        );
    }

    /// The guard is bounded by the reclaim timeout: past it, a skip.
    #[tokio::test]
    async fn the_cargo_guard_is_bounded_by_the_reclaim_timeout() {
        let dir = tempfile::tempdir().expect("tempdir");
        cargo_crate(
            dir.path(),
            "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        let policy = WorkspacePolicy {
            reclaim_timeout: Duration::ZERO,
            ..WorkspacePolicy::default()
        };
        assert_eq!(
            resolve(dir.path(), None, &policy).await,
            Resolved::Skip("could not locate target dir: cargo metadata timed out".to_owned())
        );
    }

    #[test]
    fn a_run_frees_what_it_shrank_by_or_nothing_when_unmeasured() {
        assert_eq!(freed(Some(10), Some(4)), 6);
        assert_eq!(freed(Some(4), Some(10)), 0, "a run that grew freed nothing");
        assert_eq!(freed(None, Some(4)), 0);
        assert_eq!(freed(Some(10), None), 0);
        assert_eq!(freed(None, None), 0);
    }

    /// Output past the tail's size keeps exactly its last bytes.
    #[tokio::test]
    async fn the_output_tail_keeps_exactly_the_last_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ran = execute(
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                "printf 'S%05000dE' 0".to_owned(),
            ],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(ran.succeeded, "{ran:?}");
        assert_eq!(ran.output_tail.len(), OUTPUT_TAIL_BYTES);
        assert!(ran.output_tail.ends_with("00E"), "the end is kept");
        assert!(!ran.output_tail.contains('S'), "the start is dropped");
    }

    /// A one-byte read is output like any other, not the end of it.
    #[tokio::test]
    async fn output_after_a_one_byte_read_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ran = execute(
            &[
                "sh".to_owned(),
                "-c".to_owned(),
                "printf x; sleep 0.3; printf yz".to_owned(),
            ],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert_eq!(ran.output_tail, "xyz", "{ran:?}");
    }

    /// The end of the output ends the read: a command that exits at once
    /// returns well inside the drain grace.
    #[tokio::test]
    async fn the_end_of_the_output_ends_the_reclaim_at_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let started = std::time::Instant::now();
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), "echo hi".to_owned()],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        assert!(ran.succeeded, "{ran:?}");
        assert!(
            started.elapsed() < DRAIN_GRACE,
            "took {:?}",
            started.elapsed()
        );
    }

    fn kill_now(pid: i32) {
        let _ = nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(pid),
            nix::sys::signal::Signal::SIGKILL,
        );
    }

    async fn written_pid_soon(marker: &Path) -> i32 {
        for _ in 0..100 {
            if let Ok(text) = std::fs::read_to_string(marker)
                && let Ok(pid) = text.trim().parse()
            {
                return pid;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no pid was written to {}", marker.display());
    }

    /// A reclaim dropped unfinished (its task cancelled) kills its command.
    #[tokio::test]
    async fn a_dropped_reclaim_kills_its_command() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("pid");
        let script = format!("echo $$ > {}; exec sleep 600", marker.display());
        let cwd = dir.path().to_path_buf();
        let task = tokio::spawn(async move {
            execute(
                &["sh".to_owned(), "-c".to_owned(), script],
                &cwd,
                Duration::from_secs(600),
            )
            .await
        });
        let pid = written_pid_soon(&marker).await;
        task.abort();
        let _ = task.await;
        let gone = gone_soon(pid).await;
        kill_now(pid);
        assert!(gone, "the command {pid} outlived its dropped reclaim");
    }

    /// A timed-out command gets SIGTERM first: a trap on it runs.
    #[tokio::test]
    async fn a_timed_out_command_is_asked_to_stop_first() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("termed");
        let script = format!(
            "trap 'echo termed > {}; exit 0' TERM; sleep 600 & wait",
            marker.display()
        );
        timed_out_sh(dir.path(), script).await;
        assert_eq!(
            std::fs::read_to_string(&marker)
                .map(|text| text.trim().to_owned())
                .ok(),
            Some("termed".to_owned())
        );
    }

    /// A group member that ignores SIGTERM does not outlive its leader, and
    /// a leader that honours SIGTERM at once ends the command promptly.
    #[tokio::test]
    async fn a_member_ignoring_sigterm_is_killed_with_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("pid");
        let script = format!(
            "sh -c 'trap \"\" TERM; echo $$ > {}; exec sleep 600' & wait",
            marker.display()
        );
        let started = std::time::Instant::now();
        timed_out_sh(dir.path(), script).await;
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the leader was asked first, not left to the grace: {:?}",
            started.elapsed()
        );
        let pid = written_pid_soon(&marker).await;
        let gone = gone_soon(pid).await;
        kill_now(pid);
        assert!(gone, "the TERM-ignoring {pid} outlived its group's kill");
    }

    /// Whether every task this test's runtime spawned is gone within two
    /// seconds.
    async fn no_task_left() -> bool {
        let metrics = tokio::runtime::Handle::current().metrics();
        for _ in 0..100 {
            if metrics.num_alive_tasks() == 0 {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// A process outside the command's group holding its output open is
    /// let go: the readers do not linger once the command timed out.
    #[tokio::test]
    async fn a_timed_out_commands_readers_do_not_outlive_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("pid");
        let script = format!(
            "setsid sh -c 'echo $$ > {}; exec sleep 600' & wait",
            marker.display()
        );
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir.path(),
            Duration::from_secs(1),
        )
        .await;
        let pid = written_pid_soon(&marker).await;
        let readers_gone = no_task_left().await;
        kill_now(pid);
        assert!(ran.reason.contains("timed out"), "{ran:?}");
        assert!(readers_gone, "a reader outlived the timed-out command");
    }

    /// The same past the drain grace, for a command that exited.
    #[tokio::test]
    async fn an_exited_commands_readers_do_not_outlive_the_drain() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("pid");
        let script = format!(
            "setsid sh -c 'echo $$ > {}; exec sleep 600' & echo done",
            marker.display()
        );
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir.path(),
            Duration::from_secs(60),
        )
        .await;
        let pid = written_pid_soon(&marker).await;
        let readers_gone = no_task_left().await;
        kill_now(pid);
        assert!(ran.succeeded, "{ran:?}");
        assert!(readers_gone, "a reader outlived the drain");
    }

    /// A background process that let go of the output is not killed: only
    /// one still holding it past the drain grace is.
    #[tokio::test]
    async fn a_background_process_that_released_the_output_is_left_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("pid");
        let script = format!("sleep 600 >/dev/null 2>&1 & echo $! > {}", marker.display());
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir.path(),
            Duration::from_secs(10),
        )
        .await;
        let pid = written_pid_soon(&marker).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        let alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
            && !is_zombie(pid);
        kill_now(pid);
        assert!(ran.succeeded, "{ran:?}");
        assert!(alive, "the released background process {pid} was killed");
    }
}
