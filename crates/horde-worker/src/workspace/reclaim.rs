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
use nix::sys::signal::Signal;
use tokio::io::AsyncReadExt;

use super::policy::{CommandSpec, WorkspacePolicy};

/// How much of a command's combined output a `RepoAction` keeps.
pub const OUTPUT_TAIL_BYTES: usize = 4096;

/// How long a timed-out command's group has between SIGTERM and SIGKILL.
const KILL_GRACE: Duration = Duration::from_secs(5);

/// How long a size walk may run before its answer is left unset.
pub const SIZE_WALK_CAP: Duration = Duration::from_secs(30);

/// The environment a reclaim command keeps; `CARGO_TARGET_DIR` is
/// deliberately absent.
const KEPT_ENVIRONMENT: &[&str] = &["PATH", "HOME", "LANG", "CARGO_HOME", "RUSTUP_HOME"];

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
        return match cargo_target_inside(repo_dir).await {
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

/// The cargo target directory relative to the repo, when it lies inside it.
///
/// `cargo clean` on a shared `CARGO_TARGET_DIR` or a `build.target-dir`
/// outside the checkout would wipe what other checkouts are using, so that
/// is a skip with the reason, never a blind clean.
async fn cargo_target_inside(repo_dir: &Path) -> Result<String, String> {
    let ran = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::process::Command::new("cargo")
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--offline",
            ])
            .current_dir(repo_dir)
            .env_remove("CARGO_TARGET_DIR")
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
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
    let metadata: serde_json::Value = serde_json::from_slice(&ran.stdout)
        .map_err(|error| format!("could not locate target dir: {error}"))?;
    let target = metadata
        .get("target_directory")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "could not locate target dir: cargo named none".to_owned())?;
    relative_inside(repo_dir, Path::new(target)).ok_or_else(|| {
        format!("the cargo target dir {target} is outside this checkout; cargo clean would wipe a shared directory")
    })
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
    let bytes_freed = match (before, after) {
        (Some(before), Some(after)) => before.saturating_sub(after),
        _ => 0,
    };
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

/// What running a command came to.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Ran {
    pub succeeded: bool,
    pub exit_code: i32,
    pub reason: String,
    pub output_tail: String,
}

/// Runs `argv` in `cwd` without a shell, bounded by `timeout`; on timeout
/// the whole process group gets SIGTERM, then SIGKILL after a grace.
pub async fn execute(argv: &[String], cwd: &Path, timeout: Duration) -> Ran {
    let Some(program) = argv.first() else {
        return refused("an empty reclaim command");
    };
    if !is_runnable_program(program) {
        return refused(&format!(
            "{program:?} is neither a bare program name nor an absolute path"
        ));
    }
    let mut command = tokio::process::Command::new(program);
    command
        .args(&argv[1..])
        .current_dir(cwd)
        .env_clear()
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0)
        .kill_on_drop(true);
    for name in KEPT_ENVIRONMENT {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => return refused(&format!("could not run {program}: {error}")),
    };
    let tail = Arc::new(Mutex::new(Vec::new()));
    let readers = [
        child
            .stdout
            .take()
            .map(|out| collect(out, Arc::clone(&tail))),
        child
            .stderr
            .take()
            .map(|err| collect(err, Arc::clone(&tail))),
    ];
    let leader = child.id();
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(status) => status.ok(),
        Err(_) => {
            terminate(leader, &mut child).await;
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
    for reader in readers.into_iter().flatten() {
        let _ = reader.await;
    }
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
        output_tail: tail_text(&tail),
    }
}

/// SIGTERM to the group, a grace, then SIGKILL, then the reap.
async fn terminate(leader: Option<u32>, child: &mut tokio::process::Child) {
    if let Some(leader) = leader {
        crate::process::signal_spawned_group(leader, Signal::SIGTERM);
        if tokio::time::timeout(KILL_GRACE, child.wait())
            .await
            .is_err()
        {
            crate::process::signal_spawned_group(leader, Signal::SIGKILL);
        }
    }
    let _ = child.kill().await;
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
        if std::time::Instant::now() > deadline {
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
        assert!(is_runnable_program("cargo"));
        assert!(is_runnable_program("/usr/bin/make"));
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
        let canonical = std::fs::canonicalize(dir.path()).expect("canonical");
        assert!(
            ran.output_tail.contains(&canonical.display().to_string()),
            "{ran:?}"
        );
        assert!(ran.output_tail.contains("[]"), "{ran:?}");
    }

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
        assert!(ran.reason.contains("status 3"), "{ran:?}");
        assert!(ran.output_tail.contains("boom"), "{ran:?}");
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
    }

    /// A command past its timeout is killed with its whole group: a child it
    /// forked into the background dies too.
    #[tokio::test]
    async fn a_timed_out_command_is_killed_with_its_group() {
        let dir = tempfile::tempdir().expect("tempdir");
        let marker = dir.path().join("child.pid");
        let script = format!("sleep 600 & echo $! > {}; wait", marker.display());
        let ran = execute(
            &["sh".to_owned(), "-c".to_owned(), script],
            dir.path(),
            Duration::from_secs(1),
        )
        .await;
        assert!(!ran.succeeded);
        assert!(ran.reason.contains("timed out"), "{ran:?}");
        let pid = std::fs::read_to_string(&marker).expect("the child wrote its pid");
        let pid: i32 = pid.trim().parse().expect("a pid");
        // The background sleep was in the group: it is gone (or a zombie
        // already reparented and reaped).
        let mut alive = true;
        for _ in 0..50 {
            alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
                && !is_zombie(pid);
            if !alive {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(!alive, "the grandchild {pid} survived its group's kill");
    }

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
        assert_eq!(action.reason, "no reclaim command");
        assert!(action.argv.is_empty());
    }

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
}
