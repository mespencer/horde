//! The one way this module runs git: bounded, captured, and — for anything
//! that talks to a remote — unable to stop and ask for a credential.

use std::path::Path;
use std::time::Duration;

/// A ceiling on any single local git invocation. Generous because a first
/// clone of a real remote is slow; finite because a hung git inside the
/// command loop would otherwise stall every later command forever.
pub(crate) const GIT_TIMEOUT: Duration = Duration::from_secs(600);

/// How one invocation runs.
#[derive(Debug, Clone, Copy)]
pub(crate) struct GitRun {
    pub(crate) timeout: Duration,
    /// Set for invocations that reach a remote: `GIT_TERMINAL_PROMPT=0` and an
    /// empty `GIT_ASKPASS`, so a credential prompt fails the call instead of
    /// hanging it until the timeout (ADR-0014).
    pub(crate) no_prompt: bool,
}

impl GitRun {
    /// A local invocation under the generous ceiling.
    pub(crate) const LOCAL: Self = Self {
        timeout: GIT_TIMEOUT,
        no_prompt: false,
    };

    /// A remote invocation bounded by `timeout`.
    pub(crate) fn remote(timeout: Duration) -> Self {
        Self {
            timeout,
            no_prompt: true,
        }
    }
}

/// Runs one local git invocation to completion, capturing its output.
///
/// The error is a sentence: the failing subcommand and whatever git printed,
/// because "exit status 128" diagnoses nothing.
pub(crate) async fn git(cwd: &Path, arguments: &[&str]) -> Result<String, String> {
    git_with(cwd, arguments, GitRun::LOCAL).await
}

/// Runs one git invocation as `run` says.
pub(crate) async fn git_with(
    cwd: &Path,
    arguments: &[&str],
    run: GitRun,
) -> Result<String, String> {
    let mut command = tokio::process::Command::new("git");
    command
        .args(arguments)
        .current_dir(cwd)
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    if run.no_prompt {
        command
            .env("GIT_TERMINAL_PROMPT", "0")
            .env("GIT_ASKPASS", "");
    }
    let ran = tokio::time::timeout(run.timeout, command.output())
        .await
        .map_err(|_| {
            format!(
                "git {} did not finish within {:?}",
                arguments.join(" "),
                run.timeout
            )
        })?
        .map_err(|error| format!("git {} could not run: {error}", arguments.join(" ")))?;

    if ran.status.success() {
        Ok(String::from_utf8_lossy(&ran.stdout).into_owned())
    } else {
        Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&ran.stderr).trim()
        ))
    }
}

/// Whether a git predicate holds: `Ok(true)` on exit 0, `Ok(false)` on exit 1,
/// and an error for anything else — a predicate that could not be evaluated
/// is never read as either answer.
pub(crate) async fn git_check(cwd: &Path, arguments: &[&str]) -> Result<bool, String> {
    let ran = tokio::time::timeout(
        GIT_TIMEOUT,
        tokio::process::Command::new("git")
            .args(arguments)
            .current_dir(cwd)
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| format!("git {} did not finish", arguments.join(" ")))?
    .map_err(|error| format!("git {} could not run: {error}", arguments.join(" ")))?;
    match ran.status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(format!(
            "git {} failed: {}",
            arguments.join(" "),
            String::from_utf8_lossy(&ran.stderr).trim()
        )),
    }
}

/// The commit a ref names in `repo`, or `None` when it names none.
pub(crate) async fn commit_of(repo: &Path, reference: &str) -> Option<String> {
    let spec = format!("{reference}^{{commit}}");
    git(repo, &["rev-parse", "--verify", "--quiet", &spec])
        .await
        .ok()
        .map(|sha| sha.trim().to_owned())
        .filter(|sha| !sha.is_empty())
}

pub(crate) fn path_str(path: &Path) -> String {
    path.display().to_string()
}

/// A value git would read as an option. `--` protects positionals; values
/// riding an option, and refs, must be refused here.
pub(crate) fn is_option_shaped(value: &str) -> bool {
    value.starts_with('-')
}

/// A full 40-hex (SHA-1) or 64-hex (SHA-256) object name.
pub(crate) fn is_full_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_leading_dash_is_option_shaped() {
        assert!(is_option_shaped("--upload-pack=touch /tmp/x"));
        assert!(is_option_shaped("-b"));
        assert!(!is_option_shaped("https://example.com/a-b.git"));
        assert!(!is_option_shaped(""));
    }

    #[test]
    fn a_full_sha_is_forty_or_sixty_four_hex_digits() {
        assert!(is_full_sha(&"a".repeat(40)));
        assert!(is_full_sha(&"0f".repeat(32)));
        assert!(!is_full_sha(&"a".repeat(39)));
        assert!(!is_full_sha(&"a".repeat(41)));
        assert!(!is_full_sha(&format!("{}g", "a".repeat(39))));
        assert!(!is_full_sha("main"));
    }

    /// Exit 0 is yes and exit 1 is no.
    #[tokio::test]
    async fn a_predicate_answers_yes_and_no() {
        let dir = tempfile::tempdir().expect("tempdir");
        let run = |args: &[&str]| {
            let status = std::process::Command::new("git")
                .args(["-c", "user.name=h", "-c", "user.email=h@t"])
                .args(args)
                .current_dir(dir.path())
                .status()
                .expect("git runs");
            assert!(status.success(), "git {args:?}");
        };
        run(&["init", "-q", "-b", "main"]);
        run(&["commit", "-q", "--allow-empty", "-m", "one"]);
        run(&["commit", "-q", "--allow-empty", "-m", "two"]);
        let ancestor = ["merge-base", "--is-ancestor", "HEAD~1", "HEAD"];
        assert_eq!(git_check(dir.path(), &ancestor).await, Ok(true));
        let descendant = ["merge-base", "--is-ancestor", "HEAD", "HEAD~1"];
        assert_eq!(git_check(dir.path(), &descendant).await, Ok(false));
    }

    #[tokio::test]
    async fn a_predicate_that_cannot_run_is_an_error_not_an_answer() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Not a repository: git exits 128, which is neither yes nor no.
        let answer = git_check(dir.path(), &["merge-base", "--is-ancestor", "a", "b"]).await;
        assert!(answer.is_err(), "{answer:?}");
    }

    /// The environment git runs under, as `run` sets it: an alias that
    /// prints it.
    async fn environment_of(run: GitRun) -> Vec<String> {
        let dir = tempfile::tempdir().expect("tempdir");
        git_with(
            dir.path(),
            &["-c", "alias.environment=!env", "environment"],
            run,
        )
        .await
        .expect("the alias runs")
        .lines()
        .map(str::to_owned)
        .collect()
    }

    /// A remote invocation can never stop to ask for a credential; a local
    /// one runs in the caller's environment untouched.
    #[tokio::test]
    async fn only_a_remote_run_refuses_to_prompt() {
        let remote = environment_of(GitRun::remote(Duration::from_secs(30))).await;
        assert!(
            remote.iter().any(|line| line == "GIT_TERMINAL_PROMPT=0"),
            "{remote:?}"
        );
        assert!(
            remote.iter().any(|line| line == "GIT_ASKPASS="),
            "{remote:?}"
        );
        let local = environment_of(GitRun::LOCAL).await;
        assert!(
            !local
                .iter()
                .any(|line| line == "GIT_TERMINAL_PROMPT=0" || line == "GIT_ASKPASS="),
            "{local:?}"
        );
    }

    /// Whether the process `pid` is still running: gone, or a zombie
    /// awaiting its reaper, is not.
    fn running(pid: &str) -> bool {
        std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| !rest.starts_with('Z'))
        })
    }

    /// Dropping `invocation` before it finishes kills the git it started:
    /// the alias records git's pid (its shell's parent) and then outlives
    /// the drop.
    async fn git_dies_with_its_future<F>(dir: &Path, invocation: impl FnOnce(String) -> F)
    where
        F: std::future::Future,
    {
        let pid_file = dir.join("git.pid");
        let alias = format!(
            "alias.linger=!echo $PPID > {}; sleep 20",
            pid_file.display()
        );
        let pid = {
            let invocation = invocation(alias);
            tokio::pin!(invocation);
            let mut pid = None;
            let mut finished = false;
            for _ in 0..500 {
                tokio::select! {
                    _ = &mut invocation => {
                        finished = true;
                        break;
                    }
                    () = tokio::time::sleep(Duration::from_millis(20)) => {
                        pid = std::fs::read_to_string(&pid_file)
                            .ok()
                            .filter(|pid| pid.ends_with('\n'));
                        if pid.is_some() {
                            break;
                        }
                    }
                }
            }
            assert!(!finished, "the invocation finished before it was dropped");
            pid.expect("the alias recorded git's pid").trim().to_owned()
            // The invocation is dropped here, git still running under it.
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while running(&pid) && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!running(&pid), "git {pid} outlived its dropped future");
    }

    #[tokio::test]
    async fn a_dropped_run_kills_its_git() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cwd = dir.path().to_path_buf();
        git_dies_with_its_future(dir.path(), |alias| async move {
            git(&cwd, &["-c", &alias, "linger"]).await
        })
        .await;
    }

    #[tokio::test]
    async fn a_dropped_check_kills_its_git() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cwd = dir.path().to_path_buf();
        git_dies_with_its_future(dir.path(), |alias| async move {
            git_check(&cwd, &["-c", &alias, "linger"]).await
        })
        .await;
    }

    /// A remote that accepts the connection and never answers: the ceiling
    /// fires, and it reads as a failure naming itself, never as success with
    /// empty output.
    #[tokio::test]
    async fn a_remote_run_times_out_with_a_sentence() {
        let dir = tempfile::tempdir().expect("tempdir");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a port");
        let port = listener.local_addr().expect("an address").port();
        let silent = tokio::spawn(async move {
            let held = listener.accept().await;
            tokio::time::sleep(Duration::from_secs(30)).await;
            drop(held);
        });
        let url = format!("git://127.0.0.1:{port}/silent.git");
        let failed = git_with(
            dir.path(),
            &["ls-remote", "--", &url],
            GitRun::remote(Duration::from_millis(500)),
        )
        .await;
        silent.abort();
        let error = failed.expect_err("a silent remote never answers");
        assert!(error.contains("did not finish"), "{error}");
    }
}
