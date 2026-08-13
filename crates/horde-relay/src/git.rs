//! Running real git, once, for the whole crate.
//!
//! Everything above this module speaks in paths and commit ids; this is the
//! only place a `git` process is spawned. Keeping the spawn in one function is
//! what makes the crate's promises checkable: an error always carries the
//! command that failed and what git said on stderr, and no caller can quietly
//! swallow a non-zero exit by forgetting to look at it.

use std::path::Path;
use std::process::Stdio;

/// Why a git operation did not do what it said.
#[derive(Debug, thiserror::Error)]
pub enum GitError {
    /// The `git` binary could not be started at all.
    #[error("git could not be spawned for `git {command}`: {source}")]
    Spawn {
        command: String,
        #[source]
        source: std::io::Error,
    },

    /// git ran and refused. Carries git's own words, because "fetch failed"
    /// without the stderr is a diagnosis nobody can act on.
    #[error("`git {command}` in {dir} failed{status}: {stderr}")]
    Failed {
        command: String,
        dir: String,
        /// Rendered exit status, e.g. " (exit 128)". Empty when killed by a
        /// signal, which has no code to render.
        status: String,
        stderr: String,
    },
}

/// One finished git invocation, verdict not yet applied.
pub(crate) struct GitOutcome {
    pub(crate) code: Option<i32>,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

/// Runs `git` in `dir` and waits for it, with stdin closed.
///
/// Only spawn failures are errors here; the exit code is the caller's to
/// judge, because "non-zero" is a verdict (`git`), a probe answer
/// ([`check`]), or a detached-HEAD signal, depending on who is asking.
pub(crate) async fn raw(dir: &Path, args: &[&str]) -> Result<GitOutcome, GitError> {
    let output = tokio::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .map_err(|source| GitError::Spawn {
            command: args.join(" "),
            source,
        })?;

    Ok(GitOutcome {
        code: output.status.code(),
        stdout: output.stdout,
        stderr: output.stderr,
    })
}

/// The `Failed` error for `outcome`, with everything a reader needs.
fn failed(dir: &Path, args: &[&str], outcome: &GitOutcome) -> GitError {
    GitError::Failed {
        command: args.join(" "),
        dir: dir.display().to_string(),
        status: outcome
            .code
            .map(|code| format!(" (exit {code})"))
            .unwrap_or_default(),
        stderr: String::from_utf8_lossy(&outcome.stderr).trim().to_owned(),
    }
}

/// Runs `git` in `dir`, requiring success; returns trimmed stdout.
///
/// # Errors
///
/// [`GitError::Spawn`] if git could not start, [`GitError::Failed`] with
/// git's stderr on any non-zero exit.
pub(crate) async fn git(dir: &Path, args: &[&str]) -> Result<String, GitError> {
    let outcome = raw(dir, args).await?;
    if outcome.code == Some(0) {
        Ok(String::from_utf8_lossy(&outcome.stdout).trim().to_owned())
    } else {
        Err(failed(dir, args, &outcome))
    }
}

/// Runs a yes/no probe: exit 0 is `true`, exit 1 is `false`.
///
/// The shape of `git cat-file -e` and `git symbolic-ref -q`: both reserve
/// exit 1 for "no", and anything else means the question itself failed —
/// which must stay an error, because reading "the repo is broken" as "no"
/// is how a probe becomes a liar.
///
/// # Errors
///
/// [`GitError::Spawn`] or [`GitError::Failed`] when git exited with anything
/// other than 0 or 1.
pub(crate) async fn check(dir: &Path, args: &[&str]) -> Result<bool, GitError> {
    let outcome = raw(dir, args).await?;
    match outcome.code {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(failed(dir, args, &outcome)),
    }
}

#[cfg(test)]
mod tests {
    use super::{GitError, GitOutcome, failed};
    use std::path::Path;

    /// The error must carry the command, the directory, the exit, and stderr —
    /// each is the difference between a diagnosis and a shrug.
    #[test]
    fn a_failure_reports_command_dir_exit_and_stderr() {
        let error = failed(
            Path::new("/work/repo"),
            &["fetch", "peer"],
            &GitOutcome {
                code: Some(128),
                stdout: Vec::new(),
                stderr: b"fatal: not a repository\n".to_vec(),
            },
        );
        let rendered = error.to_string();
        for needed in ["fetch peer", "/work/repo", "exit 128", "not a repository"] {
            assert!(rendered.contains(needed), "missing {needed:?}: {rendered}");
        }
    }

    /// A signal death has no exit code; the rendering must not invent one.
    #[test]
    fn a_signal_death_renders_without_an_exit_code() {
        let error = failed(
            Path::new("/work/repo"),
            &["fetch"],
            &GitOutcome {
                code: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
        );
        assert!(
            !error.to_string().contains("exit"),
            "no code means no code: {error}"
        );
    }

    /// Real exits through the real runner: 0 succeeds with stdout, non-zero
    /// carries stderr, and the probe shape maps 0/1/other as promised.
    #[tokio::test]
    async fn the_runner_judges_real_exit_codes() {
        let dir = tempfile::tempdir().expect("a scratch dir");
        super::git(dir.path(), &["init", "-b", "main", "."])
            .await
            .expect("init succeeds");

        let version = super::git(dir.path(), &["version"])
            .await
            .expect("version succeeds");
        assert!(version.starts_with("git version"), "got {version:?}");

        let error = super::git(dir.path(), &["rev-parse", "--verify", "nothing"])
            .await
            .expect_err("an unknown ref must be an error");
        assert!(matches!(error, GitError::Failed { .. }));

        // rev-parse -q --verify: exit 1 is an answer, not an error…
        let absent = super::check(
            dir.path(),
            &[
                "rev-parse",
                "-q",
                "--verify",
                "0000000000000000000000000000000000000001^{commit}",
            ],
        )
        .await
        .expect("a clean probe");
        assert!(!absent, "an unknown object must probe false");

        // …while an unusable question stays an error.
        super::check(dir.path(), &["cat-file", "--definitely-not-a-flag"])
            .await
            .expect_err("a broken probe must not answer yes or no");
    }
}
