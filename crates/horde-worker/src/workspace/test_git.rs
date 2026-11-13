//! Real git for the workspace module's tests: a command runner that fails
//! the test on any error, and a one-file commit.

use std::path::Path;

/// Runs git in `cwd` with a fixed identity, failing the test (with git's
/// stderr) on any error, and returns its trimmed stdout.
pub(crate) fn git(cwd: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(["-c", "user.name=h", "-c", "user.email=h@t"])
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

/// Commits a file named `file` (holding its own name) in `repo`, and
/// returns the new HEAD.
pub(crate) fn commit(repo: &Path, file: &str) -> String {
    std::fs::write(repo.join(file), file).expect("a file");
    git(repo, &["add", "--", file]);
    git(repo, &["commit", "-qm", file]);
    git(repo, &["rev-parse", "HEAD"])
}
