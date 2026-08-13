//! Fixture-building for the relay and share suites: real repos in temp
//! directories, arranged with a real `git` client.
//!
//! Wrapped in `#[cfg(test)]` so the workspace's `expect_used` exemption
//! applies: clippy exempts anything under a `cfg(test)` module, but not a
//! bare helper beside a `#[test]` fn.
#![allow(dead_code)]

#[cfg(test)]
pub(crate) mod harness {
    use std::path::{Path, PathBuf};

    /// How long any wait in these suites may take before it is a failure.
    ///
    /// Everything is loopback or on-disk; ten seconds is generous against a
    /// loaded machine and far below where cargo-mutants would call it a
    /// timeout — which is the point: a mutant that makes a fetch hang must
    /// fail in seconds, not stall the suite (docs/QUALITY_GATES.md).
    pub(crate) const CEILING: std::time::Duration = std::time::Duration::from_secs(10);

    /// Runs `future` with a ceiling, failing the test if it does not finish.
    pub(crate) async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
        let Ok(value) = tokio::time::timeout(CEILING, future).await else {
            panic!("{what} did not finish within {CEILING:?} — a hang, not a slow machine");
        };
        value
    }

    /// Runs git in `dir` for fixture-building, requiring success.
    ///
    /// This is the *test's* git — arranging repos to point production code
    /// at — so it asserts rather than returning errors, and it pins the
    /// identity/signing config so a host's global config cannot leak in.
    pub(crate) async fn git_in(dir: &Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args([
                "-c",
                "user.email=agent@test",
                "-c",
                "user.name=agent",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .output()
            .await
            .expect("git spawns");
        assert!(
            output.status.success(),
            "git {args:?} in {} failed: {}",
            dir.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    /// Runs git expecting *failure*, returning its stderr.
    pub(crate) async fn git_in_failing(dir: &Path, args: &[&str]) -> String {
        let output = tokio::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(args)
            .output()
            .await
            .expect("git spawns");
        assert!(
            !output.status.success(),
            "git {args:?} in {} should have failed but succeeded",
            dir.display()
        );
        String::from_utf8_lossy(&output.stderr).trim().to_owned()
    }

    /// A new repo at `root/name` on branch `main` with one committed file.
    pub(crate) async fn init_repo(root: &Path, name: &str) -> PathBuf {
        let path = root.join(name);
        std::fs::create_dir_all(&path).expect("mkdir");
        git_in(&path, &["init", "-b", "main", "."]).await;
        commit_file(&path, "README.md", "hello", "root commit").await;
        path
    }

    /// Writes `file`, commits it, and returns the new commit's id.
    pub(crate) async fn commit_file(
        repo: &Path,
        file: &str,
        content: &str,
        message: &str,
    ) -> String {
        std::fs::write(repo.join(file), content).expect("write");
        git_in(repo, &["add", "."]).await;
        git_in(repo, &["commit", "-m", message]).await;
        git_in(repo, &["rev-parse", "HEAD"]).await
    }

    /// Clones `source` (a path or URL) to `root/name`.
    pub(crate) async fn clone_to(root: &Path, source: &str, name: &str) -> PathBuf {
        git_in(root, &["clone", source, name]).await;
        root.join(name)
    }

    /// Whether `repo` holds `commit_sha` as a real commit.
    pub(crate) async fn has_commit(repo: &Path, commit_sha: &str) -> bool {
        let probe = format!("{commit_sha}^{{commit}}");
        tokio::process::Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(["cat-file", "-e", &probe])
            .status()
            .await
            .expect("git spawns")
            .success()
    }

    /// The cross-Worker arrangement: an upstream, one clone per "Worker",
    /// and a commit only the sender has.
    ///
    /// The two clones sit on one real disk; what makes them different
    /// Workers is the id their `CheckoutView` is observed under — which is
    /// exactly why resolution keys on the Worker, not the path.
    pub(crate) struct TwoWorkers {
        /// Holds every repo below; dropping it deletes them.
        pub(crate) scratch: tempfile::TempDir,
        pub(crate) origin: PathBuf,
        pub(crate) sender_repo: PathBuf,
        pub(crate) receiver_repo: PathBuf,
        /// Committed in the sender's clone and pushed nowhere.
        pub(crate) senders_commit: String,
    }

    pub(crate) async fn two_workers() -> TwoWorkers {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = init_repo(scratch.path(), "origin").await;
        let origin_url = origin.display().to_string();
        let sender_repo = clone_to(scratch.path(), &origin_url, "w1").await;
        let receiver_repo = clone_to(scratch.path(), &origin_url, "w2").await;
        let senders_commit = commit_file(
            &sender_repo,
            "shared.rs",
            "pub struct Shared;",
            "the sender's work",
        )
        .await;
        TwoWorkers {
            scratch,
            origin,
            sender_repo,
            receiver_repo,
            senders_commit,
        }
    }

    /// A served Relay for `atlas` over a fresh upstream, plus one clone made
    /// through it — the arrangement every write-path relay test starts from.
    pub(crate) struct RelayedClone {
        /// Holds the upstream and the clone.
        pub(crate) scratch: tempfile::TempDir,
        /// Holds the mirrors; dropped last of the two tempdirs.
        pub(crate) mirrors: tempfile::TempDir,
        pub(crate) origin: PathBuf,
        pub(crate) server: horde_relay::RelayServer,
        pub(crate) store: std::sync::Arc<horde_relay::MirrorStore>,
        /// `origin` cloned through the relay, so the mirror is materialized.
        pub(crate) clone: PathBuf,
        /// The relay-side URL of `atlas`.
        pub(crate) url: String,
    }

    pub(crate) async fn relay_with_clone() -> RelayedClone {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = init_repo(scratch.path(), "origin").await;
        let (server, store, mirrors) = serve_relay(&[("atlas", &origin)]).await;
        let url = server.location().repo_url("atlas");
        let clone = clone_to(scratch.path(), &url, "clone").await;
        RelayedClone {
            scratch,
            mirrors,
            origin,
            server,
            store,
            clone,
            url,
        }
    }

    /// A served Relay over a fresh store, with `repos` registered.
    ///
    /// Returns the server (dropping it stops serving), the store, and the
    /// scratch directory holding the mirrors.
    pub(crate) async fn serve_relay(
        repos: &[(&str, &Path)],
    ) -> (
        horde_relay::RelayServer,
        std::sync::Arc<horde_relay::MirrorStore>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let store = std::sync::Arc::new(horde_relay::MirrorStore::new(dir.path().join("relay")));
        for (name, path) in repos {
            store
                .register(name, &path.display().to_string())
                .expect("registers");
        }
        let server = horde_relay::RelayServer::bind(
            "127.0.0.1:0".parse().expect("an addr"),
            std::sync::Arc::clone(&store),
        )
        .await
        .expect("binds loopback");
        (server, store, dir)
    }
}
