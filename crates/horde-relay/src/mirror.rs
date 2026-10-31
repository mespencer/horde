//! The Relay's bare mirrors: one per registered repo, created lazily.
//!
//! ADR-0002: the Server keeps a bare mirror per registered repo, mirrored on
//! first use, doubling as clone cache and — end state — the credential
//! chokepoint. This module owns the mirrors on disk; serving them over
//! smart-HTTP is [`crate::http`]'s job.
//!
//! # Registration is the trust boundary
//!
//! Only registered names are ever served or materialized, and a name is
//! refused at registration unless it is a single safe path segment. The name
//! becomes a directory under the store root *and* a `PATH_INFO` segment
//! handed to `git http-backend`, so "safe" here is what keeps a request for
//! `../../etc` from ever reaching the filesystem.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::git::{GitError, git};

/// Why the mirror store refused or failed.
#[derive(Debug, thiserror::Error)]
pub enum MirrorError {
    /// The name is not a single safe path segment.
    #[error(
        "{name:?} cannot name a Relay repo: names are a single path segment \
         of [A-Za-z0-9._-], not `.` or `..`, not starting with `-`"
    )]
    InvalidRepoName { name: String },

    /// The repo was never registered. The Relay serves registered repos only;
    /// guessing at upstreams for unknown names would turn it into an open
    /// proxy.
    #[error("{name:?} is not a registered Relay repo")]
    UnknownRepo { name: String },

    /// git failed while materializing or refreshing the mirror.
    #[error(transparent)]
    Git(#[from] GitError),

    /// The store's own directories could not be managed.
    #[error("the Relay store at {path} could not be prepared: {source}")]
    Store {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

/// Whether `name` may name a mirror directory and a `PATH_INFO` segment.
///
/// One conservative character class rather than a denylist: everything the
/// filesystem or CGI layer could reinterpret (separators, `..`, percent
/// escapes, a leading `-` that git would read as a flag) falls outside it.
#[must_use]
pub fn is_valid_repo_name(name: &str) -> bool {
    let safe_chars = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    safe_chars && !name.is_empty() && name != "." && name != ".." && !name.starts_with('-')
}

/// The bare mirrors on the Server's disk.
///
/// Cheap to share behind an `Arc`; the registry lock is held only for map
/// access, and the materialization lock serializes clones so two first
/// requests for one repo cannot race each other into the same directory.
#[derive(Debug)]
pub struct MirrorStore {
    root: PathBuf,
    /// name → upstream remote URL.
    registry: Mutex<BTreeMap<String, String>>,
    /// Serializes mirror creation. An async mutex because it is held across
    /// the clone itself.
    materialize: tokio::sync::Mutex<()>,
}

impl MirrorStore {
    /// A store rooted at `root`. The directory is created on first use.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            registry: Mutex::new(BTreeMap::new()),
            materialize: tokio::sync::Mutex::new(()),
        }
    }

    /// Registers `name` as mirroring `remote_url`. Idempotent; re-registering
    /// replaces the upstream, which is how a moved remote is corrected.
    ///
    /// # Errors
    ///
    /// [`MirrorError::InvalidRepoName`] when the name cannot safely become a
    /// path segment.
    pub fn register(&self, name: &str, remote_url: &str) -> Result<(), MirrorError> {
        if !is_valid_repo_name(name) {
            return Err(MirrorError::InvalidRepoName {
                name: name.to_owned(),
            });
        }
        self.lock_registry()
            .insert(name.to_owned(), remote_url.to_owned());
        Ok(())
    }

    /// Unregisters every name not in `keep`, returning the names removed.
    ///
    /// The registry's deletion path: a repo whose workspace definition was
    /// deleted must stop being served, or the Relay keeps cloning and serving
    /// a remote nobody declared any more. The mirror on disk is left alone —
    /// a result already in the log may name a commit only that mirror holds,
    /// and re-registering the name serves it again without a clone.
    pub fn retain_only(&self, keep: &std::collections::BTreeSet<String>) -> Vec<String> {
        let mut registry = self.lock_registry();
        let removed: Vec<String> = registry
            .keys()
            .filter(|name| !keep.contains(*name))
            .cloned()
            .collect();
        for name in &removed {
            registry.remove(name);
        }
        removed
    }

    /// The registered upstream for `name`, if any.
    #[must_use]
    pub fn registered(&self, name: &str) -> Option<String> {
        self.lock_registry().get(name).cloned()
    }

    /// The mirror's directory name: always `<name>.git`, so a staging
    /// directory (which never ends in `.git`) cannot collide with it.
    #[must_use]
    pub fn dir_name(name: &str) -> String {
        format!("{name}.git")
    }

    /// Where `name`'s mirror lives (whether or not it exists yet).
    #[must_use]
    pub fn path(&self, name: &str) -> PathBuf {
        self.root.join(Self::dir_name(name))
    }

    /// The root `git http-backend` serves from.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Ensures `name`'s mirror exists, cloning it on first use.
    ///
    /// Lazy per ADR-0002: nothing is mirrored until something asks. The clone
    /// lands in a staging directory and is renamed into place, so a crash
    /// mid-clone leaves debris, never a half-mirror that `exists()` would
    /// mistake for a whole one.
    ///
    /// # Errors
    ///
    /// [`MirrorError::UnknownRepo`] for unregistered names, [`MirrorError::Git`]
    /// when the clone fails, [`MirrorError::Store`] when the root cannot be
    /// prepared.
    pub async fn ensure(&self, name: &str) -> Result<PathBuf, MirrorError> {
        let remote_url = self
            .registered(name)
            .ok_or_else(|| MirrorError::UnknownRepo {
                name: name.to_owned(),
            })?;

        let path = self.path(name);
        let _guard = self.materialize.lock().await;
        if path.exists() {
            return Ok(path);
        }

        std::fs::create_dir_all(&self.root).map_err(|source| self.store_error(source))?;
        let staging = self.root.join(format!("{name}.staging"));
        if staging.exists() {
            std::fs::remove_dir_all(&staging).map_err(|source| self.store_error(source))?;
        }

        // `--` before the URL: an upstream is configuration, not a flag, and
        // the separator keeps a URL starting with `-` from being read as one.
        //
        // `--no-local`: an upstream given as a path is somebody's live repo.
        // A local clone walks its `objects/` copying entry by entry, and the
        // detached auto-maintenance each commit there spawns creates and
        // deletes `objects/maintenance.lock` mid-walk ("failed to copy file
        // … No such file or directory"). Through the transport only the
        // reachable objects travel, as a pack — at the cost of hardlinks.
        // For a URL upstream the flag changes nothing.
        let staging_str = staging.display().to_string();
        git(
            &self.root,
            &[
                "clone",
                "--mirror",
                "--no-local",
                "--",
                &remote_url,
                &staging_str,
            ],
        )
        .await?;
        std::fs::rename(&staging, &path).map_err(|source| self.store_error(source))?;
        Ok(path)
    }

    /// Fetches the mirror's upstream again, refreshing the clone cache.
    ///
    /// Deliberately **without** `--prune`: the mirror also holds
    /// `refs/horde/share/*` from cross-Worker shares, and pruning to match the
    /// upstream would delete every share ref the upstream has never heard of.
    ///
    /// # Errors
    ///
    /// As [`MirrorStore::ensure`], which this includes.
    pub async fn refresh(&self, name: &str) -> Result<(), MirrorError> {
        let path = self.ensure(name).await?;
        git(&path, &["fetch", "origin"]).await?;
        Ok(())
    }

    fn store_error(&self, source: std::io::Error) -> MirrorError {
        MirrorError::Store {
            path: self.root.display().to_string(),
            source,
        }
    }

    /// The registry map, recovered from poisoning: the map itself cannot be
    /// left inconsistent by a panicking holder, so inheriting the poison would
    /// refuse service for nothing.
    fn lock_registry(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, String>> {
        match self.registry.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MirrorError, MirrorStore, is_valid_repo_name};

    /// The names that may become path segments, and the shapes of hostile or
    /// broken ones that must not.
    #[test]
    fn repo_names_are_a_single_safe_path_segment() {
        for good in ["atlas", "my-repo", "a.b_c", "repo2", "x"] {
            assert!(is_valid_repo_name(good), "{good:?} should be accepted");
        }
        for bad in [
            "", ".", "..", "a/b", "a\\b", "a b", "a%2fb", "-flag", "a\nb", "über",
        ] {
            assert!(!is_valid_repo_name(bad), "{bad:?} should be refused");
        }
    }

    #[test]
    fn registration_refuses_what_the_validator_refuses() {
        let store = MirrorStore::new(std::path::PathBuf::from("/nowhere"));
        let error = store
            .register("../escape", "file:///tmp/x")
            .expect_err("a traversal name must be refused");
        assert!(matches!(error, MirrorError::InvalidRepoName { .. }));
        assert!(store.registered("../escape").is_none());
    }

    #[test]
    fn reregistration_replaces_the_upstream() {
        let store = MirrorStore::new(std::path::PathBuf::from("/nowhere"));
        store.register("atlas", "file:///old").expect("registers");
        store
            .register("atlas", "file:///new")
            .expect("re-registers");
        assert_eq!(store.registered("atlas").as_deref(), Some("file:///new"));
    }

    /// The directory name always carries `.git`, and the path stays under the
    /// root — this is the pair the HTTP layer builds `PATH_INFO` from.
    #[test]
    fn mirror_paths_live_under_the_root_with_a_git_suffix() {
        let store = MirrorStore::new(std::path::PathBuf::from("/relay"));
        assert_eq!(MirrorStore::dir_name("atlas"), "atlas.git");
        assert_eq!(
            store.path("atlas"),
            std::path::PathBuf::from("/relay/atlas.git")
        );
    }

    #[tokio::test]
    async fn an_unregistered_repo_cannot_be_materialized() {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let store = MirrorStore::new(dir.path().join("relay"));
        let error = store
            .ensure("ghost")
            .await
            .expect_err("unknown repos must be refused, not guessed at");
        assert!(matches!(error, MirrorError::UnknownRepo { .. }));
    }

    /// A path upstream is a live repo whose `objects/` may be mid-maintenance.
    /// A dangling `maintenance.lock` symlink stands in for the lock that
    /// vanishes mid-copy, deterministically: a local clone walking `objects/`
    /// refuses it, a clone through the transport never sees it.
    #[tokio::test]
    async fn a_path_upstream_under_maintenance_still_mirrors() {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let upstream = dir.path().join("upstream");
        std::fs::create_dir_all(&upstream).expect("the upstream dir");
        for args in [
            ["init", "-q", "-b", "main", "."].as_slice(),
            [
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "-c",
                "maintenance.auto=false",
                "commit",
                "-qm",
                "seed",
                "--allow-empty",
            ]
            .as_slice(),
        ] {
            crate::git::git(&upstream, args)
                .await
                .expect("the upstream arrangement");
        }
        let objects = upstream.join(".git").join("objects");
        std::os::unix::fs::symlink(objects.join("gone"), objects.join("maintenance.lock"))
            .expect("plants the stray lock");
        let store = MirrorStore::new(dir.path().join("relay"));
        store
            .register("atlas", &upstream.display().to_string())
            .expect("registers");

        let mirror = store
            .ensure("atlas")
            .await
            .expect("a maintained upstream must still mirror");

        assert_eq!(
            crate::git::git(&mirror, &["rev-parse", "main"])
                .await
                .expect("the mirror holds main"),
            crate::git::git(&upstream, &["rev-parse", "main"])
                .await
                .expect("the upstream holds main"),
        );
    }
}
