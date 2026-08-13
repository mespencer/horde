//! Which way a commit travels between two checkouts.
//!
//! DESIGN §8: transport resolution is per sharing pair and invisible to
//! Agents. The decision needs exactly two facts about each side — which
//! Worker the checkout lives on, and which object store it reads — so that is
//! all a [`CheckoutView`] carries, and the resolution itself is a pure
//! function a truth table can pin down.

use std::path::{Path, PathBuf};

use crate::git::{GitError, git};

/// Where one Agent's checkout of one repo lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutView {
    /// The Worker whose disk this checkout is on.
    pub worker_id: String,

    /// The checkout's working-tree path on that Worker.
    pub repo_path: PathBuf,

    /// The object store this checkout reads: the repository's *common* git
    /// dir. Worktrees of one clone share it, which is exactly what makes the
    /// ref-checkout-only route safe — the commit is already in the receiver's
    /// objects.
    pub object_store: PathBuf,
}

impl CheckoutView {
    /// Observes a checkout on disk, asking git for its object store.
    ///
    /// Observed rather than declared (DESIGN §5): a caller guessing at
    /// `--git-common-dir` would miss worktrees, and a wrong object store turns
    /// "already shared" into "silently absent".
    ///
    /// # Errors
    ///
    /// [`GitError`] when `repo_path` is not a git checkout.
    pub async fn observe(worker_id: &str, repo_path: &Path) -> Result<Self, GitError> {
        let common = git(
            repo_path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?;
        Ok(Self {
            worker_id: worker_id.to_owned(),
            repo_path: repo_path.to_owned(),
            object_store: PathBuf::from(common),
        })
    }
}

/// How the commit gets from the sender's checkout to the receiver's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShareRoute {
    /// Same clone (worktrees included): the receiver's object store already
    /// holds the commit, so sharing is a ref checkout only.
    AlreadyShared,

    /// Same Worker, different clones: fetch straight from the sender's path.
    PeerFetch {
        /// The sender's working-tree path, fetchable as a local remote.
        source: PathBuf,
    },

    /// Different Workers: the sender pushes to the Relay, the receiver
    /// fetches from it. The only route that crosses a machine boundary.
    ViaRelay {
        /// The repo's registered name — [`crate::RelayLocation`] turns it
        /// into a URL. A name rather than a URL because sender and receiver
        /// may reach the Relay at different addresses.
        repo_name: String,
    },
}

/// Both checkouts sit on one Worker's disk.
fn on_same_worker(sender: &CheckoutView, receiver: &CheckoutView) -> bool {
    sender.worker_id == receiver.worker_id
}

/// Both checkouts read one object store — same clone or worktrees of it.
///
/// The worker check is load-bearing, not redundant: two different machines
/// can both hold a checkout at `/home/x/repo/.git`, and equal paths across
/// Workers share nothing.
fn share_an_object_store(sender: &CheckoutView, receiver: &CheckoutView) -> bool {
    on_same_worker(sender, receiver) && sender.object_store == receiver.object_store
}

/// Resolves the route for one repo between one sender and one receiver.
#[must_use]
pub fn resolve_route(
    repo_name: &str,
    sender: &CheckoutView,
    receiver: &CheckoutView,
) -> ShareRoute {
    if share_an_object_store(sender, receiver) {
        ShareRoute::AlreadyShared
    } else if on_same_worker(sender, receiver) {
        ShareRoute::PeerFetch {
            source: sender.repo_path.clone(),
        }
    } else {
        ShareRoute::ViaRelay {
            repo_name: repo_name.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CheckoutView, ShareRoute, resolve_route};
    use std::path::PathBuf;

    fn view(worker: &str, repo_path: &str, object_store: &str) -> CheckoutView {
        CheckoutView {
            worker_id: worker.to_owned(),
            repo_path: PathBuf::from(repo_path),
            object_store: PathBuf::from(object_store),
        }
    }

    /// The three topologies of DESIGN §8, plus the case that separates `&&`
    /// from `||` in the object-store predicate: equal paths on different
    /// Workers share nothing and must go through the Relay.
    #[test]
    fn the_three_topologies_resolve_to_their_three_routes() {
        let sender = view("w1", "/w1/agent-a/repo", "/w1/shared/repo/.git");

        assert_eq!(
            resolve_route(
                "atlas",
                &sender,
                &view("w1", "/w1/agent-b/repo", "/w1/shared/repo/.git"),
            ),
            ShareRoute::AlreadyShared,
            "worktrees of one clone already hold each other's commits"
        );

        assert_eq!(
            resolve_route(
                "atlas",
                &sender,
                &view("w1", "/w1/agent-c/repo", "/w1/agent-c/repo/.git"),
            ),
            ShareRoute::PeerFetch {
                source: PathBuf::from("/w1/agent-a/repo"),
            },
            "a sibling clone on the same disk fetches from the sender's path"
        );

        assert_eq!(
            resolve_route(
                "atlas",
                &sender,
                &view("w2", "/w2/agent-d/repo", "/w2/agent-d/repo/.git"),
            ),
            ShareRoute::ViaRelay {
                repo_name: "atlas".to_owned(),
            },
            "different Workers only meet at the Relay"
        );
    }

    /// Two machines can both have `/home/x/repo/.git`. Path equality across
    /// Workers is a coincidence, never a shared store.
    #[test]
    fn an_equal_store_path_on_another_worker_is_not_shared() {
        let sender = view("w1", "/home/x/repo", "/home/x/repo/.git");
        let receiver = view("w2", "/home/x/repo", "/home/x/repo/.git");
        assert_eq!(
            resolve_route("atlas", &sender, &receiver),
            ShareRoute::ViaRelay {
                repo_name: "atlas".to_owned(),
            }
        );
    }

    /// Same Worker, stores differing only in path: the peer-fetch source must
    /// be the sender's working tree, not the receiver's — fetching from
    /// yourself would "succeed" while sharing nothing.
    #[test]
    fn the_peer_fetch_source_is_the_senders_path() {
        let sender = view("w1", "/w1/a", "/w1/a/.git");
        let receiver = view("w1", "/w1/b", "/w1/b/.git");
        let ShareRoute::PeerFetch { source } = resolve_route("atlas", &sender, &receiver) else {
            panic!("two clones on one Worker must peer-fetch");
        };
        assert_eq!(source, PathBuf::from("/w1/a"));
    }

    /// Observation against real git: a worktree reports its clone's common
    /// dir, so the pair resolves as already shared; an unrelated clone does
    /// not.
    #[tokio::test]
    async fn observation_sees_worktrees_sharing_a_store() {
        let dir = tempfile::tempdir().expect("a scratch dir");
        let clone = dir.path().join("clone");
        std::fs::create_dir(&clone).expect("mkdir");
        for args in [
            vec!["init", "-b", "main", "."],
            vec![
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "--allow-empty",
                "-m",
                "root",
            ],
            vec!["worktree", "add", "../tree", "-b", "side"],
        ] {
            crate::git::git(&clone, &args).await.expect("git succeeds");
        }

        let a = CheckoutView::observe("w1", &clone).await.expect("observed");
        let b = CheckoutView::observe("w1", &dir.path().join("tree"))
            .await
            .expect("observed");
        assert_eq!(
            resolve_route("atlas", &a, &b),
            ShareRoute::AlreadyShared,
            "a worktree and its clone read one object store"
        );

        let elsewhere = dir.path().join("elsewhere");
        std::fs::create_dir(&elsewhere).expect("mkdir");
        crate::git::git(&elsewhere, &["init", "-b", "main", "."])
            .await
            .expect("init succeeds");
        let c = CheckoutView::observe("w1", &elsewhere)
            .await
            .expect("observed");
        assert!(
            matches!(resolve_route("atlas", &a, &c), ShareRoute::PeerFetch { .. }),
            "an unrelated clone on the same Worker peer-fetches"
        );
    }
}
