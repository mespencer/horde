//! The three sharing topologies of DESIGN §8, executed for real.
//!
//! Same clone → ref checkout only; same Worker → fetch from the peer's path;
//! cross-Worker → push/fetch through the Relay. Each test observes real
//! checkouts, resolves the route production-side, runs both halves of the
//! share, and asserts on what actually arrived in the receiving repo.

mod common;

#[cfg(test)]
mod suite {
    use crate::common::harness::{commit_file, git_in, has_commit, init_repo, serve_relay, within};
    use horde_relay::{
        CheckoutView, ShareError, ShareRoute, observe_head, obtain_share, require_committed,
        resolve_route, stage_share,
    };

    /// A commit no test repo contains, in valid 40-hex form.
    const ABSENT: &str = "1111111111111111111111111111111111111111";

    /// Worktrees of one clone: the route is ref-checkout-only, and the commit
    /// is checkable-out on the receiving side without any fetch having
    /// happened.
    #[tokio::test]
    async fn a_worktree_share_is_already_present() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let clone = within("clone init", init_repo(scratch.path(), "clone")).await;
        within(
            "a worktree appears",
            git_in(&clone, &["worktree", "add", "../tree", "-b", "agent-b"]),
        )
        .await;
        let tree = scratch.path().join("tree");

        let shared = within(
            "the sender commits",
            commit_file(&clone, "shared.rs", "pub struct Shared;", "add shared"),
        )
        .await;

        let sender = within("sender view", CheckoutView::observe("w1", &clone))
            .await
            .expect("a checkout");
        let receiver = within("receiver view", CheckoutView::observe("w1", &tree))
            .await
            .expect("a checkout");
        let route = resolve_route("atlas", &sender, &receiver);
        assert_eq!(route, ShareRoute::AlreadyShared);

        within("stage", stage_share(&clone, &route, None, &shared))
            .await
            .expect("staging a local share");
        within("obtain", obtain_share(&tree, &route, None, &shared))
            .await
            .expect("obtaining a local share");

        // The receiver can act on the ref — the checkout is the point.
        within(
            "checkout",
            git_in(&tree, &["checkout", "--detach", &shared]),
        )
        .await;
        assert_eq!(
            std::fs::read_to_string(tree.join("shared.rs")).expect("the shared file"),
            "pub struct Shared;"
        );
    }

    /// Two clones on one Worker: the receiver fetches straight from the
    /// sender's path, no server anywhere.
    #[tokio::test]
    async fn a_same_worker_share_fetches_from_the_peers_path() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        let origin_url = origin.display().to_string();
        let a = within(
            "clone a",
            crate::common::harness::clone_to(scratch.path(), &origin_url, "a"),
        )
        .await;
        let b = within(
            "clone b",
            crate::common::harness::clone_to(scratch.path(), &origin_url, "b"),
        )
        .await;

        let shared = within(
            "the sender commits",
            commit_file(&a, "feature.rs", "pub fn f() {}", "add feature"),
        )
        .await;
        assert!(
            !within("b starts without it", has_commit(&b, &shared)).await,
            "the receiver must not already have the commit"
        );

        let sender = within("sender view", CheckoutView::observe("w1", &a))
            .await
            .expect("a checkout");
        let receiver = within("receiver view", CheckoutView::observe("w1", &b))
            .await
            .expect("a checkout");
        let route = resolve_route("atlas", &sender, &receiver);
        assert!(
            matches!(route, ShareRoute::PeerFetch { .. }),
            "got {route:?}"
        );

        within("stage", stage_share(&a, &route, None, &shared))
            .await
            .expect("staging");
        within("obtain", obtain_share(&b, &route, None, &shared))
            .await
            .expect("obtaining");

        assert!(
            within("b now has it", has_commit(&b, &shared)).await,
            "the peer fetch must deliver the commit"
        );
    }

    /// Different Workers: the sender pushes a content-addressed share ref to
    /// the Relay, the receiver fetches it — the only route that crosses a
    /// machine boundary, over a real served Relay.
    #[tokio::test]
    async fn a_cross_worker_share_travels_through_the_relay() {
        let workers = within(
            "the two-worker arrangement",
            crate::common::harness::two_workers(),
        )
        .await;
        let (sender_repo, receiver_repo) = (&workers.sender_repo, &workers.receiver_repo);
        let shared = workers.senders_commit.clone();

        let sender = within("sender view", CheckoutView::observe("w1", sender_repo))
            .await
            .expect("a checkout");
        let receiver = within("receiver view", CheckoutView::observe("w2", receiver_repo))
            .await
            .expect("a checkout");
        let route = resolve_route("atlas", &sender, &receiver);
        assert_eq!(
            route,
            ShareRoute::ViaRelay {
                repo_name: "atlas".to_owned(),
            }
        );

        let (server, store, _mirrors) = serve_relay(&[("atlas", &workers.origin)]).await;
        let relay = server.location();

        within(
            "stage",
            stage_share(sender_repo, &route, Some(&relay), &shared),
        )
        .await
        .expect("the push through the relay");
        let mirror_refs = within(
            "the mirror lists refs",
            git_in(&store.path("atlas"), &["show-ref"]),
        )
        .await;
        assert!(
            mirror_refs.contains(&format!("refs/horde/share/{shared}")),
            "the share ref must be on the mirror: {mirror_refs}"
        );

        within(
            "obtain",
            obtain_share(receiver_repo, &route, Some(&relay), &shared),
        )
        .await
        .expect("the fetch through the relay");
        assert!(
            within("w2 now has it", has_commit(receiver_repo, &shared)).await,
            "the commit must arrive on the other Worker"
        );
    }

    /// A cross-Worker route with no Relay configured refuses instead of
    /// guessing — both halves.
    #[tokio::test]
    async fn a_cross_worker_share_without_a_relay_is_refused() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let repo = within("repo init", init_repo(scratch.path(), "repo")).await;
        let head = within("head", git_in(&repo, &["rev-parse", "HEAD"])).await;
        let route = ShareRoute::ViaRelay {
            repo_name: "atlas".to_owned(),
        };

        let staged = within("stage refuses", stage_share(&repo, &route, None, &head)).await;
        assert!(matches!(staged, Err(ShareError::NoRelay)), "got {staged:?}");
        let obtained = within("obtain refuses", obtain_share(&repo, &route, None, &head)).await;
        assert!(
            matches!(obtained, Err(ShareError::NoRelay)),
            "got {obtained:?}"
        );
    }

    /// A share ref that does not contain the commit it names must fail at
    /// obtain time. The ref is only a name; the commit id is authoritative
    /// (payload.proto), and a receiver that trusts the name would check out
    /// somebody else's work as this share.
    #[tokio::test]
    async fn a_share_ref_naming_the_wrong_commit_is_refused() {
        let workers = within(
            "the two-worker arrangement",
            crate::common::harness::two_workers(),
        )
        .await;
        // The sender's real commit becomes the decoy: it exists, it travels,
        // and it is not the commit the share ref will claim to hold.
        let decoy = workers.senders_commit.clone();
        let (server, _store, _mirrors) = serve_relay(&[("atlas", &workers.origin)]).await;
        let relay = server.location();

        // Poison the ref by hand: push the decoy under the ABSENT commit's
        // share ref, then obtain ABSENT through the production path.
        let url = relay.repo_url("atlas");
        within(
            "the poisoned push",
            git_in(
                &workers.sender_repo,
                &["push", &url, &format!("{decoy}:refs/horde/share/{ABSENT}")],
            ),
        )
        .await;

        let route = ShareRoute::ViaRelay {
            repo_name: "atlas".to_owned(),
        };
        let obtained = within(
            "obtain notices",
            obtain_share(&workers.receiver_repo, &route, Some(&relay), ABSENT),
        )
        .await;
        assert!(
            matches!(obtained, Err(ShareError::MissingCommit { .. })),
            "a ref pointing at the wrong commit must be refused: {obtained:?}"
        );
    }

    /// Staging proves the sender has what it advertises, on every route.
    #[tokio::test]
    async fn staging_a_commit_the_sender_lacks_is_refused() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let repo = within("repo init", init_repo(scratch.path(), "repo")).await;

        let staged = within(
            "stage refuses",
            stage_share(&repo, &ShareRoute::AlreadyShared, None, ABSENT),
        )
        .await;
        assert!(
            matches!(staged, Err(ShareError::MissingCommit { .. })),
            "a Payload naming a commit the sender lacks is a bug to surface: {staged:?}"
        );
    }

    /// "Commit, then share": a dirty tree is refused with the paths named,
    /// and committing is exactly what clears it.
    #[tokio::test]
    async fn a_dirty_tree_is_refused_until_committed() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let repo = within("repo init", init_repo(scratch.path(), "repo")).await;
        std::fs::write(repo.join("wip.rs"), "// not committed").expect("write");

        let refused = within("a dirty tree refuses", require_committed(&repo)).await;
        let Err(ShareError::Uncommitted { paths, .. }) = refused else {
            panic!("uncommitted work must refuse the share, got {refused:?}");
        };
        assert!(
            paths.contains("wip.rs"),
            "the culprit must be named: {paths}"
        );

        within(
            "committing clears it",
            commit_file(&repo, "wip.rs", "// committed now", "commit the wip"),
        )
        .await;
        within("a clean tree passes", require_committed(&repo))
            .await
            .expect("nothing left to refuse");
    }

    /// The Worker's observation for Payload stamping: the commit id is
    /// authoritative, the branch advisory — and absent when detached, not
    /// invented.
    #[tokio::test]
    async fn observing_head_reports_the_commit_and_the_advisory_branch() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let repo = within("repo init", init_repo(scratch.path(), "repo")).await;
        let head = within(
            "a commit lands",
            commit_file(&repo, "obs.rs", "// observed", "observe me"),
        )
        .await;

        let observed = within("observe", observe_head(&repo))
            .await
            .expect("a checkout with commits");
        assert_eq!(observed.commit_sha, head);
        assert_eq!(observed.branch.as_deref(), Some("main"));

        within("detach", git_in(&repo, &["checkout", "--detach", "HEAD"])).await;
        let detached = within("observe detached", observe_head(&repo))
            .await
            .expect("still a checkout");
        assert_eq!(detached.commit_sha, head);
        assert_eq!(
            detached.branch, None,
            "a detached HEAD has no branch to advise"
        );
    }
}
