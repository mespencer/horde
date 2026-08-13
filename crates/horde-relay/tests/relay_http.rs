//! The Relay served over real smart-HTTP, exercised by a real `git` client.
//!
//! ADR-0002's transport, end to end: a mirror store behind `git http-backend`
//! behind a loopback listener, with actual `git clone`/`push`/`fetch`
//! processes on the other side. The protocol details — pack negotiation,
//! advertisement framing, gzip — are exactly what an in-process fake cannot
//! vouch for, which is why every test here goes over the wire.

mod common;

#[cfg(test)]
mod suite {
    use crate::common::harness::{
        clone_to, commit_file, git_in, git_in_failing, has_commit, init_repo, serve_relay, within,
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The first fetch is what mirrors: nothing exists under the store until
    /// a client asks, and then the clone comes back complete.
    #[tokio::test]
    async fn a_clone_through_the_relay_lazily_mirrors_the_upstream() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        let wanted = within(
            "a commit lands upstream",
            commit_file(&origin, "src.rs", "fn main() {}", "add src"),
        )
        .await;

        let (server, store, _mirrors) = serve_relay(&[("atlas", &origin)]).await;
        assert!(
            !store.path("atlas").exists(),
            "nothing may be mirrored before anything asks"
        );

        let url = server.location().repo_url("atlas");
        let clone = within("clone via relay", clone_to(scratch.path(), &url, "clone")).await;

        assert!(
            store.path("atlas").exists(),
            "the first fetch must have materialized the mirror"
        );
        assert!(
            within("clone holds the commit", has_commit(&clone, &wanted)).await,
            "the clone must contain the upstream's commit"
        );
        assert_eq!(
            std::fs::read_to_string(clone.join("src.rs")).expect("the cloned file"),
            "fn main() {}"
        );
    }

    /// Push through the Relay: the mirror takes the branch, and a second
    /// clone sees it — the Relay is a real remote, not a read-only cache.
    #[tokio::test]
    async fn a_push_through_the_relay_reaches_the_mirror_and_later_clones() {
        let fixture = within(
            "the relayed arrangement",
            crate::common::harness::relay_with_clone(),
        )
        .await;
        let pushed = within(
            "a commit lands in the clone",
            commit_file(&fixture.clone, "feature.rs", "pub fn f() {}", "add feature"),
        )
        .await;
        within(
            "push via relay",
            git_in(&fixture.clone, &["push", "origin", "main"]),
        )
        .await;

        assert!(
            within(
                "the mirror holds the push",
                has_commit(&fixture.store.path("atlas"), &pushed),
            )
            .await,
            "the pushed commit must be in the bare mirror"
        );

        let reader = within(
            "second clone via relay",
            clone_to(fixture.scratch.path(), &fixture.url, "reader"),
        )
        .await;
        assert!(
            within("the second clone sees it", has_commit(&reader, &pushed)).await,
            "a later clone must receive what was pushed"
        );
    }

    /// The mirror is a clone cache: a refresh fetches what the upstream
    /// gained — and deliberately does not prune, because `refs/horde/share/*`
    /// live only on the mirror and a pruning refresh would garbage-collect
    /// every cross-Worker share the upstream has never heard of.
    #[tokio::test]
    async fn a_refresh_updates_the_mirror_without_pruning_share_refs() {
        let fixture = within(
            "the relayed arrangement",
            crate::common::harness::relay_with_clone(),
        )
        .await;
        let mirror = fixture.store.path("atlas");
        let head = within("head", git_in(&fixture.clone, &["rev-parse", "HEAD"])).await;
        within(
            "a share ref lands on the mirror",
            git_in(
                &fixture.clone,
                &["push", "origin", &format!("{head}:refs/horde/share/{head}")],
            ),
        )
        .await;

        // The upstream moves on without the mirror hearing about it.
        let upstream = within(
            "the upstream gains a commit",
            commit_file(
                &fixture.origin,
                "upstream.rs",
                "// upstream",
                "upstream moves",
            ),
        )
        .await;
        assert!(
            !within("the mirror is stale", has_commit(&mirror, &upstream)).await,
            "before the refresh, the mirror must not have it yet"
        );

        within("refresh", fixture.store.refresh("atlas"))
            .await
            .expect("the refresh");
        assert!(
            within("the mirror caught up", has_commit(&mirror, &upstream)).await,
            "the refresh must fetch what the upstream gained"
        );
        let refs = within("the mirror lists refs", git_in(&mirror, &["show-ref"])).await;
        assert!(
            refs.contains(&format!("refs/horde/share/{head}")),
            "the refresh must not prune the share ref: {refs}"
        );
    }

    /// Dropping the server closes the listener. A dropped Relay that kept
    /// serving would leak a live socket — and every port it was ever given —
    /// into the rest of the Server's life.
    #[tokio::test]
    async fn dropping_the_server_closes_the_listener() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        let (server, _store, _mirrors) = serve_relay(&[("atlas", &origin)]).await;
        let addr = server.addr();

        let alive = within(
            "a request while alive",
            raw_status_of(addr, "GET / HTTP/1.1"),
        )
        .await;
        assert!(
            alive.contains("404"),
            "the live server must answer (with a refusal): {alive:?}"
        );

        drop(server);

        // Abort lands asynchronously, so the check is bounded by attempts —
        // progress by count, not by clock (docs/QUALITY_GATES.md).
        within("the listener closes", async {
            let mut attempts = 0_u32;
            while tokio::net::TcpStream::connect(addr).await.is_ok() {
                attempts += 1;
                assert!(
                    attempts < 100,
                    "the listener must stop accepting once the server is dropped"
                );
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
    }

    /// Unregistered names are refused. The Relay serves registered repos
    /// only; an unknown name mirroring nothing must read as absence, not as
    /// an empty repo a client could push into by accident.
    #[tokio::test]
    async fn an_unregistered_repo_is_not_found() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        let (server, _store, _mirrors) = serve_relay(&[("atlas", &origin)]).await;

        let stderr = within(
            "clone of an unknown repo fails",
            git_in_failing(
                scratch.path(),
                &[
                    "clone",
                    &server.location().repo_url("ghost"),
                    "never-created",
                ],
            ),
        )
        .await;
        assert!(
            stderr.contains("not found") || stderr.contains("404"),
            "the client must be told the repo does not exist: {stderr}"
        );
        assert!(
            !scratch.path().join("never-created").join(".git").exists(),
            "no repo may come into being from a failed clone"
        );
    }

    /// One raw HTTP request, one raw answer — pinning the refusal status for
    /// requests no git client would send but any port scanner will.
    async fn raw_status_of(addr: std::net::SocketAddr, request_line: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connects");
        stream
            .write_all(
                format!("{request_line}\r\nHost: relay\r\nConnection: close\r\n\r\n").as_bytes(),
            )
            .await
            .expect("writes");
        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .expect("reads to close");
        String::from_utf8_lossy(&response)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned()
    }

    #[tokio::test]
    async fn hostile_paths_are_refused_with_not_found() {
        let scratch = tempfile::tempdir().expect("a scratch dir");
        let origin = within("origin init", init_repo(scratch.path(), "origin")).await;
        let (server, store, _mirrors) = serve_relay(&[("atlas", &origin)]).await;

        for hostile in [
            "GET /../../etc/passwd HTTP/1.1",
            "GET /%2e%2e/info/refs?service=git-upload-pack HTTP/1.1",
            "GET /atlas/info/refs?service=git-evil-pack HTTP/1.1",
            "DELETE /atlas/git-upload-pack HTTP/1.1",
        ] {
            let status = within("a raw refusal", raw_status_of(server.addr(), hostile)).await;
            assert!(
                status.contains("404"),
                "{hostile:?} must be refused with 404, got {status:?}"
            );
        }
        assert!(
            !store.path("atlas").exists(),
            "refused requests must not materialize anything"
        );
    }
}
