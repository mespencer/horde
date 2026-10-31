//! Mounting the git Relay in `--serve` (DESIGN §8, wave-7 fix a10).
//!
//! Before this, the Relay was a proven library no deployed process served: a
//! cross-worker share returned `NoRelay` in every real deployment. `--serve
//! --relay-bind` stands it up beside the two gRPC listeners, mirrors rooted
//! in the state directory, with its repo registry fed from the workspace
//! catalog — the definitions are where repo names meet remote URLs, so the
//! catalog is the one source that cannot drift from what workers check out.
//!
//! # Loopback only, deliberately
//!
//! The Relay speaks plain git smart-HTTP on this listener, and the workers'
//! git processes cannot yet present a client certificate (the mTLS door,
//! `RelayServer::bind_mtls`, exists and is proven — but a door nobody can
//! walk through secures nothing). Until the worker's git dial carries its
//! identity, binding beyond loopback would be exactly the unauthenticated
//! networked listener DESIGN §3 forbids, so it is refused by name rather
//! than allowed and regretted. Single-machine topologies — every worker on
//! the server's host — are served fully today.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::error::ServerError;
use crate::log::EventLog;
use crate::workspace::{CATALOG_STREAM, WorkspaceCatalog};

/// The served Relay and the task keeping its registry current.
#[derive(Debug)]
pub struct MountedRelay {
    pub server: horde_relay::RelayServer,
    follower: tokio::task::JoinHandle<()>,
}

impl Drop for MountedRelay {
    fn drop(&mut self) {
        self.follower.abort();
    }
}

/// Serves the Relay on `bind`, mirrors under `state_dir/relay`, registry fed
/// from the workspace catalog now and kept current as definitions change.
///
/// # Errors
///
/// A non-loopback bind (see the module docs), the bind itself failing, or
/// the catalog being unreadable.
pub async fn mount(
    bind: SocketAddr,
    state_dir: &Path,
    log: &EventLog,
) -> Result<MountedRelay, ServerError> {
    if !bind.ip().is_loopback() {
        return Err(ServerError::Invalid(format!(
            "--relay-bind {bind} is not loopback: the relay listener speaks \
             plain git smart-HTTP, and workers cannot present a certificate \
             to its mTLS door yet — bind 127.0.0.1 until the worker git dial \
             carries an identity"
        )));
    }

    let store = Arc::new(horde_relay::MirrorStore::new(state_dir.join("relay")));
    register_catalog(&store, log).await?;

    let server = horde_relay::RelayServer::bind(bind, Arc::clone(&store))
        .await
        .map_err(|error| ServerError::Internal(format!("binding the relay at {bind}: {error}")))?;

    // Definitions created after startup register their repos too. The
    // follower re-projects the whole catalog per notification: registration
    // is an idempotent upsert, the catalog is small, and a fold that cannot
    // drift beats a delta that can.
    let follower = tokio::spawn(follow_catalog(store, log.clone()));

    Ok(MountedRelay { server, follower })
}

/// Registers every live definition's repos, answering how many.
async fn register_catalog(
    store: &horde_relay::MirrorStore,
    log: &EventLog,
) -> Result<usize, ServerError> {
    let events = log.read_all(CATALOG_STREAM).await?;
    let catalog = WorkspaceCatalog::project(&events)?;
    let mut registered = 0_usize;
    for definition in catalog.live() {
        for repo in &definition.repos {
            match store.register(&repo.name, &repo.remote_url) {
                Ok(()) => registered += 1,
                // A name that cannot be a path segment was already refused at
                // definition time; hitting one here means an older log — skip
                // it loudly rather than refusing to serve every other repo.
                Err(error) => {
                    tracing::warn!(%error, repo = %repo.name, "skipping an unregistrable repo");
                }
            }
        }
    }
    Ok(registered)
}

/// Re-registers the catalog whenever the log moves it.
async fn follow_catalog(store: Arc<horde_relay::MirrorStore>, log: EventLog) {
    let mut notifications = log.subscribe();
    loop {
        match notifications.recv().await {
            Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                // Lagging is survivable for the reason every other follower's
                // is: the channel is a hint, the log is the truth, and the
                // re-projection reads the truth.
                if let Err(error) = register_catalog(&store, &log).await {
                    tracing::warn!(%error, "the relay registry could not re-read the catalog");
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::mount;
    use crate::log::EventLog;

    /// Journals one definition into the catalog stream under `key` — the
    /// append every catalog-shaped arrangement here shares.
    async fn journal_definition(
        log: &EventLog,
        key: &str,
        definition: horde_proto::v1::WorkspaceDefinition,
    ) {
        crate::service::shared::append_server_event(
            log,
            crate::workspace::CATALOG_STREAM,
            key,
            horde_proto::v1::Event {
                agent_id: None,
                occurred_at: None,
                idempotency_key: key.to_owned(),
                kind: Some(horde_proto::v1::event::Kind::WorkspaceDefinitionChanged(
                    horde_proto::v1::WorkspaceDefinitionChanged {
                        definition: Some(definition),
                        deleted: false,
                    },
                )),
            },
        )
        .await
        .expect("the definition journals");
    }

    /// The refusal that keeps DESIGN §3 true: no unauthenticated networked
    /// listener. The message names both the reason and the fix.
    #[tokio::test]
    async fn a_non_loopback_relay_bind_is_refused_by_name() {
        let log = EventLog::in_memory().expect("an in-memory log");
        let state = tempfile::tempdir().expect("a state dir");
        let refused = mount("0.0.0.0:0".parse().expect("an address"), state.path(), &log)
            .await
            .expect_err("a wildcard bind must be refused");
        let message = refused.to_string();
        assert!(message.contains("loopback"), "{message}");
        assert!(message.contains("--relay-bind"), "{message}");
    }

    /// Mounting serves a real socket and feeds the registry from the
    /// catalog: a repo defined before startup is fetchable through the
    /// mounted relay without any test plumbing touching the store.
    #[tokio::test]
    async fn a_mounted_relay_serves_the_catalogs_repos() {
        let log = EventLog::in_memory().expect("an in-memory log");
        let state = tempfile::tempdir().expect("a state dir");

        // A real origin with one commit, defined in the catalog the way the
        // control plane defines workspaces.
        let origin = state.path().join("origin");
        std::fs::create_dir_all(&origin).expect("mkdir");
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(&origin)
                .args(args)
                .output()
                .expect("git runs");
            assert!(output.status.success(), "git {args:?}: {output:?}");
        };
        git(&["init", "-q", "-b", "main"]);
        // No detached auto-maintenance left churning the origin's objects.
        git(&[
            "-c",
            "maintenance.auto=false",
            "commit",
            "-qm",
            "seed",
            "--allow-empty",
        ]);

        // Its own names ("maps"/"cartography"), so this fixture cannot twin
        // the worker suites' atlas definitions token for token.
        let repo = horde_proto::v1::WorkspaceRepo {
            name: "maps".to_owned(),
            remote_url: origin.display().to_string(),
            default_branch: "main".to_owned(),
            layout_path: String::new(),
        };
        let definition = horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            name: "cartography".to_owned(),
            etag: "1".to_owned(),
            repos: vec![repo],
            ..horde_proto::v1::WorkspaceDefinition::default()
        };
        journal_definition(&log, "ws-1:1", definition).await;

        let mounted = mount(
            "127.0.0.1:0".parse().expect("an address"),
            state.path(),
            &log,
        )
        .await
        .expect("the relay mounts");

        // A real clone through the served relay: the mirror materializes
        // lazily from the registered remote on first request. Async and
        // ceilinged — a blocking clone would starve the accept loop on the
        // test's own runtime, and an unbounded one turns a defect into a
        // hang.
        let clone_dir = state.path().join("clone");
        let url = format!("http://{}/maps", mounted.server.addr());
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            tokio::process::Command::new("git")
                .args(["clone", "-q", &url, &clone_dir.display().to_string()])
                .output(),
        )
        .await
        .expect("the clone must finish rather than hang")
        .expect("git runs");
        assert!(
            output.status.success(),
            "the catalog's repo must be fetchable through the mounted relay: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        // The follower keeps the registry current: a definition created
        // *after* mounting becomes fetchable too. Bounded retries, because
        // the follower reacts to a notification rather than the append.
        let late = horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-2")),
            name: "late-charts".to_owned(),
            etag: "2".to_owned(),
            repos: vec![horde_proto::v1::WorkspaceRepo {
                name: "charts".to_owned(),
                remote_url: origin.display().to_string(),
                default_branch: "main".to_owned(),
                layout_path: String::new(),
            }],
            ..horde_proto::v1::WorkspaceDefinition::default()
        };
        journal_definition(&log, "ws-2:1", late).await;

        let late_url = format!("http://{}/charts", mounted.server.addr());
        let late_clone = state.path().join("late-clone");
        let mut cloned = false;
        for _ in 0..100_u32 {
            let output = tokio::process::Command::new("git")
                .args(["clone", "-q", &late_url, &late_clone.display().to_string()])
                .output()
                .await
                .expect("git runs");
            if output.status.success() {
                cloned = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            cloned,
            "a post-mount definition must become fetchable — the follower's whole job"
        );

        // Dropping the mount stops the follower with it: a leaked follower
        // would hold the log subscription and keep folding forever.
        let follower = mounted.follower.abort_handle();
        drop(mounted);
        let mut finished = false;
        for _ in 0..100_u32 {
            if follower.is_finished() {
                finished = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(finished, "drop must abort the catalog follower");
    }

    /// `register_catalog` answers how many repos it registered — the count
    /// the mount's log line reports — skipping (not failing on) a name that
    /// cannot be a path segment.
    #[tokio::test]
    async fn register_catalog_counts_what_it_registered_and_skips_the_hostile() {
        let log = EventLog::in_memory().expect("an in-memory log");
        let definition = horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
            name: "mixed".to_owned(),
            etag: "1".to_owned(),
            repos: ["maps", "charts", "-hostile"]
                .into_iter()
                .map(|name| horde_proto::v1::WorkspaceRepo {
                    name: name.to_owned(),
                    remote_url: format!("https://git.example/{name}.git"),
                    default_branch: "main".to_owned(),
                    layout_path: String::new(),
                })
                .collect(),
            ..horde_proto::v1::WorkspaceDefinition::default()
        };
        journal_definition(&log, "ws-1:1", definition).await;

        let store =
            horde_relay::MirrorStore::new(tempfile::tempdir().expect("a dir").path().join("relay"));
        let registered = super::register_catalog(&store, &log)
            .await
            .expect("the catalog reads");
        assert_eq!(registered, 2, "both valid repos, and only those, count");
        assert!(store.registered("maps").is_some());
        assert!(store.registered("charts").is_some());
        assert!(
            store.registered("-hostile").is_none(),
            "a name that cannot be a path segment is skipped, loudly"
        );
    }
}
