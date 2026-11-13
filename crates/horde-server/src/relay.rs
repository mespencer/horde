//! Mounting the git Relay in `--serve` (DESIGN §8, wave-7 fix a10).
//!
//! Before this, the Relay was a proven library no deployed process served: a
//! cross-worker share returned `NoRelay` in every real deployment. `--serve
//! --relay-bind` stands it up beside the two gRPC listeners, mirrors rooted
//! in the state directory, with its repo registry fed from the workspace
//! catalog — the definitions are where repo names meet remote URLs, so the
//! catalog is the one source that cannot drift from what workers check out.
//!
//! # Two doors
//!
//! The loopback door (`--relay-bind`) speaks plain git smart-HTTP with the
//! ambient principal, so it refuses any non-loopback bind: an unauthenticated
//! networked listener is exactly what DESIGN §3 forbids. The mTLS door
//! (`--relay-mtls-bind`) is the one other machines dial: every connection
//! presents a certificate chaining to the server's CA, revoked serials are
//! refused at the handshake, and the principal becomes `REMOTE_USER`. A
//! worker whose `--relay` is an `https://` URL presents its own enrolled
//! identity on every git dial (`horde_relay::RelayIdentity`). Both doors
//! serve the same mirrors and the same catalog-fed registry.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::error::ServerError;
use crate::log::{EventLog, Notification};
use crate::workspace::{CATALOG_STREAM, WorkspaceCatalog, is_catalog_stream};

/// The served Relay doors and the task keeping their registry current.
#[derive(Debug)]
pub struct MountedRelay {
    /// The plaintext loopback door, when `--relay-bind` asked for one.
    pub loopback: Option<horde_relay::RelayServer>,
    /// The mTLS door, when `--relay-mtls-bind` asked for one.
    pub mtls: Option<horde_relay::RelayServer>,
    follower: tokio::task::JoinHandle<()>,
}

impl Drop for MountedRelay {
    fn drop(&mut self) {
        self.follower.abort();
    }
}

/// The material the mTLS door authenticates with: the listener credential,
/// the CA every peer chains to, and the live revocation list.
#[derive(Debug, Clone)]
pub struct MtlsDoor<'a> {
    pub bind: SocketAddr,
    pub credential: &'a horde_identity::ServerCredential,
    pub ca_pem: &'a str,
    pub deny: horde_identity::RevocationList,
}

/// Which doors to open; at least one, or there is nothing to mount.
#[derive(Debug, Clone, Default)]
pub struct RelayDoors<'a> {
    pub loopback: Option<SocketAddr>,
    pub mtls: Option<MtlsDoor<'a>>,
}

impl RelayDoors<'_> {
    /// Only the loopback door.
    #[must_use]
    pub fn loopback(bind: SocketAddr) -> Self {
        Self {
            loopback: Some(bind),
            mtls: None,
        }
    }
}

/// Serves the Relay's requested doors, mirrors under `state_dir/relay`,
/// registry fed from the workspace catalog now and kept current as
/// definitions change. `None` when no door was asked for.
///
/// # Errors
///
/// A non-loopback loopback-door bind (see the module docs), either bind
/// failing, unusable TLS material, or the catalog being unreadable.
pub async fn mount(
    doors: RelayDoors<'_>,
    state_dir: &Path,
    log: &EventLog,
) -> Result<Option<MountedRelay>, ServerError> {
    if doors.loopback.is_none() && doors.mtls.is_none() {
        return Ok(None);
    }
    if let Some(bind) = doors.loopback
        && !bind.ip().is_loopback()
    {
        return Err(ServerError::Invalid(format!(
            "--relay-bind {bind} is not loopback: that door speaks plain git \
             smart-HTTP with no identity — bind 127.0.0.1 there, and serve \
             other machines through --relay-mtls-bind"
        )));
    }

    let store = Arc::new(horde_relay::MirrorStore::new(state_dir.join("relay")));
    register_catalog(&store, log).await?;

    let loopback = match doors.loopback {
        Some(bind) => Some(
            horde_relay::RelayServer::bind(bind, Arc::clone(&store))
                .await
                .map_err(|error| {
                    ServerError::Internal(format!("binding the relay at {bind}: {error}"))
                })?,
        ),
        None => None,
    };
    let mtls = match doors.mtls {
        Some(door) => Some(
            horde_relay::RelayServer::bind_mtls(
                door.bind,
                Arc::clone(&store),
                door.credential,
                door.ca_pem,
                door.deny,
            )
            .await
            .map_err(|error| {
                ServerError::Internal(format!(
                    "binding the relay's mTLS door at {}: {error}",
                    door.bind
                ))
            })?,
        ),
        None => None,
    };

    // Definitions created after startup register their repos too. The
    // follower re-projects the whole catalog per catalog notification:
    // registration is an idempotent upsert, the catalog is small, and a fold
    // that cannot drift beats a delta that can.
    let follower = tokio::spawn(follow_catalog(store, log.clone()));

    Ok(Some(MountedRelay {
        loopback,
        mtls,
        follower,
    }))
}

/// Makes the registry exactly the live catalog's repos: registers every live
/// definition's repos and unregisters every name no live definition holds —
/// a deleted definition's repos stop being served (ADR-0011). Answers how
/// many it registered.
async fn register_catalog(
    store: &horde_relay::MirrorStore,
    log: &EventLog,
) -> Result<usize, ServerError> {
    let events = log.read_all(CATALOG_STREAM).await?;
    let catalog = WorkspaceCatalog::project(&events)?;
    let mut registered = 0_usize;
    let mut live = std::collections::BTreeSet::new();
    for definition in catalog.live() {
        for repo in &definition.repos {
            match store.register(&repo.name, &repo.remote_url) {
                Ok(()) => {
                    registered += 1;
                    live.insert(repo.name.clone());
                }
                // A name that cannot be a path segment was already refused at
                // definition time; hitting one here means an older log — skip
                // it loudly rather than refusing to serve every other repo.
                Err(error) => {
                    tracing::warn!(%error, repo = %repo.name, "skipping an unregistrable repo");
                }
            }
        }
    }
    for removed in store.retain_only(&live) {
        tracing::info!(repo = %removed, "the relay stopped serving a repo no live definition holds");
    }
    Ok(registered)
}

/// Re-registers the catalog whenever the log moves it.
async fn follow_catalog(store: Arc<horde_relay::MirrorStore>, log: EventLog) {
    let mut notifications = log.subscribe();
    loop {
        match wake_for(&notifications.recv().await) {
            Wake::Reproject => {
                if let Err(error) = register_catalog(&store, &log).await {
                    tracing::warn!(%error, "the relay registry could not re-read the catalog");
                }
            }
            Wake::Ignore => {}
            Wake::Closed => return,
        }
    }
}

/// What the follower does about one thing the notification channel said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// The catalog may have moved: re-project it.
    Reproject,
    /// Another stream moved; the catalog did not.
    Ignore,
    /// The log is gone.
    Closed,
}

/// Filters the follower to the catalog stream (ADR-0013): every other
/// session's append — every output chunk of every agent — used to re-read
/// and re-fold the whole catalog only to register nothing new.
///
/// A lag still re-projects, for the reason every other follower's does: the
/// channel is a hint, the log is the truth, and a dropped notification may
/// have been the catalog's.
fn wake_for(received: &Result<Notification, tokio::sync::broadcast::error::RecvError>) -> Wake {
    match received {
        Ok(notification) if is_catalog_stream(&notification.session_id) => Wake::Reproject,
        Ok(_) => Wake::Ignore,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => Wake::Reproject,
        Err(tokio::sync::broadcast::error::RecvError::Closed) => Wake::Closed,
    }
}

#[cfg(test)]
mod tests {
    use super::mount;
    use crate::log::EventLog;

    /// The follower wakes for the catalog, a lag and a close — and sleeps
    /// through every other stream's appends.
    #[test]
    fn the_follower_wakes_only_for_the_catalog_a_lag_or_a_close() {
        use super::{Wake, wake_for};
        use tokio::sync::broadcast::error::RecvError;
        let moved = |session: &str| {
            Ok(crate::log::Notification {
                session_id: session.to_owned(),
                sequence: 1,
            })
        };
        assert_eq!(
            wake_for(&moved(crate::workspace::CATALOG_STREAM)),
            Wake::Reproject
        );
        assert_eq!(wake_for(&moved("session-1")), Wake::Ignore);
        assert_eq!(wake_for(&Err(RecvError::Lagged(3))), Wake::Reproject);
        assert_eq!(wake_for(&Err(RecvError::Closed)), Wake::Closed);
    }

    /// Journals one definition into the catalog stream under `key` — the
    /// append every catalog-shaped arrangement here shares.
    async fn journal_definition(
        log: &EventLog,
        key: &str,
        definition: horde_proto::v1::WorkspaceDefinition,
    ) {
        journal_change(log, key, definition, false).await;
    }

    /// Repos by name, each with a `git.example` remote.
    fn example_repos(names: &[&str]) -> Vec<horde_proto::v1::WorkspaceRepo> {
        names
            .iter()
            .map(|name| horde_proto::v1::WorkspaceRepo {
                name: (*name).to_owned(),
                remote_url: format!("https://git.example/{name}.git"),
                default_branch: "main".to_owned(),
                layout_path: String::new(),
            })
            .collect()
    }

    /// One catalog change, a deletion when `deleted`.
    async fn journal_change(
        log: &EventLog,
        key: &str,
        definition: horde_proto::v1::WorkspaceDefinition,
        deleted: bool,
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
                        deleted,
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
        let refused = mount(
            super::RelayDoors::loopback("0.0.0.0:0".parse().expect("an address")),
            state.path(),
            &log,
        )
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
            super::RelayDoors::loopback("127.0.0.1:0".parse().expect("an address")),
            state.path(),
            &log,
        )
        .await
        .expect("the relay mounts")
        .expect("a door was asked for");
        let served = mounted.loopback.as_ref().expect("the loopback door").addr();

        // A real clone through the served relay: the mirror materializes
        // lazily from the registered remote on first request. Async and
        // ceilinged — a blocking clone would starve the accept loop on the
        // test's own runtime, and an unbounded one turns a defect into a
        // hang.
        let clone_dir = state.path().join("clone");
        let url = format!("http://{}/maps", served);
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

        let late_url = format!("http://{}/charts", served);
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
            repos: example_repos(&["maps", "charts", "-hostile"]),
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

    /// Deletion propagates (ADR-0011): once the definition holding a repo
    /// is deleted, the registry stops serving it, while a repo another live
    /// definition still holds stays.
    #[tokio::test]
    async fn a_deleted_definitions_repos_leave_the_registry() {
        let log = EventLog::in_memory().expect("an in-memory log");
        let definition = |id: &str, repos: &[&str]| horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(id)),
            name: id.to_owned(),
            repos: example_repos(repos),
            ..horde_proto::v1::WorkspaceDefinition::default()
        };
        journal_definition(&log, "ws-1:1", definition("ws-1", &["maps", "shared"])).await;
        journal_definition(&log, "ws-2:1", definition("ws-2", &["shared"])).await;
        let store =
            horde_relay::MirrorStore::new(tempfile::tempdir().expect("a dir").path().join("relay"));
        super::register_catalog(&store, &log)
            .await
            .expect("the catalog reads");
        assert!(store.registered("maps").is_some());

        journal_change(
            &log,
            "ws-1:2",
            definition("ws-1", &["maps", "shared"]),
            true,
        )
        .await;
        super::register_catalog(&store, &log)
            .await
            .expect("the catalog reads");
        assert!(
            store.registered("maps").is_none(),
            "a repo no live definition holds is no longer served"
        );
        assert!(
            store.registered("shared").is_some(),
            "a repo another live definition still holds stays"
        );
    }
}
