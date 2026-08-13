//! Workspaces: the server-owned Definition catalog and the Instance cache.
//!
//! The two halves have opposite truths, and this module keeps them apart on
//! purpose:
//!
//! * **Definitions are server-owned** (DESIGN §7), and ADR-0001 admits no
//!   store for server state but the event log — so the catalog is a fold over
//!   `WorkspaceDefinitionChanged` events in a reserved catalog stream, exactly
//!   like sessions are folds over their own streams. There is no definitions
//!   table that could disagree with the log.
//! * **Instances are worker-owned** ("the worker-local state file is the
//!   truth"), and the server's view is explicitly a cache. A cache that dies
//!   with the process and is refilled by the next report is honest; writing it
//!   anywhere durable would promote it to a second truth.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use horde_proto::v1::{
    Event, WorkspaceDefinition, WorkspaceInstanceReport, WorkspaceInstanceState, event::Kind,
};
use prost::Message;

use crate::error::ServerError;
use crate::log::StoredEvent;

/// The reserved stream the Definition catalog lives in.
///
/// Server-assigned session ids are `session-…`, so nothing user-created can
/// collide with it. It is a stream, not a Session: [`is_catalog_stream`] keeps
/// it out of every session listing.
pub const CATALOG_STREAM: &str = "horde-workspace-catalog";

/// Whether a log stream is the workspace catalog rather than a session.
///
/// A named predicate rather than an inline comparison because both of its
/// callers (session listing, agent lookup) would otherwise re-derive it, and
/// two spellings of one rule is how a reserved stream leaks into a UI.
#[must_use]
pub fn is_catalog_stream(session_id: &str) -> bool {
    session_id == CATALOG_STREAM
}

/// One Definition as the catalog fold currently sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct DefinitionRecord {
    /// The full post-change Definition, with `etag` already stamped from the
    /// catalog stream sequence of the change that produced it.
    pub definition: WorkspaceDefinition,
    pub deleted: bool,
}

/// The Definition catalog, folded from the catalog stream.
///
/// Pure, like `SessionState`: same events in, same catalog out. Etags are the
/// catalog stream sequence of each definition's latest change, which makes
/// them deterministic across replays — an optimistic-concurrency token that a
/// restart cannot invalidate.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WorkspaceCatalog {
    records: BTreeMap<String, DefinitionRecord>,
}

impl WorkspaceCatalog {
    /// Folds catalog events into the catalog.
    ///
    /// Events that are not `WorkspaceDefinitionChanged`, or that carry no
    /// definition or no id, are ignored rather than fatal: one malformed event
    /// must not take down every workspace operation.
    pub fn project(events: &[StoredEvent]) -> Result<Self, ServerError> {
        let mut catalog = Self::default();
        for stored in events {
            catalog.apply(stored)?;
        }
        Ok(catalog)
    }

    fn apply(&mut self, stored: &StoredEvent) -> Result<(), ServerError> {
        let event = Event::decode(stored.payload.as_slice())?;
        let Some(Kind::WorkspaceDefinitionChanged(changed)) = event.kind else {
            return Ok(());
        };
        let Some(mut definition) = changed.definition else {
            return Ok(());
        };
        let Some(id) = definition
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
        else {
            return Ok(());
        };

        definition.etag = stored.sequence.to_string();
        self.records.insert(
            id,
            DefinitionRecord {
                definition,
                deleted: changed.deleted,
            },
        );
        Ok(())
    }

    /// A live definition by id.
    #[must_use]
    pub fn get(&self, id: &str) -> Option<&WorkspaceDefinition> {
        self.records
            .get(id)
            .filter(|record| !record.deleted)
            .map(|record| &record.definition)
    }

    /// A live definition by name.
    #[must_use]
    pub fn get_by_name(&self, name: &str) -> Option<&WorkspaceDefinition> {
        self.live().find(|definition| definition.name == name)
    }

    /// Every live definition, in id order.
    pub fn live(&self) -> impl Iterator<Item = &WorkspaceDefinition> {
        self.records
            .values()
            .filter(|record| !record.deleted)
            .map(|record| &record.definition)
    }
}

/// Refuses an ill-formed definition, in words the caller can fix.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the first shortfall: an empty name, no
/// repos, a repo without a name or remote, or two repos sharing a name.
pub fn validate_definition(
    name: &str,
    repos: &[horde_proto::v1::WorkspaceRepo],
) -> Result<(), ServerError> {
    if name.trim().is_empty() {
        return Err(ServerError::Invalid(
            "a workspace definition needs a name".to_owned(),
        ));
    }
    if repos.is_empty() {
        return Err(ServerError::Invalid(
            "a workspace definition needs at least one repo".to_owned(),
        ));
    }
    let mut names = std::collections::BTreeSet::new();
    for repo in repos {
        if repo.name.trim().is_empty() {
            return Err(ServerError::Invalid("every repo needs a name".to_owned()));
        }
        if repo.remote_url.trim().is_empty() {
            return Err(ServerError::Invalid(format!(
                "repo {} needs a remote url",
                repo.name
            )));
        }
        if !names.insert(repo.name.clone()) {
            return Err(ServerError::Invalid(format!(
                "repo name {} is used twice; GitRef and layout address repos by name",
                repo.name
            )));
        }
    }
    Ok(())
}

/// Defaults each repo's layout path to its name, so a single-repo definition
/// needs no layout ceremony and a multi-repo one still composes.
#[must_use]
pub fn normalized_repos(
    repos: Vec<horde_proto::v1::WorkspaceRepo>,
) -> Vec<horde_proto::v1::WorkspaceRepo> {
    repos
        .into_iter()
        .map(|mut repo| {
            if repo.layout_path.trim().is_empty() {
                repo.layout_path = repo.name.clone();
            }
            repo
        })
        .collect()
}

/// Refuses an update whose repos add, remove, or repoint — the repo set is
/// fixed at definition time and only `AddWorkspaceRepo` widens it. Layout
/// paths are the one thing an update's repos may change.
///
/// # Errors
///
/// [`ServerError::Invalid`] naming the repo that changed.
pub fn same_repo_set(
    current: &[horde_proto::v1::WorkspaceRepo],
    updated: &[horde_proto::v1::WorkspaceRepo],
) -> Result<(), ServerError> {
    let identity = |repos: &[horde_proto::v1::WorkspaceRepo]| {
        repos
            .iter()
            .map(|repo| {
                (
                    repo.name.clone(),
                    (repo.remote_url.clone(), repo.default_branch.clone()),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
    };
    let current = identity(current);
    let updated = identity(updated);
    if current == updated {
        return Ok(());
    }
    Err(ServerError::Invalid(
        "an update may change repo layout only; adding, removing, or \
         repointing repos goes through AddWorkspaceRepo or a new definition"
            .to_owned(),
    ))
}

/// Enforces the optimistic-concurrency token on a mutation.
///
/// # Errors
///
/// [`ServerError::StaleEtag`] naming both tokens, so the caller knows to
/// re-read rather than re-send.
pub fn check_etag(definition: &WorkspaceDefinition, supplied: &str) -> Result<(), ServerError> {
    if definition.etag == supplied {
        return Ok(());
    }
    Err(ServerError::StaleEtag {
        workspace_definition_id: definition
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        current: definition.etag.clone(),
        supplied: supplied.to_owned(),
    })
}

/// The server's cached view of Workspace Instances across workers.
///
/// Fed by worker reports (`ReportWorkspaceInstances`, capability
/// advertisements, journaled `WorkspaceInstanceChanged` events) and read by
/// placement for warmth. Deliberately in-memory: the worker-local state file
/// is the truth, and a worker re-reports on connect, so nothing here needs to
/// survive a restart — and nothing here *may* claim to.
#[derive(Debug, Clone, Default)]
pub struct InstanceCache {
    /// Keyed `(worker_id, workspace_definition_id)`.
    reports: Arc<Mutex<BTreeMap<(String, String), WorkspaceInstanceReport>>>,
}

impl InstanceCache {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records one report, replacing what was cached for that pair. A report
    /// of `MISSING` removes the entry: the worker no longer has it, so caching
    /// it would keep warmth pointing at a wiped disk.
    pub fn record(&self, report: &WorkspaceInstanceReport) {
        let Some(key) = key_of(report) else {
            return;
        };
        if let Ok(mut reports) = self.reports.lock() {
            if report.state == WorkspaceInstanceState::Missing as i32 {
                reports.remove(&key);
            } else {
                reports.insert(key, report.clone());
            }
        }
    }

    /// Replaces everything cached for one worker with its authoritative set.
    pub fn replace_worker(&self, worker_id: &str, worker_reports: &[WorkspaceInstanceReport]) {
        if let Ok(mut reports) = self.reports.lock() {
            reports.retain(|(worker, _), _| worker != worker_id);
        }
        for report in worker_reports {
            self.record(report);
        }
    }

    /// Cached reports for one definition, across workers.
    #[must_use]
    pub fn for_definition(&self, workspace_definition_id: &str) -> Vec<WorkspaceInstanceReport> {
        self.reports
            .lock()
            .map(|reports| {
                reports
                    .iter()
                    .filter(|((_, definition), _)| definition == workspace_definition_id)
                    .map(|(_, report)| report.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether a worker already holds (or is building) an instance of a
    /// definition. Materializing counts: placement colocates onto it rather
    /// than starting a second materialization elsewhere.
    #[must_use]
    pub fn warm_on(&self, worker_id: &str, workspace_definition_id: &str) -> bool {
        self.reports
            .lock()
            .ok()
            .and_then(|reports| {
                reports
                    .get(&(worker_id.to_owned(), workspace_definition_id.to_owned()))
                    .map(|report| is_warm(report.state))
            })
            .unwrap_or(false)
    }
}

/// The cache key a report identifies itself by, when it does.
fn key_of(report: &WorkspaceInstanceReport) -> Option<(String, String)> {
    let worker = report.worker_id.as_ref().map(ToString::to_string)?;
    let definition = report
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)?;
    Some((worker, definition))
}

/// Whether a cached state counts as warmth for placement.
///
/// Ready and materializing both do — colocating onto an instance mid-build is
/// the point of recording the prewarm. Degraded does not: placing onto a
/// broken checkout would hand an agent a workspace that does not match its
/// definition. A named predicate because the distinction is exactly the kind
/// of compound condition a passing end-to-end run never separates.
#[must_use]
pub fn is_warm(state: i32) -> bool {
    state == WorkspaceInstanceState::Ready as i32
        || state == WorkspaceInstanceState::Materializing as i32
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        WorkerId, WorkspaceDefinitionChanged, WorkspaceDefinitionId, WorkspaceRepo,
    };

    use super::*;

    fn stored(sequence: u64, kind: Kind) -> StoredEvent {
        StoredEvent {
            session_id: CATALOG_STREAM.to_owned(),
            sequence,
            recorded_at_unix_ms: 0,
            payload: Event {
                agent_id: None,
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(kind),
            }
            .encode_to_vec(),
        }
    }

    fn definition(id: &str, name: &str) -> WorkspaceDefinition {
        WorkspaceDefinition {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(id)),
            name: name.to_owned(),
            repos: vec![WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: "https://example.com/atlas.git".to_owned(),
                default_branch: "main".to_owned(),
                layout_path: "atlas".to_owned(),
            }],
            unmanaged_paths: Vec::new(),
            created_at: None,
            etag: String::new(),
        }
    }

    fn changed(sequence: u64, id: &str, name: &str, deleted: bool) -> StoredEvent {
        stored(
            sequence,
            Kind::WorkspaceDefinitionChanged(WorkspaceDefinitionChanged {
                definition: Some(definition(id, name)),
                deleted,
            }),
        )
    }

    #[test]
    fn only_the_catalog_stream_is_the_catalog_stream() {
        assert!(is_catalog_stream(CATALOG_STREAM));
        assert!(!is_catalog_stream("session-1"));
        assert!(!is_catalog_stream(""));
    }

    // -- Validation --------------------------------------------------------

    fn repo(name: &str, remote: &str) -> WorkspaceRepo {
        WorkspaceRepo {
            name: name.to_owned(),
            remote_url: remote.to_owned(),
            default_branch: "main".to_owned(),
            layout_path: String::new(),
        }
    }

    /// Each refusal names its shortfall; the message is what a caller fixes.
    #[test]
    fn an_ill_formed_definition_is_refused_with_the_shortfall_named() {
        let refusal = |name: &str, repos: &[WorkspaceRepo]| {
            validate_definition(name, repos)
                .expect_err("must refuse")
                .to_string()
        };

        assert!(refusal("", &[repo("a", "url")]).contains("name"));
        assert!(refusal("  ", &[repo("a", "url")]).contains("name"));
        assert!(refusal("ws", &[]).contains("at least one repo"));
        assert!(refusal("ws", &[repo("", "url")]).contains("name"));
        assert!(refusal("ws", &[repo("a", "")]).contains("remote"));
        assert!(
            refusal("ws", &[repo("a", "u1"), repo("a", "u2")]).contains("twice"),
            "two repos under one name would make GitRef ambiguous"
        );

        validate_definition("ws", &[repo("a", "u1"), repo("b", "u2")]).expect("a sound definition");
    }

    #[test]
    fn normalization_defaults_layout_to_the_repo_name_and_keeps_explicit_layouts() {
        let mut explicit = repo("atlas", "url");
        explicit.layout_path = "code/atlas".to_owned();
        let normalized = normalized_repos(vec![repo("docs", "url"), explicit]);
        assert_eq!(normalized[0].layout_path, "docs");
        assert_eq!(normalized[1].layout_path, "code/atlas");
    }

    /// Layout is the one thing an update's repos may change; everything else
    /// is an add/remove/repoint and goes through the ceremonial verbs.
    #[test]
    fn an_update_may_relayout_but_not_reshape_the_repo_set() {
        let current = vec![repo("a", "u1"), repo("b", "u2")];

        let mut relaid = current.clone();
        relaid[0].layout_path = "elsewhere/a".to_owned();
        same_repo_set(&current, &relaid).expect("layout changes are the point of update");

        let widened = vec![repo("a", "u1"), repo("b", "u2"), repo("c", "u3")];
        assert!(same_repo_set(&current, &widened).is_err(), "no additions");

        let narrowed = vec![repo("a", "u1")];
        assert!(same_repo_set(&current, &narrowed).is_err(), "no removals");

        let repointed = vec![repo("a", "u1"), repo("b", "somewhere-else")];
        assert!(
            same_repo_set(&current, &repointed).is_err(),
            "repointing a remote is not a layout change"
        );
    }

    #[test]
    fn the_etag_gate_admits_only_the_current_token() {
        let mut held = definition("ws-1", "atlas");
        held.etag = "7".to_owned();

        check_etag(&held, "7").expect("the current token passes");
        let stale = check_etag(&held, "3").expect_err("a stale token aborts");
        assert!(matches!(stale, ServerError::StaleEtag { .. }));
        assert!(
            check_etag(&held, "").is_err(),
            "an empty token is not a skeleton key"
        );
    }

    #[test]
    fn a_created_definition_is_live_with_its_sequence_as_etag() {
        let catalog =
            WorkspaceCatalog::project(&[changed(3, "ws-1", "atlas", false)]).expect("project");

        let found = catalog.get("ws-1").expect("live");
        assert_eq!(found.name, "atlas");
        assert_eq!(
            found.etag, "3",
            "the etag is the catalog sequence of the latest change, \
             deterministic across replays"
        );
        assert_eq!(
            catalog.get_by_name("atlas").map(|d| d.etag.as_str()),
            Some("3")
        );
    }

    #[test]
    fn the_latest_change_wins() {
        let catalog = WorkspaceCatalog::project(&[
            changed(1, "ws-1", "atlas", false),
            changed(2, "ws-1", "atlas-renamed", false),
        ])
        .expect("project");

        assert_eq!(
            catalog.get("ws-1").map(|d| d.name.as_str()),
            Some("atlas-renamed")
        );
        assert!(
            catalog.get_by_name("atlas").is_none(),
            "the old name is gone"
        );
    }

    #[test]
    fn a_deleted_definition_is_not_live_but_can_be_recreated() {
        let catalog = WorkspaceCatalog::project(&[
            changed(1, "ws-1", "atlas", false),
            changed(2, "ws-1", "atlas", true),
        ])
        .expect("project");
        assert!(catalog.get("ws-1").is_none());
        assert!(catalog.get_by_name("atlas").is_none());
        assert_eq!(catalog.live().count(), 0);

        let recreated = WorkspaceCatalog::project(&[
            changed(1, "ws-1", "atlas", false),
            changed(2, "ws-1", "atlas", true),
            changed(3, "ws-2", "atlas", false),
        ])
        .expect("project");
        assert_eq!(
            recreated.get_by_name("atlas").map(|d| d.etag.as_str()),
            Some("3"),
            "deletion frees the name"
        );
    }

    #[test]
    fn non_catalog_events_are_ignored_rather_than_fatal() {
        let noise = stored(
            1,
            Kind::TurnInterrupted(horde_proto::v1::TurnInterrupted::default()),
        );
        let catalog = WorkspaceCatalog::project(&[noise]).expect("project");
        assert_eq!(catalog.live().count(), 0);
    }

    #[test]
    fn an_undecodable_catalog_row_is_a_decode_error() {
        let garbage = StoredEvent {
            session_id: CATALOG_STREAM.to_owned(),
            sequence: 1,
            recorded_at_unix_ms: 0,
            payload: vec![0xff, 0xff, 0xff],
        };
        assert!(matches!(
            WorkspaceCatalog::project(&[garbage]),
            Err(ServerError::Decode(_))
        ));
    }

    // -- Instance cache ----------------------------------------------------

    fn report(
        worker: &str,
        definition: &str,
        state: WorkspaceInstanceState,
    ) -> WorkspaceInstanceReport {
        WorkspaceInstanceReport {
            workspace_definition_id: Some(WorkspaceDefinitionId::new(definition)),
            worker_id: Some(WorkerId::new(worker)),
            state: state as i32,
            root_path: format!("/workspaces/{definition}"),
            repos: Vec::new(),
            detail: String::new(),
            observed_at: None,
        }
    }

    #[test]
    fn a_recorded_instance_is_warm_on_its_worker_only() {
        let cache = InstanceCache::new();
        cache.record(&report("worker-a", "ws-1", WorkspaceInstanceState::Ready));

        assert!(cache.warm_on("worker-a", "ws-1"));
        assert!(!cache.warm_on("worker-b", "ws-1"));
        assert!(!cache.warm_on("worker-a", "ws-2"));
        assert_eq!(cache.for_definition("ws-1").len(), 1);
    }

    /// Ready and materializing are warmth; degraded and missing are not.
    /// Inline these four states collapse to "is it in the map", which every
    /// end-to-end run answers the same way.
    #[test]
    fn warmth_counts_ready_and_materializing_and_nothing_else() {
        assert!(is_warm(WorkspaceInstanceState::Ready as i32));
        assert!(is_warm(WorkspaceInstanceState::Materializing as i32));
        assert!(!is_warm(WorkspaceInstanceState::Degraded as i32));
        assert!(!is_warm(WorkspaceInstanceState::Missing as i32));
        assert!(!is_warm(WorkspaceInstanceState::Unspecified as i32));
    }

    #[test]
    fn a_missing_report_evicts_the_cached_instance() {
        let cache = InstanceCache::new();
        cache.record(&report("worker-a", "ws-1", WorkspaceInstanceState::Ready));
        cache.record(&report("worker-a", "ws-1", WorkspaceInstanceState::Missing));

        assert!(!cache.warm_on("worker-a", "ws-1"));
        assert!(cache.for_definition("ws-1").is_empty());
    }

    #[test]
    fn a_report_that_names_no_worker_or_definition_is_not_cached() {
        let cache = InstanceCache::new();
        let mut nameless = report("worker-a", "ws-1", WorkspaceInstanceState::Ready);
        nameless.worker_id = None;
        cache.record(&nameless);

        let mut definitionless = report("worker-a", "ws-1", WorkspaceInstanceState::Ready);
        definitionless.workspace_definition_id = None;
        cache.record(&definitionless);

        assert!(cache.for_definition("ws-1").is_empty());
    }

    /// An authoritative report is the worker's complete set, so the server may
    /// drop cached instances absent from it — but only that worker's.
    #[test]
    fn an_authoritative_replacement_drops_only_that_workers_stale_entries() {
        let cache = InstanceCache::new();
        cache.record(&report("worker-a", "ws-1", WorkspaceInstanceState::Ready));
        cache.record(&report("worker-a", "ws-2", WorkspaceInstanceState::Ready));
        cache.record(&report("worker-b", "ws-1", WorkspaceInstanceState::Ready));

        cache.replace_worker(
            "worker-a",
            &[report("worker-a", "ws-2", WorkspaceInstanceState::Ready)],
        );

        assert!(
            !cache.warm_on("worker-a", "ws-1"),
            "absent from the set, so dropped"
        );
        assert!(cache.warm_on("worker-a", "ws-2"));
        assert!(
            cache.warm_on("worker-b", "ws-1"),
            "another worker's cache is not touched"
        );
    }
}
