//! What both service spines share.
//!
//! Lives beside the spines rather than in their parent module: the parent
//! re-exports both spines, so anything the spines import from it would close a
//! module cycle.

use std::sync::Arc;

use horde_proto::v1::{Event, event::Kind};
use prost::Message;

use crate::error::ServerError;
use crate::log::store::Appended;
use crate::log::{EventLog, PendingEvent};
use crate::scheduler::{CapabilityScheduler, Scheduler, WorkerRegistry};
use crate::workspace::InstanceCache;

/// Everything the two services share.
///
/// One value, cloned into both, so there is no question of them disagreeing
/// about which log or which registry they are talking to.
///
/// There is no command queue here any more. Commands are folded out of `log`
/// on demand by `service::commands`, so the only thing to share is the log
/// they are derived from — which is also the only thing that survives a
/// restart. The instance cache is the one exception, and it is allowed to be:
/// instances are worker-owned truth the workers re-report on connect, so the
/// cache is honestly a cache and never a second store.
#[derive(Clone)]
pub struct ServerCore {
    pub log: EventLog,
    pub workers: WorkerRegistry,
    pub instances: InstanceCache,
    pub scheduler: Arc<dyn Scheduler>,
    /// Serializes placement decisions against their appends. Two concurrent
    /// passes — a spawn and a drain woken by a registration — each project the
    /// log and then write placements; unserialized, both can place the same
    /// agent and the fold would command two spawns. The lock makes
    /// project-then-append atomic per decision. It guards no data, so a
    /// restart losing it loses nothing.
    pub placement_lock: Arc<tokio::sync::Mutex<()>>,
}

impl ServerCore {
    /// Builds a core around a log, with the capability scheduler.
    #[must_use]
    pub fn new(log: EventLog) -> Self {
        let workers = WorkerRegistry::new();
        let instances = InstanceCache::new();
        let scheduler = Arc::new(CapabilityScheduler::new(workers.clone(), instances.clone()));
        Self {
            log,
            workers,
            instances,
            scheduler,
            placement_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Replaces the scheduler. Wave 4 swaps in the real one here.
    #[must_use]
    pub fn with_scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = scheduler;
        self
    }
}

impl std::fmt::Debug for ServerCore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerCore")
            .field("workers", &self.workers.connected().len())
            .finish_non_exhaustive()
    }
}

/// Appends one server-originated event, refusing idempotency-key collisions.
///
/// The single append path for everything the server itself writes — the
/// control plane's verbs and the placement passes — so the collision refusal
/// below cannot be forgotten by a new write site.
///
/// # Errors
///
/// Storage failures, and [`ServerError::IdempotencyKeyCollision`] when the key
/// already holds a different kind of event.
pub async fn append_event(
    core: &ServerCore,
    session_id: &str,
    idempotency_key: &str,
    event: Event,
) -> Result<u64, ServerError> {
    let appended = core
        .log
        .append(PendingEvent {
            session_id: session_id.to_owned(),
            origin_worker_id: String::new(),
            journal_sequence: None,
            idempotency_key: idempotency_key.to_owned(),
            payload: event.encode_to_vec(),
        })
        .await?;

    refuse_kind_collision(&appended, session_id, idempotency_key, &event)?;

    Ok(appended.sequence)
}

/// This build's semver, echoed in every handshake.
pub const SERVER_SEMVER: &str = env!("CARGO_PKG_VERSION");

/// Below this, a peer's role is refused outright (DESIGN.md §2).
pub const MIN_SUPPORTED_SEMVER: &str = "0.1.0";

/// A human-readable name for an event kind.
///
/// Only for diagnostics, which is why it is a `&'static str` rather than
/// anything the wire carries. `event_contract.rs` owns the canonical names; a
/// drift between the two would cost a slightly worse error message and nothing
/// else, which is the right amount of coupling for a string that exists to be
/// read by a person.
#[must_use]
pub fn kind_name(kind: Option<&Kind>) -> &'static str {
    // Exhaustive rather than wildcarded, matching `event_contract.rs`: a new
    // arm on the oneof should stop compiling here too, so a kind added later
    // cannot collide under a name nobody chose for it.
    match kind {
        Some(Kind::AgentOutputChunk(_)) => "agent_output_chunk",
        Some(Kind::AgentSpawned(_)) => "agent_spawned",
        Some(Kind::AssignmentSent(_)) => "assignment_sent",
        Some(Kind::NoteSent(_)) => "note_sent",
        Some(Kind::ResultReported(_)) => "result_reported",
        Some(Kind::PermissionRequested(_)) => "permission_requested",
        Some(Kind::PermissionDecided(_)) => "permission_decided",
        Some(Kind::UsageRecorded(_)) => "usage_recorded",
        Some(Kind::WorkspaceInstanceChanged(_)) => "workspace_instance_changed",
        Some(Kind::AgentLifecycleChanged(_)) => "agent_lifecycle_changed",
        Some(Kind::SessionLifecycleChanged(_)) => "session_lifecycle_changed",
        Some(Kind::PlacementChanged(_)) => "placement_changed",
        Some(Kind::TurnInterrupted(_)) => "turn_interrupted",
        Some(Kind::WorkspaceDefinitionChanged(_)) => "workspace_definition_changed",
        Some(Kind::WorkspacePrewarmRequested(_)) => "workspace_prewarm_requested",
        None => "empty",
    }
}

/// Refuses a deduplication that silently swallowed a *different* kind of event.
///
/// # Why this is an error rather than a log line
///
/// Deduplication exists for at-least-once delivery, and a retry replays the
/// same event — so a hit whose kind differs from the stored one is never a
/// retry. It is two different events sharing a key, and the second is about to
/// disappear.
///
/// The failure that produces is uniquely hard to diagnose, which is the whole
/// argument for spending an error on it: **the write is acked, the log stays
/// silent, and the symptom appears in a different component entirely — a reader
/// that blocks forever on an event that was never written.** Nothing points
/// back at the write. Finding it once cost a dump of every event in a session
/// to notice a `result_reported` was missing where an `agent_spawned` sat.
///
/// An exact-kind hit still deduplicates silently, because that one really is
/// the system working.
///
/// # Errors
///
/// [`ServerError::IdempotencyKeyCollision`], naming both kinds and the key, so
/// the message alone is enough to fix the caller.
pub fn refuse_kind_collision(
    appended: &Appended,
    session_id: &str,
    idempotency_key: &str,
    incoming: &Event,
) -> Result<(), ServerError> {
    let Some(stored_payload) = appended.existing_payload.as_ref() else {
        return Ok(());
    };

    // A stored payload that will not decode is a corrupt row, not a collision.
    // Reported as itself rather than dressed up as one.
    let stored = Event::decode(stored_payload.as_slice())?;
    let stored_kind = kind_name(stored.kind.as_ref());
    let incoming_kind = kind_name(incoming.kind.as_ref());

    if stored_kind == incoming_kind {
        return Ok(());
    }

    Err(ServerError::IdempotencyKeyCollision {
        session_id: session_id.to_owned(),
        key: idempotency_key.to_owned(),
        sequence: appended.sequence,
        stored: stored_kind,
        incoming: incoming_kind,
    })
}

#[cfg(test)]
mod tests {
    use super::{ServerCore, kind_name, refuse_kind_collision};
    use crate::error::ServerError;
    use crate::log::EventLog;
    use crate::log::store::Appended;
    use horde_proto::v1::{Event, PermissionRequested, ResultReported, event::Kind};
    use prost::Message;

    fn event(kind: Kind) -> Event {
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }
    }

    fn deduplicated_against(stored: &Event) -> Appended {
        Appended {
            sequence: 7,
            deduplicated: true,
            existing_payload: Some(stored.encode_to_vec()),
        }
    }

    /// The bug this exists to make loud: two kinds, one key, and the second
    /// event silently gone.
    #[test]
    fn a_different_kind_under_the_same_key_is_refused() {
        let stored = event(Kind::AgentSpawned(horde_proto::v1::AgentSpawned::default()));
        let incoming = event(Kind::ResultReported(ResultReported::default()));

        let error = refuse_kind_collision(&deduplicated_against(&stored), "s1", "key-1", &incoming)
            .expect_err("a kind mismatch is a collision, never a retry");

        let rendered = error.to_string();
        assert!(
            rendered.contains("key-1"),
            "the key must be named: {rendered}"
        );
        assert!(
            rendered.contains("agent_spawned"),
            "the stored kind must be named: {rendered}"
        );
        assert!(
            rendered.contains("result_reported"),
            "the incoming kind must be named: {rendered}"
        );
        assert!(
            matches!(
                error,
                ServerError::IdempotencyKeyCollision { sequence: 7, .. }
            ),
            "the position of the event that won must be reported"
        );
    }

    /// The case that must stay silent. Worker journals and client retries are
    /// both at-least-once; making a real retry noisy would break delivery.
    #[test]
    fn a_genuine_retry_of_the_same_kind_still_deduplicates_silently() {
        let stored = event(Kind::ResultReported(ResultReported::default()));
        let incoming = event(Kind::ResultReported(ResultReported {
            // Same kind, different contents — still a retry as far as this
            // check is concerned, because only the kind distinguishes a
            // collision from a replay.
            result: Some(horde_proto::v1::AgentResult::default()),
        }));

        assert!(
            refuse_kind_collision(&deduplicated_against(&stored), "s1", "key-1", &incoming).is_ok()
        );
    }

    #[test]
    fn an_append_that_was_not_deduplicated_is_never_a_collision() {
        let appended = Appended {
            sequence: 1,
            deduplicated: false,
            existing_payload: None,
        };
        let incoming = event(Kind::PermissionRequested(PermissionRequested::default()));
        assert!(refuse_kind_collision(&appended, "s1", "key-1", &incoming).is_ok());
    }

    /// A corrupt row is its own failure and must not be reported as a
    /// collision, which would send someone looking at the caller's keys.
    #[test]
    fn an_undecodable_stored_payload_is_a_decode_error_not_a_collision() {
        let appended = Appended {
            sequence: 3,
            deduplicated: true,
            existing_payload: Some(vec![0xff, 0xff, 0xff]),
        };
        let incoming = event(Kind::ResultReported(ResultReported::default()));

        assert!(matches!(
            refuse_kind_collision(&appended, "s1", "key-1", &incoming),
            Err(ServerError::Decode(_))
        ));
    }

    /// Every arm, because a name only exists to be read in an error and an arm
    /// nobody has exercised is a name nobody has read. Also pins that no two
    /// kinds share a name — two kinds called the same thing would make the
    /// collision error say nothing at all.
    #[test]
    fn every_kind_has_its_own_name() {
        use horde_proto::v1::*;
        let all = [
            Kind::AgentOutputChunk(AgentOutputChunk::default()),
            Kind::AgentSpawned(AgentSpawned::default()),
            Kind::AssignmentSent(AssignmentSent::default()),
            Kind::NoteSent(NoteSent::default()),
            Kind::ResultReported(ResultReported::default()),
            Kind::PermissionRequested(PermissionRequested::default()),
            Kind::PermissionDecided(PermissionDecided::default()),
            Kind::UsageRecorded(UsageRecorded::default()),
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
            Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
            Kind::SessionLifecycleChanged(SessionLifecycleChanged::default()),
            Kind::PlacementChanged(PlacementChanged::default()),
            Kind::TurnInterrupted(TurnInterrupted::default()),
            Kind::WorkspaceDefinitionChanged(WorkspaceDefinitionChanged::default()),
            Kind::WorkspacePrewarmRequested(WorkspacePrewarmRequested::default()),
        ];

        let mut seen = std::collections::BTreeSet::new();
        for kind in &all {
            let name = kind_name(Some(kind));
            assert!(!name.is_empty(), "every kind needs a name");
            assert!(
                seen.insert(name),
                "{name} is used by more than one kind, which makes a collision \
                 error unreadable"
            );
        }
        assert_eq!(seen.len(), all.len());
        assert!(!seen.contains("empty"), "`empty` belongs to the None case");
    }

    #[test]
    fn every_named_kind_is_distinguishable() {
        // An error that called two different kinds by the same name would be
        // worse than no error at all.
        assert_eq!(
            kind_name(Some(&Kind::ResultReported(ResultReported::default()))),
            "result_reported"
        );
        assert_eq!(
            kind_name(Some(&Kind::PermissionRequested(
                PermissionRequested::default()
            ))),
            "permission_requested"
        );
        assert_eq!(kind_name(None), "empty");
    }

    #[test]
    fn a_core_debugs_as_how_many_workers_it_can_see() {
        let core = ServerCore::new(EventLog::in_memory().expect("in-memory log"));
        let rendered = format!("{core:?}");
        assert!(rendered.contains("ServerCore"), "{rendered}");
        assert!(
            rendered.contains('0'),
            "a fresh core has no connected workers: {rendered}"
        );
    }
}
