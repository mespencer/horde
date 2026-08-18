//! What both service spines share.
//!
//! Lives beside the spines rather than in their parent module: the parent
//! re-exports both spines, so anything the spines import from it would close a
//! module cycle.

use std::sync::Arc;

use horde_proto::v1::{Event, event::Kind};
use prost::Message;

use crate::error::ServerError;
use crate::forward::ForwardRegistry;
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
    /// The content-addressed attachment store, when this server has a state
    /// directory to keep one in. `None` makes the blob verbs answer
    /// `Unimplemented` honestly.
    pub blobs: Option<Arc<crate::blob::BlobStore>>,
    /// Serializes placement decisions against their appends. Two concurrent
    /// passes — a spawn and a drain woken by a registration — each project the
    /// log and then write placements; unserialized, both can place the same
    /// agent and the fold would command two spawns. The lock makes
    /// project-then-append atomic per decision. It guards no data, so a
    /// restart losing it loses nothing.
    pub placement_lock: Arc<tokio::sync::Mutex<()>>,
    /// The forward plane's connection-liveness state (ADR-0006): which
    /// workers and clients hold live forward streams, and the tunnel legs
    /// awaiting their pair. Ephemeral on purpose — realization is tied to
    /// connections, so a restart honestly forgetting all of it is correct.
    pub forwards: ForwardRegistry,
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
            blobs: None,
            placement_lock: Arc::new(tokio::sync::Mutex::new(())),
            forwards: ForwardRegistry::new(),
        }
    }

    /// Replaces the scheduler. Wave 4 swaps in the real one here.
    #[must_use]
    pub fn with_scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = scheduler;
        self
    }

    /// Attaches the blob store — the serving path does, and any test that
    /// wants real attachment transfer.
    #[must_use]
    pub fn with_blob_store(mut self, blobs: Arc<crate::blob::BlobStore>) -> Self {
        self.blobs = Some(blobs);
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

/// This build's semver, echoed in every handshake.
pub const SERVER_SEMVER: &str = env!("CARGO_PKG_VERSION");

/// A unique-enough identifier without pulling in a uuid dependency.
///
/// Ids are opaque to every consumer; the only requirement is that two calls
/// never collide within a server's lifetime, which a monotonic counter over a
/// process-start nonce satisfies.
pub(crate) fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static START: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

    let start = *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64)
    });
    let ordinal = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{start:x}-{ordinal:x}")
}

/// How often a long-lived stream re-checks the deny-list.
///
/// Streams are the one place the per-RPC interceptor cannot reach: a
/// `StreamCommands`, `PushEvents`, or `SyncEvents` stream lives for its
/// peer's whole connection, so revocation would otherwise wait for a
/// reconnect (DESIGN §3, wave-7 fix a4). A quarter second is far below any
/// human revocation-to-effect expectation and costs one atomic read per tick
/// per stream.
pub(crate) const REVOCATION_RECHECK_PERIOD: std::time::Duration =
    std::time::Duration::from_millis(250);

/// The serial of the certificate this request arrived under, if the mTLS
/// interceptor stamped one. Plaintext test channels have none, and for them
/// [`severed`] never resolves — there is no certificate to revoke.
pub(crate) fn peer_serial<T>(request: &tonic::Request<T>) -> Option<String> {
    request
        .extensions()
        .get::<horde_identity::PeerIdentity>()
        .map(|identity| identity.serial_hex.clone())
}

/// Resolves when this stream's certificate is revoked — the severing half of
/// revocation. Select the stream's pump against this; when it resolves, end
/// the stream with the same `PermissionDenied` the interceptor gives unary
/// calls. Never resolves without both a deny-list and a serial (a plaintext
/// or unsecured server has neither, and severing nothing is correct there).
pub(crate) async fn severed(deny: Option<horde_identity::RevocationList>, serial: Option<String>) {
    let (Some(deny), Some(serial)) = (deny, serial) else {
        return std::future::pending().await;
    };
    loop {
        if deny.is_revoked(&serial) {
            return;
        }
        tokio::time::sleep(REVOCATION_RECHECK_PERIOD).await;
    }
}

/// The refusal a severed stream ends with: the same code and vocabulary the
/// interceptor uses for a revoked unary call, so a peer sees one story.
pub(crate) fn severed_status(serial: Option<&str>) -> tonic::Status {
    tonic::Status::permission_denied(format!(
        "certificate {} has been revoked; this stream is severed",
        serial.unwrap_or("<unknown>")
    ))
}

/// Races one stream's pump against revocation, ending the stream with the
/// severed refusal when revocation wins (fix a4). One helper for all three
/// long-lived streams — `StreamCommands`, `PushEvents`, `SyncEvents` — so
/// their severing cannot drift apart. Cancelling a pump mid-step is safe
/// precisely because the stream ends here: its cursors die with it.
pub(crate) async fn run_severable<T, F>(
    pump: F,
    deny: Option<horde_identity::RevocationList>,
    serial: Option<String>,
    sender: &tokio::sync::mpsc::Sender<Result<T, tonic::Status>>,
) where
    F: Future<Output = ()>,
{
    tokio::select! {
        () = pump => {}
        () = severed(deny, serial.clone()) => {
            let _ = sender.send(Err(severed_status(serial.as_deref()))).await;
        }
    }
}

/// Below this, a peer is refused outright (DESIGN.md §2). Enforced by
/// [`version_gate`] at both handshakes — `Hello` on the control plane and
/// `RegisterWorker` on the worker spine.
pub const MIN_SUPPORTED_SEMVER: &str = "0.1.0";

/// [`MIN_SUPPORTED_SEMVER`] as the triple comparisons run on. A test pins
/// the two together so the string in refusal messages cannot drift from the
/// number actually enforced.
const MINIMUM_TRIPLE: (u64, u64, u64) = (0, 1, 0);

/// Enforces the version handshake (DESIGN §2, wave-7 fix a6): a peer must
/// say what it is, and what it says must be a semver at or above
/// [`MIN_SUPPORTED_SEMVER`].
///
/// `FailedPrecondition` for every refusal — the code the worker daemon's
/// fate table treats as fatal, which is right: neither a missing Hello nor
/// an old build heals by redialing.
///
/// # Errors
///
/// A refusal naming what was missing, unreadable, or too old — and the
/// minimum, so the operator knows what to upgrade to.
pub fn version_gate(hello: Option<&horde_proto::v1::Hello>) -> Result<(), tonic::Status> {
    let Some(hello) = hello else {
        return Err(tonic::Status::failed_precondition(format!(
            "this handshake carried no Hello; peers below {MIN_SUPPORTED_SEMVER} \
             (or saying nothing) are refused"
        )));
    };
    let Some(version) = parse_semver(&hello.semver) else {
        return Err(tonic::Status::failed_precondition(format!(
            "{:?} is not a semver this server can compare; the minimum supported \
             version is {MIN_SUPPORTED_SEMVER}",
            hello.semver
        )));
    };
    if version < MINIMUM_TRIPLE {
        return Err(tonic::Status::failed_precondition(format!(
            "version {} is below the minimum supported {MIN_SUPPORTED_SEMVER}; \
             upgrade the peer",
            hello.semver
        )));
    }
    Ok(())
}

/// `MAJOR.MINOR.PATCH` as a comparable triple, or `None` for anything else.
///
/// Deliberately no pre-release or build-metadata handling: every version
/// Horde has ever stamped is a bare triple, and accepting more would let a
/// typo compare as valid.
fn parse_semver(semver: &str) -> Option<(u64, u64, u64)> {
    let mut parts = semver.split('.');
    let triple = (
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
        parts.next()?.parse().ok()?,
    );
    parts.next().is_none().then_some(triple)
}

/// A human-readable name for an event kind.
///
/// Only for diagnostics, which is why it is a `&'static str` rather than
/// anything the wire carries. The names come from the compiled schema through
/// [`horde_proto::kind_field_name`], so they cannot drift from `event.proto`
/// and no hand-kept match has to grow with the oneof. `empty` covers both a
/// kindless event and a kind the descriptor cannot name — either way, an
/// event this build cannot describe must not collide under a neighbour's
/// name.
#[must_use]
pub fn kind_name(kind: Option<&Kind>) -> &'static str {
    kind.and_then(horde_proto::kind_field_name)
        .unwrap_or("empty")
}

/// Appends a server-originated event: no origin worker, no journal sequence.
///
/// The one append shape everything server-side shares — the control plane's
/// verbs, the placement passes, and the permission chain all write through
/// here, so the kind-collision refusal below cannot be forgotten by any of
/// them.
///
/// # Errors
///
/// Whatever the log refuses, plus
/// [`ServerError::IdempotencyKeyCollision`] when the key already holds a
/// different kind of event, and [`ServerError::InlineBulk`] when the event
/// smuggles attachment content past the inline threshold.
pub async fn append_server_event(
    log: &EventLog,
    session_id: &str,
    idempotency_key: &str,
    event: Event,
) -> Result<u64, ServerError> {
    append_server_event_returning(log, session_id, idempotency_key, event)
        .await
        .map(|appended| appended.sequence)
}

/// [`append_server_event`], keeping the whole [`Appended`] — for callers that
/// must read a deduplicated original back (a retried `OpenForward` returns
/// the forward id the *stored* event minted, not a fresh one).
///
/// # Errors
///
/// Exactly [`append_server_event`]'s.
pub async fn append_server_event_returning(
    log: &EventLog,
    session_id: &str,
    idempotency_key: &str,
    event: Event,
) -> Result<Appended, ServerError> {
    refuse_inline_bulk(&event)?;
    let appended = log
        .append(PendingEvent {
            session_id: session_id.to_owned(),
            origin_worker_id: String::new(),
            journal_sequence: None,
            idempotency_key: idempotency_key.to_owned(),
            payload: event.encode_to_vec(),
        })
        .await?;

    refuse_kind_collision(&appended, session_id, idempotency_key, &event)?;
    Ok(appended)
}

/// Refuses an event that smuggles attachment bulk past the inline threshold.
///
/// Events carry blob references and hashes, never bulk (ADR-0001): the log
/// is replayed to every attached client for the life of the session, so an
/// event's size is paid over and over, and the Blob Store exists precisely
/// so that large content is fetched once, lazily, by whoever wants it. Both
/// append chokepoints — server-originated events and the worker journal
/// ingest — run this before anything is stored, because a refusal at the
/// write is diagnosable where a bloated log is a tax nobody traces back.
///
/// # Errors
///
/// [`ServerError::InlineBulk`], naming the attachment, its size, and the
/// remedy.
pub fn refuse_inline_bulk(event: &Event) -> Result<(), ServerError> {
    match horde_proto::blob::smuggled_bulk(event) {
        None => Ok(()),
        Some(attachment) => Err(ServerError::InlineBulk {
            name: attachment.name.clone(),
            inline_bytes: attachment.inline.len() as u64,
            threshold: horde_proto::blob::INLINE_THRESHOLD_BYTES,
        }),
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
            Kind::EnrollmentCodeMinted(EnrollmentCodeMinted::default()),
            Kind::IdentityEnrolled(IdentityEnrolled::default()),
            Kind::EnrollmentRevoked(EnrollmentRevoked::default()),
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

    /// The version gate's truth table (fix a6): no Hello, an unreadable
    /// version, and a version below the minimum all refuse with
    /// FailedPrecondition — the code the worker daemon treats as fatal — and
    /// the minimum rides every refusal so the operator knows the target.
    #[test]
    fn the_version_gate_refuses_missing_unreadable_and_old_peers() {
        use super::version_gate;
        let hello = |semver: &str| horde_proto::v1::Hello {
            semver: semver.to_owned(),
            ..horde_proto::v1::Hello::default()
        };

        assert!(
            version_gate(Some(&hello("0.1.0"))).is_ok(),
            "the floor passes"
        );
        assert!(version_gate(Some(&hello("0.2.7"))).is_ok());
        assert!(version_gate(Some(&hello("1.0.0"))).is_ok());

        for (peer, needle) in [
            (None, "no Hello"),
            (Some(hello("")), "not a semver"),
            (Some(hello("banana")), "not a semver"),
            (Some(hello("0.1")), "not a semver"),
            (Some(hello("0.1.0.1")), "not a semver"),
            (Some(hello("0.0.9")), "below the minimum"),
        ] {
            let refused = version_gate(peer.as_ref()).expect_err("must refuse");
            assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
            assert!(
                refused.message().contains(needle),
                "{peer:?} must be refused naming {needle}: {}",
                refused.message()
            );
            assert!(
                refused.message().contains(super::MIN_SUPPORTED_SEMVER),
                "every refusal names the minimum: {}",
                refused.message()
            );
        }
    }

    /// The string in refusal messages and the triple actually enforced are
    /// two spellings of one number; this is what keeps them one.
    #[test]
    fn the_advertised_minimum_is_the_enforced_minimum() {
        assert_eq!(
            super::parse_semver(super::MIN_SUPPORTED_SEMVER),
            Some(super::MINIMUM_TRIPLE)
        );
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
