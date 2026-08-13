//! The `SyncEvents` stream itself: enveloping, subtree scoping, catch-up,
//! and the heartbeat that makes a dead follow stream announce itself.
//!
//! Split out of `control_plane` so the RPC method is only about resolving the
//! request — cursors, scope, refusals — and everything that happens *on* the
//! stream lives here, testable without a transport.
//!
//! # The heartbeat is a bounded wait, not a sweeper
//!
//! There is no task scanning streams on a clock. Each stream's own wait for
//! the next batch simply has a ceiling, and reaching the ceiling emits a
//! [`SyncHeartbeat`](horde_proto::v1::SyncHeartbeat) — a positive send whose
//! failure is the evidence that the client is gone, which ends the stream and
//! drops the tail. A quiet connection becomes distinguishable from a dead one
//! on both ends: the client sees heartbeats stop, the server sees the send
//! fail. This is the clock-as-ceiling shape QUALITY_GATES permits, not the
//! clock-as-progress-bound shape it forbids.

use std::collections::BTreeMap;
use std::time::Duration;

use horde_proto::v1::{
    Event, EventBatch, EventEnvelope, SessionId, SyncEventsResponse, event::Kind,
    sync_events_response,
};
use prost::Message;
use tokio::sync::mpsc::Sender;
use tonic::Status;

use crate::error::ServerError;
use crate::log::{MultiTail, StoredEvent};

/// How long a follow stream may be silent before it owes a heartbeat.
pub(crate) const DEFAULT_HEARTBEAT_PERIOD: Duration = Duration::from_secs(15);

/// Envelopes one stored event for the wire.
pub(crate) fn envelope(event: &StoredEvent) -> Result<EventEnvelope, ServerError> {
    Ok(EventEnvelope {
        session_id: Some(SessionId::new(event.session_id.clone())),
        sequence: event.sequence,
        recorded_at: Some(recorded_at(event.recorded_at_unix_ms)),
        event: Some(Event::decode(event.payload.as_slice())?),
    })
}

/// Converts the log's stored milliseconds into a wire timestamp.
///
/// The log has always recorded this; it was never put on the wire, so a client
/// could not render when anything happened or reconcile clock skew against the
/// server (DESIGN.md §2). Mutation testing found it: the stored value had no
/// reader, so replacing the clock with a constant changed nothing observable.
fn recorded_at(unix_ms: i64) -> prost_types::Timestamp {
    // Floor division, so a pre-epoch reading yields a negative second and a
    // non-negative nanosecond rather than a timestamp that reads as later than
    // it was.
    let seconds = unix_ms.div_euclid(1_000);
    let nanos = i32::try_from(unix_ms.rem_euclid(1_000)).unwrap_or(0) * 1_000_000;
    prost_types::Timestamp { seconds, nanos }
}

/// Whether the stream ends once history has been delivered.
///
/// Two independent reasons: the client hung up, or it never asked to follow.
/// Extracted because inline they are not separately reachable — a
/// non-following client also drops the stream immediately afterwards, so `&&`
/// still returns in every end-to-end run, and dropping the `!` merely inverts
/// which of two paths a passing test takes.
fn stop_after_caught_up(send_failed: bool, follow: bool) -> bool {
    send_failed || !follow
}

/// Restricts a stream to one Agent's subtree (DESIGN §11: attaching to an
/// agent scopes the view; session-root attachment sees everything).
///
/// The tree is folded from the `AgentSpawned` events that pass through the
/// stream, seeded from the session's full history first so a cursor that
/// starts mid-log still knows about agents spawned before its offset.
pub(crate) struct SubtreeFilter {
    /// `None` is the unscoped stream — everything passes.
    root: Option<String>,
    /// `agent -> parent`, per session. The parent is `None` for a root.
    parents: BTreeMap<String, BTreeMap<String, Option<String>>>,
}

impl SubtreeFilter {
    /// The filter for a stream with no subtree scope: admits everything.
    pub(crate) fn unscoped() -> Self {
        Self {
            root: None,
            parents: BTreeMap::new(),
        }
    }

    /// A filter admitting only `root`'s subtree.
    pub(crate) fn scoped(root: impl Into<String>) -> Self {
        Self {
            root: Some(root.into()),
            parents: BTreeMap::new(),
        }
    }

    /// Folds a session's stored history into the tree, so membership is
    /// judged against every spawn that precedes the stream — not only the
    /// ones the cursor happens to replay.
    pub(crate) fn seed(&mut self, session_id: &str, history: &[StoredEvent]) {
        for stored in history {
            // An event that will not decode is the stream's problem, not the
            // seed's: the tree only needs the spawns it can read.
            if let Ok(event) = Event::decode(stored.payload.as_slice()) {
                self.observe(session_id, &event);
            }
        }
    }

    /// Learns the spawn an event may carry. Called for every event, before
    /// [`Self::admits`], so a subtree member's own spawn is admitted.
    fn observe(&mut self, session_id: &str, event: &Event) {
        if let Some(Kind::AgentSpawned(spawned)) = event.kind.as_ref()
            && let Some(agent_id) = event.agent_id.as_ref()
        {
            self.parents
                .entry(session_id.to_owned())
                .or_default()
                .insert(
                    agent_id.to_string(),
                    spawned.parent_agent_id.as_ref().map(ToString::to_string),
                );
        }
    }

    /// Whether the scope admits this event.
    ///
    /// An agent event passes when the agent sits in the root's subtree; a
    /// session-scoped event (no agent) passes when its session holds the
    /// root at all, because a lifecycle change of the session an attachment
    /// is scoped into is that attachment's business. An agent the tree has
    /// no spawn record for is outside every subtree — membership is proven,
    /// never presumed (same rule as the TUI's client-side scope).
    fn admits(&self, session_id: &str, event: Option<&Event>) -> bool {
        let Some(root) = self.root.as_deref() else {
            return true;
        };
        static EMPTY: BTreeMap<String, Option<String>> = BTreeMap::new();
        let parents = self.parents.get(session_id).unwrap_or(&EMPTY);
        match event.and_then(|event| event.agent_id.as_ref()) {
            Some(agent_id) => in_subtree(parents, root, &agent_id.to_string()),
            None => parents.contains_key(root),
        }
    }

    /// Folds and filters one batch of envelopes, in order: every event is
    /// observed (the tree must grow even from events the scope drops — a
    /// descendant of a dropped agent is still outside the subtree), then
    /// only the admitted ones survive.
    pub(crate) fn retain(&mut self, envelopes: Vec<EventEnvelope>) -> Vec<EventEnvelope> {
        envelopes
            .into_iter()
            .filter(|envelope| {
                let session_id = envelope
                    .session_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                if let Some(event) = envelope.event.as_ref() {
                    self.observe(&session_id, event);
                }
                self.admits(&session_id, envelope.event.as_ref())
            })
            .collect()
    }
}

/// Whether `agent_id` sits in the subtree rooted at `root`.
///
/// Counted ceiling, not a clock: a malformed parent chain could cycle, and
/// each legitimate hop consumes one distinct known agent — plus one final
/// comparison against a root known only as a parent reference.
fn in_subtree(parents: &BTreeMap<String, Option<String>>, root: &str, agent_id: &str) -> bool {
    let mut current = agent_id.to_owned();
    for _ in 0..=parents.len() {
        if current == root {
            return true;
        }
        match parents.get(&current) {
            Some(Some(parent)) => current = parent.clone(),
            // A root, or an agent nobody spawned: either way the walk is over.
            _ => return false,
        }
    }
    false
}

/// One sync stream: history, a caught-up marker, then the live tail with
/// heartbeats. Single-session and merged syncs are the same code path — a
/// single session is a merge of one.
pub(crate) async fn stream_merged(
    sender: Sender<Result<SyncEventsResponse, Status>>,
    history: Vec<StoredEvent>,
    mut tail: MultiTail,
    follow: bool,
    mut filter: SubtreeFilter,
    heartbeat_period: Duration,
) {
    if !send_stored(&sender, &history, &mut filter).await {
        return;
    }

    // Caught up. A TUI stops showing a loading state here rather than
    // guessing when history ended; the heads are per-session resume points.
    let caught_up = SyncEventsResponse {
        update: Some(sync_events_response::Update::CaughtUp(
            horde_proto::v1::SyncCaughtUp {
                heads: tail
                    .heads()
                    .into_iter()
                    .map(|(session_id, delivered)| horde_proto::v1::SessionCursor {
                        session_id: Some(SessionId::new(session_id)),
                        from_sequence: delivered,
                    })
                    .collect(),
            },
        )),
    };
    if stop_after_caught_up(sender.send(Ok(caught_up)).await.is_err(), follow) {
        return;
    }

    loop {
        match tokio::time::timeout(heartbeat_period, tail.next_batch()).await {
            // Quiet past the ceiling: the heartbeat is both the client's
            // staleness signal and the server's dead-client probe — a failed
            // send is how this task learns nobody is listening.
            Err(_elapsed) => {
                if sender.send(Ok(heartbeat())).await.is_err() {
                    return;
                }
            }
            Ok(None) => return,
            Ok(Some(Err(error))) => {
                let _ = sender.send(Err(Status::from(error))).await;
                return;
            }
            Ok(Some(Ok(events))) => {
                if !send_stored(&sender, &events, &mut filter).await {
                    return;
                }
            }
        }
    }
}

/// The keepalive frame, stamped with the server's clock so a client can also
/// reconcile skew.
fn heartbeat() -> SyncEventsResponse {
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        });
    SyncEventsResponse {
        update: Some(sync_events_response::Update::Heartbeat(
            horde_proto::v1::SyncHeartbeat {
                server_time: Some(recorded_at(unix_ms)),
            },
        )),
    }
}

/// Envelopes, filters, and sends one batch. `false` means the stream is
/// finished — the receiver hung up, or an event would not decode and the
/// error has been sent.
async fn send_stored(
    sender: &Sender<Result<SyncEventsResponse, Status>>,
    events: &[StoredEvent],
    filter: &mut SubtreeFilter,
) -> bool {
    let batch: Result<Vec<_>, _> = events.iter().map(envelope).collect();
    let batch = match batch {
        Ok(batch) => filter.retain(batch),
        Err(error) => {
            let _ = sender.send(Err(Status::from(error))).await;
            return false;
        }
    };
    if batch.is_empty() {
        return true;
    }
    let response = SyncEventsResponse {
        update: Some(sync_events_response::Update::Batch(EventBatch {
            events: batch,
        })),
    };
    sender.send(Ok(response)).await.is_ok()
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{AgentId, AgentSpawned, Event, EventEnvelope, SessionId, event::Kind};

    use super::{SubtreeFilter, recorded_at, stop_after_caught_up};

    #[test]
    fn a_sync_stream_ends_after_history_unless_the_client_asked_to_follow() {
        // The only case that keeps the stream open.
        assert!(!stop_after_caught_up(false, true));

        assert!(
            stop_after_caught_up(false, false),
            "a scriptable sync replays to head and completes"
        );
        assert!(
            stop_after_caught_up(true, true),
            "a client that hung up gets no tail however much it asked to follow"
        );
        assert!(stop_after_caught_up(true, false));
    }

    /// Asserted against known values rather than a "plausible window": a window
    /// wide enough not to flake is also wide enough to accept `secs + 1000` and
    /// `secs / 1000`, which is exactly how both mutants survived the
    /// integration test that checked the clock looked about right.
    #[test]
    fn milliseconds_split_into_whole_seconds_and_the_remainder_as_nanoseconds() {
        let stamp = recorded_at(1_700_000_123_456);
        assert_eq!(stamp.seconds, 1_700_000_123);
        assert_eq!(
            stamp.nanos, 456_000_000,
            "456 ms is 456 million nanoseconds; adding or dividing by a million is not the same"
        );
    }

    #[test]
    fn a_whole_second_has_no_remainder() {
        let stamp = recorded_at(1_000);
        assert_eq!(stamp.seconds, 1);
        assert_eq!(stamp.nanos, 0);
    }

    #[test]
    fn the_epoch_is_zero_in_both_fields() {
        let stamp = recorded_at(0);
        assert_eq!(stamp.seconds, 0);
        assert_eq!(stamp.nanos, 0);
    }

    #[test]
    fn a_sub_second_reading_is_all_remainder() {
        let stamp = recorded_at(999);
        assert_eq!(stamp.seconds, 0);
        assert_eq!(stamp.nanos, 999_000_000);
    }

    /// Floor division, so a pre-epoch reading is a negative second with a
    /// non-negative nanosecond — not a timestamp that reads as later than it
    /// was.
    #[test]
    fn a_pre_epoch_reading_floors_rather_than_truncating_toward_zero() {
        let stamp = recorded_at(-1);
        assert_eq!(stamp.seconds, -1);
        assert_eq!(stamp.nanos, 999_000_000);
    }

    /// The heartbeat carries the server's clock; a keepalive with no
    /// timestamp would tell the client it is alive but not *when*, and skew
    /// reconciliation (DESIGN §2) reads exactly this field.
    #[test]
    fn a_heartbeat_carries_the_servers_clock() {
        let frame = super::heartbeat();
        let Some(horde_proto::v1::sync_events_response::Update::Heartbeat(beat)) = frame.update
        else {
            panic!("expected a heartbeat frame, got {frame:?}");
        };
        let stamp = beat.server_time.expect("the server's clock rides along");
        assert!(
            stamp.seconds > 1_700_000_000,
            "the stamp must be the real clock, not a default: {stamp:?}"
        );
    }

    /// One enveloped event, the way the stream would hand it to the filter.
    fn envelope_of(session: &str, sequence: u64, agent: Option<&str>, kind: Kind) -> EventEnvelope {
        let event = Event {
            agent_id: agent.map(AgentId::new),
            kind: Some(kind),
            ..Event::default()
        };
        EventEnvelope {
            session_id: Some(SessionId::new(session)),
            sequence,
            recorded_at: None,
            event: Some(event),
        }
    }

    fn spawn_envelope(
        session: &str,
        sequence: u64,
        agent: &str,
        parent: Option<&str>,
    ) -> EventEnvelope {
        let spawned = AgentSpawned {
            parent_agent_id: parent.map(AgentId::new),
            ..AgentSpawned::default()
        };
        envelope_of(session, sequence, Some(agent), Kind::AgentSpawned(spawned))
    }

    fn output_envelope(session: &str, sequence: u64, agent: Option<&str>) -> EventEnvelope {
        let chunk = horde_proto::v1::AgentOutputChunk::default();
        envelope_of(session, sequence, agent, Kind::AgentOutputChunk(chunk))
    }

    fn sequences(envelopes: &[EventEnvelope]) -> Vec<u64> {
        envelopes.iter().map(|envelope| envelope.sequence).collect()
    }

    #[test]
    fn an_unscoped_filter_admits_everything_including_the_unattributable() {
        let mut filter = SubtreeFilter::unscoped();
        let retained = filter.retain(vec![
            output_envelope("s1", 1, Some("anyone")),
            output_envelope("s1", 2, None),
            EventEnvelope::default(),
        ]);
        assert_eq!(retained.len(), 3);
    }

    /// The scope, walked through a whole little tree: the root's subtree
    /// passes (its own spawn included), a sibling's does not, and membership
    /// goes down the tree, never up.
    #[test]
    fn a_scoped_filter_admits_the_subtree_and_drops_the_rest() {
        let mut filter = SubtreeFilter::scoped("child-1");
        let retained = filter.retain(vec![
            spawn_envelope("s1", 1, "root-1", None),
            spawn_envelope("s1", 2, "child-1", Some("root-1")),
            spawn_envelope("s1", 3, "child-2", Some("root-1")),
            spawn_envelope("s1", 4, "grandchild-1", Some("child-1")),
            output_envelope("s1", 5, Some("grandchild-1")),
            output_envelope("s1", 6, Some("child-2")),
            output_envelope("s1", 7, Some("root-1")),
        ]);
        assert_eq!(
            sequences(&retained),
            vec![2, 4, 5],
            "the attached agent's spawn, its descendant's, and the \
             descendant's output — nothing of the sibling's or the parent's"
        );
    }

    /// Membership is proven, never presumed: an agent with no spawn record is
    /// outside every subtree, and a cycle in the parent chain terminates.
    #[test]
    fn an_unknown_agent_and_a_cycled_chain_are_both_outside() {
        let mut filter = SubtreeFilter::scoped("root-1");
        assert!(
            filter
                .retain(vec![output_envelope("s1", 1, Some("nobody-spawned"))])
                .is_empty()
        );

        let mut cycled = SubtreeFilter::scoped("root-1");
        cycled.retain(vec![
            spawn_envelope("s1", 1, "a", Some("b")),
            spawn_envelope("s1", 2, "b", Some("a")),
        ]);
        assert!(
            cycled
                .retain(vec![output_envelope("s1", 3, Some("a"))])
                .is_empty(),
            "a malformed parent cycle must terminate the walk as a non-member"
        );
    }

    /// Session-scoped events (no agent) belong to an attachment scoped into
    /// that session — but only once the session actually holds the root.
    #[test]
    fn a_session_scoped_event_passes_only_where_the_root_lives() {
        let mut filter = SubtreeFilter::scoped("root-1");
        assert!(
            filter
                .retain(vec![output_envelope("s1", 1, None)])
                .is_empty(),
            "before the root spawns, the session has nothing of the scope's"
        );

        filter.retain(vec![spawn_envelope("s1", 2, "root-1", None)]);
        assert_eq!(
            sequences(&filter.retain(vec![output_envelope("s1", 3, None)])),
            vec![3]
        );
        assert!(
            filter
                .retain(vec![output_envelope("elsewhere", 4, None)])
                .is_empty(),
            "another session's lifecycle is not this attachment's business"
        );
    }

    /// The seed is what lets a mid-log cursor judge membership: the spawns
    /// lie before the offset, so without seeding every event would be
    /// dropped as unproven.
    #[test]
    fn seeding_from_history_proves_membership_for_a_mid_log_cursor() {
        let spawn = |agent: &str, parent: Option<&str>| {
            let envelope = spawn_envelope("s1", 1, agent, parent);
            crate::log::StoredEvent {
                session_id: "s1".to_owned(),
                sequence: 1,
                recorded_at_unix_ms: 0,
                payload: prost::Message::encode_to_vec(&envelope.event.unwrap_or_default()),
            }
        };
        let mut filter = SubtreeFilter::scoped("child-1");
        filter.seed(
            "s1",
            &[spawn("root-1", None), spawn("child-1", Some("root-1"))],
        );

        assert_eq!(
            sequences(&filter.retain(vec![output_envelope("s1", 9, Some("child-1"))])),
            vec![9],
            "membership learned from the seed, not from the streamed events"
        );
    }
}
