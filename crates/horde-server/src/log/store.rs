//! Blocking SQLite operations.
//!
//! Every function here runs on a blocking thread and takes `&Connection`
//! directly. Keeping them synchronous and free of any runtime types is what
//! makes them testable without a runtime and reviewable as plain SQL, and it
//! keeps the async/blocking boundary in exactly one place (`super::EventLog`)
//! rather than smeared through the storage layer.

use rusqlite::{Connection, OptionalExtension, params};

use super::schema::{PRAGMAS, SCHEMA};
use crate::error::ServerError;

/// One row of the log, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub session_id: String,
    /// Per-session, dense, 1-based.
    pub sequence: u64,
    pub recorded_at_unix_ms: i64,
    /// Encoded `horde.v1.Event`.
    pub payload: Vec<u8>,
}

/// One row of the log, with its position in the log's *append* order.
///
/// The third sequence space in this system, and the only one that is total.
/// `sequence` is per session and `journal_sequence` is per worker, so neither
/// can order two events from different sessions — which is exactly what
/// deriving one worker's command stream out of the whole log requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalEvent {
    /// Append order. See [`read_global_from`] for why this is `rowid`.
    pub row: u64,
    pub event: StoredEvent,
    /// The worker whose journal this row came from; empty for an event the
    /// Server appended itself.
    ///
    /// Folds over the whole log need it because one event kind can mean two
    /// different things depending on who wrote it: a `TurnInterrupted` the
    /// Server journaled is a *request* to interrupt, and one a Worker
    /// journaled is that Worker *acknowledging* it carried one out. A fold
    /// that cannot tell them apart answers every acknowledgement with another
    /// request (ADR-0009).
    pub origin_worker_id: String,
}

/// What to append, before the server has assigned it a position.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PendingEvent {
    pub session_id: String,
    /// Empty for server-originated events.
    pub origin_worker_id: String,
    /// `None` for server-originated events.
    pub journal_sequence: Option<u64>,
    /// Empty when the originator supplied none.
    pub idempotency_key: String,
    pub payload: Vec<u8>,
}

/// Where an append landed, and whether it was new.
///
/// No longer `Copy`: it carries the deduplicated event's payload, which is a
/// `Vec`. Worth the trade — the alternative is a caller that cannot tell a
/// retry from a collision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Appended {
    pub sequence: u64,
    /// True when an equivalent event was already present.
    ///
    /// Not an error: worker journals and client retries are both at-least-once,
    /// so a duplicate is the system working. The caller still learns which
    /// position the event occupies, so it can be pointed at the log.
    pub deduplicated: bool,
    /// The payload already stored, when this append was deduplicated.
    ///
    /// Returned because "a duplicate" is a claim worth checking rather than
    /// trusting. This layer holds payloads as opaque bytes and cannot tell a
    /// genuine retry from an idempotency-key collision, but a caller that
    /// understands the encoding can — so the bytes travel up to whoever can
    /// read them. See `service::shared::refuse_kind_collision`.
    pub existing_payload: Option<Vec<u8>>,
}

/// Opens a connection and brings it up to schema.
pub fn open(path: &str) -> Result<Connection, ServerError> {
    let connection =
        Connection::open(path).map_err(ServerError::storage("opening the database"))?;
    prepare(&connection)?;
    Ok(connection)
}

/// Opens a read-only companion connection onto a database [`open`] already
/// brought up to schema — one member of the read pool (ADR-0013).
///
/// `query_only` rather than a read-only open: a read-only handle cannot
/// create the WAL index when it is the first to need one, while a
/// query-only handle shares the writer's WAL like any other connection and
/// still refuses every write. No schema is applied: the writer owns it.
pub fn open_reader(path: &str) -> Result<Connection, ServerError> {
    let connection =
        Connection::open(path).map_err(ServerError::storage("opening a read connection"))?;
    connection
        .execute_batch(READER_PRAGMAS)
        .map_err(ServerError::storage("applying reader pragmas"))?;
    Ok(connection)
}

/// Pragmas for a read-pool connection: wait out a checkpoint rather than
/// fail, and refuse any write that reaches it by mistake.
const READER_PRAGMAS: &str = "PRAGMA busy_timeout = 5000; PRAGMA query_only = ON;";

/// Opens a private in-memory database. Used by tests and by `--ephemeral`.
pub fn open_in_memory() -> Result<Connection, ServerError> {
    let connection = Connection::open_in_memory()
        .map_err(ServerError::storage("opening an in-memory database"))?;
    prepare(&connection)?;
    Ok(connection)
}

fn prepare(connection: &Connection) -> Result<(), ServerError> {
    for pragma in PRAGMAS {
        // `journal_mode` returns a row; `execute_batch` tolerates that where
        // `execute` would reject it.
        connection
            .execute_batch(pragma)
            .map_err(ServerError::storage("applying pragmas"))?;
    }
    connection
        .execute_batch(SCHEMA)
        .map_err(ServerError::storage("applying the schema"))?;
    Ok(())
}

/// Appends one event, assigning the next sequence for its session.
///
/// The sequence is chosen and the row written inside a single `IMMEDIATE`
/// transaction, so two concurrent appends cannot both read the same head and
/// then both claim the position after it. That is what makes per-session
/// sequences dense rather than merely increasing.
pub fn append(connection: &mut Connection, event: &PendingEvent) -> Result<Appended, ServerError> {
    let transaction = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(ServerError::storage("beginning an append"))?;
    let appended = append_within(&transaction, event)?;
    transaction
        .commit()
        .map_err(ServerError::storage("committing an append"))?;
    Ok(appended)
}

/// One write of a batch, and the write its outcome called for, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batched {
    pub appended: Appended,
    /// The follow-up appended straight after this write: its session and
    /// where it landed.
    pub follow_up: Option<(String, Appended)>,
}

/// Appends `events` in order inside **one** `IMMEDIATE` transaction
/// (ADR-0013), each exactly as [`append`] would — the same dedupe, the same
/// dense per-session sequences — so a batch is indistinguishable from the
/// same appends made one at a time, except that it commits once.
///
/// `follow_up` is asked after each write, with the write's index and
/// outcome, whether that outcome calls for another write; one it returns is
/// appended immediately after, still inside the transaction. That is what
/// keeps an ingest dead letter for a key collision in the place the
/// one-at-a-time loop gave it — directly behind the write that collided,
/// ahead of the next entry's.
///
/// All or nothing: a failure anywhere rolls the whole batch back, which a
/// journal batch can afford because its sender re-sends it whole.
pub fn append_all<F>(
    connection: &mut Connection,
    events: &[PendingEvent],
    mut follow_up: F,
) -> Result<Vec<Batched>, ServerError>
where
    F: FnMut(usize, &Appended) -> Option<PendingEvent>,
{
    let transaction = connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(ServerError::storage("beginning a batch append"))?;
    let mut batched = Vec::with_capacity(events.len());
    for (index, event) in events.iter().enumerate() {
        let appended = append_within(&transaction, event)?;
        let follow_up = match follow_up(index, &appended) {
            Some(next) => Some((next.session_id.clone(), append_within(&transaction, &next)?)),
            None => None,
        };
        batched.push(Batched {
            appended,
            follow_up,
        });
    }
    transaction
        .commit()
        .map_err(ServerError::storage("committing a batch append"))?;
    Ok(batched)
}

/// One append inside a transaction the caller owns and commits.
fn append_within(
    transaction: &rusqlite::Transaction<'_>,
    event: &PendingEvent,
) -> Result<Appended, ServerError> {
    if let Some((sequence, existing_payload)) = find_duplicate(transaction, event)? {
        return Ok(Appended {
            sequence,
            deduplicated: true,
            existing_payload: Some(existing_payload),
        });
    }

    let next = head_within(transaction, &event.session_id)?.saturating_add(1);
    let recorded_at_unix_ms = now_unix_ms();

    transaction
        .execute(
            "INSERT INTO events (
                 session_id, sequence, recorded_at_unix_ms,
                 origin_worker_id, journal_sequence, idempotency_key, payload
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                event.session_id,
                next,
                recorded_at_unix_ms,
                event.origin_worker_id,
                event.journal_sequence,
                event.idempotency_key,
                event.payload,
            ],
        )
        .map_err(ServerError::storage("inserting an event"))?;

    Ok(Appended {
        sequence: next,
        deduplicated: false,
        existing_payload: None,
    })
}

/// Looks for an event this one would duplicate.
///
/// Checked explicitly rather than by catching a constraint violation, because
/// the caller needs the *position* of the original, and a rejected insert does
/// not carry one. The unique indexes remain the real guarantee; this is the
/// path that turns a duplicate into a useful answer.
fn find_duplicate(
    transaction: &rusqlite::Transaction<'_>,
    event: &PendingEvent,
) -> Result<Option<(u64, Vec<u8>)>, ServerError> {
    if let Some(journal_sequence) = event.journal_sequence
        && !event.origin_worker_id.is_empty()
    {
        let existing: Option<(u64, Vec<u8>)> = transaction
            .query_row(
                JOURNAL_DUPLICATE_SQL,
                params![event.origin_worker_id, journal_sequence],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(ServerError::storage("checking the journal watermark"))?;
        if existing.is_some() {
            return Ok(existing);
        }
    }

    if event.idempotency_key.is_empty() {
        return Ok(None);
    }

    transaction
        .query_row(
            KEY_LOOKUP_SQL,
            params![event.session_id, event.idempotency_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ServerError::storage("checking the idempotency key"))
}

/// The journal dedupe probe. `journal_sequence IS NOT NULL` is spelled out,
/// though the equality implies it, so the probe visibly matches the partial
/// `events_worker_journal` index's predicate (ADR-0013).
const JOURNAL_DUPLICATE_SQL: &str = "SELECT sequence, payload FROM events
     WHERE origin_worker_id = ?1 AND journal_sequence = ?2
       AND journal_sequence IS NOT NULL";

/// The idempotency-key probe. `idempotency_key <> ''` is what lets the
/// planner use the partial `events_idempotency` index: a bound parameter
/// does not imply the index's predicate the way a literal would. Every
/// caller has already refused the empty key, so the term changes no answer.
const KEY_LOOKUP_SQL: &str = "SELECT sequence, payload FROM events
     WHERE session_id = ?1 AND idempotency_key = ?2 AND idempotency_key <> ''";

/// A worker's watermark. Without `journal_sequence IS NOT NULL` the planner
/// cannot use the partial `events_worker_journal` index and scans every row
/// the worker's id appears on — server-originated rows included, which never
/// carry a journal sequence (ADR-0013).
const WATERMARK_SQL: &str = "SELECT MAX(journal_sequence) FROM events
     WHERE origin_worker_id = ?1 AND journal_sequence IS NOT NULL";

/// The event already stored under `(session_id, key)`, if one is: its
/// sequence and payload — the same question the append's dedupe asks,
/// exposed so a verb can recognize a retry *before* running validations the
/// original already passed (a retried `OpenForward` must not be refused as
/// its own duplicate).
pub fn stored_under_key(
    connection: &Connection,
    session_id: &str,
    key: &str,
) -> Result<Option<(u64, Vec<u8>)>, ServerError> {
    if key.is_empty() {
        return Ok(None); // Empty is the log's "never deduplicate".
    }
    connection
        .query_row(KEY_LOOKUP_SQL, params![session_id, key], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(ServerError::storage("looking up an idempotency key"))
}

/// Highest sequence in a session, or zero when the session has no events.
///
/// Zero is meaningful: sequences are 1-based, so "head is 0" and "session does
/// not exist" are the same statement, which is the point of having no separate
/// session table to disagree with.
pub fn head(connection: &Connection, session_id: &str) -> Result<u64, ServerError> {
    head_within(connection, session_id)
}

fn head_within(connection: &Connection, session_id: &str) -> Result<u64, ServerError> {
    let head: Option<u64> = connection
        .query_row(
            "SELECT MAX(sequence) FROM events WHERE session_id = ?1",
            params![session_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ServerError::storage("reading the session head"))?
        .flatten();
    Ok(head.unwrap_or(0))
}

/// Reads up to `limit` events from `from_sequence` inclusive, in order.
pub fn read_from(
    connection: &Connection,
    session_id: &str,
    from_sequence: u64,
    limit: u32,
) -> Result<Vec<StoredEvent>, ServerError> {
    // Zero is normalized to 1 by the wire contract; doing it here too means a
    // caller inside the process cannot accidentally skip event 1.
    let from = from_sequence.max(1);

    let mut statement = connection
        .prepare_cached(
            "SELECT session_id, sequence, recorded_at_unix_ms, payload
             FROM events
             WHERE session_id = ?1 AND sequence >= ?2
             ORDER BY sequence ASC
             LIMIT ?3",
        )
        .map_err(ServerError::storage("preparing a replay"))?;

    let rows = statement
        .query_map(params![session_id, from, limit], |row| {
            Ok(StoredEvent {
                session_id: row.get(0)?,
                sequence: row.get(1)?,
                recorded_at_unix_ms: row.get(2)?,
                payload: row.get(3)?,
            })
        })
        .map_err(ServerError::storage("replaying events"))?;

    let mut events = Vec::new();
    for row in rows {
        events.push(row.map_err(ServerError::storage("decoding a replayed event"))?);
    }
    Ok(events)
}

/// Reads up to `limit` events from anywhere in the log, in append order,
/// starting after `after_row`.
///
/// **Why `rowid` is the append order, and why that is safe to lean on.** The
/// log is append-only: nothing is ever deleted or updated, so SQLite never
/// recycles a rowid, and each insert takes one strictly greater than every row
/// before it. Appends are serialized through a single connection inside an
/// `IMMEDIATE` transaction, so "the order rows were committed" and "the order
/// of their rowids" are the same statement. This is not a new dependence:
/// [`session_ids`] already orders by `MIN(rowid)` to report the order sessions
/// came into existence.
///
/// The alternative — ordering by `(session_id, sequence)` — is deterministic
/// but wrong for this job, and wrong in a way that only shows up in
/// production. It is a *sort*, not an append order, so an event in a
/// lexicographically earlier session sorts ahead of commands already sent, and
/// every later command renumbers. A worker's acked watermark would then point
/// at a different command than it did a moment ago.
pub fn read_global_from(
    connection: &Connection,
    after_row: u64,
    limit: u32,
) -> Result<Vec<GlobalEvent>, ServerError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT rowid, session_id, sequence, recorded_at_unix_ms, payload, origin_worker_id
             FROM events
             WHERE rowid > ?1
             ORDER BY rowid ASC
             LIMIT ?2",
        )
        .map_err(ServerError::storage("preparing a global replay"))?;

    let rows = statement
        .query_map(params![after_row, limit], |row| {
            Ok(GlobalEvent {
                row: row.get(0)?,
                event: StoredEvent {
                    session_id: row.get(1)?,
                    sequence: row.get(2)?,
                    recorded_at_unix_ms: row.get(3)?,
                    payload: row.get(4)?,
                },
                origin_worker_id: row.get(5)?,
            })
        })
        .map_err(ServerError::storage("replaying the log"))?;

    let mut events = Vec::new();
    for row in rows {
        events.push(row.map_err(ServerError::storage("decoding a replayed event"))?);
    }
    Ok(events)
}

/// Every session that has at least one event, in creation order.
///
/// Ordered by first sequence rather than by id, so listing is stable and
/// matches the order sessions actually came into existence.
pub fn session_ids(connection: &Connection) -> Result<Vec<String>, ServerError> {
    let mut statement = connection
        .prepare_cached(
            "SELECT session_id, MIN(rowid) AS first_row
             FROM events GROUP BY session_id ORDER BY first_row ASC",
        )
        .map_err(ServerError::storage("preparing a session list"))?;

    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(ServerError::storage("listing sessions"))?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(ServerError::storage("decoding a session id"))?);
    }
    Ok(ids)
}

/// The highest journal sequence durably stored for a worker, or zero.
///
/// Derived, like everything else, rather than tracked in a table that could
/// disagree with the events actually present.
pub fn worker_journal_watermark(
    connection: &Connection,
    worker_id: &str,
) -> Result<u64, ServerError> {
    let watermark: Option<u64> = connection
        .query_row(WATERMARK_SQL, params![worker_id], |row| row.get(0))
        .optional()
        .map_err(ServerError::storage("reading a worker watermark"))?
        .flatten();
    Ok(watermark.unwrap_or(0))
}

fn now_unix_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    // A clock before the epoch is not a condition worth propagating as an
    // error through every append; it is recorded as 0 and remains monotonic
    // with `sequence`, which is what ordering actually relies on.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// A minimal event for one session, for tests across this module's tree.
///
/// Lives here rather than in each test module because both `log` and
/// `log::store` need it, and `log` already depends on `log::store` — putting it
/// the other way round would close a module cycle.
#[cfg(test)]
pub(crate) fn pending(session: &str, payload: &[u8]) -> PendingEvent {
    PendingEvent {
        session_id: session.to_owned(),
        payload: payload.to_vec(),
        ..PendingEvent::default()
    }
}

/// An event as a worker journals it: attributed to that worker, at that
/// position in the worker's own sequence space.
#[cfg(test)]
pub(crate) fn journaled(worker: &str, journal_sequence: u64) -> PendingEvent {
    PendingEvent {
        origin_worker_id: worker.to_owned(),
        journal_sequence: Some(journal_sequence),
        ..pending("s1", b"x")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PendingEvent, append, head, journaled, open_in_memory, pending, read_from, session_ids,
        worker_journal_watermark,
    };

    #[test]
    fn sequences_are_dense_and_one_based_per_session() {
        let mut connection = open_in_memory().expect("in-memory database");

        for expected in 1_u64..=5 {
            let appended = append(&mut connection, &pending("s1", b"x")).expect("append");
            assert_eq!(appended.sequence, expected);
            assert!(!appended.deduplicated);
        }
        assert_eq!(head(&connection, "s1").expect("head"), 5);
    }

    #[test]
    fn sessions_number_independently_of_each_other() {
        let mut connection = open_in_memory().expect("in-memory database");
        append(&mut connection, &pending("s1", b"a")).expect("append");
        append(&mut connection, &pending("s1", b"b")).expect("append");
        let other = append(&mut connection, &pending("s2", b"c")).expect("append");

        assert_eq!(other.sequence, 1, "a new session starts its own numbering");
        assert_eq!(head(&connection, "s1").expect("head"), 2);
        assert_eq!(head(&connection, "s2").expect("head"), 1);
    }

    #[test]
    fn an_unknown_session_has_head_zero_rather_than_an_error() {
        let connection = open_in_memory().expect("in-memory database");
        assert_eq!(head(&connection, "never-existed").expect("head"), 0);
    }

    #[test]
    fn a_replayed_journal_event_is_deduplicated_to_its_original_position() {
        let mut connection = open_in_memory().expect("in-memory database");
        let event = journaled("worker-a", 7);

        let first = append(&mut connection, &event).expect("append");
        let again = append(&mut connection, &event).expect("re-append");

        assert!(!first.deduplicated);
        assert!(
            again.deduplicated,
            "a replayed journal must not double-append"
        );
        assert_eq!(again.sequence, first.sequence);
        assert_eq!(head(&connection, "s1").expect("head"), 1);
    }

    /// Two workers may legitimately use the same journal numbers; they are
    /// per-worker sequences, so only the pair identifies an event.
    #[test]
    fn the_same_journal_sequence_from_two_workers_is_two_events() {
        let mut connection = open_in_memory().expect("in-memory database");
        let mut event = journaled("worker-a", 1);
        append(&mut connection, &event).expect("append");

        event.origin_worker_id = "worker-b".to_owned();
        let second = append(&mut connection, &event).expect("append");

        assert!(!second.deduplicated);
        assert_eq!(head(&connection, "s1").expect("head"), 2);
    }

    #[test]
    fn a_retried_client_write_is_deduplicated_by_its_key() {
        let mut connection = open_in_memory().expect("in-memory database");
        let event = PendingEvent {
            session_id: "s1".to_owned(),
            idempotency_key: "abc".to_owned(),
            payload: b"x".to_vec(),
            ..PendingEvent::default()
        };

        let first = append(&mut connection, &event).expect("append");
        let again = append(&mut connection, &event).expect("re-append");

        assert_eq!(again.sequence, first.sequence);
        assert!(again.deduplicated);
    }

    /// An empty key means "the originator supplied none", so those events must
    /// not collide with each other.
    #[test]
    fn events_without_an_idempotency_key_are_never_deduplicated() {
        let mut connection = open_in_memory().expect("in-memory database");
        append(&mut connection, &pending("s1", b"a")).expect("append");
        let second = append(&mut connection, &pending("s1", b"a")).expect("append");

        assert!(!second.deduplicated);
        assert_eq!(head(&connection, "s1").expect("head"), 2);
    }

    #[test]
    fn replay_starts_at_the_requested_offset_and_respects_the_limit() {
        let mut connection = open_in_memory().expect("in-memory database");
        for byte in 1_u8..=5 {
            append(&mut connection, &pending("s1", &[byte])).expect("append");
        }

        let tail = read_from(&connection, "s1", 3, 10).expect("replay");
        let sequences: Vec<u64> = tail.iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, vec![3, 4, 5]);

        let limited = read_from(&connection, "s1", 1, 2).expect("replay");
        assert_eq!(limited.len(), 2);
    }

    #[test]
    fn offset_zero_is_normalized_to_the_first_event() {
        let mut connection = open_in_memory().expect("in-memory database");
        append(&mut connection, &pending("s1", b"a")).expect("append");

        let from_zero = read_from(&connection, "s1", 0, 10).expect("replay");
        let from_one = read_from(&connection, "s1", 1, 10).expect("replay");
        assert_eq!(from_zero, from_one);
        assert_eq!(from_zero.len(), 1);
    }

    #[test]
    fn sessions_are_listed_in_the_order_they_first_appeared() {
        let mut connection = open_in_memory().expect("in-memory database");
        append(&mut connection, &pending("second", b"a")).expect("append");
        append(&mut connection, &pending("first", b"a")).expect("append");
        append(&mut connection, &pending("second", b"b")).expect("append");

        assert_eq!(
            session_ids(&connection).expect("list"),
            vec!["second".to_owned(), "first".to_owned()]
        );
    }

    #[test]
    fn a_worker_watermark_is_derived_from_the_events_actually_stored() {
        let mut connection = open_in_memory().expect("in-memory database");
        assert_eq!(
            worker_journal_watermark(&connection, "worker-a").expect("watermark"),
            0,
            "an unknown worker has stored nothing"
        );

        for journal in [1_u64, 2, 3] {
            append(
                &mut connection,
                &PendingEvent {
                    session_id: "s1".to_owned(),
                    origin_worker_id: "worker-a".to_owned(),
                    journal_sequence: Some(journal),
                    payload: b"x".to_vec(),
                    ..PendingEvent::default()
                },
            )
            .expect("append");
        }

        assert_eq!(
            worker_journal_watermark(&connection, "worker-a").expect("watermark"),
            3
        );
        assert_eq!(
            worker_journal_watermark(&connection, "worker-b").expect("watermark"),
            0
        );
    }

    /// What the planner does with one of this module's probes: the detail
    /// column of `EXPLAIN QUERY PLAN`, joined.
    fn plan_of(connection: &rusqlite::Connection, sql: &str) -> String {
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("explainable");
        let arguments = std::iter::repeat_n("w", statement.parameter_count());
        let details: Vec<String> = statement
            .query_map(rusqlite::params_from_iter(arguments), |row| {
                row.get::<_, String>(3)
            })
            .expect("a plan")
            .map(|row| row.expect("a plan row"))
            .collect();
        details.join(" | ")
    }

    /// The probes the review flagged reach their partial indexes instead of
    /// scanning — the predicate is what makes the index usable at all.
    #[test]
    fn the_journal_and_key_probes_use_their_partial_indexes() {
        let connection = open_in_memory().expect("in-memory database");
        let watermark = plan_of(&connection, super::WATERMARK_SQL);
        assert!(watermark.contains("events_worker_journal"), "{watermark}");
        let duplicate = plan_of(&connection, super::JOURNAL_DUPLICATE_SQL);
        assert!(duplicate.contains("events_worker_journal"), "{duplicate}");
        let key = plan_of(&connection, super::KEY_LOOKUP_SQL);
        assert!(key.contains("events_idempotency"), "{key}");
    }

    /// Opening a database that still carries the old duplicate replay index
    /// drops it, and replay keeps using the primary key's index.
    #[test]
    fn opening_an_older_database_drops_the_duplicate_replay_index() {
        let directory = tempfile::tempdir().expect("a directory");
        let path = directory.path().join("log.sqlite");
        let path = path.to_str().expect("a utf-8 path");
        {
            let connection = super::open(path).expect("a database");
            connection
                .execute_batch(
                    "CREATE INDEX events_session_sequence ON events (session_id, sequence)",
                )
                .expect("the old index");
        }
        let connection = super::open(path).expect("reopened");
        let indexes: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name = 'events_session_sequence'",
                [],
                |row| row.get(0),
            )
            .expect("a count");
        assert_eq!(indexes, 0, "the migration retires the duplicate index");
    }

    /// A batch is the same appends made one at a time: dense sequences, the
    /// journal dedupe applied within the batch too, and each follow-up landing
    /// directly behind the write that asked for it.
    #[test]
    fn a_batch_appends_in_order_with_follow_ups_behind_their_writes() {
        let mut connection = open_in_memory().expect("in-memory database");
        let batch = vec![
            journaled("worker-a", 1),
            journaled("worker-a", 2),
            journaled("worker-a", 1),
        ];
        let batched = super::append_all(&mut connection, &batch, |index, appended| {
            let marker = u8::try_from(index).expect("a small batch");
            (index == 0 || appended.deduplicated).then(|| pending("dead", &[marker]))
        })
        .expect("the batch");

        let positions: Vec<(u64, bool)> = batched
            .iter()
            .map(|landed| (landed.appended.sequence, landed.appended.deduplicated))
            .collect();
        assert_eq!(positions, vec![(1, false), (2, false), (1, true)]);
        let follow_ups: Vec<Option<(String, u64)>> = batched
            .iter()
            .map(|landed| {
                landed
                    .follow_up
                    .as_ref()
                    .map(|(session, appended)| (session.clone(), appended.sequence))
            })
            .collect();
        assert_eq!(
            follow_ups,
            vec![
                Some(("dead".to_owned(), 1)),
                None,
                Some(("dead".to_owned(), 2))
            ]
        );
        let dead = read_from(&connection, "dead", 1, 10).expect("replay");
        let payloads: Vec<Vec<u8>> = dead.into_iter().map(|event| event.payload).collect();
        assert_eq!(payloads, vec![vec![0], vec![2]], "in the order asked for");
        let global = super::read_global_from(&connection, 0, 10).expect("the log");
        let order: Vec<&str> = global
            .iter()
            .map(|row| row.event.session_id.as_str())
            .collect();
        assert_eq!(order, vec!["s1", "dead", "s1", "dead"]);
    }

    /// A read-pool connection reads what the writer committed and refuses
    /// to write.
    #[test]
    fn a_reader_sees_committed_rows_and_cannot_write() {
        let directory = tempfile::tempdir().expect("a directory");
        let path = directory.path().join("log.sqlite");
        let path = path.to_str().expect("a utf-8 path");
        let mut writer = super::open(path).expect("the writer");
        let mut reader = super::open_reader(path).expect("a reader");
        append(&mut writer, &pending("s1", b"a")).expect("append");

        assert_eq!(head(&reader, "s1").expect("head"), 1);
        assert!(
            append(&mut reader, &pending("s1", b"b")).is_err(),
            "a query-only connection refuses writes"
        );
    }
}
