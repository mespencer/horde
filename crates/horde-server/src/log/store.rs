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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Appended {
    pub sequence: u64,
    /// True when an equivalent event was already present.
    ///
    /// Not an error: worker journals and client retries are both at-least-once,
    /// so a duplicate is the system working. The caller still learns which
    /// position the event occupies, so it can be pointed at the log.
    pub deduplicated: bool,
}

/// Opens a connection and brings it up to schema.
pub fn open(path: &str) -> Result<Connection, ServerError> {
    let connection =
        Connection::open(path).map_err(ServerError::storage("opening the database"))?;
    prepare(&connection)?;
    Ok(connection)
}

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

    if let Some(existing) = find_duplicate(&transaction, event)? {
        return Ok(Appended {
            sequence: existing,
            deduplicated: true,
        });
    }

    let next = head_within(&transaction, &event.session_id)?.saturating_add(1);
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

    transaction
        .commit()
        .map_err(ServerError::storage("committing an append"))?;

    Ok(Appended {
        sequence: next,
        deduplicated: false,
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
) -> Result<Option<u64>, ServerError> {
    if let Some(journal_sequence) = event.journal_sequence
        && !event.origin_worker_id.is_empty()
    {
        let existing: Option<u64> = transaction
            .query_row(
                "SELECT sequence FROM events
                 WHERE origin_worker_id = ?1 AND journal_sequence = ?2",
                params![event.origin_worker_id, journal_sequence],
                |row| row.get(0),
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
            "SELECT sequence FROM events
             WHERE session_id = ?1 AND idempotency_key = ?2",
            params![event.session_id, event.idempotency_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ServerError::storage("checking the idempotency key"))
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
            "SELECT rowid, session_id, sequence, recorded_at_unix_ms, payload
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
        .query_row(
            "SELECT MAX(journal_sequence) FROM events WHERE origin_worker_id = ?1",
            params![worker_id],
            |row| row.get(0),
        )
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
}
