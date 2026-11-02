//! The storage schema, and the reason there is only one table in it.
//!
//! ADR-0001 makes the per-session append-only log the source of truth, and
//! notes that side channels are bugs. The schema takes that literally: `events`
//! is the **only** table this server writes. Session lists, agent trees,
//! lifecycle states, per-session heads, and worker journal watermarks are all
//! folded out of it rather than stored beside it.
//!
//! That is not minimalism for its own sake. A second table holding "the current
//! state" is a second source of truth, and two sources of truth drift the first
//! time a write path forgets one of them — which is exactly the class of bug
//! that is invisible until a crash, a partition, or a replay disagrees with the
//! live view. With one table there is nothing to disagree with: the projection
//! is a pure function of the log, recomputed at startup and advanced by the
//! same append that wrote the event.
//!
//! Two uniqueness constraints do real work rather than merely describing
//! intent:
//!
//! * `(origin_worker_id, journal_sequence)` makes a worker replaying its
//!   journal after a partition structurally unable to double-append. The
//!   dedupe is the database's job, not a check someone has to remember.
//! * `(session_id, idempotency_key)` does the same for client retries.
//!
//! Both are partial indexes, because server-originated events have neither a
//! journal sequence nor a client key, and `NULL`/`''` must not collide.

/// Statements applied to every connection, in order.
///
/// Idempotent by construction (`IF NOT EXISTS`, `IF EXISTS`), so opening an
/// existing database is the same code path as creating one — and migrating
/// one is the same code path too: there is no version table to consult.
pub(crate) const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS events (
    -- Per-session, dense, 1-based. Assigned by the server on append.
    session_id           TEXT    NOT NULL,
    sequence             INTEGER NOT NULL,
    recorded_at_unix_ms  INTEGER NOT NULL,

    -- Empty for server-originated events. Names the worker whose journal the
    -- event came from, so a resume watermark is a query rather than a table.
    origin_worker_id     TEXT    NOT NULL DEFAULT '',

    -- The worker's own journal position. NULL for server-originated events.
    -- This is the *other* sequence space; it must never be confused with
    -- `sequence` above.
    journal_sequence     INTEGER,

    -- Worker- or client-assigned. Empty when the originator supplied none.
    idempotency_key      TEXT    NOT NULL DEFAULT '',

    -- An encoded horde.v1.Event. Opaque here on purpose: the log stores what
    -- it was given, and meaning is applied when it is folded into state.
    payload              BLOB    NOT NULL,

    PRIMARY KEY (session_id, sequence)
) STRICT;

-- A partitioned worker replays from its last acked journal sequence. If it
-- replays too far, this index rejects the duplicate instead of appending it
-- twice under a fresh session sequence.
CREATE UNIQUE INDEX IF NOT EXISTS events_worker_journal
    ON events (origin_worker_id, journal_sequence)
    WHERE journal_sequence IS NOT NULL;

-- The same protection for client retries after a lost response.
CREATE UNIQUE INDEX IF NOT EXISTS events_idempotency
    ON events (session_id, idempotency_key)
    WHERE idempotency_key <> '';

-- Replay is always 'this session, from this offset, in order' — which the
-- primary key's own index already answers. An earlier schema created a
-- second, identical index for it, paid for on every append (ADR-0013); this
-- statement is the migration that retires it from databases that have one.
DROP INDEX IF EXISTS events_session_sequence;
";

/// Pragmas applied per connection.
///
/// WAL is what lets replay reads proceed while an append is in flight;
/// `synchronous = NORMAL` is the documented companion to WAL, trading an fsync
/// per commit for one per checkpoint. `foreign_keys` is on for the tables a
/// later wave adds, so it is never retrofitted onto data written without it.
pub(crate) const PRAGMAS: &[&str] = &[
    "PRAGMA journal_mode = WAL",
    "PRAGMA synchronous = NORMAL",
    "PRAGMA foreign_keys = ON",
    "PRAGMA busy_timeout = 5000",
];
