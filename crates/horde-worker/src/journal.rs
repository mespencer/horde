//! The Worker's local journal — the partition-survival half of ADR-0001.
//!
//! Every event a Worker produces lands here first, in SQLite on the Worker's
//! own disk, and is pushed to the Server afterwards. A partitioned Worker keeps
//! journaling; reconnection replays from the Server's ack. The Server's session
//! log is the source of truth for *Sessions*; this journal is the source of
//! truth for *what this Worker has done and not yet proven delivered*.
//!
//! # The counter is explicit, never `MAX(journal_sequence)`
//!
//! `journal_sequence` must be dense, 1-based, and monotonic across restarts —
//! the Server refuses a Worker whose reported head regresses
//! (`JournalRegressed`), because renumbering forges history and defeats the
//! dedupe index. Truncation is the trap: the Server invites the Worker to
//! truncate everything below its ack, and a head derived from `MAX(...)` would
//! drop below the Server's watermark the moment acked rows are deleted —
//! getting a perfectly healthy Worker refused. The next sequence therefore
//! lives in an explicit meta-table counter that truncation never touches.
//!
//! # One transaction per applied command
//!
//! Applying a command and journaling its effect commit atomically
//! ([`Journal::apply_command`]). Split in two, a crash between them leaves
//! acked work with no evidence (the ack rides the next event batch and would
//! claim a command whose effects were lost) or evidence for work that was
//! never marked applied (replayed and double-run after restart).
//!
//! # The blocking boundary
//!
//! SQLite is a blocking API and is treated as one: every call runs in
//! `spawn_blocking` against a connection behind a `std::sync::Mutex`, and the
//! guard is taken and released inside the closure, never across an `.await`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use horde_proto::prost::Message;
use horde_proto::prost_types::Timestamp;
use horde_proto::v1::event::Kind;
use horde_proto::v1::{AgentId, Event, JournaledEvent, SessionId};
use rusqlite::{Connection, params};
use tokio::sync::watch;

use crate::error::WorkerError;

/// One event on its way into the journal.
///
/// Maps 1:1 onto `horde.v1.Event` plus the Session that owns it. The journal
/// stamps `occurred_at` at record time; everything else is the caller's.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    /// The Session whose log this event belongs in, Server-side.
    pub session_id: String,
    /// The Agent the event is about; `None` for Session-scoped events.
    pub agent_id: Option<String>,
    /// Stable across replays of the same event — the Server dedupes on it, so
    /// a replayed journal cannot double-append. Caller-minted and opaque.
    ///
    /// Leave empty to let the journal mint one from its persistent identity
    /// and the assigned sequence: unique across Workers and across restarts,
    /// and stable across replays because it is stored in the encoded event the
    /// moment it is assigned. Supply your own only when a richer key exists —
    /// one derived from a fact the event is about, like a permission request
    /// id — never a timestamp or a counter that restarts.
    pub idempotency_key: String,
    /// What happened.
    pub kind: Kind,
}

/// What became of a command offered to [`Journal::apply_command`].
///
/// Commands are at-least-once; redelivery is normal, not an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandApplication {
    /// First sighting: the command was marked applied and its effects
    /// journaled, in one transaction.
    Applied,
    /// A redelivery. Nothing was appended and nothing was re-run.
    AlreadyApplied,
}

/// The append-only half of the journal, for components that only produce
/// events.
///
/// This is the seam the Worker-hosted MCP endpoint records
/// `PermissionRequested` / `ResultReported` through: a clonable handle that can
/// append and do nothing else — no truncation, no command bookkeeping.
///
/// The returned `u64` is the event's `journal_sequence`: **per-Worker
/// bookkeeping only**. It is not a Session offset and must never be used as
/// one — Sessions are numbered by the Server on ingestion, in a different
/// sequence space (`event.proto`).
#[derive(Debug, Clone)]
pub struct JournalSink {
    journal: Journal,
}

impl JournalSink {
    /// Journals one event durably, returning its per-Worker journal sequence.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the database refuses the append.
    pub async fn record(&self, entry: JournalEntry) -> Result<u64, WorkerError> {
        self.journal.record(entry).await
    }
}

/// The Worker's local journal.
///
/// Cheap to clone; all clones share one connection and one head watch.
#[derive(Clone)]
pub struct Journal {
    connection: Arc<Mutex<Connection>>,
    /// Last journal sequence appended, for push pumps to wait on. Announced
    /// only after the commit, so a woken pump can always read what it was
    /// woken for.
    head: watch::Sender<u64>,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Journal")
            .field("head", &*self.head.borrow())
            .finish_non_exhaustive()
    }
}

/// Meta keys. One row each, created at open.
const NEXT_SEQUENCE: &str = "next_journal_sequence";
const FLOOR: &str = "floor";
const APPLIED_COMMAND: &str = "applied_command_sequence";
const IDENTITY: &str = "journal_identity";

impl Journal {
    /// Opens (or creates) a journal at `path`.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the file cannot be opened or the schema
    /// cannot be created.
    pub fn open(path: &Path) -> Result<Self, WorkerError> {
        let connection = Connection::open(path).map_err(open_error)?;
        Self::from_connection(connection)
    }

    /// Opens a private in-memory journal, for tests and throwaway Workers.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the schema cannot be created.
    pub fn in_memory() -> Result<Self, WorkerError> {
        let connection = Connection::open_in_memory().map_err(open_error)?;
        Self::from_connection(connection)
    }

    fn from_connection(connection: Connection) -> Result<Self, WorkerError> {
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS journal (
                     journal_sequence INTEGER PRIMARY KEY,
                     session_id       TEXT NOT NULL,
                     event            BLOB NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS applied_command (
                     command_id       TEXT PRIMARY KEY,
                     command_sequence INTEGER NOT NULL
                 ) STRICT;
                 CREATE TABLE IF NOT EXISTS meta (
                     key   TEXT PRIMARY KEY,
                     value INTEGER NOT NULL
                 ) STRICT;
                 INSERT OR IGNORE INTO meta (key, value)
                     VALUES ('next_journal_sequence', 1), ('floor', 1),
                            ('applied_command_sequence', 0);",
            )
            .map_err(|source| WorkerError::Journal {
                context: "creating the journal schema",
                source,
            })?;

        // A persistent identity, minted once per journal, for auto-minted
        // idempotency keys: it is what keeps a key unique across Workers and
        // across restarts without any coordination.
        connection
            .execute(
                "INSERT OR IGNORE INTO meta (key, value) VALUES (?1, ?2)",
                params![IDENTITY, mint_identity()],
            )
            .map_err(|source| WorkerError::Journal {
                context: "minting the journal identity",
                source,
            })?;

        let last = meta(&connection, NEXT_SEQUENCE)
            .map_err(|source| WorkerError::Journal {
                context: "reading the journal head",
                source,
            })?
            .saturating_sub(1);

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            head: watch::Sender::new(last),
        })
    }

    /// A narrow, append-only handle onto this journal.
    #[must_use]
    pub fn sink(&self) -> JournalSink {
        JournalSink {
            journal: self.clone(),
        }
    }

    /// Journals one event durably, returning its journal sequence.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the database refuses the append.
    pub async fn record(&self, entry: JournalEntry) -> Result<u64, WorkerError> {
        const CONTEXT: &str = "appending to the journal";
        let sequence = self
            .with_connection(CONTEXT, move |connection| {
                in_transaction(connection, CONTEXT, |transaction| {
                    append_all(transaction, &[entry])
                })
            })
            .await?;
        self.announce(sequence);
        Ok(sequence)
    }

    /// Marks a command applied and journals its effects, in one transaction.
    ///
    /// Dedupes on `command_id`: a redelivered command appends nothing and
    /// reports [`CommandApplication::AlreadyApplied`], so the caller knows not
    /// to run it again. The ack watermark ratchets to the highest
    /// `command_sequence` ever applied — a redelivery of an old command cannot
    /// regress it.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the transaction cannot commit.
    pub async fn apply_command(
        &self,
        command_id: &str,
        command_sequence: u64,
        effects: Vec<JournalEntry>,
    ) -> Result<CommandApplication, WorkerError> {
        const CONTEXT: &str = "recording an applied command";
        let command_id = command_id.to_owned();
        let (application, head) = self
            .with_connection(CONTEXT, move |connection| {
                in_transaction(connection, CONTEXT, |transaction| {
                    let seen: Option<u64> = transaction
                        .query_row(
                            "SELECT command_sequence FROM applied_command WHERE command_id = ?1",
                            params![command_id],
                            |row| row.get(0),
                        )
                        .map(Some)
                        .or_else(|error| match error {
                            rusqlite::Error::QueryReturnedNoRows => Ok(None),
                            other => Err(other),
                        })?;
                    if seen.is_some() {
                        return Ok((CommandApplication::AlreadyApplied, None));
                    }

                    transaction.execute(
                        "INSERT INTO applied_command (command_id, command_sequence)
                             VALUES (?1, ?2)",
                        params![command_id, command_sequence],
                    )?;
                    let acked = meta(transaction, APPLIED_COMMAND)?;
                    set_meta(transaction, APPLIED_COMMAND, acked.max(command_sequence))?;

                    let last = append_all(transaction, &effects)?;
                    Ok((CommandApplication::Applied, Some(last)))
                })
            })
            .await?;

        if let Some(head) = head {
            self.announce(head);
        }
        Ok(application)
    }

    /// Reads up to `limit` journaled events from `from_sequence` inclusive.
    ///
    /// # Errors
    ///
    /// [`WorkerError::TruncatedPast`] when `from_sequence` is below the
    /// truncation floor — the Server was promised contiguity, and sending only
    /// what survives would leave a gap. [`WorkerError::CorruptJournalEntry`]
    /// when a stored row no longer decodes; the sequence must stay contiguous,
    /// so a hole is fatal to the read rather than skipped.
    pub async fn read_from(
        &self,
        from_sequence: u64,
        limit: u32,
    ) -> Result<Vec<JournaledEvent>, WorkerError> {
        self.with_connection("reading the journal", move |connection| {
            let floor = meta(connection, FLOOR).map_err(read_error)?;
            if from_sequence < floor {
                return Err(WorkerError::TruncatedPast {
                    requested: from_sequence,
                    floor,
                });
            }
            read_rows(connection, from_sequence, limit)
        })
        .await
    }

    /// Deletes everything below `resume_from`, which the Server has durably
    /// stored and will never ask for again.
    ///
    /// The floor ratchets up so a later `read_from` below it fails loudly
    /// rather than sending a gapped sequence. The floor is capped at the next
    /// sequence: a Server claiming to hold more than this journal ever wrote
    /// cannot push the floor past the head and wedge the journal.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the deletion cannot commit.
    pub async fn truncate_below(&self, resume_from: u64) -> Result<(), WorkerError> {
        const CONTEXT: &str = "truncating acked events";
        self.with_connection(CONTEXT, move |connection| {
            in_transaction(connection, CONTEXT, |transaction| {
                let next = meta(transaction, NEXT_SEQUENCE)?;
                let target = resume_from.min(next);
                transaction.execute(
                    "DELETE FROM journal WHERE journal_sequence < ?1",
                    params![target],
                )?;
                let floor = meta(transaction, FLOOR)?;
                set_meta(transaction, FLOOR, floor.max(target))
            })
        })
        .await
    }

    /// The highest journal sequence ever assigned — what `PushEventsStart`
    /// reports as `last_local_journal_sequence`.
    ///
    /// Derived from the counter, not from surviving rows, so truncation cannot
    /// regress it and get this Worker refused as having lost its journal.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the counter cannot be read.
    pub async fn last_local_sequence(&self) -> Result<u64, WorkerError> {
        self.with_connection("reading the journal head", |connection| {
            Ok(meta(connection, NEXT_SEQUENCE)
                .map_err(read_error)?
                .saturating_sub(1))
        })
        .await
    }

    /// Whether a command was already applied, by its stable id.
    ///
    /// The pre-execution dedupe check: commands are at-least-once, and the
    /// side effects of applying one (spawning a harness process) must not run
    /// twice. [`Self::apply_command`] re-checks inside its transaction, so
    /// this being a separate read is a fast path, not a race.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the lookup fails.
    pub async fn is_command_applied(&self, command_id: &str) -> Result<bool, WorkerError> {
        let command_id = command_id.to_owned();
        self.with_connection("checking a command for redelivery", move |connection| {
            connection
                .query_row(
                    "SELECT 1 FROM applied_command WHERE command_id = ?1",
                    params![command_id],
                    |_| Ok(()),
                )
                .map(|()| true)
                .or_else(|error| match error {
                    rusqlite::Error::QueryReturnedNoRows => Ok(false),
                    other => Err(other),
                })
                .map_err(read_error)
        })
        .await
    }

    /// The highest command sequence ever applied — what the Worker acks and
    /// resumes `StreamCommands` from.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Journal`] when the watermark cannot be read.
    pub async fn applied_command_sequence(&self) -> Result<u64, WorkerError> {
        self.with_connection("reading the applied-command watermark", |connection| {
            meta(connection, APPLIED_COMMAND).map_err(read_error)
        })
        .await
    }

    /// A watch over the last appended journal sequence.
    ///
    /// How the push pump waits for work without spinning: the value changes
    /// only after a commit, so whatever the watch reports is readable.
    #[must_use]
    pub fn watch_head(&self) -> watch::Receiver<u64> {
        self.head.subscribe()
    }

    fn announce(&self, sequence: u64) {
        // send_replace, not send: an announcement with no listener is normal.
        self.head.send_replace(sequence);
    }

    /// Runs a blocking database operation on the blocking pool.
    ///
    /// A poisoned mutex means an earlier operation panicked while holding the
    /// connection; that is reported rather than propagated, because one bad
    /// statement must not take down a Worker that is supervising live agents.
    async fn with_connection<T, F>(
        &self,
        context: &'static str,
        operation: F,
    ) -> Result<T, WorkerError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, WorkerError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let mut guard = connection
                .lock()
                .map_err(|_| WorkerError::JournalPoisoned(context))?;
            operation(&mut guard)
        })
        .await?
    }
}

/// Runs one transaction, mapping any database failure to `context`.
fn in_transaction<T>(
    connection: &mut Connection,
    context: &'static str,
    operation: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, rusqlite::Error>,
) -> Result<T, WorkerError> {
    let attempt = (|| {
        let transaction = connection.transaction()?;
        let value = operation(&transaction)?;
        transaction.commit()?;
        Ok(value)
    })();
    attempt.map_err(|source| WorkerError::Journal { context, source })
}

/// Reads and decodes journal rows from `from_sequence`, in order.
fn read_rows(
    connection: &Connection,
    from_sequence: u64,
    limit: u32,
) -> Result<Vec<JournaledEvent>, WorkerError> {
    let mut statement = connection
        .prepare(
            "SELECT journal_sequence, session_id, event FROM journal
                 WHERE journal_sequence >= ?1
                 ORDER BY journal_sequence
                 LIMIT ?2",
        )
        .map_err(read_error)?;
    let rows = statement
        .query_map(params![from_sequence, limit], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Vec<u8>>(2)?,
            ))
        })
        .map_err(read_error)?;

    let mut events = Vec::new();
    for row in rows {
        let (journal_sequence, session_id, payload) = row.map_err(read_error)?;
        let event = Event::decode(payload.as_slice()).map_err(|source| {
            WorkerError::CorruptJournalEntry {
                journal_sequence,
                source,
            }
        })?;
        events.push(JournaledEvent {
            journal_sequence,
            session_id: Some(SessionId::new(session_id)),
            event: Some(event),
        });
    }
    Ok(events)
}

/// Encodes and appends every entry, returning the last sequence assigned.
///
/// The counter advances with the rows inside the same transaction, so a
/// rollback rolls both back together and density survives any failure.
fn append_all(
    transaction: &rusqlite::Transaction<'_>,
    entries: &[JournalEntry],
) -> Result<u64, rusqlite::Error> {
    let identity = meta(transaction, IDENTITY)?;
    let mut next = meta(transaction, NEXT_SEQUENCE)?;
    for entry in entries {
        let idempotency_key = if entry.idempotency_key.is_empty() {
            format!("j{identity:016x}:{next}")
        } else {
            entry.idempotency_key.clone()
        };
        let event = Event {
            agent_id: entry.agent_id.clone().map(AgentId::new),
            occurred_at: Some(now()),
            idempotency_key,
            kind: Some(entry.kind.clone()),
        };
        transaction.execute(
            "INSERT INTO journal (journal_sequence, session_id, event) VALUES (?1, ?2, ?3)",
            params![next, entry.session_id, event.encode_to_vec()],
        )?;
        next = next.saturating_add(1);
    }
    set_meta(transaction, NEXT_SEQUENCE, next)?;
    Ok(next.saturating_sub(1))
}

fn meta(connection: &Connection, key: &str) -> Result<u64, rusqlite::Error> {
    connection.query_row(
        "SELECT value FROM meta WHERE key = ?1",
        params![key],
        |row| row.get(0),
    )
}

fn set_meta(connection: &Connection, key: &str, value: u64) -> Result<(), rusqlite::Error> {
    connection.execute(
        "UPDATE meta SET value = ?2 WHERE key = ?1",
        params![key, value],
    )?;
    Ok(())
}

/// A fresh journal identity: unique enough that two Workers' auto-minted
/// idempotency keys cannot collide, without a coordination service or a
/// dependency on a random-number crate.
///
/// Masked to 63 bits because SQLite stores INTEGER as i64 and the value only
/// needs to be distinct, not uniformly random.
fn mint_identity() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    #[allow(clippy::cast_possible_truncation)] // uniqueness needs only the low bits
    let time = nanos as u64;
    // The pid lands in the middle bits, where it survives the cast — two
    // journals created in the same instant by different processes still
    // differ.
    let pid = u64::from(std::process::id());
    (time ^ (pid << 32)) & (i64::MAX as u64)
}

/// When the journal observed an event, as a protobuf timestamp.
fn now() -> Timestamp {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX),
        // Sub-second nanos are always below a billion, far inside i32.
        nanos: i32::try_from(since_epoch.subsec_nanos()).unwrap_or(0),
    }
}

fn open_error(source: rusqlite::Error) -> WorkerError {
    WorkerError::Journal {
        context: "opening the journal database",
        source,
    }
}

fn read_error(source: rusqlite::Error) -> WorkerError {
    WorkerError::Journal {
        context: "reading the journal",
        source,
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::TurnInterrupted;
    use horde_proto::v1::event::Kind;

    use super::{CommandApplication, Journal, JournalEntry};
    use crate::error::WorkerError;

    fn entry(session: &str, key: &str) -> JournalEntry {
        JournalEntry {
            session_id: session.to_owned(),
            agent_id: Some("agent-1".to_owned()),
            idempotency_key: key.to_owned(),
            kind: Kind::TurnInterrupted(TurnInterrupted::default()),
        }
    }

    fn journal() -> Journal {
        Journal::in_memory().expect("an in-memory journal")
    }

    #[tokio::test]
    async fn sequences_are_dense_one_based_and_carried_on_the_events() {
        let journal = journal();
        for (index, key) in ["k1", "k2", "k3"].iter().enumerate() {
            let sequence = journal
                .record(entry("s1", key))
                .await
                .expect("the append succeeds");
            assert_eq!(sequence, index as u64 + 1, "dense and 1-based");
        }

        let read = journal.read_from(1, 10).await.expect("a readable journal");
        assert_eq!(read.len(), 3);
        for (index, journaled) in read.iter().enumerate() {
            assert_eq!(journaled.journal_sequence, index as u64 + 1);
            assert_eq!(
                journaled.session_id.as_ref().map(ToString::to_string),
                Some("s1".to_owned())
            );
        }
    }

    #[tokio::test]
    async fn a_recorded_event_carries_what_the_caller_said_and_when_it_happened() {
        let journal = journal();
        journal
            .sink()
            .record(entry("s1", "key-1"))
            .await
            .expect("the sink appends");

        let read = journal.read_from(1, 10).await.expect("readable");
        let event = read[0].event.as_ref().expect("an event");
        assert_eq!(event.idempotency_key, "key-1");
        assert_eq!(
            event.agent_id.as_ref().map(ToString::to_string),
            Some("agent-1".to_owned())
        );
        assert!(
            event.occurred_at.is_some(),
            "the journal stamps observation time"
        );
        assert!(matches!(event.kind, Some(Kind::TurnInterrupted(_))));
    }

    #[tokio::test]
    async fn the_counter_survives_a_reopen() {
        let dir = tempfile::tempdir().expect("a tempdir");
        let path = dir.path().join("journal.sqlite");

        {
            let journal = Journal::open(&path).expect("a fresh journal");
            journal.record(entry("s1", "k1")).await.expect("append");
            journal.record(entry("s1", "k2")).await.expect("append");
        }

        let reopened = Journal::open(&path).expect("the same journal again");
        assert_eq!(
            reopened.last_local_sequence().await.expect("a head"),
            2,
            "the head is remembered, not re-derived"
        );
        let sequence = reopened.record(entry("s1", "k3")).await.expect("append");
        assert_eq!(sequence, 3, "numbering continues across restarts");
    }

    #[tokio::test]
    async fn truncation_ratchets_the_floor_and_a_read_below_it_fails_loudly() {
        let journal = journal();
        for key in ["k1", "k2", "k3"] {
            journal.record(entry("s1", key)).await.expect("append");
        }

        journal
            .truncate_below(3)
            .await
            .expect("truncation succeeds");

        let refused = journal
            .read_from(1, 10)
            .await
            .expect_err("a read below the floor must fail, not gap the sequence");
        match refused {
            WorkerError::TruncatedPast { requested, floor } => {
                assert_eq!(requested, 1);
                assert_eq!(floor, 3);
            }
            other => panic!("expected TruncatedPast, got {other}"),
        }

        let survivors = journal.read_from(3, 10).await.expect("above the floor");
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].journal_sequence, 3);
    }

    /// The reason the counter is a meta row and never `MAX(journal_sequence)`:
    /// after the Server acks everything and the journal truncates to empty, the
    /// next event must continue the numbering. A MAX-derived head would restart
    /// at 1 and the Server would refuse the Worker as having lost its journal.
    #[tokio::test]
    async fn full_truncation_does_not_regress_the_numbering() {
        let journal = journal();
        for key in ["k1", "k2", "k3"] {
            journal.record(entry("s1", key)).await.expect("append");
        }

        journal
            .truncate_below(4)
            .await
            .expect("everything acked, everything truncated");
        assert_eq!(
            journal.last_local_sequence().await.expect("a head"),
            3,
            "the head reports what was ever assigned, not what survives"
        );

        let sequence = journal.record(entry("s1", "k4")).await.expect("append");
        assert_eq!(sequence, 4, "numbering continues past the truncated rows");
    }

    /// A Server bug (or impostor) claiming to hold more than this journal ever
    /// wrote must not wedge the journal by pushing the floor past the head.
    #[tokio::test]
    async fn the_floor_is_capped_at_the_head() {
        let journal = journal();
        journal
            .truncate_below(100)
            .await
            .expect("an over-eager truncation is clamped, not obeyed");

        let sequence = journal.record(entry("s1", "k1")).await.expect("append");
        assert_eq!(sequence, 1);
        let read = journal.read_from(1, 10).await.expect("still readable");
        assert_eq!(read.len(), 1);
    }

    #[tokio::test]
    async fn a_redelivered_command_is_applied_once_and_appends_nothing() {
        let journal = journal();

        let first = journal
            .apply_command("s1:2", 1, vec![entry("s1", "k1")])
            .await
            .expect("the first delivery applies");
        assert_eq!(first, CommandApplication::Applied);

        let second = journal
            .apply_command("s1:2", 1, vec![entry("s1", "k1-again")])
            .await
            .expect("a redelivery is not an error");
        assert_eq!(second, CommandApplication::AlreadyApplied);

        let read = journal.read_from(1, 10).await.expect("readable");
        assert_eq!(read.len(), 1, "the redelivery appended nothing");
        assert_eq!(
            journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            1
        );
    }

    #[tokio::test]
    async fn the_ack_watermark_ratchets_and_never_regresses() {
        let journal = journal();
        journal
            .apply_command("s1:2", 1, Vec::new())
            .await
            .expect("apply");
        journal
            .apply_command("s1:4", 2, Vec::new())
            .await
            .expect("apply");

        // An old command redelivered after a newer one was applied.
        journal
            .apply_command("s1:2", 1, Vec::new())
            .await
            .expect("redelivery");

        assert_eq!(
            journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            2,
            "the watermark is the highest ever applied, not the last seen"
        );
    }

    #[tokio::test]
    async fn command_effects_and_events_share_one_dense_numbering() {
        let journal = journal();
        journal.record(entry("s1", "k1")).await.expect("append");
        journal
            .apply_command("s1:2", 1, vec![entry("s1", "k2"), entry("s2", "k3")])
            .await
            .expect("apply");
        journal.record(entry("s2", "k4")).await.expect("append");

        let read = journal.read_from(1, 10).await.expect("readable");
        let sequences: Vec<u64> = read.iter().map(|event| event.journal_sequence).collect();
        assert_eq!(sequences, vec![1, 2, 3, 4], "one space, no gaps");
    }

    #[tokio::test]
    async fn the_head_watch_reports_the_last_committed_sequence() {
        let journal = journal();
        let watch = journal.watch_head();
        assert_eq!(*watch.borrow(), 0, "an empty journal has no head");

        journal.record(entry("s1", "k1")).await.expect("append");
        assert_eq!(
            *watch.borrow(),
            1,
            "the watch moves only after the commit, so this value is readable"
        );
    }

    #[tokio::test]
    async fn an_empty_idempotency_key_is_minted_stored_and_stable() {
        let journal = journal();
        journal.record(entry("s1", "")).await.expect("append");
        journal.record(entry("s1", "")).await.expect("append");

        let keys = |events: &[horde_proto::v1::JournaledEvent]| -> Vec<String> {
            events
                .iter()
                .map(|journaled| {
                    journaled
                        .event
                        .as_ref()
                        .expect("an event")
                        .idempotency_key
                        .clone()
                })
                .collect()
        };

        let first_read = keys(&journal.read_from(1, 10).await.expect("readable"));
        assert!(
            !first_read[0].is_empty() && !first_read[1].is_empty(),
            "an empty key would defeat the server's dedupe entirely"
        );
        assert_ne!(
            first_read[0], first_read[1],
            "two events must never share a minted key"
        );

        // A replay reads the same stored bytes, so the keys cannot move.
        let second_read = keys(&journal.read_from(1, 10).await.expect("readable"));
        assert_eq!(
            first_read, second_read,
            "minted keys are stored, not derived"
        );
    }

    #[tokio::test]
    async fn two_journals_mint_from_different_identities() {
        let first = journal();
        let second = journal();
        first.record(entry("s1", "")).await.expect("append");
        second.record(entry("s1", "")).await.expect("append");

        async fn first_key(journal: &Journal) -> String {
            journal.read_from(1, 10).await.expect("readable")[0]
                .event
                .as_ref()
                .expect("an event")
                .idempotency_key
                .clone()
        }
        assert_ne!(
            first_key(&first).await,
            first_key(&second).await,
            "same sequence on two workers must not collide in the server's dedupe"
        );
    }

    #[tokio::test]
    async fn redelivery_is_visible_before_anything_executes() {
        let journal = journal();
        assert!(
            !journal
                .is_command_applied("s1:2")
                .await
                .expect("a readable journal"),
            "a command never seen is not applied"
        );

        journal
            .apply_command("s1:2", 1, Vec::new())
            .await
            .expect("apply");

        assert!(
            journal
                .is_command_applied("s1:2")
                .await
                .expect("a readable journal"),
            "the pre-execution check is what stops a harness spawning twice"
        );
    }

    /// A row that no longer decodes is corruption, and it is fatal to the read:
    /// skipping it would put a hole in a sequence the Server requires to be
    /// contiguous.
    #[tokio::test]
    async fn a_corrupt_row_is_fatal_to_the_read_and_names_its_position() {
        let journal = journal();
        journal.record(entry("s1", "k1")).await.expect("append");

        {
            let guard = journal.connection.lock().expect("an unpoisoned journal");
            guard
                .execute(
                    "UPDATE journal SET event = ?1 WHERE journal_sequence = 1",
                    rusqlite::params![vec![0xff_u8, 0xff, 0xff]],
                )
                .expect("the corruption is inserted");
        }

        let error = journal
            .read_from(1, 10)
            .await
            .expect_err("garbage must not decode");
        match error {
            WorkerError::CorruptJournalEntry {
                journal_sequence, ..
            } => assert_eq!(journal_sequence, 1, "the position is the diagnosis"),
            other => panic!("expected CorruptJournalEntry, got {other}"),
        }
    }
}
