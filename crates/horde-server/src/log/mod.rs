//! The append-only event log (ADR-0001).
//!
//! This module is the async face of [`store`], and it owns exactly two things
//! the blocking layer cannot: the boundary onto the blocking pool, and the
//! notification channel that lets a replay hand over to a live tail without a
//! gap.
//!
//! # The blocking boundary
//!
//! SQLite is a blocking API. Rather than adopt an async SQL layer that hides
//! that fact, every database call runs inside `spawn_blocking` against a
//! connection behind a `std::sync::Mutex`. The guard is taken and released
//! *inside* the blocking closure and never crosses an `.await`, which is the
//! rule that makes a `std` mutex safe here.
//!
//! Serializing writers through one connection is not a concession: it is what
//! makes per-session sequences dense. A pool of writers would each have to
//! re-derive the head under contention, and denseness would depend on the
//! transaction isolation being right every time rather than on there being only
//! one writer.
//!
//! Reads do not share that serialization (ADR-0013). A file-backed log keeps
//! a small pool of query-only connections beside the writer, and WAL lets
//! each read its own snapshot while an append is in flight — so a replay, a
//! projection catch-up or a placement pass never queues behind a batch of
//! journal appends, and never holds the writer up either.
//!
//! # Why a tail cannot miss an event
//!
//! [`EventLog::tail`] uses two independent mechanisms, because the first one
//! alone is correct but not verifiable:
//!
//! 1. **Subscribe before replaying.** From that moment every append is
//!    announced, so an event written during the replay is either in the replay
//!    or in the channel.
//! 2. **Sweep after subscribing.** The head is read again and any events the
//!    replay missed are folded into the history before it is returned.
//!
//! Mechanism 2 is what actually makes the guarantee testable. The subscribe
//! window is only observable when the *final* append of a session lands inside
//! it — [`Tail::next_batch`] re-reads from `delivered + 1` on any notification,
//! so an earlier loss is self-healing, and the last one leaves the follower
//! waiting for a notification that already fired. That window is real but too
//! narrow to lose reliably in a test, which makes it exactly the kind of
//! ordering that rots: a refactor could swap the two lines and nothing would go
//! red. The sweep removes the dependence on the ordering altogether, and its
//! postcondition — the returned history reaches the head — is a fact a test can
//! assert directly.

pub mod schema;
pub mod store;

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use tokio::sync::broadcast;
use tracing::instrument;

pub use store::{Appended, GlobalEvent, PendingEvent, StoredEvent};

use crate::error::ServerError;

/// How many appends may queue for a slow subscriber before it is told it fell
/// behind.
///
/// A lagging subscriber is not an error: it re-reads from the log, which is
/// authoritative. The buffer only decides how often that happens.
const NOTIFY_CAPACITY: usize = 1024;

/// An append happened. Deliberately just a position: a subscriber that missed
/// it reads the log rather than trusting a payload that arrived out of band.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    pub session_id: String,
    pub sequence: u64,
}

/// How many read connections a file-backed log keeps beside its writer.
///
/// Enough that the projection's catch-up, a replaying tail and a placement
/// pass do not queue behind one another; few enough that file handles are
/// not a concern. A busy pool is waited on, never grown.
const READ_POOL_SIZE: usize = 4;

/// The append-only log, shared by every service.
#[derive(Clone)]
pub struct EventLog {
    /// The one writer. Every append goes through it, which is what keeps
    /// per-session sequences dense (see the module docs).
    connection: Arc<Mutex<Connection>>,
    /// Query-only connections for reads, so a read never waits on the writer
    /// (ADR-0013). Empty for an in-memory log, whose database is private to
    /// its one connection: there reads share the writer.
    readers: Arc<ReadPool>,
    notifier: broadcast::Sender<Notification>,
}

/// The read connections, handed out round-robin.
#[derive(Default)]
struct ReadPool {
    connections: Vec<Mutex<Connection>>,
    next: std::sync::atomic::AtomicUsize,
}

impl ReadPool {
    /// `size` query-only connections onto the database at `path`.
    fn open(path: &str, size: usize) -> Result<Self, ServerError> {
        let mut connections = Vec::with_capacity(size);
        for _ in 0..size {
            connections.push(Mutex::new(store::open_reader(path)?));
        }
        Ok(Self {
            connections,
            next: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// The connection a read should use: the first idle one from this
    /// read's starting turn, or — every one busy — the turn's own, waited
    /// on. `None` when the pool is empty.
    fn checkout(&self) -> Option<Result<std::sync::MutexGuard<'_, Connection>, ServerError>> {
        let count = self.connections.len();
        let start = self
            .next
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            .checked_rem(count)?;
        let own = self.connections.get(start)?;
        let idle = self
            .connections
            .iter()
            .cycle()
            .skip(start)
            .take(count)
            .find_map(|connection| connection.try_lock().ok());
        Some(idle.map_or_else(|| own.lock().map_err(|_| poisoned()), Ok))
    }
}

/// The failure a poisoned connection mutex reports.
fn poisoned() -> ServerError {
    ServerError::Storage {
        context: "acquiring the connection after an earlier panic",
        source: rusqlite::Error::InvalidQuery,
    }
}

impl std::fmt::Debug for EventLog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventLog")
            .field("subscribers", &self.notifier.receiver_count())
            .finish_non_exhaustive()
    }
}

impl EventLog {
    /// Opens (or creates) a log at `path`.
    ///
    /// The writer opens first, so the schema exists before any reader
    /// connects to it.
    pub fn open(path: &str) -> Result<Self, ServerError> {
        let writer = store::open(path)?;
        Ok(Self::from_connections(
            writer,
            ReadPool::open(path, READ_POOL_SIZE)?,
        ))
    }

    /// Opens a private in-memory log.
    pub fn in_memory() -> Result<Self, ServerError> {
        Ok(Self::from_connections(
            store::open_in_memory()?,
            ReadPool::default(),
        ))
    }

    fn from_connections(connection: Connection, readers: ReadPool) -> Self {
        let (notifier, _) = broadcast::channel(NOTIFY_CAPACITY);
        Self {
            connection: Arc::new(Mutex::new(connection)),
            readers: Arc::new(readers),
            notifier,
        }
    }

    /// Runs a blocking database operation on the blocking pool.
    ///
    /// A poisoned mutex means a previous operation panicked while holding the
    /// connection. That is reported as a storage failure rather than
    /// propagating the panic, so one bad query cannot take the process down.
    async fn with_connection<T, F>(&self, operation: F) -> Result<T, ServerError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, ServerError> + Send + 'static,
    {
        let connection = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || {
            let mut guard = connection.lock().map_err(|_| poisoned())?;
            operation(&mut guard)
        })
        .await?
    }

    /// Runs a blocking read on a pool connection, or on the writer when the
    /// log has no pool (in memory).
    ///
    /// Safe to route every read here because a read is its own implicit
    /// transaction: under WAL it sees every commit made before it began, so
    /// an append that has returned is visible to the next read wherever it
    /// runs.
    async fn with_reader<T, F>(&self, operation: F) -> Result<T, ServerError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, ServerError> + Send + 'static,
    {
        let readers = Arc::clone(&self.readers);
        let writer = Arc::clone(&self.connection);
        tokio::task::spawn_blocking(move || match readers.checkout() {
            Some(guard) => operation(&*guard?),
            None => operation(&*writer.lock().map_err(|_| poisoned())?),
        })
        .await?
    }

    /// Appends one event and announces its position.
    ///
    /// Announcing only after the commit means a subscriber can never be told
    /// about an event it would then fail to read.
    #[instrument(skip(self, event), fields(session_id = %event.session_id))]
    pub async fn append(&self, event: PendingEvent) -> Result<Appended, ServerError> {
        let session_id = event.session_id.clone();
        let appended = self
            .with_connection(move |connection| store::append(connection, &event))
            .await?;

        if appended.deduplicated {
            tracing::debug!(
                sequence = appended.sequence,
                "event already present; not re-appended"
            );
            return Ok(appended);
        }

        self.announce(session_id, appended.sequence);
        Ok(appended)
    }

    /// Tells every subscriber an append landed.
    fn announce(&self, session_id: String, sequence: u64) {
        // A send error means nobody is listening, which is normal.
        let _ = self.notifier.send(Notification {
            session_id,
            sequence,
        });
    }

    /// Appends `events` in one transaction (ADR-0013) — see
    /// [`store::append_all`] for the ordering, the dedupe and `follow_up` —
    /// and announces every new row in append order once it has committed.
    ///
    /// Answers each event's own outcome, in order; follow-ups are announced
    /// but not answered, since the caller wrote them.
    #[instrument(skip_all, fields(events = events.len()))]
    pub async fn append_batch<F>(
        &self,
        events: Vec<PendingEvent>,
        follow_up: F,
    ) -> Result<Vec<Appended>, ServerError>
    where
        F: FnMut(usize, &Appended) -> Option<PendingEvent> + Send + 'static,
    {
        let sessions: Vec<String> = events
            .iter()
            .map(|event| event.session_id.clone())
            .collect();
        let batched = self
            .with_connection(move |connection| store::append_all(connection, &events, follow_up))
            .await?;
        let mut answers = Vec::with_capacity(batched.len());
        for (session_id, landed) in sessions.into_iter().zip(batched) {
            let writes = std::iter::once((session_id, &landed.appended)).chain(
                landed
                    .follow_up
                    .iter()
                    .map(|(session, appended)| (session.clone(), appended)),
            );
            for (session_id, appended) in writes {
                if !appended.deduplicated {
                    self.announce(session_id, appended.sequence);
                }
            }
            answers.push(landed.appended);
        }
        Ok(answers)
    }

    /// Reads up to `limit` events from `from_sequence` inclusive.
    pub async fn read_from(
        &self,
        session_id: &str,
        from_sequence: u64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, ServerError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |connection| {
            store::read_from(connection, &session_id, from_sequence, limit)
        })
        .await
    }

    /// Every event of a session, in order.
    ///
    /// The reference against which partial reads are compared; the proptests
    /// use it as the definition of "full replay".
    pub async fn read_all(&self, session_id: &str) -> Result<Vec<StoredEvent>, ServerError> {
        self.read_from(session_id, 1, u32::MAX).await
    }

    /// Highest sequence in a session; zero when it has none.
    pub async fn head(&self, session_id: &str) -> Result<u64, ServerError> {
        let session_id = session_id.to_owned();
        self.with_reader(move |connection| store::head(connection, &session_id))
            .await
    }

    /// Reads up to `limit` events from anywhere in the log, in append order,
    /// starting after `after_row`.
    ///
    /// The whole-log read the per-worker command projection folds over; see
    /// [`store::read_global_from`] for why append order is `rowid`.
    pub async fn read_global_from(
        &self,
        after_row: u64,
        limit: u32,
    ) -> Result<Vec<GlobalEvent>, ServerError> {
        self.with_reader(move |connection| store::read_global_from(connection, after_row, limit))
            .await
    }

    /// Every session that has at least one event, oldest first.
    pub async fn session_ids(&self) -> Result<Vec<String>, ServerError> {
        self.with_reader(store::session_ids).await
    }

    /// The event already stored under `(session_id, key)`: its sequence and
    /// payload. `None` for an empty key — the log's "never deduplicate".
    pub async fn stored_under_key(
        &self,
        session_id: &str,
        key: &str,
    ) -> Result<Option<(u64, Vec<u8>)>, ServerError> {
        let session_id = session_id.to_owned();
        let key = key.to_owned();
        self.with_reader(move |connection| store::stored_under_key(connection, &session_id, &key))
            .await
    }

    /// The highest journal sequence durably stored for a worker.
    pub async fn worker_journal_watermark(&self, worker_id: &str) -> Result<u64, ServerError> {
        let worker_id = worker_id.to_owned();
        self.with_reader(move |connection| store::worker_journal_watermark(connection, &worker_id))
            .await
    }

    /// Subscribes to append notifications from this moment on.
    ///
    /// Call this *before* replaying if the replay is going to be followed by a
    /// live tail; see the module documentation.
    pub fn subscribe(&self) -> broadcast::Receiver<Notification> {
        self.notifier.subscribe()
    }

    /// Replays a session from `from_sequence`, then follows it live.
    ///
    /// Returns the historical events and a [`Tail`] positioned so that the next
    /// event it yields is the one immediately after them.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub async fn tail(
        &self,
        session_id: &str,
        from_sequence: u64,
    ) -> Result<(Vec<StoredEvent>, Tail), ServerError> {
        // Step 1: subscribe before reading, so nothing appended during the read
        // can slip between the two.
        let notifications = self.subscribe();

        // Step 2: replay history.
        let mut history = self.read_all_from(session_id, from_sequence).await?;
        let mut delivered = history
            .last()
            .map_or(from_sequence.max(1).saturating_sub(1), |event| {
                event.sequence
            });

        // Step 3: sweep. Anything appended while step 2 ran is picked up here
        // rather than being left to the notification that may have raced us.
        // Bounded by the head read at entry, so a continuously written session
        // cannot keep this loop alive indefinitely.
        let head_after_replay = self.head(session_id).await?;
        if needs_sweep(head_after_replay, delivered) {
            let missed = self
                .read_all_from(session_id, sweep_from(delivered))
                .await?;
            if let Some(last) = missed.last() {
                delivered = last.sequence;
                history.extend(missed);
            }
        }

        Ok((
            history,
            Tail {
                log: self.clone(),
                session_id: session_id.to_owned(),
                notifications,
                delivered,
            },
        ))
    }

    /// Replays several sessions and follows them all through one channel —
    /// the merged tail the multi-session overview and the global inbox read,
    /// so a client holds one stream instead of one per session.
    ///
    /// `cursors` name the sessions to replay and from where; those get the
    /// same no-loss guarantee as [`EventLog::tail`] (subscribe, replay,
    /// sweep). With `all_sessions`, every other session — including ones that
    /// do not exist yet — is followed from its current head: no history is
    /// replayed for them, because "the overview wants the tail" is exactly
    /// what the flag means (`control_plane.proto`). `excluded` names streams
    /// the all-sessions net must not catch (the workspace catalog lives in
    /// the log but is not a session); an excluded stream can still be
    /// followed by naming it in a cursor.
    ///
    /// History is returned per cursor session, in cursor order. There is no
    /// global order across sessions to preserve — sequences are per-session —
    /// and every consumer folds per-session state.
    pub async fn tail_merged(
        &self,
        cursors: &[(String, u64)],
        all_sessions: bool,
        excluded: &[String],
    ) -> Result<(Vec<StoredEvent>, MultiTail), ServerError> {
        // Subscribe before reading, for the same reason `tail` does.
        let notifications = self.subscribe();

        let mut delivered = std::collections::BTreeMap::new();
        let mut history = Vec::new();
        for (session_id, from_sequence) in cursors {
            let events = self.read_all_from(session_id, *from_sequence).await?;
            let last = events
                .last()
                .map_or((*from_sequence).max(1).saturating_sub(1), |event| {
                    event.sequence
                });
            history.extend(events);
            delivered.insert(session_id.clone(), last);
        }

        if all_sessions {
            for session_id in self.session_ids().await? {
                if delivered.contains_key(&session_id) || excluded.contains(&session_id) {
                    continue;
                }
                // From the current head: the overview wants the live tail,
                // not a replay of every session it can see.
                let head = self.head(&session_id).await?;
                delivered.insert(session_id, head);
            }
        }

        // Sweep the cursor sessions, exactly as `tail` does: anything that
        // landed during the replay is folded into the history rather than
        // left to a notification that may have raced the subscription.
        for (session_id, _) in cursors {
            let head_after_replay = self.head(session_id).await?;
            let already = delivered.get(session_id).copied().unwrap_or(0);
            if needs_sweep(head_after_replay, already) {
                let missed = self.read_all_from(session_id, sweep_from(already)).await?;
                if let Some(last) = missed.last() {
                    delivered.insert(session_id.clone(), last.sequence);
                    history.extend(missed);
                }
            }
        }

        Ok((
            history,
            MultiTail {
                log: self.clone(),
                notifications,
                delivered,
                all_sessions,
                excluded: excluded.iter().cloned().collect(),
            },
        ))
    }

    /// Reads every event from an offset, paging so one enormous session cannot
    /// be pulled into memory by a single unbounded query.
    /// Terminates on an empty page rather than on a short one.
    ///
    /// A short-page early exit saves exactly one indexed query per replay, and
    /// costs a branch whose failure mode is silent truncation. Mutation testing
    /// made the trade concrete: `page.len() > PAGE` is indistinguishable from
    /// the correct `<` — a page can never exceed the limit it was read with, so
    /// the branch simply never fires and the empty-page exit does the work
    /// anyway. A condition that cannot be wrong in one direction is not
    /// carrying its weight.
    async fn read_all_from(
        &self,
        session_id: &str,
        from_sequence: u64,
    ) -> Result<Vec<StoredEvent>, ServerError> {
        const PAGE: u32 = 512;

        let mut all = Vec::new();
        let mut next = from_sequence.max(1);
        loop {
            let page = self.read_from(session_id, next, PAGE).await?;
            let Some(last) = page.last() else {
                break;
            };
            next = last.sequence.saturating_add(1);
            all.extend(page);
        }
        Ok(all)
    }
}

/// Whether the head moved past what the replay delivered.
///
/// Extracted from [`EventLog::tail`] because an inline comparison there is not
/// reachable by a test: the sweep only differs from a no-op when an append
/// lands between the replay and this check, and that race cannot be forced
/// deterministically. As a predicate it is an ordinary function with an
/// ordinary truth table, so the boundary is pinned instead of hoped for.
fn needs_sweep(head_after_replay: u64, delivered: u64) -> bool {
    head_after_replay > delivered
}

/// The first sequence a sweep must ask for.
///
/// One past what was delivered: asking for `delivered` again would re-deliver
/// the last event, and asking beyond it would skip one. Extracted for the third
/// time in this module for the same reason — the sweep's own dedup and the
/// tail's re-read heal a wrong window, so nothing downstream observes the
/// offset and `delivered - 1` or `delivered * 1` survived every end-to-end
/// test. As a function it is one line with three cases.
fn sweep_from(delivered: u64) -> u64 {
    delivered.saturating_add(1)
}

/// Whether a notification tells this tail nothing new.
///
/// Extracted for the same reason as [`needs_sweep`]: inline, the two halves of
/// this condition are not separately reachable. A tail only ever sees
/// notifications it already delivered *or* notifications for other sessions in
/// the same run, so an `&&` still behaves correctly often enough to pass —
/// as a predicate the truth table is four cases and all four are asserted.
fn is_stale(notification: &Notification, session_id: &str, delivered: u64) -> bool {
    notification.session_id != session_id || notification.sequence <= delivered
}

/// A live follow of one session, already positioned after its replay.
pub struct Tail {
    log: EventLog,
    session_id: String,
    notifications: broadcast::Receiver<Notification>,
    /// Highest sequence already handed to the caller.
    delivered: u64,
}

impl Tail {
    /// The highest sequence this tail has produced so far.
    #[must_use]
    pub fn delivered(&self) -> u64 {
        self.delivered
    }

    /// Waits for the next batch of events in this session.
    ///
    /// `None` means the log was dropped and no further events can arrive.
    ///
    /// Notifications are only a prompt to read: a notification for another
    /// session is ignored, one for an event already delivered is dropped, and a
    /// subscriber that fell behind re-reads from the log instead of giving up.
    /// That last case is why lagging is survivable — the channel is a hint, the
    /// log is the truth.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<StoredEvent>, ServerError>> {
        loop {
            match self.notifications.recv().await {
                Ok(notification) => {
                    if is_stale(&notification, &self.session_id, self.delivered) {
                        continue;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(
                        missed,
                        session_id = %self.session_id,
                        "tail fell behind; re-reading from the log"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }

            let from = self.delivered.saturating_add(1);
            match self.log.read_from(&self.session_id, from, 512).await {
                Ok(batch) => {
                    if let Some(last) = batch.last() {
                        self.delivered = last.sequence;
                        return Some(Ok(batch));
                    }
                    // The notification was for a session-mate we had already
                    // read, or for a deduplicated append. Keep waiting.
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// Whether a notification tells a merged tail nothing.
///
/// A followed session skips what was already delivered — the same boundary
/// [`is_stale`] pins. An *unfollowed* session is news only when the tail
/// follows all sessions and the stream is not excluded; that arm is how a
/// session created after the subscription joins the merge. Extracted for the
/// same reason as `is_stale`: the corners are not separately reachable
/// through a live stream, and as a predicate all six rows are assertable.
fn merged_skip(delivered: Option<u64>, all_sessions: bool, excluded: bool, sequence: u64) -> bool {
    match delivered {
        Some(delivered) => sequence <= delivered,
        None => !all_sessions || excluded,
    }
}

/// A live follow of several sessions through one channel.
///
/// The merged counterpart of [`Tail`], built by [`EventLog::tail_merged`].
/// One receiver, one `delivered` watermark per session; a batch never mixes
/// sessions, because each wake reads exactly the session its notification
/// named.
pub struct MultiTail {
    log: EventLog,
    notifications: broadcast::Receiver<Notification>,
    /// Highest sequence handed out, per followed session.
    delivered: std::collections::BTreeMap<String, u64>,
    /// Adopt sessions this map has never seen, from their current position.
    all_sessions: bool,
    /// Streams the all-sessions net must not catch.
    excluded: std::collections::BTreeSet<String>,
}

impl MultiTail {
    /// Every followed session and the highest sequence delivered for it —
    /// the per-session heads a catch-up marker reports.
    #[must_use]
    pub fn heads(&self) -> Vec<(String, u64)> {
        self.delivered
            .iter()
            .map(|(session_id, delivered)| (session_id.clone(), *delivered))
            .collect()
    }

    /// Waits for the next batch of events from any followed session.
    ///
    /// `None` means the log was dropped. Same contract as [`Tail`]: the
    /// channel is a hint, the log is the truth, and a lagged subscriber
    /// re-reads every followed session rather than giving up.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<StoredEvent>, ServerError>> {
        loop {
            let woken_for = match self.notifications.recv().await {
                Ok(notification) => {
                    let delivered = self.delivered.get(&notification.session_id).copied();
                    let excluded = self.excluded.contains(&notification.session_id);
                    if merged_skip(
                        delivered,
                        self.all_sessions,
                        excluded,
                        notification.sequence,
                    ) {
                        continue;
                    }
                    Some(notification.session_id)
                }
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(missed, "merged tail fell behind; re-reading every session");
                    None
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            };

            let read = match woken_for {
                Some(session_id) => self.read_session(&session_id).await,
                None => self.read_everything().await,
            };
            match read {
                Ok(batch) if batch.is_empty() => {
                    // A deduplicated append, or a session-mate already read.
                }
                Ok(batch) => return Some(Ok(batch)),
                Err(error) => return Some(Err(error)),
            }
        }
    }

    /// Reads one session forward from its watermark.
    async fn read_session(&mut self, session_id: &str) -> Result<Vec<StoredEvent>, ServerError> {
        let from = sweep_from(self.delivered.get(session_id).copied().unwrap_or(0));
        let batch = self.log.read_from(session_id, from, 512).await?;
        if let Some(last) = batch.last() {
            self.delivered.insert(session_id.to_owned(), last.sequence);
        }
        Ok(batch)
    }

    /// The lag recovery: rediscover sessions, then read every followed one
    /// forward. Notifications were dropped, so nothing narrower is safe.
    async fn read_everything(&mut self) -> Result<Vec<StoredEvent>, ServerError> {
        if self.all_sessions {
            for session_id in self.log.session_ids().await? {
                if !self.delivered.contains_key(&session_id) && !self.excluded.contains(&session_id)
                {
                    // Never seen and the notification that would have
                    // announced it is gone: adopt it from the start.
                    self.delivered.insert(session_id, 0);
                }
            }
        }
        let sessions: Vec<String> = self.delivered.keys().cloned().collect();
        let mut batch = Vec::new();
        for session_id in sessions {
            batch.extend(self.read_session(&session_id).await?);
        }
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::store::pending;
    use super::{EventLog, PendingEvent};

    /// The ceiling on any wait in these tests, so a hang fails fast.
    const BOUND: std::time::Duration = std::time::Duration::from_secs(10);

    /// A file-backed log — so it has a read pool — holding one event in
    /// `s1`, and the directory that must outlive it.
    async fn file_backed_with_one_event() -> (tempfile::TempDir, EventLog) {
        let directory = tempfile::tempdir().expect("a directory");
        let path = directory.path().join("log.sqlite");
        let log = EventLog::open(path.to_str().expect("a utf-8 path")).expect("a log");
        log.append(pending("s1", b"a")).await.expect("append");
        (directory, log)
    }

    /// The read pool's whole point: with the writer held — an append in
    /// flight, as far as a reader can tell — every kind of read still
    /// answers, from its own connection.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::await_holding_lock,
        reason = "holding the writer across the reads is the arrangement under test"
    )]
    async fn reads_on_a_file_backed_log_do_not_wait_for_the_writer() {
        let (_directory, log) = file_backed_with_one_event().await;

        let held = log.connection.lock().expect("the writer");
        let reads = async {
            assert_eq!(log.head("s1").await.expect("head"), 1);
            assert_eq!(log.read_all("s1").await.expect("replay").len(), 1);
            assert_eq!(log.read_global_from(0, 10).await.expect("global").len(), 1);
            assert_eq!(log.session_ids().await.expect("ids"), vec!["s1".to_owned()]);
            assert_eq!(log.stored_under_key("s1", "k").await.expect("key"), None);
            assert_eq!(log.worker_journal_watermark("w").await.expect("mark"), 0);
        };
        tokio::time::timeout(BOUND, reads)
            .await
            .expect("no read waited on the held writer");
        drop(held);
    }

    /// Every pool connection busy is a wait, not a failure: the read lands
    /// once one frees.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(
        clippy::await_holding_lock,
        reason = "holding every reader across the wait is the arrangement under test"
    )]
    async fn a_read_waits_for_a_busy_pool_rather_than_failing() {
        let (_directory, log) = file_backed_with_one_event().await;

        let busy: Vec<_> = log
            .readers
            .connections
            .iter()
            .map(|connection| connection.lock().expect("a reader"))
            .collect();
        let read = tokio::spawn({
            let log = log.clone();
            async move { log.head("s1").await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!read.is_finished(), "every reader is held");
        drop(busy);
        let head = tokio::time::timeout(BOUND, read)
            .await
            .expect("the read lands once a reader frees")
            .expect("the task")
            .expect("head");
        assert_eq!(head, 1);
    }

    /// A batch announces each new row once it has committed — follow-ups
    /// included, in append order — and answers each event's own outcome.
    #[tokio::test]
    async fn a_batch_announces_every_new_row_in_order_and_answers_its_events() {
        let log = EventLog::in_memory().expect("log");
        log.append(pending("s1", b"a")).await.expect("append");
        let mut notifications = log.subscribe();
        let key = |session: &str, key: &str| PendingEvent {
            idempotency_key: key.to_owned(),
            ..pending(session, b"k")
        };

        let answers = log
            .append_batch(
                vec![key("s1", "dup"), key("s2", "x"), key("s1", "dup")],
                |index, _| (index == 1).then(|| pending("dead", b"d")),
            )
            .await
            .expect("the batch");

        let positions: Vec<(u64, bool)> = answers
            .iter()
            .map(|appended| (appended.sequence, appended.deduplicated))
            .collect();
        assert_eq!(positions, vec![(2, false), (1, false), (2, true)]);
        let mut announced = Vec::new();
        while let Ok(notification) = notifications.try_recv() {
            announced.push((notification.session_id, notification.sequence));
        }
        assert_eq!(
            announced,
            vec![
                ("s1".to_owned(), 2),
                ("s2".to_owned(), 1),
                ("dead".to_owned(), 1)
            ],
            "the deduplicated write is not announced"
        );
    }

    /// A log with two events already in session `s1`.
    async fn log_with_two_events() -> EventLog {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");
        log.append(pending("s1", b"b")).await.expect("append");
        log
    }

    /// `Debug` on the log is what an operator sees in a span; an empty struct
    /// tells them nothing about whether anyone is attached.
    #[tokio::test]
    async fn the_debug_view_reports_how_many_tails_are_attached() {
        let log = EventLog::in_memory().expect("in-memory log");
        assert!(format!("{log:?}").contains("EventLog"));
        assert!(
            format!("{log:?}").contains("subscribers: 0"),
            "got {:?}",
            format!("{log:?}")
        );

        let _subscriber = log.subscribe();
        assert!(
            format!("{log:?}").contains("subscribers: 1"),
            "an attached tail must be visible: {:?}",
            format!("{log:?}")
        );
    }

    /// The sweep exists to fold in whatever landed during the replay. It must
    /// fire when the head moved and stay quiet when it did not — a comparison
    /// that always fires would re-read on every tail, and one that never fires
    /// reopens the window the sweep closes.
    #[tokio::test]
    async fn the_sweep_folds_in_events_that_landed_during_the_replay() {
        let log = EventLog::in_memory().expect("in-memory log");
        for _ in 0..3 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        // Nothing moved between the read and the sweep: history is exactly the
        // three events, and `delivered` sits on the last of them.
        let (history, tail) = log.tail("s1", 1).await.expect("tail");
        assert_eq!(history.len(), 3);
        assert_eq!(tail.delivered(), 3);

        // A tail taken at the head must return nothing and still be positioned
        // correctly, which is the boundary case `>` versus `>=` disagrees on.
        let (empty, tail) = log.tail("s1", 4).await.expect("tail");
        assert!(
            empty.is_empty(),
            "there is no event 4 yet, so the sweep must find nothing"
        );
        assert_eq!(tail.delivered(), 3);
    }

    /// Paging stops on a short page. Getting the comparison wrong either stops
    /// after the first full page or never stops at all.
    #[tokio::test]
    async fn replaying_a_session_longer_than_one_page_returns_every_event() {
        let log = EventLog::in_memory().expect("in-memory log");
        // One more than the 512-event page, so the loop must go round twice.
        for _ in 0..513 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        let (history, _tail) = log.tail("s1", 1).await.expect("tail");
        let sequences: Vec<u64> = history.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            (1..=513).collect::<Vec<u64>>(),
            "a page boundary must not truncate or duplicate the replay"
        );
    }

    /// `next_batch` skips a notification when it is for another session OR for
    /// something already delivered. With `&&` it would skip only when both were
    /// true, so a duplicate for this session would be re-read forever.
    #[tokio::test]
    async fn a_tail_ignores_notifications_it_has_already_delivered() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");

        // Replay everything, so the next notification for s1 is one the tail
        // has already seen.
        let (history, mut tail) = log.tail("s1", 1).await.expect("tail");
        assert_eq!(history.len(), 1);

        // A notification for another session must not wake this tail into
        // returning anything.
        log.append(pending("other", b"x")).await.expect("append");
        log.append(pending("s1", b"b")).await.expect("append");

        let batch = tokio::time::timeout(std::time::Duration::from_millis(500), tail.next_batch())
            .await
            .expect("the tail must not stall")
            .expect("the log outlives the tail")
            .expect("read");

        let sequences: Vec<u64> = batch.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            vec![2],
            "only the new s1 event, and never a redelivery of event 1"
        );
    }

    /// The sweep must fire only when the head genuinely moved. Firing when
    /// equal costs a pointless read; never firing reopens the window the sweep
    /// exists to close.
    #[test]
    fn the_sweep_fires_only_when_the_head_moved_past_what_was_delivered() {
        assert!(
            super::needs_sweep(4, 3),
            "an event landed during the replay"
        );
        assert!(!super::needs_sweep(3, 3), "nothing moved");
        assert!(
            !super::needs_sweep(2, 3),
            "a head below what was delivered is not a reason to re-read"
        );
        assert!(!super::needs_sweep(0, 0), "an empty session sweeps nothing");
    }

    /// Paging is tested directly rather than through `tail`, because the sweep
    /// compensates for a paging error — every mutation of the page-boundary
    /// comparison survived a 513-event replay taken through `tail`, since the
    /// sweep silently fetched whatever the paging loop dropped. Two mechanisms
    /// covering for each other is exactly how a bug in one stays invisible.
    #[tokio::test]
    async fn paging_returns_every_event_across_a_page_boundary() {
        let log = EventLog::in_memory().expect("in-memory log");
        for _ in 0..513 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        let all = log
            .read_all_from("s1", 1)
            .await
            .expect("paging must not fail");
        let sequences: Vec<u64> = all.iter().map(|event| event.sequence).collect();
        assert_eq!(
            sequences,
            (1..=513).collect::<Vec<u64>>(),
            "a page boundary must not truncate, duplicate, or reorder"
        );
    }

    #[tokio::test]
    async fn paging_from_an_offset_returns_the_suffix_across_a_boundary() {
        let log = EventLog::in_memory().expect("in-memory log");
        for _ in 0..600 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        let tail = log.read_all_from("s1", 100).await.expect("paging");
        let sequences: Vec<u64> = tail.iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, (100..=600).collect::<Vec<u64>>());
    }

    /// All four corners of the skip condition. Inline these are not separately
    /// reachable, which is why an `&&` survived: a tail that skipped only when
    /// a notification was *both* for another session *and* already delivered
    /// still behaves correctly in the common case.
    #[test]
    fn a_notification_is_stale_when_it_is_for_another_session_or_already_delivered() {
        let notification = |session: &str, sequence: u64| super::Notification {
            session_id: session.to_owned(),
            sequence,
        };

        // Neither: this is the one notification that must wake the tail.
        assert!(!super::is_stale(&notification("s1", 5), "s1", 4));

        // Wrong session only.
        assert!(super::is_stale(&notification("other", 5), "s1", 4));

        // Already delivered only — both the equal and the below-head cases,
        // since `<=` and `<` disagree exactly at the boundary.
        assert!(super::is_stale(&notification("s1", 4), "s1", 4));
        assert!(super::is_stale(&notification("s1", 3), "s1", 4));

        // Both.
        assert!(super::is_stale(&notification("other", 3), "s1", 4));
    }

    /// The sweep's read window. A wrong offset is invisible end-to-end — the
    /// re-read and the tail's own dedup heal it — so it is asserted here
    /// instead.
    #[test]
    fn a_sweep_asks_for_the_sequence_after_what_was_delivered() {
        assert_eq!(super::sweep_from(0), 1, "an empty session starts at 1");
        assert_eq!(super::sweep_from(4), 5);
        assert_eq!(
            super::sweep_from(1),
            2,
            "asking for 1 again would re-deliver the event already handed over"
        );
        assert_eq!(
            super::sweep_from(u64::MAX),
            u64::MAX,
            "saturating, so an absurd head cannot wrap the window back to zero"
        );
    }

    /// All six rows of the merged-skip table. The unfollowed arms are the
    /// ones a live stream cannot reach separately: an unfollowed session
    /// only ever notifies while `all_sessions` streams are also receiving
    /// their own traffic, so a flipped `!` or a dropped `||` passes
    /// end-to-end.
    #[test]
    fn a_merged_tail_skips_delivered_events_and_unfollowed_sessions_it_must_not_adopt() {
        // Followed: only the watermark decides; the flags must not matter.
        assert!(!super::merged_skip(Some(4), false, false, 5));
        assert!(super::merged_skip(Some(4), false, false, 4));
        assert!(super::merged_skip(Some(4), true, true, 3));

        // Unfollowed: adopted only under all_sessions, and never an excluded
        // stream.
        assert!(!super::merged_skip(None, true, false, 1));
        assert!(super::merged_skip(None, false, false, 1));
        assert!(super::merged_skip(None, true, true, 1));
    }

    /// Bounded wait for a merged tail's next batch — same rationale as
    /// [`next_batch_within`].
    async fn merged_batch_within(tail: &mut super::MultiTail) -> Vec<super::StoredEvent> {
        tokio::time::timeout(std::time::Duration::from_millis(500), tail.next_batch())
            .await
            .expect("the merged tail must produce a batch rather than hang")
            .expect("the log outlives the tail")
            .expect("read")
    }

    /// The merged replay: cursor sessions get history from their offsets and
    /// the per-session heads say where the live tail begins.
    #[tokio::test]
    async fn a_merged_tail_replays_each_cursor_from_its_own_offset() {
        let log = EventLog::in_memory().expect("in-memory log");
        for _ in 0..3 {
            log.append(pending("s1", b"a")).await.expect("append");
        }
        log.append(pending("s2", b"b")).await.expect("append");

        let cursors = vec![("s1".to_owned(), 2), ("s2".to_owned(), 1)];
        let (history, tail) = log
            .tail_merged(&cursors, false, &[])
            .await
            .expect("merged tail");

        let seen: Vec<(String, u64)> = history
            .iter()
            .map(|event| (event.session_id.clone(), event.sequence))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("s1".to_owned(), 2),
                ("s1".to_owned(), 3),
                ("s2".to_owned(), 1),
            ],
            "each cursor replays from its own offset, per session in cursor order"
        );
        assert_eq!(
            tail.heads(),
            vec![("s1".to_owned(), 3), ("s2".to_owned(), 1)],
            "the heads are per-session resume points"
        );
    }

    /// One stream serves them all: events landing in either followed session
    /// come out of the same tail, positioned after each session's replay.
    #[tokio::test]
    async fn a_merged_tail_follows_every_cursor_session_live() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");
        log.append(pending("s2", b"b")).await.expect("append");

        let cursors = vec![("s1".to_owned(), 1), ("s2".to_owned(), 1)];
        let (history, mut tail) = log
            .tail_merged(&cursors, false, &[])
            .await
            .expect("merged tail");
        assert_eq!(history.len(), 2);

        log.append(pending("s2", b"x")).await.expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(batch.len(), 1);
        assert_eq!((batch[0].session_id.as_str(), batch[0].sequence), ("s2", 2));

        log.append(pending("s1", b"y")).await.expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!((batch[0].session_id.as_str(), batch[0].sequence), ("s1", 2));
    }

    /// The all-sessions net: an unnamed session is followed from its current
    /// head (no replay), and a session born after the subscription is adopted
    /// from its start — the overview hears about sessions it never asked for.
    #[tokio::test]
    async fn an_all_sessions_tail_adopts_existing_heads_and_newborn_sessions() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("veteran", b"old"))
            .await
            .expect("append");

        let (history, mut tail) = log.tail_merged(&[], true, &[]).await.expect("merged tail");
        assert!(
            history.is_empty(),
            "an unnamed session is followed from its head, not replayed"
        );
        assert_eq!(tail.heads(), vec![("veteran".to_owned(), 1)]);

        // The veteran moves: only the new event arrives.
        log.append(pending("veteran", b"new"))
            .await
            .expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(
            (batch[0].session_id.as_str(), batch[0].sequence),
            ("veteran", 2)
        );

        // A session created after the subscription arrives from its start.
        log.append(pending("newborn", b"first"))
            .await
            .expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(
            (batch[0].session_id.as_str(), batch[0].sequence),
            ("newborn", 1)
        );
    }

    /// The exclusion: an excluded stream is invisible to the all-sessions
    /// net, but an explicit cursor for it still works — the catalog can be
    /// synced by name, never by accident.
    #[tokio::test]
    async fn an_excluded_stream_is_not_adopted_but_can_be_named() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("catalog", b"defn"))
            .await
            .expect("append");
        log.append(pending("s1", b"a")).await.expect("append");

        let excluded = vec!["catalog".to_owned()];
        let (_, mut tail) = log
            .tail_merged(&[], true, &excluded)
            .await
            .expect("merged tail");
        assert_eq!(
            tail.heads(),
            vec![("s1".to_owned(), 1)],
            "the excluded stream must not be adopted"
        );

        log.append(pending("catalog", b"more"))
            .await
            .expect("append");
        log.append(pending("s1", b"b")).await.expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(
            (batch[0].session_id.as_str(), batch[0].sequence),
            ("s1", 2),
            "the catalog append must not wake the merge with catalog events"
        );

        // Named explicitly, the same stream replays fine.
        let (history, _) = log
            .tail_merged(&[("catalog".to_owned(), 1)], false, &excluded)
            .await
            .expect("merged tail");
        assert_eq!(history.len(), 2, "an explicit cursor beats the exclusion");
    }

    /// The lag path re-reads from the log rather than trusting the channel:
    /// force a lag by filling the notification buffer while the tail is not
    /// reading, then require every event to arrive anyway.
    #[tokio::test]
    async fn a_lagged_merged_tail_recovers_everything_from_the_log() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"seed")).await.expect("append");

        let (_, mut tail) = log
            .tail_merged(&[("s1".to_owned(), 1)], true, &[])
            .await
            .expect("merged tail");

        // NOTIFY_CAPACITY is 1024; overflow it so the receiver lags, with a
        // newborn session hidden inside the dropped window.
        log.append(pending("newborn", b"x")).await.expect("append");
        for _ in 0..1100 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        let mut seen_s1 = 1_u64;
        let mut seen_newborn = 0_u64;
        // Counted ceiling: everything is already in the log, so the batches
        // must arrive without waiting on further appends.
        for _ in 0..40 {
            if seen_s1 >= 1101 && seen_newborn >= 1 {
                break;
            }
            for event in merged_batch_within(&mut tail).await {
                match event.session_id.as_str() {
                    "s1" => seen_s1 = seen_s1.max(event.sequence),
                    "newborn" => seen_newborn = seen_newborn.max(event.sequence),
                    other => panic!("unexpected session {other}"),
                }
            }
        }
        assert_eq!(seen_s1, 1101, "the lag must not lose cursor-session events");
        assert_eq!(
            seen_newborn, 1,
            "a session born inside the dropped window must still be adopted"
        );
    }

    /// A lag caused entirely by *unfollowed* traffic finds nothing to read,
    /// and that wake must be swallowed: yielding an empty batch would have
    /// every consumer see phantom wakes — and with more skipped
    /// notifications buffered behind it, a busy loop of them. The follow-up
    /// append proves the tail is still live after swallowing the wake, so
    /// the first assertion is not a bare "it blocked".
    #[tokio::test]
    async fn a_lag_with_nothing_new_yields_no_empty_batch() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"seed")).await.expect("append");

        // Follow s1 only; the noise stays unfollowed (all_sessions false).
        let (_, mut tail) = log
            .tail_merged(&[("s1".to_owned(), 1)], false, &[])
            .await
            .expect("merged tail");
        for _ in 0..1100 {
            log.append(pending("noise", b"x")).await.expect("append");
        }

        // The lag wake reads every followed session, finds nothing, and must
        // keep waiting rather than hand back an empty vector.
        let woken =
            tokio::time::timeout(std::time::Duration::from_millis(300), tail.next_batch()).await;
        assert!(
            woken.is_err(),
            "with nothing new in any followed session the tail must keep \
             waiting, not yield: {woken:?}"
        );

        // And it is still alive: the next real event arrives, exactly once.
        log.append(pending("s1", b"real")).await.expect("append");
        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(rows(&batch), vec![("s1".to_owned(), 2)]);
    }

    /// The `(session, sequence)` rows of one batch — what the lag tests
    /// compare against.
    fn rows(batch: &[super::StoredEvent]) -> Vec<(String, u64)> {
        batch
            .iter()
            .map(|event| (event.session_id.clone(), event.sequence))
            .collect()
    }

    /// Lag recovery must neither adopt the excluded stream nor rewind a
    /// followed session to zero — the two halves of the rediscovery
    /// condition, which only a lag can reach.
    #[tokio::test]
    async fn lag_recovery_neither_adopts_the_excluded_nor_rewinds_the_followed() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");

        let excluded = vec!["catalog".to_owned()];
        let (history, mut tail) = log
            .tail_merged(&[("s1".to_owned(), 1)], true, &excluded)
            .await
            .expect("merged tail");
        assert_eq!(history.len(), 1, "event 1 was already delivered as history");

        // Overflow the buffer with excluded-stream appends, then land one
        // real event.
        for _ in 0..1100 {
            log.append(pending("catalog", b"defn"))
                .await
                .expect("append");
        }
        log.append(pending("s1", b"b")).await.expect("append");

        let batch = merged_batch_within(&mut tail).await;
        assert_eq!(
            rows(&batch),
            vec![("s1".to_owned(), 2)],
            "recovery must deliver only the new followed event — no catalog \
             adoption, and no re-delivery of event 1"
        );
    }

    /// Waits for the next batch, bounded.
    ///
    /// An unbounded `next_batch().await` turns a defect into a hang: a mutation
    /// that makes reads come back empty leaves the tail waiting for an event
    /// the log will never hand it, and cargo-mutants records a timeout rather
    /// than a kill. A timeout is a mutant that was not evaluated, so every wait
    /// here has a ceiling.
    async fn next_batch_within(tail: &mut super::Tail) -> Vec<super::StoredEvent> {
        tokio::time::timeout(std::time::Duration::from_millis(500), tail.next_batch())
            .await
            .expect("the tail must produce a batch rather than hang")
            .expect("the log outlives the tail")
            .expect("read")
    }

    #[tokio::test]
    async fn appends_are_visible_to_a_later_read() {
        let log = log_with_two_events().await;

        let all = log.read_all("s1").await.expect("read");
        assert_eq!(all.len(), 2);
        assert_eq!(log.head("s1").await.expect("head"), 2);
    }

    /// The property the whole module exists for, in its simplest form.
    #[tokio::test]
    async fn a_tail_started_mid_stream_sees_everything_appended_afterwards() {
        let log = log_with_two_events().await;

        let (history, mut tail) = log.tail("s1", 2).await.expect("tail");
        assert_eq!(history.len(), 1, "replay starts at the requested offset");
        assert_eq!(tail.delivered(), 2);

        log.append(pending("s1", b"c")).await.expect("append");
        let batch = next_batch_within(&mut tail).await;
        let sequences: Vec<u64> = batch.iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, vec![3]);
    }

    #[tokio::test]
    async fn a_tail_ignores_other_sessions() {
        let log = EventLog::in_memory().expect("in-memory log");
        let (_, mut tail) = log.tail("s1", 1).await.expect("tail");

        log.append(pending("other", b"x")).await.expect("append");
        log.append(pending("s1", b"mine")).await.expect("append");

        let batch = next_batch_within(&mut tail).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].session_id, "s1");
    }

    #[tokio::test]
    async fn a_tail_of_an_empty_session_starts_from_the_first_event() {
        let log = EventLog::in_memory().expect("in-memory log");
        let (history, mut tail) = log.tail("s1", 1).await.expect("tail");
        assert!(history.is_empty());
        assert_eq!(tail.delivered(), 0);

        log.append(pending("s1", b"a")).await.expect("append");
        let batch = next_batch_within(&mut tail).await;
        assert_eq!(batch[0].sequence, 1, "no event is skipped");
    }

    /// Appending between the subscribe and the replay is the window a naive
    /// implementation leaves open.
    #[tokio::test]
    async fn no_event_is_lost_when_appends_race_the_replay() {
        let log = EventLog::in_memory().expect("in-memory log");
        for _ in 0..5 {
            log.append(pending("s1", b"x")).await.expect("append");
        }

        let writer = log.clone();
        let writing = tokio::spawn(async move {
            for _ in 0..20 {
                writer.append(pending("s1", b"y")).await.expect("append");
            }
        });

        let (history, mut tail) = log.tail("s1", 1).await.expect("tail");
        writing.await.expect("the writer task must finish");

        // Bounded: a defect that stalls the tail must red this test rather than
        // hang it. An unbounded wait converts a clear failure into a suite that
        // never reports, which is strictly worse.
        let mut seen: Vec<u64> = history.iter().map(|event| event.sequence).collect();
        // The ceiling is a count, not the clock below: a defect that makes
        // `next_batch` resolve instantly with an empty batch never yields, so
        // the timeout wrapped around it is never polled and cannot fire
        // (QUALITY_GATES, "a clock cannot bound a loop that fails to make
        // progress"). Every healthy batch carries at least one new event, so
        // 25 events can never need more than 25 batches.
        let mut rounds = 0_u32;
        while seen.last().copied().unwrap_or(0) < 25 {
            rounds += 1;
            assert!(
                rounds <= 25,
                "{rounds} batches without reaching sequence 25: an empty or \
                 repeated batch is a defect, not something to wait out"
            );
            let batch =
                tokio::time::timeout(std::time::Duration::from_millis(500), tail.next_batch())
                    .await
                    .expect("the tail must not stall below the known head")
                    .expect("live")
                    .expect("read");
            seen.extend(batch.iter().map(|event| event.sequence));
        }

        let expected: Vec<u64> = (1..=25).collect();
        assert_eq!(seen, expected, "every sequence exactly once, in order");
    }

    #[tokio::test]
    async fn a_deduplicated_append_does_not_advance_the_log() {
        let log = EventLog::in_memory().expect("in-memory log");
        let event = PendingEvent {
            session_id: "s1".to_owned(),
            idempotency_key: "k".to_owned(),
            payload: b"a".to_vec(),
            ..PendingEvent::default()
        };

        let first = log.append(event.clone()).await.expect("append");
        let second = log.append(event).await.expect("re-append");

        assert_eq!(first.sequence, second.sequence);
        assert!(second.deduplicated);
        assert_eq!(log.head("s1").await.expect("head"), 1);
    }

    #[tokio::test]
    async fn sessions_are_discovered_from_the_log_itself() {
        let log = EventLog::in_memory().expect("in-memory log");
        assert!(log.session_ids().await.expect("list").is_empty());

        log.append(pending("s1", b"a")).await.expect("append");
        log.append(pending("s2", b"a")).await.expect("append");

        assert_eq!(
            log.session_ids().await.expect("list"),
            vec!["s1".to_owned(), "s2".to_owned()]
        );
    }
}
