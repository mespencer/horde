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
//! # Why a tail cannot miss an event
//!
//! The ordering in [`EventLog::tail`] is the whole trick, and it is the
//! property the proptests exist to pin:
//!
//! 1. **Subscribe first.** From this moment every append is announced to us.
//! 2. **Then replay** from the requested offset up to the current head.
//! 3. **Then drain** the notifications, discarding any whose sequence the
//!    replay already covered.
//!
//! An event appended during step 2 is either included in the replay or waiting
//! in the channel from step 1 — never neither. Doing it the other way round
//! (read, then subscribe) leaves exactly that window open, and the bug it
//! creates is invisible until something is actually writing during a reconnect.

pub mod schema;
pub mod store;

use std::sync::{Arc, Mutex};

use rusqlite::Connection;
use tokio::sync::broadcast;
use tracing::instrument;

pub use store::{Appended, PendingEvent, StoredEvent};

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

/// The append-only log, shared by every service.
#[derive(Clone)]
pub struct EventLog {
    connection: Arc<Mutex<Connection>>,
    notifier: broadcast::Sender<Notification>,
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
    pub fn open(path: &str) -> Result<Self, ServerError> {
        Ok(Self::from_connection(store::open(path)?))
    }

    /// Opens a private in-memory log.
    pub fn in_memory() -> Result<Self, ServerError> {
        Ok(Self::from_connection(store::open_in_memory()?))
    }

    fn from_connection(connection: Connection) -> Self {
        let (notifier, _) = broadcast::channel(NOTIFY_CAPACITY);
        Self {
            connection: Arc::new(Mutex::new(connection)),
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
            let mut guard = connection.lock().map_err(|_| ServerError::Storage {
                context: "acquiring the connection after an earlier panic",
                source: rusqlite::Error::InvalidQuery,
            })?;
            operation(&mut guard)
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

        // A send error means nobody is listening, which is normal.
        let _ = self.notifier.send(Notification {
            session_id,
            sequence: appended.sequence,
        });
        Ok(appended)
    }

    /// Reads up to `limit` events from `from_sequence` inclusive.
    pub async fn read_from(
        &self,
        session_id: &str,
        from_sequence: u64,
        limit: u32,
    ) -> Result<Vec<StoredEvent>, ServerError> {
        let session_id = session_id.to_owned();
        self.with_connection(move |connection| {
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
        self.with_connection(move |connection| store::head(connection, &session_id))
            .await
    }

    /// Every session that has at least one event, oldest first.
    pub async fn session_ids(&self) -> Result<Vec<String>, ServerError> {
        self.with_connection(|connection| store::session_ids(connection))
            .await
    }

    /// The highest journal sequence durably stored for a worker.
    pub async fn worker_journal_watermark(&self, worker_id: &str) -> Result<u64, ServerError> {
        let worker_id = worker_id.to_owned();
        self.with_connection(move |connection| {
            store::worker_journal_watermark(connection, &worker_id)
        })
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
        let history = self.read_all_from(session_id, from_sequence).await?;
        let delivered = history
            .last()
            .map_or(from_sequence.max(1).saturating_sub(1), |event| {
                event.sequence
            });

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

    /// Reads every event from an offset, paging so one enormous session cannot
    /// be pulled into memory by a single unbounded query.
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
            let short = page.len() < PAGE as usize;
            all.extend(page);
            if short {
                break;
            }
        }
        Ok(all)
    }
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
                    if notification.session_id != self.session_id
                        || notification.sequence <= self.delivered
                    {
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

#[cfg(test)]
mod tests {
    use super::{EventLog, PendingEvent};

    fn pending(session: &str, payload: &[u8]) -> PendingEvent {
        PendingEvent {
            session_id: session.to_owned(),
            payload: payload.to_vec(),
            ..PendingEvent::default()
        }
    }

    #[tokio::test]
    async fn appends_are_visible_to_a_later_read() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");
        log.append(pending("s1", b"b")).await.expect("append");

        let all = log.read_all("s1").await.expect("read");
        assert_eq!(all.len(), 2);
        assert_eq!(log.head("s1").await.expect("head"), 2);
    }

    /// The property the whole module exists for, in its simplest form.
    #[tokio::test]
    async fn a_tail_started_mid_stream_sees_everything_appended_afterwards() {
        let log = EventLog::in_memory().expect("in-memory log");
        log.append(pending("s1", b"a")).await.expect("append");
        log.append(pending("s1", b"b")).await.expect("append");

        let (history, mut tail) = log.tail("s1", 2).await.expect("tail");
        assert_eq!(history.len(), 1, "replay starts at the requested offset");
        assert_eq!(tail.delivered(), 2);

        log.append(pending("s1", b"c")).await.expect("append");
        let batch = tail
            .next_batch()
            .await
            .expect("the tail is live")
            .expect("read");
        let sequences: Vec<u64> = batch.iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, vec![3]);
    }

    #[tokio::test]
    async fn a_tail_ignores_other_sessions() {
        let log = EventLog::in_memory().expect("in-memory log");
        let (_, mut tail) = log.tail("s1", 1).await.expect("tail");

        log.append(pending("other", b"x")).await.expect("append");
        log.append(pending("s1", b"mine")).await.expect("append");

        let batch = tail.next_batch().await.expect("live").expect("read");
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
        let batch = tail.next_batch().await.expect("live").expect("read");
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

        let mut seen: Vec<u64> = history.iter().map(|event| event.sequence).collect();
        while seen.last().copied().unwrap_or(0) < 25 {
            let batch = tail.next_batch().await.expect("live").expect("read");
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
