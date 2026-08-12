//! The seam onto the Worker's journal.
//!
//! # Why this is a trait and not a client
//!
//! Permission requests and results are **observed facts of execution**, and the
//! only write path for those into the event log is the Worker's `PushEvents`
//! journal — `ingest_batch` on the Server appends whatever kind it is handed,
//! opaquely, which makes the journal the universal writer. There is
//! deliberately no ControlPlane RPC that authors these events: DESIGN §5
//! prefers the orchestrator observing what happened over Agents declaring it,
//! and an RPC an Agent could call directly would be exactly the declaring path.
//!
//! So this crate does not own the write. It states what it needs from whoever
//! does, which is a single append that returns the journal sequence the entry
//! landed at. The Worker implements it; the tests implement it in memory; the
//! endpoint cannot tell the difference and must not try.
//!
//! # The returned sequence is not a resume offset
//!
//! It is tempting to treat the number `append` hands back as the position a
//! blocked Agent resumes reading the log from. It is not, and the mistake is
//! invisible rather than loud. `event.proto` opens by naming the two spaces:
//!
//! * `EventEnvelope.sequence` — per **Session**, assigned by the Server. This
//!   is what `SyncEvents` cursors mean.
//! * `JournaledEvent.journal_sequence` — per **Worker**, assigned by the
//!   Worker's local journal. This is what an append can answer with.
//!
//! Journal 7 and session 7 are unrelated events, and on a Worker serving more
//! than one Session they diverge immediately — so feeding one into the other
//! yields a plausible offset pointing somewhere arbitrary, with no error. The
//! Server does not even assign the session sequence until `ingest_batch`, well
//! after this call returns, so the right number cannot be known here in
//! principle.
//!
//! So nothing in this crate resumes from it. Waiters match on **identity**
//! instead — a `PermissionDecided` carrying the `request_id` that was raised —
//! and subscribe *before* appending, which is what closes the window rather
//! than an offset. The sequence stays in the signature as the Worker's own
//! bookkeeping, useful to an implementer and load-bearing for nobody here.
//! That is the seam working correctly: this crate's correctness does not
//! depend on the Worker's storage internals.

use horde_proto::v1::event::Kind as EventKind;

/// One event to record, as the endpoint knows it.
///
/// The journal owns sequencing, framing, and `occurred_at`; this is only the
/// part the endpoint can speak to. Mirrors the Worker's own `JournalEntry`
/// field for field, so replacing this module with the real type at merge is a
/// rename rather than a redesign.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    /// The Session whose log this belongs to.
    pub session_id: String,
    /// The Agent the event is about — the authenticated caller, never a value
    /// the Agent supplied for itself.
    ///
    /// `None` for session-scoped events that belong to no single Agent.
    /// Everything this crate writes is Agent-scoped, so in practice it is
    /// always `Some`; the shape matches the Worker's so the swap stays
    /// mechanical.
    pub agent_id: Option<String>,
    /// Deduplication key, minted by the **caller** and stable across replays.
    ///
    /// The journal is at-least-once and the Server dedupes on
    /// `(session_id, idempotency_key)`, so this is the field that stops a
    /// retried raise becoming a second permission request. Stable is the
    /// operative word: a fresh random key on every attempt would dedupe
    /// nothing. This crate mints it from the permission request's own id,
    /// which is already unique and already survives a replay.
    pub idempotency_key: String,
    pub kind: EventKind,
}

/// Why a record did not land.
///
/// The Worker's implementation fails with its own `WorkerError`. This crate
/// keeps its own error until the two branches merge, at which point the
/// adapter between them is the only thing that has to change — deliberately
/// not a re-export, because a crate that names another crate's error type
/// before it can compile against it is a merge conflict waiting to be written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    /// The journal is not reachable — the Worker is disconnected, or shutting
    /// down. Retryable in principle, and the caller says so to the Agent.
    #[error("the journal is unavailable: {0}")]
    Unavailable(String),

    /// The journal refused the entry itself. Not retryable: sending the same
    /// bytes again produces the same refusal.
    #[error("the journal rejected the entry: {0}")]
    Rejected(String),
}

/// Appends observed facts to the Worker's journal.
///
/// Implemented by the Worker in production. `async_trait` rather than a native
/// async fn because the endpoint holds this behind a `dyn` — one endpoint is
/// configured with one sink, chosen at construction, and neither the tool
/// handlers nor their tests should be generic over which.
#[async_trait::async_trait]
pub trait JournalSink: Send + Sync + core::fmt::Debug {
    /// Records one event, answering with the Worker-local journal sequence it
    /// landed at.
    ///
    /// Named to match the Worker's `JournalSink::record` rather than the
    /// `append` this started as, so the swap at merge is a type substitution
    /// and not a call-site edit.
    ///
    /// That number is the Worker's own bookkeeping and is **not** a
    /// `SyncEvents` offset — see the module docs. Callers here ignore it.
    ///
    /// # Errors
    ///
    /// [`JournalError`] when the entry did not land. A caller must never treat
    /// a failed record as a written event: an unwritten permission request is
    /// one no human will ever see, so it has to surface to the Agent as an
    /// error rather than as a wait.
    async fn record(&self, entry: JournalEntry) -> Result<u64, JournalError>;
}

#[cfg(test)]
pub(crate) mod testing {
    //! An in-memory journal, for tests that need the endpoint's half of the
    //! seam without a Worker behind it.

    use super::{JournalEntry, JournalError, JournalSink};
    use std::sync::{Arc, Mutex};

    /// Records what was appended and hands back ascending sequences.
    #[derive(Debug, Default, Clone)]
    pub(crate) struct RecordingJournal {
        state: Arc<Mutex<State>>,
    }

    #[derive(Debug, Default)]
    struct State {
        entries: Vec<JournalEntry>,
        /// Set to fail every later append, so a caller's behaviour on an
        /// unwritable journal is testable.
        failure: Option<JournalError>,
    }

    impl RecordingJournal {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// Everything appended so far, in order.
        pub(crate) fn entries(&self) -> Vec<JournalEntry> {
            self.lock().entries.clone()
        }

        /// Makes every subsequent record fail.
        pub(crate) fn fail_with(&self, error: JournalError) {
            self.lock().failure = Some(error);
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            // A poisoned lock means a test thread panicked while holding it;
            // the recorded entries are still readable and the panic that
            // matters has already been reported.
            self.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    #[async_trait::async_trait]
    impl JournalSink for RecordingJournal {
        async fn record(&self, entry: JournalEntry) -> Result<u64, JournalError> {
            let mut state = self.lock();
            if let Some(failure) = &state.failure {
                return Err(failure.clone());
            }
            state.entries.push(entry);
            // One-based: sequence zero is "nothing written yet" everywhere else
            // in the log, and handing it out as a real offset would make a
            // written event indistinguishable from an unwritten one.
            Ok(state.entries.len() as u64)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::RecordingJournal;
    use super::{JournalEntry, JournalError, JournalSink};
    use horde_proto::v1::event::Kind as EventKind;

    fn entry(key: &str) -> JournalEntry {
        JournalEntry {
            session_id: "s1".to_owned(),
            agent_id: Some("agent-42".to_owned()),
            idempotency_key: key.to_owned(),
            kind: EventKind::PermissionRequested(horde_proto::v1::PermissionRequested::default()),
        }
    }

    #[tokio::test]
    async fn records_answer_with_ascending_sequences() {
        // Ordering is the Worker's own bookkeeping rather than anything this
        // crate resumes from, but a sink that renumbered or repeated would be
        // lying to whoever does read it.
        let journal = RecordingJournal::new();

        assert_eq!(journal.record(entry("a")).await, Ok(1));
        assert_eq!(journal.record(entry("b")).await, Ok(2));
        assert_eq!(journal.entries().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_record_is_an_error_rather_than_a_silent_drop() {
        // An unwritten permission request is one no human will ever answer.
        // The caller has to hear about it rather than settle in to wait.
        let journal = RecordingJournal::new();
        journal.fail_with(JournalError::Unavailable("worker offline".to_owned()));

        assert_eq!(
            journal.record(entry("a")).await,
            Err(JournalError::Unavailable("worker offline".to_owned()))
        );
        assert!(
            journal.entries().is_empty(),
            "a rejected entry must not appear in the log"
        );
    }

    #[test]
    fn each_journal_failure_explains_itself_distinctly() {
        assert_eq!(
            JournalError::Unavailable("worker offline".to_owned()).to_string(),
            "the journal is unavailable: worker offline"
        );
        assert_eq!(
            JournalError::Rejected("no such session".to_owned()).to_string(),
            "the journal rejected the entry: no such session"
        );
    }
}
