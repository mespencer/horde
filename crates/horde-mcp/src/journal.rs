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
//! The sequence comes back because the permission flow needs it: an Agent
//! blocked on a decision resumes by reading the log forward from a known point,
//! and "the point my request was written at" is the only offset that cannot
//! miss a decision answered before the reader started looking.

use horde_proto::v1::event::Kind as EventKind;

/// One event to append, as the endpoint knows it.
///
/// The journal owns sequencing and framing; this is only the part the endpoint
/// can speak to.
#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    /// The Session whose log this belongs to.
    pub session_id: String,
    /// The Agent the event is about — the authenticated caller, never a value
    /// the Agent supplied for itself.
    pub agent_id: String,
    /// Deduplication key. The journal is at-least-once, so a retried append
    /// must not become a second permission request.
    pub idempotency_key: String,
    pub kind: EventKind,
}

/// Why an append did not land.
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
    /// Appends one event, answering with the journal sequence it landed at.
    ///
    /// # Errors
    ///
    /// [`JournalError`] when the entry did not land. A caller must never treat
    /// a failed append as a written event: an unwritten permission request is
    /// one no human will ever see, so it has to surface to the Agent as an
    /// error rather than as a wait.
    async fn append(&self, entry: JournalEntry) -> Result<u64, JournalError>;
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

        /// Makes every subsequent append fail.
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
        async fn append(&self, entry: JournalEntry) -> Result<u64, JournalError> {
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
            agent_id: "agent-42".to_owned(),
            idempotency_key: key.to_owned(),
            kind: EventKind::PermissionRequested(horde_proto::v1::PermissionRequested::default()),
        }
    }

    #[tokio::test]
    async fn appends_answer_with_ascending_sequences() {
        // The sequence is the offset a blocked caller resumes reading from, so
        // it has to be both real and ordered.
        let journal = RecordingJournal::new();

        assert_eq!(journal.append(entry("a")).await, Ok(1));
        assert_eq!(journal.append(entry("b")).await, Ok(2));
        assert_eq!(journal.entries().len(), 2);
    }

    #[tokio::test]
    async fn a_failed_append_is_an_error_rather_than_a_silent_drop() {
        // An unwritten permission request is one no human will ever answer.
        // The caller has to hear about it rather than settle in to wait.
        let journal = RecordingJournal::new();
        journal.fail_with(JournalError::Unavailable("worker offline".to_owned()));

        assert_eq!(
            journal.append(entry("a")).await,
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
