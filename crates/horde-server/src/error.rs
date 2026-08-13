//! Server failures, and how they become gRPC status codes.

use tonic::{Code, Status};

/// What can go wrong in the control plane.
///
/// Variants are distinguished by the answer they imply for the caller, which is
/// also what decides the gRPC code: a client that cannot tell "you asked for
/// something that does not exist" from "the server is broken" cannot retry
/// correctly.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("storage failed while {context}: {source}")]
    Storage {
        context: &'static str,
        #[source]
        source: rusqlite::Error,
    },

    /// The blocking pool could not run, or panicked.
    #[error("a storage task failed to complete: {0}")]
    StorageTask(#[from] tokio::task::JoinError),

    #[error("no session {session_id}")]
    NoSuchSession { session_id: String },

    #[error("no agent {agent_id}")]
    NoSuchAgent { agent_id: String },

    #[error("no workspace definition {wanted}")]
    NoSuchWorkspace { wanted: String },

    /// A create would shadow a live definition's name.
    #[error("a workspace definition named {name} already exists")]
    WorkspaceNameTaken { name: String },

    /// The optimistic-concurrency token did not match: someone else changed
    /// the definition since the caller read it. ABORTED per the proto, so the
    /// caller re-reads and retries rather than blindly re-sending.
    #[error(
        "workspace definition {workspace_definition_id} is at etag {current}, \
         but the request supplied {supplied:?}; re-read and retry"
    )]
    StaleEtag {
        workspace_definition_id: String,
        current: String,
        supplied: String,
    },

    #[error("no worker is registered, so {what} cannot be placed")]
    NoWorkerAvailable { what: String },

    /// The request was understood but is not valid.
    #[error("invalid request: {0}")]
    Invalid(String),

    /// A worker reported a journal position below what the server already
    /// stored, which means it lost its journal. Refusing is deliberate:
    /// silently renumbering would forge history.
    #[error(
        "worker {worker_id} reports journal head {reported} but the server has already stored \
         {stored}; refusing to renumber"
    )]
    JournalRegressed {
        worker_id: String,
        reported: u64,
        stored: u64,
    },

    /// Two different kinds of event were written under one idempotency key.
    ///
    /// Deduplication exists for at-least-once delivery, where a retry replays
    /// the *same* event — so a hit whose kind differs from the stored one is
    /// never a retry. It is a key collision, and the caller's event is about to
    /// be discarded.
    ///
    /// Refused loudly because the alternative is the worst failure shape there
    /// is: the write is acked, the log stays silent, and the symptom surfaces
    /// somewhere else entirely as a reader that blocks forever, with nothing
    /// pointing back at the write that vanished.
    #[error(
        "idempotency key {key} in session {session_id} already holds a {stored} event at \
         sequence {sequence}; refusing to silently discard this {incoming} one"
    )]
    IdempotencyKeyCollision {
        session_id: String,
        key: String,
        sequence: u64,
        stored: &'static str,
        incoming: &'static str,
    },

    /// Reached the wire but could not be decoded.
    #[error("could not decode a stored event: {0}")]
    Decode(#[from] prost::DecodeError),

    /// Deliberately not implemented yet.
    ///
    /// Present so an unbuilt RPC answers honestly instead of returning a
    /// fabricated success that a caller would build on.
    #[error("{0} is not implemented yet")]
    Unimplemented(&'static str),
}

impl ServerError {
    /// Curries a storage context, so call sites read as one expression.
    pub(crate) fn storage(context: &'static str) -> impl FnOnce(rusqlite::Error) -> Self {
        move |source| Self::Storage { context, source }
    }
}

impl From<ServerError> for Status {
    fn from(error: ServerError) -> Self {
        let message = error.to_string();
        let code = match &error {
            ServerError::NoSuchSession { .. }
            | ServerError::NoSuchAgent { .. }
            | ServerError::NoSuchWorkspace { .. } => Code::NotFound,
            ServerError::WorkspaceNameTaken { .. } => Code::AlreadyExists,
            ServerError::StaleEtag { .. } => Code::Aborted,
            ServerError::Invalid(_) => Code::InvalidArgument,
            ServerError::NoWorkerAvailable { .. } => Code::FailedPrecondition,
            ServerError::JournalRegressed { .. } => Code::FailedPrecondition,
            // The caller must change the key, not retry: sending the same
            // bytes again produces the same collision.
            ServerError::IdempotencyKeyCollision { .. } => Code::InvalidArgument,
            ServerError::Unimplemented(_) => Code::Unimplemented,
            ServerError::Storage { .. } | ServerError::StorageTask(_) | ServerError::Decode(_) => {
                Code::Internal
            }
        };
        Status::new(code, message)
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::ServerError;

    #[test]
    fn a_missing_session_is_not_found_rather_than_internal() {
        let status = tonic::Status::from(ServerError::NoSuchSession {
            session_id: "s1".to_owned(),
        });
        assert_eq!(status.code(), Code::NotFound);
        assert!(status.message().contains("s1"));
    }

    /// An unbuilt RPC must be distinguishable from a broken one, or callers
    /// will retry something that will never work.
    #[test]
    fn an_unbuilt_rpc_reports_unimplemented() {
        let status = tonic::Status::from(ServerError::Unimplemented("workspace CRUD"));
        assert_eq!(status.code(), Code::Unimplemented);
        assert!(status.message().contains("workspace CRUD"));
    }

    #[test]
    fn a_regressed_journal_is_a_failed_precondition_naming_both_positions() {
        let status = tonic::Status::from(ServerError::JournalRegressed {
            worker_id: "w1".to_owned(),
            reported: 3,
            stored: 9,
        });
        assert_eq!(status.code(), Code::FailedPrecondition);
        let message = status.message();
        assert!(message.contains('3') && message.contains('9'));
    }

    #[test]
    fn storage_failures_are_internal() {
        let status = tonic::Status::from(ServerError::Storage {
            context: "testing",
            source: rusqlite::Error::QueryReturnedNoRows,
        });
        assert_eq!(status.code(), Code::Internal);
    }
}
