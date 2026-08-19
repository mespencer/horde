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

    #[error("no MCP server named {name}")]
    NoSuchMcpServer { name: String },

    /// A worker-forwarded hub call named a tool nothing routes for this
    /// caller. One refusal for unknown, deleted, `local`-placed, and
    /// invisible alike: invisible hub tools are unlisted *and* unroutable,
    /// and a distinct message would reveal what the allowlist hides.
    #[error("no hub tool named {name}")]
    NoSuchHubTool { name: String },

    /// A worker forwarded a hub call for an agent the log places elsewhere.
    /// The worker's channel authenticates the *worker*; this is what stops an
    /// authenticated worker from speaking as somebody else's agent.
    #[error("agent {agent_id} is not placed on worker {worker_id}")]
    AgentNotOnWorker { agent_id: String, worker_id: String },

    /// A create would shadow a live MCP server's name.
    #[error("an MCP server named {name} already exists; supply its etag to update it")]
    McpServerNameTaken { name: String },

    /// The MCP catalog's optimistic-concurrency token did not match.
    #[error(
        "MCP server {name} is at etag {current}, but the request supplied \
         {supplied:?}; re-read and retry"
    )]
    StaleMcpServerEtag {
        name: String,
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

    /// An event tried to carry attachment content larger than the inline
    /// threshold in its own body. Events carry blob references and hashes,
    /// never bulk (ADR-0001): bulk belongs in the Blob Store, and letting it
    /// into the log would tax every replay of the session forever.
    #[error(
        "events carry blob references, never bulk: attachment {name:?} carries \
         {inline_bytes} inline bytes, over the {threshold}-byte inline threshold — \
         stash the content in the blob store and send its digest instead"
    )]
    InlineBulk {
        name: String,
        inline_bytes: u64,
        threshold: u64,
    },

    /// Reached the wire but could not be decoded.
    #[error("could not decode a stored event: {0}")]
    Decode(#[from] prost::DecodeError),

    /// An enrollment code that does not redeem: wrong, expired, or spent.
    ///
    /// Unauthenticated on the wire — the caller has no identity yet and the
    /// credential it presented was not accepted; the remedy is a fresh code.
    #[error("enrollment refused: {0}")]
    EnrollmentRefused(&'static str),

    /// Something on the server's own side of the trust machinery broke —
    /// the CA refusing to sign, the randomness source failing.
    #[error("internal failure: {0}")]
    Internal(String),

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
            | ServerError::NoSuchWorkspace { .. }
            | ServerError::NoSuchMcpServer { .. }
            | ServerError::NoSuchHubTool { .. } => Code::NotFound,
            // The caller is authenticated (it passed the mTLS door); what it
            // asked for is not its to ask. Not Unauthenticated — the channel
            // is fine — and not NotFound, which would invite retries.
            ServerError::AgentNotOnWorker { .. } => Code::PermissionDenied,
            ServerError::WorkspaceNameTaken { .. } | ServerError::McpServerNameTaken { .. } => {
                Code::AlreadyExists
            }
            ServerError::StaleEtag { .. } | ServerError::StaleMcpServerEtag { .. } => Code::Aborted,
            ServerError::Invalid(_) => Code::InvalidArgument,
            ServerError::NoWorkerAvailable { .. } => Code::FailedPrecondition,
            ServerError::JournalRegressed { .. } => Code::FailedPrecondition,
            // The caller must change the key, not retry: sending the same
            // bytes again produces the same collision.
            ServerError::IdempotencyKeyCollision { .. } => Code::InvalidArgument,
            // The caller must re-shape the event (stash, then reference);
            // retrying the same bulk produces the same refusal.
            ServerError::InlineBulk { .. } => Code::InvalidArgument,
            ServerError::EnrollmentRefused(_) => Code::Unauthenticated,
            ServerError::Unimplemented(_) => Code::Unimplemented,
            ServerError::Storage { .. }
            | ServerError::StorageTask(_)
            | ServerError::Decode(_)
            | ServerError::Internal(_) => Code::Internal,
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

    /// A refused code is a credential problem, not a server fault: the joiner
    /// reacts by getting a fresh code, so the code must say Unauthenticated.
    #[test]
    fn a_refused_enrollment_is_unauthenticated_and_says_why() {
        let status = tonic::Status::from(ServerError::EnrollmentRefused("the code has expired"));
        assert_eq!(status.code(), Code::Unauthenticated);
        assert!(status.message().contains("expired"));
    }

    #[test]
    fn a_broken_trust_machinery_is_internal() {
        let status = tonic::Status::from(ServerError::Internal("the CA refused".to_owned()));
        assert_eq!(status.code(), Code::Internal);
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

    /// The three workspace refusals map to three different retry strategies,
    /// so each needs its own code: re-ask (NotFound), re-name
    /// (AlreadyExists), re-read-and-retry (Aborted).
    #[test]
    fn workspace_refusals_are_distinguishable_by_code() {
        let missing = tonic::Status::from(ServerError::NoSuchWorkspace {
            wanted: "ws-9".to_owned(),
        });
        assert_eq!(missing.code(), Code::NotFound);
        assert!(missing.message().contains("ws-9"));

        let taken = tonic::Status::from(ServerError::WorkspaceNameTaken {
            name: "atlas".to_owned(),
        });
        assert_eq!(taken.code(), Code::AlreadyExists);
        assert!(taken.message().contains("atlas"));

        let stale = tonic::Status::from(ServerError::StaleEtag {
            workspace_definition_id: "ws-1".to_owned(),
            current: "7".to_owned(),
            supplied: "3".to_owned(),
        });
        assert_eq!(stale.code(), Code::Aborted, "the proto promises ABORTED");
        assert!(
            stale.message().contains('7') && stale.message().contains('3'),
            "both tokens are needed to see what raced: {}",
            stale.message()
        );
    }

    /// The refusal must tell the caller how to fix the event, not merely
    /// that it was refused — the remedy is a different shape, not a retry.
    #[test]
    fn smuggled_bulk_is_invalid_argument_and_names_the_remedy() {
        let status = tonic::Status::from(ServerError::InlineBulk {
            name: "trace.bin".to_owned(),
            inline_bytes: 90_000,
            threshold: 65_536,
        });
        assert_eq!(status.code(), Code::InvalidArgument);
        let message = status.message();
        for needed in ["trace.bin", "90000", "65536", "blob store"] {
            assert!(message.contains(needed), "missing {needed:?}: {message}");
        }
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
