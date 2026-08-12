//! What can go wrong between a Worker and its Server.
//!
//! Separate from [`SupervisorError`](crate::process::SupervisorError), which is
//! about a child process. This is about the dial: the journal on disk, the two
//! streams, and the commands that arrive on one of them.

use horde_adapter::AdapterError;

/// A failure in the Worker's connection to the Server, or in its own journal.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// The journal's database refused an operation.
    ///
    /// `context` says which one, in words an operator can act on: a failure
    /// "appending to the journal" and one "recording an applied command" have
    /// very different consequences, and a bare rusqlite error says neither.
    #[error("the journal failed while {context}: {source}")]
    Journal {
        context: &'static str,
        source: rusqlite::Error,
    },

    /// A previous journal operation panicked while holding the connection.
    ///
    /// Reported rather than propagated, for the same reason the server's log
    /// does it: one bad statement must not take the Worker's process down while
    /// it is supervising live agents.
    #[error("the journal is unusable after an earlier panic while {0}")]
    JournalPoisoned(&'static str),

    /// The blocking pool dropped a journal operation mid-flight.
    #[error("a journal operation did not finish: {0}")]
    JournalTask(#[from] tokio::task::JoinError),

    /// The journal holds an event that is not a decodable `horde.v1.Event`.
    ///
    /// The Worker encoded every one of these itself, so this is corruption
    /// rather than a peer sending nonsense — which is why it is fatal to the
    /// push rather than a row to skip. Skipping would put a hole in a sequence
    /// the Server requires to be contiguous.
    #[error("journal entry {journal_sequence} is not a decodable event: {source}")]
    CorruptJournalEntry {
        journal_sequence: u64,
        source: horde_proto::prost::DecodeError,
    },

    /// The Server asked the Worker to replay from a position it has truncated.
    ///
    /// The mirror image of the Server's own `JournalRegressed`. Sending only the
    /// events the Worker still holds would leave a gap in a sequence the Server
    /// requires to be contiguous, and inventing the missing ones is not
    /// available, so this is refused rather than papered over.
    #[error(
        "the server asked to resume at journal sequence {requested}, but everything below \
         {floor} has been truncated"
    )]
    TruncatedPast { requested: u64, floor: u64 },

    /// The connection could not be established.
    #[error("could not reach the server: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// The Server refused an RPC, or a stream failed mid-flight.
    #[error("the server refused: {0}")]
    Rpc(#[from] tonic::Status),

    /// A response arrived without a field the protocol requires.
    ///
    /// Protobuf makes every message field optional on the wire, so "the Server
    /// answered" and "the Server answered usefully" are different facts. Naming
    /// the missing field is what makes a version-skew bug diagnosable.
    #[error("the server's reply carried no {0}")]
    Incomplete(&'static str),

    /// A command arrived that this Worker has no way to apply.
    ///
    /// Fatal to the command loop on purpose, and the reasoning matches the
    /// Server's own `should_stop`: acking past a command that was never applied
    /// tells the Server work happened that did not. Version skew is supposed to
    /// prevent this — commands gate on the capability flags sent at
    /// registration (DESIGN.md §2) — so reaching this means the handshake
    /// promised something the Worker cannot honour, and stopping loudly is how
    /// that gets noticed.
    #[error("this worker cannot apply a {0} command")]
    UnsupportedCommand(&'static str),

    /// A spawn command named a provider this Worker has no adapter for.
    #[error("no adapter is registered for provider {0:?}")]
    UnknownProvider(String),

    /// A domain value could not be put on the wire, or a wire value could not
    /// be read back into the domain.
    ///
    /// Carries `horde-wire`'s own reason, which names the exact refusal — a
    /// non-object argument bag, an unrepresentable number, an unusable
    /// outcome, or a rewrite this adapter cannot apply.
    #[error("could not translate between domain and wire: {0}")]
    Translate(#[from] horde_wire::ConversionError),

    /// The harness itself failed.
    #[error("the harness failed: {0}")]
    Adapter(#[from] AdapterError),
}

#[cfg(test)]
mod tests {
    use super::WorkerError;

    /// The messages an operator reads. Each failure has to name the fact that
    /// distinguishes it, or a report of one is indistinguishable from a report
    /// of another — the same bar `SupervisorError` is held to.
    #[test]
    fn each_failure_names_what_an_operator_would_act_on() {
        let journal = WorkerError::Journal {
            context: "appending to the journal",
            source: rusqlite::Error::InvalidQuery,
        }
        .to_string();
        assert!(
            journal.contains("appending to the journal"),
            "the operation is the diagnosis: {journal}"
        );

        let truncated = WorkerError::TruncatedPast {
            requested: 4,
            floor: 10,
        }
        .to_string();
        assert!(
            truncated.contains('4') && truncated.contains("10"),
            "both positions are needed to tell a stale server from a lost journal: {truncated}"
        );

        assert!(
            WorkerError::Incomplete("worker id")
                .to_string()
                .contains("worker id"),
            "an unnamed missing field is not diagnosable under version skew"
        );

        assert!(
            WorkerError::UnsupportedCommand("materialize workspace")
                .to_string()
                .contains("materialize workspace")
        );

        assert!(
            WorkerError::UnknownProvider("codex".to_owned())
                .to_string()
                .contains("codex")
        );

        assert!(
            WorkerError::JournalPoisoned("truncating")
                .to_string()
                .contains("truncating")
        );
    }
}
