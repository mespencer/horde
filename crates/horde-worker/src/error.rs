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

    /// A remembered spawn command no longer decodes.
    ///
    /// This Worker encoded it itself when the spawn applied, so it is
    /// corruption rather than peer input — same standing as
    /// [`Self::CorruptJournalEntry`], scoped to the agent it strands.
    #[error("the remembered spawn for agent {agent_id} is not decodable: {source}")]
    CorruptAgentRecord {
        agent_id: String,
        source: horde_proto::prost::DecodeError,
    },

    /// An accepted command held in the inbox no longer decodes.
    ///
    /// Same standing as [`Self::CorruptAgentRecord`]: this Worker encoded
    /// the row when it accepted the command, so a row that will not decode
    /// is corruption, scoped to the command it strands.
    #[error("the inbox entry for command {command_id} is not decodable: {source}")]
    CorruptInboxEntry {
        command_id: String,
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

    /// An Agent's MCP endpoint could not be served.
    ///
    /// Like [`Self::Workspace`], this is the *spawn's* failure: an agent
    /// launched without its endpoint would run with no orchestrator verbs and
    /// no permission prompt tool, which is worse than failing the spawn
    /// honestly.
    #[error("the agent's MCP endpoint could not be served: {0}")]
    EndpointUnavailable(String),

    /// The agent's proxied MCP surface could not be assembled: a `local`
    /// server failed to spawn or connect, or the hub link could not be
    /// fetched over the Worker's channel. Fails the spawn rather than
    /// launching a degraded agent — an agent silently missing a promised
    /// toolset is the failure shape ADR-0004's audit warned about.
    #[error("the agent's MCP surface could not be assembled: {0}")]
    McpSurface(String),

    /// A workspace checkout could not be produced for an agent.
    ///
    /// This is the *spawn's* failure, not the Worker's: the runtime journals
    /// it as a failed lifecycle so the Server hears which agent has no
    /// working directory, and every other agent keeps running.
    #[error("the workspace checkout failed: {0}")]
    Workspace(String),

    /// A Payload's attachment could not be stashed, fetched, or verified.
    ///
    /// The message carries the specific refusal — a missing blob service, a
    /// malformed address, inline content that does not hash to its digest —
    /// because "the attachment failed" names nothing anyone can fix.
    #[error("the attachment transfer failed: {0}")]
    Attachment(String),

    /// A Payload's commits could not be staged or obtained.
    ///
    /// Carries the share layer's own words — a dirty tree, a missing commit,
    /// a cross-Worker route with no Relay — because "the payload failed" is
    /// not actionable and each of those is.
    #[error("the payload share failed: {0}")]
    Share(#[from] horde_relay::ShareError),

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
            WorkerError::Workspace("no instance of ws-1".to_owned())
                .to_string()
                .contains("ws-1"),
            "the failing workspace is what an operator materializes"
        );

        assert!(
            WorkerError::JournalPoisoned("truncating")
                .to_string()
                .contains("truncating")
        );
    }
}
