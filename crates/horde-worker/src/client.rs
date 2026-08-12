//! The Worker's dial: one Server, two streams, resume decided by local state.
//!
//! The Worker dials outbound (NAT-friendly, DESIGN.md §2); the Server never
//! dials a Worker. Commands arrive on a server stream, journaled events leave
//! on a bidirectional stream whose replies carry the acks — separate streams so
//! a burst of agent output cannot head-of-line block a command, reconnecting
//! independently for the same reason.
//!
//! # Resume positions are the Worker's, not the Server's
//!
//! Both streams resume from what this Worker durably recorded — the applied
//! command watermark and the journal — never from the Server's answer at
//! registration. The Server's numbers are ceilings derived from its own log;
//! only the Worker knows what it actually applied, and `worker_service.proto`
//! says its state is authoritative. The one Server number that matters is
//! `PushEventsAccepted.resume_from_journal_sequence`, which is a fact about
//! the *Server's* storage: everything below it is durably stored and may be
//! truncated.
//!
//! # The pump is lock-step
//!
//! One batch in flight, then its ack, then the next batch. That trades
//! throughput nobody needs yet for a property the partition tests can hold
//! directly: every truncation is justified by an ack the Server actually sent,
//! and a severed connection can lose at most one unacked batch — which the
//! journal still holds, by construction.

use async_trait::async_trait;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{
    Hello, JournaledEventBatch, PushEventsRequest, PushEventsResponse, PushEventsStart,
    RegisterWorkerResponse, StreamCommandsRequest, StreamCommandsResponse, WorkerCapabilities,
    WorkerId, push_events_request, push_events_response,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::error::WorkerError;
use crate::journal::Journal;

/// How many journaled events ride one batch.
const PAGE: u32 = 256;

/// What the command feed drives — the seam between the wire and the Runtime.
///
/// A trait rather than a concrete type so the stream mechanics are testable
/// against a recorder, and so the Runtime never learns about tonic.
#[async_trait]
pub trait ApplyCommand: Send {
    /// Applies one command: dedupe, execute, and journal its effect —
    /// atomically, per the journal's contract.
    ///
    /// An error is fatal to the command loop on purpose: acking past a command
    /// that was never applied tells the Server work happened that did not.
    async fn apply(&mut self, command: StreamCommandsResponse) -> Result<(), WorkerError>;
}

/// Everything needed to dial one Server as one Worker.
#[derive(Debug, Clone)]
pub struct DialConfig {
    /// The Server's endpoint, e.g. `http://127.0.0.1:50051`.
    pub endpoint: String,
    /// The Worker's own view of its identity. The Server may correct it from
    /// the mTLS certificate once enrollment exists.
    pub worker_id: String,
    /// Version and capability-flag handshake, sent at registration.
    pub hello: Hello,
    /// The full capability advertisement placement matches on.
    pub capabilities: WorkerCapabilities,
}

/// One registered connection to the Server.
#[derive(Debug, Clone)]
pub struct Connection {
    client: WorkerServiceClient<Channel>,
    worker_id: String,
}

impl Connection {
    /// Dials and registers, returning a connection under the Server's
    /// authoritative view of the Worker's identity.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Transport`] when the endpoint is unreachable,
    /// [`WorkerError::Rpc`] when registration is refused, and
    /// [`WorkerError::Incomplete`] when the response is missing a field the
    /// protocol requires.
    pub async fn dial(config: &DialConfig) -> Result<(Self, RegisterWorkerResponse), WorkerError> {
        let mut client = WorkerServiceClient::connect(config.endpoint.clone()).await?;

        let response = client
            .register_worker(horde_proto::v1::RegisterWorkerRequest {
                hello: Some(config.hello.clone()),
                worker_id: Some(WorkerId::new(config.worker_id.clone())),
                capabilities: Some(config.capabilities.clone()),
            })
            .await?
            .into_inner();

        let worker_id = response
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
            .ok_or(WorkerError::Incomplete("worker id"))?;

        Ok((Self { client, worker_id }, response))
    }

    /// The identity the Server registered this connection under.
    #[must_use]
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// Streams commands into `target` until the Server closes the stream.
    ///
    /// Resumes from the journal's applied-command watermark plus one — the
    /// Worker's own record of what it durably applied, which outranks anything
    /// the Server said at registration.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Rpc`] when the stream fails mid-flight; whatever
    /// `target` reports when a command cannot be applied.
    pub async fn feed_commands(
        &mut self,
        journal: &Journal,
        target: &mut dyn ApplyCommand,
    ) -> Result<(), WorkerError> {
        let resume_from = journal.applied_command_sequence().await?.saturating_add(1);
        let mut stream = self
            .client
            .stream_commands(StreamCommandsRequest {
                worker_id: Some(WorkerId::new(self.worker_id.clone())),
                resume_from_command_sequence: resume_from,
            })
            .await?
            .into_inner();

        while let Some(command) = stream.message().await? {
            target.apply(command).await?;
        }
        Ok(())
    }

    /// Runs both streams until either finishes; the other is cancelled by
    /// drop.
    ///
    /// Returning means this connection is over — a partition, a server
    /// restart, a refused batch. Reconnection is the caller's policy, not this
    /// function's: dial again and run again over the same journal and
    /// runtime, and the resume rules do the rest.
    ///
    /// # Errors
    ///
    /// Whatever ended the connection first; `Ok` when the Server closed a
    /// stream cleanly.
    pub async fn run(
        &mut self,
        journal: &Journal,
        target: &mut dyn ApplyCommand,
    ) -> Result<(), WorkerError> {
        let mut push_side = self.clone();
        tokio::select! {
            outcome = self.feed_commands(journal, target) => outcome,
            outcome = push_side.push_events(journal) => outcome,
        }
    }

    /// Pushes journaled events until the Server hangs up, truncating on acks.
    ///
    /// Opens the stream with the journal's highest-ever sequence (which
    /// truncation cannot regress), replays from where the Server says its
    /// storage ends, and then follows the journal live.
    ///
    /// Returns `Ok` when the Server ends the stream — reconnecting is the
    /// caller's policy, not this function's.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Rpc`] when the stream fails or the Server refuses a
    /// batch, [`WorkerError::TruncatedPast`] when the Server asks for events
    /// this journal no longer holds, and [`WorkerError::Incomplete`] when a
    /// reply is missing what the protocol requires.
    pub async fn push_events(&mut self, journal: &Journal) -> Result<(), WorkerError> {
        // Capacity 2: the Start, then at most one lock-step batch in flight.
        let (to_server, outbound) = tokio::sync::mpsc::channel(2);

        let start = PushEventsRequest {
            body: Some(push_events_request::Body::Start(PushEventsStart {
                worker_id: Some(WorkerId::new(self.worker_id.clone())),
                last_local_journal_sequence: journal.last_local_sequence().await?,
            })),
        };
        // Capacity is reserved and the receiver is alive; this cannot fail yet.
        let _ = to_server.send(start).await;

        let mut inbound = self
            .client
            .push_events(ReceiverStream::new(outbound))
            .await?
            .into_inner();

        let PushReply::Accepted(accepted) = next_reply(&mut inbound).await? else {
            return Err(WorkerError::Incomplete("accepted resume position"));
        };

        // Everything below the resume point is durably stored server-side.
        journal
            .truncate_below(accepted.resume_from_journal_sequence)
            .await?;

        let mut cursor = accepted.resume_from_journal_sequence;
        let mut head = journal.watch_head();
        loop {
            let events = journal.read_from(cursor, PAGE).await?;
            if events.is_empty() {
                // Nothing owed: park until the journal moves. A closed watch
                // means the journal's last handle is gone — the Worker is
                // shutting down, and an empty journal is fully delivered.
                if head.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }

            let batch = PushEventsRequest {
                body: Some(push_events_request::Body::Batch(JournaledEventBatch {
                    events,
                    // Piggybacked so command acks flow even when the command
                    // stream is quiet.
                    acked_command_sequence: journal.applied_command_sequence().await?,
                })),
            };
            if to_server.send(batch).await.is_err() {
                // The transport dropped the outbound side. The inbound read
                // below reports the reason, which is more than "send failed".
            }

            match next_reply(&mut inbound).await? {
                PushReply::Ack(ack) => {
                    // Both the truncation and the cursor follow the *ack*,
                    // never the batch: the server's word on what is durably
                    // stored is the only justification for either. A cursor
                    // that drifted below the floor this truncation sets would
                    // fail the next read loudly (TruncatedPast) instead of
                    // silently re-sending forever.
                    let resume = ack.acked_journal_sequence.saturating_add(1);
                    journal.truncate_below(resume).await?;
                    cursor = resume;
                }
                PushReply::Accepted(_) => return Err(WorkerError::Incomplete("journal ack")),
                PushReply::Ended => return Ok(()),
            }
        }
    }
}

/// One reply on the push stream, seen through the protocol's eyes.
///
/// The wire allows any body in any position; the pump does not. Naming the
/// three shapes here keeps the pump's loop about *ordering* (an ack answers a
/// batch, an acceptance answers a start) while this stays about decoding.
enum PushReply {
    Accepted(horde_proto::v1::PushEventsAccepted),
    Ack(horde_proto::v1::JournalAck),
    /// The Server closed the stream.
    Ended,
}

/// Reads the next reply from the Server's half of the push stream.
async fn next_reply(
    inbound: &mut tonic::Streaming<PushEventsResponse>,
) -> Result<PushReply, WorkerError> {
    match inbound.message().await? {
        Some(PushEventsResponse {
            body: Some(push_events_response::Body::Accepted(accepted)),
        }) => Ok(PushReply::Accepted(accepted)),
        Some(PushEventsResponse {
            body: Some(push_events_response::Body::Ack(ack)),
        }) => Ok(PushReply::Ack(ack)),
        Some(PushEventsResponse { body: None }) => Err(WorkerError::Incomplete("push reply body")),
        None => Ok(PushReply::Ended),
    }
}

#[cfg(test)]
mod tests {
    // The dial, the feed, and the pump are exercised end-to-end against a real
    // `horde-server` over a real socket in `tests/connection.rs` — stream
    // mechanics have no meaningful unit below "both ends of a socket agree".
    // What lives here is what is pure: nothing yet, and this comment is the
    // record of that decision rather than an empty module pretending coverage.
}
