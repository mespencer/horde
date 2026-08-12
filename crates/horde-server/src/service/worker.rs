//! The worker-facing service.
//!
//! Workers dial the server, so "commands down" is a server stream and "events
//! up" is a bidirectional stream the worker opens. The two are separate on
//! purpose: a burst of agent output must not head-of-line block a placement
//! command, and each direction reconnects independently.
//!
//! The only interesting logic here is journal ingestion, which is where the two
//! sequence spaces meet — per-worker journal positions coming in, per-session
//! log positions going out.

use std::pin::Pin;

use horde_proto::v1::{
    JournalAck, PushEventsAccepted, PushEventsRequest, PushEventsResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportCapabilitiesRequest, ReportCapabilitiesResponse,
    ReportWorkspaceInstancesRequest, ReportWorkspaceInstancesResponse, ServerHello,
    StreamCommandsRequest, StreamCommandsResponse, WorkerId, push_events_request,
    push_events_response, worker_service_server::WorkerService,
};
use prost::Message;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use super::commands::{self, CommandStream};
use super::shared::{MIN_SUPPORTED_SEMVER, SERVER_SEMVER, ServerCore};
use crate::error::ServerError;
use crate::log::PendingEvent;

/// Implements `horde.v1.WorkerService`.
#[derive(Clone, Debug)]
pub struct WorkerFacing {
    core: ServerCore,
}

impl WorkerFacing {
    #[must_use]
    pub fn new(core: ServerCore) -> Self {
        Self { core }
    }

    fn server_hello() -> ServerHello {
        ServerHello {
            semver: SERVER_SEMVER.to_owned(),
            capability_flags: Vec::new(),
            min_supported_semver: MIN_SUPPORTED_SEMVER.to_owned(),
            server_time: None,
            // Derived from the mTLS certificate once enrollment exists (wave 5).
            // Empty is honest today; inventing a principal would make an
            // unauthenticated peer look authenticated.
            principal: String::new(),
        }
    }
}

/// Ingests one journaled batch, translating journal positions into session
/// positions.
///
/// Returns the highest journal sequence durably stored, which is what the
/// worker needs in order to truncate.
///
/// Factored out of the stream handler because it is the part worth testing: a
/// pure-ish function over a batch, rather than something reachable only by
/// standing up a gRPC connection.
async fn ingest_batch(
    core: &ServerCore,
    worker_id: &str,
    events: Vec<horde_proto::v1::JournaledEvent>,
) -> Result<u64, ServerError> {
    let mut highest = 0_u64;

    for journaled in events {
        let session_id = journaled
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .ok_or_else(|| {
                ServerError::Invalid("a journaled event must name its session".to_owned())
            })?;
        let event = journaled.event.ok_or_else(|| {
            ServerError::Invalid("a journaled event must carry an event".to_owned())
        })?;

        let appended = core
            .log
            .append(PendingEvent {
                session_id: session_id.clone(),
                origin_worker_id: worker_id.to_owned(),
                journal_sequence: Some(journaled.journal_sequence),
                idempotency_key: event.idempotency_key.clone(),
                payload: event.encode_to_vec(),
            })
            .await?;

        // A journal replay is at-least-once and deduplicates silently, as it
        // should. Two *different* events under one key is a different thing
        // entirely, and would otherwise vanish here.
        crate::service::shared::refuse_kind_collision(
            &appended,
            &session_id,
            &event.idempotency_key,
            &event,
        )?;

        highest = highest.max(journaled.journal_sequence);
    }

    Ok(highest)
}

#[tonic::async_trait]
impl WorkerService for WorkerFacing {
    #[instrument(skip_all)]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        let request = request.into_inner();
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Status::from(ServerError::Invalid(
                    "a worker must supply its id until certificates carry it".to_owned(),
                ))
            })?;

        self.core
            .workers
            .register(&worker_id, request.capabilities.unwrap_or_default());

        // Both watermarks are derived from the log rather than remembered, so a
        // restarted server answers a reconnecting worker with the same numbers
        // it would have given before the restart. The journal one comes from
        // the events actually stored; the command one from folding the command
        // stream. Neither survives in memory, and neither needs to.
        let resume_from_journal_sequence = self
            .core
            .log
            .worker_journal_watermark(&worker_id)
            .await
            .map_err(Status::from)?
            .saturating_add(1);

        // What the server has ever issued this worker. The worker corrects this
        // downwards on StreamCommands if it applied fewer; it is a ceiling, not
        // an instruction, and saying "everything" would replay the world.
        let issued = commands::issued_to(&self.core.log, &worker_id)
            .await
            .map_err(Status::from)?;

        tracing::info!(worker_id, resume_from_journal_sequence, "worker registered");

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: Some(WorkerId::new(worker_id.clone())),
            server: Some(Self::server_hello()),
            resume_from_journal_sequence,
            resume_from_command_sequence: issued.saturating_add(1),
        }))
    }

    type StreamCommandsStream =
        Pin<Box<dyn Stream<Item = Result<StreamCommandsResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn stream_commands(
        &self,
        request: Request<StreamCommandsRequest>,
    ) -> Result<Response<Self::StreamCommandsStream>, Status> {
        let request = request.into_inner();
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // The worker's own state decides what is replayed, not the server's
        // guess: it is the only side that knows what it durably applied. That
        // was true when commands lived in memory and it is load-bearing now
        // they are folded from the log, because the fold can rebuild every
        // command a worker was ever issued and only the worker knows which of
        // them it finished.
        let mut stream = CommandStream::open(
            &self.core.log,
            &worker_id,
            request.resume_from_command_sequence,
        );

        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(batch) = stream.next_batch().await {
                let batch = match batch {
                    Ok(batch) => batch,
                    Err(error) => {
                        let _ = sender.send(Err(Status::from(error))).await;
                        return;
                    }
                };
                for command in batch {
                    if sender.send(Ok(command)).await.is_err() {
                        return; // The worker hung up.
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    type PushEventsStream = Pin<Box<dyn Stream<Item = Result<PushEventsResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn push_events(
        &self,
        request: Request<Streaming<PushEventsRequest>>,
    ) -> Result<Response<Self::PushEventsStream>, Status> {
        let mut inbound = request.into_inner();
        let core = self.core.clone();

        let (sender, receiver) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let mut worker_id = String::new();

            while let Some(message) = inbound.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };

                let outcome = match message.body {
                    Some(push_events_request::Body::Start(start)) => {
                        worker_id = start
                            .worker_id
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_default();
                        handle_start(&core, &worker_id, start.last_local_journal_sequence).await
                    }
                    Some(push_events_request::Body::Batch(batch)) => {
                        // `acked_command_sequence` is deliberately not recorded
                        // server-side. The worker replays from its own ack on
                        // the next StreamCommands, so a server-held copy would
                        // be a second answer to a question the worker already
                        // answers — and the copy is the one a restart loses.
                        ingest_batch(&core, &worker_id, batch.events).await.map(
                            |acked_journal_sequence| PushEventsResponse {
                                body: Some(push_events_response::Body::Ack(JournalAck {
                                    acked_journal_sequence,
                                })),
                            },
                        )
                    }
                    None => Err(ServerError::Invalid(
                        "a PushEvents message must carry a body".to_owned(),
                    )),
                };

                let reply = match outcome {
                    Ok(reply) => Ok(reply),
                    Err(error) => Err(Status::from(error)),
                };
                let reply_failed = reply.is_err();
                let send_failed = sender.send(reply).await.is_err();
                if should_stop(send_failed, reply_failed) {
                    return;
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    #[instrument(skip_all)]
    async fn report_capabilities(
        &self,
        request: Request<ReportCapabilitiesRequest>,
    ) -> Result<Response<ReportCapabilitiesResponse>, Status> {
        let request = request.into_inner();
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // Wholesale replacement, per the proto: a partial update is how a
        // scheduler ends up matching on facts that stopped being true.
        self.core
            .workers
            .register(&worker_id, request.capabilities.unwrap_or_default());
        Ok(Response::new(ReportCapabilitiesResponse {}))
    }

    #[instrument(skip_all)]
    async fn report_workspace_instances(
        &self,
        _request: Request<ReportWorkspaceInstancesRequest>,
    ) -> Result<Response<ReportWorkspaceInstancesResponse>, Status> {
        // Workspace instances land with workspaces in wave 4. Accepting and
        // discarding would make a worker believe the server knows about its
        // checkouts.
        Err(Status::from(ServerError::Unimplemented(
            "workspace instance reporting",
        )))
    }
}

/// Whether the ingest loop should stop after this reply.
///
/// Extracted because inline the two halves are not separately reachable: a
/// failed reply is usually followed by a worker that hangs up, so `&&` still
/// stops the loop in most runs and survives every end-to-end test. As a
/// predicate all four cases are asserted.
///
/// Stopping on a failed reply is deliberate. The stream carries one worker's
/// journal, and a rejected batch means the server and the worker disagree about
/// what is stored; continuing to accept batches after that would append on top
/// of a disagreement rather than making the worker resynchronise.
fn should_stop(send_failed: bool, reply_failed: bool) -> bool {
    send_failed || reply_failed
}

/// Answers `PushEventsStart`, refusing a worker that lost its journal.
async fn handle_start(
    core: &ServerCore,
    worker_id: &str,
    last_local_journal_sequence: u64,
) -> Result<PushEventsResponse, ServerError> {
    let stored = core.log.worker_journal_watermark(worker_id).await?;

    // A worker reporting a head below what we already stored has lost its
    // journal. Renumbering its events under fresh journal positions would
    // forge history and silently defeat the dedupe index, so this is refused.
    if last_local_journal_sequence < stored {
        return Err(ServerError::JournalRegressed {
            worker_id: worker_id.to_owned(),
            reported: last_local_journal_sequence,
            stored,
        });
    }

    Ok(PushEventsResponse {
        body: Some(push_events_response::Body::Accepted(PushEventsAccepted {
            resume_from_journal_sequence: stored.saturating_add(1),
        })),
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, Event, JournaledEvent, SessionId, TurnInterrupted, event::Kind,
    };

    use super::super::shared::ServerCore;
    use super::{handle_start, ingest_batch};
    use crate::EventLog;
    use crate::error::ServerError;

    fn core() -> ServerCore {
        ServerCore::new(EventLog::in_memory().expect("in-memory log"))
    }

    fn journaled(session: &str, journal_sequence: u64, key: &str) -> JournaledEvent {
        JournaledEvent {
            journal_sequence,
            session_id: Some(SessionId::new(session)),
            event: Some(Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: key.to_owned(),
                kind: Some(Kind::TurnInterrupted(TurnInterrupted::default())),
            }),
        }
    }

    #[test]
    fn the_ingest_loop_stops_when_either_the_reply_or_the_send_failed() {
        // Neither: the only case that keeps the stream open.
        assert!(!super::should_stop(false, false));

        // The worker hung up, so there is nobody to answer.
        assert!(super::should_stop(true, false));

        // The batch was rejected; continuing would append on top of a
        // disagreement about what is stored.
        assert!(super::should_stop(false, true));

        assert!(super::should_stop(true, true));
    }

    #[tokio::test]
    async fn ingestion_translates_journal_positions_into_session_positions() {
        let core = core();
        let acked = ingest_batch(
            &core,
            "worker-a",
            vec![
                journaled("s1", 1, "k1"),
                journaled("s1", 2, "k2"),
                journaled("s2", 3, "k3"),
            ],
        )
        .await
        .expect("ingest");

        assert_eq!(acked, 3, "the ack is in the worker's sequence space");
        assert_eq!(core.log.head("s1").await.expect("head"), 2);
        assert_eq!(
            core.log.head("s2").await.expect("head"),
            1,
            "each session numbers independently of the worker's journal"
        );
    }

    #[tokio::test]
    async fn re_ingesting_a_batch_after_a_reconnect_appends_nothing_new() {
        let core = core();
        let batch = vec![journaled("s1", 1, "k1"), journaled("s1", 2, "k2")];

        ingest_batch(&core, "worker-a", batch.clone())
            .await
            .expect("ingest");
        let acked = ingest_batch(&core, "worker-a", batch)
            .await
            .expect("re-ingest");

        assert_eq!(acked, 2, "the ack still reflects what is durably stored");
        assert_eq!(
            core.log.head("s1").await.expect("head"),
            2,
            "a replayed journal must not grow the session log"
        );
    }

    #[tokio::test]
    async fn a_journaled_event_without_a_session_is_rejected() {
        let core = core();
        let orphan = JournaledEvent {
            journal_sequence: 1,
            session_id: None,
            event: Some(Event::default()),
        };
        let error = ingest_batch(&core, "worker-a", vec![orphan])
            .await
            .expect_err("must reject");
        assert!(matches!(error, ServerError::Invalid(_)));
    }

    #[tokio::test]
    async fn a_fresh_worker_is_told_to_start_at_one() {
        let core = core();
        let response = handle_start(&core, "worker-a", 0).await.expect("start");
        let body = response.body.expect("a body");
        match body {
            horde_proto::v1::push_events_response::Body::Accepted(accepted) => {
                assert_eq!(accepted.resume_from_journal_sequence, 1);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_worker_resumes_after_what_the_server_already_stored() {
        let core = core();
        ingest_batch(&core, "worker-a", vec![journaled("s1", 1, "k1")])
            .await
            .expect("ingest");

        let response = handle_start(&core, "worker-a", 1).await.expect("start");
        match response.body.expect("a body") {
            horde_proto::v1::push_events_response::Body::Accepted(accepted) => {
                assert_eq!(accepted.resume_from_journal_sequence, 2);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    /// Renumbering would forge history and defeat the dedupe index, so a worker
    /// that lost its journal is refused rather than accommodated.
    #[tokio::test]
    async fn a_worker_that_lost_its_journal_is_refused() {
        let core = core();
        for journal_sequence in 1..=5 {
            ingest_batch(
                &core,
                "worker-a",
                vec![journaled(
                    "s1",
                    journal_sequence,
                    &format!("k{journal_sequence}"),
                )],
            )
            .await
            .expect("ingest");
        }

        let error = handle_start(&core, "worker-a", 2)
            .await
            .expect_err("a regressed journal must be refused");
        match error {
            ServerError::JournalRegressed {
                reported, stored, ..
            } => {
                assert_eq!(reported, 2);
                assert_eq!(stored, 5);
            }
            other => panic!("expected JournalRegressed, got {other}"),
        }
    }
}
