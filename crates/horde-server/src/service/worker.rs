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
use crate::placement;

/// Implements `horde.v1.WorkerService`.
#[derive(Clone, Debug)]
pub struct WorkerFacing {
    core: ServerCore,
    /// The live deny-list, when this spine serves behind the mTLS door.
    /// Streams re-check it so revocation reaches them without waiting for a
    /// reconnect (wave-7 fix a4); `None` — the plaintext test shape — has no
    /// certificates to revoke.
    deny: Option<horde_identity::RevocationList>,
}

impl WorkerFacing {
    #[must_use]
    pub fn new(core: ServerCore) -> Self {
        Self { core, deny: None }
    }

    /// Attaches the deny-list the mTLS door shares, so live streams are
    /// severed on revocation rather than at the next reconnect.
    #[must_use]
    pub fn with_deny(mut self, deny: horde_identity::RevocationList) -> Self {
        self.deny = Some(deny);
        self
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
        // The other append chokepoint: the worker journal must not carry
        // bulk into the log any more than a client verb may.
        crate::service::shared::refuse_inline_bulk(&event)?;

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

        // A journaled instance report is also the freshest fact about that
        // worker's disk, so the cache placement reads hears it too.
        if let Some(horde_proto::v1::event::Kind::WorkspaceInstanceChanged(changed)) = &event.kind
            && let Some(report) = &changed.report
        {
            core.instances.record(report);
        }

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
        // The version gate (DESIGN §2, fix a6): a worker below the minimum —
        // or one saying nothing — is refused at registration, with the code
        // the daemon's fate table treats as fatal.
        super::shared::version_gate(request.hello.as_ref())?;
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

        register_and_replace(
            &self.core,
            &worker_id,
            request.capabilities.unwrap_or_default(),
        )
        .await
        .map_err(Status::from)?;

        let (resume_from_journal_sequence, resume_from_command_sequence) =
            resume_positions(&self.core, &worker_id)
                .await
                .map_err(Status::from)?;

        tracing::info!(worker_id, resume_from_journal_sequence, "worker registered");

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: Some(WorkerId::new(worker_id.clone())),
            server: Some(Self::server_hello()),
            resume_from_journal_sequence,
            resume_from_command_sequence,
        }))
    }

    type StreamCommandsStream =
        Pin<Box<dyn Stream<Item = Result<StreamCommandsResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn stream_commands(
        &self,
        request: Request<StreamCommandsRequest>,
    ) -> Result<Response<Self::StreamCommandsStream>, Status> {
        let serial = super::shared::peer_serial(&request);
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
        let core = self.core.clone();
        let deny = self.deny.clone();
        // Captured now, checked at disconnect: by the time this stream
        // notices its peer is gone, the worker may have re-registered, and a
        // stale stream must not knock the fresh registration out of placement.
        let epoch = core.workers.epoch_of(&worker_id);
        tokio::spawn(async move {
            let pump = async {
                'stream: while let Some(batch) = stream.next_batch().await {
                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(error) => {
                            let _ = sender.send(Err(Status::from(error))).await;
                            break 'stream;
                        }
                    };
                    for command in batch {
                        if sender.send(Ok(command)).await.is_err() {
                            break 'stream; // The worker hung up.
                        }
                    }
                }
            };
            // Revocation severs the stream (fix a4).
            super::shared::run_severable(pump, deny, serial, &sender).await;

            // The command stream is the worker's lifeline: once it is gone the
            // server cannot command this worker, so it leaves placement and
            // the agents it never started are re-queued — unless the worker
            // has re-registered since, in which case this stream is stale and
            // the disconnect is not its to declare. This is a background
            // task, so a failed re-queue can only be logged — the next
            // placement pass will see the same queued agents anyway.
            if !worker_id.is_empty()
                && core.workers.deregister_if_current(&worker_id, epoch)
                && let Err(error) = worker_lost(&core, &worker_id).await
            {
                tracing::warn!(
                    %error,
                    worker_id,
                    "re-queueing a disconnected worker's agents failed"
                );
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
        let serial = super::shared::peer_serial(&request);
        let mut inbound = request.into_inner();
        let core = self.core.clone();
        let deny = self.deny.clone();

        let (sender, receiver) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let pump = async {
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
                            // `acked_command_sequence` is deliberately not
                            // recorded server-side. The worker replays from
                            // its own ack on the next StreamCommands, so a
                            // server-held copy would be a second answer to a
                            // question the worker already answers — and the
                            // copy is the one a restart loses.
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
            };
            // Revocation severs the journal push mid-connection (fix a4): a
            // revoked worker must not keep feeding events into the log until
            // it happens to reconnect.
            super::shared::run_severable(pump, deny, serial, &sender).await;
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
        register_and_replace(
            &self.core,
            &worker_id,
            request.capabilities.unwrap_or_default(),
        )
        .await
        .map_err(Status::from)?;
        Ok(Response::new(ReportCapabilitiesResponse {}))
    }

    #[instrument(skip_all)]
    async fn report_workspace_instances(
        &self,
        request: Request<ReportWorkspaceInstancesRequest>,
    ) -> Result<Response<ReportWorkspaceInstancesResponse>, Status> {
        let request = request.into_inner();
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // The worker-local state file is the truth; this call corrects the
        // server's cache. An authoritative report is the worker's complete
        // set, so cached instances absent from it are dropped.
        if request.authoritative {
            self.core
                .instances
                .replace_worker(&worker_id, &request.reports);
        } else {
            for report in &request.reports {
                self.core.instances.record(report);
            }
        }
        Ok(Response::new(ReportWorkspaceInstancesResponse {}))
    }
}

/// Where a registering worker resumes both streams: `(journal, command)`.
///
/// Both watermarks are derived from the log rather than remembered, so a
/// restarted server answers a reconnecting worker with the same numbers it
/// would have given before the restart. The journal one comes from the events
/// actually stored; the command one from folding the command stream — a
/// ceiling the worker corrects downwards on `StreamCommands` if it applied
/// fewer, because saying "everything" would replay the world.
async fn resume_positions(core: &ServerCore, worker_id: &str) -> Result<(u64, u64), ServerError> {
    let journal = core
        .log
        .worker_journal_watermark(worker_id)
        .await?
        .saturating_add(1);
    let issued = commands::issued_to(&core.log, worker_id).await?;
    Ok((journal, issued.saturating_add(1)))
}

/// Registers a worker's capabilities, refreshes the instance cache from its
/// advertised warm instances, and runs a placement pass — a worker appearing
/// (or changing) is exactly the moment a queued agent may become placeable.
async fn register_and_replace(
    core: &ServerCore,
    worker_id: &str,
    capabilities: horde_proto::v1::WorkerCapabilities,
) -> Result<(), ServerError> {
    core.instances
        .replace_worker(worker_id, &capabilities.warm_instances);
    core.workers.register(worker_id, capabilities);
    placement::place_queued(core).await
}

/// What losing a worker means once it has left the registry: the agents it
/// never started are re-queued and re-placed elsewhere. Agents mid-turn stay
/// placed — a disconnect is a partition, not a death.
async fn worker_lost(core: &ServerCore, worker_id: &str) -> Result<(), ServerError> {
    placement::queue_lost_worker(core, worker_id).await?;
    placement::place_queued(core).await
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

    /// The registration handler itself: a versioned worker registers and is
    /// told where to resume; a nameless one is refused; a versionless one is
    /// stopped by the a6 gate before anything registers.
    #[tokio::test]
    async fn registration_admits_versioned_named_workers_and_refuses_the_rest() {
        use horde_proto::v1::worker_service_server::WorkerService as _;

        let facing = super::WorkerFacing::new(core());
        let request = |hello: Option<&str>, worker_id: Option<&str>| {
            tonic::Request::new(horde_proto::v1::RegisterWorkerRequest {
                hello: hello.map(|semver| horde_proto::v1::Hello {
                    semver: semver.to_owned(),
                    ..horde_proto::v1::Hello::default()
                }),
                worker_id: worker_id.map(horde_proto::v1::WorkerId::new),
                capabilities: Some(horde_proto::v1::WorkerCapabilities::default()),
            })
        };

        let registered = facing
            .register_worker(request(Some("0.1.0"), Some("worker-a")))
            .await
            .expect("a versioned, named worker registers")
            .into_inner();
        assert_eq!(
            registered.worker_id.map(|id| id.to_string()),
            Some("worker-a".to_owned())
        );
        assert_eq!(
            registered.resume_from_journal_sequence, 1,
            "a fresh worker resumes from the first position"
        );

        let unnamed = facing
            .register_worker(request(Some("0.1.0"), None))
            .await
            .expect_err("a nameless worker is refused");
        assert_eq!(unnamed.code(), tonic::Code::InvalidArgument);

        let unversioned = facing
            .register_worker(request(None, Some("worker-b")))
            .await
            .expect_err("the version gate runs before registration");
        assert_eq!(unversioned.code(), tonic::Code::FailedPrecondition);
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

    /// Both resume positions come from the log, so a fresh worker starts both
    /// streams at one, and a worker with stored history resumes after it.
    #[tokio::test]
    async fn resume_positions_are_folded_from_the_log_not_remembered() {
        let core = core();
        let (journal, command) = super::resume_positions(&core, "worker-a")
            .await
            .expect("positions");
        assert_eq!((journal, command), (1, 1), "a fresh worker starts at one");

        ingest_batch(&core, "worker-a", vec![journaled("s1", 1, "k1")])
            .await
            .expect("ingest");
        let (journal, _) = super::resume_positions(&core, "worker-a")
            .await
            .expect("positions");
        assert_eq!(journal, 2, "the journal resumes after what is stored");
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
