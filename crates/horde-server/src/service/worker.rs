//! The worker-facing service.
//!
//! Workers dial the server, so "commands down" is a server stream and "events
//! up" is a bidirectional stream the worker opens. The two are separate on
//! purpose: a burst of agent output must not head-of-line block a placement
//! command, and each direction reconnects independently.
//!
//! The interesting logic — journal ingestion, where the two sequence spaces
//! meet and where a worker's events are judged before they land — lives in
//! [`super::ingest`]. Who a request speaks for is `crate::trust`'s answer.

use std::pin::Pin;

use horde_proto::v1::{
    CallHubToolRequest, CallHubToolResponse, JournalAck, ListHubToolsRequest, ListHubToolsResponse,
    PushEventsAccepted, PushEventsRequest, PushEventsResponse, RegisterWorkerRequest,
    RegisterWorkerResponse, ReportCapabilitiesRequest, ReportCapabilitiesResponse,
    ReportWorkspaceInstancesRequest, ReportWorkspaceInstancesResponse, ServerHello,
    StreamCommandsRequest, StreamCommandsResponse, WorkerId, push_events_request,
    push_events_response, worker_service_server::WorkerService,
};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::instrument;

use super::commands::{self, CommandStream};
use super::shared::{MIN_SUPPORTED_SEMVER, SERVER_SEMVER, ServerCore};
use crate::error::ServerError;
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

#[tonic::async_trait]
impl WorkerService for WorkerFacing {
    #[instrument(skip_all)]
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        // The certificate names the worker (ADR-0009); the body's claim
        // stands only where no certificate exists — the plaintext test shape.
        let worker_id =
            crate::trust::caller_worker_id(&request, request.get_ref().worker_id.as_ref())?;
        let request = request.into_inner();
        // The version gate (DESIGN §2, fix a6): a worker below the minimum —
        // or one saying nothing — is refused at registration, with the code
        // the daemon's fate table treats as fatal.
        super::shared::version_gate(request.hello.as_ref())?;
        if worker_id.is_empty() {
            return Err(Status::from(ServerError::Invalid(
                "a worker must supply its id when no certificate carries it".to_owned(),
            )));
        }
        // A regressed journal is refused here, before any stream opens: its
        // command stream would otherwise replay a history its fresh state
        // never applied (ADR-0011). Unset from older workers, which are
        // still judged at PushEventsStart.
        if let Some(reported) = request.last_local_journal_sequence {
            refuse_regressed(&self.core, &worker_id, reported)
                .await
                .map_err(Status::from)?;
        }
        let capabilities = request.capabilities.unwrap_or_default();
        refuse_foreign_reports(&worker_id, &capabilities.warm_instances)?;

        register_and_replace(&self.core, &worker_id, capabilities)
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
        let worker_id =
            crate::trust::caller_worker_id(&request, request.get_ref().worker_id.as_ref())?;
        let request = request.into_inner();

        // The worker's own state decides what is replayed, not the server's
        // guess: it is the only side that knows what it durably applied. That
        // was true when commands lived in memory and it is load-bearing now
        // they are folded from the log, because the fold can rebuild every
        // command a worker was ever issued and only the worker knows which of
        // them it finished.
        let mut stream = CommandStream::resuming(
            &self.core.log,
            &worker_id,
            commands::Resume {
                sequence: request.resume_from_command_sequence,
                log_position: request.resume_after_log_position,
            },
        );

        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let core = self.core.clone();
        let deny = self.deny.clone();
        // Captured now, checked at disconnect: by the time this stream
        // notices its peer is gone, the worker may have re-registered, and a
        // stale stream must not knock the fresh registration out of placement.
        let epoch = core.workers.epoch_of(&worker_id);
        tokio::spawn(async move {
            // Revocation severs the stream (fix a4); a dead peer ends it as
            // soon as the transport says so, not at the next command.
            let pump = pump_commands(&mut stream, &sender);
            super::shared::run_severable(pump, deny, serial, &sender).await;

            // The command stream is the worker's lifeline: once it is gone the
            // server cannot command this worker, so it leaves placement, the
            // agents it never started are re-queued, and its lease starts
            // (ADR-0011) — unless the worker has re-registered since, in
            // which case this stream is stale and the disconnect is not its
            // to declare. This is a background task, so a failed re-queue can
            // only be logged — the next placement pass will see the same
            // queued agents anyway.
            if !worker_id.is_empty() && core.workers.deregister_if_current(&worker_id, epoch) {
                crate::liveness::start_lease(&core, &worker_id, epoch);
                if let Err(error) = worker_lost(&core, &worker_id).await {
                    tracing::warn!(
                        %error,
                        worker_id,
                        "re-queueing a disconnected worker's agents failed"
                    );
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
        let serial = super::shared::peer_serial(&request);
        // Read before the body is consumed: the certificate, not the Start
        // message, says whose journal this stream carries (ADR-0009).
        let identity = request
            .extensions()
            .get::<horde_identity::PeerIdentity>()
            .cloned();
        let mut inbound = request.into_inner();
        let core = self.core.clone();
        let deny = self.deny.clone();

        let (sender, receiver) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            let pump = async {
                // A certificate names the worker from the first message; the
                // plaintext shape learns it from Start.
                let mut worker_id = identity
                    .as_ref()
                    .map(|identity| identity.principal.name().to_owned())
                    .unwrap_or_default();
                let mut origins = if identity.is_some() {
                    super::ingest::OriginView::new()
                } else {
                    super::ingest::OriginView::ambient()
                };

                while let Some(message) = inbound.next().await {
                    let message = match message {
                        Ok(message) => message,
                        Err(status) => {
                            let _ = sender.send(Err(status)).await;
                            return;
                        }
                    };

                    let outcome =
                        match message.body {
                            Some(push_events_request::Body::Start(start)) => {
                                match crate::trust::worker_identity(
                                    identity.as_ref(),
                                    start
                                        .worker_id
                                        .as_ref()
                                        .map(ToString::to_string)
                                        .unwrap_or_default(),
                                ) {
                                    Ok(claimed) => {
                                        worker_id = claimed;
                                        handle_start(
                                            &core,
                                            &worker_id,
                                            start.last_local_journal_sequence,
                                        )
                                        .await
                                    }
                                    Err(status) => {
                                        let _ = sender.send(Err(status)).await;
                                        return;
                                    }
                                }
                            }
                            Some(push_events_request::Body::Batch(batch)) => {
                                // `acked_command_sequence` is deliberately not
                                // recorded server-side. The worker replays from
                                // its own ack on the next StreamCommands, so a
                                // server-held copy would be a second answer to a
                                // question the worker already answers — and the
                                // copy is the one a restart loses.
                                super::ingest::ingest_batch(
                                    &core,
                                    &mut origins,
                                    &worker_id,
                                    batch.events,
                                )
                                .await
                                .map(|acked_journal_sequence| PushEventsResponse {
                                    body: Some(push_events_response::Body::Ack(JournalAck {
                                        acked_journal_sequence,
                                    })),
                                })
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

    type StreamForwardsStream =
        Pin<Box<dyn Stream<Item = Result<horde_proto::v1::StreamForwardsResponse, Status>> + Send>>;

    /// The forward plane's worker leg (ADR-0006). Deliberately NOT the
    /// command stream: intents ride this ephemeral stream so that its death
    /// takes the realization with it, and a reconnect re-sends the whole set.
    #[instrument(skip_all)]
    async fn stream_forwards(
        &self,
        request: Request<horde_proto::v1::StreamForwardsRequest>,
    ) -> Result<Response<Self::StreamForwardsStream>, Status> {
        let serial = super::shared::peer_serial(&request);
        let worker_id = named_worker(&request, request.get_ref().worker_id.as_ref(), "forward")?;

        let (epoch, lane) = self.core.forwards.register_worker(&worker_id);
        // Subscribe before the first computation, so an intent appended
        // during it is either in the set or in the channel.
        let notifications = self.core.log.subscribe();
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let deny = self.deny.clone();
        let core = self.core.clone();
        tokio::spawn(async move {
            let pump = pump_forwards(&core, &worker_id, lane, notifications, &sender);
            super::shared::run_severable(pump, deny, serial, &sender).await;
            core.forwards
                .deregister_worker_if_current(&worker_id, epoch);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    type WorkerForwardTunnelStream = Pin<
        Box<dyn Stream<Item = Result<horde_proto::v1::WorkerForwardTunnelResponse, Status>> + Send>,
    >;

    #[instrument(skip_all)]
    async fn worker_forward_tunnel(
        &self,
        request: Request<Streaming<horde_proto::v1::WorkerForwardTunnelRequest>>,
    ) -> Result<Response<Self::WorkerForwardTunnelStream>, Status> {
        Ok(super::forwards::tunnel_response(
            &self.core,
            self.deny.clone(),
            request,
            |message: horde_proto::v1::WorkerForwardTunnelRequest| {
                message.frame.unwrap_or_default()
            },
            |frame| horde_proto::v1::WorkerForwardTunnelResponse { frame: Some(frame) },
        ))
    }

    type StreamLoginsStream =
        Pin<Box<dyn Stream<Item = Result<horde_proto::v1::StreamLoginsResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn stream_logins(
        &self,
        request: Request<horde_proto::v1::StreamLoginsRequest>,
    ) -> Result<Response<Self::StreamLoginsStream>, Status> {
        let serial = super::shared::peer_serial(&request);
        let worker_id = named_worker(&request, request.get_ref().worker_id.as_ref(), "login")?;

        let (epoch, lane) = self.core.logins.register_worker(&worker_id);
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let deny = self.deny.clone();
        let logins = self.core.logins.clone();
        tokio::spawn(async move {
            let pump = super::logins::pump_logins(lane, &sender);
            super::shared::run_severable(pump, deny, serial, &sender).await;
            logins.deregister_worker_if_current(&worker_id, epoch);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    type WorkerLoginTunnelStream = Pin<
        Box<dyn Stream<Item = Result<horde_proto::v1::WorkerLoginTunnelResponse, Status>> + Send>,
    >;

    #[instrument(skip_all)]
    async fn worker_login_tunnel(
        &self,
        request: Request<Streaming<horde_proto::v1::WorkerLoginTunnelRequest>>,
    ) -> Result<Response<Self::WorkerLoginTunnelStream>, Status> {
        let logins = self.core.logins.clone();
        Ok(super::logins::login_response(
            self.deny.clone(),
            request,
            |message: horde_proto::v1::WorkerLoginTunnelRequest| message.frame.unwrap_or_default(),
            |frame| horde_proto::v1::WorkerLoginTunnelResponse { frame: Some(frame) },
            Box::new(move |caller, io| {
                Box::pin(async move {
                    crate::login::relay::run_worker_login_tunnel(&logins, caller, io).await;
                })
            }),
        ))
    }

    /// The hub tools one Agent may see (ADR-0004's forwarded path): worker
    /// identity from the channel, agent identity from the request, visibility
    /// and placement re-checked in `crate::hub` against the log.
    #[instrument(skip_all)]
    async fn list_hub_tools(
        &self,
        request: Request<ListHubToolsRequest>,
    ) -> Result<Response<ListHubToolsResponse>, Status> {
        let (worker_id, session_id, agent_id) = hub_caller(&request)?;

        let tools_json = crate::hub::list_hub_tools(&self.core, &worker_id, &session_id, &agent_id)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(ListHubToolsResponse { tools_json }))
    }

    /// One Agent's forwarded hub call. The permission chain already gated it
    /// at the Agent's endpoint; this leg is attribution (the placed worker),
    /// visibility (fails closed), and routing to the connected upstream.
    #[instrument(skip_all)]
    async fn call_hub_tool(
        &self,
        request: Request<CallHubToolRequest>,
    ) -> Result<Response<CallHubToolResponse>, Status> {
        let (worker_id, session_id, agent_id) = hub_caller(&request)?;

        let inner = request.get_ref();
        let result_json = crate::hub::call_hub_tool(
            &self.core,
            &worker_id,
            &session_id,
            &agent_id,
            &inner.tool_name,
            &inner.arguments_json,
        )
        .await
        .map_err(Status::from)?;
        Ok(Response::new(CallHubToolResponse { result_json }))
    }

    #[instrument(skip_all)]
    async fn report_capabilities(
        &self,
        request: Request<ReportCapabilitiesRequest>,
    ) -> Result<Response<ReportCapabilitiesResponse>, Status> {
        let worker_id =
            crate::trust::caller_worker_id(&request, request.get_ref().worker_id.as_ref())?;
        let capabilities = request.into_inner().capabilities.unwrap_or_default();
        refuse_foreign_reports(&worker_id, &capabilities.warm_instances)?;

        // Wholesale replacement, per the proto: a partial update is how a
        // scheduler ends up matching on facts that stopped being true.
        register_and_replace(&self.core, &worker_id, capabilities)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(ReportCapabilitiesResponse {}))
    }

    #[instrument(skip_all)]
    async fn report_workspace_instances(
        &self,
        request: Request<ReportWorkspaceInstancesRequest>,
    ) -> Result<Response<ReportWorkspaceInstancesResponse>, Status> {
        let worker_id =
            crate::trust::caller_worker_id(&request, request.get_ref().worker_id.as_ref())?;
        let request = request.into_inner();
        refuse_foreign_reports(&worker_id, &request.reports)?;

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
    let journal = crate::liveness::own_watermark(&core.log, worker_id)
        .await?
        .saturating_add(1);
    let issued = commands::issued_to(&core.log, worker_id).await?;
    Ok((journal, issued.saturating_add(1)))
}

/// Delivers a command stream until the log closes, the fold fails, or the
/// worker is gone.
///
/// The worker going away is noticed through `sender.closed()` — the moment
/// the transport drops the response stream — rather than at the next failed
/// send, which for a quiet worker might be hours away and would hold its
/// registration (and so its agents' placement) hostage to the next command
/// anyone happened to send it (ADR-0011). Cancelling a pending fold read on
/// that branch loses nothing: the stream is ending, and the next one
/// re-folds from the log.
async fn pump_commands(
    stream: &mut CommandStream,
    sender: &tokio::sync::mpsc::Sender<Result<StreamCommandsResponse, Status>>,
) {
    loop {
        let batch = tokio::select! {
            () = sender.closed() => return,
            batch = stream.next_batch() => batch,
        };
        let batch = match batch {
            None => return,
            Some(Ok(batch)) => batch,
            Some(Err(error)) => {
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
}

/// Refuses a worker whose reported journal head is below what the log holds
/// for its current journal — it lost its journal (see [`handle_start`]).
async fn refuse_regressed(
    core: &ServerCore,
    worker_id: &str,
    reported: u64,
) -> Result<u64, ServerError> {
    let stored = crate::liveness::own_watermark(&core.log, worker_id).await?;
    if reported < stored {
        return Err(ServerError::JournalRegressed {
            worker_id: worker_id.to_owned(),
            reported,
            stored,
        });
    }
    Ok(stored)
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
    // A worker reporting a head below what we already stored has lost its
    // journal. Renumbering its events under fresh journal positions would
    // forge history and silently defeat the dedupe index, so this is refused
    // — until an operator resets it (ADR-0011), after which "stored" counts
    // only what its current journal sent.
    let stored = refuse_regressed(core, worker_id, last_local_journal_sequence).await?;

    Ok(PushEventsResponse {
        body: Some(push_events_response::Body::Accepted(PushEventsAccepted {
            resume_from_journal_sequence: stored.saturating_add(1),
        })),
    })
}

/// The identity fields both hub-forwarding request shapes carry, so
/// [`hub_caller`] can read them without a per-handler unpacking dance.
trait HubForwardRequest {
    fn ids(
        &self,
    ) -> (
        Option<&WorkerId>,
        Option<&horde_proto::v1::SessionId>,
        Option<&horde_proto::v1::AgentId>,
    );
}

/// One accessor body for every hub-forwarding request shape: a macro rather
/// than two hand-written impls, so the field wiring cannot drift between
/// them (and the duplication gate has nothing to find).
macro_rules! hub_forward_request {
    ($($request:ty),+ $(,)?) => {$(
        impl HubForwardRequest for $request {
            fn ids(
                &self,
            ) -> (
                Option<&WorkerId>,
                Option<&horde_proto::v1::SessionId>,
                Option<&horde_proto::v1::AgentId>,
            ) {
                (
                    self.worker_id.as_ref(),
                    self.session_id.as_ref(),
                    self.agent_id.as_ref(),
                )
            }
        }
    )+};
}

hub_forward_request!(ListHubToolsRequest, CallHubToolRequest);

/// The attribution triple every hub-forwarding RPC opens with: the worker
/// from the certificate (`crate::trust::caller_worker_id` — a contradicting
/// claim is refused), the session and agent from the request — refused when
/// either is unnamed. An empty worker fails the placement gate downstream
/// rather than being refused here, so the refusal names the agent it was
/// about.
fn hub_caller<T: HubForwardRequest>(
    request: &Request<T>,
) -> Result<(String, String, String), Status> {
    let (claimed, session_id, agent_id) = request.get_ref().ids();
    let worker_id = crate::trust::caller_worker_id(request, claimed)?;
    let session_id = session_id
        .map(ToString::to_string)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            Status::from(ServerError::Invalid(
                "a hub call must name its session".to_owned(),
            ))
        })?;
    let agent_id = agent_id
        .map(ToString::to_string)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| {
            Status::from(ServerError::Invalid(
                "a hub call must name its agent".to_owned(),
            ))
        })?;
    Ok((worker_id, session_id, agent_id))
}

/// The worker a per-worker stream serves: the certificate's, or on a
/// plaintext channel the claim — which must then name someone, because a
/// stream registered under the empty id serves nobody.
///
/// # Errors
///
/// `PermissionDenied` from the identity rule; `InvalidArgument` for a
/// nameless plaintext stream.
fn named_worker<T>(
    request: &Request<T>,
    claimed: Option<&WorkerId>,
    stream: &str,
) -> Result<String, Status> {
    let worker_id = crate::trust::caller_worker_id(request, claimed)?;
    if worker_id.is_empty() {
        return Err(Status::from(ServerError::Invalid(format!(
            "a {stream} stream must name its worker"
        ))));
    }
    Ok(worker_id)
}

/// Refuses instance reports that name a worker other than the caller.
///
/// The instance cache is keyed by the worker a report *names*, so a report
/// the caller could address to another worker would rewrite that worker's
/// warmth — and placement prefers warm workers. An unnamed report keys
/// nothing and is let through.
///
/// # Errors
///
/// `PermissionDenied` naming both workers.
fn refuse_foreign_reports(
    worker_id: &str,
    reports: &[horde_proto::v1::WorkspaceInstanceReport],
) -> Result<(), Status> {
    match reports
        .iter()
        .filter_map(|report| report.worker_id.as_ref().map(ToString::to_string))
        .find(|named| !named.is_empty() && named != worker_id)
    {
        Some(named) => Err(Status::permission_denied(format!(
            "worker {worker_id:?} may not report instances for worker {named:?}"
        ))),
        None => Ok(()),
    }
}

/// How many queued notifications one recompute may absorb. A bound, not a
/// deadline: the drain is by count, and anything past it simply wakes the
/// next iteration.
const NOTIFICATION_DRAIN: u32 = 256;

/// The forward stream's life: push the current intent set, then re-push on
/// every intent-moving append and relay connection requests as they arrive.
async fn pump_forwards(
    core: &ServerCore,
    worker_id: &str,
    mut lane: tokio::sync::mpsc::Receiver<horde_proto::v1::ForwardConnectionRequest>,
    mut notifications: tokio::sync::broadcast::Receiver<crate::log::Notification>,
    sender: &tokio::sync::mpsc::Sender<Result<horde_proto::v1::StreamForwardsResponse, Status>>,
) {
    // The first send is unconditional, empty set included: a reconnecting
    // worker must learn "you have no intents" to drop stale listeners.
    let mut last_sent: Option<Vec<horde_proto::v1::ForwardIntent>> = None;
    if !push_intents(core, worker_id, &mut last_sent, sender).await {
        return;
    }
    loop {
        tokio::select! {
            // The worker hung up. Noticed here rather than on the next failed
            // send, because deregistration is what turns tunnel opens into
            // fast WORKER_UNREACHABLE refusals instead of pairing-deadline
            // stalls.
            () = sender.closed() => return,
            connection = lane.recv() => {
                // A closed lane means a newer stream replaced this one; the
                // replacement carries the intents from here on.
                let Some(connection) = connection else { return };
                let update = horde_proto::v1::StreamForwardsResponse {
                    update: Some(horde_proto::v1::stream_forwards_response::Update::Connection(
                        connection,
                    )),
                };
                if sender.send(Ok(update)).await.is_err() {
                    return;
                }
            }
            first = notifications.recv() => {
                let mut notes = Vec::new();
                let lagged_first = match first {
                    Ok(note) => {
                        notes.push(note);
                        false
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    // Lagged: the hint channel overflowed. The log is the
                    // truth, so recompute unconditionally.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => true,
                };
                let lagged = drain_notifications(&mut notifications, &mut notes) || lagged_first;
                // A lag means rows were missed, so the filter cannot vouch
                // for them; a read error is treated the same, conservatively.
                let recompute = lagged
                    || super::forwards::any_affects_forwards(core, &notes)
                        .await
                        .unwrap_or(true);
                if recompute && !push_intents(core, worker_id, &mut last_sent, sender).await {
                    return;
                }
            }
        }
    }
}

/// Drains queued notifications into `notes`, count-bounded. Returns whether
/// the drain itself demands a recompute — it lagged, so rows were missed and
/// the filter cannot vouch for them.
fn drain_notifications(
    notifications: &mut tokio::sync::broadcast::Receiver<crate::log::Notification>,
    notes: &mut Vec<crate::log::Notification>,
) -> bool {
    use tokio::sync::broadcast::error::TryRecvError;
    let mut lagged = false;
    for _ in 0..NOTIFICATION_DRAIN {
        match notifications.try_recv() {
            Ok(note) => notes.push(note),
            Err(TryRecvError::Lagged(_)) => lagged = true,
            Err(_) => break,
        }
    }
    lagged
}

/// Recomputes and pushes the intent set if it changed since the last push.
/// Returns whether the stream is still worth pumping.
async fn push_intents(
    core: &ServerCore,
    worker_id: &str,
    last_sent: &mut Option<Vec<horde_proto::v1::ForwardIntent>>,
    sender: &tokio::sync::mpsc::Sender<Result<horde_proto::v1::StreamForwardsResponse, Status>>,
) -> bool {
    let mut intents = match crate::forward::intent::intents_for_worker(&core.log, worker_id).await {
        Ok(intents) => intents,
        Err(error) => {
            let _ = sender.send(Err(Status::from(error))).await;
            return false;
        }
    };
    // Live logins' callback exposes ride every push (DESIGN §4): the worker
    // replaces its dialable map wholesale per set, so a merge done in only
    // some pushes would flap the callback route mid-login. Merged from the
    // registry, not the log — ephemeral intent has no journaled fold.
    intents.extend(core.logins.callback_intents_for(worker_id));
    intents.sort_by_key(|intent| {
        intent
            .forward_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default()
    });
    if last_sent.as_ref() == Some(&intents) {
        return true;
    }
    let update = horde_proto::v1::StreamForwardsResponse {
        update: Some(horde_proto::v1::stream_forwards_response::Update::Intents(
            horde_proto::v1::ForwardIntentSet {
                intents: intents.clone(),
            },
        )),
    };
    let delivered = sender.send(Ok(update)).await.is_ok();
    *last_sent = Some(intents);
    delivered
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, Event, JournaledEvent, SessionId, TurnInterrupted, event::Kind,
    };

    use super::super::shared::ServerCore;
    use super::handle_start;
    use crate::EventLog;
    use crate::error::ServerError;

    /// Ingests through the production path as `worker-a`, with `agent-1`
    /// placed on it in `s1` first so the entries are admissible.
    async fn ingest(core: &ServerCore, events: Vec<JournaledEvent>) {
        super::super::ingest::placed_for_test(core, "s1", "agent-1", "worker-a").await;
        super::super::ingest::ingest_batch(
            core,
            &mut super::super::ingest::OriginView::new(),
            "worker-a",
            events,
        )
        .await
        .expect("ingest");
    }

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
                last_local_journal_sequence: None,
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

    fn registration(reported: Option<u64>) -> tonic::Request<horde_proto::v1::RegisterWorkerRequest> {
        tonic::Request::new(horde_proto::v1::RegisterWorkerRequest {
            hello: Some(horde_proto::v1::Hello {
                semver: "0.1.0".to_owned(),
                ..horde_proto::v1::Hello::default()
            }),
            worker_id: Some(horde_proto::v1::WorkerId::new("worker-a")),
            capabilities: Some(horde_proto::v1::WorkerCapabilities::default()),
            last_local_journal_sequence: reported,
        })
    }

    /// A worker reporting a journal head below what the log stored is
    /// refused at registration, naming the reset (ADR-0011); after the
    /// reset, its fresh journal registers and resumes from one.
    #[tokio::test]
    async fn a_regressed_journal_is_refused_at_registration_until_reset() {
        use horde_proto::v1::worker_service_server::WorkerService as _;
        let core = core();
        ingest(
            &core,
            vec![journaled("s1", 1, "k1"), journaled("s1", 2, "k2")],
        )
        .await;
        let facing = super::WorkerFacing::new(core.clone());

        let refused = facing
            .register_worker(registration(Some(0)))
            .await
            .expect_err("a lost journal is refused before any stream opens");
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
        assert!(
            refused.message().contains("horde worker reset"),
            "the refusal names the recovery: {}",
            refused.message()
        );
        assert!(
            facing.register_worker(registration(Some(2))).await.is_ok(),
            "a worker at the stored head registers"
        );
        assert!(
            facing.register_worker(registration(None)).await.is_ok(),
            "an older worker reporting nothing is judged at PushEventsStart"
        );

        crate::liveness::reset_worker_journal(&core, "worker-a", "disk", None)
            .await
            .expect("reset");
        let registered = facing
            .register_worker(registration(Some(0)))
            .await
            .expect("the fresh journal registers after the reset")
            .into_inner();
        assert_eq!(
            registered.resume_from_journal_sequence, 1,
            "the fresh journal resumes from its own first position"
        );
    }

    /// The command stream's end is noticed the moment the transport drops
    /// it — no command needs to fail — and the lease that starts then ends,
    /// unanswered, in the worker's started agent declared lost (ADR-0011).
    #[tokio::test]
    async fn a_dropped_command_stream_starts_the_lease_that_loses_the_worker() {
        use horde_proto::v1::worker_service_server::WorkerService as _;
        let core = core().with_worker_lease(std::time::Duration::from_millis(1));
        let facing = super::WorkerFacing::new(core.clone());
        facing
            .register_worker(registration(None))
            .await
            .expect("registered");
        super::super::ingest::placed_for_test(&core, "s1", "agent-1", "worker-a").await;
        crate::service::shared::append_server_event(
            &core.log,
            "s1",
            "",
            Event {
                agent_id: Some(AgentId::new("agent-1")),
                kind: Some(Kind::AgentLifecycleChanged(
                    horde_proto::v1::AgentLifecycleChanged {
                        state: horde_proto::v1::AgentLifecycleState::Running as i32,
                        ..Default::default()
                    },
                )),
                ..Event::default()
            },
        )
        .await
        .expect("the lifecycle appends");

        let stream = facing
            .stream_commands(tonic::Request::new(
                horde_proto::v1::StreamCommandsRequest {
                    worker_id: Some(horde_proto::v1::WorkerId::new("worker-a")),
                    resume_from_command_sequence: 1,
                    resume_after_log_position: 0,
                },
            ))
            .await
            .expect("the stream opens");
        drop(stream);

        let lost = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let events = core.log.read_all("s1").await.expect("read");
                let state = crate::state::SessionState::project("s1", &events).expect("project");
                if state.agents["agent-1"].placement_state
                    == horde_proto::v1::PlacementState::Lost
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(lost.is_ok(), "the lapsed lease must declare the agent lost");
        assert!(!core.workers.contains("worker-a"), "and the worker left placement");
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

    /// Both resume positions come from the log, so a fresh worker starts both
    /// streams at one, and a worker with stored history resumes after it.
    #[tokio::test]
    async fn resume_positions_are_folded_from_the_log_not_remembered() {
        let core = core();
        let (journal, command) = super::resume_positions(&core, "worker-a")
            .await
            .expect("positions");
        assert_eq!((journal, command), (1, 1), "a fresh worker starts at one");

        ingest(&core, vec![journaled("s1", 1, "k1")]).await;
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
        ingest(&core, vec![journaled("s1", 1, "k1")]).await;

        let response = handle_start(&core, "worker-a", 1).await.expect("start");
        match response.body.expect("a body") {
            horde_proto::v1::push_events_response::Body::Accepted(accepted) => {
                assert_eq!(accepted.resume_from_journal_sequence, 2);
            }
            other => panic!("expected Accepted, got {other:?}"),
        }
    }

    /// A per-worker stream must end up naming someone: the certificate's
    /// worker, or the plaintext claim — and a nameless plaintext stream is
    /// refused rather than registered under the empty id.
    #[test]
    fn a_per_worker_stream_must_name_its_worker() {
        use horde_identity::PrincipalRole;
        use horde_proto::v1::WorkerId;

        let claimed = WorkerId::new("w9");
        let plain = tonic::Request::new(());
        assert_eq!(
            super::named_worker(&plain, Some(&claimed), "login").ok(),
            Some("w9".to_owned())
        );
        let nameless = super::named_worker(&plain, None, "login")
            .expect_err("a nameless plaintext stream serves nobody");
        assert_eq!(nameless.code(), tonic::Code::InvalidArgument);
        assert!(nameless.message().contains("login"), "{nameless}");

        let certified = as_peer((), Some(PrincipalRole::Worker));
        assert_eq!(
            super::named_worker(&certified, None, "forward").ok(),
            Some("gpu-box".to_owned())
        );
    }

    /// Instance reports may name the caller or nobody; naming another
    /// worker is refused, since the cache keys by the named worker.
    #[test]
    fn instance_reports_naming_another_worker_are_refused() {
        use horde_proto::v1::{WorkerId, WorkspaceInstanceReport};

        let report = |worker: Option<&str>| WorkspaceInstanceReport {
            worker_id: worker.map(WorkerId::new),
            ..WorkspaceInstanceReport::default()
        };
        assert!(super::refuse_foreign_reports("w1", &[report(Some("w1")), report(None)]).is_ok());
        assert!(super::refuse_foreign_reports("w1", &[report(Some(""))]).is_ok());
        let refused =
            super::refuse_foreign_reports("w1", &[report(Some("w1")), report(Some("w2"))])
                .expect_err("a report for another worker is refused");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        assert!(refused.message().contains("w2"), "{refused}");
    }

    /// A request with an optional peer identity stamped, the way the mTLS
    /// interceptor stamps one — shared by every principal-resolution test.
    fn as_peer<T>(body: T, identity: Option<horde_identity::PrincipalRole>) -> tonic::Request<T> {
        let mut request = tonic::Request::new(body);
        if let Some(role) = identity {
            request
                .extensions_mut()
                .insert(horde_identity::PeerIdentity {
                    principal: horde_identity::Principal::new(role, "gpu-box")
                        .expect("a valid name"),
                    serial_hex: "0a".to_owned(),
                });
        }
        request
    }

    /// The hub-forwarding preamble's whole table: the certificate outranks
    /// the claim (a client cert names no worker), and a call naming no
    /// session or no agent is refused before anything reads the log.
    #[test]
    fn a_hub_call_takes_its_worker_from_cert_then_claim_and_demands_both_ids() {
        use horde_identity::PrincipalRole;
        use horde_proto::v1::{AgentId, CallHubToolRequest, SessionId, WorkerId};

        let body =
            |worker: Option<&str>, session: Option<&str>, agent: Option<&str>| CallHubToolRequest {
                worker_id: worker.map(WorkerId::new),
                session_id: session.map(SessionId::new),
                agent_id: agent.map(AgentId::new),
                ..CallHubToolRequest::default()
            };
        let complete = || body(Some("claimed"), Some("s1"), Some("agent-1"));

        let from_cert = super::hub_caller(&as_peer(
            body(None, Some("s1"), Some("agent-1")),
            Some(PrincipalRole::Worker),
        ))
        .expect("a complete call passes");
        assert_eq!(
            from_cert,
            ("gpu-box".to_owned(), "s1".to_owned(), "agent-1".to_owned()),
            "the certificate names the worker"
        );
        let forged = super::hub_caller(&as_peer(complete(), Some(PrincipalRole::Worker)))
            .expect_err("a claim contradicting the certificate is refused");
        assert_eq!(forged.code(), tonic::Code::PermissionDenied);
        let from_claim =
            super::hub_caller(&as_peer(complete(), None)).expect("plaintext bootstrap");
        assert_eq!(from_claim.0, "claimed");
        let client_cert = super::hub_caller(&as_peer(complete(), Some(PrincipalRole::Client)))
            .expect_err("a client cannot speak for a worker");
        assert_eq!(client_cert.code(), tonic::Code::PermissionDenied);

        let no_session =
            super::hub_caller(&as_peer(body(Some("claimed"), None, Some("agent-1")), None))
                .expect_err("a call naming no session is refused");
        assert_eq!(no_session.code(), tonic::Code::InvalidArgument);
        assert!(no_session.message().contains("session"), "{no_session}");

        let no_agent = super::hub_caller(&as_peer(body(Some("claimed"), Some("s1"), None), None))
            .expect_err("a call naming no agent is refused");
        assert_eq!(no_agent.code(), tonic::Code::InvalidArgument);
        assert!(no_agent.message().contains("agent"), "{no_agent}");

        let anonymous = super::hub_caller(&as_peer(body(None, Some("s1"), Some("agent-1")), None))
            .expect("no worker anywhere is an empty id, refused by the placement gate");
        assert_eq!(anonymous.0, "");

        // Both request shapes read the same fields; the listing shape's
        // accessor is pinned here so a field mix-up in either impl fails.
        let listing = horde_proto::v1::ListHubToolsRequest {
            worker_id: Some(WorkerId::new("claimed")),
            session_id: Some(SessionId::new("s1")),
            agent_id: Some(AgentId::new("agent-1")),
        };
        let listed = super::hub_caller(&as_peer(listing, None)).expect("the listing shape passes");
        assert_eq!(
            listed,
            ("claimed".to_owned(), "s1".to_owned(), "agent-1".to_owned())
        );
    }

    /// The hub-forwarding RPCs themselves, over a real upstream child: the
    /// listing carries the agent's namespaced toolset and the call carries
    /// the upstream's own answer — neither is a defaultable empty response.
    #[tokio::test]
    async fn the_hub_rpcs_answer_with_the_real_toolset_and_result() {
        use horde_proto::v1::worker_service_server::WorkerService as _;
        use horde_proto::v1::{AgentId, SessionId, WorkerId};

        let core = core();
        crate::forward::testfix::placed_expose(&core.log, "s1", "agent-1", "w1").await;
        crate::hub::testfix::seed_catalog(
            &core.log,
            crate::hub::testfix::stub_config("stub", "hub", false, &[]).await,
        )
        .await;
        let facing = super::WorkerFacing::new(core);

        let listed = facing
            .list_hub_tools(tonic::Request::new(horde_proto::v1::ListHubToolsRequest {
                worker_id: Some(WorkerId::new("w1")),
                session_id: Some(SessionId::new("s1")),
                agent_id: Some(AgentId::new("agent-1")),
            }))
            .await
            .expect("the listing answers")
            .into_inner();
        assert!(
            listed
                .tools_json
                .iter()
                .any(|json| json.contains("stub__echo")),
            "the RPC must carry the agent's real toolset: {:?}",
            listed.tools_json
        );

        let called = facing
            .call_hub_tool(tonic::Request::new(horde_proto::v1::CallHubToolRequest {
                worker_id: Some(WorkerId::new("w1")),
                session_id: Some(SessionId::new("s1")),
                agent_id: Some(AgentId::new("agent-1")),
                tool_name: "stub__echo".to_owned(),
                arguments_json: r#"{"message":"over the spine"}"#.to_owned(),
            }))
            .await
            .expect("the call answers")
            .into_inner();
        assert!(
            called.result_json.contains("echo:over the spine"),
            "the RPC must carry the upstream's own answer: {}",
            called.result_json
        );
    }

    /// One agent placed on `worker`, exposing — the pump tests' log fixture.
    async fn forward_core(worker: &str) -> ServerCore {
        let core = ServerCore::new(crate::log::EventLog::in_memory().expect("log"));
        crate::forward::testfix::placed_expose(&core.log, "s1", "agent-1", worker).await;
        core
    }

    fn intents_of(
        update: &horde_proto::v1::StreamForwardsResponse,
    ) -> Option<&horde_proto::v1::ForwardIntentSet> {
        match update.update.as_ref() {
            Some(horde_proto::v1::stream_forwards_response::Update::Intents(set)) => Some(set),
            _ => None,
        }
    }

    /// The pump's opening move and its whole life: the current set arrives
    /// first (even before any log movement), and a connection request pushed
    /// down the lane is relayed after it — which a pump that returned after
    /// its first successful push (the deleted-`!` mutant) never does.
    #[tokio::test]
    async fn the_forward_pump_sends_the_set_then_relays_connections() {
        let core = forward_core("w1").await;
        let (epoch, lane) = core.forwards.register_worker("w1");
        let lane_feed = core
            .forwards
            .lane_to(&crate::forward::registry::PassiveTarget::Worker(
                "w1".to_owned(),
            ))
            .expect("the lane is registered");
        let notifications = core.log.subscribe();
        let (sender, mut updates) = tokio::sync::mpsc::channel(8);

        let pump_core = core.clone();
        let pump = tokio::spawn(async move {
            super::pump_forwards(&pump_core, "w1", lane, notifications, &sender).await;
        });

        let first = tokio::time::timeout(std::time::Duration::from_secs(5), updates.recv())
            .await
            .expect("the set must arrive")
            .expect("an update")
            .expect("an ok update");
        let set = intents_of(&first).expect("the first update is the whole set");
        assert_eq!(set.intents.len(), 1);
        assert_eq!(
            set.intents[0].forward_id.as_ref().map(ToString::to_string),
            Some("fwd-agent-1".to_owned())
        );

        lane_feed
            .try_send(horde_proto::v1::ForwardConnectionRequest {
                forward_id: Some(horde_proto::v1::ForwardId::new("fwd-agent-1")),
                connection_id: "c1".to_owned(),
            })
            .expect("the lane takes the request");
        let second = tokio::time::timeout(std::time::Duration::from_secs(5), updates.recv())
            .await
            .expect("the connection must arrive")
            .expect("an update")
            .expect("an ok update");
        match second.update {
            Some(horde_proto::v1::stream_forwards_response::Update::Connection(request)) => {
                assert_eq!(request.connection_id, "c1");
            }
            other => panic!("expected the relayed connection, got {other:?}"),
        }

        pump.abort();
        core.forwards.deregister_worker_if_current("w1", epoch);
    }

    /// The drain is a bounded collector and a lag detector, both directions:
    /// an intact queue drains false, an overflowed one reports the lag that
    /// forces an unconditional recompute.
    #[test]
    fn draining_notifications_collects_and_reports_lag() {
        let (sender, mut intact) = tokio::sync::broadcast::channel(16);
        sender
            .send(crate::log::Notification {
                session_id: "s1".to_owned(),
                sequence: 1,
            })
            .expect("send");
        let mut notes = Vec::new();
        assert!(
            !super::drain_notifications(&mut intact, &mut notes),
            "nothing was missed"
        );
        assert_eq!(notes.len(), 1, "the queued note was collected");

        let (sender, mut lagged) = tokio::sync::broadcast::channel(1);
        for sequence in 1..=3 {
            sender
                .send(crate::log::Notification {
                    session_id: "s1".to_owned(),
                    sequence,
                })
                .expect("send");
        }
        let mut notes = Vec::new();
        assert!(
            super::drain_notifications(&mut lagged, &mut notes),
            "an overflowed hint channel cannot vouch for what it dropped"
        );
    }

    /// The push's table: a first push sends and returns true; an unchanged
    /// recompute sends nothing (the `==` the resend mutant flips); a closed
    /// peer returns false, which is what ends the pump.
    #[tokio::test]
    async fn push_intents_sends_changes_only_and_reports_a_dead_peer() {
        let core = forward_core("w1").await;
        let (sender, mut updates) = tokio::sync::mpsc::channel(8);
        let mut last_sent = None;

        assert!(super::push_intents(&core, "w1", &mut last_sent, &sender).await);
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), updates.recv())
            .await
            .expect("the set arrives")
            .expect("an update")
            .expect("ok");
        assert_eq!(
            intents_of(&first).map(|set| set.intents.len()),
            Some(1),
            "the first push carries the whole set"
        );

        assert!(
            super::push_intents(&core, "w1", &mut last_sent, &sender).await,
            "an unchanged set is still a healthy stream"
        );
        assert!(
            updates.try_recv().is_err(),
            "an unchanged set must not be re-sent"
        );

        drop(updates);
        assert!(
            !super::push_intents(&core, "w2", &mut last_sent, &sender).await,
            "a hung-up peer ends the pump"
        );
    }

    /// Renumbering would forge history and defeat the dedupe index, so a worker
    /// that lost its journal is refused rather than accommodated.
    #[tokio::test]
    async fn a_worker_that_lost_its_journal_is_refused() {
        let core = core();
        ingest(
            &core,
            (1..=5)
                .map(|journal_sequence| {
                    journaled("s1", journal_sequence, &format!("k{journal_sequence}"))
                })
                .collect(),
        )
        .await;

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
