//! The client-facing control plane.
//!
//! Every write here becomes an event before it becomes anything else, and every
//! read is a projection of the log. `SyncEvents` is the resilience primitive:
//! attach, scrollback, reconnect, and the multi-session overview are the same
//! call with a different offset.
//!
//! RPCs whose backend does not exist yet return `Unimplemented` rather than a
//! fabricated success. A client that receives an empty workspace list cannot
//! tell "there are none" from "this is not built"; a client that receives
//! `Unimplemented` can.

use std::pin::Pin;

use horde_proto::v1::{
    AddWorkspaceRepoRequest, AddWorkspaceRepoResponse, AgentId, AgentSpawned,
    AnswerPermissionRequest, AnswerPermissionResponse, ArchiveSessionRequest,
    ArchiveSessionResponse, AssignmentId, AssignmentSent, CreateSessionRequest,
    CreateSessionResponse, CreateWorkspaceDefinitionRequest, CreateWorkspaceDefinitionResponse,
    DeleteWorkspaceDefinitionRequest, DeleteWorkspaceDefinitionResponse, Event, EventBatch,
    GetSessionRequest, GetSessionResponse, GetWorkspaceDefinitionRequest,
    GetWorkspaceDefinitionResponse, HelloRequest, HelloResponse, InterruptAgentRequest,
    InterruptAgentResponse, ListSessionsRequest, ListSessionsResponse, ListWorkersRequest,
    ListWorkersResponse, ListWorkspaceDefinitionsRequest, ListWorkspaceDefinitionsResponse,
    MintEnrollmentCodeRequest, MintEnrollmentCodeResponse, NoteSent, PrewarmWorkspaceRequest,
    PrewarmWorkspaceResponse, QueryUsageRequest, QueryUsageResponse, RevokeEnrollmentRequest,
    RevokeEnrollmentResponse, SendMessageRequest, SendMessageResponse, ServerHello, SessionId,
    SessionSummary, SpawnAgentRequest, SpawnAgentResponse, SyncEventsRequest, SyncEventsResponse,
    UpdateWorkspaceDefinitionRequest, UpdateWorkspaceDefinitionResponse,
    control_plane_service_server::ControlPlaneService, event::Kind, send_message_request,
    sync_events_response,
};
use prost::Message;
use tokio::sync::mpsc::Sender;
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tracing::instrument;

use super::shared::{MIN_SUPPORTED_SEMVER, SERVER_SEMVER, ServerCore};
use crate::error::ServerError;
use crate::log::{PendingEvent, StoredEvent, Tail};
use crate::scheduler::Placement;
use crate::state::SessionState;

/// Implements `horde.v1.ControlPlaneService`.
#[derive(Clone, Debug)]
pub struct ControlPlane {
    core: ServerCore,
}

impl ControlPlane {
    #[must_use]
    pub fn new(core: ServerCore) -> Self {
        Self { core }
    }

    /// Appends a server-originated event to a session.
    /// Appends an event that belongs to one agent and carries no idempotency
    /// key — the shape every client-initiated agent action has.
    async fn append_agent_event(
        &self,
        session_id: &str,
        agent_id: String,
        kind: Kind,
    ) -> Result<u64, ServerError> {
        self.append(
            session_id,
            "",
            Event {
                agent_id: Some(AgentId::new(agent_id)),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(kind),
            },
        )
        .await
    }

    async fn append(
        &self,
        session_id: &str,
        idempotency_key: &str,
        event: Event,
    ) -> Result<u64, ServerError> {
        let appended = self
            .core
            .log
            .append(PendingEvent {
                session_id: session_id.to_owned(),
                origin_worker_id: String::new(),
                journal_sequence: None,
                idempotency_key: idempotency_key.to_owned(),
                payload: event.encode_to_vec(),
            })
            .await?;
        Ok(appended.sequence)
    }

    /// Projects a session, refusing one that has no events.
    async fn project(&self, session_id: &str) -> Result<SessionState, ServerError> {
        let events = self.core.log.read_all(session_id).await?;
        if events.is_empty() {
            return Err(ServerError::NoSuchSession {
                session_id: session_id.to_owned(),
            });
        }
        SessionState::project(session_id, &events)
    }

    /// Finds which session an agent belongs to.
    ///
    /// A scan across sessions today. It is correct and it is honest about being
    /// the naive implementation; an agent→session index is a wave-4 concern
    /// once there are enough sessions for it to matter.
    async fn session_of_agent(&self, agent_id: &str) -> Result<String, ServerError> {
        for session_id in self.core.log.session_ids().await? {
            let state = self.project(&session_id).await?;
            if state.agents.contains_key(agent_id) {
                return Ok(session_id);
            }
        }
        Err(ServerError::NoSuchAgent {
            agent_id: agent_id.to_owned(),
        })
    }

    fn server_hello() -> ServerHello {
        ServerHello {
            semver: SERVER_SEMVER.to_owned(),
            capability_flags: Vec::new(),
            min_supported_semver: MIN_SUPPORTED_SEMVER.to_owned(),
            server_time: None,
            principal: String::new(),
        }
    }

    fn summary(state: &SessionState) -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new(state.session_id.clone())),
            title: String::new(),
            root_agent_id: state.root_agent_id.clone().map(AgentId::new),
            workspace_definition_id: None,
            state: state.lifecycle as i32,
            created_at: None,
            head_sequence: state.head_sequence,
            agent_count: u32::try_from(state.agents.len()).unwrap_or(u32::MAX),
        }
    }
}

fn envelope(event: &StoredEvent) -> Result<horde_proto::v1::EventEnvelope, ServerError> {
    Ok(horde_proto::v1::EventEnvelope {
        session_id: Some(SessionId::new(event.session_id.clone())),
        sequence: event.sequence,
        recorded_at: Some(recorded_at(event.recorded_at_unix_ms)),
        event: Some(Event::decode(event.payload.as_slice())?),
    })
}

/// Converts the log's stored milliseconds into a wire timestamp.
///
/// The log has always recorded this; it was never put on the wire, so a client
/// could not render when anything happened or reconcile clock skew against the
/// server (DESIGN.md §2). Mutation testing found it: the stored value had no
/// reader, so replacing the clock with a constant changed nothing observable.
fn recorded_at(unix_ms: i64) -> prost_types::Timestamp {
    // Floor division, so a pre-epoch reading yields a negative second and a
    // non-negative nanosecond rather than a timestamp that reads as later than
    // it was.
    let seconds = unix_ms.div_euclid(1_000);
    let nanos = i32::try_from(unix_ms.rem_euclid(1_000)).unwrap_or(0) * 1_000_000;
    prost_types::Timestamp { seconds, nanos }
}

/// Whether the stream ends once history has been delivered.
///
/// Two independent reasons: the client hung up, or it never asked to follow.
/// Extracted because inline they are not separately reachable — a
/// non-following client also drops the stream immediately afterwards, so `&&`
/// still returns in every end-to-end run, and dropping the `!` merely inverts
/// which of two paths a passing test takes.
fn stop_after_caught_up(send_failed: bool, follow: bool) -> bool {
    send_failed || !follow
}

/// One sync stream: history, a caught-up marker, then the live tail.
///
/// Split out of `sync_events` so the RPC is only about resolving the cursor,
/// and so history and tail deliver through one code path rather than two
/// copies of the same envelope-and-send loop.
async fn stream_session(
    sender: Sender<Result<SyncEventsResponse, Status>>,
    session_id: String,
    history: Vec<StoredEvent>,
    mut tail: Tail,
    follow: bool,
) {
    if !send_batch(&sender, &history).await {
        return;
    }

    // Caught up. A TUI stops showing a loading state here rather than guessing
    // when history ended.
    let caught_up = SyncEventsResponse {
        update: Some(sync_events_response::Update::CaughtUp(
            horde_proto::v1::SyncCaughtUp {
                heads: vec![horde_proto::v1::SessionCursor {
                    session_id: Some(SessionId::new(session_id)),
                    from_sequence: tail.delivered(),
                }],
            },
        )),
    };
    if stop_after_caught_up(sender.send(Ok(caught_up)).await.is_err(), follow) {
        return;
    }

    while let Some(next) = tail.next_batch().await {
        let events = match next {
            Ok(events) => events,
            Err(error) => {
                let _ = sender.send(Err(Status::from(error))).await;
                return;
            }
        };
        if !send_batch(&sender, &events).await {
            return;
        }
    }
}

/// Envelopes and sends one batch. `false` means the stream is finished — the
/// receiver hung up, or an event would not decode and the error has been sent.
async fn send_batch(
    sender: &Sender<Result<SyncEventsResponse, Status>>,
    events: &[StoredEvent],
) -> bool {
    let batch: Result<Vec<_>, _> = events.iter().map(envelope).collect();
    let batch = match batch {
        Ok(batch) => batch,
        Err(error) => {
            let _ = sender.send(Err(Status::from(error))).await;
            return false;
        }
    };
    if batch.is_empty() {
        return true;
    }
    let response = SyncEventsResponse {
        update: Some(sync_events_response::Update::Batch(EventBatch {
            events: batch,
        })),
    };
    sender.send(Ok(response)).await.is_ok()
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlane {
    #[instrument(skip_all)]
    async fn hello(
        &self,
        _request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        Ok(Response::new(HelloResponse {
            server: Some(Self::server_hello()),
            // Assigned from the client certificate once enrollment exists.
            client_id: None,
        }))
    }

    #[instrument(skip_all)]
    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<CreateSessionResponse>, Status> {
        let request = request.into_inner();

        // The session id and root agent id are server-assigned. Derived from the
        // idempotency key when one is supplied, so a retry after a lost response
        // lands on the same session rather than creating a second one.
        let session_id = if request.idempotency_key.is_empty() {
            format!("session-{}", uuid_like())
        } else {
            format!("session-{}", request.idempotency_key)
        };
        let root_agent_id = format!("{session_id}-root");

        let spawned = Event {
            agent_id: Some(AgentId::new(root_agent_id.clone())),
            occurred_at: None,
            idempotency_key: request.idempotency_key.clone(),
            kind: Some(Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: None,
                spawned_by: None,
                spec: request.root_agent,
                tree_path: vec![request.title.clone()],
            })),
        };

        self.append(&session_id, &request.idempotency_key, spawned)
            .await
            .map_err(Status::from)?;

        let state = self.project(&session_id).await.map_err(Status::from)?;
        tracing::info!(session_id, root_agent_id, "session created");

        Ok(Response::new(CreateSessionResponse {
            session: Some(Self::summary(&state)),
            root_agent_id: Some(AgentId::new(root_agent_id)),
        }))
    }

    #[instrument(skip_all)]
    async fn list_sessions(
        &self,
        request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let include_archived = request.into_inner().include_archived;

        let mut sessions = Vec::new();
        for session_id in self.core.log.session_ids().await.map_err(Status::from)? {
            let state = self.project(&session_id).await.map_err(Status::from)?;
            let archived = state.lifecycle == horde_proto::v1::SessionLifecycleState::Archived;
            if archived && !include_archived {
                continue;
            }
            sessions.push(Self::summary(&state));
        }

        Ok(Response::new(ListSessionsResponse {
            sessions,
            // Paging lands when there are enough sessions to need it; an empty
            // token honestly means "that was all of them".
            next_page_token: String::new(),
        }))
    }

    #[instrument(skip_all)]
    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let session_id = request
            .into_inner()
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let state = self.project(&session_id).await.map_err(Status::from)?;

        Ok(Response::new(GetSessionResponse {
            session: Some(Self::summary(&state)),
            agents: Vec::new(),
        }))
    }

    #[instrument(skip_all)]
    async fn archive_session(
        &self,
        request: Request<ArchiveSessionRequest>,
    ) -> Result<Response<ArchiveSessionResponse>, Status> {
        let request = request.into_inner();
        let session_id = request
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        self.project(&session_id).await.map_err(Status::from)?;

        let event = Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::SessionLifecycleChanged(
                horde_proto::v1::SessionLifecycleChanged {
                    previous_state: horde_proto::v1::SessionLifecycleState::Active as i32,
                    state: horde_proto::v1::SessionLifecycleState::Archived as i32,
                    reason: request.reason,
                },
            )),
        };
        self.append(&session_id, "", event)
            .await
            .map_err(Status::from)?;

        let state = self.project(&session_id).await.map_err(Status::from)?;
        Ok(Response::new(ArchiveSessionResponse {
            session: Some(Self::summary(&state)),
        }))
    }

    type SyncEventsStream = Pin<Box<dyn Stream<Item = Result<SyncEventsResponse, Status>> + Send>>;

    /// The resilience primitive: replay from an offset, then follow.
    #[instrument(skip_all)]
    async fn sync_events(
        &self,
        request: Request<SyncEventsRequest>,
    ) -> Result<Response<Self::SyncEventsStream>, Status> {
        let request = request.into_inner();

        // One session per call for now. Multi-cursor and `all_sessions` need a
        // merged tail, which is worth building against a real client rather
        // than guessing at now.
        let cursor = request.cursors.first().cloned().ok_or_else(|| {
            Status::from(ServerError::Invalid(
                "sync requires at least one cursor; all_sessions is not implemented yet".to_owned(),
            ))
        })?;
        let session_id = cursor
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        let (history, tail) = self
            .core
            .log
            .tail(&session_id, cursor.from_sequence)
            .await
            .map_err(Status::from)?;

        let follow = request.follow;
        let (sender, receiver) = tokio::sync::mpsc::channel(64);

        tokio::spawn(stream_session(sender, session_id, history, tail, follow));

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    #[instrument(skip_all)]
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let request = request.into_inner();
        let agent_id = request
            .to_agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let session_id = self
            .session_of_agent(&agent_id)
            .await
            .map_err(Status::from)?;

        let (kind, assignment_id) = match request.body {
            Some(send_message_request::Body::Assignment(mut assignment)) => {
                let id = AssignmentId::new(format!("assignment-{}", uuid_like()));
                assignment.assignment_id = Some(id.clone());
                (
                    Kind::AssignmentSent(AssignmentSent {
                        from: None,
                        assignment: Some(assignment),
                        delivery: request.delivery,
                    }),
                    Some(id),
                )
            }
            Some(send_message_request::Body::Note(note)) => (
                Kind::NoteSent(NoteSent {
                    from: None,
                    note: Some(note),
                    delivery: request.delivery,
                }),
                None,
            ),
            None => {
                return Err(Status::from(ServerError::Invalid(
                    "a message must be an assignment or a note".to_owned(),
                )));
            }
        };

        let sequence = self
            .append(
                &session_id,
                &request.idempotency_key,
                Event {
                    agent_id: Some(AgentId::new(agent_id)),
                    occurred_at: None,
                    idempotency_key: request.idempotency_key.clone(),
                    kind: Some(kind),
                },
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(SendMessageResponse {
            assignment_id,
            session_id: Some(SessionId::new(session_id)),
            sequence,
        }))
    }

    #[instrument(skip_all)]
    async fn interrupt_agent(
        &self,
        request: Request<InterruptAgentRequest>,
    ) -> Result<Response<InterruptAgentResponse>, Status> {
        let request = request.into_inner();
        let agent_id = request
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let session_id = self
            .session_of_agent(&agent_id)
            .await
            .map_err(Status::from)?;

        let sequence = self
            .append_agent_event(
                &session_id,
                agent_id,
                Kind::TurnInterrupted(horde_proto::v1::TurnInterrupted {
                    reason: request.reason,
                    ..horde_proto::v1::TurnInterrupted::default()
                }),
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(InterruptAgentResponse { sequence }))
    }

    #[instrument(skip_all)]
    async fn answer_permission(
        &self,
        request: Request<AnswerPermissionRequest>,
    ) -> Result<Response<AnswerPermissionResponse>, Status> {
        let request = request.into_inner();
        let request_id = request
            .request_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // The decision is an event like everything else, which is what makes it
        // answerable from any attached client (DESIGN.md §10).
        let mut found = None;
        for session_id in self.core.log.session_ids().await.map_err(Status::from)? {
            let state = self.project(&session_id).await.map_err(Status::from)?;
            if let Some(agent) = state.pending_permissions.get(&request_id) {
                found = Some((session_id, agent.clone()));
                break;
            }
        }
        let (session_id, agent_id) = found.ok_or_else(|| {
            Status::from(ServerError::Invalid(format!(
                "no permission request {request_id} is awaiting a decision"
            )))
        })?;

        let sequence = self
            .append_agent_event(
                &session_id,
                agent_id,
                Kind::PermissionDecided(horde_proto::v1::PermissionDecided {
                    decision: Some(horde_proto::v1::PermissionDecision {
                        request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                        outcome: request.outcome,
                        decided_by: horde_proto::v1::PermissionLayer::Human as i32,
                        decided_by_actor: None,
                        reason: request.reason,
                        remember_for_session: request.remember_for_session,
                        updated_input: None,
                    }),
                }),
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(AnswerPermissionResponse { sequence }))
    }

    #[instrument(skip_all)]
    async fn spawn_agent(
        &self,
        request: Request<SpawnAgentRequest>,
    ) -> Result<Response<SpawnAgentResponse>, Status> {
        let request = request.into_inner();
        let session_id = request
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let state = self.project(&session_id).await.map_err(Status::from)?;

        let parent = request
            .parent_agent_id
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| state.root_agent_id.clone());
        let agent_id = format!("agent-{}", uuid_like());
        let spec = request.spec.unwrap_or_default();

        // The agent exists whether or not it is placed: spawn never blocks, and
        // an unmatched spawn comes back queued with a reason (DESIGN.md §6).
        let placement = self
            .core
            .scheduler
            .place(spec.placement.as_ref().unwrap_or(&Default::default()));

        self.append(
            &session_id,
            &request.idempotency_key,
            Event {
                agent_id: Some(AgentId::new(agent_id.clone())),
                occurred_at: None,
                idempotency_key: request.idempotency_key.clone(),
                kind: Some(Kind::AgentSpawned(AgentSpawned {
                    parent_agent_id: parent.clone().map(AgentId::new),
                    spawned_by: None,
                    spec: Some(spec.clone()),
                    tree_path: Vec::new(),
                })),
            },
        )
        .await
        .map_err(Status::from)?;

        let wire_placement = match &placement {
            Placement::Placed { worker_id } => {
                self.core.commands.enqueue(
                    worker_id,
                    horde_proto::v1::stream_commands_response::Command::SpawnAgent(
                        horde_proto::v1::SpawnAgentCommand {
                            session_id: Some(SessionId::new(session_id.clone())),
                            agent_id: Some(AgentId::new(agent_id.clone())),
                            parent_agent_id: parent.map(AgentId::new),
                            spec: Some(spec),
                            binding: None,
                            tree_path: Vec::new(),
                            prompt_overlay: String::new(),
                        },
                    ),
                );
                horde_proto::v1::Placement {
                    agent_id: Some(AgentId::new(agent_id.clone())),
                    state: horde_proto::v1::PlacementState::Placed as i32,
                    worker_id: Some(horde_proto::v1::WorkerId::new(worker_id.clone())),
                    reason: String::new(),
                }
            }
            Placement::Queued { reason } => horde_proto::v1::Placement {
                agent_id: Some(AgentId::new(agent_id.clone())),
                state: horde_proto::v1::PlacementState::Queued as i32,
                worker_id: None,
                reason: reason.clone(),
            },
        };

        self.append(
            &session_id,
            "",
            Event {
                agent_id: Some(AgentId::new(agent_id.clone())),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::PlacementChanged(horde_proto::v1::PlacementChanged {
                    placement: Some(wire_placement.clone()),
                })),
            },
        )
        .await
        .map_err(Status::from)?;

        Ok(Response::new(SpawnAgentResponse {
            agent_id: Some(AgentId::new(agent_id)),
            placement: Some(wire_placement),
        }))
    }

    // -- Not built yet. Honest refusals, not fabricated successes. -----------

    async fn create_workspace_definition(
        &self,
        _request: Request<CreateWorkspaceDefinitionRequest>,
    ) -> Result<Response<CreateWorkspaceDefinitionResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn get_workspace_definition(
        &self,
        _request: Request<GetWorkspaceDefinitionRequest>,
    ) -> Result<Response<GetWorkspaceDefinitionResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn list_workspace_definitions(
        &self,
        _request: Request<ListWorkspaceDefinitionsRequest>,
    ) -> Result<Response<ListWorkspaceDefinitionsResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn update_workspace_definition(
        &self,
        _request: Request<UpdateWorkspaceDefinitionRequest>,
    ) -> Result<Response<UpdateWorkspaceDefinitionResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn add_workspace_repo(
        &self,
        _request: Request<AddWorkspaceRepoRequest>,
    ) -> Result<Response<AddWorkspaceRepoResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn delete_workspace_definition(
        &self,
        _request: Request<DeleteWorkspaceDefinitionRequest>,
    ) -> Result<Response<DeleteWorkspaceDefinitionResponse>, Status> {
        Err(ServerError::Unimplemented("workspace definitions").into())
    }

    async fn prewarm_workspace(
        &self,
        _request: Request<PrewarmWorkspaceRequest>,
    ) -> Result<Response<PrewarmWorkspaceResponse>, Status> {
        Err(ServerError::Unimplemented("workspace prewarming").into())
    }

    async fn list_workers(
        &self,
        _request: Request<ListWorkersRequest>,
    ) -> Result<Response<ListWorkersResponse>, Status> {
        // Answered from the registry rather than stubbed: the registry is real
        // even though enrollment is not, and returning an empty list would make
        // a connected worker indistinguishable from none.
        let workers = self
            .core
            .workers
            .connected()
            .into_iter()
            .map(|worker| horde_proto::v1::WorkerSummary {
                worker_id: Some(horde_proto::v1::WorkerId::new(worker.worker_id.clone())),
                name: worker.worker_id,
                // Connection state is what the registry knows: a worker is in
                // it because it registered and has not been dropped.
                state: horde_proto::v1::WorkerConnectionState::Online as i32,
                // Filled in with the handshake once enrollment carries it.
                semver: String::new(),
                capabilities: Some(worker.capabilities),
                last_seen_at: None,
                running_agents: 0,
            })
            .collect();

        Ok(Response::new(ListWorkersResponse { workers }))
    }

    async fn mint_enrollment_code(
        &self,
        _request: Request<MintEnrollmentCodeRequest>,
    ) -> Result<Response<MintEnrollmentCodeResponse>, Status> {
        Err(ServerError::Unimplemented("enrollment").into())
    }

    async fn revoke_enrollment(
        &self,
        _request: Request<RevokeEnrollmentRequest>,
    ) -> Result<Response<RevokeEnrollmentResponse>, Status> {
        Err(ServerError::Unimplemented("enrollment").into())
    }

    async fn query_usage(
        &self,
        _request: Request<QueryUsageRequest>,
    ) -> Result<Response<QueryUsageResponse>, Status> {
        Err(ServerError::Unimplemented("usage aggregation").into())
    }
}

/// A unique-enough identifier without pulling in a uuid dependency.
///
/// Ids are opaque to every consumer; the only requirement is that two calls
/// never collide within a server's lifetime, which a monotonic counter over a
/// process-start nonce satisfies.
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    static START: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

    let start = *START.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos() as u64)
    });
    let ordinal = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{start:x}-{ordinal:x}")
}

#[cfg(test)]
mod tests {
    use super::{recorded_at, stop_after_caught_up};

    #[test]
    fn a_sync_stream_ends_after_history_unless_the_client_asked_to_follow() {
        // The only case that keeps the stream open.
        assert!(!stop_after_caught_up(false, true));

        assert!(
            stop_after_caught_up(false, false),
            "a scriptable sync replays to head and completes"
        );
        assert!(
            stop_after_caught_up(true, true),
            "a client that hung up gets no tail however much it asked to follow"
        );
        assert!(stop_after_caught_up(true, false));
    }

    /// Asserted against known values rather than a "plausible window": a window
    /// wide enough not to flake is also wide enough to accept `secs + 1000` and
    /// `secs / 1000`, which is exactly how both mutants survived the
    /// integration test that checked the clock looked about right.
    #[test]
    fn milliseconds_split_into_whole_seconds_and_the_remainder_as_nanoseconds() {
        let stamp = recorded_at(1_700_000_123_456);
        assert_eq!(stamp.seconds, 1_700_000_123);
        assert_eq!(
            stamp.nanos, 456_000_000,
            "456 ms is 456 million nanoseconds; adding or dividing by a million is not the same"
        );
    }

    #[test]
    fn a_whole_second_has_no_remainder() {
        let stamp = recorded_at(1_000);
        assert_eq!(stamp.seconds, 1);
        assert_eq!(stamp.nanos, 0);
    }

    #[test]
    fn the_epoch_is_zero_in_both_fields() {
        let stamp = recorded_at(0);
        assert_eq!(stamp.seconds, 0);
        assert_eq!(stamp.nanos, 0);
    }

    #[test]
    fn a_sub_second_reading_is_all_remainder() {
        let stamp = recorded_at(999);
        assert_eq!(stamp.seconds, 0);
        assert_eq!(stamp.nanos, 999_000_000);
    }

    /// Floor division, so a pre-epoch reading is a negative second with a
    /// non-negative nanosecond — not a timestamp that reads as later than it
    /// was.
    #[test]
    fn a_pre_epoch_reading_floors_rather_than_truncating_toward_zero() {
        let stamp = recorded_at(-1);
        assert_eq!(stamp.seconds, -1);
        assert_eq!(stamp.nanos, 999_000_000);
    }
}
