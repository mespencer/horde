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
    ArchiveSessionResponse, AssignmentId, AssignmentSent, ConfigureMcpServerRequest,
    ConfigureMcpServerResponse, CreateSessionRequest, CreateSessionResponse,
    CreateWorkspaceDefinitionRequest, CreateWorkspaceDefinitionResponse, DeleteMcpServerRequest,
    DeleteMcpServerResponse, DeleteWorkspaceDefinitionRequest, DeleteWorkspaceDefinitionResponse,
    Event, GetSessionRequest, GetSessionResponse, GetWorkspaceDefinitionRequest,
    GetWorkspaceDefinitionResponse, HelloRequest, HelloResponse, InterruptAgentRequest,
    InterruptAgentResponse, ListMcpServersRequest, ListMcpServersResponse, ListSessionsRequest,
    ListSessionsResponse, ListWorkersRequest, ListWorkersResponse, ListWorkspaceDefinitionsRequest,
    ListWorkspaceDefinitionsResponse, McpServerConfig, MintEnrollmentCodeRequest,
    MintEnrollmentCodeResponse, NoteSent, PrewarmWorkspaceRequest, PrewarmWorkspaceResponse,
    QueryUsageRequest, QueryUsageResponse, RevokeEnrollmentRequest, RevokeEnrollmentResponse,
    SendMessageRequest, SendMessageResponse, ServerHello, SessionId, SessionSummary,
    SpawnAgentRequest, SpawnAgentResponse, SyncEventsRequest, SyncEventsResponse,
    UpdateWorkspaceDefinitionRequest, UpdateWorkspaceDefinitionResponse, WorkspaceDefinitionId,
    control_plane_service_server::ControlPlaneService, event::Kind, send_message_request,
};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status};
use tracing::instrument;

use super::shared::{MIN_SUPPORTED_SEMVER, SERVER_SEMVER, ServerCore};
use super::sync::{self, SubtreeFilter};
use crate::error::ServerError;
use crate::mcp::{MCP_CATALOG_STREAM, McpServerCatalog, validate_config};
use crate::placement;
use crate::scheduler::PlacementRequest;
use crate::state::SessionState;
use crate::workspace::{
    CATALOG_STREAM, WorkspaceCatalog, check_etag, normalized_repos, same_repo_set,
    validate_definition, widened_with,
};

/// Implements `horde.v1.ControlPlaneService`.
#[derive(Clone, Debug)]
pub struct ControlPlane {
    core: ServerCore,
    /// The enrollment authority behind `MintEnrollmentCode` and
    /// `RevokeEnrollment`. `None` on a plane serving without transport
    /// security (in-process harnesses), where those verbs honestly answer
    /// `Unimplemented`: minting identities over an unauthenticated channel
    /// would be worse than not minting them.
    enrollment: Option<std::sync::Arc<super::enrollment::EnrollmentAuthority>>,
    /// How long a follow stream may be silent before it owes a heartbeat.
    heartbeat_period: std::time::Duration,
}

impl ControlPlane {
    #[must_use]
    pub fn new(core: ServerCore) -> Self {
        Self {
            core,
            enrollment: None,
            heartbeat_period: sync::DEFAULT_HEARTBEAT_PERIOD,
        }
    }

    /// A control plane whose enrollment verbs are live, for serving behind
    /// mTLS (`crate::transport::serve_main`).
    #[must_use]
    pub fn with_enrollment(
        core: ServerCore,
        enrollment: std::sync::Arc<super::enrollment::EnrollmentAuthority>,
    ) -> Self {
        Self {
            enrollment: Some(enrollment),
            ..Self::new(core)
        }
    }

    /// Overrides the heartbeat period — tests shrink it to observe a quiet
    /// stream announcing itself without waiting fifteen seconds for it.
    #[must_use]
    pub fn with_heartbeat_period(mut self, period: std::time::Duration) -> Self {
        self.heartbeat_period = period;
        self
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
        // Shared with the placement passes and the permission chain: one
        // append shape, one place the kind-collision refusal lives.
        crate::service::shared::append_server_event(
            &self.core.log,
            session_id,
            idempotency_key,
            event,
        )
        .await
    }

    /// The workspace catalog, folded fresh from its stream.
    async fn catalog(&self) -> Result<WorkspaceCatalog, ServerError> {
        let events = self.core.log.read_all(CATALOG_STREAM).await?;
        WorkspaceCatalog::project(&events)
    }

    /// Appends one catalog change and returns the definition with the etag
    /// that change produced.
    async fn append_definition(
        &self,
        definition: horde_proto::v1::WorkspaceDefinition,
        deleted: bool,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ServerError> {
        let mut stamped = definition.clone();
        let sequence = self
            .append(
                CATALOG_STREAM,
                "",
                Event {
                    agent_id: None,
                    occurred_at: None,
                    idempotency_key: String::new(),
                    kind: Some(Kind::WorkspaceDefinitionChanged(
                        horde_proto::v1::WorkspaceDefinitionChanged {
                            definition: Some(definition),
                            deleted,
                        },
                    )),
                },
            )
            .await?;
        stamped.etag = sequence.to_string();
        Ok(stamped)
    }

    /// The MCP Hub catalog, folded fresh from its stream.
    async fn mcp_catalog(&self) -> Result<McpServerCatalog, ServerError> {
        let events = self.core.log.read_all(MCP_CATALOG_STREAM).await?;
        McpServerCatalog::project(&events)
    }

    /// Appends one MCP catalog change and returns the config with the etag
    /// that change produced.
    async fn append_mcp_config(
        &self,
        config: McpServerConfig,
        deleted: bool,
    ) -> Result<McpServerConfig, ServerError> {
        let mut stamped = config.clone();
        let sequence = self
            .append(
                MCP_CATALOG_STREAM,
                "",
                Event {
                    agent_id: None,
                    occurred_at: None,
                    idempotency_key: String::new(),
                    kind: Some(Kind::McpServerConfigChanged(
                        horde_proto::v1::McpServerConfigChanged {
                            config: Some(config),
                            deleted,
                        },
                    )),
                },
            )
            .await?;
        stamped.etag = sequence.to_string();
        Ok(stamped)
    }

    /// One configure, decided and journaled — the whole verb minus the tonic
    /// wrapping, so every refusal here is a plain `ServerError` and the RPC
    /// shell stays two calls.
    ///
    /// # Errors
    ///
    /// `validate_config`'s refusals for an ill-formed config,
    /// `check_configure`'s for a create/update that contradicts the catalog,
    /// and the log's own failures from the append.
    async fn configure_mcp(&self, config: McpServerConfig) -> Result<McpServerConfig, ServerError> {
        validate_config(&config)?;
        // Create versus update is `check_configure`'s ruling: the etag names
        // the caller's intent, and both intents are checked against the
        // catalog rather than trusted.
        let catalog = self.mcp_catalog().await?;
        crate::mcp::check_configure(catalog.get(&config.name), &config)?;
        self.append_mcp_config(config, false).await
    }

    /// A live definition by id, or the error that names what was asked for.
    async fn required_definition(
        &self,
        workspace_definition_id: &str,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ServerError> {
        self.catalog()
            .await?
            .get(workspace_definition_id)
            .cloned()
            .ok_or_else(|| ServerError::NoSuchWorkspace {
                wanted: workspace_definition_id.to_owned(),
            })
    }

    /// The preamble every workspace mutation shares: resolve the id, fetch the
    /// live definition, and enforce the optimistic-concurrency token. One
    /// copy, so no mutation can forget the etag check.
    async fn mutable_definition(
        &self,
        workspace_definition_id: Option<&horde_proto::v1::WorkspaceDefinitionId>,
        etag: &str,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ServerError> {
        let id = workspace_definition_id
            .map(ToString::to_string)
            .unwrap_or_default();
        let current = self.required_definition(&id).await?;
        check_etag(&current, etag)?;
        Ok(current)
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
            if crate::enrollment::is_reserved_stream(&session_id) {
                continue;
            }
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
            // Projected from the root spawn's tree path — the same rule the
            // TUI used to reconstruct titles client-side before summaries
            // carried them.
            title: state.title.clone(),
            root_agent_id: state.root_agent_id.clone().map(AgentId::new),
            workspace_definition_id: None,
            state: state.lifecycle as i32,
            created_at: None,
            head_sequence: state.head_sequence,
            agent_count: u32::try_from(state.agents.len()).unwrap_or(u32::MAX),
        }
    }
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

        // The request-level workspace is consumed, not decoration: a client
        // may set it beside a spec that names none, and the root agent works
        // in it either way.
        let mut root_agent = request.root_agent.unwrap_or_default();
        let spec_workspace = root_agent
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        if spec_workspace.is_none()
            && let Some(id) = request
                .workspace_definition_id
                .as_ref()
                .map(ToString::to_string)
                .filter(|id| !id.is_empty())
        {
            root_agent.workspace_definition_id = Some(WorkspaceDefinitionId::new(id));
        }

        // A root agent naming a workspace the catalog does not hold — or a
        // tier the catalog cannot resolve — is refused before the session
        // exists: the same checks spawn makes, made here for the same reason.
        let root_workspace = root_agent
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        if let Some(id) = root_workspace.as_deref() {
            self.required_definition(id).await.map_err(Status::from)?;
        }
        validate_model_selection(Some(&root_agent))?;

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
                spec: Some(root_agent),
                tree_path: vec![request.title.clone()],
            })),
        };

        self.append(&session_id, &request.idempotency_key, spawned)
            .await
            .map_err(Status::from)?;

        // The root agent needs a worker like any other agent; a placement
        // pass answers its spawn now (placed, or queued with the reason)
        // instead of leaving it to wait for the pool to change.
        placement::place_queued(&self.core)
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
            // The reserved streams (catalog, enrollment) live in the log but
            // are not sessions.
            if crate::enrollment::is_reserved_stream(&session_id) {
                continue;
            }
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

    /// The resilience primitive: replay from an offset, then follow. Named
    /// cursors, the all-sessions overview, and a subtree-scoped attach are
    /// all this one call — the merged tail serves every shape.
    #[instrument(skip_all)]
    async fn sync_events(
        &self,
        request: Request<SyncEventsRequest>,
    ) -> Result<Response<Self::SyncEventsStream>, Status> {
        let serial = super::shared::peer_serial(&request);
        let request = request.into_inner();

        let cursors: Vec<(String, u64)> = request
            .cursors
            .iter()
            .map(|cursor| {
                (
                    cursor
                        .session_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                    cursor.from_sequence,
                )
            })
            .collect();
        if cursors.is_empty() && !request.all_sessions {
            return Err(Status::from(ServerError::Invalid(
                "sync requires a cursor to replay, or all_sessions".to_owned(),
            )));
        }

        let subtree_root = request
            .subtree_root
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        let filter = match subtree_root {
            None => SubtreeFilter::unscoped(),
            Some(root) => {
                // A subtree scopes an *attach* — named sessions. Combined
                // with all_sessions it would silently drop almost everything
                // the flag promised, so the combination is refused rather
                // than guessed at.
                if request.all_sessions {
                    return Err(Status::from(ServerError::Invalid(
                        "subtree_root scopes named cursors; it cannot be combined with \
                         all_sessions"
                            .to_owned(),
                    )));
                }
                // Seed from each session's full history: the spawns proving
                // membership may lie before the cursor's offset.
                let mut filter = SubtreeFilter::scoped(root);
                for (session_id, _) in &cursors {
                    let history = self
                        .core
                        .log
                        .read_all(session_id)
                        .await
                        .map_err(Status::from)?;
                    filter.seed(session_id, &history);
                }
                filter
            }
        };

        // The workspace catalog lives in the log but is not a session; the
        // all-sessions net must not catch it, while an explicit cursor for it
        // still works.
        let (history, tail) = self
            .core
            .log
            .tail_merged(&cursors, request.all_sessions, &[CATALOG_STREAM.to_owned()])
            .await
            .map_err(Status::from)?;

        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let deny = self
            .enrollment
            .as_ref()
            .map(|authority| authority.deny_list());
        let stream = sync::stream_merged(
            sender.clone(),
            history,
            tail,
            request.follow,
            filter,
            self.heartbeat_period,
        );
        tokio::spawn(async move {
            tokio::select! {
                () = stream => {}
                // Revocation severs a live follow too (fix a4): a revoked
                // client must not keep reading the log until it reconnects.
                () = super::shared::severed(deny, serial.clone()) => {
                    let _ = sender
                        .send(Err(super::shared::severed_status(serial.as_deref())))
                        .await;
                }
            }
        });

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
            if let Some(pending) = state.pending_permissions.get(&request_id) {
                found = Some((session_id, pending.agent_id.clone()));
                break;
            }
        }
        let (session_id, agent_id) = found.ok_or_else(|| {
            Status::from(ServerError::Invalid(format!(
                "no permission request {request_id} is awaiting a decision"
            )))
        })?;

        // A narrowing only means anything alongside ALLOW. Carrying one on a
        // denial would write a decision whose replacement arguments every
        // reader is entitled to ignore — a rewrite that silently evaporates,
        // which is the exact failure permission.proto's reject-don't-drop note
        // exists to prevent. Refused here rather than dropped here.
        if request.updated_input.is_some()
            && request.outcome != i32::from(horde_proto::v1::PermissionOutcome::Allow)
        {
            return Err(Status::from(ServerError::Invalid(
                "a permission decision may only carry updated_input with outcome ALLOW".to_owned(),
            )));
        }

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
                        // Carried through rather than dropped. Absence and an
                        // empty Struct are different instructions — "allow as
                        // proposed" versus "allow with no arguments at all" —
                        // and `Option` is what keeps them distinct across this
                        // hop, so this is deliberately a move and not a
                        // `unwrap_or_default`.
                        updated_input: request.updated_input,
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

        // A spawn naming a workspace the catalog does not hold — or a tier
        // it cannot resolve — is refused before anything is written: placing
        // it would eventually command a materialization of nothing, or bind
        // a model of nothing.
        let workspace = spec
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        if let Some(id) = workspace.as_deref() {
            self.required_definition(id).await.map_err(Status::from)?;
        }
        validate_model_selection(Some(&spec))?;

        // The whole decision — project, place, append — runs under the
        // placement lock so a drain pass woken by a worker registering cannot
        // place this agent between our appends.
        let serialized = self.core.placement_lock.clone();
        let _serialized = serialized.lock().await;

        // The agent exists whether or not it is placed: spawn never blocks, and
        // an unmatched spawn comes back queued with a reason (DESIGN.md §6).
        let load = placement::worker_load(&self.core)
            .await
            .map_err(Status::from)?;
        let parent_worker = parent
            .as_ref()
            .and_then(|parent_id| state.agents.get(parent_id))
            .and_then(|parent_state| parent_state.worker_id.clone());
        let constraints = spec.placement.clone().unwrap_or_default();
        let placement = self.core.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            parent_worker_id: parent_worker.as_deref(),
            workspace_definition_id: workspace.as_deref(),
            load: &load,
            now_unix_ms: placement::now_unix_ms(),
        });

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

        // No command is enqueued here. Recording the placement below *is* how
        // the worker is told to spawn: `service::commands` folds that event
        // into a `SpawnAgentCommand`. Writing the event and separately pushing
        // a command would be two sources of truth for one decision, and the
        // one held in memory is the one a restart loses. The prewarm request,
        // when one is needed, is appended *first*, so the worker's command
        // stream carries materialize-then-spawn.
        if let crate::scheduler::Placement::Placed { worker_id } = &placement {
            placement::ensure_instance(&self.core, &session_id, workspace.as_deref(), worker_id)
                .await
                .map_err(Status::from)?;
        }

        let wire_placement = placement::wire_placement(&agent_id, &placement);
        placement::append_placement(&self.core, &session_id, wire_placement.clone())
            .await
            .map_err(Status::from)?;

        Ok(Response::new(SpawnAgentResponse {
            agent_id: Some(AgentId::new(agent_id)),
            placement: Some(wire_placement),
        }))
    }

    // -- Workspaces -----------------------------------------------------------

    #[instrument(skip_all)]
    async fn create_workspace_definition(
        &self,
        request: Request<CreateWorkspaceDefinitionRequest>,
    ) -> Result<Response<CreateWorkspaceDefinitionResponse>, Status> {
        let request = request.into_inner();
        let repos = normalized_repos(request.repos);
        validate_definition(&request.name, &repos).map_err(Status::from)?;

        let catalog = self.catalog().await.map_err(Status::from)?;
        if catalog.get_by_name(&request.name).is_some() {
            return Err(Status::from(ServerError::WorkspaceNameTaken {
                name: request.name,
            }));
        }

        let definition = horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(format!(
                "ws-{}",
                uuid_like()
            ))),
            name: request.name,
            repos,
            unmanaged_paths: request.unmanaged_paths,
            created_at: None,
            etag: String::new(),
        };
        let stamped = self
            .append_definition(definition, false)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(CreateWorkspaceDefinitionResponse {
            definition: Some(stamped),
        }))
    }

    #[instrument(skip_all)]
    async fn get_workspace_definition(
        &self,
        request: Request<GetWorkspaceDefinitionRequest>,
    ) -> Result<Response<GetWorkspaceDefinitionResponse>, Status> {
        let request = request.into_inner();
        let catalog = self.catalog().await.map_err(Status::from)?;

        let id = request
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        let definition = match (id, request.name.as_str()) {
            (Some(id), _) => catalog
                .get(&id)
                .cloned()
                .ok_or(ServerError::NoSuchWorkspace { wanted: id }),
            (None, name) if !name.is_empty() => {
                catalog
                    .get_by_name(name)
                    .cloned()
                    .ok_or(ServerError::NoSuchWorkspace {
                        wanted: name.to_owned(),
                    })
            }
            (None, _) => Err(ServerError::Invalid(
                "a lookup needs an id or a name".to_owned(),
            )),
        }
        .map_err(Status::from)?;

        let instances = definition
            .workspace_definition_id
            .as_ref()
            .map(|id| self.core.instances.for_definition(&id.to_string()))
            .unwrap_or_default();

        Ok(Response::new(GetWorkspaceDefinitionResponse {
            definition: Some(definition),
            instances,
        }))
    }

    #[instrument(skip_all)]
    async fn list_workspace_definitions(
        &self,
        _request: Request<ListWorkspaceDefinitionsRequest>,
    ) -> Result<Response<ListWorkspaceDefinitionsResponse>, Status> {
        let catalog = self.catalog().await.map_err(Status::from)?;
        Ok(Response::new(ListWorkspaceDefinitionsResponse {
            definitions: catalog.live().cloned().collect(),
            // Paging lands when a catalog is large enough to need it; an
            // empty token honestly means "that was all of them".
            next_page_token: String::new(),
        }))
    }

    #[instrument(skip_all)]
    async fn update_workspace_definition(
        &self,
        request: Request<UpdateWorkspaceDefinitionRequest>,
    ) -> Result<Response<UpdateWorkspaceDefinitionResponse>, Status> {
        let request = request.into_inner();
        let current = self
            .mutable_definition(request.workspace_definition_id.as_ref(), &request.etag)
            .await
            .map_err(Status::from)?;

        // Layout changes only: the update's repos may not add or remove.
        // An empty list means "unchanged" — proto3 cannot tell an omitted
        // repeated field from an empty one, and reading empty as "remove
        // every repo" would let the field edit the proto forbids.
        let repos = if request.repos.is_empty() {
            current.repos.clone()
        } else {
            let updated = normalized_repos(request.repos);
            same_repo_set(&current.repos, &updated).map_err(Status::from)?;
            updated
        };

        let name = if request.name.is_empty() {
            current.name.clone()
        } else {
            request.name
        };
        validate_definition(&name, &repos).map_err(Status::from)?;
        let catalog = self.catalog().await.map_err(Status::from)?;
        if let Some(holder) = catalog.get_by_name(&name)
            && holder.workspace_definition_id != current.workspace_definition_id
        {
            return Err(Status::from(ServerError::WorkspaceNameTaken { name }));
        }

        let stamped = self
            .append_definition(
                horde_proto::v1::WorkspaceDefinition {
                    name,
                    repos,
                    unmanaged_paths: request.unmanaged_paths,
                    etag: String::new(),
                    ..current
                },
                false,
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(UpdateWorkspaceDefinitionResponse {
            definition: Some(stamped),
        }))
    }

    #[instrument(skip_all)]
    async fn add_workspace_repo(
        &self,
        request: Request<AddWorkspaceRepoRequest>,
    ) -> Result<Response<AddWorkspaceRepoResponse>, Status> {
        let request = request.into_inner();
        let current = self
            .mutable_definition(request.workspace_definition_id.as_ref(), &request.etag)
            .await
            .map_err(Status::from)?;

        let widened = widened_with(current, request.repo).map_err(Status::from)?;
        let stamped = self
            .append_definition(widened, false)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(AddWorkspaceRepoResponse {
            definition: Some(stamped),
        }))
    }

    #[instrument(skip_all)]
    async fn delete_workspace_definition(
        &self,
        request: Request<DeleteWorkspaceDefinitionRequest>,
    ) -> Result<Response<DeleteWorkspaceDefinitionResponse>, Status> {
        let request = request.into_inner();
        let current = self
            .mutable_definition(request.workspace_definition_id.as_ref(), &request.etag)
            .await
            .map_err(Status::from)?;

        self.append_definition(current, true)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(DeleteWorkspaceDefinitionResponse {}))
    }

    #[instrument(skip_all)]
    async fn prewarm_workspace(
        &self,
        request: Request<PrewarmWorkspaceRequest>,
    ) -> Result<Response<PrewarmWorkspaceResponse>, Status> {
        let request = request.into_inner();
        let id = request
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        self.required_definition(&id).await.map_err(Status::from)?;
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !self.core.workers.contains(&worker_id) {
            return Err(Status::from(ServerError::NoWorkerAvailable {
                what: format!("workspace {id} on worker `{worker_id}`"),
            }));
        }

        // Progress rides the named session's log when one is given; otherwise
        // the catalog stream carries it, so prewarming ahead of any session
        // still journals its materialization somewhere findable.
        let carrying_stream = request
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|session| !session.is_empty())
            .unwrap_or_else(|| CATALOG_STREAM.to_owned());

        placement::ensure_instance(&self.core, &carrying_stream, Some(&id), &worker_id)
            .await
            .map_err(Status::from)?;

        Ok(Response::new(PrewarmWorkspaceResponse {
            report: Some(
                self.core
                    .instances
                    .for_definition(&id)
                    .into_iter()
                    .find(|report| {
                        report
                            .worker_id
                            .as_ref()
                            .map(ToString::to_string)
                            .as_deref()
                            == Some(worker_id.as_str())
                    })
                    .unwrap_or_default(),
            ),
        }))
    }

    // -- MCP Hub ---------------------------------------------------------------

    #[instrument(skip_all)]
    async fn configure_mcp_server(
        &self,
        request: Request<ConfigureMcpServerRequest>,
    ) -> Result<Response<ConfigureMcpServerResponse>, Status> {
        // The shell is deliberately two calls: the decisions live in
        // `crate::mcp` as truth-tabled pure functions (`validate_config`,
        // `check_configure`, `required_config`) and the sequencing in
        // `configure_mcp`, where every branch is a plain `?`.
        let config = crate::mcp::required_config(request.into_inner().config)?;
        let stamped = self.configure_mcp(config).await.map_err(Status::from)?;
        Ok(Response::new(ConfigureMcpServerResponse {
            config: Some(stamped),
        }))
    }

    #[instrument(skip_all)]
    async fn list_mcp_servers(
        &self,
        _request: Request<ListMcpServersRequest>,
    ) -> Result<Response<ListMcpServersResponse>, Status> {
        let catalog = self.mcp_catalog().await.map_err(Status::from)?;
        Ok(Response::new(ListMcpServersResponse {
            servers: catalog.live().cloned().collect(),
        }))
    }

    #[instrument(skip_all)]
    async fn delete_mcp_server(
        &self,
        request: Request<DeleteMcpServerRequest>,
    ) -> Result<Response<DeleteMcpServerResponse>, Status> {
        let request = request.into_inner();
        let catalog = self.mcp_catalog().await.map_err(Status::from)?;
        let current = catalog
            .get(&request.name)
            .ok_or_else(|| {
                Status::from(ServerError::NoSuchMcpServer {
                    name: request.name.clone(),
                })
            })?
            .clone();
        crate::mcp::check_etag(&current, &request.etag).map_err(Status::from)?;

        // The event carries the last live state, so the log alone records
        // what was removed.
        self.append_mcp_config(current, true)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(DeleteMcpServerResponse {}))
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
        request: Request<MintEnrollmentCodeRequest>,
    ) -> Result<Response<MintEnrollmentCodeResponse>, Status> {
        let Some(authority) = self.enrollment.as_ref() else {
            return Err(ServerError::Unimplemented("enrollment on this listener").into());
        };
        require_client_role(&request, "mint enrollment codes")?;
        let request = request.into_inner();

        let role = crate::enrollment::role_from_wire(request.role)
            .ok_or_else(|| Status::invalid_argument("a code must name a role: worker or client"))?;
        let principal = horde_identity::Principal::new(role, request.name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let requested_ttl_ms = request
            .ttl
            .as_ref()
            .map_or(0, crate::enrollment::duration_to_ms);

        let minted = authority
            .mint(
                &principal,
                requested_ttl_ms,
                super::enrollment::wall_clock_ms(),
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(MintEnrollmentCodeResponse {
            code: minted.join_code(),
            server_cert_fingerprint: minted.server_fingerprint.clone(),
            expires_at: Some(crate::enrollment::ms_to_timestamp(
                minted.expires_at_unix_ms,
            )),
        }))
    }

    async fn revoke_enrollment(
        &self,
        request: Request<RevokeEnrollmentRequest>,
    ) -> Result<Response<RevokeEnrollmentResponse>, Status> {
        let Some(authority) = self.enrollment.as_ref() else {
            return Err(ServerError::Unimplemented("enrollment on this listener").into());
        };
        require_client_role(&request, "revoke enrollment")?;
        let request = request.into_inner();

        use horde_proto::v1::revoke_enrollment_request::Principal as Wire;
        let principal = match request.principal {
            Some(Wire::WorkerId(id)) => horde_identity::Principal::new(
                horde_identity::PrincipalRole::Worker,
                id.to_string(),
            ),
            Some(Wire::ClientId(id)) => horde_identity::Principal::new(
                horde_identity::PrincipalRole::Client,
                id.to_string(),
            ),
            None => {
                return Err(Status::invalid_argument(
                    "a revocation must name a worker or client",
                ));
            }
        }
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let revoked = authority
            .revoke(
                &principal,
                &request.reason,
                super::enrollment::wall_clock_ms(),
            )
            .await
            .map_err(Status::from)?;

        Ok(Response::new(RevokeEnrollmentResponse {
            // The proto carries one serial; every revoked serial is in the
            // log. Joined so a caller revoking a re-enrolled principal still
            // sees everything that stopped working.
            revoked_serial: revoked.join(","),
        }))
    }

    /// Usage reporting (DESIGN §12): a pure fold over the named sessions'
    /// logs — no usage table exists to disagree with the events.
    #[instrument(skip_all)]
    async fn query_usage(
        &self,
        request: Request<QueryUsageRequest>,
    ) -> Result<Response<QueryUsageResponse>, Status> {
        let request = request.into_inner();
        let grouping = crate::usage::Grouping::from_wire(request.group_by).ok_or_else(|| {
            Status::from(ServerError::Invalid(
                "a usage query must say how to group: by agent, session, \
                 tree path, provider account, or model"
                    .to_owned(),
            ))
        })?;

        let mut query = crate::usage::UsageQuery::grouped_by(grouping);
        if let Some(root) = request
            .subtree_root
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
        {
            query = query.scoped_to_subtree(root);
        }
        if let Some(since) = request.since.as_ref() {
            query = query.since(crate::usage::timestamp_unix_ms(since));
        }
        if let Some(until) = request.until.as_ref() {
            query = query.until(crate::usage::timestamp_unix_ms(until));
        }

        let named: Vec<String> = request
            .session_ids
            .iter()
            .map(ToString::to_string)
            .collect();
        let sessions = if named.is_empty() {
            // Empty means all sessions; the reserved streams (catalog,
            // enrollment) live in the log but are not sessions.
            self.core
                .log
                .session_ids()
                .await
                .map_err(Status::from)?
                .into_iter()
                .filter(|session_id| !crate::enrollment::is_reserved_stream(session_id))
                .collect()
        } else {
            named
        };

        let mut fold = crate::usage::UsageFold::new(query);
        for session_id in &sessions {
            let events = self
                .core
                .log
                .read_all(session_id)
                .await
                .map_err(Status::from)?;
            // A *named* session that does not exist is refused rather than
            // silently reported as zero usage — a typo must not read as
            // thrift.
            if events.is_empty() {
                return Err(Status::from(ServerError::NoSuchSession {
                    session_id: session_id.clone(),
                }));
            }
            fold.absorb_session(session_id, &events)
                .map_err(Status::from)?;
        }

        // Declared quotas (usage::DeclaredQuota) await the config layer;
        // until it lands, only provider-reported windows produce headroom.
        let accounts = fold.accounts(placement::now_unix_ms(), &std::collections::BTreeMap::new());
        Ok(Response::new(QueryUsageResponse {
            aggregates: fold.aggregates(),
            accounts,
        }))
    }
}

/// Refuses a spec naming a tier the catalog cannot resolve.
///
/// At the RPC, not the fold: the fold is deterministic over the log and must
/// not be where an unknown name surfaces — by then the spawn is an event of
/// record. The refusal lists the tiers that do exist, because "unknown tier"
/// alone sends the caller to the docs this message could have been.
///
/// # Errors
///
/// `InvalidArgument` naming the tier and the known ones.
fn validate_model_selection(spec: Option<&horde_proto::v1::AgentSpec>) -> Result<(), Status> {
    let Some(horde_proto::v1::model_selection::Selection::Tier(tier)) = spec
        .and_then(|spec| spec.model.as_ref())
        .and_then(|model| model.selection.as_ref())
    else {
        return Ok(());
    };
    let catalog = crate::tiers::TierCatalog::shipped_defaults();
    if catalog.get(tier).is_none() {
        return Err(Status::invalid_argument(format!(
            "tier {tier:?} is not in the catalog; known tiers: {}",
            catalog.names().join(", ")
        )));
    }
    Ok(())
}

/// Requires the caller's certificate to name a Client principal.
///
/// Mint and revoke are *operator* verbs (DESIGN §3, wave-7 fix a3): a worker
/// certificate must not be able to mint codes for any role or revoke every
/// client. The interceptor stamped the verified [`PeerIdentity`] into the
/// request extensions; an absent identity means this request did not come
/// through the mTLS door at all, which is the same refusal — this check runs
/// only on servers whose enrollment authority is live, and those always
/// front their control plane with the interceptor.
///
/// # Errors
///
/// `PermissionDenied` naming the caller and the verb it may not use.
fn require_client_role<T>(request: &Request<T>, verb: &str) -> Result<(), Status> {
    let identity = request
        .extensions()
        .get::<horde_identity::PeerIdentity>()
        .ok_or_else(|| {
            Status::permission_denied(format!(
                "only an enrolled client may {verb}; this channel carries no peer identity"
            ))
        })?;
    if identity.principal.role() != horde_identity::PrincipalRole::Client {
        return Err(Status::permission_denied(format!(
            "only an enrolled client may {verb}; {} is a {}",
            identity.principal,
            identity.principal.role()
        )));
    }
    Ok(())
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
