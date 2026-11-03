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

use std::ops::ControlFlow;
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
    QueryUsageRequest, QueryUsageResponse, ResetWorkerJournalRequest, ResetWorkerJournalResponse,
    RevokeEnrollmentRequest, RevokeEnrollmentResponse, SendMessageRequest, SendMessageResponse,
    ServerHello, SessionId, SessionSummary, SpawnAgentRequest, SpawnAgentResponse,
    SyncEventsRequest, SyncEventsResponse, UpdateWorkspaceDefinitionRequest,
    UpdateWorkspaceDefinitionResponse, WorkspaceDefinitionId,
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

    /// A session's projected state, refusing one that has no events.
    async fn project(&self, session_id: &str) -> Result<SessionState, ServerError> {
        self.core
            .sessions
            .state(session_id)
            .await?
            .ok_or_else(|| ServerError::NoSuchSession {
                session_id: session_id.to_owned(),
            })
    }

    /// The session a request names, and its projection — refused, as a
    /// status, when it has no events.
    async fn project_named(
        &self,
        session_id: Option<&SessionId>,
    ) -> Result<(String, SessionState), Status> {
        let session_id = session_id.map(ToString::to_string).unwrap_or_default();
        let state = self.project(&session_id).await.map_err(Status::from)?;
        Ok((session_id, state))
    }

    /// The workspace a spawn names, once it is known to exist.
    ///
    /// A spawn naming a workspace the catalog does not hold — or a tier it
    /// cannot resolve — is refused before anything is written: placing it
    /// would eventually command a materialization of nothing, or bind a
    /// model of nothing.
    async fn validated_workspace(
        &self,
        spec: &horde_proto::v1::AgentSpec,
    ) -> Result<Option<String>, Status> {
        let workspace = spec
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        if let Some(id) = workspace.as_deref() {
            self.required_definition(id).await.map_err(Status::from)?;
        }
        validate_model_selection(Some(spec))?;
        Ok(workspace)
    }

    /// The spec a spawn runs with: what it asked for, with every unset field
    /// filled from the named parent's spec as the log recorded it — never
    /// from anything the spawner asserts (DESIGN §5). A spawn an agent made
    /// through its worker is then refused if the result holds more than that
    /// parent does (ADR-0009). A spawn naming no parent, or a parent the
    /// session has never seen, is what it asked for.
    async fn lineage_spec(
        &self,
        state: &SessionState,
        named_parent: Option<&str>,
        from_agent: bool,
        asked: horde_proto::v1::AgentSpec,
    ) -> Result<horde_proto::v1::AgentSpec, Status> {
        let Some(parent) = named_parent
            .and_then(|id| state.agents.get(id))
            .map(|agent| agent.spec.clone().unwrap_or_default())
        else {
            return Ok(asked);
        };
        let spec = crate::lineage::inherited(&parent, asked);
        if from_agent {
            let catalog = self.mcp_catalog().await.map_err(Status::from)?;
            if let Some(reason) = crate::lineage::exceeds(&parent, &spec, &catalog) {
                return Err(Status::permission_denied(reason));
            }
        }
        Ok(spec)
    }

    /// The two answers a spawn owes before it creates anything (ADR-0011),
    /// asked under the placement lock: a retry answers with the agent the
    /// first attempt created, placed wherever it is now — instead of a
    /// phantom id whose placement names an agent the log never spawned, and
    /// with no racing twin able to miss the stored one — and a spawn under an
    /// ended parent, or into an archived session, is refused by name.
    /// `Ok(None)` means: go ahead and spawn.
    async fn spawn_retried_or_refused(
        &self,
        session_id: &str,
        idempotency_key: &str,
        state: &SessionState,
        parent: Option<&str>,
    ) -> Result<Option<SpawnAgentResponse>, Status> {
        if let Some((_, payload)) = self
            .core
            .log
            .stored_under_key(session_id, idempotency_key)
            .await
            .map_err(Status::from)?
        {
            let current = self.project(session_id).await.map_err(Status::from)?;
            if let Some(response) = spawn_retry(&current, &payload) {
                return Ok(Some(response));
            }
        }
        match parent {
            Some(parent) => refuse_ended(state, parent)?,
            None => refuse_archived_session(state)?,
        }
        Ok(None)
    }

    /// Finds which session an agent belongs to.
    ///
    /// A walk of the live projection (ADR-0013): in-memory states, no log
    /// reads beyond the catch-up.
    async fn session_of_agent(&self, agent_id: &str) -> Result<String, ServerError> {
        self.core
            .sessions
            .walk(crate::enrollment::is_session_stream, |state| {
                if state.agents.contains_key(agent_id) {
                    ControlFlow::Break(state.session_id.clone())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .await?
            .ok_or_else(|| ServerError::NoSuchAgent {
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
            created_at: state
                .created_at_unix_ms
                .map(crate::enrollment::ms_to_timestamp),
            head_sequence: state.head_sequence,
            agent_count: u32::try_from(state.agents.len()).unwrap_or(u32::MAX),
            // A session with no events cannot be projected at all, so a
            // folded state always has real activity to report.
            last_activity_at: Some(crate::enrollment::ms_to_timestamp(
                state.last_activity_unix_ms,
            )),
        }
    }
}

/// Whether `ListSessions` lists a session: an archived one only when the
/// request includes archived sessions, and — when the request names one —
/// only a session whose name (its title, the root spawn's first tree-path
/// element) is exactly that name.
fn listed(state: &SessionState, request: &ListSessionsRequest) -> bool {
    let archived = state.lifecycle == horde_proto::v1::SessionLifecycleState::Archived;
    (request.include_archived || !archived)
        && request
            .name
            .as_ref()
            .is_none_or(|name| state.title == *name)
}

#[tonic::async_trait]
impl ControlPlaneService for ControlPlane {
    #[instrument(skip_all)]
    async fn hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloResponse>, Status> {
        // Echoed per `handshake.proto`: whom the channel's certificate
        // names, so a client can show who it is talking as. Empty on a
        // plaintext channel, which carries no identity.
        let principal = principal_of(&request);
        // The version gate (DESIGN §2, fix a6): the client's Hello is read
        // and compared, not discarded.
        super::shared::version_gate(request.into_inner().hello.as_ref())?;
        Ok(Response::new(HelloResponse {
            server: Some(ServerHello {
                principal,
                ..Self::server_hello()
            }),
            // Assigned from the client certificate once enrollment exists.
            client_id: None,
        }))
    }

    #[instrument(skip_all)]
    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<CreateSessionResponse>, Status> {
        let actor = actor_of(&request);
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
                spawned_by: actor,
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
        let request = request.into_inner();

        // The reserved streams (catalog, enrollment) live in the log but are
        // not sessions; the walk turns them away by id.
        let mut sessions = Vec::new();
        self.core
            .sessions
            .walk(crate::enrollment::is_session_stream, |state| {
                if listed(state, &request) {
                    sessions.push(Self::summary(state));
                }
                ControlFlow::<()>::Continue(())
            })
            .await
            .map_err(Status::from)?;

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
        let (session_id, state) = self.project_named(request.session_id.as_ref()).await?;

        // Each agent's archive is a fact of its own, written here with the
        // state it was archived *from* (ADR-0011): the worker that carries
        // the archive out cannot say — it does not track idle-versus-running
        // — and an agent no worker holds (queued, lost) has nobody else to
        // record its end at all. The fold, placement and the verbs all read
        // the agent's own lifecycle, so archive is terminal for each of them.
        for agent in state.agents.values() {
            if agent.lifecycle == horde_proto::v1::AgentLifecycleState::Archived {
                continue;
            }
            self.append_agent_event(
                &session_id,
                agent.agent_id.clone(),
                Kind::AgentLifecycleChanged(horde_proto::v1::AgentLifecycleChanged {
                    previous_state: agent.lifecycle as i32,
                    state: horde_proto::v1::AgentLifecycleState::Archived as i32,
                    reason: archive_reason(&request.reason),
                }),
            )
            .await
            .map_err(Status::from)?;
        }

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
        let conduit = crate::trust::conduit_worker(&request);
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
        // A worker reads on behalf of its agents (ADR-0009's gap, closed in
        // ADR-0011): only sessions it holds an agent in, named one by one —
        // never the all-sessions net, which would hand it every session's
        // traffic, including ones it has no agent in.
        if let Some(worker_id) = conduit.as_deref() {
            if request.all_sessions {
                return Err(Status::permission_denied(format!(
                    "worker {worker_id} reads only sessions it runs agents in; all_sessions is \
                     a client's view"
                )));
            }
            for (session_id, _) in &cursors {
                let state = self.project(session_id).await.map_err(Status::from)?;
                conduit_holds_session(&state, worker_id, "read")?;
            }
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
        // Revocation severs a live follow too (fix a4): a revoked client
        // must not keep reading the log until it reconnects.
        tokio::spawn(async move {
            super::shared::run_severable(stream, deny, serial, &sender).await;
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    #[instrument(skip_all)]
    async fn send_message(
        &self,
        request: Request<SendMessageRequest>,
    ) -> Result<Response<SendMessageResponse>, Status> {
        let actor = actor_of(&request);
        let conduit = crate::trust::conduit_worker(&request);
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
        let state = self.project(&session_id).await.map_err(Status::from)?;
        // A worker sends on behalf of one of its agents (ADR-0009): it may
        // only reach sessions it is running an agent in.
        if let Some(worker_id) = conduit.as_deref() {
            conduit_holds_session(&state, worker_id, "message into")?;
        }
        refuse_ended(&state, &agent_id)?;

        let (kind, assignment_id) = match request.body {
            Some(send_message_request::Body::Assignment(mut assignment)) => {
                let id = AssignmentId::new(format!("assignment-{}", uuid_like()));
                assignment.assignment_id = Some(id.clone());
                (
                    Kind::AssignmentSent(AssignmentSent {
                        from: actor.clone(),
                        assignment: Some(assignment),
                        delivery: request.delivery,
                    }),
                    Some(id),
                )
            }
            Some(send_message_request::Body::Note(note)) => (
                Kind::NoteSent(NoteSent {
                    from: actor,
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

        let appended = crate::service::shared::append_server_event_returning(
            &self.core.log,
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
        let sequence = appended.sequence;
        // A retry of a send that already landed answers with what the first
        // one minted (ADR-0011), not with the assignment id this attempt
        // made up and the log never stored.
        if let Some(stored) = appended.existing_payload.as_deref() {
            return Ok(Response::new(send_retry(&session_id, sequence, stored)?));
        }

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
        let actor = actor_of(&request);
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
                    requested_by: actor,
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
        let actor = actor_of(&request);
        let request = request.into_inner();
        let request_id = request
            .request_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();

        // The decision is an event like everything else, which is what makes it
        // answerable from any attached client (DESIGN.md §10).
        let found = self
            .core
            .sessions
            .walk(
                |_| true,
                |state| match state.pending_permissions.get(&request_id) {
                    Some(pending) => {
                        let rewrites = state
                            .agents
                            .get(&pending.agent_id)
                            .is_some_and(|agent| agent.rewrites_tool_input);
                        ControlFlow::Break((
                            state.session_id.clone(),
                            pending.agent_id.clone(),
                            rewrites,
                        ))
                    }
                    None => ControlFlow::Continue(()),
                },
            )
            .await
            .map_err(Status::from)?;
        let (session_id, agent_id, rewrites) = found.ok_or_else(|| {
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
        refuse_unappliable_narrowing(request.updated_input.is_some(), rewrites, &agent_id)?;

        let sequence = self
            .append_agent_event(
                &session_id,
                agent_id,
                Kind::PermissionDecided(horde_proto::v1::PermissionDecided {
                    decision: Some(horde_proto::v1::PermissionDecision {
                        request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                        outcome: request.outcome,
                        decided_by: horde_proto::v1::PermissionLayer::Human as i32,
                        decided_by_actor: actor,
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
        let actor = actor_of(&request);
        let conduit = crate::trust::conduit_worker(&request);
        let request = request.into_inner();
        let (session_id, state) = self.project_named(request.session_id.as_ref()).await?;
        // A worker spawns on behalf of one of its agents (ADR-0009): the
        // parent must be named, and placed on the calling worker.
        if let Some(worker_id) = conduit.as_deref() {
            conduit_may_spawn(
                &state,
                request
                    .parent_agent_id
                    .as_ref()
                    .map(ToString::to_string)
                    .as_deref(),
                worker_id,
            )?;
        }

        let parent = request
            .parent_agent_id
            .as_ref()
            .map(ToString::to_string)
            .or_else(|| state.root_agent_id.clone());
        let agent_id = format!("agent-{}", uuid_like());
        let spec = self
            .lineage_spec(
                &state,
                request.parent_agent_id.as_ref().map(|id| id.value.as_str()),
                conduit.is_some(),
                request.spec.unwrap_or_default(),
            )
            .await?;

        let workspace = self.validated_workspace(&spec).await?;

        // The whole decision — project, place, append — runs under the
        // placement lock so a drain pass woken by a worker registering cannot
        // place this agent between our appends.
        let serialized = self.core.placement_lock.clone();
        let _serialized = serialized.lock().await;

        if let Some(response) = self
            .spawn_retried_or_refused(
                &session_id,
                &request.idempotency_key,
                &state,
                parent.as_deref(),
            )
            .await?
        {
            return Ok(Response::new(response));
        }

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
        let harness = placement::requested_provider(Some(&spec));
        let placement = self.core.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            parent_worker_id: parent_worker.as_deref(),
            workspace_definition_id: workspace.as_deref(),
            harness: harness.as_deref(),
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
                    // A client identity, or nothing: an MCP-initiated spawn
                    // arrives over the worker's channel and its true actor is
                    // the parent agent, which `parent_agent_id` already names.
                    spawned_by: actor,
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
        placement::append_placement(&self.core, &session_id, wire_placement.clone(), Some(&spec))
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
            // The minting client prints one self-contained token (ADR-0007);
            // it owns the host it dialed, this is the listener beside it.
            enrollment_port: u32::from(authority.enrollment_port()),
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

    /// The operator's recovery for a worker whose journal regressed
    /// (ADR-0011): accepts its fresh journal and declares its agents lost.
    #[instrument(skip_all)]
    async fn reset_worker_journal(
        &self,
        request: Request<ResetWorkerJournalRequest>,
    ) -> Result<Response<ResetWorkerJournalResponse>, Status> {
        let actor = actor_of(&request);
        let request = request.into_inner();
        let worker_id = request
            .worker_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let reset =
            crate::liveness::reset_worker_journal(&self.core, &worker_id, &request.reason, actor)
                .await
                .map_err(Status::from)?;
        tracing::warn!(
            worker_id,
            journal_floor = reset.journal_floor,
            lost = reset.lost_agents.len(),
            "worker journal reset"
        );
        Ok(Response::new(ResetWorkerJournalResponse {
            journal_floor: reset.journal_floor,
            lost_agents: reset.lost_agents.into_iter().map(AgentId::new).collect(),
            sequence: reset.sequence,
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

    // -- Port forwards (ADR-0006) ---------------------------------------------

    #[instrument(skip_all)]
    async fn open_forward(
        &self,
        request: Request<horde_proto::v1::OpenForwardRequest>,
    ) -> Result<Response<horde_proto::v1::OpenForwardResponse>, Status> {
        let actor = actor_of(&request);
        let response = super::forwards::open_forward(&self.core, actor, request.into_inner())
            .await
            .map_err(Status::from)?;
        Ok(Response::new(response))
    }

    #[instrument(skip_all)]
    async fn close_forward(
        &self,
        request: Request<horde_proto::v1::CloseForwardRequest>,
    ) -> Result<Response<horde_proto::v1::CloseForwardResponse>, Status> {
        let actor = actor_of(&request);
        let request = request.into_inner();
        let session_id = request
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let forward_id = request
            .forward_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let sequence = super::forwards::close_forward(
            &self.core,
            actor,
            &session_id,
            &forward_id,
            request.reason,
        )
        .await
        .map_err(Status::from)?;
        Ok(Response::new(horde_proto::v1::CloseForwardResponse {
            sequence,
        }))
    }

    #[instrument(skip_all)]
    async fn list_forwards(
        &self,
        request: Request<horde_proto::v1::ListForwardsRequest>,
    ) -> Result<Response<horde_proto::v1::ListForwardsResponse>, Status> {
        let filter = request
            .into_inner()
            .session_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        let response = super::forwards::list_forwards(&self.core, filter)
            .await
            .map_err(Status::from)?;
        Ok(Response::new(response))
    }

    type ServeForwardsStream =
        Pin<Box<dyn Stream<Item = Result<horde_proto::v1::ServeForwardsResponse, Status>> + Send>>;

    /// Backing IS this stream's liveness: registering the lane is the act of
    /// backing, and the lane leaving the registry — however the stream ends —
    /// is what "unbacked" means. The owning client's return re-backs by
    /// simply calling again.
    #[instrument(skip_all)]
    async fn serve_forwards(
        &self,
        request: Request<horde_proto::v1::ServeForwardsRequest>,
    ) -> Result<Response<Self::ServeForwardsStream>, Status> {
        let serial = super::shared::peer_serial(&request);
        let Some(principal) = client_principal_of(&request) else {
            return Err(Status::unauthenticated(
                "backing is keyed to a client principal, and this channel names none; \
                 connect over mTLS",
            ));
        };

        let (epoch, mut lane) = self.core.forwards.register_client(&principal);
        let (sender, receiver) = tokio::sync::mpsc::channel(64);
        let deny = self
            .enrollment
            .as_ref()
            .map(|authority| authority.deny_list());
        let registry = self.core.forwards.clone();
        tokio::spawn(async move {
            let pump = async {
                loop {
                    tokio::select! {
                        // The client hung up. Noticed here rather than on the
                        // next failed send, because deregistering is what
                        // "unbacked" means — a detached backer must refuse
                        // worker-side connections promptly, not after the
                        // next one happens to be pushed.
                        () = sender.closed() => return,
                        connection = lane.recv() => {
                            let Some(connection) = connection else { return };
                            let update = horde_proto::v1::ServeForwardsResponse {
                                connection: Some(connection),
                            };
                            if sender.send(Ok(update)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            };
            super::shared::run_severable(pump, deny, serial, &sender).await;
            registry.deregister_client_if_current(&principal, epoch);
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(receiver))))
    }

    type ForwardTunnelStream =
        Pin<Box<dyn Stream<Item = Result<horde_proto::v1::ForwardTunnelResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn forward_tunnel(
        &self,
        request: Request<tonic::Streaming<horde_proto::v1::ForwardTunnelRequest>>,
    ) -> Result<Response<Self::ForwardTunnelStream>, Status> {
        let deny = self
            .enrollment
            .as_ref()
            .map(|authority| authority.deny_list());
        Ok(super::forwards::tunnel_response(
            &self.core,
            deny,
            request,
            |message: horde_proto::v1::ForwardTunnelRequest| message.frame.unwrap_or_default(),
            |frame| horde_proto::v1::ForwardTunnelResponse { frame: Some(frame) },
        ))
    }

    type WorkerLoginStream =
        Pin<Box<dyn Stream<Item = Result<horde_proto::v1::WorkerLoginResponse, Status>> + Send>>;

    #[instrument(skip_all)]
    async fn worker_login(
        &self,
        request: Request<tonic::Streaming<horde_proto::v1::WorkerLoginRequest>>,
    ) -> Result<Response<Self::WorkerLoginStream>, Status> {
        let deny = self
            .enrollment
            .as_ref()
            .map(|authority| authority.deny_list());
        let core = self.core.clone();
        Ok(super::logins::login_response(
            deny,
            request,
            |message: horde_proto::v1::WorkerLoginRequest| message.frame.unwrap_or_default(),
            |frame| horde_proto::v1::WorkerLoginResponse { frame: Some(frame) },
            Box::new(move |caller, io| {
                Box::pin(async move {
                    super::logins::run_client_login(&core, caller, io).await;
                })
            }),
        ))
    }
}

/// The actor a request's channel identity names, for event attribution.
///
/// Multi-attach is why this exists: "the user did it" is not specific enough
/// once several clients can drive one session (`id.proto`, wave-7 fix a13).
/// A client certificate names the client; a worker channel is a *conduit* —
/// the true actor is an agent the request does not name — so it attributes
/// nothing rather than crediting the machine. Plaintext test channels carry
/// no identity and attribute nothing either.
fn actor_of<T>(request: &Request<T>) -> Option<horde_proto::v1::Actor> {
    let identity = request.extensions().get::<horde_identity::PeerIdentity>()?;
    if identity.principal.role() != horde_identity::PrincipalRole::Client {
        return None;
    }
    Some(horde_proto::v1::Actor {
        actor: Some(horde_proto::v1::actor::Actor::ClientId(
            horde_proto::v1::ClientId::new(identity.principal.name()),
        )),
    })
}

/// The common name (`client:laptop`) of whatever principal a request's
/// channel certificate names — any role — or empty on a plaintext channel.
fn principal_of<T>(request: &Request<T>) -> String {
    request
        .extensions()
        .get::<horde_identity::PeerIdentity>()
        .map(|identity| identity.principal.common_name())
        .unwrap_or_default()
}

/// The client principal name a request's channel carries, if any — the key
/// forward backing is registered under. Same identity rules as [`actor_of`];
/// this is the bare name where that is the wire's `Actor`.
fn client_principal_of<T>(request: &Request<T>) -> Option<String> {
    let identity = request.extensions().get::<horde_identity::PeerIdentity>()?;
    if identity.principal.role() != horde_identity::PrincipalRole::Client {
        return None;
    }
    Some(identity.principal.name().to_owned())
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

/// Refuses a narrowing for an agent whose adapter cannot apply one
/// (ADR-0009). Delivered anyway, the replacement arguments are either dropped
/// — the unnarrowed call runs — or refused by the worker mid-turn; on Codex,
/// which has no `TOOL_INPUT_REWRITE`, the latter took the worker down.
///
/// # Errors
///
/// `FailedPrecondition` naming the agent and the missing capability.
fn refuse_unappliable_narrowing(
    narrowing: bool,
    rewrites_tool_input: bool,
    agent_id: &str,
) -> Result<(), Status> {
    if narrowing && !rewrites_tool_input {
        return Err(Status::failed_precondition(format!(
            "agent {agent_id}'s adapter cannot rewrite tool input \
             (no TOOL_INPUT_REWRITE), so a narrowed decision cannot be applied; \
             allow or deny the call as proposed"
        )));
    }
    Ok(())
}

/// Whether a worker may spawn under `parent` in `state`'s session: the
/// parent must be named — a worker spawning a root-level agent would be
/// acting for nobody — and placed on the calling worker.
///
/// # Errors
///
/// `PermissionDenied` naming the parent and the worker.
fn conduit_may_spawn(
    state: &SessionState,
    parent: Option<&str>,
    worker_id: &str,
) -> Result<(), Status> {
    let parent = parent.filter(|id| !id.is_empty()).ok_or_else(|| {
        Status::permission_denied(format!(
            "worker {worker_id} spawns only on behalf of one of its agents; name the parent"
        ))
    })?;
    let placed_on = state
        .agents
        .get(parent)
        .and_then(|agent| agent.worker_id.as_deref());
    if crate::trust::holds_agent(placed_on, worker_id) {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "agent {parent} is not placed on worker {worker_id}, which may not spawn for it"
        )))
    }
}

/// Whether a worker may message into `state`'s session: only when it is
/// running one of that session's agents, the sender it speaks for.
///
/// # Errors
///
/// `PermissionDenied` naming the session and the worker.
fn conduit_holds_session(state: &SessionState, worker_id: &str, verb: &str) -> Result<(), Status> {
    if state
        .agents
        .values()
        .any(|agent| crate::trust::holds_agent(agent.worker_id.as_deref(), worker_id))
    {
        Ok(())
    } else {
        Err(Status::permission_denied(format!(
            "worker {worker_id} runs no agent in session {} and may not {verb} it",
            state.session_id
        )))
    }
}

/// Why an agent takes no more messages or children, when it takes none
/// (ADR-0011): its session or the agent itself is archived, or it was lost
/// with its worker. Archive is terminal; so is loss, because the context a
/// message would continue died with the worker.
fn ended(state: &SessionState, agent_id: &str) -> Option<String> {
    if state.lifecycle == horde_proto::v1::SessionLifecycleState::Archived {
        return Some(format!(
            "is in archived session {}; archive is terminal",
            state.session_id
        ));
    }
    let agent = state.agents.get(agent_id)?;
    if agent.lifecycle == horde_proto::v1::AgentLifecycleState::Archived {
        return Some("is archived; archive is terminal".to_owned());
    }
    (agent.placement_state == horde_proto::v1::PlacementState::Lost)
        .then(|| "was lost with its worker and its context with it; spawn a replacement".to_owned())
}

/// [`ended`] as a refusal: `FailedPrecondition`, naming the agent and why.
fn refuse_ended(state: &SessionState, agent_id: &str) -> Result<(), Status> {
    match ended(state, agent_id) {
        Some(why) => Err(Status::from(ServerError::AgentEnded {
            agent_id: agent_id.to_owned(),
            why,
        })),
        None => Ok(()),
    }
}

/// Refuses work in an archived session that names no agent to blame.
fn refuse_archived_session(state: &SessionState) -> Result<(), Status> {
    if state.lifecycle == horde_proto::v1::SessionLifecycleState::Archived {
        return Err(Status::failed_precondition(format!(
            "session {} is archived; archive is terminal",
            state.session_id
        )));
    }
    Ok(())
}

/// The reason each agent's archive records.
fn archive_reason(reason: &str) -> String {
    if reason.is_empty() {
        "the session was archived".to_owned()
    } else {
        reason.to_owned()
    }
}

/// The answer a retried spawn gets: the agent its stored `AgentSpawned`
/// created, with the placement it has now. `None` when the stored event is
/// not a spawn — the append that follows refuses that collision by name.
fn spawn_retry(state: &SessionState, stored: &[u8]) -> Option<SpawnAgentResponse> {
    use prost::Message;
    let event = Event::decode(stored).ok()?;
    let Some(Kind::AgentSpawned(_)) = event.kind else {
        return None;
    };
    let agent_id = event.agent_id?.to_string();
    let agent = state.agents.get(&agent_id)?;
    Some(SpawnAgentResponse {
        agent_id: Some(AgentId::new(agent_id.clone())),
        placement: Some(horde_proto::v1::Placement {
            agent_id: Some(AgentId::new(agent_id)),
            state: agent.placement_state as i32,
            worker_id: agent.worker_id.clone().map(horde_proto::v1::WorkerId::new),
            reason: String::new(),
        }),
    })
}

/// The answer a retried send gets: the stored message's own assignment id
/// and position.
///
/// # Errors
///
/// `Internal` when the stored payload does not decode.
fn send_retry(
    session_id: &str,
    sequence: u64,
    stored: &[u8],
) -> Result<SendMessageResponse, Status> {
    use prost::Message;
    let event = Event::decode(stored).map_err(|error| Status::from(ServerError::from(error)))?;
    let assignment_id = match event.kind {
        Some(Kind::AssignmentSent(sent)) => sent.assignment.and_then(|a| a.assignment_id),
        _ => None,
    };
    Ok(SendMessageResponse {
        assignment_id,
        session_id: Some(SessionId::new(session_id)),
        sequence,
    })
}

// `uuid_like` lives in `service::shared` now that the forward verbs mint ids
// too; re-imported here so every existing call site reads unchanged.
use super::shared::uuid_like;

#[cfg(test)]
mod tests {
    use super::actor_of;
    use horde_identity::{PeerIdentity, Principal, PrincipalRole};

    fn request_as(role: Option<PrincipalRole>) -> tonic::Request<()> {
        let mut request = tonic::Request::new(());
        if let Some(role) = role {
            request.extensions_mut().insert(PeerIdentity {
                principal: Principal::new(role, "laptop").expect("a valid name"),
                serial_hex: "aa".to_owned(),
            });
        }
        request
    }

    /// The Hello's echoed principal: whatever the certificate names, any
    /// role, and nobody on a plaintext channel.
    #[test]
    fn the_hello_echoes_the_channels_principal() {
        assert_eq!(
            super::principal_of(&request_as(Some(PrincipalRole::Client))),
            "client:laptop"
        );
        assert_eq!(
            super::principal_of(&request_as(Some(PrincipalRole::Worker))),
            "worker:laptop"
        );
        assert_eq!(super::principal_of(&request_as(None)), "");
    }

    /// Attribution's truth table (fix a13): a client certificate names the
    /// client, a worker channel is a conduit and names nobody, and a
    /// plaintext channel has nobody to name.
    #[test]
    fn only_a_client_identity_becomes_the_events_actor() {
        let actor = actor_of(&request_as(Some(PrincipalRole::Client)))
            .expect("a client identity attributes");
        assert_eq!(
            actor.actor,
            Some(horde_proto::v1::actor::Actor::ClientId(
                horde_proto::v1::ClientId::new("laptop")
            ))
        );

        assert_eq!(
            actor_of(&request_as(Some(PrincipalRole::Worker))),
            None,
            "a worker channel is a conduit; crediting the machine would be \
             attributing the wrong thing"
        );
        assert_eq!(actor_of(&request_as(None)), None);
    }

    /// The narrowing gate's table: only a narrowing for an agent that cannot
    /// rewrite is refused, with a message naming the agent and the fix.
    #[test]
    fn a_narrowing_needs_an_agent_that_can_rewrite() {
        assert!(super::refuse_unappliable_narrowing(false, false, "a").is_ok());
        assert!(super::refuse_unappliable_narrowing(false, true, "a").is_ok());
        assert!(super::refuse_unappliable_narrowing(true, true, "a").is_ok());
        let refused = super::refuse_unappliable_narrowing(true, false, "agent-7")
            .expect_err("a narrowing nobody can apply is refused");
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
        assert!(
            refused.message().contains("agent-7")
                && refused.message().contains("TOOL_INPUT_REWRITE"),
            "{}",
            refused.message()
        );
    }

    /// The conduit scopes, over a projected session: a worker spawns only
    /// for a named parent it holds, and messages only into a session it
    /// runs an agent in.
    #[test]
    fn a_worker_conduit_is_scoped_to_its_own_agents() {
        use horde_proto::v1::{AgentId, Event, event::Kind};

        let placed = crate::placement::wire_placement(
            "parent",
            &crate::scheduler::Placement::Placed {
                worker_id: "w-1".to_owned(),
            },
        );
        let state = crate::state::fixture::projected(&[
            crate::state::fixture::spawned("parent", None),
            Event {
                agent_id: Some(AgentId::new("parent")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::PlacementChanged(horde_proto::v1::PlacementChanged {
                    placement: Some(placed),
                    adapter_capabilities: Vec::new(),
                })),
            },
        ]);

        assert!(super::conduit_may_spawn(&state, Some("parent"), "w-1").is_ok());
        for (parent, worker) in [
            (Some("parent"), "w-2"),
            (None, "w-1"),
            (Some(""), "w-1"),
            (Some("ghost"), "w-1"),
        ] {
            let refused = super::conduit_may_spawn(&state, parent, worker)
                .expect_err("only the holding worker spawns for a named parent");
            assert_eq!(
                refused.code(),
                tonic::Code::PermissionDenied,
                "{parent:?} {worker}"
            );
        }
        assert!(super::conduit_holds_session(&state, "w-1", "read").is_ok());
        let refused = super::conduit_holds_session(&state, "w-2", "message into")
            .expect_err("a worker with no agent here cannot message into it");
        assert!(refused.message().contains("s1"), "{}", refused.message());
        assert!(
            refused.message().contains("message into"),
            "{}",
            refused.message()
        );
    }

    /// Why an agent takes nothing more (ADR-0011): an archived session, an
    /// archived agent, or a lost one — and a live agent, or one the session
    /// never spawned, is not ended.
    #[test]
    fn archived_and_lost_agents_are_ended_and_live_ones_are_not() {
        use crate::state::fixture::{projected, spawned};
        use horde_proto::v1::{
            AgentId, AgentLifecycleChanged, AgentLifecycleState, Event, Placement,
            PlacementChanged, PlacementState, SessionLifecycleChanged, SessionLifecycleState,
            event::Kind,
        };
        let on = |agent: &str, kind: Kind| Event {
            agent_id: Some(AgentId::new(agent)),
            kind: Some(kind),
            ..Event::default()
        };
        let events = vec![
            spawned("live", None),
            spawned("archived", Some("live")),
            spawned("lost", Some("live")),
            on(
                "archived",
                Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    state: AgentLifecycleState::Archived as i32,
                    ..AgentLifecycleChanged::default()
                }),
            ),
            on(
                "lost",
                Kind::PlacementChanged(PlacementChanged {
                    placement: Some(Placement {
                        agent_id: Some(AgentId::new("lost")),
                        state: PlacementState::Lost as i32,
                        ..Placement::default()
                    }),
                    adapter_capabilities: Vec::new(),
                }),
            ),
        ];
        let state = projected(&events);
        assert_eq!(super::ended(&state, "live"), None);
        assert_eq!(super::ended(&state, "never-spawned"), None);
        assert!(
            super::ended(&state, "archived").is_some_and(|why| why.contains("archived")),
            "an archived agent is ended"
        );
        assert!(
            super::ended(&state, "lost").is_some_and(|why| why.contains("lost")),
            "a lost agent is ended"
        );
        let refused = super::refuse_ended(&state, "archived").expect_err("refused");
        assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
        assert!(super::refuse_archived_session(&state).is_ok());

        let mut with_archive = events;
        with_archive.push(Event {
            kind: Some(Kind::SessionLifecycleChanged(SessionLifecycleChanged {
                state: SessionLifecycleState::Archived as i32,
                ..SessionLifecycleChanged::default()
            })),
            ..Event::default()
        });
        let archived_session = projected(&with_archive);
        assert!(
            super::ended(&archived_session, "live").is_some_and(|why| why.contains("session")),
            "everything in an archived session is ended"
        );
        assert_eq!(
            super::refuse_archived_session(&archived_session)
                .expect_err("refused")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[test]
    fn a_session_archive_names_its_reason_or_says_so() {
        assert_eq!(super::archive_reason(""), "the session was archived");
        assert_eq!(super::archive_reason("done"), "done");
    }

    /// The backing key follows the same rules as attribution, and it is the
    /// *name* — misnaming it would register backing under a principal no
    /// inject records, which reads as permanently unbacked.
    #[test]
    fn only_a_client_identity_names_the_backing_principal() {
        assert_eq!(
            super::client_principal_of(&request_as(Some(PrincipalRole::Client))),
            Some("laptop".to_owned()),
            "the exact principal name is the backing key"
        );
        assert_eq!(
            super::client_principal_of(&request_as(Some(PrincipalRole::Worker))),
            None,
            "a worker cannot back; only a human's client can"
        );
        assert_eq!(super::client_principal_of(&request_as(None)), None);
    }
}

/// The ADR-0011 verbs and refusals, through the production handlers.
#[cfg(test)]
mod delivery_tests {
    use horde_identity::{PeerIdentity, Principal, PrincipalRole};
    use horde_proto::v1::control_plane_service_server::ControlPlaneService;
    use horde_proto::v1::{
        AgentId, AgentLifecycleState, ArchiveSessionRequest, Assignment, CreateSessionRequest,
        Note, PlacementState, ResetWorkerJournalRequest, SendMessageRequest, SessionCursor,
        SessionId, SpawnAgentRequest, SyncEventsRequest, WorkerId, send_message_request::Body,
    };
    use tonic::{Code, Request};

    use super::ControlPlane;
    use crate::EventLog;
    use crate::service::shared::ServerCore;
    use crate::state::SessionState;

    fn plane() -> (ControlPlane, ServerCore) {
        let core = ServerCore::new(EventLog::in_memory().expect("in-memory log"));
        (ControlPlane::new(core.clone()), core)
    }

    /// The handshake answers with the principal the channel's certificate
    /// names, through the production handler.
    #[tokio::test]
    async fn the_hello_answer_carries_the_callers_principal() {
        let (plane, _core) = plane();
        let mut request = Request::new(horde_proto::v1::HelloRequest {
            hello: Some(horde_proto::v1::Hello {
                semver: env!("CARGO_PKG_VERSION").to_owned(),
                role: horde_proto::v1::Role::Client as i32,
                ..horde_proto::v1::Hello::default()
            }),
        });
        request.extensions_mut().insert(PeerIdentity {
            principal: Principal::new(PrincipalRole::Client, "laptop").expect("a valid name"),
            serial_hex: "aa".to_owned(),
        });
        let server = plane
            .hello(request)
            .await
            .expect("the handshake answers")
            .into_inner()
            .server
            .expect("a server hello");
        assert_eq!(server.principal, "client:laptop");
    }

    /// `message` as the channel would deliver it: stamped with `role`'s
    /// certificate identity, or carrying none at all.
    fn calling_as<T>(message: T, role: Option<PrincipalRole>, name: &str) -> Request<T> {
        let mut request = Request::new(message);
        if let Some(role) = role {
            request.extensions_mut().insert(PeerIdentity {
                principal: Principal::new(role, name).expect("a valid name"),
                serial_hex: "aa".to_owned(),
            });
        }
        request
    }

    /// Mint and revoke are operator verbs (wave-7 fix a3), and the handler
    /// holds that line itself: a worker certificate, or a request with no
    /// identity at all, is refused `PermissionDenied` naming what it is,
    /// while a client mints. On the wire the interceptor's method table
    /// refuses a worker first; this is the handler's own check behind it, so
    /// a route that skipped the table still finds the door shut.
    #[tokio::test]
    async fn the_enrollment_verbs_refuse_every_caller_but_a_client() {
        use horde_proto::v1::{
            ClientId, MintEnrollmentCodeRequest, RevokeEnrollmentRequest, Role,
            revoke_enrollment_request::Principal as Revoked,
        };
        let authority = crate::service::enrollment::tests::bare_authority().await;
        let plane = ControlPlane::with_enrollment(
            ServerCore::new(EventLog::in_memory().expect("in-memory log")),
            std::sync::Arc::new(authority),
        );
        let mint = || MintEnrollmentCodeRequest {
            role: Role::Worker as i32,
            name: "gpu-box".to_owned(),
            ttl: None,
        };

        let refused = plane
            .mint_enrollment_code(calling_as(
                mint(),
                Some(PrincipalRole::Worker),
                "mac-studio",
            ))
            .await
            .expect_err("a worker identity must not mint");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert!(
            refused.message().contains("worker:mac-studio"),
            "the refusal names the caller: {}",
            refused.message()
        );
        let refused = plane
            .revoke_enrollment(calling_as(
                RevokeEnrollmentRequest {
                    principal: Some(Revoked::ClientId(ClientId::new("laptop"))),
                    reason: "a worker trying to revoke the operator".to_owned(),
                },
                Some(PrincipalRole::Worker),
                "mac-studio",
            ))
            .await
            .expect_err("a worker identity must not revoke");
        assert_eq!(refused.code(), Code::PermissionDenied);
        let refused = plane
            .mint_enrollment_code(calling_as(mint(), None, ""))
            .await
            .expect_err("no identity must not mint");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert!(
            refused.message().contains("no peer identity"),
            "{}",
            refused.message()
        );

        let minted = plane
            .mint_enrollment_code(calling_as(mint(), Some(PrincipalRole::Client), "laptop"))
            .await
            .expect("a client identity mints")
            .into_inner();
        assert!(!minted.code.is_empty(), "the client got a code");
    }

    /// A fresh session: its id and its root agent.
    async fn session(plane: &ControlPlane) -> (String, String) {
        let created = plane
            .create_session(Request::new(CreateSessionRequest {
                title: "delivery".to_owned(),
                ..CreateSessionRequest::default()
            }))
            .await
            .expect("the session is created")
            .into_inner();
        let session_id = created
            .session
            .and_then(|summary| summary.session_id)
            .map(|id| id.to_string())
            .expect("a session id");
        let root = created
            .root_agent_id
            .map(|id| id.to_string())
            .expect("a root agent");
        (session_id, root)
    }

    async fn state(core: &ServerCore, session_id: &str) -> SessionState {
        let events = core.log.read_all(session_id).await.expect("read");
        SessionState::project(session_id, &events).expect("project")
    }

    fn spawn(session_id: &str, key: &str) -> Request<SpawnAgentRequest> {
        Request::new(SpawnAgentRequest {
            session_id: Some(SessionId::new(session_id)),
            idempotency_key: key.to_owned(),
            ..SpawnAgentRequest::default()
        })
    }

    fn send(agent: &str, body: Body, key: &str) -> Request<SendMessageRequest> {
        Request::new(SendMessageRequest {
            to_agent_id: Some(AgentId::new(agent)),
            body: Some(body),
            idempotency_key: key.to_owned(),
            ..SendMessageRequest::default()
        })
    }

    fn assignment(text: &str) -> Body {
        Body::Assignment(Assignment {
            text: text.to_owned(),
            ..Assignment::default()
        })
    }

    /// A retried spawn answers with the first attempt's agent and creates
    /// nothing — not a second agent, not a placement for a phantom.
    #[tokio::test]
    async fn a_retried_spawn_returns_the_agent_the_first_one_made() {
        let (plane, core) = plane();
        let (session_id, _) = session(&plane).await;
        let first = plane
            .spawn_agent(spawn(&session_id, "spawn-1"))
            .await
            .expect("spawned")
            .into_inner();
        let head = core.log.head(&session_id).await.expect("head");
        let again = plane
            .spawn_agent(spawn(&session_id, "spawn-1"))
            .await
            .expect("the retry answers")
            .into_inner();
        assert_eq!(again.agent_id, first.agent_id);
        assert_eq!(
            again.placement.map(|placement| placement.state),
            Some(PlacementState::Queued as i32)
        );
        assert_eq!(
            core.log.head(&session_id).await.expect("head"),
            head,
            "the retry appended nothing"
        );
        assert_eq!(state(&core, &session_id).await.agents.len(), 2);
    }

    /// A retried send answers with the stored assignment id, not a fresh one
    /// the log never held; a note has none either way.
    #[tokio::test]
    async fn a_retried_send_returns_the_stored_ids() {
        let (plane, _) = plane();
        let (_, root) = session(&plane).await;
        let first = plane
            .send_message(send(&root, assignment("do it"), "send-1"))
            .await
            .expect("sent")
            .into_inner();
        let again = plane
            .send_message(send(&root, assignment("do it"), "send-1"))
            .await
            .expect("the retry answers")
            .into_inner();
        assert!(first.assignment_id.is_some());
        assert_eq!(again.assignment_id, first.assignment_id);
        assert_eq!(again.sequence, first.sequence);

        let note = |key: &str| {
            send(
                &root,
                Body::Note(Note {
                    text: "fyi".to_owned(),
                    ..Note::default()
                }),
                key,
            )
        };
        let first = plane
            .send_message(note("note-1"))
            .await
            .expect("sent")
            .into_inner();
        let again = plane
            .send_message(note("note-1"))
            .await
            .expect("again")
            .into_inner();
        assert_eq!(
            (again.assignment_id, again.sequence),
            (None, first.sequence)
        );
    }

    /// Archive is terminal (ADR-0011): every agent records its own archive
    /// with the state it left, placement never places them, and both verbs
    /// refuse by name.
    #[tokio::test]
    async fn archive_is_terminal_for_every_agent_and_every_verb() {
        let (plane, core) = plane();
        let (session_id, root) = session(&plane).await;
        let child = plane
            .spawn_agent(spawn(&session_id, ""))
            .await
            .expect("spawned")
            .into_inner()
            .agent_id
            .map(|id| id.to_string())
            .expect("a child");
        plane
            .archive_session(Request::new(ArchiveSessionRequest {
                session_id: Some(SessionId::new(session_id.clone())),
                reason: String::new(),
            }))
            .await
            .expect("archived");

        let archived = state(&core, &session_id).await;
        for agent in [&root, &child] {
            assert_eq!(
                archived.agents[agent].lifecycle,
                AgentLifecycleState::Archived
            );
        }

        // A worker appearing must not place an archived agent.
        core.workers.register(
            "late-worker",
            horde_proto::v1::WorkerCapabilities::default(),
        );
        crate::placement::place_queued(&core).await.expect("a pass");
        let after = state(&core, &session_id).await;
        assert_ne!(after.agents[&child].placement_state, PlacementState::Placed);

        let refused = plane
            .send_message(send(&root, assignment("too late"), ""))
            .await
            .expect_err("an archived agent takes no messages");
        assert_eq!(refused.code(), Code::FailedPrecondition);
        assert!(
            refused.message().contains("archive"),
            "{}",
            refused.message()
        );
        let refused = plane
            .spawn_agent(spawn(&session_id, ""))
            .await
            .expect_err("an archived session takes no children");
        assert_eq!(refused.code(), Code::FailedPrecondition);
    }

    fn as_worker<T>(name: &str, mut request: Request<T>) -> Request<T> {
        request.extensions_mut().insert(PeerIdentity {
            principal: Principal::new(PrincipalRole::Worker, name).expect("a valid name"),
            serial_hex: "aa".to_owned(),
        });
        request
    }

    fn sync(cursors: &[&str], all_sessions: bool) -> Request<SyncEventsRequest> {
        Request::new(SyncEventsRequest {
            cursors: cursors
                .iter()
                .map(|session| SessionCursor {
                    session_id: Some(SessionId::new(*session)),
                    from_sequence: 1,
                })
                .collect(),
            all_sessions,
            ..SyncEventsRequest::default()
        })
    }

    /// A worker reads only sessions it holds agents in, and never the
    /// all-sessions net (ADR-0011, closing ADR-0009's gap).
    #[tokio::test]
    async fn a_worker_reads_only_sessions_it_holds_agents_in() {
        let (plane, core) = plane();
        let (held, _) = session(&plane).await;
        let (foreign, _) = session(&plane).await;
        crate::service::ingest::placed_for_test(&core, &held, "agent-w", "w1").await;

        assert!(
            plane
                .sync_events(as_worker("w1", sync(&[&held], false)))
                .await
                .is_ok()
        );
        let refused = plane
            .sync_events(as_worker("w1", sync(&[&held, &foreign], false)))
            .await
            .err()
            .expect("a session it holds no agent in is refused");
        assert_eq!(refused.code(), Code::PermissionDenied);
        let refused = plane
            .sync_events(as_worker("w1", sync(&[], true)))
            .await
            .err()
            .expect("the all-sessions net is a client's view");
        assert_eq!(refused.code(), Code::PermissionDenied);
        assert!(
            plane.sync_events(sync(&[&foreign], false)).await.is_ok(),
            "a client reads any session"
        );
    }

    /// The reset verb answers its floor and the agents it lost.
    #[tokio::test]
    async fn the_reset_verb_reports_its_floor_and_losses() {
        let (plane, core) = plane();
        let (session_id, _) = session(&plane).await;
        crate::service::ingest::placed_for_test(&core, &session_id, "agent-w", "w1").await;
        let reset = plane
            .reset_worker_journal(Request::new(ResetWorkerJournalRequest {
                worker_id: Some(WorkerId::new("w1")),
                reason: "disk replaced".to_owned(),
            }))
            .await
            .expect("reset")
            .into_inner();
        assert_eq!(reset.journal_floor, 0);
        assert_eq!(
            reset
                .lost_agents
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            Vec::<String>::new(),
            "an agent never seen running is re-queued, not lost"
        );
        let refused = plane
            .reset_worker_journal(Request::new(ResetWorkerJournalRequest::default()))
            .await
            .expect_err("a reset must name its worker");
        assert_eq!(refused.code(), Code::InvalidArgument);
    }
}

#[cfg(test)]
mod list_tests {
    use horde_proto::v1::control_plane_service_server::ControlPlaneService;
    use horde_proto::v1::{
        ArchiveSessionRequest, CreateSessionRequest, ListSessionsRequest, SessionId,
    };
    use tonic::Request;

    use super::ControlPlane;
    use crate::EventLog;
    use crate::service::shared::ServerCore;

    /// A session titled `title`, created under `key`; its id.
    async fn titled(plane: &ControlPlane, title: &str, key: &str) -> String {
        plane
            .create_session(Request::new(CreateSessionRequest {
                title: title.to_owned(),
                idempotency_key: key.to_owned(),
                ..CreateSessionRequest::default()
            }))
            .await
            .expect("the session is created")
            .into_inner()
            .session
            .and_then(|summary| summary.session_id)
            .map(|id| id.to_string())
            .expect("a session id")
    }

    /// The ids `ListSessions` answers for `request`, in listing order.
    async fn listed(plane: &ControlPlane, request: ListSessionsRequest) -> Vec<String> {
        plane
            .list_sessions(Request::new(request))
            .await
            .expect("the list")
            .into_inner()
            .sessions
            .into_iter()
            .filter_map(|summary| summary.session_id.map(|id| id.to_string()))
            .collect()
    }

    /// The name filter lists exactly the sessions with that name, still
    /// honouring `include_archived`; no name lists every session.
    #[tokio::test]
    async fn the_name_filter_lists_exactly_the_sessions_with_that_name() {
        let plane = ControlPlane::new(ServerCore::new(
            EventLog::in_memory().expect("in-memory log"),
        ));
        let first = titled(&plane, "alpha", "one").await;
        let other = titled(&plane, "beta", "two").await;
        let archived = titled(&plane, "alpha", "three").await;
        plane
            .archive_session(Request::new(ArchiveSessionRequest {
                session_id: Some(SessionId::new(archived.clone())),
                reason: String::new(),
            }))
            .await
            .expect("archived");
        let named = |name: &str, include_archived: bool| ListSessionsRequest {
            name: Some(name.to_owned()),
            include_archived,
            ..ListSessionsRequest::default()
        };

        assert_eq!(
            listed(&plane, named("alpha", false)).await,
            vec![first.clone()]
        );
        assert_eq!(
            listed(&plane, named("alpha", true)).await,
            vec![first.clone(), archived.clone()]
        );
        assert_eq!(
            listed(&plane, named("beta", false)).await,
            vec![other.clone()]
        );
        assert!(
            listed(&plane, named("alph", true)).await.is_empty(),
            "exact, not a prefix"
        );
        assert_eq!(
            listed(&plane, ListSessionsRequest::default()).await,
            vec![first, other],
            "no name is no filter"
        );
    }
}
