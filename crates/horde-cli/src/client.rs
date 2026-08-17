//! Typed calls against `ControlPlaneService`.
//!
//! One method per verb, each returning the wire type rather than a string, so
//! the CLI and the future TUI can render the same answer differently without
//! either of them re-deriving it.

use horde_proto::v1::{
    AgentId, AgentSpec, AnswerPermissionRequest, ArchiveSessionRequest, Assignment,
    ConfigureMcpServerRequest, CreateSessionRequest, DeleteMcpServerRequest, DeliveryMode,
    EventEnvelope, GetSessionRequest, Hello, HelloRequest, InterruptAgentRequest,
    ListMcpServersRequest, ListSessionsRequest, McpServerConfig, Note, PermissionOutcome,
    PermissionRequestId, Role, SendMessageRequest, SendMessageResponse, ServerHello, SessionCursor,
    SessionId, SessionSummary, SpawnAgentRequest, SpawnAgentResponse, SyncEventsRequest,
    control_plane_service_client::ControlPlaneServiceClient, send_message_request,
    sync_events_response,
};
use tokio_stream::StreamExt;
use tonic::transport::Channel;

/// What can go wrong talking to a server.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("could not reach the server at {endpoint}: {source}")]
    Connect {
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    /// The server answered, and said no.
    ///
    /// Kept distinct from a transport failure because the two need different
    /// reactions: one is retried, the other is read.
    #[error("{0}")]
    Rpc(#[from] tonic::Status),

    /// The endpoint string itself is not a URI.
    ///
    /// Separate from [`Self::Connect`] because nothing was attempted: the user
    /// mistyped an address, and telling them the server is unreachable would
    /// send them looking in the wrong place.
    #[error(
        "{endpoint} is not a valid endpoint URI (expected something like http://127.0.0.1:7777)"
    )]
    Endpoint { endpoint: String },

    /// The replacement arguments on a narrowing answer cannot cross the wire.
    ///
    /// Refused before the RPC rather than sent as a plain allow: the whole
    /// point of a narrowing is that the *narrowed* call runs, so an answer
    /// whose narrowing was dropped would permit the original, wider call —
    /// the failure `permission.proto` calls out by name.
    #[error("the narrowing on this answer cannot be sent: {source}")]
    Narrowing {
        #[source]
        source: horde_wire::permission::ConversionError,
    },

    /// The replacement arguments were not valid JSON.
    ///
    /// Caught before the RPC: a typo belongs to the person who typed it, and
    /// reporting it as a server refusal would send them looking at the server.
    #[error("the replacement arguments are not valid JSON: {source}")]
    NarrowingJson {
        #[source]
        source: serde_json::Error,
    },

    /// The identity directory did not yield a usable identity.
    ///
    /// Distinct from a connection failure because the remedy is local:
    /// enroll (or point `--identity` at where the enrollment landed) rather
    /// than chase the server.
    #[error("no usable identity at {directory}: {source}")]
    Identity {
        directory: std::path::PathBuf,
        #[source]
        source: horde_identity::IdentityError,
    },

    /// Enrollment failed: a malformed code, a refused join, or the identity
    /// failing to persist.
    #[error("enrollment failed: {0}")]
    Enrollment(String),

    /// A local file the verb needed could not be read or written.
    ///
    /// Names the path because the remedy is local — a typo'd filename, a
    /// missing directory, a permissions problem — not anything the server
    /// can explain.
    #[error("could not use the local file {path}: {source}")]
    File {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// A blob address the user gave is not `sha256:<64 lowercase hex>`.
    ///
    /// Caught before the RPC, like the JSON typo above: the mistake is in
    /// the argument, and reporting it as a server refusal would send the
    /// user to look at a server that was never asked.
    #[error("{given:?} is not a blob address (expected sha256:<64 lowercase hex>)")]
    BlobAddress { given: String },

    /// The terminal itself failed the TUI — raw mode refused, a draw failed.
    ///
    /// Its own variant because the remedy is different in kind: every other
    /// error here sends the user toward the server or their own arguments,
    /// and this one means the *local* terminal is not usable (no TTY, a
    /// dumb terminal, a closed pty).
    #[error("the terminal is not usable for the TUI: {0}")]
    Terminal(#[source] std::io::Error),
}

impl ClientError {
    /// Whether the server understood the request but has not built the feature.
    ///
    /// Surfaced separately so the CLI can say "not built yet" rather than
    /// printing a bare error — an unbuilt verb is a roadmap fact, not a fault.
    #[must_use]
    pub fn is_unimplemented(&self) -> bool {
        matches!(self, Self::Rpc(status) if status.code() == tonic::Code::Unimplemented)
    }
}

/// A connected control-plane client.
#[derive(Clone, Debug)]
pub struct Client {
    inner: ControlPlaneServiceClient<Channel>,
    /// The same channel, kept so sibling services on the server's one
    /// endpoint (the blob store) are reachable without a second dial.
    channel: Channel,
}

impl Client {
    /// Dials a server in plaintext — the in-process and loopback-test shape.
    ///
    /// The secured server refuses this: its listener requires the client
    /// certificate [`Self::connect_with_identity`] presents (DESIGN §3).
    pub async fn connect(endpoint: &str) -> Result<Self, ClientError> {
        let endpoint = validated_endpoint(endpoint)?;
        Self::connect_endpoint(endpoint).await
    }

    /// Dials a server over mTLS as an enrolled identity (DESIGN §3).
    ///
    /// The TLS name is the endpoint's own host: what the user dialed is what
    /// the Server's credential must answer for.
    pub async fn connect_with_identity(
        endpoint: &str,
        identity: &horde_identity::StoredIdentity,
    ) -> Result<Self, ClientError> {
        let channel = validated_endpoint(endpoint)?;
        let domain = channel.uri().host().unwrap_or("localhost").to_owned();
        let channel = channel
            .tls_config(horde_identity::tls::grpc_client_mtls(identity, &domain))
            .map_err(|source| ClientError::Connect {
                endpoint: endpoint.to_owned(),
                source,
            })?;
        Self::connect_endpoint(channel).await
    }

    async fn connect_endpoint(endpoint: tonic::transport::Endpoint) -> Result<Self, ClientError> {
        let described = endpoint.uri().to_string();
        let channel = endpoint
            .connect()
            .await
            .map_err(|source| ClientError::Connect {
                endpoint: described,
                source,
            })?;
        Ok(Self::with_channel(channel))
    }

    /// Mints a one-time enrollment code (DESIGN §3). Requires an enrolled,
    /// connected client — the server refuses this verb on any other channel.
    pub async fn mint_enrollment_code(
        &mut self,
        role: horde_proto::v1::Role,
        name: &str,
        ttl_minutes: Option<u32>,
    ) -> Result<horde_proto::v1::MintEnrollmentCodeResponse, ClientError> {
        Ok(self
            .inner
            .mint_enrollment_code(horde_proto::v1::MintEnrollmentCodeRequest {
                role: role as i32,
                name: name.to_owned(),
                ttl: ttl_of(ttl_minutes),
            })
            .await?
            .into_inner())
    }

    /// Revokes every active certificate a principal holds. Returns the
    /// revoked serials, comma-joined as the server reports them.
    pub async fn revoke_enrollment(
        &mut self,
        principal: horde_proto::v1::revoke_enrollment_request::Principal,
        reason: &str,
    ) -> Result<String, ClientError> {
        Ok(self
            .inner
            .revoke_enrollment(horde_proto::v1::RevokeEnrollmentRequest {
                principal: Some(principal),
                reason: reason.to_owned(),
            })
            .await?
            .into_inner()
            .revoked_serial)
    }

    /// Builds a client over a channel the caller already has.
    ///
    /// The seam for anything that owns its transport rather than wanting one
    /// dialled: a frontend embedding a server, a custom connector, a pooled or
    /// pre-warmed channel. [`Self::connect`] is this plus the dialling, so this
    /// is the wider of the two and the one a test enters through.
    #[must_use]
    pub fn with_channel(channel: Channel) -> Self {
        Self {
            inner: ControlPlaneServiceClient::new(channel.clone()),
            channel,
        }
    }

    /// Stashes bytes into the server's Blob Store, returning the verified
    /// digest and size an `Attachment` (or a later fetch) would use.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] with the server's refusal — including
    /// `Unimplemented` from a server running without a store.
    pub async fn stash_blob(
        &mut self,
        bytes: &[u8],
    ) -> Result<horde_proto::v1::UploadBlobResponse, ClientError> {
        let mut blobs =
            horde_proto::v1::blob_service_client::BlobServiceClient::new(self.channel.clone());
        Ok(horde_proto::blob::upload_blob(&mut blobs, bytes).await?)
    }

    /// Fetches one blob by address, verified against it before returning.
    ///
    /// # Errors
    ///
    /// [`ClientError::BlobAddress`] when `address` is malformed — a local
    /// complaint, nothing was dialed — and [`ClientError::Rpc`] for the
    /// server's refusals: `NotFound`, or `DataLoss` when the store's copy
    /// no longer matches its address.
    pub async fn fetch_blob(&mut self, address: &str) -> Result<Vec<u8>, ClientError> {
        let digest = horde_proto::blob::BlobDigest::parse(address).ok_or_else(|| {
            ClientError::BlobAddress {
                given: address.to_owned(),
            }
        })?;
        let mut blobs =
            horde_proto::v1::blob_service_client::BlobServiceClient::new(self.channel.clone());
        Ok(horde_proto::blob::download_blob(&mut blobs, &digest).await?)
    }

    /// Version and capability handshake.
    pub async fn hello(&mut self, client_semver: &str) -> Result<ServerHello, ClientError> {
        let response = self
            .inner
            .hello(HelloRequest {
                hello: Some(Hello {
                    semver: client_semver.to_owned(),
                    capability_flags: Vec::new(),
                    role: Role::Client as i32,
                    display_name: "horde-cli".to_owned(),
                }),
            })
            .await?
            .into_inner();

        response.server.ok_or_else(|| {
            ClientError::Rpc(tonic::Status::internal(
                "the server's handshake carried no ServerHello",
            ))
        })
    }

    pub async fn create_session(
        &mut self,
        title: &str,
        idempotency_key: &str,
    ) -> Result<(SessionSummary, AgentId), ClientError> {
        self.create_session_with(title, AgentSpec::default(), idempotency_key)
            .await
    }

    /// Creates a session whose root agent is what `root_agent` says — type,
    /// tier, workspace, checkout policy, permission mode, first assignment.
    /// The everyday `create_session` is this with a default spec.
    pub async fn create_session_with(
        &mut self,
        title: &str,
        root_agent: AgentSpec,
        idempotency_key: &str,
    ) -> Result<(SessionSummary, AgentId), ClientError> {
        let response = self
            .inner
            .create_session(CreateSessionRequest {
                title: title.to_owned(),
                // Set either way, per the field's contract: the root spec's
                // workspace is the one the session references.
                workspace_definition_id: root_agent.workspace_definition_id.clone(),
                root_agent: Some(root_agent),
                idempotency_key: idempotency_key.to_owned(),
            })
            .await?
            .into_inner();

        let summary = response.session.ok_or_else(|| {
            ClientError::Rpc(tonic::Status::internal(
                "the created session had no summary",
            ))
        })?;
        let root = response.root_agent_id.ok_or_else(|| {
            ClientError::Rpc(tonic::Status::internal(
                "the created session had no root agent",
            ))
        })?;
        Ok((summary, root))
    }

    pub async fn list_sessions(
        &mut self,
        include_archived: bool,
    ) -> Result<Vec<SessionSummary>, ClientError> {
        Ok(self
            .inner
            .list_sessions(ListSessionsRequest {
                include_archived,
                page_size: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner()
            .sessions)
    }

    pub async fn get_session(&mut self, session_id: &str) -> Result<SessionSummary, ClientError> {
        self.inner
            .get_session(GetSessionRequest {
                session_id: Some(SessionId::new(session_id)),
            })
            .await?
            .into_inner()
            .session
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no summary in the response")))
    }

    pub async fn archive_session(
        &mut self,
        session_id: &str,
        reason: &str,
    ) -> Result<SessionSummary, ClientError> {
        self.inner
            .archive_session(ArchiveSessionRequest {
                session_id: Some(SessionId::new(session_id)),
                reason: reason.to_owned(),
            })
            .await?
            .into_inner()
            .session
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no summary in the response")))
    }

    pub async fn send_assignment(
        &mut self,
        agent_id: &str,
        text: &str,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        self.send_assignment_with_payload(agent_id, text, None, delivery, idempotency_key)
            .await
    }

    /// An assignment whose message names work: the payload's commits and
    /// attachments are made present in the receiving agent's checkout before
    /// the text reaches it (DESIGN §5 — transport is never the agent's
    /// concern).
    pub async fn send_assignment_with_payload(
        &mut self,
        agent_id: &str,
        text: &str,
        payload: Option<horde_proto::v1::Payload>,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        self.send(
            agent_id,
            send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: text.to_owned(),
                payload,
            }),
            delivery,
            idempotency_key,
        )
        .await
    }

    pub async fn send_note(
        &mut self,
        agent_id: &str,
        text: &str,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        self.send_note_with_payload(agent_id, text, None, delivery, idempotency_key)
            .await
    }

    /// A note carrying a payload — context that rides as commits or
    /// attachments rather than prose.
    pub async fn send_note_with_payload(
        &mut self,
        agent_id: &str,
        text: &str,
        payload: Option<horde_proto::v1::Payload>,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        self.send(
            agent_id,
            send_message_request::Body::Note(Note {
                text: text.to_owned(),
                payload,
            }),
            delivery,
            idempotency_key,
        )
        .await
    }

    async fn send(
        &mut self,
        agent_id: &str,
        body: send_message_request::Body,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        Ok(self
            .inner
            .send_message(SendMessageRequest {
                to_agent_id: Some(AgentId::new(agent_id)),
                body: Some(body),
                delivery: delivery as i32,
                idempotency_key: idempotency_key.to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Creates or updates one MCP server in the Hub catalog (ADR-0004).
    ///
    /// An empty `config.etag` creates; a non-empty one updates the version it
    /// was read from. The answer is the config as journaled, carrying the etag
    /// its change produced.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`]: `AlreadyExists` for a create over a live name,
    /// `Aborted` for a stale etag, `InvalidArgument` for a config the server
    /// refused.
    pub async fn configure_mcp_server(
        &mut self,
        config: McpServerConfig,
    ) -> Result<McpServerConfig, ClientError> {
        self.inner
            .configure_mcp_server(ConfigureMcpServerRequest {
                config: Some(config),
            })
            .await?
            .into_inner()
            .config
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no config in the response")))
    }

    /// Every live MCP server in the Hub catalog.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] if the server refused.
    pub async fn list_mcp_servers(&mut self) -> Result<Vec<McpServerConfig>, ClientError> {
        Ok(self
            .inner
            .list_mcp_servers(ListMcpServersRequest {})
            .await?
            .into_inner()
            .servers)
    }

    /// Removes one MCP server from the Hub catalog.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`]: `NotFound` for a name the catalog does not hold,
    /// `Aborted` for a stale etag.
    pub async fn delete_mcp_server(&mut self, name: &str, etag: &str) -> Result<(), ClientError> {
        self.inner
            .delete_mcp_server(DeleteMcpServerRequest {
                name: name.to_owned(),
                etag: etag.to_owned(),
            })
            .await?;
        Ok(())
    }

    pub async fn interrupt(&mut self, agent_id: &str, reason: &str) -> Result<u64, ClientError> {
        Ok(self
            .inner
            .interrupt_agent(InterruptAgentRequest {
                agent_id: Some(AgentId::new(agent_id)),
                reason: reason.to_owned(),
            })
            .await?
            .into_inner()
            .sequence)
    }

    /// Answers a human-layer permission escalation.
    ///
    /// # Errors
    ///
    /// [`ClientError::Narrowing`] when the answer carries replacement arguments
    /// the wire cannot represent, and the RPC's own failures otherwise.
    pub async fn answer_permission(
        &mut self,
        answer: PermissionAnswer<'_>,
    ) -> Result<u64, ClientError> {
        // Converted through the one place that owns this correspondence rather
        // than hand-rolled: a narrowing that silently altered an argument on
        // its way to the wire would be a widened permission wearing a
        // narrowing's clothes.
        let updated_input = match answer.updated_input {
            Some(value) => {
                let decision =
                    horde_adapter::permission::PermissionDecision::allow_with(value.clone());
                horde_wire::permission::permission_decision_to_wire(&decision)
                    .map_err(|source| ClientError::Narrowing { source })?
                    .updated_input
            }
            None => None,
        };

        Ok(self
            .inner
            .answer_permission(AnswerPermissionRequest {
                request_id: Some(PermissionRequestId::new(answer.request_id)),
                outcome: answer.outcome as i32,
                reason: answer.reason.to_owned(),
                remember_for_session: answer.remember_for_session,
                updated_input,
            })
            .await?
            .into_inner()
            .sequence)
    }

    pub async fn spawn_agent(
        &mut self,
        session_id: &str,
        parent_agent_id: Option<&str>,
        idempotency_key: &str,
    ) -> Result<SpawnAgentResponse, ClientError> {
        self.spawn_agent_with_spec(
            session_id,
            parent_agent_id,
            AgentSpec::default(),
            idempotency_key,
        )
        .await
    }

    /// [`Self::spawn_agent`], with a caller-built spec.
    ///
    /// The seam the MCP endpoint's `spawn_agent` verb speaks through: an
    /// agent-initiated spawn carries its task as the spec's initial
    /// assignment, and a spec-less spawn is just this with the default.
    pub async fn spawn_agent_with_spec(
        &mut self,
        session_id: &str,
        parent_agent_id: Option<&str>,
        spec: AgentSpec,
        idempotency_key: &str,
    ) -> Result<SpawnAgentResponse, ClientError> {
        Ok(self
            .inner
            .spawn_agent(SpawnAgentRequest {
                session_id: Some(SessionId::new(session_id)),
                parent_agent_id: parent_agent_id.map(AgentId::new),
                spec: Some(spec),
                idempotency_key: idempotency_key.to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Usage and headroom, folded server-side from the journaled usage
    /// events (DESIGN §12).
    pub async fn query_usage(
        &mut self,
        options: &UsageOptions,
    ) -> Result<horde_proto::v1::QueryUsageResponse, ClientError> {
        Ok(self
            .inner
            .query_usage(horde_proto::v1::QueryUsageRequest {
                session_ids: options.session_ids.iter().map(SessionId::new).collect(),
                subtree_root: options.subtree_root.clone().map(AgentId::new),
                group_by: options.group_by as i32,
                since: None,
                until: None,
            })
            .await?
            .into_inner())
    }

    /// Opens a follow stream and hands it back before reading anything.
    ///
    /// The subscribe half of subscribe-before-write. See [`EventStream`] for
    /// why the two steps are separate.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] if the subscription is refused.
    pub async fn follow(&mut self, options: &SyncOptions) -> Result<EventStream, ClientError> {
        let inner = self
            .inner
            .sync_events(sync_request(options, true))
            .await?
            .into_inner();

        Ok(EventStream {
            inner,
            queued: std::collections::VecDeque::new(),
        })
    }

    /// Opens *one* follow stream carrying every named session from its
    /// cursor, plus every other session — current and future — from its head.
    ///
    /// This is the multi-session overview and the global inbox as a single
    /// subscription: name the sessions whose history you need replayed, and
    /// the `all_sessions` net carries everything else live, including
    /// sessions created after the call. One stream, where the TUI used to
    /// fan in one per session.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] if the subscription is refused.
    pub async fn follow_everything(
        &mut self,
        cursors: &[(String, u64)],
    ) -> Result<EventStream, ClientError> {
        let request = SyncEventsRequest {
            cursors: cursors
                .iter()
                .map(|(session_id, from_sequence)| SessionCursor {
                    session_id: Some(SessionId::new(session_id.clone())),
                    from_sequence: *from_sequence,
                })
                .collect(),
            all_sessions: true,
            subtree_root: None,
            follow: true,
            max_batch_events: 0,
        };
        let inner = self.inner.sync_events(request).await?.into_inner();
        Ok(EventStream {
            inner,
            queued: std::collections::VecDeque::new(),
        })
    }

    /// Replays a session from an offset, then optionally follows it.
    ///
    /// Each event is handed to `on_event` as it arrives rather than collected,
    /// so `--follow` streams to stdout instead of buffering a session that may
    /// never end. `on_caught_up` fires once, between history and live, which is
    /// what lets a frontend stop showing a loading state without guessing.
    pub async fn sync<E, C>(
        &mut self,
        options: &SyncOptions,
        mut on_event: E,
        mut on_caught_up: C,
    ) -> Result<u64, ClientError>
    where
        E: FnMut(&EventEnvelope),
        C: FnMut(u64),
    {
        let mut stream = self
            .inner
            .sync_events(sync_request(options, options.follow))
            .await?
            .into_inner();

        // The highest sequence seen, so a caller that reconnects resumes from
        // exactly here — the client half of the log's offset contract.
        let mut highest = options.from_sequence.saturating_sub(1);

        while let Some(message) = stream.next().await {
            match message?.update {
                Some(sync_events_response::Update::Batch(batch)) => {
                    for event in &batch.events {
                        highest = highest.max(event.sequence);
                        on_event(event);
                    }
                }
                Some(sync_events_response::Update::CaughtUp(caught_up)) => {
                    let head = caught_up
                        .heads
                        .first()
                        .map_or(highest, |cursor| cursor.from_sequence);
                    on_caught_up(head);
                }
                // Heartbeats exist so a quiet connection is distinguishable
                // from a dead one; they carry nothing to render.
                Some(sync_events_response::Update::Heartbeat(_)) | None => {}
            }
        }

        Ok(highest)
    }
}

/// A minutes flag as the wire duration a mint carries; `None` lets the
/// server choose.
fn ttl_of(ttl_minutes: Option<u32>) -> Option<horde_proto::prost_types::Duration> {
    ttl_minutes.map(|minutes| horde_proto::prost_types::Duration {
        seconds: i64::from(minutes) * 60,
        nanos: 0,
    })
}

/// An endpoint string that names a scheme and an authority, as a dialable
/// endpoint.
///
/// A bare word like `localhost:7777` or `not-a-uri` parses happily as a
/// path-only URI, so `from_shared` alone would accept it and the failure
/// would surface as "could not reach the server" — sending the user to look
/// for a server that was never addressed. Requiring a scheme and an
/// authority puts the complaint where the mistake is.
fn validated_endpoint(endpoint: &str) -> Result<tonic::transport::Endpoint, ClientError> {
    let refused = || ClientError::Endpoint {
        endpoint: endpoint.to_owned(),
    };
    let parsed: tonic::codegen::http::Uri = endpoint.parse().map_err(|_| refused())?;
    if parsed.scheme().is_none() || parsed.authority().is_none() {
        return Err(refused());
    }
    Channel::from_shared(endpoint.to_owned()).map_err(|_| refused())
}

/// The request one set of options asks for.
///
/// Shared by [`Client::sync`] and [`Client::follow`] because the cursor is the
/// same question either way — only whether to keep listening differs, and a
/// second copy of this would be a second place for `all_sessions` to be wrong.
fn sync_request(options: &SyncOptions, follow: bool) -> SyncEventsRequest {
    SyncEventsRequest {
        cursors: vec![SessionCursor {
            session_id: Some(SessionId::new(options.session_id.clone())),
            from_sequence: options.from_sequence,
        }],
        all_sessions: false,
        subtree_root: options.subtree_root.clone().map(AgentId::new),
        follow,
        max_batch_events: 0,
    }
}

/// A live event stream, already open.
///
/// Separate from [`Client::sync`] because of *when* it exists. `sync` opens a
/// stream and drains it in one call, which is right for replay and for
/// `--follow`, and wrong for anyone who must not miss the answer to something
/// they are about to write: between deciding to write and starting to listen
/// there is a window, and the answer can land inside it.
///
/// This is the two-step shape that closes that window — open the stream, do the
/// write, then read. The server's own log tail is built the same way and for
/// the same reason.
///
/// Dropping this cancels the subscription; there is no separate close.
#[derive(Debug)]
pub struct EventStream {
    inner: tonic::Streaming<horde_proto::v1::SyncEventsResponse>,
    /// Updates from a frame not yet handed out one at a time.
    queued: std::collections::VecDeque<StreamUpdate>,
}

/// One meaningful thing a follow stream can say.
///
/// Heartbeats are absorbed below this surface: they exist to keep the
/// connection distinguishable from a dead one, and a consumer that had to
/// skip them itself would be one more loop with a skip-arm to get wrong.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamUpdate {
    /// The next event, in log order. Boxed: an envelope dwarfs the marker
    /// variant, and every update would pay its size otherwise.
    Event(Box<EventEnvelope>),
    /// One session's replay finished at this head; everything after it is
    /// live. A frontend stops showing that session's loading state here
    /// instead of guessing. A merged stream yields one of these per followed
    /// session.
    CaughtUp { session_id: String, head: u64 },
}

impl EventStream {
    /// The next event or catch-up marker, or `Ok(None)` when the stream ends.
    ///
    /// Batches are handed out one event at a time so a consumer folds a
    /// single shape; the batching is the transport's business, not the
    /// view's.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] if the stream itself fails.
    pub async fn next_update(&mut self) -> Result<Option<StreamUpdate>, ClientError> {
        loop {
            if let Some(update) = self.queued.pop_front() {
                return Ok(Some(update));
            }
            let Some(message) = self.inner.next().await else {
                return Ok(None);
            };
            match message?.update {
                Some(sync_events_response::Update::Batch(batch)) => {
                    self.queued.extend(
                        batch
                            .events
                            .into_iter()
                            .map(|event| StreamUpdate::Event(Box::new(event))),
                    );
                    // An empty batch said nothing; the loop keeps reading.
                }
                Some(sync_events_response::Update::CaughtUp(caught_up)) => {
                    // One marker per followed session: a merged stream's
                    // consumer needs to know *which* replay finished.
                    self.queued
                        .extend(caught_up.heads.into_iter().map(|cursor| {
                            StreamUpdate::CaughtUp {
                                session_id: cursor
                                    .session_id
                                    .as_ref()
                                    .map(ToString::to_string)
                                    .unwrap_or_default(),
                                head: cursor.from_sequence,
                            }
                        }));
                }
                Some(sync_events_response::Update::Heartbeat(_)) | None => {}
            }
        }
    }

    /// Reads forward until `matches` yields a value.
    ///
    /// `Ok(None)` means the stream ended without a match — the server shut down
    /// or the session went away. It is not a timeout: nothing here imposes one,
    /// because the thing being waited for may legitimately take as long as a
    /// human takes to answer. A caller that needs a deadline wraps this in one,
    /// and a caller that needs to give up drops the stream.
    ///
    /// # Errors
    ///
    /// [`ClientError::Rpc`] if the stream itself fails.
    pub async fn next_matching<F, T>(&mut self, mut matches: F) -> Result<Option<T>, ClientError>
    where
        F: FnMut(&EventEnvelope) -> Option<T>,
    {
        while let Some(update) = self.next_update().await? {
            // A catch-up marker only says history ended — it is not an answer,
            // and it does not end the wait.
            if let StreamUpdate::Event(event) = update
                && let Some(found) = matches(&event)
            {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }
}

/// How a human answered a permission escalation.
///
/// A struct rather than five positional arguments, matching [`SyncOptions`]:
/// the two booleans and two strings are otherwise indistinguishable at a call
/// site, and `remember_for_session` silently swapped with `deny` is a mistake
/// the compiler could not catch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionAnswer<'a> {
    pub request_id: &'a str,
    pub outcome: PermissionOutcome,
    pub reason: &'a str,
    /// Widen the deterministic-allow layer for the rest of the Session.
    pub remember_for_session: bool,
    /// Replacement arguments, when the human narrowed the call rather than
    /// merely permitting it — clamping a path, dropping a flag.
    ///
    /// `None` means "allow as proposed". `Some` of an empty object means
    /// "allow with no arguments at all", which is a different instruction, and
    /// the distinction survives all the way to the harness.
    pub updated_input: Option<&'a serde_json::Value>,
}

impl<'a> PermissionAnswer<'a> {
    /// Allow the call exactly as the Agent proposed it.
    #[must_use]
    pub fn allow(request_id: &'a str) -> Self {
        Self {
            request_id,
            outcome: PermissionOutcome::Allow,
            reason: "",
            remember_for_session: false,
            updated_input: None,
        }
    }

    /// Refuse the call, telling the Agent why.
    #[must_use]
    pub fn deny(request_id: &'a str, reason: &'a str) -> Self {
        Self {
            request_id,
            outcome: PermissionOutcome::Deny,
            reason,
            remember_for_session: false,
            updated_input: None,
        }
    }

    /// Allow the call, but with these arguments instead of the proposed ones.
    #[must_use]
    pub fn narrowed(mut self, updated_input: &'a serde_json::Value) -> Self {
        self.updated_input = Some(updated_input);
        self
    }

    #[must_use]
    pub fn remembering(mut self, remember: bool) -> Self {
        self.remember_for_session = remember;
        self
    }

    #[must_use]
    pub fn because(mut self, reason: &'a str) -> Self {
        self.reason = reason;
        self
    }
}

/// What a usage query covers, and how its rows group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageOptions {
    /// Empty means every session.
    pub session_ids: Vec<String>,
    /// Restrict to one agent's subtree — tree-level attribution.
    pub subtree_root: Option<String>,
    pub group_by: horde_proto::v1::UsageGroupBy,
}

impl UsageOptions {
    #[must_use]
    pub fn grouped_by(group_by: horde_proto::v1::UsageGroupBy) -> Self {
        Self {
            session_ids: Vec::new(),
            subtree_root: None,
            group_by,
        }
    }

    #[must_use]
    pub fn sessions(mut self, session_ids: Vec<String>) -> Self {
        self.session_ids = session_ids;
        self
    }

    #[must_use]
    pub fn scoped_to_subtree(mut self, root: Option<String>) -> Self {
        self.subtree_root = root;
        self
    }
}

/// What to replay, and whether to keep listening.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncOptions {
    pub session_id: String,
    /// First sequence wanted. Zero is normalized to 1 by the server.
    pub from_sequence: u64,
    /// False replays to head and completes — the scriptable form.
    pub follow: bool,
    /// Restrict the stream to this agent's subtree, server-side. `None` is
    /// session-root attachment, which sees everything (DESIGN §11).
    pub subtree_root: Option<String>,
}

impl SyncOptions {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            from_sequence: 0,
            follow: false,
            subtree_root: None,
        }
    }

    #[must_use]
    pub fn from_sequence(mut self, from_sequence: u64) -> Self {
        self.from_sequence = from_sequence;
        self
    }

    #[must_use]
    pub fn following(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }

    /// Scope the stream to one agent's subtree.
    #[must_use]
    pub fn scoped_to(mut self, agent_id: impl Into<String>) -> Self {
        self.subtree_root = Some(agent_id.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientError, SyncOptions};

    /// Minutes to wire seconds, exactly: a ttl silently divided or offset
    /// would mint codes that die early or linger.
    #[test]
    fn the_mint_ttl_flag_is_minutes_of_sixty_seconds() {
        assert_eq!(super::ttl_of(None), None, "unset lets the server choose");
        let five = super::ttl_of(Some(5)).expect("five minutes");
        assert_eq!((five.seconds, five.nanos), (300, 0));
    }

    #[test]
    fn sync_options_default_to_a_full_scriptable_replay() {
        let options = SyncOptions::new("s1");
        assert_eq!(options.from_sequence, 0, "zero means from the beginning");
        assert!(!options.follow, "the default completes rather than hanging");
    }

    #[test]
    fn sync_options_build_up_without_disturbing_each_other() {
        let options = SyncOptions::new("s1").from_sequence(7).following(true);
        assert_eq!(options.session_id, "s1");
        assert_eq!(options.from_sequence, 7);
        assert!(options.follow);
    }

    /// Each of these is a plausible typo that `http::Uri` would otherwise
    /// accept as a path-only URI, producing a connection error for an address
    /// that was never dialable.
    #[tokio::test]
    async fn an_endpoint_without_a_scheme_and_authority_is_refused_before_dialing() {
        for typo in [
            "not a uri at all",
            "not-a-uri",
            "127.0.0.1:7777",
            "/sessions",
        ] {
            match super::Client::connect(typo).await {
                Err(ClientError::Endpoint { endpoint }) => assert_eq!(endpoint, typo),
                Err(other) => panic!(
                    "{typo} has no scheme or authority, so it must be refused as an endpoint \
                     rather than reported as unreachable; got: {other}"
                ),
                Ok(_) => panic!("{typo} is not dialable and must not yield a client"),
            }
        }
    }

    /// An unbuilt verb is a roadmap fact rather than a fault, and the CLI needs
    /// to tell the two apart to say so.
    #[test]
    fn unimplemented_is_distinguishable_from_other_failures() {
        let unimplemented = ClientError::Rpc(tonic::Status::unimplemented("workspace definitions"));
        assert!(unimplemented.is_unimplemented());

        let denied = ClientError::Rpc(tonic::Status::not_found("no session s1"));
        assert!(!denied.is_unimplemented());
    }
}
