//! Typed calls against `ControlPlaneService`.
//!
//! One method per verb, each returning the wire type rather than a string, so
//! the CLI and the future TUI can render the same answer differently without
//! either of them re-deriving it.

use horde_proto::v1::{
    AgentId, AgentSpec, AnswerPermissionRequest, ArchiveSessionRequest, Assignment,
    CreateSessionRequest, DeliveryMode, EventEnvelope, GetSessionRequest, Hello, HelloRequest,
    InterruptAgentRequest, ListSessionsRequest, Note, PermissionOutcome, PermissionRequestId, Role,
    SendMessageRequest, SendMessageResponse, ServerHello, SessionCursor, SessionId, SessionSummary,
    SpawnAgentRequest, SpawnAgentResponse, SyncEventsRequest,
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
        Ok(Self {
            inner: ControlPlaneServiceClient::new(channel),
        })
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
                ttl: ttl_minutes.map(|minutes| horde_proto::prost_types::Duration {
                    seconds: i64::from(minutes) * 60,
                    nanos: 0,
                }),
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
            inner: ControlPlaneServiceClient::new(channel),
        }
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
        let response = self
            .inner
            .create_session(CreateSessionRequest {
                title: title.to_owned(),
                workspace_definition_id: None,
                root_agent: Some(AgentSpec::default()),
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
        self.send(
            agent_id,
            send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: text.to_owned(),
                payload: None,
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
        self.send(
            agent_id,
            send_message_request::Body::Note(Note {
                text: text.to_owned(),
                payload: None,
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
        Ok(self
            .inner
            .spawn_agent(SpawnAgentRequest {
                session_id: Some(SessionId::new(session_id)),
                parent_agent_id: parent_agent_id.map(AgentId::new),
                spec: Some(AgentSpec::default()),
                idempotency_key: idempotency_key.to_owned(),
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
        subtree_root: None,
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
    /// Events from a batch not yet handed out one at a time.
    queued: std::collections::VecDeque<EventEnvelope>,
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
    /// Replay finished at this head; everything after is live. A frontend
    /// stops showing its loading state here instead of guessing.
    CaughtUp(u64),
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
        if let Some(event) = self.queued.pop_front() {
            return Ok(Some(StreamUpdate::Event(Box::new(event))));
        }
        while let Some(message) = self.inner.next().await {
            match message?.update {
                Some(sync_events_response::Update::Batch(batch)) => {
                    self.queued.extend(batch.events);
                    if let Some(event) = self.queued.pop_front() {
                        return Ok(Some(StreamUpdate::Event(Box::new(event))));
                    }
                    // An empty batch said nothing; keep reading.
                }
                Some(sync_events_response::Update::CaughtUp(caught_up)) => {
                    let head = caught_up
                        .heads
                        .first()
                        .map_or(0, |cursor| cursor.from_sequence);
                    return Ok(Some(StreamUpdate::CaughtUp(head)));
                }
                Some(sync_events_response::Update::Heartbeat(_)) | None => {}
            }
        }
        Ok(None)
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

/// What to replay, and whether to keep listening.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncOptions {
    pub session_id: String,
    /// First sequence wanted. Zero is normalized to 1 by the server.
    pub from_sequence: u64,
    /// False replays to head and completes — the scriptable form.
    pub follow: bool,
}

impl SyncOptions {
    #[must_use]
    pub fn new(session_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            from_sequence: 0,
            follow: false,
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
}

#[cfg(test)]
mod tests {
    use super::{ClientError, SyncOptions};

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
