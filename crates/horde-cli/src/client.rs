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
    /// Dials a server.
    ///
    /// Plaintext: mTLS arrives with enrollment in wave 5, and DESIGN.md §3
    /// forbids binding beyond localhost until it does, so there is nothing to
    /// secure yet and pretending otherwise would be theatre.
    pub async fn connect(endpoint: &str) -> Result<Self, ClientError> {
        // A bare word like `localhost:7777` or `not-a-uri` parses happily as a
        // path-only URI, so `from_shared` alone would accept it and the failure
        // would surface as "could not reach the server" — sending the user to
        // look for a server that was never addressed. Requiring a scheme and an
        // authority puts the complaint where the mistake is.
        let parsed: tonic::codegen::http::Uri =
            endpoint.parse().map_err(|_| ClientError::Endpoint {
                endpoint: endpoint.to_owned(),
            })?;
        if parsed.scheme().is_none() || parsed.authority().is_none() {
            return Err(ClientError::Endpoint {
                endpoint: endpoint.to_owned(),
            });
        }

        let channel = Channel::from_shared(endpoint.to_owned())
            .map_err(|_| ClientError::Endpoint {
                endpoint: endpoint.to_owned(),
            })?
            .connect()
            .await
            .map_err(|source| ClientError::Connect {
                endpoint: endpoint.to_owned(),
                source,
            })?;

        Ok(Self {
            inner: ControlPlaneServiceClient::new(channel),
        })
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
    pub async fn answer_permission(
        &mut self,
        request_id: &str,
        outcome: PermissionOutcome,
        reason: &str,
        remember_for_session: bool,
    ) -> Result<u64, ClientError> {
        Ok(self
            .inner
            .answer_permission(AnswerPermissionRequest {
                request_id: Some(PermissionRequestId::new(request_id)),
                outcome: outcome as i32,
                reason: reason.to_owned(),
                remember_for_session,
                // The CLI has no way to author a narrowing yet — there is no
                // flag that would carry replacement arguments — so this is
                // honestly absent rather than a dropped value. A frontend that
                // grows one needs a parameter here; until then "allow as
                // proposed" is the only thing this verb can mean.
                updated_input: None,
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
            .sync_events(SyncEventsRequest {
                cursors: vec![SessionCursor {
                    session_id: Some(SessionId::new(options.session_id.clone())),
                    from_sequence: options.from_sequence,
                }],
                all_sessions: false,
                subtree_root: None,
                follow: options.follow,
                max_batch_events: 0,
            })
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
