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
    /// reactions: one is retried, the other is read. Displayed as the
    /// server's own words under a plain-language code — never tonic's
    /// `Debug`-shaped `status: …, metadata: …` dump.
    #[error("{}", status_text(.0))]
    Rpc(#[from] tonic::Status),

    /// The endpoint string itself is not a URI.
    ///
    /// Separate from [`Self::Connect`] because nothing was attempted: the user
    /// mistyped an address, and telling them the server is unreachable would
    /// send them looking in the wrong place.
    #[error(
        "{endpoint} is not a valid endpoint URI (expected something like https://127.0.0.1:7777)"
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

    /// Enrollment failed: a malformed token, a refused join, or the
    /// identity failing to persist.
    #[error("enrollment failed: {0}")]
    Enrollment(String),

    /// `horde up` could not stand this machine up, or a daemon it started
    /// died — in operator words, with what the daemon said.
    #[error("horde up: {0}")]
    Up(String),

    /// An enrolled identity with no endpoint from any source (ADR-0007).
    ///
    /// A refusal that says where endpoints come from, replacing the old
    /// always-there plaintext default that could never connect to the
    /// secured server it implied.
    #[error(
        "the identity in {directory} stores no endpoint; pass --endpoint, set HORDE_ENDPOINT, \
         or re-join (`horde enroll join <token>` stores the server beside the credential)"
    )]
    NoEndpoint { directory: std::path::PathBuf },

    /// An `https://` endpoint with no identity anywhere (ADR-0007).
    ///
    /// Refused before the dial: attempting it would fail with a TLS error
    /// naming neither the missing identity nor the fix.
    #[error(
        "{endpoint} is an mTLS endpoint but {directory} holds no identity; join first with \
         `horde enroll join <token>`"
    )]
    Unenrolled {
        endpoint: String,
        directory: std::path::PathBuf,
    },

    /// No endpoint from any source and nothing joined (ADR-0007, 2026-10-31
    /// amendment): there is no default server to guess at — the old
    /// plaintext default could never reach a secured one — so the refusal
    /// names the two ways a machine comes to have a server.
    #[error(
        "no server to talk to: no --endpoint, no HORDE_ENDPOINT, and nothing joined in \
         {directory}; join one with `horde enroll join <token>` (a token its operator minted), \
         or stand one up on this machine with `horde up`"
    )]
    NoServer { directory: std::path::PathBuf },

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

    /// An interactive login could not run, was refused, or ended in failure.
    ///
    /// Always in the operator's words — the relay's refusal detail, a taken
    /// callback port, the login command's nonzero exit — because every one
    /// of them has a different remedy and none of them is a transport fault.
    #[error("login failed: {0}")]
    Login(String),

    /// The terminal itself failed the TUI — raw mode refused, a draw failed.
    ///
    /// Its own variant because the remedy is different in kind: every other
    /// error here sends the user toward the server or their own arguments,
    /// and this one means the *local* terminal is not usable (no TTY, a
    /// dumb terminal, a closed pty).
    #[error("the terminal is not usable for the TUI: {0}")]
    Terminal(#[source] std::io::Error),

    /// A session reference matched more than one session (ADR-0008).
    ///
    /// Refused naming every candidate rather than guessed: the next attempt
    /// can be exact — the full name, or a longer id prefix.
    #[error(
        "{reference:?} matches {} sessions:\n{}",
        candidates.len(),
        candidates.join("\n")
    )]
    AmbiguousSession {
        reference: String,
        candidates: Vec<String>,
    },

    /// A bare invocation arrived with no terminal to pick a session in.
    ///
    /// The fast failure ADR-0008 prescribes for headless and `--json`
    /// invocations that would have prompted: the candidate list rides in the
    /// error, so the caller's next invocation can name its session.
    #[error("{}", headless_refusal(candidates))]
    Headless { candidates: Vec<String> },

    /// The chosen session was archived between listing and attaching.
    ///
    /// The archived-while-you-chose race gets a fast named refusal, per the
    /// ADR-0006 philosophy that an explained refusal beats a hang.
    #[error("{reference} was archived while you chose; `horde` again to pick from what is live")]
    ArchivedWhileChoosing { reference: String },

    /// Every name in the word list is taken by a live session.
    ///
    /// Reachable only at sixteen hundred live sessions, but a refusal that
    /// names the remedy still beats an infinite redraw.
    #[error("every generated name is taken by a live session; pass a title instead")]
    NamesExhausted,

    /// A session reference nothing answers to — no name, no id, no prefix,
    /// live or archived.
    ///
    /// Refused client-side because the list resolution consulted is the
    /// whole list: passing the reference on would only earn the server's
    /// less specific "not found" for the same fact.
    #[error(
        "no session is named {reference:?} or has an id starting with it; `horde session list` \
         shows the live ones"
    )]
    UnknownSession { reference: String },

    /// The reference names only an archived session, and the verb was not
    /// asked to look there (ADR-0008: archived sessions stay out of name
    /// resolution unless `--all`).
    #[error("{reference:?} names the archived session {session_id}; pass --all to address it")]
    ArchivedSession {
        reference: String,
        session_id: String,
    },

    /// No searched session's log mentions the agent.
    #[error(
        "no agent {reference:?} in the searched sessions; pass --session <name> to look in one, \
         or --all to search archived sessions too"
    )]
    UnknownAgent { reference: String },

    /// A message to an agent that has ended — failed, lost with its worker
    /// (ADR-0011), or archived. The server refuses it (`FAILED_PRECONDITION`,
    /// `ServerError::AgentEnded`); the CLI refuses with what to do instead
    /// rather than a bare status.
    #[error(
        "cannot send to {agent_id}: {detail}. Its context is gone, so nothing will deliver \
         this; `horde agent status {agent_id}` shows why, and `horde spawn` starts an agent \
         to carry the work on"
    )]
    AgentEnded { agent_id: String, detail: String },

    /// `horde worker add` could not bootstrap the remote machine: a step
    /// failed, or the machine (or the local binary) is not one it serves —
    /// always naming the step and what to do.
    #[error("worker add: {0}")]
    WorkerAdd(String),

    /// `horde doctor` found something to fix; the report above says what.
    #[error("doctor found {failures} problem(s); each FAIL line above names its fix")]
    Unhealthy { failures: usize },
}

impl ClientError {
    /// A send's refusal, read for the one case that needs its own words:
    /// the server's ended-agent `FAILED_PRECONDITION`. Anything else is
    /// returned as it came.
    #[must_use]
    pub fn for_send(self, agent_id: &str) -> Self {
        match self {
            Self::Rpc(status) if status.code() == tonic::Code::FailedPrecondition => {
                Self::AgentEnded {
                    agent_id: agent_id.to_owned(),
                    detail: status.message().to_owned(),
                }
            }
            other => other,
        }
    }
}

/// The plain-language names of the gRPC codes a server answers with, in the
/// order a refusal reads ("not found: …"). A table rather than a match so
/// the phrasing stays data and the function under it stays trivial.
const CODE_PHRASES: &[(tonic::Code, &str)] = &[
    (tonic::Code::Cancelled, "cancelled"),
    (tonic::Code::InvalidArgument, "invalid request"),
    (tonic::Code::DeadlineExceeded, "timed out"),
    (tonic::Code::NotFound, "not found"),
    (tonic::Code::AlreadyExists, "already exists"),
    (tonic::Code::PermissionDenied, "permission denied"),
    (tonic::Code::ResourceExhausted, "limit reached"),
    (tonic::Code::FailedPrecondition, "refused"),
    (tonic::Code::Aborted, "conflict"),
    (tonic::Code::OutOfRange, "out of range"),
    (tonic::Code::Unimplemented, "not built on this server"),
    (tonic::Code::Internal, "server error"),
    (tonic::Code::Unavailable, "server unavailable"),
    (tonic::Code::DataLoss, "data loss"),
    (tonic::Code::Unauthenticated, "not authenticated"),
];

/// A server refusal as one plain line: the code's phrase, then the server's
/// message when it gave one.
#[must_use]
pub fn status_text(status: &tonic::Status) -> String {
    let phrase = CODE_PHRASES
        .iter()
        .find(|(code, _)| *code == status.code())
        .map_or("error", |(_, phrase)| phrase);
    if status.message().is_empty() {
        phrase.to_owned()
    } else {
        format!("{phrase}: {}", status.message())
    }
}

/// `CamelCase` as `snake_case`: the stable machine name a JSON error's
/// `kind` carries, derived from the variant so a new variant names itself.
fn snake_case(camel: &str) -> String {
    let mut out = String::with_capacity(camel.len() + 4);
    for (index, c) in camel.chars().enumerate() {
        if c.is_ascii_uppercase() && index > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_lowercase());
    }
    out
}

/// The headless bare-invocation message: what exists, and what to run next —
/// which differs with whether there is anything to attach at all.
fn headless_refusal(candidates: &[String]) -> String {
    if candidates.is_empty() {
        "no live sessions, and no terminal to create one interactively; \
         `horde new [title] --detach` creates one"
            .to_owned()
    } else {
        format!(
            "no terminal to pick a session in; attach one by name or id:\n{}",
            candidates.join("\n")
        )
    }
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

    /// The actionable cause buried in this error's source chain — a TLS
    /// refusal, nothing listening — in plain words, when there is one
    /// ([`horde_identity::diagnosis`]). A top-level `transport error`
    /// alone sends the operator nowhere.
    #[must_use]
    pub fn diagnosis(&self) -> Option<String> {
        horde_identity::diagnosis::diagnose(self)
    }

    /// The stderr text the binary prints for this refusal: an unbuilt verb
    /// is a roadmap fact (`not built yet: …`), anything else `error: …`,
    /// followed by a `hint:` line when the chain names a cause.
    #[must_use]
    pub fn report(&self) -> String {
        let head = if self.is_unimplemented() {
            format!("not built yet: {self}")
        } else {
            format!("error: {self}")
        };
        match self.diagnosis() {
            Some(hint) => format!("{head}\nhint: {hint}"),
            None => head,
        }
    }

    /// The variant's machine name (`unknown_session`, `rpc`), read off the
    /// `Debug` spelling so it can never disagree with the type.
    #[must_use]
    pub fn kind(&self) -> String {
        let debug = format!("{self:?}");
        let name: String = debug
            .chars()
            .take_while(char::is_ascii_alphanumeric)
            .collect();
        snake_case(&name)
    }

    /// The refusal as one JSON object, for `--json` invocations: a script
    /// branches on `kind` (and `code`, for a server refusal) and reads
    /// `message`, `hint` and any `candidates` without parsing prose.
    #[must_use]
    pub fn report_json(&self) -> String {
        let code = match self {
            Self::Rpc(status) => Some(format!("{:?}", status.code())),
            _ => None,
        };
        let candidates = match self {
            Self::AmbiguousSession { candidates, .. } | Self::Headless { candidates } => {
                Some(candidates.clone())
            }
            _ => None,
        };
        serde_json::json!({
            "error": {
                "kind": self.kind(),
                "message": self.to_string(),
                "code": code,
                "hint": self.diagnosis(),
                "candidates": candidates,
                "unimplemented": self.is_unimplemented(),
            }
        })
        .to_string()
    }

    /// The stderr text for `format`: [`Self::report`] for a terminal,
    /// [`Self::report_json`] under `--json`.
    #[must_use]
    pub fn report_for(&self, format: crate::Format) -> String {
        match format {
            crate::Format::Human => self.report(),
            crate::Format::Json => self.report_json(),
        }
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

    /// Resets a worker whose journal regressed (ADR-0011).
    pub async fn reset_worker_journal(
        &mut self,
        worker_id: &str,
        reason: &str,
    ) -> Result<horde_proto::v1::ResetWorkerJournalResponse, ClientError> {
        Ok(self
            .inner
            .reset_worker_journal(horde_proto::v1::ResetWorkerJournalRequest {
                worker_id: Some(horde_proto::v1::WorkerId::new(worker_id)),
                reason: reason.to_owned(),
            })
            .await?
            .into_inner())
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
            // The shared ceiling (`horde_proto::limits`), so an event the
            // worker spine ingests is one this client can be sent back.
            inner: horde_proto::limited!(ControlPlaneServiceClient::new(channel.clone())),
            channel,
        }
    }

    /// The blob service over this client's channel, at the shared ceiling.
    fn blobs(&self) -> horde_proto::v1::blob_service_client::BlobServiceClient<Channel> {
        horde_proto::limited!(
            horde_proto::v1::blob_service_client::BlobServiceClient::new(self.channel.clone())
        )
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
        Ok(horde_proto::blob::upload_blob(&mut self.blobs(), bytes).await?)
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
        Ok(horde_proto::blob::download_blob(&mut self.blobs(), &digest).await?)
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
        self.list_sessions_filtered(include_archived, None).await
    }

    /// The sessions titled exactly `name`, filtered server-side
    /// (ADR-0013) — what name resolution asks first, instead of listing
    /// everything to find one.
    pub async fn list_sessions_named(
        &mut self,
        name: &str,
        include_archived: bool,
    ) -> Result<Vec<SessionSummary>, ClientError> {
        self.list_sessions_filtered(include_archived, Some(name.to_owned()))
            .await
    }

    async fn list_sessions_filtered(
        &mut self,
        include_archived: bool,
        name: Option<String>,
    ) -> Result<Vec<SessionSummary>, ClientError> {
        Ok(self
            .inner
            .list_sessions(ListSessionsRequest {
                include_archived,
                page_size: 0,
                page_token: String::new(),
                name,
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
        self.send_with_payload(agent_id, false, text, None, delivery, idempotency_key)
            .await
    }

    pub async fn send_note(
        &mut self,
        agent_id: &str,
        text: &str,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        self.send_with_payload(agent_id, true, text, None, delivery, idempotency_key)
            .await
    }

    /// A message whose payload names work: the commits and attachments are
    /// made present in the receiving agent's checkout before the text
    /// reaches it (DESIGN §5 — transport is never the agent's concern).
    /// `note` is the one-bit message kind: a note owes no result.
    pub async fn send_with_payload(
        &mut self,
        agent_id: &str,
        note: bool,
        text: &str,
        payload: Option<horde_proto::v1::Payload>,
        delivery: DeliveryMode,
        idempotency_key: &str,
    ) -> Result<SendMessageResponse, ClientError> {
        let body = if note {
            send_message_request::Body::Note(Note {
                text: text.to_owned(),
                payload,
            })
        } else {
            send_message_request::Body::Assignment(Assignment {
                assignment_id: None,
                text: text.to_owned(),
                payload,
            })
        };
        self.send(agent_id, body, delivery, idempotency_key).await
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

    // -- Port forwards (ADR-0006) ---------------------------------------------

    /// Journals a forward intent. For an inject, the server records this
    /// client's principal as the backer — the initiator is the backer.
    ///
    /// # Errors
    ///
    /// The server's refusals for an incomplete spec, a dead agent, a live
    /// duplicate name, or an unidentifiable backer.
    pub async fn open_forward(
        &mut self,
        session_id: &str,
        agent_id: &str,
        spec: horde_proto::v1::ForwardSpec,
        idempotency_key: &str,
    ) -> Result<horde_proto::v1::OpenForwardResponse, ClientError> {
        Ok(self
            .inner
            .open_forward(horde_proto::v1::OpenForwardRequest {
                session_id: Some(SessionId::new(session_id)),
                agent_id: Some(AgentId::new(agent_id)),
                spec: Some(spec),
                idempotency_key: idempotency_key.to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Closes a forward intent for good.
    pub async fn close_forward(
        &mut self,
        session_id: &str,
        forward_id: &str,
        reason: &str,
    ) -> Result<u64, ClientError> {
        Ok(self
            .inner
            .close_forward(horde_proto::v1::CloseForwardRequest {
                session_id: Some(SessionId::new(session_id)),
                forward_id: Some(horde_proto::v1::ForwardId::new(forward_id)),
                reason: reason.to_owned(),
            })
            .await?
            .into_inner()
            .sequence)
    }

    /// Live forward intents plus their realization overlay — worker
    /// reachability and backing, which the log deliberately does not carry.
    pub async fn list_forwards(
        &mut self,
        session_id: Option<&str>,
    ) -> Result<Vec<horde_proto::v1::ForwardStatus>, ClientError> {
        Ok(self
            .inner
            .list_forwards(horde_proto::v1::ListForwardsRequest {
                session_id: session_id.map(SessionId::new),
            })
            .await?
            .into_inner()
            .forwards)
    }

    /// Backs this client's inject forwards: the returned stream carries one
    /// connection request per worker-side connection, each answered by
    /// dialing the local target and opening a tunnel. Backing IS this
    /// stream's liveness.
    pub async fn serve_forwards(
        &mut self,
    ) -> Result<tonic::Streaming<horde_proto::v1::ServeForwardsResponse>, ClientError> {
        Ok(self
            .inner
            .serve_forwards(horde_proto::v1::ServeForwardsRequest {})
            .await?
            .into_inner())
    }

    /// Opens the interactive-login stream (DESIGN §4), stripped to bare
    /// frames: the caller sends `open` first and pumps keystrokes and PTY
    /// bytes until the login ends.
    pub async fn worker_login(&mut self) -> Result<crate::login::LoginStream, ClientError> {
        use tokio_stream::StreamExt as _;
        let (to_relay, outbound) = tokio::sync::mpsc::channel(16);
        let requests = tokio_stream::wrappers::ReceiverStream::new(outbound)
            .map(|frame| horde_proto::v1::WorkerLoginRequest { frame: Some(frame) });
        let inbound = self.inner.worker_login(requests).await?.into_inner();
        Ok(crate::login::LoginStream {
            to_relay,
            from_relay: Box::pin(
                inbound.map(|message| message.map(|reply| reply.frame.unwrap_or_default())),
            ),
        })
    }

    /// Opens one tunnel leg, stripped to bare frames for the pump.
    pub async fn open_forward_tunnel(&mut self) -> Result<crate::forward::Tunnel, ClientError> {
        use tokio_stream::StreamExt as _;
        let (to_relay, outbound) = tokio::sync::mpsc::channel(16);
        let requests = tokio_stream::wrappers::ReceiverStream::new(outbound)
            .map(|frame| horde_proto::v1::ForwardTunnelRequest { frame: Some(frame) });
        let inbound = self.inner.forward_tunnel(requests).await?.into_inner();
        Ok(crate::forward::Tunnel {
            to_relay,
            from_relay: Box::pin(
                inbound.map(|message| message.map(|reply| reply.frame.unwrap_or_default())),
            ),
        })
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

    /// Defines a workspace: the fixed repo set agents check out (DESIGN §7).
    pub async fn create_workspace_definition(
        &mut self,
        name: &str,
        repos: Vec<horde_proto::v1::WorkspaceRepo>,
        unmanaged_paths: Vec<String>,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ClientError> {
        self.inner
            .create_workspace_definition(horde_proto::v1::CreateWorkspaceDefinitionRequest {
                name: name.to_owned(),
                repos,
                unmanaged_paths,
            })
            .await?
            .into_inner()
            .definition
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no definition came back")))
    }

    /// One definition and its known instances, by id — or by name when no
    /// definition has the id (ids win, so a name that happens to be an id
    /// resolves as the id).
    pub async fn get_workspace_definition(
        &mut self,
        reference: &str,
    ) -> Result<horde_proto::v1::GetWorkspaceDefinitionResponse, ClientError> {
        let by_id = self
            .inner
            .get_workspace_definition(horde_proto::v1::GetWorkspaceDefinitionRequest {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                    reference,
                )),
                name: String::new(),
            })
            .await;
        let status = match by_id {
            Ok(response) => return Ok(response.into_inner()),
            Err(status) => status,
        };
        // A plain branch rather than a match guard: a guard's other side is
        // unreachable through any server this client can talk to (an id
        // lookup only misses as NotFound), and an unreachable arm is an
        // unkillable mutant. The predicate carries the policy; this carries
        // the plumbing.
        if !retry_as_name(&status) {
            return Err(status.into());
        }
        Ok(self
            .inner
            .get_workspace_definition(horde_proto::v1::GetWorkspaceDefinitionRequest {
                workspace_definition_id: None,
                name: reference.to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Every live workspace definition.
    pub async fn list_workspace_definitions(
        &mut self,
    ) -> Result<Vec<horde_proto::v1::WorkspaceDefinition>, ClientError> {
        Ok(self
            .inner
            .list_workspace_definitions(horde_proto::v1::ListWorkspaceDefinitionsRequest {
                page_size: 0,
                page_token: String::new(),
            })
            .await?
            .into_inner()
            .definitions)
    }

    /// Widens a definition's repo set by one repo — the one deliberate way a
    /// repo set changes after creation (DESIGN §7).
    pub async fn add_workspace_repo(
        &mut self,
        workspace_definition_id: &str,
        repo: horde_proto::v1::WorkspaceRepo,
        etag: &str,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ClientError> {
        self.inner
            .add_workspace_repo(horde_proto::v1::AddWorkspaceRepoRequest {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                    workspace_definition_id,
                )),
                repo: Some(repo),
                etag: etag.to_owned(),
            })
            .await?
            .into_inner()
            .definition
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no definition came back")))
    }

    /// Updates a definition's name, layout, or unmanaged paths — never its
    /// repo set (the server refuses adds and removes here by design).
    pub async fn update_workspace_definition(
        &mut self,
        request: horde_proto::v1::UpdateWorkspaceDefinitionRequest,
    ) -> Result<horde_proto::v1::WorkspaceDefinition, ClientError> {
        self.inner
            .update_workspace_definition(request)
            .await?
            .into_inner()
            .definition
            .ok_or_else(|| ClientError::Rpc(tonic::Status::internal("no definition came back")))
    }

    /// Removes a definition at the version `etag` names.
    pub async fn delete_workspace_definition(
        &mut self,
        workspace_definition_id: &str,
        etag: &str,
    ) -> Result<(), ClientError> {
        self.inner
            .delete_workspace_definition(horde_proto::v1::DeleteWorkspaceDefinitionRequest {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                    workspace_definition_id,
                )),
                etag: etag.to_owned(),
            })
            .await?;
        Ok(())
    }

    /// Asks a worker to materialize a workspace instance ahead of any spawn.
    pub async fn prewarm_workspace(
        &mut self,
        workspace_definition_id: &str,
        worker_id: &str,
        session_id: Option<&str>,
    ) -> Result<Option<horde_proto::v1::WorkspaceInstanceReport>, ClientError> {
        Ok(self
            .inner
            .prewarm_workspace(horde_proto::v1::PrewarmWorkspaceRequest {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                    workspace_definition_id,
                )),
                worker_id: Some(horde_proto::v1::WorkerId::new(worker_id)),
                session_id: session_id.map(SessionId::new),
            })
            .await?
            .into_inner()
            .report)
    }

    /// The connected workers, as the registry sees them.
    pub async fn list_workers(
        &mut self,
    ) -> Result<Vec<horde_proto::v1::WorkerSummary>, ClientError> {
        Ok(self
            .inner
            .list_workers(horde_proto::v1::ListWorkersRequest {
                include_offline: false,
            })
            .await?
            .into_inner()
            .workers)
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

/// Whether an id lookup's failure means "no definition has this id" — the
/// one failure a name lookup can still answer. Anything else (a dead server,
/// a malformed request) must propagate: retrying it as a name would bury the
/// real refusal under a second, more confusing one. A named predicate so the
/// distinction is a truth table rather than a guard no test can reach both
/// sides of.
fn retry_as_name(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::NotFound
}

/// One meaningful thing a follow stream can say.
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
    /// The server saying "still alive, nothing to say". Surfaced rather
    /// than absorbed (fix a9): the heartbeat exists so quiet and dead are
    /// distinguishable, and a client that swallowed it here left no
    /// component able to make the distinction — the audit's collision #6.
    Heartbeat,
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
                Some(sync_events_response::Update::Heartbeat(_)) => {
                    return Ok(Some(StreamUpdate::Heartbeat));
                }
                None => {}
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

    /// The name filter asks the server for exactly the sessions titled so —
    /// not everything, and not nothing.
    #[tokio::test]
    async fn the_named_listing_returns_exactly_the_titled_sessions() {
        let (_core, mut client) = crate::forward::testing::served().await;
        for (title, key) in [("alpha", "k-1"), ("beta", "k-2")] {
            client
                .create_session(title, key)
                .await
                .expect("a session creates");
        }
        let named = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.list_sessions_named("beta", false),
        )
        .await
        .expect("bounded")
        .expect("lists");
        assert_eq!(
            named.iter().map(|s| s.title.as_str()).collect::<Vec<_>>(),
            vec!["beta"]
        );
        assert!(
            client
                .list_sessions_named("gamma", true)
                .await
                .expect("lists")
                .is_empty()
        );
    }

    /// A server refusal reads as plain words — the code's phrase and the
    /// server's message — never tonic's `status: …, metadata: …` dump.
    #[test]
    fn server_refusals_read_as_plain_text() {
        let refused = ClientError::Rpc(tonic::Status::not_found("no session s1"));
        assert_eq!(refused.to_string(), "not found: no session s1");
        assert_eq!(
            super::status_text(&tonic::Status::failed_precondition("")),
            "refused"
        );
        for (code, phrase) in super::CODE_PHRASES {
            let text = super::status_text(&tonic::Status::new(*code, "m"));
            assert_eq!(text, format!("{phrase}: m"));
            assert!(!text.contains("metadata"), "{text}");
        }
        assert_eq!(
            super::status_text(&tonic::Status::new(tonic::Code::Unknown, "odd")),
            "error: odd",
            "a code without a phrase still reads plainly"
        );
    }

    /// Under `--json` a refusal is one object a script branches on: the
    /// variant's machine name, the gRPC code for a server refusal, the
    /// message, and any candidates.
    #[test]
    fn refusals_have_a_json_form() {
        let rpc = ClientError::Rpc(tonic::Status::unimplemented("later"));
        let json: serde_json::Value = serde_json::from_str(&rpc.report_json()).expect("json");
        assert_eq!(json["error"]["kind"], "rpc");
        assert_eq!(json["error"]["code"], "Unimplemented");
        assert_eq!(json["error"]["message"], "not built on this server: later");
        assert_eq!(json["error"]["unimplemented"], true);
        assert_eq!(json["error"]["candidates"], serde_json::Value::Null);

        let ambiguous = ClientError::AmbiguousSession {
            reference: "deploy".to_owned(),
            candidates: vec!["deploy  s1".to_owned(), "deploy  s2".to_owned()],
        };
        let json: serde_json::Value =
            serde_json::from_str(&ambiguous.report_for(crate::Format::Json)).expect("json");
        assert_eq!(json["error"]["kind"], "ambiguous_session");
        assert_eq!(json["error"]["code"], serde_json::Value::Null);
        assert_eq!(json["error"]["candidates"][1], "deploy  s2");
        let headless = ClientError::Headless {
            candidates: vec!["a".to_owned()],
        };
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&headless.report_json()).expect("json")["error"]
                ["candidates"][0],
            "a"
        );
        assert_eq!(ClientError::NamesExhausted.kind(), "names_exhausted");
        assert_eq!(
            ambiguous.report_for(crate::Format::Human),
            ambiguous.report(),
            "a terminal gets the prose"
        );
    }

    /// A send refused because the agent ended becomes the refusal that says
    /// what to do; any other refusal passes through untouched.
    #[test]
    fn an_ended_agents_send_refusal_names_the_way_forward() {
        let ended = ClientError::Rpc(tonic::Status::failed_precondition("agent-a has failed"))
            .for_send("agent-a");
        let message = ended.to_string();
        assert!(
            message.starts_with("cannot send to agent-a: agent-a has failed")
                && message.contains("horde agent status agent-a")
                && message.contains("horde spawn"),
            "{message}"
        );
        let other = ClientError::Rpc(tonic::Status::not_found("x")).for_send("agent-a");
        assert!(matches!(other, ClientError::Rpc(_)), "{other:?}");
    }

    /// Minutes to wire seconds, exactly: a ttl silently divided or offset
    /// would mint codes that die early or linger.
    #[test]
    fn the_mint_ttl_flag_is_minutes_of_sixty_seconds() {
        assert_eq!(super::ttl_of(None), None, "unset lets the server choose");
        let five = super::ttl_of(Some(5)).expect("five minutes");
        assert_eq!((five.seconds, five.nanos), (300, 0));
    }

    /// The id-lookup fallback's truth table: only NotFound retries as a
    /// name; every other refusal is the real answer and must propagate.
    #[test]
    fn only_a_missing_id_retries_the_lookup_as_a_name() {
        assert!(super::retry_as_name(&tonic::Status::not_found("no ws")));
        assert!(!super::retry_as_name(&tonic::Status::unavailable("down")));
        assert!(!super::retry_as_name(&tonic::Status::invalid_argument(
            "bad"
        )));
        assert!(!super::retry_as_name(&tonic::Status::internal("oops")));
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
