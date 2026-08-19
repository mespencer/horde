//! What is endpoint-shaped about the shared server harness.
//!
//! The harness itself — both spines on a loopback socket, session seeding,
//! and the worker-shaped journal push — lives in `horde-server-testkit`,
//! because `horde-cli` and `horde-server` test against the same arrangement
//! and three private copies of a transport harness drift. What stays here is
//! what only this crate wants: the [`JournalSink`] backed by that push, the
//! contexts the verbs need, and identities minted through the real
//! [`AgentRegistry`].
//!
//! Each test binary compiles its own copy and uses a subset, so items unused
//! by one file are dead code there — hence the module-scoped allow.
#![allow(dead_code, unreachable_pub)]

use horde_cli::client::Client;
use horde_mcp::context::AgentContext;
use horde_mcp::identity::{AgentIdentity, AgentRegistry};
use horde_mcp::journal::{JournalEntry, JournalError, JournalSink};

pub use horde_server_testkit::Setup;

/// A journal that pushes through the real `WorkerService`.
///
/// The in-memory fake in `journal::testing` proves the endpoint's half of the
/// seam in isolation. This one exists because these tests need the event to
/// actually reach the log: an escalation nobody can see is not an escalation,
/// and a result nobody can read is not a result.
#[derive(Debug, Clone)]
pub struct ServerJournal {
    endpoint: String,
    next: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[async_trait::async_trait]
impl JournalSink for ServerJournal {
    async fn record(&self, entry: JournalEntry) -> Result<u64, JournalError> {
        let sequence = self
            .next
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(1);

        let event = horde_proto::v1::Event {
            agent_id: entry.agent_id.as_deref().map(horde_proto::v1::AgentId::new),
            occurred_at: None,
            idempotency_key: entry.idempotency_key.clone(),
            kind: Some(entry.kind.clone()),
        };
        horde_server_testkit::journal_event_at(&self.endpoint, &entry.session_id, sequence, event)
            .await
            .map_err(|error| JournalError::Unavailable(error.to_string()))?;
        Ok(sequence)
    }
}

impl ServerJournal {
    #[must_use]
    pub fn new(endpoint: &str) -> Self {
        Self {
            endpoint: endpoint.to_owned(),
            next: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

/// A running server, a session on it, and the agent the calls speak for.
pub struct Harness {
    pub endpoint: String,
    pub session_id: String,
    pub agent_id: String,
}

impl Harness {
    /// A journal that writes into this harness's server.
    #[must_use]
    pub fn journal(&self) -> std::sync::Arc<dyn JournalSink> {
        std::sync::Arc::new(ServerJournal::new(&self.endpoint))
    }

    /// Another client onto the same server.
    pub async fn client(&self) -> Setup<Client> {
        Ok(Client::connect(&self.endpoint).await?)
    }

    /// A context wired to this harness — the thing every verb needs.
    ///
    /// Each call builds a fresh one, which is what a test wanting two
    /// independent actors (a reporter and a waiter, say) needs.
    pub async fn context(&self) -> Setup<AgentContext> {
        Ok(AgentContext::new(
            self.journal(),
            self.client().await?,
            self.session_id.clone(),
        ))
    }

    /// A context whose journal points nowhere, for testing the unwritable case.
    pub async fn context_with_dead_journal(&self) -> Setup<AgentContext> {
        Ok(AgentContext::new(
            std::sync::Arc::new(ServerJournal::new("http://127.0.0.1:1")),
            self.client().await?,
            self.session_id.clone(),
        ))
    }
}

/// A harness, an authenticated identity for its agent, and the verbs.
///
/// Three lines that opened almost every verb test identically, which the
/// duplication gate flagged. Sharing them also means a test reads as what it is
/// testing rather than as four lines of arrangement.
pub async fn verbs_for_one_agent() -> Setup<(Harness, AgentIdentity, horde_mcp::verbs::Verbs)> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;
    let verbs = horde_mcp::verbs::Verbs::new(harness.context().await?);
    Ok((harness, identity, verbs))
}

/// Stands up both spines, creates a session, and returns what tests need.
pub async fn harness() -> Setup<Harness> {
    seeded_on(horde_server_testkit::serve().await?).await
}

/// [`harness`], with the server's permission chain live under `modes` and an
/// optional Policy Judge — the arrangement the chain e2e opts into.
pub async fn harness_with_permissions(
    modes: horde_server::permission::PermissionModes,
    judge: Option<std::sync::Arc<dyn horde_server::permission::PolicyJudge>>,
) -> Setup<Harness> {
    seeded_on(horde_server_testkit::serve_with_permissions(modes, judge).await?).await
}

/// Seeds one session on an already-running server.
async fn seeded_on(endpoint: String) -> Setup<Harness> {
    let (_client, session_id, root) = horde_server_testkit::seeded(&endpoint, "endpoint").await?;

    Ok(Harness {
        endpoint,
        session_id,
        agent_id: root,
    })
}

/// A harness-shaped prompt, the way the Claude permission tool would send it.
pub fn prompt_request(
    tool: &str,
    input: serde_json::Value,
    tool_use_id: &str,
) -> Setup<horde_adapter_claude::permission::PromptRequest> {
    Ok(serde_json::from_value(serde_json::json!({
        "tool_name": tool,
        "input": input,
        "tool_use_id": tool_use_id,
    }))?)
}

/// Unwraps the decision from the harness-facing tool response.
///
/// The harness wants the decision stringified inside a text content block, not
/// as a nested object. Getting this wrong is invisible until a real agent
/// silently ignores every decision.
pub fn decode_tool_response(response: &serde_json::Value) -> Setup<serde_json::Value> {
    let text = response["content"][0]["text"]
        .as_str()
        .ok_or("the decision must be a string inside a text block")?;
    Ok(serde_json::from_str(text)?)
}

/// The request id of a `PermissionRequested` envelope, if that is what it is.
///
/// The matcher every suite that answers an escalation hands to
/// `next_matching`: the HTTP round trip and the live smoke both watch the log
/// for the request they are about to decide.
#[must_use]
pub fn escalation_request_id(envelope: &horde_proto::v1::EventEnvelope) -> Option<String> {
    let horde_proto::v1::event::Kind::PermissionRequested(requested) =
        envelope.event.as_ref()?.kind.as_ref()?
    else {
        return None;
    };
    requested
        .request
        .as_ref()?
        .request_id
        .as_ref()
        .map(ToString::to_string)
}

/// An authenticated identity for `agent_id`.
///
/// Built through the real [`AgentRegistry`] rather than constructed directly —
/// `AgentIdentity` has no public constructor precisely so that holding one is
/// proof a token matched, and a test that forged one would be testing a type
/// that does not exist in production.
pub fn identity_for(agent_id: &str) -> Setup<AgentIdentity> {
    let registry = AgentRegistry::new();
    registry.insert(agent_id, "token");

    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "Bearer token".parse()?);
    headers.insert("x-horde-agent", agent_id.parse()?);
    Ok(registry.authenticate(&headers)?)
}

/// A real in-process MCP server for the hub suites to consume as an upstream.
///
/// Not a mock: this is rmcp's own `ServerHandler` machinery — the same code
/// path a user's stdio server runs — served over an in-memory duplex, so the
/// handshake, listing, and calls are all the real protocol with no process
/// or socket. The toolset is the `mcp_stub_server` fixture's own, included
/// by `#[path]` so the child-process path (which spawns that binary) and
/// this in-memory path can never drift apart.
// Declared here rather than inside `stub_mcp` because `#[path]` resolves
// from the declaring module's directory, and an inline module's directory
// (`tests/common/stub_mcp/`) does not exist on disk — the traversal through
// it fails before `..` can walk back out.
#[path = "../../src/bin/mcp_stub_server/tools.rs"]
mod stub_tools;

pub mod stub_mcp {
    use super::Setup;

    // `pub(crate)` in the file (the bin host's clippy demands it), so the
    // re-export matches rather than widening it.
    pub(crate) use super::stub_tools::StubTools;

    /// The stub server binary, where cargo built it for this test run.
    pub const BINARY: &str = env!("CARGO_BIN_EXE_mcp_stub_server");

    /// How long the operating system may take to admit [`BINARY`]'s first
    /// launch — an OS bound, deliberately distinct from the harness's
    /// 5-second wait ceiling, which stays for every protocol wait.
    ///
    /// The cost this bounds is real and was measured: macOS validates a
    /// freshly linked binary on its first-ever exec (code-signature and
    /// XProtect assessment, cached per file afterwards), which took 327ms
    /// against 8ms warm on an idle machine — and multi-second when the
    /// validation service is contended by concurrent builds minting new
    /// binaries, exactly what a cargo-mutants run does. Kept well under
    /// cargo-mutants' 60-second test floor so a genuinely stuck launch is
    /// still this harness's loud failure, never the gate's opaque TIMEOUT.
    const FIRST_LAUNCH_CEILING: std::time::Duration = std::time::Duration::from_secs(30);

    /// Pays [`BINARY`]'s first-launch cost once per test process, before any
    /// suite puts an MCP handshake behind it.
    ///
    /// Every cargo-mutants scratch tree links a brand-new stub binary, so a
    /// suite whose tests each spawn it concurrently pays the OS's first-exec
    /// validation *inside* every test's wait ceiling at once — which is how
    /// the hub e2e baseline failed 0/6 at its ceiling on a loaded machine
    /// while the same tree passed everywhere warm. Warming is serialized by
    /// the cell: one launch pays, the rest find the OS's per-file cache.
    ///
    /// # Errors
    ///
    /// The binary failing to execute at all, or the OS not admitting it
    /// within [`FIRST_LAUNCH_CEILING`] — both named, neither a hang.
    pub async fn warm() -> Setup<()> {
        static WARMED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        WARMED.get_or_try_init(first_launch).await?;
        Ok(())
    }

    /// One execution of [`BINARY`] with its stdio closed: the stub sees EOF
    /// and exits immediately. The exit status is deliberately ignored — that
    /// the process *executed* is the whole payment.
    async fn first_launch() -> Setup<()> {
        let run = tokio::process::Command::new(BINARY)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        match tokio::time::timeout(FIRST_LAUNCH_CEILING, run).await {
            Ok(executed) => {
                let _exit_of_an_eof_fed_stub = executed?;
                Ok(())
            }
            Err(_elapsed) => Err(format!(
                "the stub binary's first launch did not finish within \
                 {FIRST_LAUNCH_CEILING:?} ({BINARY}); the OS admits a freshly \
                 built binary only after validating it, and that validation \
                 is starved or stuck"
            )
            .into()),
        }
    }

    /// A connected [`Upstream`](horde_mcp::hub::Upstream) over a real rmcp
    /// server, in memory. The serving half is parked on its own task for the
    /// life of the test process.
    pub async fn upstream(name: &str) -> Setup<horde_mcp::hub::Upstream> {
        use rmcp::ServiceExt;

        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        // The serving half runs on its own task: `serve` completes the
        // handshake, so awaiting it inline would deadlock against the client
        // half of the same duplex. Parked with `waiting`, not dropped —
        // RunningService cancels itself when dropped.
        tokio::spawn(async move {
            let Ok(serving) = StubTools::new().serve(server_io).await else {
                return;
            };
            let _ = serving.waiting().await;
        });

        Ok(horde_mcp::hub::Upstream::over(name, client_io).await?)
    }
}

/// A legacy-era MCP client, one identity, one session.
///
/// Deliberately small — the session flow the dual-era spike recorded from
/// Claude Code 2.1.228: `initialize`, the session id riding `Mcp-Session-Id`,
/// `notifications/initialized`, then the calls. The subject of every test
/// driving it is the *server* side of the wire, so the client stays exactly
/// as sophisticated as the recorded traffic and no more.
pub mod mcp_client {
    use super::Setup;

    pub struct McpClient {
        pub address: std::net::SocketAddr,
        pub agent_id: String,
        pub token: String,
        session: Option<String>,
    }

    impl McpClient {
        /// Initializes against the endpoint the way a legacy harness does,
        /// and keeps the session the server assigned.
        pub async fn connect(
            address: std::net::SocketAddr,
            agent_id: &str,
            token: &str,
        ) -> Setup<Self> {
            let mut client = Self {
                address,
                agent_id: agent_id.to_owned(),
                token: token.to_owned(),
                session: None,
            };

            let (parts, body) = client
                .post(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": { "name": "endpoint-test", "version": "0.0.0" },
                    },
                }))
                .await?;
            if !parts.status.is_success() {
                return Err(format!("initialize was refused {}: {body}", parts.status).into());
            }
            let session = parts
                .headers
                .get("mcp-session-id")
                .and_then(|value| value.to_str().ok())
                .ok_or("a legacy-era initialize is assigned a session id")?
                .to_owned();
            client.session = Some(session);

            // The notification the lifecycle requires between initialize and
            // use.
            let (parts, body) = client
                .post(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                }))
                .await?;
            if !parts.status.is_success() {
                return Err(format!("initialized was refused {}: {body}", parts.status).into());
            }

            Ok(client)
        }

        /// One JSON-RPC POST, answered with the response head and raw body.
        pub async fn post(
            &self,
            body: &serde_json::Value,
        ) -> Setup<(http::response::Parts, String)> {
            let stream = tokio::net::TcpStream::connect(self.address).await?;
            let (mut sender, connection) =
                hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await?;
            // The connection future drives the socket; the request below
            // resolves through it.
            tokio::spawn(connection);

            let mut request = http::Request::builder()
                .method("POST")
                .uri("/mcp")
                .header("host", self.address.to_string())
                .header("content-type", "application/json")
                .header("accept", "application/json, text/event-stream")
                .header("authorization", format!("Bearer {}", self.token))
                .header("x-horde-agent", self.agent_id.clone());
            if let Some(session) = &self.session {
                request = request.header("mcp-session-id", session.clone());
            }

            let response =
                sender
                    .send_request(request.body(http_body_util::Full::new(
                        hyper::body::Bytes::from(body.to_string()),
                    ))?)
                    .await?;
            let (parts, body) = response.into_parts();
            let bytes = http_body_util::BodyExt::collect(body).await?.to_bytes();
            Ok((parts, String::from_utf8(bytes.to_vec())?))
        }

        /// One request, answered with the decoded JSON-RPC message.
        ///
        /// The session flow answers over SSE; a stateless answer is plain
        /// JSON. Same JSON-RPC message either way.
        pub async fn rpc(&self, body: &serde_json::Value) -> Setup<serde_json::Value> {
            let (parts, text) = self.post(body).await?;
            if !parts.status.is_success() {
                return Err(format!("the endpoint answered {}: {text}", parts.status).into());
            }
            let payload = match text
                .lines()
                .filter_map(|line| line.strip_prefix("data: "))
                .next_back()
            {
                Some(framed) => framed.to_owned(),
                None => text,
            };
            Ok(serde_json::from_str(&payload)?)
        }

        /// One `tools/call`, answered with the raw JSON-RPC message — errors,
        /// `isError` results and all. The caller asserts on the shape it
        /// expects; refusals are answers here, not test failures.
        pub async fn call_tool_raw(
            &self,
            name: &str,
            arguments: serde_json::Value,
        ) -> Setup<serde_json::Value> {
            self.rpc(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": { "name": name, "arguments": arguments },
            }))
            .await
        }

        /// One `tools/call` expected to succeed, answered with the result's
        /// text content.
        pub async fn call_tool(&self, name: &str, arguments: serde_json::Value) -> Setup<String> {
            let rpc = self.call_tool_raw(name, arguments).await?;
            if let Some(error) = rpc.get("error") {
                return Err(format!("the call was refused: {error}").into());
            }
            let result = rpc.get("result").ok_or("a reply must carry a result")?;
            if result.get("isError").and_then(serde_json::Value::as_bool) == Some(true) {
                return Err(format!("the tool answered an error: {result}").into());
            }
            result["content"][0]["text"]
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| "the result must carry one text content block".into())
        }

        /// The names `tools/list` advertises.
        pub async fn list_tool_names(&self) -> Setup<Vec<String>> {
            let rpc = self
                .rpc(&serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/list",
                }))
                .await?;
            let tools = rpc["result"]["tools"]
                .as_array()
                .ok_or_else(|| format!("a listing must carry tools: {rpc}"))?;
            Ok(tools
                .iter()
                .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
                .collect())
        }
    }
}

// Unused where a binary never waits on the log; the module-scoped `dead_code`
// allow does not reach imports.
#[cfg(test)]
#[allow(unused_imports)]
pub use bounds::{stage, within};

/// The wait ceiling, in its own `#[cfg(test)]` module (and last in the file)
/// so the workspace's `panic` exemption applies: clippy exempts anything
/// under a `cfg(test)` module, but not a bare helper beside a `#[test]` fn.
#[cfg(test)]
mod bounds {
    /// How long any wait against the log may take before it is a failure.
    ///
    /// Generous against a loaded machine, and still an order of magnitude
    /// below the point where cargo-mutants gives up.
    pub const CEILING: std::time::Duration = std::time::Duration::from_secs(5);

    /// Runs `future` with a ceiling, failing the test if it does not finish.
    ///
    /// Every wait against the log goes through here, because an unbounded
    /// await turns a defect into a hang. A `respond` that answers without
    /// ever raising the request, or a journaled request whose `request_id`
    /// was dropped, each leaves a watcher waiting for an event that will
    /// never match, and cargo-mutants then records a TIMEOUT. A timeout is a
    /// mutant nobody evaluated (docs/QUALITY_GATES.md), so it protects
    /// nothing. With a ceiling the same mutants fail in seconds and are
    /// caught.
    ///
    /// This bound is a test-harness ceiling, not a decision deadline: the
    /// production wait stays unbounded on purpose (see `permission.rs`'s
    /// module docs), because a human may answer from a phone.
    pub async fn within<F: std::future::Future>(what: &str, future: F) -> F::Output {
        let Ok(value) = tokio::time::timeout(CEILING, future).await else {
            panic!("{what} must finish rather than hang");
        };
        value
    }

    /// Bounds one *arrangement* step with the same ceiling, answering an
    /// `Err` that names the step instead of panicking.
    ///
    /// [`within`] is for the wait a test asserts on; this is for the hops a
    /// harness stands up on the way there. The distinction earned its keep
    /// the hard way: the hub e2e suite once wrapped its whole eight-hop
    /// stack in a single `within`, and when a loaded machine starved the
    /// stand-up the only evidence was six copies of "the stack stands up
    /// must finish rather than hang" — a ceiling that said nothing about
    /// *which* hop was slow. Per-stage bounds keep the ceiling per wait (no
    /// widening) and make the failure name its hop.
    pub async fn stage<T, F>(what: &str, future: F) -> super::Setup<T>
    where
        F: std::future::Future<Output = super::Setup<T>>,
    {
        match tokio::time::timeout(CEILING, future).await {
            Ok(result) => result,
            Err(_elapsed) => {
                Err(format!("{what} did not finish within {CEILING:?}").into())
            }
        }
    }
}
