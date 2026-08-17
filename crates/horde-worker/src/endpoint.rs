//! Standing up each Agent's MCP endpoint — the wire ADR-0004 left to the
//! Worker.
//!
//! Every Agent gets exactly one Horde-controlled MCP endpoint (DESIGN §5).
//! The endpoint machinery itself lives in `horde-mcp`; what only the Worker
//! can supply is here, and it is exactly the three things the ADR's
//! implementation note listed as owed:
//!
//! * **Token minting.** One unguessable bearer token per Agent, registered in
//!   the [`AgentRegistry`] the serving gate checks on every request, revoked
//!   when the Agent is archived.
//! * **The journal sink.** `report_result` is an observed fact, so it lands
//!   through the Worker's [`StampingSink`] — the reporting Agent's commits are
//!   observed and stamped on the way into the journal, never declared.
//! * **The control-plane channel.** `spawn_agent` and `send_message` are
//!   Server decisions, issued over the Worker's own authenticated channel.
//!
//! # One endpoint per Session
//!
//! The endpoint's [`AgentContext`] binds one Session (its verbs journal into
//! and follow that Session's log), so the Worker serves one loopback endpoint
//! per Session it runs agents for, lazily, and hands each agent that
//! Session's URL with its own identity headers. Loopback is correct here: the
//! harness processes are children of this Worker, and DESIGN §3's
//! localhost-only rule is exactly what a loopback bind satisfies.

use std::collections::BTreeMap;
use std::sync::Arc;

use horde_adapter::McpEndpoint;
use horde_mcp::{AgentRegistry, EndpointConfig, JournalEntry, JournalError, JournalSink};

use crate::error::WorkerError;
use crate::share::StampingSink;

/// Serves the per-Session endpoints and owns the per-Agent tokens.
pub struct EndpointManager {
    /// The Worker's authenticated channel to the Server — the verbs that are
    /// ControlPlane RPCs ride it, as does the permission wait's sync stream.
    channel: tonic::transport::Channel,
    /// The tokens the serving gate accepts; shared with every endpoint this
    /// manager serves.
    registry: Arc<AgentRegistry>,
    served: tokio::sync::Mutex<BTreeMap<String, ServedEndpoint>>,
}

struct ServedEndpoint {
    url: String,
    /// The accept loop; aborted at shutdown, kept alive by being held here.
    serving: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for EndpointManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EndpointManager")
            .finish_non_exhaustive()
    }
}

impl EndpointManager {
    #[must_use]
    pub fn new(channel: tonic::transport::Channel) -> Self {
        Self {
            channel,
            registry: Arc::new(AgentRegistry::new()),
            served: tokio::sync::Mutex::new(BTreeMap::new()),
        }
    }

    /// The endpoint one Agent's harness is pointed at: the Session's served
    /// URL (bound now if this is the Session's first agent) and the identity
    /// headers minted for exactly this Agent.
    ///
    /// Re-minting for the same agent (a revival) replaces the old token —
    /// the previous incarnation's credential must not outlive it.
    ///
    /// # Errors
    ///
    /// [`WorkerError::EndpointUnavailable`] when the loopback bind fails.
    pub async fn endpoint_for(
        &self,
        session_id: &str,
        agent_id: &str,
        sink: StampingSink,
    ) -> Result<McpEndpoint, WorkerError> {
        let url = self.session_endpoint(session_id, sink).await?;
        let token = mint_token();
        self.registry.insert(agent_id, token.clone());
        Ok(McpEndpoint::Http {
            url,
            headers: vec![
                ("Authorization".to_owned(), format!("Bearer {token}")),
                ("X-Horde-Agent".to_owned(), agent_id.to_owned()),
            ],
        })
    }

    /// Revokes an archived Agent's token, effective on its next request.
    pub fn forget_agent(&self, agent_id: &str) {
        self.registry.remove(agent_id);
    }

    /// Stops every serving loop. Idempotent; called from Runtime shutdown.
    pub async fn shutdown(&self) {
        let mut served = self.served.lock().await;
        for (_, endpoint) in std::mem::take(&mut *served) {
            endpoint.serving.abort();
        }
    }

    /// The served URL for one Session's endpoint, binding it on first use.
    async fn session_endpoint(
        &self,
        session_id: &str,
        sink: StampingSink,
    ) -> Result<String, WorkerError> {
        let mut served = self.served.lock().await;
        if let Some(endpoint) = served.get(session_id) {
            return Ok(endpoint.url.clone());
        }

        let context = horde_mcp::context::AgentContext::new(
            Arc::new(EndpointJournal { sink }),
            horde_cli::client::Client::with_channel(self.channel.clone()),
            session_id,
        );
        let endpoint = horde_mcp::endpoint::HordeEndpoint::new(context);
        let (bound, serving) = horde_mcp::server::serve(
            &EndpointConfig::loopback(0),
            Arc::clone(&self.registry),
            endpoint,
        )
        .await
        .map_err(|error| WorkerError::EndpointUnavailable(error.to_string()))?;

        let url = format!("http://{bound}/mcp");
        served.insert(
            session_id.to_owned(),
            ServedEndpoint {
                url: url.clone(),
                serving,
            },
        );
        Ok(url)
    }
}

/// One unguessable bearer token.
///
/// From the OS entropy source, not a counter or a clock: the token is the
/// only thing separating one agent's verbs from another's on a shared
/// loopback port.
fn mint_token() -> String {
    let mut bytes = [0_u8; 32];
    // The OS entropy source failing is a machine that cannot do TLS either;
    // there is no meaningful degraded mode. Fall back to a panic-free but
    // loud refusal via the zeroed check below rather than unwrapping.
    let filled = getrandom::fill(&mut bytes).is_ok();
    assert!(
        filled && bytes.iter().any(|byte| *byte != 0),
        "the OS entropy source must yield token bytes"
    );
    hex::encode(bytes)
}

/// The Worker's journal, as the endpoint's seam sees it.
///
/// `horde-mcp` deliberately owns only the trait; the Worker owns the write.
/// Every entry goes through the [`StampingSink`], so a result reported
/// without a payload is stamped with the reporting Agent's observed HEAD —
/// and refused, not silently truncated, when its working tree is dirty.
#[derive(Debug, Clone)]
struct EndpointJournal {
    sink: StampingSink,
}

#[async_trait::async_trait]
impl JournalSink for EndpointJournal {
    async fn record(&self, entry: JournalEntry) -> Result<u64, JournalError> {
        self.sink
            .record(crate::journal::JournalEntry {
                session_id: entry.session_id,
                agent_id: entry.agent_id,
                idempotency_key: entry.idempotency_key,
                kind: entry.kind,
            })
            .await
            .map_err(|error| match error {
                // A stamp refusal — the dirty-tree case included — is the
                // entry being rejected: retrying the same bytes re-observes
                // the same tree. The message carries the fix ("commit, then
                // share"), and it must reach the agent verbatim.
                WorkerError::Share(_) => JournalError::Rejected(error.to_string()),
                other => JournalError::Unavailable(other.to_string()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointManager, mint_token};
    use crate::share::{ShareBroker, StampingSink};

    /// Two mints must differ, and each must be 64 hex characters of real
    /// entropy — a predictable token is another agent's verbs.
    #[test]
    fn minted_tokens_are_long_and_never_repeat() {
        let first = mint_token();
        let second = mint_token();
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    }

    fn manager() -> EndpointManager {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        EndpointManager::new(channel)
    }

    fn sink() -> StampingSink {
        let journal = crate::journal::Journal::in_memory().expect("an in-memory journal");
        StampingSink::new(journal.sink(), std::sync::Arc::new(ShareBroker::new(None)))
    }

    /// The Session mapping: two agents of one Session share its endpoint
    /// (one AgentContext binds one Session), while a second Session gets its
    /// own — its verbs journal into and follow a different log.
    #[tokio::test]
    async fn agents_share_their_sessions_endpoint_and_sessions_do_not() {
        let manager = manager();

        let first = manager
            .endpoint_for("s1", "agent-1", sink())
            .await
            .expect("the first endpoint serves");
        let sibling = manager
            .endpoint_for("s1", "agent-2", sink())
            .await
            .expect("the sibling shares it");
        let elsewhere = manager
            .endpoint_for("s2", "agent-3", sink())
            .await
            .expect("a second session binds its own");

        let url_of = |endpoint: &horde_adapter::McpEndpoint| match endpoint {
            horde_adapter::McpEndpoint::Http { url, .. } => url.clone(),
            horde_adapter::McpEndpoint::Stdio { .. } => panic!("the worker serves HTTP"),
        };
        assert_eq!(url_of(&first), url_of(&sibling));
        assert_ne!(url_of(&first), url_of(&elsewhere));

        let headers_of = |endpoint: &horde_adapter::McpEndpoint| match endpoint {
            horde_adapter::McpEndpoint::Http { headers, .. } => headers.clone(),
            horde_adapter::McpEndpoint::Stdio { .. } => panic!("the worker serves HTTP"),
        };
        assert_ne!(
            headers_of(&first),
            headers_of(&sibling),
            "siblings share a port, so the tokens are what separate them"
        );

        manager.shutdown().await;
    }

    /// The minted pair, exercised against the served socket with a real MCP
    /// client: it authenticates while registered, stops the moment the agent
    /// is forgotten, and the socket itself stops answering after shutdown —
    /// a `forget_agent` or `shutdown` that silently did nothing would leave
    /// an archived agent's credential (and the whole endpoint) alive.
    #[tokio::test]
    async fn minted_credentials_dial_until_forgotten_and_the_socket_dies_at_shutdown() {
        let manager = manager();
        let endpoint = manager
            .endpoint_for("s1", "agent-1", sink())
            .await
            .expect("the endpoint serves");
        let horde_adapter::McpEndpoint::Http { url, headers } = endpoint else {
            panic!("the worker serves HTTP");
        };
        let token = headers
            .iter()
            .find(|(name, _)| name == "Authorization")
            .and_then(|(_, value)| value.strip_prefix("Bearer "))
            .expect("a bearer token")
            .to_owned();

        let dial = |token: String, url: String| async move {
            horde_mcp::hub::Upstream::dial(
                "horde",
                &url,
                Some(&token),
                &[("x-horde-agent", "agent-1")],
            )
            .await
        };

        dial(token.clone(), url.clone())
            .await
            .expect("the minted pair authenticates against the served endpoint");

        manager.forget_agent("agent-1");
        assert!(
            dial(token.clone(), url.clone()).await.is_err(),
            "a forgotten agent's token must stop authenticating immediately"
        );

        manager.shutdown().await;
        // The accept loop is aborted; give the runtime one breath to reap it,
        // then the socket must refuse outright.
        tokio::task::yield_now().await;
        let address = url
            .strip_prefix("http://")
            .and_then(|rest| rest.strip_suffix("/mcp"))
            .expect("the served url shape");
        assert!(
            tokio::net::TcpStream::connect(address).await.is_err(),
            "a shut-down manager must not keep serving its port"
        );
    }

    /// The Debug form reaches logs; it must say what it is (the credential
    /// map stays out of it by construction). A tokio test because even a
    /// lazy tonic channel wants a reactor to be born on.
    #[tokio::test]
    async fn the_manager_debugs_by_name() {
        let rendered = format!("{:?}", manager());
        assert!(rendered.contains("EndpointManager"), "{rendered}");
    }
}
