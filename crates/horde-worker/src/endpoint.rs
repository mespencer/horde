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
use horde_mcp::hub::{AgentProxies, HubServerConfig, ToolProxy, Upstream};
use horde_mcp::{AgentRegistry, EndpointConfig, JournalEntry, JournalError, JournalSink};

use crate::error::WorkerError;
use crate::hub::ForwardedHubLink;
use crate::share::StampingSink;

/// The proxied MCP surface one spawn command resolved for its Agent
/// (ADR-0004): the `local` configs to spawn beside it, and the hub servers
/// whose forwarded tools it may see. Both halves were resolved Server-side
/// against the journaled catalog and the Agent's type; the Worker consumes
/// them without a second opinion.
#[derive(Debug, Clone, Default)]
pub struct AgentMcpSurface {
    /// Full configs — the Worker must spawn them without a round trip.
    pub local: Vec<horde_proto::v1::McpServerConfig>,
    /// Hub server names, by which the endpoint filters the forwarded link.
    pub visible_hub: Vec<String>,
}

impl AgentMcpSurface {
    /// Whether this Agent gets any proxied tools at all. Named because the
    /// empty case skips real work (no hub listing RPC, no registry entry),
    /// and an inverted condition would silently cost every spawn a round
    /// trip — or every hub-visible agent its toolset.
    #[must_use]
    fn is_empty(&self) -> bool {
        self.local.is_empty() && self.visible_hub.is_empty()
    }
}

/// Serves the per-Session endpoints and owns the per-Agent tokens.
pub struct EndpointManager {
    /// The Worker's authenticated channel to the Server — the verbs that are
    /// ControlPlane RPCs ride it, the permission wait's sync stream too, and
    /// (the 2026-08-19 amendment) every forwarded hub call.
    channel: tonic::transport::Channel,
    /// The identity forwarded hub calls are attributed under. The mTLS
    /// certificate outranks it at the Server; carrying it keeps the
    /// plaintext test shape honest, the same way `RegisterWorker` does.
    worker_id: String,
    /// The tokens the serving gate accepts; shared with every endpoint this
    /// manager serves.
    registry: Arc<AgentRegistry>,
    /// Each Agent's proxied toolset — hub link plus `local` children —
    /// shared with every endpoint this manager serves, resolved per request
    /// by caller identity.
    proxies: Arc<AgentProxies>,
    served: tokio::sync::Mutex<Served>,
}

/// The manager's serving state. One lock guards both fields, so "may a new
/// endpoint bind?" and "which endpoints exist?" can never disagree.
struct Served {
    /// Cleared by [`EndpointManager::shutdown`]. A manager that has shut
    /// down refuses new endpoints rather than resurrecting the port behind
    /// a Worker on its way out.
    accepting: bool,
    endpoints: BTreeMap<String, ServedEndpoint>,
}

struct ServedEndpoint {
    url: String,
    /// The agents this endpoint was minted for and not yet forgotten. The
    /// last one forgotten takes the endpoint down with it — a Session whose
    /// agents are all archived has nobody left to serve.
    agents: std::collections::BTreeSet<String>,
    /// The serving handle; shut down (listener and admitted connections
    /// both reaped) at shutdown, kept alive by being held here.
    serving: horde_mcp::server::ServingEndpoint,
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
    pub fn new(channel: tonic::transport::Channel, worker_id: &str) -> Self {
        Self {
            channel,
            worker_id: worker_id.to_owned(),
            registry: Arc::new(AgentRegistry::new()),
            proxies: Arc::new(AgentProxies::new()),
            served: tokio::sync::Mutex::new(Served {
                accepting: true,
                endpoints: BTreeMap::new(),
            }),
        }
    }

    /// The endpoint one Agent's harness is pointed at: the Session's served
    /// URL (bound now if this is the Session's first agent), the Agent's
    /// proxied toolset assembled and registered, and the identity headers
    /// minted for exactly this Agent.
    ///
    /// The surface before the token: a `local` server that cannot spawn or a
    /// hub listing the Server refuses fails the spawn *here*, with no minted
    /// credential left behind — an agent missing a promised toolset is a
    /// different (and wrongly-scoped) product, not a degraded one.
    ///
    /// Re-minting for the same agent (a revival) replaces the old token and
    /// the old proxy — the previous incarnation's credential and `local`
    /// children must not outlive it.
    ///
    /// # Errors
    ///
    /// [`WorkerError::EndpointUnavailable`] when the loopback bind fails;
    /// [`WorkerError::McpSurface`] when the proxied toolset cannot be
    /// assembled.
    pub async fn endpoint_for(
        &self,
        session_id: &str,
        agent_id: &str,
        sink: StampingSink,
        surface: &AgentMcpSurface,
    ) -> Result<McpEndpoint, WorkerError> {
        let url = self.session_endpoint(session_id, agent_id, sink).await?;
        self.register_surface(session_id, agent_id, surface).await?;
        let token = mint_token()?;
        self.registry.insert(agent_id, token.clone());
        Ok(McpEndpoint::Http {
            url,
            headers: vec![
                ("Authorization".to_owned(), format!("Bearer {token}")),
                ("X-Horde-Agent".to_owned(), agent_id.to_owned()),
            ],
        })
    }

    /// Assembles and registers one Agent's proxied toolset: one connected
    /// [`Upstream`] per `local` config (a real child process, killed by
    /// handle when the proxy drops), and the forwarded hub link when any hub
    /// server is visible. An empty surface registers nothing — the endpoint
    /// falls back to the empty proxy, which serves exactly the verbs.
    async fn register_surface(
        &self,
        session_id: &str,
        agent_id: &str,
        surface: &AgentMcpSurface,
    ) -> Result<(), WorkerError> {
        if surface.is_empty() {
            // A revival whose surface shrank to nothing must still shed the
            // previous incarnation's children.
            self.proxies.remove(agent_id);
            return Ok(());
        }

        let mut proxy = ToolProxy::new();
        for wire in &surface.local {
            let config = HubServerConfig::try_from(wire)
                .map_err(|error| WorkerError::McpSurface(error.to_string()))?;
            let upstream = Upstream::connect(&config)
                .await
                .map_err(|error| WorkerError::McpSurface(error.to_string()))?;
            proxy = proxy.with_server(upstream);
        }
        if !surface.visible_hub.is_empty() {
            let link = ForwardedHubLink::fetch(
                self.channel.clone(),
                &self.worker_id,
                session_id,
                agent_id,
            )
            .await?;
            proxy = proxy.with_hub_link(link, surface.visible_hub.iter().cloned());
        }

        self.proxies.insert(agent_id, Arc::new(proxy));
        Ok(())
    }

    /// Revokes an Agent's token (effective on its next request) and drops its
    /// proxied toolset — the `local` children die by handle with the last
    /// clone of their upstreams. Called at archive, and for a spawn or
    /// revival that minted an endpoint but never got a running harness: a
    /// credential nobody holds must not stay valid.
    ///
    /// When this was the last agent of its Session's endpoint, the endpoint
    /// is shut down too (listener and connections reaped) rather than left
    /// serving nobody until the Worker exits. Returns whether that happened.
    pub async fn forget_agent(&self, agent_id: &str) -> bool {
        self.registry.remove(agent_id);
        self.proxies.remove(agent_id);

        let mut served = self.served.lock().await;
        let emptied = served.endpoints.iter_mut().find_map(|(session, endpoint)| {
            (endpoint.agents.remove(agent_id) && endpoint.agents.is_empty())
                .then(|| session.clone())
        });
        let released = emptied.and_then(|session| served.endpoints.remove(&session));
        drop(served);

        match released {
            Some(endpoint) => {
                endpoint.serving.shutdown().await;
                true
            }
            None => false,
        }
    }

    /// Stops every serving loop, and does not return until each has actually
    /// stopped serving: every listener socket *and every connection those
    /// listeners ever admitted* is provably dropped by the time this
    /// returns. The admitted connections matter as much as the listeners —
    /// an accept loop reaped alone leaves its connections alive in detached
    /// tasks, still answering an archived agent's verbs and still holding
    /// the port. Bounded: the reap is an abort plus a join per task, each
    /// resolving at its next await point. Idempotent (a second call finds
    /// nothing to reap and returns 0); called from Runtime shutdown.
    ///
    /// From here on the manager refuses to serve: a later
    /// [`endpoint_for`](Self::endpoint_for) is an error, not a fresh bind.
    ///
    /// Returns how many endpoints' reaps provably completed — each one's
    /// [`ReapWitness`](horde_mcp::server::ReapWitness) confirming its
    /// accept loop finished after draining its connection `JoinSet`.
    /// Returned rather than logged for the same reason `Runtime::shutdown`
    /// returns its count: "shutdown finished" and "shutdown actually
    /// reaped" are different facts, and only the second one is evidence.
    pub async fn shutdown(&self) -> usize {
        let mut served = self.served.lock().await;
        served.accepting = false;
        let endpoints = std::mem::take(&mut served.endpoints);
        drop(served);

        // Every agent's proxied toolset goes with the endpoints: no `local`
        // child may outlive the manager that spawned it.
        self.proxies.clear();

        let mut reaped = 0;
        for (_, endpoint) in endpoints {
            let witness = endpoint.serving.reap_witness();
            endpoint.serving.shutdown().await;
            if witness.is_reaped() {
                reaped += 1;
            }
        }
        reaped
    }

    /// The served URL for one Session's endpoint, binding it on first use,
    /// with `agent_id` recorded as one of the agents it serves — before the
    /// rest of the mint can fail, so a failed spawn's `forget_agent` still
    /// finds it and releases an endpoint nobody else uses.
    async fn session_endpoint(
        &self,
        session_id: &str,
        agent_id: &str,
        sink: StampingSink,
    ) -> Result<String, WorkerError> {
        let mut served = self.served.lock().await;
        if !served.accepting {
            return Err(WorkerError::EndpointUnavailable(
                "the endpoint manager has shut down".to_owned(),
            ));
        }
        if let Some(endpoint) = served.endpoints.get_mut(session_id) {
            endpoint.agents.insert(agent_id.to_owned());
            return Ok(endpoint.url.clone());
        }

        let context = horde_mcp::context::AgentContext::new(
            Arc::new(EndpointJournal { sink }),
            horde_cli::client::Client::with_channel(self.channel.clone()),
            session_id,
        );
        let endpoint = horde_mcp::endpoint::HordeEndpoint::new(context)
            .with_agent_proxies(Arc::clone(&self.proxies));
        let (bound, serving) = horde_mcp::server::serve(
            &EndpointConfig::loopback(0),
            Arc::clone(&self.registry),
            endpoint,
        )
        .await
        .map_err(|error| WorkerError::EndpointUnavailable(error.to_string()))?;

        let url = format!("http://{bound}/mcp");
        served.endpoints.insert(
            session_id.to_owned(),
            ServedEndpoint {
                url: url.clone(),
                agents: std::collections::BTreeSet::from([agent_id.to_owned()]),
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
///
/// # Errors
///
/// [`WorkerError::EndpointUnavailable`] when the entropy source fails or
/// yields all zeroes. A machine in that state cannot do TLS either, so there
/// is no degraded mode — but it is the spawn that fails, loudly, never the
/// Worker that panics.
fn mint_token() -> Result<String, WorkerError> {
    let mut bytes = [0_u8; 32];
    let filled = getrandom::fill(&mut bytes).is_ok();
    if filled && bytes.iter().any(|byte| *byte != 0) {
        Ok(hex::encode(bytes))
    } else {
        Err(WorkerError::EndpointUnavailable(
            "the OS entropy source yielded no token bytes".to_owned(),
        ))
    }
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
        let first = mint_token().expect("entropy");
        let second = mint_token().expect("entropy");
        assert_ne!(first, second);
        assert_eq!(first.len(), 64);
        assert!(first.chars().all(|c| c.is_ascii_hexdigit()));
    }

    fn manager() -> EndpointManager {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        EndpointManager::new(channel, "w-test")
    }

    /// No proxied surface: the shape every plain spawn has.
    fn bare() -> super::AgentMcpSurface {
        super::AgentMcpSurface::default()
    }

    fn sink() -> StampingSink {
        let journal = crate::journal::Journal::in_memory().expect("an in-memory journal");
        StampingSink::new(journal.sink(), std::sync::Arc::new(ShareBroker::new(None)))
    }

    /// The empty-surface shortcut's truth table: only a surface with neither
    /// `local` configs nor visible hub servers is empty. Each false clause
    /// alone must flip it — a surface wrongly judged empty silently strips a
    /// promised toolset, wrongly judged non-empty costs every plain spawn a
    /// hub round trip.
    #[test]
    fn a_surface_is_empty_only_with_neither_half() {
        let local = horde_proto::v1::McpServerConfig {
            name: "browser".to_owned(),
            ..horde_proto::v1::McpServerConfig::default()
        };

        assert!(super::AgentMcpSurface::default().is_empty());
        assert!(
            !super::AgentMcpSurface {
                local: vec![local.clone()],
                visible_hub: Vec::new(),
            }
            .is_empty(),
            "a local config alone is a surface"
        );
        assert!(
            !super::AgentMcpSurface {
                local: Vec::new(),
                visible_hub: vec!["linear".to_owned()],
            }
            .is_empty(),
            "a visible hub server alone is a surface"
        );
        assert!(
            !super::AgentMcpSurface {
                local: vec![local],
                visible_hub: vec!["linear".to_owned()],
            }
            .is_empty()
        );
    }

    /// A surface whose `local` config cannot even convert fails the endpoint
    /// (and so the spawn) loudly — a register step that silently succeeded
    /// would launch the agent missing its promised toolset, ADR-0004's
    /// dreaded failure shape.
    #[tokio::test]
    async fn an_unusable_local_config_fails_the_endpoint() {
        let manager = manager();
        let surface = super::AgentMcpSurface {
            local: vec![horde_proto::v1::McpServerConfig {
                name: "browser".to_owned(),
                // No transport at all: the conversion refuses it.
                ..horde_proto::v1::McpServerConfig::default()
            }],
            visible_hub: Vec::new(),
        };

        let refused = manager
            .endpoint_for("s1", "agent-1", sink(), &surface)
            .await
            .expect_err("an unusable local config must fail the spawn");
        assert!(
            matches!(&refused, crate::error::WorkerError::McpSurface(_)),
            "got {refused:?}"
        );

        // The failed spawn's cleanup: forgetting the agent that never got a
        // harness releases the endpoint bound for it, rather than leaving it
        // serving nobody until shutdown.
        assert!(
            manager.forget_agent("agent-1").await,
            "the endpoint bound for the failed spawn is released"
        );
        assert_eq!(manager.shutdown().await, 0, "nothing was left to reap");
    }

    /// Forgetting one sibling keeps the Session's endpoint for the other;
    /// forgetting the last takes it down. An agent the manager never minted
    /// for releases nothing.
    #[tokio::test]
    async fn a_sessions_endpoint_lives_exactly_as_long_as_its_agents() {
        let manager = manager();
        for agent in ["agent-1", "agent-2"] {
            manager
                .endpoint_for("s1", agent, sink(), &bare())
                .await
                .expect("serves");
        }
        assert!(!manager.forget_agent("agent-nobody").await);
        assert!(
            !manager.forget_agent("agent-1").await,
            "a sibling still needs the endpoint"
        );
        assert!(
            manager.forget_agent("agent-2").await,
            "the last agent takes the endpoint down"
        );
        assert!(
            !manager.forget_agent("agent-2").await,
            "forgetting twice releases nothing twice"
        );

        // A new agent of the same Session binds afresh.
        manager
            .endpoint_for("s1", "agent-3", sink(), &bare())
            .await
            .expect("a fresh endpoint");
        assert_eq!(manager.shutdown().await, 1);
    }

    /// The Session mapping: two agents of one Session share its endpoint
    /// (one AgentContext binds one Session), while a second Session gets its
    /// own — its verbs journal into and follow a different log.
    #[tokio::test]
    async fn agents_share_their_sessions_endpoint_and_sessions_do_not() {
        let manager = manager();

        let first = manager
            .endpoint_for("s1", "agent-1", sink(), &bare())
            .await
            .expect("the first endpoint serves");
        let sibling = manager
            .endpoint_for("s1", "agent-2", sink(), &bare())
            .await
            .expect("the sibling shares it");
        let elsewhere = manager
            .endpoint_for("s2", "agent-3", sink(), &bare())
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

        assert_eq!(
            manager.shutdown().await,
            2,
            "both sessions' endpoints must complete their reaps"
        );
    }

    /// The minted pair, exercised against the served socket with a real MCP
    /// client: it authenticates while registered, stops the moment the agent
    /// is forgotten, and shutdown provably reaps the endpoint — a
    /// `forget_agent` or `shutdown` that silently did nothing would leave
    /// an archived agent's credential (and the whole endpoint) alive.
    ///
    /// After shutdown, every assertion is on state this process owns: the
    /// manager's reaped count and its refusal to serve again. Three sweeps
    /// proved that asserting on OS-level port state here instead — connect
    /// fails, or the freed address re-binds — is unsound on a shared
    /// machine: an ephemeral-wraparound squatter in another process
    /// answered the connect probe, and this test's own earlier client
    /// sockets (TIME_WAIT) defeated the re-bind probe. The port is a
    /// resource this test does not own; the manager's word is.
    #[tokio::test]
    async fn minted_credentials_dial_until_forgotten_and_the_socket_dies_at_shutdown() {
        let manager = manager();
        let endpoint = manager
            .endpoint_for("s1", "agent-1", sink(), &bare())
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

        // A sibling keeps the endpoint up, so what is observed below is the
        // token's revocation, not the socket's death.
        manager
            .endpoint_for("s1", "agent-sibling", sink(), &bare())
            .await
            .expect("the sibling shares it");
        assert!(!manager.forget_agent("agent-1").await);
        assert!(
            dial(token.clone(), url.clone()).await.is_err(),
            "a forgotten agent's token must stop authenticating immediately"
        );

        // The reaped count is the terminal fact: 1 means this test's one
        // endpoint ran its reap to completion — accept loop finished,
        // listener dropped, every connection the dials opened aborted AND
        // joined — before shutdown() returned. A shutdown that only
        // signalled and left the reap in the background cannot produce it:
        // the witness behind the count is set as the serving task's final
        // act, and awaiting that task is exactly what shutdown owes.
        assert_eq!(
            manager.shutdown().await,
            1,
            "the served endpoint must complete its reap at shutdown"
        );

        // And the manager knows it is shut down: a new endpoint through its
        // own API is refused, not freshly bound. Owned state again — no
        // other process on the machine can make this call succeed or fail.
        assert!(
            manager
                .endpoint_for("s1", "agent-2", sink(), &bare())
                .await
                .is_err(),
            "a shut-down manager must refuse to serve new endpoints"
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
