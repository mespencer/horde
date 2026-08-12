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
    let endpoint = horde_server_testkit::serve().await?;
    let (_client, session_id, root) = horde_server_testkit::seeded(&endpoint, "endpoint").await?;

    Ok(Harness {
        endpoint,
        session_id,
        agent_id: root,
    })
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
    let mut registry = AgentRegistry::new();
    registry.insert(agent_id, "token");

    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "Bearer token".parse()?);
    headers.insert("x-horde-agent", agent_id.parse()?);
    Ok(registry.authenticate(&headers)?)
}
