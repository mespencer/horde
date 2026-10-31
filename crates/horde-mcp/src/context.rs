//! What every verb on one Agent's endpoint needs.
//!
//! [`PermissionBroker`](crate::permission::PermissionBroker) and
//! [`Verbs`](crate::verbs::Verbs) were separate types holding the same three
//! things and constructed the same way, which is how the duplication gate found
//! them. They are not two contexts; they are one context with two vocabularies —
//! the permission chain and the orchestrator verbs — so the shared half is named
//! once here and each of them borrows it.
//!
//! Both planes are present because an Agent's endpoint straddles them
//! (DESIGN §5): the control-plane client for requests the Server decides, and
//! the journal for facts the Worker observes. A type holding only one could not
//! serve a single verb correctly.

use crate::journal::JournalSink;
use crate::watch::SessionWatch;

/// The context one Agent's endpoint operates in.
///
/// Cheap to clone — the journal is behind an `Arc` and cloning a tonic channel
/// shares the underlying connection — which is what lets each in-flight tool
/// call take its own copy instead of queueing behind a lock. That matters more
/// than it sounds: the permission verb blocks for as long as a human takes to
/// answer, so a shared lock here would stall every other agent on the endpoint
/// for minutes.
#[derive(Clone)]
pub struct AgentContext {
    journal: std::sync::Arc<dyn JournalSink>,
    client: horde_cli::client::Client,
    session_id: String,
    /// The Session's log as the blocking verbs read it — shared by every clone,
    /// so each wait starts where the last one's fold left off.
    watch: SessionWatch,
}

impl std::fmt::Debug for AgentContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AgentContext")
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl AgentContext {
    #[must_use]
    pub fn new(
        journal: std::sync::Arc<dyn JournalSink>,
        client: horde_cli::client::Client,
        session_id: impl Into<String>,
    ) -> Self {
        let session_id = session_id.into();
        Self {
            journal,
            watch: SessionWatch::new(client.clone(), session_id.clone()),
            client,
            session_id,
        }
    }

    /// The Session every event here belongs to.
    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// The journal, for facts the Worker observes.
    #[must_use]
    pub fn journal(&self) -> &std::sync::Arc<dyn JournalSink> {
        &self.journal
    }

    /// A control-plane client to issue one call through.
    ///
    /// Handed out by value because every RPC needs `&mut`, and a caller that
    /// borrowed one would serialize against every other caller.
    #[must_use]
    pub fn client(&self) -> horde_cli::client::Client {
        self.client.clone()
    }

    /// The shared, redialing view of the Session's log.
    #[must_use]
    pub fn watch(&self) -> &SessionWatch {
        &self.watch
    }
}

#[cfg(test)]
mod tests {
    use super::AgentContext;

    fn context() -> AgentContext {
        let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
        AgentContext::new(
            std::sync::Arc::new(crate::journal::testing::RecordingJournal::new()),
            horde_cli::client::Client::with_channel(channel),
            "s1",
        )
    }

    #[tokio::test]
    async fn a_context_carries_the_session_every_event_belongs_to() {
        assert_eq!(context().session_id(), "s1");
    }

    /// Each call takes its own client. A shared one would mean a blocked
    /// permission wait — which can legitimately last minutes — holding up every
    /// other agent's verbs on the same endpoint.
    #[tokio::test]
    async fn each_caller_gets_its_own_client_handle() {
        let context = context();
        let _first = context.client();
        let _second = context.client();
        // Both exist at once; the point is that this compiles and does not
        // require exclusive access to the context.
        assert_eq!(context.session_id(), "s1");
    }

    #[tokio::test]
    async fn a_context_does_not_debug_its_credentials_or_connection() {
        let rendered = format!("{:?}", context());
        assert!(rendered.contains("s1"), "{rendered}");
        assert!(
            !rendered.contains("Channel"),
            "a debug line is a log line; it must not spill the transport: {rendered}"
        );
    }
}
