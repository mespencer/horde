//! Builders the crate's own unit tests share.
//!
//! `permission` and `verbs` both match on events pulled off the log, so both
//! need to hand their matcher a realistic `EventEnvelope`. Written twice, the
//! two copies drifted in exactly the way the duplication gate exists to catch —
//! and a matcher tested against a subtly different envelope than the one it
//! meets in production is a test that proves nothing.

use horde_proto::v1::{AgentId, Event, EventEnvelope, SessionId, event::Kind};

/// One log envelope carrying `kind`, reported by `agent`.
///
/// `sequence` is fixed: nothing in this crate matches on position — that is the
/// whole point of the identity-matching design — so a varying sequence here
/// would suggest a significance it does not have.
pub(crate) fn envelope(agent: &str, kind: Kind) -> EventEnvelope {
    EventEnvelope {
        session_id: Some(SessionId::new("s1")),
        sequence: 1,
        recorded_at: None,
        event: Some(Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(kind),
        }),
    }
}

/// An envelope whose event is missing entirely.
///
/// A malformed row rather than an exotic one: every matcher has to survive it
/// without panicking and without matching, and it is the case a hand-built
/// happy-path envelope never covers.
pub(crate) fn envelope_without_event(agent: &str) -> EventEnvelope {
    EventEnvelope {
        event: None,
        ..envelope(agent, Kind::TurnInterrupted(Default::default()))
    }
}

/// An identity for `agent_id`, minted the only way one can be: through the
/// registry.
///
/// `AgentIdentity` has no public constructor precisely so that holding one is
/// proof a token matched, so the tests go through the same door production
/// does.
pub(crate) fn identity(agent_id: &str) -> crate::identity::AgentIdentity {
    let registry = crate::identity::AgentRegistry::new();
    registry.insert(agent_id, "token");
    let mut headers = http::HeaderMap::new();
    headers.insert("authorization", "Bearer token".parse().expect("header"));
    headers.insert(
        "x-horde-agent",
        agent_id.parse().expect("agent ids are header-safe"),
    );
    registry.authenticate(&headers).expect("authenticates")
}
