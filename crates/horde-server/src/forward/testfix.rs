//! Log fixtures the forward suites share: one placed, exposing agent —
//! the arrangement both the intent tests and the relay tests open with,
//! kept here so neither restates it (the duplication gate's rule applies
//! to test code too).

use horde_proto::v1::{
    AgentId, AgentSpawned, Event, ForwardDirection, ForwardId, ForwardOpened, ForwardSpec,
    Placement, PlacementChanged, PlacementState, event::Kind,
};
use prost::Message;

use crate::log::{EventLog, PendingEvent};

/// Appends one keyed agent event, panicking on the failures a fixture
/// cannot survive.
pub(crate) async fn append(log: &EventLog, session: &str, key: &str, agent: &str, kind: Kind) {
    let event = Event {
        agent_id: Some(AgentId::new(agent)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(kind),
    };
    log.append(PendingEvent {
        session_id: session.to_owned(),
        idempotency_key: key.to_owned(),
        payload: event.encode_to_vec(),
        ..PendingEvent::default()
    })
    .await
    .expect("the fixture appends");
}

/// A ceilinged receive: the one positive channel wait the relay suites —
/// forward and login alike — are allowed, kept here per the duplication
/// gate's rule that test code shares its shapes too.
pub(crate) async fn within<T>(what: &str, receiver: &mut tokio::sync::mpsc::Receiver<T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
        .await
        .unwrap_or_else(|_| panic!("{what}: nothing arrived within the ceiling"))
        .unwrap_or_else(|| panic!("{what}: the channel closed instead"))
}

/// An expose named "vite" from worker port 5173.
pub(crate) fn expose_spec() -> ForwardSpec {
    ForwardSpec {
        direction: ForwardDirection::Expose as i32,
        name: "vite".to_owned(),
        worker_port: 5173,
        client_port: 0,
    }
}

/// One session: `agent` spawned, placed on `worker`, exposing
/// `fwd-{agent}`.
pub(crate) async fn placed_expose(log: &EventLog, session: &str, agent: &str, worker: &str) {
    append(
        log,
        session,
        &format!("{agent}:spawn"),
        agent,
        Kind::AgentSpawned(AgentSpawned::default()),
    )
    .await;
    append(
        log,
        session,
        &format!("{agent}:place"),
        agent,
        Kind::PlacementChanged(PlacementChanged {
            adapter_capabilities: Vec::new(),
            placement: Some(Placement {
                agent_id: Some(AgentId::new(agent)),
                state: PlacementState::Placed as i32,
                worker_id: Some(horde_proto::v1::WorkerId::new(worker)),
                reason: String::new(),
            }),
        }),
    )
    .await;
    append(
        log,
        session,
        &format!("{agent}:open"),
        agent,
        Kind::ForwardOpened(ForwardOpened {
            forward_id: Some(ForwardId::new(format!("fwd-{agent}"))),
            spec: Some(expose_spec()),
            opened_by: None,
            backer_client_id: None,
        }),
    )
    .await;
}
