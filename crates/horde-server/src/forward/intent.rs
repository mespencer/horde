//! The durable half of a forward: intent validation and the opener.
//!
//! Everything here is either pure or a keyed append, because the intent
//! plane must replay to the same state from any offset. The one writer of
//! chain-driven `ForwardOpened` events is the permission driver's fold, which
//! calls [`open_for_settled`] for every `PermissionDecided` row it reads —
//! that single, log-ordered writer is what makes the first-accept-backs race
//! deterministic: whichever acceptance settled the request *in the log* is
//! the one whose principal backs the inject, no matter how the concurrent
//! RPCs interleaved.

use horde_proto::v1::{
    Actor, AgentId, ClientId, Event, ForwardDirection, ForwardId, ForwardIntent, ForwardOpened,
    ForwardSpec, PermissionDecision, PermissionOutcome, SessionId, event::Kind,
};
use prost::Message;

use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent};
use crate::state::{PendingPermission, SessionState, forward_is_live};

/// Why a spec cannot become an intent. Rendered to the caller verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecRefusal(pub String);

impl std::fmt::Display for SpecRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Whether a spec is complete enough to journal (ADR-0006).
///
/// The rules exist because the log is the only durable record: a port the
/// log cannot name would be ephemeral state smuggled into the intent, and a
/// nameless forward could never be rendered as `vite → localhost:5173`.
///
/// # Errors
///
/// A [`SpecRefusal`] naming the missing piece.
pub fn openable(spec: &ForwardSpec) -> Result<ForwardDirection, SpecRefusal> {
    let direction = match ForwardDirection::try_from(spec.direction) {
        Ok(ForwardDirection::Expose) => ForwardDirection::Expose,
        Ok(ForwardDirection::Inject) => ForwardDirection::Inject,
        _ => {
            return Err(SpecRefusal(
                "a forward must say which way it flows: expose or inject".to_owned(),
            ));
        }
    };
    if spec.name.is_empty() {
        return Err(SpecRefusal(
            "forwards are named, not numbered; give this one a name".to_owned(),
        ));
    }
    if spec.worker_port == 0 {
        return Err(SpecRefusal(
            "a forward must name its worker-side port; the log cannot hold an ephemeral one"
                .to_owned(),
        ));
    }
    if direction == ForwardDirection::Inject && spec.client_port == 0 {
        return Err(SpecRefusal(
            "an inject must name the client-side port its backer dials".to_owned(),
        ));
    }
    Ok(direction)
}

/// Whether `name` already names a live forward in this session.
///
/// Extracted because both openers (the RPC and the chain fold) must agree,
/// and because the compound — same name *and* still live — is exactly the
/// pair an inline `&&` would leave untested: a closed forward's name is free
/// again, and that row is unreachable through any single RPC sequence a test
/// can stage without this predicate.
#[must_use]
pub fn name_in_use(state: &SessionState, name: &str) -> bool {
    state
        .live_forwards()
        .any(|forward| forward.spec.as_ref().is_some_and(|spec| spec.name == name))
}

/// Whether this pending request is one no automatic layer may settle.
///
/// An inject is human-gated by physics rather than policy: acceptance and
/// backing are one act, and only a client's connection can back one — a
/// judge or a rule that "allowed" an inject would open an intent nothing can
/// ever serve. Both halves matter: a pending *expose* flows the chain like
/// any proposed action, and a request with no forward at all is the chain's
/// ordinary tool-call case.
#[must_use]
pub fn forward_needs_human(pending: &PendingPermission) -> bool {
    pending
        .forward
        .as_ref()
        .is_some_and(|spec| spec.direction == ForwardDirection::Inject as i32)
}

/// The client principal a settling decision would back an inject with.
///
/// `None` for every non-client actor: the chain's own decisions are stamped
/// `ServerActor`, and a server cannot back an inject — no machine of its own
/// serves the traffic.
#[must_use]
pub fn backing_client_of(decision: &PermissionDecision) -> Option<String> {
    match decision.decided_by_actor.as_ref()?.actor.as_ref()? {
        horde_proto::v1::actor::Actor::ClientId(client) => Some(client.to_string()),
        _ => None,
    }
}

/// The `PermissionDecision` this log row settles, if it settles one.
#[must_use]
pub fn settled_in(global: &GlobalEvent) -> Option<PermissionDecision> {
    let event = Event::decode(global.event.payload.as_slice()).ok()?;
    match event.kind? {
        Kind::PermissionDecided(decided) => decided.decision,
        _ => None,
    }
}

pub use horde_proto::forward::derived_forward_id;

/// Opens the forward a settling decision approved, if it approved one.
/// Returns how many events were appended (0 or 1).
///
/// Called by the permission driver for every `PermissionDecided` row, in log
/// order. The projection is of the log *before* this row, so only the
/// decision that actually settled the request opens anything — a duplicate
/// decision (two humans racing) projects the request as already decided and
/// appends nothing, which is the decided-set race handling doing double duty
/// as backer election.
///
/// # Errors
///
/// Whatever reading or appending the log refuses.
pub async fn open_for_settled(
    log: &EventLog,
    global: &GlobalEvent,
    decision: &PermissionDecision,
) -> Result<u64, ServerError> {
    if decision.outcome != PermissionOutcome::Allow as i32 {
        return Ok(0);
    }
    let Some(request_id) = decision.request_id.as_ref().map(ToString::to_string) else {
        return Ok(0);
    };

    let session_id = global.event.session_id.clone();
    let events = log.read_all(&session_id).await?;
    let before: Vec<_> = events
        .into_iter()
        .filter(|stored| stored.sequence < global.event.sequence)
        .collect();
    let state = SessionState::project(&session_id, &before)?;

    let Some(pending) = state.pending_permissions.get(&request_id) else {
        // Already settled earlier in the log — this row is the losing side of
        // a race — or never raised at all.
        return Ok(0);
    };
    let Some(spec) = pending.forward.clone() else {
        return Ok(0); // An ordinary tool call; nothing to open.
    };

    let Some(event) = opened_event(&state, &request_id, pending, spec, decision) else {
        return Ok(0);
    };
    let appended = crate::service::shared::append_server_event_returning(
        log,
        &session_id,
        &format!("forward-open:{request_id}"),
        event,
    )
    .await?;
    // A replayed decision recomputes the same open and deduplicates into it;
    // only a first-time append counts, so a restarted driver reports the
    // idempotence it claims.
    Ok(u64::from(!appended.deduplicated))
}

/// The `ForwardOpened` a settled forward request produces, or `None` when
/// opening would journal an intent nothing can serve.
fn opened_event(
    state: &SessionState,
    request_id: &str,
    pending: &PendingPermission,
    spec: ForwardSpec,
    decision: &PermissionDecision,
) -> Option<Event> {
    if name_in_use(state, &spec.name) {
        tracing::warn!(
            request_id,
            name = spec.name,
            "an approved forward's name is already live in this session; not opening"
        );
        return None;
    }
    let backer = if spec.direction == ForwardDirection::Inject as i32 {
        let Some(backer) = backing_client_of(decision) else {
            tracing::warn!(
                request_id,
                "an inject was allowed by something that is not a client; \
                 an unbackable intent is not opened"
            );
            return None;
        };
        Some(ClientId::new(backer))
    } else {
        None
    };

    Some(Event {
        agent_id: Some(AgentId::new(pending.agent_id.clone())),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::ForwardOpened(ForwardOpened {
            forward_id: Some(ForwardId::new(derived_forward_id(request_id))),
            spec: Some(spec),
            opened_by: Some(Actor {
                actor: Some(horde_proto::v1::actor::Actor::AgentId(AgentId::new(
                    pending.agent_id.clone(),
                ))),
            }),
            backer_client_id: backer,
        })),
    })
}

/// The live intents a worker should realize: every live forward in every
/// session whose owning agent is placed on `worker_id`, sorted by id so two
/// computations of the same log compare equal.
///
/// # Errors
///
/// Whatever reading the log refuses.
pub async fn intents_for_worker(
    log: &EventLog,
    worker_id: &str,
) -> Result<Vec<ForwardIntent>, ServerError> {
    let mut intents = Vec::new();
    for session_id in log.session_ids().await? {
        let events = log.read_all(&session_id).await?;
        let state = SessionState::project(&session_id, &events)?;
        for forward in state.live_forwards() {
            if placed_on(&state, &forward.agent_id) == Some(worker_id) {
                intents.push(ForwardIntent {
                    forward_id: Some(ForwardId::new(forward.forward_id.clone())),
                    session_id: Some(SessionId::new(session_id.clone())),
                    agent_id: Some(AgentId::new(forward.agent_id.clone())),
                    spec: forward.spec.clone(),
                    backer_client_id: forward.backer_client_id.clone().map(ClientId::new),
                });
            }
        }
    }
    intents.sort_by(|a, b| {
        let key = |intent: &ForwardIntent| {
            intent
                .forward_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default()
        };
        key(a).cmp(&key(b))
    });
    Ok(intents)
}

/// The worker an agent is currently placed on, if any.
#[must_use]
pub fn placed_on<'a>(state: &'a SessionState, agent_id: &str) -> Option<&'a str> {
    state
        .agents
        .get(agent_id)
        .and_then(|agent| agent.worker_id.as_deref())
}

/// One forward as the relay needs to see it: direction, placement, backing —
/// and nothing a listener could be mistaken for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedForward {
    pub forward_id: String,
    pub session_id: String,
    pub direction: ForwardDirection,
    pub name: String,
    /// The worker the owning agent is placed on, when it is placed.
    pub placed_worker: Option<String>,
    /// INJECT only.
    pub backer: Option<String>,
}

/// Finds a live forward by id, across sessions. `None` covers never-opened,
/// closed, and owned-by-a-dead-agent alike: to the tunnel plane they are all
/// the same refusal.
///
/// # Errors
///
/// Whatever reading the log refuses.
pub async fn resolve_live(
    log: &EventLog,
    forward_id: &str,
) -> Result<Option<ResolvedForward>, ServerError> {
    for session_id in log.session_ids().await? {
        let events = log.read_all(&session_id).await?;
        let state = SessionState::project(&session_id, &events)?;
        let Some(forward) = state.forwards.get(forward_id) else {
            continue;
        };
        if !forward_is_live(forward, &state.agents) {
            return Ok(None);
        }
        let direction = forward
            .spec
            .as_ref()
            .and_then(|spec| ForwardDirection::try_from(spec.direction).ok())
            .unwrap_or(ForwardDirection::Unspecified);
        return Ok(Some(ResolvedForward {
            forward_id: forward.forward_id.clone(),
            session_id,
            direction,
            name: forward
                .spec
                .as_ref()
                .map(|spec| spec.name.clone())
                .unwrap_or_default(),
            placed_worker: placed_on(&state, &forward.agent_id).map(ToString::to_string),
            backer: forward.backer_client_id.clone(),
        }));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        Actor, AgentId, ClientId, ForwardDirection, ForwardSpec, PermissionDecision, ServerActor,
        actor::Actor as ActorKind,
    };

    use super::{
        SpecRefusal, backing_client_of, derived_forward_id, forward_needs_human, openable,
    };
    use crate::state::PendingPermission;

    fn spec(direction: ForwardDirection, name: &str, worker: u32, client: u32) -> ForwardSpec {
        ForwardSpec {
            direction: direction as i32,
            name: name.to_owned(),
            worker_port: worker,
            client_port: client,
        }
    }

    #[test]
    fn a_complete_expose_or_inject_is_openable() {
        assert_eq!(
            openable(&spec(ForwardDirection::Expose, "vite", 5173, 0)),
            Ok(ForwardDirection::Expose),
            "an expose may leave the client port to the realizing client"
        );
        assert_eq!(
            openable(&spec(ForwardDirection::Inject, "db", 5432, 5432)),
            Ok(ForwardDirection::Inject)
        );
    }

    #[test]
    fn every_incomplete_spec_is_refused_naming_the_gap() {
        let refusal = |spec: &ForwardSpec| openable(spec).expect_err("must refuse").0;

        assert!(
            refusal(&spec(ForwardDirection::Unspecified, "x", 1, 1)).contains("which way"),
            "no direction"
        );
        assert!(
            refusal(&spec(ForwardDirection::Expose, "", 1, 0)).contains("named"),
            "no name"
        );
        assert!(
            refusal(&spec(ForwardDirection::Expose, "x", 0, 0)).contains("worker-side port"),
            "no worker port"
        );
        assert!(
            refusal(&spec(ForwardDirection::Inject, "x", 1, 0)).contains("client-side port"),
            "an inject with no target on the backer's machine"
        );
    }

    #[test]
    fn a_spec_refusal_renders_its_own_words() {
        assert_eq!(SpecRefusal("just so".to_owned()).to_string(), "just so");
    }

    /// The physics gate: only an inject demands a human, because only a
    /// client's connection can back one. All three shapes matter — an expose
    /// flows the chain, an ordinary tool call is not a forward at all.
    #[test]
    fn only_a_pending_inject_needs_a_human() {
        let pending = |forward: Option<ForwardSpec>| PendingPermission {
            agent_id: "a1".to_owned(),
            tool_call: None,
            forward,
        };

        assert!(forward_needs_human(&pending(Some(spec(
            ForwardDirection::Inject,
            "db",
            5432,
            5432
        )))));
        assert!(!forward_needs_human(&pending(Some(spec(
            ForwardDirection::Expose,
            "vite",
            5173,
            0
        )))));
        assert!(!forward_needs_human(&pending(None)));
    }

    /// Only a client actor can back an inject: the chain's `ServerActor`
    /// decisions, agent actors, and unattributed decisions all yield nothing.
    #[test]
    fn only_a_client_decision_backs_an_inject() {
        let decided_by = |actor: Option<ActorKind>| PermissionDecision {
            decided_by_actor: actor.map(|actor| Actor { actor: Some(actor) }),
            ..PermissionDecision::default()
        };

        assert_eq!(
            backing_client_of(&decided_by(Some(ActorKind::ClientId(ClientId::new(
                "laptop"
            ))))),
            Some("laptop".to_owned())
        );
        assert_eq!(
            backing_client_of(&decided_by(Some(ActorKind::Server(ServerActor {})))),
            None,
            "no machine of the server's serves the traffic"
        );
        assert_eq!(
            backing_client_of(&decided_by(Some(ActorKind::AgentId(AgentId::new("a1"))))),
            None
        );
        assert_eq!(backing_client_of(&decided_by(None)), None);
        assert_eq!(
            backing_client_of(&PermissionDecision::default()),
            None,
            "an unattributed decision backs nothing"
        );
    }

    #[test]
    fn the_derived_id_is_deterministic_in_the_request() {
        assert_eq!(derived_forward_id("req-9"), "fwd-req-9");
        assert_eq!(
            derived_forward_id("req-9"),
            derived_forward_id("req-9"),
            "a restarted driver must re-derive the same id to deduplicate"
        );
    }
}

#[cfg(test)]
mod log_backed_tests {
    use horde_proto::v1::{
        AgentLifecycleChanged, AgentLifecycleState, ForwardDirection, event::Kind,
    };

    use super::super::testfix::{append, placed_expose};
    use super::{intents_for_worker, placed_on, resolve_live};
    use crate::log::EventLog;
    use crate::state::SessionState;

    /// The set is exactly the placed worker's live intents, whole — id,
    /// session, agent, and spec — and nobody else's.
    #[tokio::test]
    async fn a_worker_hears_exactly_its_own_live_intents() {
        let log = EventLog::in_memory().expect("log");
        placed_expose(&log, "s1", "agent-1", "w1").await;
        placed_expose(&log, "s2", "agent-2", "w2").await;

        let intents = intents_for_worker(&log, "w1").await.expect("intents");
        assert_eq!(intents.len(), 1, "w2's intent is not w1's to realize");
        assert_eq!(
            intents[0].forward_id.as_ref().map(ToString::to_string),
            Some("fwd-agent-1".to_owned())
        );
        assert_eq!(
            intents[0].session_id.as_ref().map(ToString::to_string),
            Some("s1".to_owned())
        );
        assert_eq!(
            intents[0].agent_id.as_ref().map(ToString::to_string),
            Some("agent-1".to_owned())
        );
        assert_eq!(
            intents[0].spec.as_ref().map(|spec| spec.worker_port),
            Some(5173),
            "the spec rides whole; the worker dials what it names"
        );

        assert!(
            intents_for_worker(&log, "w3")
                .await
                .expect("intents")
                .is_empty(),
            "an unknown worker has nothing to realize"
        );
    }

    #[tokio::test]
    async fn placed_on_answers_the_placement_or_nothing() {
        let log = EventLog::in_memory().expect("log");
        placed_expose(&log, "s1", "agent-1", "w1").await;
        let events = log.read_all("s1").await.expect("read");
        let state = SessionState::project("s1", &events).expect("project");

        assert_eq!(placed_on(&state, "agent-1"), Some("w1"));
        assert_eq!(placed_on(&state, "ghost"), None, "nothing is invented");
    }

    /// Resolution across sessions: a live forward comes back whole, and a
    /// dead one — its agent failed — resolves to nothing, because to the
    /// tunnel plane they are the same refusal.
    #[tokio::test]
    async fn resolve_live_finds_live_intents_and_refuses_dead_ones() {
        let log = EventLog::in_memory().expect("log");
        placed_expose(&log, "s1", "agent-1", "w1").await;

        let resolved = resolve_live(&log, "fwd-agent-1")
            .await
            .expect("read")
            .expect("a live forward resolves");
        assert_eq!(resolved.forward_id, "fwd-agent-1");
        assert_eq!(resolved.session_id, "s1");
        assert_eq!(resolved.direction, ForwardDirection::Expose);
        assert_eq!(resolved.name, "vite");
        assert_eq!(resolved.placed_worker.as_deref(), Some("w1"));
        assert_eq!(resolved.backer, None);

        assert!(
            resolve_live(&log, "fwd-nothing")
                .await
                .expect("read")
                .is_none(),
            "never-opened resolves to nothing"
        );

        append(
            &log,
            "s1",
            "agent-1:failed",
            "agent-1",
            Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running as i32,
                state: AgentLifecycleState::Failed as i32,
                reason: "harness exited".to_owned(),
            }),
        )
        .await;
        assert!(
            resolve_live(&log, "fwd-agent-1")
                .await
                .expect("read")
                .is_none(),
            "a dead agent's forward is the same refusal as no forward"
        );
    }
}
