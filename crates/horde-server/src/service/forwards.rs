//! The forward verbs' logic, shared by both service spines (ADR-0006).
//!
//! Both spines carry forward RPCs — the control plane's client leg and the
//! worker spine's worker leg — and both need the same identity translation
//! and the same intent bookkeeping. Like [`super::shared`], this lives
//! beside the spines so neither depends back on its parent.

use horde_proto::v1::{
    Actor, AgentId, ClientId, Event, ForwardId, ForwardIntent, ForwardOpened, ForwardSpec,
    ForwardStatus, ListForwardsResponse, OpenForwardRequest, OpenForwardResponse, SessionId,
    event::Kind,
};
use prost::Message;
use tonic::Request;

use super::shared::{ServerCore, append_server_event, append_server_event_returning, uuid_like};
use crate::error::ServerError;
use crate::forward::intent::{name_in_use, openable, placed_on};
use crate::forward::registry::TunnelPrincipal;
use crate::state::{SessionState, agent_carries_forwards};

/// Who a forward-plane stream's channel says its peer is.
///
/// The mTLS certificate is authoritative when there is one; a plaintext
/// loopback channel is [`TunnelPrincipal::Ambient`] — the bootstrap trust
/// shape, same as the git relay's ambient door.
pub fn tunnel_principal_of<T>(request: &Request<T>) -> TunnelPrincipal {
    match request.extensions().get::<horde_identity::PeerIdentity>() {
        None => TunnelPrincipal::Ambient,
        Some(identity) => match identity.principal.role() {
            horde_identity::PrincipalRole::Client => {
                TunnelPrincipal::Client(identity.principal.name().to_owned())
            }
            horde_identity::PrincipalRole::Worker => {
                TunnelPrincipal::Worker(identity.principal.name().to_owned())
            }
        },
    }
}

/// Journals a client-initiated forward intent.
///
/// `actor` is the caller as the channel identifies it: it stamps
/// `opened_by`, and a client actor becomes the backer of an inject — a
/// client-initiated inject has no selection problem, the initiator is the
/// backer.
///
/// # Errors
///
/// Refusals for an incomplete spec, an unknown or dead agent, a name already
/// live in the session, or an inject with nobody identifiable to back it —
/// plus whatever the log refuses.
pub async fn open_forward(
    core: &ServerCore,
    actor: Option<Actor>,
    request: OpenForwardRequest,
) -> Result<OpenForwardResponse, ServerError> {
    let spec = request.spec.ok_or_else(|| {
        ServerError::Invalid("an OpenForward must carry the forward's spec".to_owned())
    })?;
    let direction = openable(&spec).map_err(|refusal| ServerError::Invalid(refusal.0))?;

    let session_id = request
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let agent_id = request
        .agent_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    // A retry short-circuits before any validation: the original already
    // passed them, and re-running the duplicate-name check against the very
    // forward the first attempt opened would refuse the retry as its own
    // duplicate.
    if let Some((sequence, payload)) = core
        .log
        .stored_under_key(&session_id, &request.idempotency_key)
        .await?
        && let Some(forward_id) = opened_id_in(&payload)
    {
        return Ok(OpenForwardResponse {
            forward_id: Some(ForwardId::new(forward_id)),
            sequence,
        });
    }

    let events = core.log.read_all(&session_id).await?;
    let state = SessionState::project(&session_id, &events)?;
    check_owner(&state, &agent_id)?;
    if name_in_use(&state, &spec.name) {
        return Err(ServerError::Invalid(format!(
            "a live forward named {} already exists in session {session_id}",
            spec.name
        )));
    }

    let backer = backer_for(direction, client_named_by(actor.as_ref()))?;
    // An empty key is the log's "never deduplicate"; a caller that wants a
    // safe retry supplies one, exactly as on CreateSession.
    let opened = opened_event(&agent_id, spec, actor, backer);
    let appended =
        append_server_event_returning(&core.log, &session_id, &request.idempotency_key, opened.1)
            .await?;

    Ok(OpenForwardResponse {
        forward_id: Some(ForwardId::new(stored_or_minted(
            appended.existing_payload.as_deref(),
            opened.0,
        ))),
        sequence: appended.sequence,
    })
}

/// Which id an open answers with: the stored event's, when the append
/// deduplicated, else the freshly minted one.
///
/// The keyed pre-check already short-circuits ordinary retries, so this arm
/// is the *race* case — two concurrent opens under one key, where the loser
/// deduplicates in the append itself. Returning the fresh id there would
/// hand the caller an identity the log never heard of, which is why the
/// choice is a named function with its own table rather than an inline
/// match nothing concurrent can stage.
fn stored_or_minted(existing_payload: Option<&[u8]>, minted: String) -> String {
    match existing_payload.map(opened_id_in) {
        Some(Some(stored)) => stored,
        _ => minted,
    }
}

/// The owning agent must exist and still be able to carry intents.
fn check_owner(state: &SessionState, agent_id: &str) -> Result<(), ServerError> {
    let Some(agent) = state.agents.get(agent_id) else {
        return Err(ServerError::NoSuchAgent {
            agent_id: agent_id.to_owned(),
        });
    };
    if !agent_carries_forwards(agent.lifecycle) {
        return Err(ServerError::Invalid(format!(
            "agent {agent_id} is {:?}; the process behind the port is gone, so a forward would \
             have nothing to mean",
            agent.lifecycle
        )));
    }
    Ok(())
}

/// The backer an open records: an inject takes the initiating client, and
/// refuses when the channel names none — an unbackable intent must not be
/// journaled. An expose records nobody.
fn backer_for(
    direction: horde_proto::v1::ForwardDirection,
    client: Option<String>,
) -> Result<Option<ClientId>, ServerError> {
    if direction != horde_proto::v1::ForwardDirection::Inject {
        return Ok(None);
    }
    match client {
        Some(name) => Ok(Some(ClientId::new(name))),
        None => Err(ServerError::Invalid(
            "a client-initiated inject records its initiator as the backer, and this channel \
             names no client principal; connect over mTLS"
                .to_owned(),
        )),
    }
}

/// The client principal an actor names, if it names one.
fn client_named_by(actor: Option<&Actor>) -> Option<String> {
    match actor?.actor.as_ref()? {
        horde_proto::v1::actor::Actor::ClientId(client) => Some(client.to_string()),
        _ => None,
    }
}

/// A fresh `ForwardOpened` and the id it minted.
fn opened_event(
    agent_id: &str,
    spec: ForwardSpec,
    opened_by: Option<Actor>,
    backer: Option<ClientId>,
) -> (String, Event) {
    let forward_id = format!("fwd-{}", uuid_like());
    let event = Event {
        agent_id: Some(AgentId::new(agent_id)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::ForwardOpened(ForwardOpened {
            forward_id: Some(ForwardId::new(forward_id.clone())),
            spec: Some(spec),
            opened_by,
            backer_client_id: backer,
        })),
    };
    (forward_id, event)
}

/// The forward id a stored `ForwardOpened` payload carries.
fn opened_id_in(payload: &[u8]) -> Option<String> {
    match Event::decode(payload).ok()?.kind? {
        Kind::ForwardOpened(opened) => opened.forward_id.as_ref().map(ToString::to_string),
        _ => None,
    }
}

/// Journals an explicit close. Idempotent per forward: the keyed append
/// deduplicates a retry, and closing what is already closed changes nothing.
///
/// # Errors
///
/// A refusal for a forward the session never opened, plus the log's own.
pub async fn close_forward(
    core: &ServerCore,
    actor: Option<Actor>,
    session_id: &str,
    forward_id: &str,
    reason: String,
) -> Result<u64, ServerError> {
    let events = core.log.read_all(session_id).await?;
    let state = SessionState::project(session_id, &events)?;
    let Some(forward) = state.forwards.get(forward_id) else {
        return Err(ServerError::Invalid(format!(
            "session {session_id} has no forward {forward_id}"
        )));
    };

    let event = Event {
        agent_id: Some(AgentId::new(forward.agent_id.clone())),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(Kind::ForwardClosed(horde_proto::v1::ForwardClosed {
            forward_id: Some(ForwardId::new(forward_id)),
            closed_by: actor,
            reason,
        })),
    };
    append_server_event(
        &core.log,
        session_id,
        &format!("forward-close:{forward_id}"),
        event,
    )
    .await
}

/// Live intents plus the realization overlay the log deliberately does not
/// carry — reachability and backing are facts about connections, read from
/// the registry at the moment of asking.
///
/// # Errors
///
/// Whatever reading the log refuses.
pub async fn list_forwards(
    core: &ServerCore,
    session_filter: Option<String>,
) -> Result<ListForwardsResponse, ServerError> {
    let sessions = match session_filter {
        Some(session_id) => vec![session_id],
        None => core.log.session_ids().await?,
    };

    let mut forwards = Vec::new();
    for session_id in sessions {
        let events = core.log.read_all(&session_id).await?;
        let state = SessionState::project(&session_id, &events)?;
        for forward in state.live_forwards() {
            forwards.push(status_of(core, &session_id, &state, forward));
        }
    }
    Ok(ListForwardsResponse { forwards })
}

/// One live forward with its overlay.
fn status_of(
    core: &ServerCore,
    session_id: &str,
    state: &SessionState,
    forward: &crate::state::ForwardState,
) -> ForwardStatus {
    let placed = placed_on(state, &forward.agent_id);
    ForwardStatus {
        intent: Some(ForwardIntent {
            forward_id: Some(ForwardId::new(forward.forward_id.clone())),
            session_id: Some(SessionId::new(session_id)),
            agent_id: Some(AgentId::new(forward.agent_id.clone())),
            spec: forward.spec.clone(),
            backer_client_id: forward.backer_client_id.clone().map(ClientId::new),
        }),
        worker_reachable: placed.is_some_and(|worker| core.forwards.worker_reachable(worker)),
        backed: forward
            .backer_client_id
            .as_deref()
            .is_some_and(|backer| core.forwards.client_backing(backer)),
    }
}

/// One spine's tunnel answer: its response type, boxed and severable.
pub(crate) type TunnelAnswer<T> =
    std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<T, tonic::Status>> + Send>>;

/// A leg's inbound, stripped to bare frames.
type FrameStream = std::pin::Pin<
    Box<
        dyn tokio_stream::Stream<Item = Result<horde_proto::v1::ForwardTunnelFrame, tonic::Status>>
            + Send,
    >,
>;

/// One tunnel leg answered whole: identity and serial read off the request,
/// the relay driven over bare frames, an adapter lifting them into the
/// spine's response type, and revocation severing the stream. Shared by
/// both spines — the client leg and the worker leg differ only in their
/// wrapper types, and two copies of this plumbing is exactly the drift the
/// duplication gate exists to refuse.
pub(crate) fn tunnel_response<Req, T, U, W>(
    core: &ServerCore,
    deny: Option<horde_identity::RevocationList>,
    request: tonic::Request<tonic::Streaming<Req>>,
    unwrap: U,
    wrap: W,
) -> tonic::Response<TunnelAnswer<T>>
where
    Req: Send + 'static,
    U: Fn(Req) -> horde_proto::v1::ForwardTunnelFrame + Send + 'static,
    T: Send + 'static,
    W: Fn(horde_proto::v1::ForwardTunnelFrame) -> T + Send + 'static,
{
    use tokio_stream::StreamExt as _;
    let serial = super::shared::peer_serial(&request);
    let caller = tunnel_principal_of(&request);
    let inbound = request
        .into_inner()
        .map(move |message| message.map(&unwrap));
    tonic::Response::new(tunnel_stream(
        core,
        deny,
        serial,
        caller,
        Box::pin(inbound),
        wrap,
    ))
}

fn tunnel_stream<T, W>(
    core: &ServerCore,
    deny: Option<horde_identity::RevocationList>,
    serial: Option<String>,
    caller: TunnelPrincipal,
    inbound: FrameStream,
    wrap: W,
) -> TunnelAnswer<T>
where
    T: Send + 'static,
    W: Fn(horde_proto::v1::ForwardTunnelFrame) -> T + Send + 'static,
{
    let (typed, stream) = tokio::sync::mpsc::channel(16);
    let (frames, frame_rx) = tokio::sync::mpsc::channel(16);
    let log = core.log.clone();
    let registry = core.forwards.clone();
    let logins = core.logins.clone();

    super::shared::lift_frames(frame_rx, typed.clone(), wrap);
    tokio::spawn(async move {
        let pump = crate::forward::relay::run_tunnel(
            &log,
            &registry,
            &logins,
            caller,
            crate::forward::relay::LegIo {
                inbound,
                outbound: frames,
            },
        );
        super::shared::run_severable(pump, deny, serial, &typed).await;
    });
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(stream))
}

/// Whether an appended event can change any worker's realized intent set.
///
/// The worker forward streams recompute their set on log movement, and this
/// is the filter that keeps a chatty agent from turning every output chunk
/// into a whole-log projection: opens and closes change the set directly,
/// lifecycle changes carry intent death and revival, and placement changes
/// move an agent's intents between workers. Everything else cannot.
#[must_use]
pub fn affects_forwards(kind: Option<&Kind>) -> bool {
    matches!(
        kind,
        Some(
            Kind::ForwardOpened(_)
                | Kind::ForwardClosed(_)
                | Kind::AgentLifecycleChanged(_)
                | Kind::PlacementChanged(_)
                // Login lifecycle moves the ephemeral callback expose in and
                // out of the merged intent set (DESIGN §4): the started
                // append is deliberately what wakes the push that makes the
                // worker's dialable map carry the callback port.
                | Kind::WorkerLoginStarted(_)
                | Kind::WorkerLoginEnded(_)
        )
    )
}

/// Whether any of a batch of freshly notified rows could change intent sets.
///
/// Reads exactly the notified rows, never the whole log.
///
/// # Errors
///
/// Whatever reading the log refuses.
pub async fn any_affects_forwards(
    core: &ServerCore,
    notes: &[crate::log::Notification],
) -> Result<bool, ServerError> {
    for note in notes {
        let rows = core
            .log
            .read_from(&note.session_id, note.sequence, 1)
            .await?;
        for row in rows {
            let event = Event::decode(row.payload.as_slice())?;
            if affects_forwards(event.kind.as_ref()) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentOutputChunk, AgentSpawned, ForwardClosed, ForwardOpened, PlacementChanged, event::Kind,
    };

    use super::affects_forwards;

    /// The race arm's table: a deduplicated append answers with the stored
    /// event's id, anything else with the minted one — including a stored
    /// payload that is not a `ForwardOpened`, which the kind-collision
    /// refusal upstream makes unreachable but this choice must not invent
    /// an id for.
    #[test]
    fn a_deduplicated_open_answers_with_the_stored_id() {
        use horde_proto::prost::Message;

        let stored = horde_proto::v1::Event {
            kind: Some(Kind::ForwardOpened(ForwardOpened {
                forward_id: Some(horde_proto::v1::ForwardId::new("fwd-first")),
                ..ForwardOpened::default()
            })),
            ..horde_proto::v1::Event::default()
        }
        .encode_to_vec();

        assert_eq!(
            super::stored_or_minted(Some(&stored), "fwd-fresh".to_owned()),
            "fwd-first",
            "the log's identity outranks the loser's mint"
        );
        assert_eq!(
            super::stored_or_minted(None, "fwd-fresh".to_owned()),
            "fwd-fresh",
            "a fresh append keeps its mint"
        );
        let not_an_open = horde_proto::v1::Event::default().encode_to_vec();
        assert_eq!(
            super::stored_or_minted(Some(&not_an_open), "fwd-fresh".to_owned()),
            "fwd-fresh",
            "a stored non-open yields no identity to steal"
        );
    }

    /// Only a client actor names a backer; servers and agents name nothing.
    #[test]
    fn only_a_client_actor_names_a_principal() {
        use horde_proto::v1::{Actor, AgentId, ClientId, ServerActor, actor::Actor as ActorKind};

        let of = |kind: Option<ActorKind>| {
            super::client_named_by(kind.map(|actor| Actor { actor: Some(actor) }).as_ref())
        };
        assert_eq!(
            of(Some(ActorKind::ClientId(ClientId::new("laptop")))),
            Some("laptop".to_owned())
        );
        assert_eq!(of(Some(ActorKind::Server(ServerActor {}))), None);
        assert_eq!(of(Some(ActorKind::AgentId(AgentId::new("a1")))), None);
        assert_eq!(of(None), None);
        assert_eq!(super::client_named_by(None), None);
    }

    /// The recompute trigger over real rows: a chatty append must answer
    /// false, an intent-moving one true — both directions, because the
    /// mutants replacing this with a constant each pass one half.
    #[tokio::test]
    async fn any_affects_forwards_reads_exactly_the_notified_rows() {
        use crate::log::Notification;

        let core = super::ServerCore::new(crate::log::EventLog::in_memory().expect("log"));
        crate::forward::testfix::append(
            &core.log,
            "s1",
            "chunk",
            "agent-1",
            Kind::AgentOutputChunk(AgentOutputChunk::default()),
        )
        .await;
        crate::forward::testfix::append(
            &core.log,
            "s1",
            "open",
            "agent-1",
            Kind::ForwardOpened(ForwardOpened::default()),
        )
        .await;

        let note = |sequence| Notification {
            session_id: "s1".to_owned(),
            sequence,
        };
        assert!(
            !super::any_affects_forwards(&core, &[note(1)])
                .await
                .expect("read"),
            "an output chunk moves no intent set"
        );
        assert!(
            super::any_affects_forwards(&core, &[note(2)])
                .await
                .expect("read"),
            "an open does"
        );
        assert!(
            !super::any_affects_forwards(&core, &[]).await.expect("read"),
            "no rows, no recompute"
        );
    }

    /// The filter's table: the four kinds that move intent sets, and the two
    /// commonest kinds that must not — output chunks are the chatty case the
    /// filter exists for.
    #[test]
    fn only_intent_moving_kinds_trigger_a_recompute() {
        assert!(affects_forwards(Some(&Kind::ForwardOpened(
            ForwardOpened::default()
        ))));
        assert!(affects_forwards(Some(&Kind::ForwardClosed(
            ForwardClosed::default()
        ))));
        assert!(affects_forwards(Some(&Kind::AgentLifecycleChanged(
            horde_proto::v1::AgentLifecycleChanged::default()
        ))));
        assert!(affects_forwards(Some(&Kind::PlacementChanged(
            PlacementChanged::default()
        ))));

        assert!(!affects_forwards(Some(&Kind::AgentOutputChunk(
            AgentOutputChunk::default()
        ))));
        assert!(!affects_forwards(Some(&Kind::AgentSpawned(
            AgentSpawned::default()
        ))));
        assert!(!affects_forwards(None));
    }
}
