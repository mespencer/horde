//! The byte plane: pairing and splicing the two legs of one tunneled TCP
//! connection.
//!
//! Every tunnel is two gRPC bidi streams meeting here — the originating
//! peer's (a client for an expose, the worker for an inject) and the passive
//! peer's, opened in answer to a `ForwardConnectionRequest`. The relay
//! validates the originator against the *log's* view of the forward, parks
//! the connection, dispatches the request down the passive side's serve
//! lane, and then moves frames one at a time in both directions. Bounded
//! channels and per-frame awaits mean HTTP/2 flow control on either leg is
//! the backpressure for the whole path.
//!
//! Every wait here is ceilinged except the splice itself, whose lifetime is
//! the connection's — ended by structure (both directions closing), never by
//! clock.

use std::time::Duration;

use horde_proto::forward::{accepted_frame, refusal_frame};
use horde_proto::v1::{
    ForwardConnectionRequest, ForwardDirection, ForwardId, ForwardRefusalReason,
    ForwardTunnelFrame, ForwardTunnelOpen, forward_tunnel_frame::Frame,
};
use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt};
use tonic::Status;

use super::intent::{self, ResolvedForward};
use super::registry::{Claim, ForwardRegistry, PassiveLeg, PassiveTarget, TunnelPrincipal};
use crate::log::EventLog;

/// How long the opening frame may take to arrive.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// How long the passive side has to dial its target and answer.
const PAIR_DEADLINE: Duration = Duration::from_secs(10);

/// Depth of each splice channel: enough to keep bytes moving, small enough
/// that a stalled peer exerts backpressure within a few frames.
const SPLICE_DEPTH: usize = 16;

/// One leg's I/O, stripped to bare frames. The service layer owns the gRPC
/// wrapper types and the severing; the relay never learns them — which is
/// also what keeps every function here monomorphic and the two spines'
/// handlers one shared bridge.
pub struct LegIo {
    pub inbound: std::pin::Pin<Box<dyn Stream<Item = Result<ForwardTunnelFrame, Status>> + Send>>,
    pub outbound: mpsc::Sender<ForwardTunnelFrame>,
}

impl LegIo {
    /// Sends one frame toward this leg's peer, shrugging when it is gone —
    /// a peer that hung up has no use for the refusal it caused. `&mut` for
    /// the same reason as the client engine's `Tunnel`: a `&LegIo` held
    /// across an await drags the non-`Sync` boxed stream into the bound.
    async fn say(&mut self, frame: ForwardTunnelFrame) {
        let _ = self.outbound.send(frame).await;
    }
}

/// Runs one tunnel stream to completion: handshake, claim-or-originate,
/// splice.
pub async fn run_tunnel(
    log: &EventLog,
    registry: &ForwardRegistry,
    logins: &crate::login::registry::LoginRegistry,
    caller: TunnelPrincipal,
    mut io: LegIo,
) {
    let open = match handshake(&mut io).await {
        Ok(open) => open,
        Err(refusal) => return io.say(refusal).await,
    };
    match registry.claim(&open.connection_id, &caller) {
        Claim::Matched(pair) => run_passive(pair, io).await,
        Claim::WrongPrincipal => {
            let mut io = io;
            io.say(refusal_frame(
                ForwardRefusalReason::WrongPrincipal,
                format!(
                    "connection {} was dispatched to a different peer",
                    open.connection_id
                ),
            ))
            .await;
        }
        Claim::Unknown => run_originator(log, registry, logins, caller, open, io).await,
    }
}

/// Waits (ceilinged) for the opening frame.
async fn handshake(io: &mut LegIo) -> Result<ForwardTunnelOpen, ForwardTunnelFrame> {
    let protocol_refusal = || {
        refusal_frame(
            ForwardRefusalReason::Unspecified,
            "a tunnel's first frame must be `open`, within the handshake deadline",
        )
    };
    match tokio::time::timeout(HANDSHAKE_DEADLINE, io.inbound.next()).await {
        Ok(Some(Ok(ForwardTunnelFrame {
            frame: Some(Frame::Open(open)),
        }))) => Ok(open),
        _ => Err(protocol_refusal()),
    }
}

/// The passive side: complete the pairing, then splice.
async fn run_passive(pair: tokio::sync::oneshot::Sender<PassiveLeg>, io: LegIo) {
    let (to_origin, from_passive) = mpsc::channel(SPLICE_DEPTH);
    let (to_passive, from_origin) = mpsc::channel(SPLICE_DEPTH);
    if pair
        .send(PassiveLeg {
            to_passive,
            from_passive,
        })
        .is_err()
    {
        // The originator stopped waiting. Ending the stream is the message;
        // the passive peer tears its dialed connection down on it.
        return;
    }
    splice(io, to_origin, from_origin).await;
}

/// The originating side: validate, dispatch, wait for the pairing, splice.
async fn run_originator(
    log: &EventLog,
    registry: &ForwardRegistry,
    logins: &crate::login::registry::LoginRegistry,
    caller: TunnelPrincipal,
    open: ForwardTunnelOpen,
    mut io: LegIo,
) {
    let leg = match originate(log, registry, logins, &caller, &open).await {
        Ok(leg) => leg,
        Err(refusal) => return io.say(refusal).await,
    };
    io.say(accepted_frame()).await;
    splice(io, leg.to_passive, leg.from_passive).await;
}

/// Everything between "an open frame arrived" and "both legs are up".
async fn originate(
    log: &EventLog,
    registry: &ForwardRegistry,
    logins: &crate::login::registry::LoginRegistry,
    caller: &TunnelPrincipal,
    open: &ForwardTunnelOpen,
) -> Result<PassiveLeg, ForwardTunnelFrame> {
    let forward_id = open
        .forward_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let resolved = intent::resolve_live(log, &forward_id)
        .await
        .map_err(|error| {
            refusal_frame(
                ForwardRefusalReason::Unspecified,
                format!("the relay could not read the log: {error}"),
            )
        })?
        // Not in the log by design: a live login's callback expose is
        // ephemeral intent, resolved from the login registry and ruled on
        // exactly like a journaled expose — clients originate, the placed
        // (login-hosting) worker carries.
        .or_else(|| logins.callback_expose(&forward_id).map(callback_resolved));
    let target =
        originate_ruling(resolved.as_ref(), caller).map_err(|refusal| ForwardTunnelFrame {
            frame: Some(Frame::Refused(refusal.into_refusal())),
        })?;

    let Some(lane) = registry.lane_to(&target) else {
        return Err(missing_lane_refusal(&target));
    };
    let paired = registry.park_pending(&open.connection_id, target.clone());
    let request = ForwardConnectionRequest {
        forward_id: Some(ForwardId::new(forward_id)),
        connection_id: open.connection_id.clone(),
    };
    if lane.try_send(request).is_err() {
        registry.abandon_pending(&open.connection_id);
        return Err(missing_lane_refusal(&target));
    }
    match tokio::time::timeout(PAIR_DEADLINE, paired).await {
        Ok(Ok(leg)) => Ok(leg),
        _ => {
            registry.abandon_pending(&open.connection_id);
            Err(unanswered_refusal(&target))
        }
    }
}

/// A live login's callback expose as the ruling sees it: an expose placed
/// on the login-hosting worker, backed by nobody. `session_id` is empty —
/// the login owns no session, and nothing in the ruling reads it.
fn callback_resolved(expose: crate::login::registry::CallbackExpose) -> ResolvedForward {
    ResolvedForward {
        forward_id: crate::login::registry::callback_forward_id(&expose.login_id),
        session_id: String::new(),
        direction: ForwardDirection::Expose,
        name: format!("{}-login-callback", expose.provider),
        placed_worker: Some(expose.worker_id),
        backer: None,
    }
}

/// A ruling refusal: the reason code plus its human detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub reason: ForwardRefusalReason,
    pub detail: String,
}

impl Refusal {
    fn new(reason: ForwardRefusalReason, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    fn into_refusal(self) -> horde_proto::v1::ForwardTunnelRefusal {
        horde_proto::v1::ForwardTunnelRefusal {
            reason: self.reason as i32,
            detail: self.detail,
        }
    }
}

/// Who carries the far side of an originated connection — or why nobody can.
///
/// This is the routing truth table (ADR-0006). Direction decides the roles:
/// an expose is originated by clients and carried by the placed worker; an
/// inject is originated by that worker and carried by the backer. Ambient
/// (plaintext loopback) peers may originate either, because locality is the
/// bootstrap trust.
pub fn originate_ruling(
    resolved: Option<&ResolvedForward>,
    caller: &TunnelPrincipal,
) -> Result<PassiveTarget, Refusal> {
    let Some(forward) = resolved else {
        return Err(Refusal::new(
            ForwardRefusalReason::UnknownForward,
            "no live forward by that id: never opened, closed, or its agent is gone",
        ));
    };
    match forward.direction {
        ForwardDirection::Expose => expose_ruling(forward, caller),
        ForwardDirection::Inject => inject_ruling(forward, caller),
        ForwardDirection::Unspecified => Err(Refusal::new(
            ForwardRefusalReason::UnknownForward,
            "the forward's journaled spec names no direction",
        )),
    }
}

/// Expose: any client (or ambient peer) originates; the placed worker carries.
fn expose_ruling(
    forward: &ResolvedForward,
    caller: &TunnelPrincipal,
) -> Result<PassiveTarget, Refusal> {
    if let TunnelPrincipal::Worker(name) = caller {
        return Err(Refusal::new(
            ForwardRefusalReason::WrongPrincipal,
            format!("an expose is originated by clients; {name} is a worker"),
        ));
    }
    match &forward.placed_worker {
        Some(worker) => Ok(PassiveTarget::Worker(worker.clone())),
        None => Err(Refusal::new(
            ForwardRefusalReason::WorkerUnreachable,
            format!(
                "forward {} has no placed worker: its agent is not placed",
                forward.name
            ),
        )),
    }
}

/// Inject: only the placed worker (or an ambient peer) originates; the
/// backer carries.
fn inject_ruling(
    forward: &ResolvedForward,
    caller: &TunnelPrincipal,
) -> Result<PassiveTarget, Refusal> {
    let allowed = match caller {
        TunnelPrincipal::Worker(name) => is_the_placed_worker(forward, name),
        TunnelPrincipal::Ambient => true,
        TunnelPrincipal::Client(_) => false,
    };
    if !allowed {
        return Err(Refusal::new(
            ForwardRefusalReason::WrongPrincipal,
            format!(
                "an inject is originated by its agent's worker ({})",
                forward.placed_worker.as_deref().unwrap_or("unplaced")
            ),
        ));
    }
    match &forward.backer {
        Some(backer) => Ok(PassiveTarget::Client(backer.clone())),
        None => Err(Refusal::new(
            ForwardRefusalReason::Unbacked,
            format!("inject {} records no backer", forward.name),
        )),
    }
}

/// Whether a worker principal is the one the forward's agent is placed on.
///
/// Both halves matter: an unplaced agent has no worker at all (so *no*
/// worker principal may originate), and a placed one admits exactly its own.
/// The mismatch row is unreachable through an honest worker, which is why
/// this is a named predicate rather than an inline compare.
fn is_the_placed_worker(forward: &ResolvedForward, name: &str) -> bool {
    forward.placed_worker.as_deref() == Some(name)
}

/// The refusal for a passive side that is not connected (or not draining its
/// lane, which refuses the same way: the peer is not carrying connections).
fn missing_lane_refusal(target: &PassiveTarget) -> ForwardTunnelFrame {
    match target {
        PassiveTarget::Worker(worker) => refusal_frame(
            ForwardRefusalReason::WorkerUnreachable,
            format!("worker {worker} has no live forward stream"),
        ),
        PassiveTarget::Client(backer) => refusal_frame(
            ForwardRefusalReason::Unbacked,
            format!("backer {backer} is detached; its return re-backs this inject"),
        ),
    }
}

/// The refusal for a passive side that was dispatched to but never answered.
fn unanswered_refusal(target: &PassiveTarget) -> ForwardTunnelFrame {
    match target {
        PassiveTarget::Worker(worker) => refusal_frame(
            ForwardRefusalReason::WorkerUnreachable,
            format!(
                "worker {worker} did not answer within {}s",
                PAIR_DEADLINE.as_secs()
            ),
        ),
        PassiveTarget::Client(backer) => refusal_frame(
            ForwardRefusalReason::Unbacked,
            format!(
                "backer {backer} did not answer within {}s",
                PAIR_DEADLINE.as_secs()
            ),
        ),
    }
}

/// Moves frames both ways until either leg ends.
///
/// TCP half-close is the explicit `eof` *frame*, relayed like any other; a
/// leg's stream ending is full teardown — the peer process finished or
/// died — and must end the whole tunnel, or a worker partition would leave
/// every idle client connection hanging open against a relay with nothing
/// on the other side. Ends by structure, no clock: every arm's terminal
/// case returns, and returning drops both channel ends, which is what the
/// paired splice observes as its own terminal case.
async fn splice(
    mut io: LegIo,
    to_peer: mpsc::Sender<ForwardTunnelFrame>,
    mut from_peer: mpsc::Receiver<ForwardTunnelFrame>,
) {
    loop {
        tokio::select! {
            frame = io.inbound.next() => {
                let Some(Ok(frame)) = frame else {
                    return; // This leg ended or errored: teardown.
                };
                if to_peer.send(frame).await.is_err() {
                    return; // The paired splice returned already.
                }
            }
            frame = from_peer.recv() => {
                let Some(frame) = frame else {
                    return; // The peer leg ended: teardown.
                };
                if io.outbound.send(frame).await.is_err() {
                    return; // Our own peer hung up; nothing left to carry.
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::forward::{accepted_frame, data_frame, eof_frame, open_frame, refusal_in};
    use horde_proto::v1::{ForwardDirection, ForwardRefusalReason, ForwardTunnelFrame};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::Status;

    use super::super::registry::{PassiveTarget, TunnelPrincipal};
    use super::{Refusal, ResolvedForward, originate_ruling, splice};

    fn resolved(
        direction: ForwardDirection,
        placed: Option<&str>,
        backer: Option<&str>,
    ) -> ResolvedForward {
        ResolvedForward {
            forward_id: "fwd-1".to_owned(),
            session_id: "s1".to_owned(),
            direction,
            name: "vite".to_owned(),
            placed_worker: placed.map(ToString::to_string),
            backer: backer.map(ToString::to_string),
        }
    }

    fn client(name: &str) -> TunnelPrincipal {
        TunnelPrincipal::Client(name.to_owned())
    }

    fn worker(name: &str) -> TunnelPrincipal {
        TunnelPrincipal::Worker(name.to_owned())
    }

    fn reason_of(ruling: Result<PassiveTarget, Refusal>) -> ForwardRefusalReason {
        match ruling {
            Ok(target) => panic!("expected a refusal, got {target:?}"),
            Err(refusal) => refusal.reason,
        }
    }

    // -- the routing truth table --------------------------------------------

    #[test]
    fn an_unknown_or_directionless_forward_refuses_everyone() {
        assert_eq!(
            reason_of(originate_ruling(None, &client("laptop"))),
            ForwardRefusalReason::UnknownForward
        );
        assert_eq!(
            reason_of(originate_ruling(
                Some(&resolved(ForwardDirection::Unspecified, Some("w1"), None)),
                &client("laptop"),
            )),
            ForwardRefusalReason::UnknownForward
        );
    }

    #[test]
    fn an_expose_is_carried_by_the_placed_worker_for_any_client() {
        let forward = resolved(ForwardDirection::Expose, Some("w1"), None);
        assert_eq!(
            originate_ruling(Some(&forward), &client("laptop")),
            Ok(PassiveTarget::Worker("w1".to_owned()))
        );
        assert_eq!(
            originate_ruling(Some(&forward), &TunnelPrincipal::Ambient),
            Ok(PassiveTarget::Worker("w1".to_owned())),
            "loopback locality is the bootstrap trust"
        );
    }

    #[test]
    fn a_worker_may_not_originate_an_expose() {
        let forward = resolved(ForwardDirection::Expose, Some("w1"), None);
        assert_eq!(
            reason_of(originate_ruling(Some(&forward), &worker("w1"))),
            ForwardRefusalReason::WrongPrincipal,
            "even the placed worker: an expose flows toward clients"
        );
    }

    #[test]
    fn an_unplaced_expose_refuses_as_worker_unreachable() {
        let forward = resolved(ForwardDirection::Expose, None, None);
        assert_eq!(
            reason_of(originate_ruling(Some(&forward), &client("laptop"))),
            ForwardRefusalReason::WorkerUnreachable
        );
    }

    #[test]
    fn an_inject_is_originated_by_its_placed_worker_and_carried_by_the_backer() {
        let forward = resolved(ForwardDirection::Inject, Some("w1"), Some("laptop"));
        assert_eq!(
            originate_ruling(Some(&forward), &worker("w1")),
            Ok(PassiveTarget::Client("laptop".to_owned()))
        );
        assert_eq!(
            originate_ruling(Some(&forward), &TunnelPrincipal::Ambient),
            Ok(PassiveTarget::Client("laptop".to_owned()))
        );
    }

    #[test]
    fn no_other_principal_may_originate_an_inject() {
        let forward = resolved(ForwardDirection::Inject, Some("w1"), Some("laptop"));
        assert_eq!(
            reason_of(originate_ruling(Some(&forward), &worker("w2"))),
            ForwardRefusalReason::WrongPrincipal,
            "a different worker is not this agent's worker"
        );
        assert_eq!(
            reason_of(originate_ruling(Some(&forward), &client("laptop"))),
            ForwardRefusalReason::WrongPrincipal,
            "not even the backer originates worker-side connections"
        );

        let unplaced = resolved(ForwardDirection::Inject, None, Some("laptop"));
        assert_eq!(
            reason_of(originate_ruling(Some(&unplaced), &worker("w1"))),
            ForwardRefusalReason::WrongPrincipal,
            "an unplaced agent has no worker at all"
        );
    }

    #[test]
    fn an_inject_with_no_recorded_backer_is_unbacked() {
        let forward = resolved(ForwardDirection::Inject, Some("w1"), None);
        assert_eq!(
            reason_of(originate_ruling(Some(&forward), &worker("w1"))),
            ForwardRefusalReason::Unbacked
        );
    }

    // -- the splice ---------------------------------------------------------

    /// Everything a splice needs on the test's side of the fence.
    struct SpliceHarness {
        /// Feeds the inbound gRPC stream.
        inbound: tokio::sync::mpsc::Sender<Result<ForwardTunnelFrame, Status>>,
        /// What the splice pushed toward the paired peer.
        to_peer: tokio::sync::mpsc::Receiver<ForwardTunnelFrame>,
        /// Feeds frames as the paired peer.
        from_peer: tokio::sync::mpsc::Sender<ForwardTunnelFrame>,
        /// What the splice sent back down its own leg.
        outbound: tokio::sync::mpsc::Receiver<ForwardTunnelFrame>,
        task: tokio::task::JoinHandle<()>,
    }

    /// A [`super::LegIo`] the test plays the peer of: feed the returned
    /// sender, read the returned receiver.
    fn scripted_leg() -> (
        super::LegIo,
        tokio::sync::mpsc::Sender<Result<ForwardTunnelFrame, Status>>,
        tokio::sync::mpsc::Receiver<ForwardTunnelFrame>,
    ) {
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(8);
        let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(8);
        let io = super::LegIo {
            inbound: Box::pin(ReceiverStream::new(inbound_rx)),
            outbound: outbound_tx,
        };
        (io, inbound_tx, outbound_rx)
    }

    fn spliced() -> SpliceHarness {
        let (io, inbound_tx, outbound_rx) = scripted_leg();
        let (to_peer_tx, to_peer_rx) = tokio::sync::mpsc::channel(8);
        let (from_peer_tx, from_peer_rx) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move {
            splice(io, to_peer_tx, from_peer_rx).await;
        });
        SpliceHarness {
            inbound: inbound_tx,
            to_peer: to_peer_rx,
            from_peer: from_peer_tx,
            outbound: outbound_rx,
            task,
        }
    }

    /// A ceilinged receive: the positive wait this suite is allowed.
    async fn within<T>(what: &str, receiver: &mut tokio::sync::mpsc::Receiver<T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), receiver.recv())
            .await
            .unwrap_or_else(|_| panic!("{what}: nothing arrived within the ceiling"))
            .unwrap_or_else(|| panic!("{what}: the channel closed instead"))
    }

    #[tokio::test]
    async fn frames_flow_both_ways_through_a_splice() {
        let mut harness = spliced();

        harness
            .inbound
            .send(Ok(data_frame(vec![1, 2])))
            .await
            .expect("send inbound");
        let relayed = within("inbound reaches the peer", &mut harness.to_peer).await;
        assert_eq!(relayed, data_frame(vec![1, 2]));

        harness
            .from_peer
            .send(data_frame(vec![3]))
            .await
            .expect("send from peer");
        let echoed = within("peer frames reach outbound", &mut harness.outbound).await;
        assert_eq!(echoed, data_frame(vec![3]));

        harness.task.abort();
    }

    /// A leg's stream ending is teardown, not half-close: the peer channel
    /// closes and the splice returns. Half-close is the `eof` frame, which
    /// relays like data — the previous test's second exchange proves that.
    /// This is the partition contract: a dead worker leg must not leave an
    /// idle client connection hanging.
    #[tokio::test]
    async fn a_legs_stream_ending_tears_the_whole_splice_down() {
        let mut harness = spliced();

        drop(harness.inbound);
        let peer_saw_close =
            tokio::time::timeout(std::time::Duration::from_secs(5), harness.to_peer.recv())
                .await
                .expect("the peer channel must close within the ceiling");
        assert!(
            peer_saw_close.is_none(),
            "a dead leg closes the peer channel"
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), harness.task)
            .await
            .expect("the splice must end with its leg, not idle on")
            .expect("the splice task must not panic");
    }

    /// The eof frame relays as a frame — half-close is carried, never
    /// interpreted as teardown by the relay.
    #[tokio::test]
    async fn an_eof_frame_relays_without_ending_the_splice() {
        let mut harness = spliced();
        harness.inbound.send(Ok(eof_frame())).await.expect("send");
        let relayed = within("eof relays as a frame", &mut harness.to_peer).await;
        assert_eq!(relayed, eof_frame());

        // Still alive: the other direction delivers after the eof.
        harness
            .from_peer
            .send(data_frame(vec![9]))
            .await
            .expect("send from peer");
        let echoed = within(
            "frames still flow after a relayed eof",
            &mut harness.outbound,
        )
        .await;
        assert_eq!(echoed, data_frame(vec![9]));
        harness.task.abort();
    }

    /// An inbound *error* (peer reset) ends the splice like an end does.
    #[tokio::test]
    async fn an_inbound_error_tears_the_splice_down() {
        let mut harness = spliced();
        harness
            .inbound
            .send(Err(Status::unavailable("reset")))
            .await
            .expect("send error");
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(5), harness.to_peer.recv())
                .await
                .expect("must close within the ceiling");
        assert!(closed.is_none());
        tokio::time::timeout(std::time::Duration::from_secs(5), harness.task)
            .await
            .expect("the splice must end on an errored leg")
            .expect("no panic");
    }

    // -- the whole tunnel, over scripted legs -------------------------------

    /// A scripted leg driven through the full [`super::run_tunnel`], with
    /// the registry and log the test arranged.
    /// One scripted leg's handles: its feed, its outbound, its task.
    type LegHandles = (
        tokio::sync::mpsc::Sender<Result<ForwardTunnelFrame, Status>>,
        tokio::sync::mpsc::Receiver<ForwardTunnelFrame>,
        tokio::task::JoinHandle<()>,
    );

    fn tunneling(
        log: &crate::log::EventLog,
        registry: &super::ForwardRegistry,
        caller: TunnelPrincipal,
    ) -> LegHandles {
        let (io, feed, out) = scripted_leg();
        let log = log.clone();
        let registry = registry.clone();
        let task = tokio::spawn(async move {
            let logins = crate::login::registry::LoginRegistry::new();
            super::run_tunnel(&log, &registry, &logins, caller, io).await;
        });
        (feed, out, task)
    }

    /// A log holding one live expose (`fwd-agent-1`, placed on `w1`) and a
    /// fresh registry — the arrangement every ruling test shares.
    async fn expose_fixture() -> (crate::log::EventLog, super::ForwardRegistry) {
        let log = crate::log::EventLog::in_memory().expect("log");
        crate::forward::testfix::placed_expose(&log, "s1", "agent-1", "w1").await;
        (log, super::ForwardRegistry::new())
    }

    /// One leg run as `caller`, its opening frame for `fwd-agent-1`/`c1`
    /// already sent.
    async fn opened_as(
        log: &crate::log::EventLog,
        registry: &super::ForwardRegistry,
        caller: TunnelPrincipal,
    ) -> LegHandles {
        let (feed, out, task) = tunneling(log, registry, caller);
        feed.send(Ok(open_frame("fwd-agent-1", "c1")))
            .await
            .expect("the opening frame sends");
        (feed, out, task)
    }

    /// A protocol breach is refused on the offender's own leg — the send
    /// the `say` helper exists for.
    #[tokio::test]
    async fn a_tunnel_opening_with_data_is_refused_on_its_own_leg() {
        let log = crate::log::EventLog::in_memory().expect("log");
        let registry = super::ForwardRegistry::new();
        let (feed, mut out, task) = tunneling(&log, &registry, TunnelPrincipal::Ambient);

        feed.send(Ok(data_frame(vec![1]))).await.expect("send");
        let refused = within("the protocol refusal arrives", &mut out).await;
        let refusal = refusal_in(&refused).expect("a refusal frame");
        assert!(refusal.detail.contains("first frame"), "{}", refusal.detail);
        task.abort();
    }

    /// The ruling's refusal crosses the wire with its code intact — a
    /// defaulted conversion would say `UNSPECIFIED` to a peer that branches
    /// on the reason.
    #[tokio::test]
    async fn a_wrong_principal_refusal_carries_its_reason_code() {
        let (log, registry) = expose_fixture().await;
        let (_feed, mut out, task) =
            opened_as(&log, &registry, TunnelPrincipal::Worker("w1".to_owned())).await;
        let refused = within("the ruling refusal arrives", &mut out).await;
        let refusal = refusal_in(&refused).expect("a refusal frame");
        assert_eq!(
            refusal.reason,
            ForwardRefusalReason::WrongPrincipal as i32,
            "the code is the contract; the detail is for humans"
        );
        task.abort();
    }

    /// No lane registered: the originator hears WORKER_UNREACHABLE naming
    /// the worker, fast — the whole fast-refusal promise, at the relay.
    #[tokio::test]
    async fn an_absent_worker_lane_refuses_naming_the_worker() {
        let (log, registry) = expose_fixture().await;
        let (_feed, mut out, task) = opened_as(&log, &registry, TunnelPrincipal::Ambient).await;
        let refused = within("the unreachable refusal arrives", &mut out).await;
        let refusal = refusal_in(&refused).expect("a refusal frame");
        assert_eq!(
            refusal.reason,
            ForwardRefusalReason::WorkerUnreachable as i32
        );
        assert!(refusal.detail.contains("w1"), "{}", refusal.detail);
        task.abort();
    }

    /// The whole pairing: originator dispatched down the lane, the passive
    /// side claims, `accepted` reaches the originator, and frames cross both
    /// ways. This is the arm-by-arm proof that pairing succeeds — a deleted
    /// success arm turns this accepted into a refusal.
    #[tokio::test]
    async fn two_legs_pair_and_carry_frames_both_ways() {
        let (log, registry) = expose_fixture().await;
        let (_epoch, mut lane) = registry.register_worker("w1");

        let (origin_feed, mut origin_out, origin_task) =
            opened_as(&log, &registry, TunnelPrincipal::Ambient).await;

        let dispatched = tokio::time::timeout(std::time::Duration::from_secs(5), lane.recv())
            .await
            .expect("the lane must hear the dispatch")
            .expect("a connection request");
        assert_eq!(dispatched.connection_id, "c1");
        assert_eq!(
            dispatched.forward_id.as_ref().map(ToString::to_string),
            Some("fwd-agent-1".to_owned())
        );

        let (answer_feed, mut answer_out, answer_task) =
            opened_as(&log, &registry, TunnelPrincipal::Worker("w1".to_owned())).await;

        let accepted = within("the originator hears accepted", &mut origin_out).await;
        assert_eq!(accepted, accepted_frame());

        origin_feed
            .send(Ok(data_frame(b"request".to_vec())))
            .await
            .expect("send");
        let crossed = within("origin bytes reach the answerer", &mut answer_out).await;
        assert_eq!(crossed, data_frame(b"request".to_vec()));

        answer_feed
            .send(Ok(data_frame(b"response".to_vec())))
            .await
            .expect("send");
        let echoed = within("answer bytes reach the originator", &mut origin_out).await;
        assert_eq!(echoed, data_frame(b"response".to_vec()));

        origin_task.abort();
        answer_task.abort();
    }

    // -- refusal plumbing ---------------------------------------------------

    #[test]
    fn missing_and_unanswered_lanes_refuse_with_the_directions_vocabulary() {
        let worker_gone = super::missing_lane_refusal(&PassiveTarget::Worker("gpu-box".to_owned()));
        let refusal = refusal_in(&worker_gone).expect("a refusal");
        assert_eq!(
            refusal.reason,
            ForwardRefusalReason::WorkerUnreachable as i32
        );
        assert!(
            refusal.detail.contains("gpu-box"),
            "a fast refusal names the unreachable worker: {}",
            refusal.detail
        );

        let backer_gone = super::missing_lane_refusal(&PassiveTarget::Client("laptop".to_owned()));
        let refusal = refusal_in(&backer_gone).expect("a refusal");
        assert_eq!(refusal.reason, ForwardRefusalReason::Unbacked as i32);
        assert!(refusal.detail.contains("laptop"), "{}", refusal.detail);

        let slow_worker = super::unanswered_refusal(&PassiveTarget::Worker("w1".to_owned()));
        let refusal = refusal_in(&slow_worker).expect("a refusal");
        assert_eq!(
            refusal.reason,
            ForwardRefusalReason::WorkerUnreachable as i32
        );
        assert!(
            refusal.detail.contains("did not answer"),
            "{}",
            refusal.detail
        );

        let slow_backer = super::unanswered_refusal(&PassiveTarget::Client("laptop".to_owned()));
        let refusal = refusal_in(&slow_backer).expect("a refusal");
        assert_eq!(refusal.reason, ForwardRefusalReason::Unbacked as i32);
    }

    /// The handshake refuses a first frame that is not `open`, and a stream
    /// that says nothing at all — through the code's own API, not a timeout
    /// observation: the refusal frame is the assertion.
    #[tokio::test]
    async fn the_handshake_refuses_wrong_and_missing_openings() {
        let (mut io, feed, _out) = scripted_leg();
        feed.send(Ok(data_frame(vec![9]))).await.expect("send");
        let refused = super::handshake(&mut io)
            .await
            .expect_err("data before open must refuse");
        assert!(refusal_in(&refused).is_some());

        let (mut io, feed, _out) = scripted_leg();
        drop(feed);
        let refused = super::handshake(&mut io)
            .await
            .expect_err("an empty stream must refuse");
        assert!(refusal_in(&refused).is_some());

        let (mut io, feed, _out) = scripted_leg();
        feed.send(Ok(open_frame("fwd-1", "c1")))
            .await
            .expect("send");
        let open = super::handshake(&mut io)
            .await
            .expect("a proper open passes the handshake");
        assert_eq!(open.connection_id, "c1");
    }
}
