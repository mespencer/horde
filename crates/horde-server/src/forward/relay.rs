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

/// Runs one tunnel stream to completion: handshake, claim-or-originate,
/// splice. `wrap` lifts a bare frame into the service's response type.
pub async fn run_tunnel<S, T, W>(
    log: &EventLog,
    registry: &ForwardRegistry,
    caller: TunnelPrincipal,
    mut inbound: S,
    sender: &mpsc::Sender<Result<T, Status>>,
    wrap: W,
) where
    S: Stream<Item = Result<ForwardTunnelFrame, Status>> + Unpin + Send,
    W: Fn(ForwardTunnelFrame) -> T,
{
    let open = match handshake(&mut inbound).await {
        Ok(open) => open,
        Err(refusal) => {
            let _ = sender.send(Ok(wrap(refusal))).await;
            return;
        }
    };

    match registry.claim(&open.connection_id, &caller) {
        Claim::Matched(pair) => run_passive(pair, inbound, sender, wrap).await,
        Claim::WrongPrincipal => {
            let refusal = refusal_frame(
                ForwardRefusalReason::WrongPrincipal,
                format!(
                    "connection {} was dispatched to a different peer",
                    open.connection_id
                ),
            );
            let _ = sender.send(Ok(wrap(refusal))).await;
        }
        Claim::Unknown => run_originator(log, registry, caller, open, inbound, sender, wrap).await,
    }
}

/// Waits (ceilinged) for the opening frame.
async fn handshake<S>(inbound: &mut S) -> Result<ForwardTunnelOpen, ForwardTunnelFrame>
where
    S: Stream<Item = Result<ForwardTunnelFrame, Status>> + Unpin + Send,
{
    let protocol_refusal = || {
        refusal_frame(
            ForwardRefusalReason::Unspecified,
            "a tunnel's first frame must be `open`, within the handshake deadline",
        )
    };
    match tokio::time::timeout(HANDSHAKE_DEADLINE, inbound.next()).await {
        Ok(Some(Ok(ForwardTunnelFrame {
            frame: Some(Frame::Open(open)),
        }))) => Ok(open),
        _ => Err(protocol_refusal()),
    }
}

/// The passive side: complete the pairing, then splice.
async fn run_passive<S, T, W>(
    pair: tokio::sync::oneshot::Sender<PassiveLeg>,
    inbound: S,
    sender: &mpsc::Sender<Result<T, Status>>,
    wrap: W,
) where
    S: Stream<Item = Result<ForwardTunnelFrame, Status>> + Unpin + Send,
    W: Fn(ForwardTunnelFrame) -> T,
{
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
    splice(inbound, to_origin, from_origin, sender, wrap).await;
}

/// The originating side: validate, dispatch, wait for the pairing, splice.
async fn run_originator<S, T, W>(
    log: &EventLog,
    registry: &ForwardRegistry,
    caller: TunnelPrincipal,
    open: ForwardTunnelOpen,
    inbound: S,
    sender: &mpsc::Sender<Result<T, Status>>,
    wrap: W,
) where
    S: Stream<Item = Result<ForwardTunnelFrame, Status>> + Unpin + Send,
    W: Fn(ForwardTunnelFrame) -> T,
{
    let leg = match originate(log, registry, &caller, &open).await {
        Ok(leg) => leg,
        Err(refusal) => {
            let _ = sender.send(Ok(wrap(refusal))).await;
            return;
        }
    };
    if sender.send(Ok(wrap(accepted_frame()))).await.is_err() {
        return;
    }
    splice(inbound, leg.to_passive, leg.from_passive, sender, wrap).await;
}

/// Everything between "an open frame arrived" and "both legs are up".
async fn originate(
    log: &EventLog,
    registry: &ForwardRegistry,
    caller: &TunnelPrincipal,
    open: &ForwardTunnelOpen,
) -> Result<PassiveLeg, ForwardTunnelFrame> {
    let forward_id = open
        .forward_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let resolved = intent::resolve_live(log, &forward_id).await.map_err(|error| {
        refusal_frame(
            ForwardRefusalReason::Unspecified,
            format!("the relay could not read the log: {error}"),
        )
    })?;
    let target = originate_ruling(resolved.as_ref(), caller).map_err(|refusal| {
        ForwardTunnelFrame {
            frame: Some(Frame::Refused(refusal.into_refusal())),
        }
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

/// Moves frames both ways until both directions end.
///
/// Ends by structure: the inbound stream finishing (or failing) drops
/// `to_peer`, which the peer's splice observes as its channel closing; the
/// peer channel closing disables that arm. When both arms are disabled the
/// `else` branch returns. No clock — a tunnel lives exactly as long as its
/// connection.
async fn splice<S, T, W>(
    mut inbound: S,
    to_peer: mpsc::Sender<ForwardTunnelFrame>,
    mut from_peer: mpsc::Receiver<ForwardTunnelFrame>,
    sender: &mpsc::Sender<Result<T, Status>>,
    wrap: W,
) where
    S: Stream<Item = Result<ForwardTunnelFrame, Status>> + Unpin + Send,
    W: Fn(ForwardTunnelFrame) -> T,
{
    let mut to_peer = Some(to_peer);
    let mut peer_open = true;
    loop {
        tokio::select! {
            frame = inbound.next(), if to_peer.is_some() => {
                let delivered = match frame {
                    Some(Ok(frame)) => match &to_peer {
                        Some(lane) => lane.send(frame).await.is_ok(),
                        None => false,
                    },
                    _ => false,
                };
                if !delivered {
                    // Inbound ended, errored, or the peer is gone: dropping
                    // the sender is the half-close the peer observes.
                    to_peer = None;
                }
            }
            frame = from_peer.recv(), if peer_open => {
                match frame {
                    Some(frame) => {
                        if sender.send(Ok(wrap(frame))).await.is_err() {
                            return; // Our own peer hung up; nothing left to carry.
                        }
                    }
                    None => peer_open = false,
                }
            }
            else => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::forward::{data_frame, eof_frame, open_frame, refusal_in};
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
        /// What the splice sent back down its own gRPC stream.
        outbound: tokio::sync::mpsc::Receiver<Result<ForwardTunnelFrame, Status>>,
        task: tokio::task::JoinHandle<()>,
    }

    fn spliced() -> SpliceHarness {
        let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(8);
        let (to_peer_tx, to_peer_rx) = tokio::sync::mpsc::channel(8);
        let (from_peer_tx, from_peer_rx) = tokio::sync::mpsc::channel(8);
        let (outbound_tx, outbound_rx) = tokio::sync::mpsc::channel(8);
        let task = tokio::spawn(async move {
            splice(
                ReceiverStream::new(inbound_rx),
                to_peer_tx,
                from_peer_rx,
                &outbound_tx,
                |frame| frame,
            )
            .await;
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
        assert_eq!(echoed.expect("an ok frame"), data_frame(vec![3]));

        harness.task.abort();
    }

    /// Inbound ending must half-close toward the peer while the peer's
    /// frames keep flowing — and the splice must end once both do.
    #[tokio::test]
    async fn a_splice_half_closes_and_then_ends_by_structure() {
        let mut harness = spliced();

        drop(harness.inbound);
        let peer_saw_close =
            tokio::time::timeout(std::time::Duration::from_secs(5), harness.to_peer.recv())
                .await
                .expect("the peer channel must close within the ceiling");
        assert!(
            peer_saw_close.is_none(),
            "dropping inbound is the peer's half-close"
        );

        // The other direction still delivers.
        harness
            .from_peer
            .send(eof_frame())
            .await
            .expect("send from peer");
        let last = within("late peer frames still relay", &mut harness.outbound).await;
        assert_eq!(last.expect("an ok frame"), eof_frame());

        // And closing it ends the splice — by structure, not by clock.
        drop(harness.from_peer);
        tokio::time::timeout(std::time::Duration::from_secs(5), harness.task)
            .await
            .expect("the splice must end when both directions have")
            .expect("the splice task must not panic");
    }

    /// An inbound *error* (peer reset) tears down the peer direction the
    /// same way an end does.
    #[tokio::test]
    async fn an_inbound_error_half_closes_toward_the_peer() {
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
        harness.task.abort();
    }

    // -- refusal plumbing ---------------------------------------------------

    #[test]
    fn missing_and_unanswered_lanes_refuse_with_the_directions_vocabulary() {
        let worker_gone =
            super::missing_lane_refusal(&PassiveTarget::Worker("gpu-box".to_owned()));
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
        assert!(refusal.detail.contains("did not answer"), "{}", refusal.detail);

        let slow_backer = super::unanswered_refusal(&PassiveTarget::Client("laptop".to_owned()));
        let refusal = refusal_in(&slow_backer).expect("a refusal");
        assert_eq!(refusal.reason, ForwardRefusalReason::Unbacked as i32);
    }

    /// The handshake refuses a first frame that is not `open`, and a stream
    /// that says nothing at all — through the code's own API, not a timeout
    /// observation: the refusal frame is the assertion.
    #[tokio::test]
    async fn the_handshake_refuses_wrong_and_missing_openings() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ForwardTunnelFrame, Status>>(4);
        tx.send(Ok(data_frame(vec![9]))).await.expect("send");
        let mut stream = ReceiverStream::new(rx);
        let refused = super::handshake(&mut stream)
            .await
            .expect_err("data before open must refuse");
        assert!(refusal_in(&refused).is_some());

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ForwardTunnelFrame, Status>>(4);
        drop(tx);
        let mut stream = ReceiverStream::new(rx);
        let refused = super::handshake(&mut stream)
            .await
            .expect_err("an empty stream must refuse");
        assert!(refusal_in(&refused).is_some());

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ForwardTunnelFrame, Status>>(4);
        tx.send(Ok(open_frame("fwd-1", "c1"))).await.expect("send");
        let mut stream = ReceiverStream::new(rx);
        let open = super::handshake(&mut stream)
            .await
            .expect("a proper open passes the handshake");
        assert_eq!(open.connection_id, "c1");
    }
}
