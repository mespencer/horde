//! Client-side forward realization (ADR-0006), and the TCP↔frame pump every
//! tunnel peer shares.
//!
//! The durable intent lives in the log; everything in this module is the
//! ephemeral half — bound listeners, dialed sockets, and the frame pumps
//! carrying one TCP connection's bytes over a relay leg. Nothing here
//! survives this process, deliberately: reattach re-realizes every intent in
//! scope, and a listener that outlived its connection would be the lie the
//! ADR's state model forbids.
//!
//! The pump and the tunnel shape are used by the Worker's realization too
//! (its legs differ only in the gRPC wrapper types), which is why they live
//! in the client *library* rather than under the CLI commands.

use std::time::Duration;

use horde_proto::forward::{data_frame, eof_frame, open_frame, refusal_frame};
use horde_proto::v1::{
    ForwardRefusalReason, ForwardTunnelFrame, ForwardTunnelRefusal, forward_tunnel_frame::Frame,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;

/// How long an originator waits for the relay to answer `open` before the
/// local connection is refused. Covers the relay's own handshake and pairing
/// deadlines (10s each) with room for the passive side's dial.
pub const CONFIRM_DEADLINE: Duration = Duration::from_secs(30);

/// How long a passive side gives its local target to accept a dial.
pub const DIAL_DEADLINE: Duration = Duration::from_secs(5);

/// Read size of the TCP→frame direction. One frame per read keeps frames
/// bounded and lets HTTP/2 flow control meter the whole path.
const READ_CHUNK: usize = 16 * 1024;

/// One open tunnel leg: frames in, frames out. The gRPC wrappers are already
/// stripped, so the Worker's leg and the Client's leg meet the pump as the
/// same type.
pub struct Tunnel {
    /// Frames toward the relay.
    pub to_relay: tokio::sync::mpsc::Sender<ForwardTunnelFrame>,
    /// Frames from the relay.
    pub from_relay: std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ForwardTunnelFrame, tonic::Status>> + Send>,
    >,
}

impl std::fmt::Debug for Tunnel {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Tunnel").finish_non_exhaustive()
    }
}

impl Tunnel {
    /// Sends the opening frame that names this connection.
    ///
    /// # Errors
    ///
    /// [`TunnelGone`] when the relay leg is already closed.
    pub async fn open(&mut self, forward_id: &str, connection_id: &str) -> Result<(), TunnelGone> {
        self.to_relay
            .send(open_frame(forward_id, connection_id))
            .await
            .map_err(|_| TunnelGone)
    }

    /// Sends a terminal refusal — the passive side's "I could not carry
    /// this" — and finishes the leg. Consumes the tunnel on purpose: a
    /// refusal is the last word, and the finish is what makes it audible.
    pub async fn refuse(self, reason: ForwardRefusalReason, detail: String) {
        let _ = self.to_relay.send(refusal_frame(reason, detail)).await;
        self.finish().await;
    }

    /// Ends this leg gracefully: the send half closes cleanly — which is
    /// what actually transmits any still-queued frames, where dropping the
    /// whole tunnel cancels the RPC and races them — and the receive half
    /// is drained (count-bounded, ceilinged) until the relay closes it.
    ///
    /// Found the hard way: a passive answer that only refused and returned
    /// dropped its tunnel before the open and refusal frames left the
    /// process, so the originator saw a pairing deadline instead of the
    /// coded reason.
    pub async fn finish(self) {
        drop(self.to_relay);
        // Bounded by structure — `take` owns the count — with the ceiling
        // for a relay that stops sending without closing.
        let mut trailing = self.from_relay.take(FINISH_FRAME_BOUND);
        let _ = tokio::time::timeout(FINISH_DEADLINE, async {
            while trailing.next().await.is_some() {}
        })
        .await;
    }
}

/// How long a finishing leg waits for the relay to close it.
const FINISH_DEADLINE: Duration = Duration::from_secs(5);

/// How many trailing frames a finishing leg drains before giving up on a
/// babbling relay. A count bound, so a clock is never asked to end a loop.
const FINISH_FRAME_BOUND: usize = 64;

/// The relay leg closed under a send.
#[derive(Debug, thiserror::Error)]
#[error("the tunnel's relay leg is closed")]
pub struct TunnelGone;

/// Why an originated connection did not reach the pumping stage.
#[derive(Debug, thiserror::Error)]
pub enum ConfirmEnd {
    /// The relay refused, with the reason a caller can branch on.
    #[error("refused: {}", .0.detail)]
    Refused(ForwardTunnelRefusal),

    /// The relay neither accepted nor refused within the deadline, or the
    /// stream ended. A fast local refusal follows either way.
    #[error("the relay did not answer within {CONFIRM_DEADLINE:?}")]
    NoAnswer,
}

/// How many non-answer frames the confirmation wait tolerates before giving
/// up. Nothing legitimate precedes the answer, so this is a count bound on a
/// misbehaving peer, not a budget the happy path spends.
const CONFIRM_FRAME_BOUND: u32 = 16;

/// Waits (ceilinged) for the relay to accept or refuse an originated
/// connection.
///
/// # Errors
///
/// [`ConfirmEnd`] — and the caller's job on either variant is the same: close
/// the local socket fast, because a fast explained refusal beats a hang.
pub async fn confirm_opened(tunnel: &mut Tunnel) -> Result<(), ConfirmEnd> {
    let answer = tokio::time::timeout(CONFIRM_DEADLINE, async {
        let mut seen = 0_u32;
        while seen < CONFIRM_FRAME_BOUND {
            match tunnel.from_relay.next().await {
                Some(Ok(ForwardTunnelFrame {
                    frame: Some(Frame::Accepted(_)),
                })) => return Ok(()),
                Some(Ok(ForwardTunnelFrame {
                    frame: Some(Frame::Refused(refusal)),
                })) => return Err(ConfirmEnd::Refused(refusal)),
                Some(Ok(_)) => seen += 1,
                Some(Err(_)) | None => return Err(ConfirmEnd::NoAnswer),
            }
        }
        Err(ConfirmEnd::NoAnswer)
    })
    .await;
    match answer {
        Ok(outcome) => outcome,
        Err(_elapsed) => Err(ConfirmEnd::NoAnswer),
    }
}

/// How one pumped connection ended.
#[derive(Debug)]
pub enum PumpEnd {
    /// Both directions delivered their EOFs.
    Clean,
    /// The relay refused mid-stream; the local socket was torn down.
    Refused(ForwardTunnelRefusal),
    /// The relay leg ended before the connection finished — a partition or a
    /// peer teardown. In-flight bytes are gone; the intent is not.
    RelayEnded,
    /// The local socket failed.
    Io(std::io::Error),
}

/// Carries one TCP connection over one tunnel leg until either side finishes.
///
/// Bounded by structure, not clock: every iteration either moves bytes or
/// retires a direction, and when both directions have delivered their EOFs
/// the pump returns [`PumpEnd::Clean`]. A long-lived quiet tunnel is a
/// feature, not a leak — its lifetime is the connection's.
pub async fn pump_tcp(tcp: TcpStream, mut tunnel: Tunnel) -> PumpEnd {
    let (mut local_read, mut local_write) = tcp.into_split();
    let mut buffer = vec![0_u8; READ_CHUNK];
    let mut local_eof = false;
    let mut relay_eof = false;

    loop {
        tokio::select! {
            read = local_read.read(&mut buffer), if !local_eof => {
                match read {
                    Ok(0) => {
                        local_eof = true;
                        if tunnel.to_relay.send(eof_frame()).await.is_err() {
                            return PumpEnd::RelayEnded;
                        }
                    }
                    Ok(count) => {
                        if tunnel
                            .to_relay
                            .send(data_frame(buffer[..count].to_vec()))
                            .await
                            .is_err()
                        {
                            return PumpEnd::RelayEnded;
                        }
                    }
                    Err(error) => return PumpEnd::Io(error),
                }
            }
            frame = tunnel.from_relay.next(), if !relay_eof => {
                match frame {
                    Some(Ok(ForwardTunnelFrame { frame: Some(Frame::Data(bytes)) })) => {
                        if let Err(error) = local_write.write_all(&bytes).await {
                            return PumpEnd::Io(error);
                        }
                    }
                    Some(Ok(ForwardTunnelFrame { frame: Some(Frame::Eof(_)) })) => {
                        relay_eof = true;
                        // Half-close toward the local peer; reads continue.
                        let _ = local_write.shutdown().await;
                    }
                    Some(Ok(ForwardTunnelFrame { frame: Some(Frame::Refused(refusal)) })) => {
                        return PumpEnd::Refused(refusal);
                    }
                    Some(Ok(_)) => {}
                    Some(Err(_)) | None => return PumpEnd::RelayEnded,
                }
            }
        }
        if pump_is_done(local_eof, relay_eof) {
            return PumpEnd::Clean;
        }
    }
}

/// Whether a pump has carried everything it will ever carry.
///
/// Both EOFs, not either: TCP half-close is real — a peer that finished
/// writing still reads — and `||` here would cut off every response to a
/// request the client half-closed after sending, which is exactly how
/// `curl --http1.0` style exchanges end.
fn pump_is_done(local_eof: bool, relay_eof: bool) -> bool {
    local_eof && relay_eof
}

/// A bound listener realizing one expose intent.
#[derive(Debug)]
pub struct BoundListener {
    pub listener: TcpListener,
    /// The port the intent asked for could not be bound, so this is an
    /// ephemeral one — print the mapping.
    pub fell_back: bool,
}

/// The local port an expose asks a realizing client to try first: the spec's
/// client port, or the worker port when the spec left the choice open.
#[must_use]
pub fn requested_local_port(worker_port: u32, client_port: u32) -> u16 {
    let wanted = if client_port == 0 {
        worker_port
    } else {
        client_port
    };
    u16::try_from(wanted).unwrap_or(0)
}

/// Binds `127.0.0.1:port`, falling back to an ephemeral port when the
/// requested one is taken — which also makes two horde processes on one
/// machine merely redundant, never wrong (ADR-0006).
///
/// # Errors
///
/// The bind error when even `:0` cannot be bound.
pub async fn bind_with_fallback(port: u16) -> std::io::Result<BoundListener> {
    match TcpListener::bind(("127.0.0.1", port)).await {
        Ok(listener) => Ok(BoundListener {
            listener,
            fell_back: false,
        }),
        Err(_taken) => Ok(BoundListener {
            listener: TcpListener::bind(("127.0.0.1", 0)).await?,
            fell_back: port != 0,
        }),
    }
}

/// A connection id no other originator of this process will mint again.
#[must_use]
pub fn connection_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static ORDINAL: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    format!(
        "conn-{nanos:x}-{:x}",
        ORDINAL.fetch_add(1, Ordering::Relaxed)
    )
}

/// How many consecutive accept failures end a listener's loop. A count
/// bound, because a clock cannot bound a loop that fails without waiting.
pub const ACCEPT_FAILURE_BOUND: u32 = 32;

/// Whether an accept loop is still within its failure budget. Named — and
/// shared with the worker's inject listener — because the boundary is
/// unreachable in any test that cannot stage thirty-two consecutive accept
/// failures, and off-by-one here is the difference between a bounded loop
/// and a spin.
#[must_use]
pub fn keeps_accepting(consecutive_failures: u32) -> bool {
    consecutive_failures < ACCEPT_FAILURE_BOUND
}

/// One realized expose: a bound local listener whose every accepted
/// connection becomes a tunnel. Stopping it (or dropping it) closes the
/// listener and aborts every in-flight connection — realization is tied to
/// this process's liveness and nothing else's.
#[derive(Debug)]
pub struct RealizedExpose {
    pub forward_id: String,
    pub name: String,
    pub local_addr: std::net::SocketAddr,
    /// The intent's requested port could not be bound; `local_addr` is the
    /// ephemeral fallback, and the caller prints the mapping.
    pub fell_back: bool,
    /// `Some` until stopped; `Drop` aborts what `stop` was not called on.
    accept_task: Option<tokio::task::JoinHandle<()>>,
}

impl RealizedExpose {
    /// Tears the realization down and waits for the accept loop to be
    /// provably gone — the owned witness is the task's own join.
    pub async fn stop(mut self) {
        if let Some(task) = self.accept_task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for RealizedExpose {
    fn drop(&mut self) {
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}

/// Realizes one expose intent: binds the requested local port (ephemeral on
/// fallback) and serves it until stopped.
///
/// # Errors
///
/// The bind error when not even an ephemeral port can be bound.
pub async fn realize_expose(
    client: &crate::client::Client,
    forward_id: &str,
    name: &str,
    worker_port: u32,
    client_port: u32,
) -> std::io::Result<RealizedExpose> {
    let bound = bind_with_fallback(requested_local_port(worker_port, client_port)).await?;
    let local_addr = bound.listener.local_addr()?;
    let accept_task = tokio::spawn(accept_loop(
        bound.listener,
        client.clone(),
        forward_id.to_owned(),
    ));
    Ok(RealizedExpose {
        forward_id: forward_id.to_owned(),
        name: name.to_owned(),
        local_addr,
        fell_back: bound.fell_back,
        accept_task: Some(accept_task),
    })
}

/// Accepts until stopped or persistently failing; one task per connection,
/// all of them dying with this loop (the `JoinSet` aborts on drop).
async fn accept_loop(listener: TcpListener, client: crate::client::Client, forward_id: String) {
    let mut connections = tokio::task::JoinSet::new();
    let mut consecutive_failures = 0_u32;
    while keeps_accepting(consecutive_failures) {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((tcp, _peer)) => {
                    consecutive_failures = 0;
                    connections.spawn(carry_expose_connection(
                        client.clone(),
                        forward_id.clone(),
                        tcp,
                    ));
                }
                Err(_) => consecutive_failures += 1,
            },
            // Reaped as they finish, so a long-lived listener does not
            // accumulate one handle per connection it ever carried.
            Some(_) = connections.join_next() => {}
        }
    }
}

/// One accepted local connection, originated through the relay.
///
/// A refusal — worker unreachable, unknown forward, no answer — closes the
/// socket immediately with nothing written: the fast refusal the ADR
/// promises instead of a hang.
async fn carry_expose_connection(
    mut client: crate::client::Client,
    forward_id: String,
    tcp: TcpStream,
) {
    let Ok(mut tunnel) = client.open_forward_tunnel().await else {
        return; // Server unreachable: dropping the socket is the refusal.
    };
    let connection = connection_id();
    if tunnel.open(&forward_id, &connection).await.is_err() {
        return;
    }
    match confirm_opened(&mut tunnel).await {
        Ok(()) => {
            let _ = pump_tcp(tcp, tunnel).await;
        }
        Err(refusal) => {
            tracing::info!(forward_id, %refusal, "local connection refused fast");
            // Dropped without a byte written — the peer sees a reset, not a
            // hang.
            drop(tcp);
        }
    }
}

/// One inject this client backs: where its traffic lands locally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectTarget {
    pub name: String,
    pub client_port: u16,
}

/// The targets a backing stream serves, shared with whoever maintains them.
pub type InjectTargets =
    std::sync::Arc<std::sync::RwLock<std::collections::BTreeMap<String, InjectTarget>>>;

/// Backs this client's injects until the serve stream ends.
///
/// Every pushed connection request is answered — with a dialed target and a
/// pump, or with a coded refusal — on its own task; the tasks die with this
/// call. Returns when the server ends the stream (or the connection drops),
/// which is also the moment the server counts this client as unbacked.
///
/// # Errors
///
/// The RPC failure that opened or ended the stream.
pub async fn serve_backing(
    client: &crate::client::Client,
    targets: InjectTargets,
) -> Result<(), crate::client::ClientError> {
    let mut dialer = client.clone();
    let mut stream = dialer.serve_forwards().await?;
    let mut answers = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            pushed = stream.message() => {
                let Some(update) = pushed? else {
                    return Ok(());
                };
                let Some(request) = update.connection else { continue };
                answers.spawn(answer_inject_connection(
                    client.clone(),
                    targets.clone(),
                    request,
                ));
            }
            Some(_) = answers.join_next() => {}
        }
    }
}

/// Answers one worker-side connection to an inject this client backs.
async fn answer_inject_connection(
    mut client: crate::client::Client,
    targets: InjectTargets,
    request: horde_proto::v1::ForwardConnectionRequest,
) {
    let forward_id = request
        .forward_id
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    let Ok(mut tunnel) = client.open_forward_tunnel().await else {
        return;
    };
    if tunnel
        .open(&forward_id, &request.connection_id)
        .await
        .is_err()
    {
        return;
    }

    let target = lookup_target(&targets, &forward_id);
    let Some(target) = target else {
        tunnel
            .refuse(
                ForwardRefusalReason::UnknownForward,
                format!("this client is not serving {forward_id}"),
            )
            .await;
        return;
    };
    dial_and_pump(tunnel, target.client_port).await;
}

/// The passive side's shared tail: dial the local target and pump, or
/// refuse with the coded reason. The worker's expose dial and the backer's
/// inject dial are this same three-way outcome, which is why it lives once,
/// here, where both peers already meet the [`Tunnel`].
pub async fn dial_and_pump(tunnel: Tunnel, port: u16) {
    let dialed = tokio::time::timeout(DIAL_DEADLINE, TcpStream::connect(("127.0.0.1", port))).await;
    match dialed {
        Ok(Ok(tcp)) => {
            let _ = pump_tcp(tcp, tunnel).await;
        }
        Ok(Err(error)) => {
            tunnel
                .refuse(
                    ForwardRefusalReason::TargetDialFailed,
                    format!("dialing 127.0.0.1:{port} failed: {error}"),
                )
                .await;
        }
        Err(_elapsed) => {
            tunnel
                .refuse(
                    ForwardRefusalReason::TargetDialFailed,
                    format!("127.0.0.1:{port} did not accept within {DIAL_DEADLINE:?}"),
                )
                .await;
        }
    }
}

/// One expose `forward serve` will realize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedExpose {
    pub forward_id: String,
    pub name: String,
    pub worker_port: u32,
    pub client_port: u32,
}

/// What one `forward serve` invocation realizes: every live expose (each
/// client realizes exposes independently — no coordination, no conflict),
/// and exactly the injects this principal backs.
#[derive(Debug, Default)]
pub struct ServePlan {
    pub exposes: Vec<PlannedExpose>,
    pub injects: InjectTargets,
}

impl ServePlan {
    /// Plans from a `ListForwards` answer and this invocation's principal.
    #[must_use]
    pub fn from_statuses(
        statuses: &[horde_proto::v1::ForwardStatus],
        principal: Option<&str>,
    ) -> Self {
        let mut plan = Self::default();
        for status in statuses {
            let Some(intent) = status.intent.as_ref() else {
                continue;
            };
            let Some(spec) = intent.spec.as_ref() else {
                continue;
            };
            let forward_id = intent
                .forward_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            match horde_proto::v1::ForwardDirection::try_from(spec.direction) {
                Ok(horde_proto::v1::ForwardDirection::Expose) => {
                    plan.exposes.push(PlannedExpose {
                        forward_id,
                        name: spec.name.clone(),
                        worker_port: spec.worker_port,
                        client_port: spec.client_port,
                    });
                }
                Ok(horde_proto::v1::ForwardDirection::Inject) => {
                    let backer = intent.backer_client_id.as_ref().map(ToString::to_string);
                    if backs_here(backer.as_deref(), principal)
                        && let Ok(port) = u16::try_from(spec.client_port)
                        && let Ok(mut map) = plan.injects.write()
                    {
                        map.insert(
                            forward_id,
                            InjectTarget {
                                name: spec.name.clone(),
                                client_port: port,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
        plan
    }
}

/// Whether an inject's recorded backer is this invocation's principal.
///
/// Both must exist and match: an unidentified invocation backs nothing (it
/// has no principal the server could route to), and an inject with no
/// recorded backer is nobody's to serve. The mixed rows are unreachable
/// end-to-end — the server refuses to journal backerless injects — which is
/// exactly why the predicate is named.
#[must_use]
pub fn backs_here(backer: Option<&str>, principal: Option<&str>) -> bool {
    match (backer, principal) {
        (Some(backer), Some(principal)) => backer == principal,
        _ => false,
    }
}

/// The target an id maps to right now, read without holding the lock across
/// an await.
fn lookup_target(targets: &InjectTargets, forward_id: &str) -> Option<InjectTarget> {
    match targets.read() {
        Ok(map) => map.get(forward_id).cloned(),
        // A poisoned map still answers: the data is plain values, and
        // refusing every inject over a panic elsewhere would turn one bug
        // into an outage.
        Err(poisoned) => poisoned.into_inner().get(forward_id).cloned(),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::forward::{accepted_frame, data_frame, eof_frame, refusal_frame};
    use horde_proto::v1::ForwardRefusalReason;
    use tokio_stream::{StreamExt as _, wrappers::ReceiverStream};

    use super::{
        ConfirmEnd, PumpEnd, Tunnel, bind_with_fallback, confirm_opened, connection_id,
        pump_is_done, pump_tcp, requested_local_port,
    };

    /// A tunnel whose relay side the test plays: feed frames through the
    /// returned sender, observe what the pump sent through the receiver.
    fn scripted_tunnel() -> (
        Tunnel,
        tokio::sync::mpsc::Sender<Result<horde_proto::v1::ForwardTunnelFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<horde_proto::v1::ForwardTunnelFrame>,
    ) {
        let (to_relay, sent) = tokio::sync::mpsc::channel(16);
        let (feed, from_relay) = tokio::sync::mpsc::channel(16);
        let tunnel = Tunnel {
            to_relay,
            from_relay: Box::pin(ReceiverStream::new(from_relay)),
        };
        (tunnel, feed, sent)
    }

    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| panic!("{what}: nothing within the ceiling"))
    }

    #[test]
    fn the_requested_port_prefers_the_specs_client_port() {
        assert_eq!(
            requested_local_port(5173, 0),
            5173,
            "zero means same-as-worker"
        );
        assert_eq!(requested_local_port(5173, 8080), 8080);
        assert_eq!(
            requested_local_port(70_000, 0),
            0,
            "a port that fits no u16 asks for ephemeral rather than truncating"
        );
    }

    #[test]
    fn both_eofs_end_a_pump_and_neither_alone_does() {
        assert!(pump_is_done(true, true));
        assert!(
            !pump_is_done(true, false),
            "the response may still be coming"
        );
        assert!(!pump_is_done(false, true), "the request may still be going");
        assert!(!pump_is_done(false, false));
    }

    #[test]
    fn connection_ids_do_not_repeat() {
        let a = connection_id();
        let b = connection_id();
        assert_ne!(a, b);
        assert!(a.starts_with("conn-"));
    }

    /// The chunk size is contract, not tuning: it is the frame ceiling the
    /// relay's flow control meters, so the literal is pinned rather than
    /// recomputed.
    #[test]
    fn the_read_chunk_is_sixteen_kib() {
        assert_eq!(super::READ_CHUNK, 16_384);
    }

    /// The failure budget's boundary, which no test can stage through a real
    /// listener: within the budget accepts continue, at it the loop ends.
    #[test]
    fn the_accept_budget_ends_exactly_at_its_bound() {
        use super::{ACCEPT_FAILURE_BOUND, keeps_accepting};
        assert!(keeps_accepting(0));
        assert!(keeps_accepting(ACCEPT_FAILURE_BOUND - 1));
        assert!(!keeps_accepting(ACCEPT_FAILURE_BOUND));
        assert!(!keeps_accepting(ACCEPT_FAILURE_BOUND + 1));
    }

    /// A tunnel speaks exactly the frames its methods name — and reports the
    /// leg's death rather than pretending the open landed.
    #[tokio::test]
    async fn a_tunnel_says_open_and_refusals_and_notices_a_dead_leg() {
        let (mut tunnel, _feed, mut sent) = scripted_tunnel();
        assert!(
            format!("{tunnel:?}").contains("Tunnel"),
            "the debug view names the type"
        );
        tunnel.open("fwd-1", "c1").await.expect("the leg is open");
        assert_eq!(
            within("the open frame", sent.recv())
                .await
                .expect("a frame"),
            horde_proto::forward::open_frame("fwd-1", "c1")
        );

        // The refusal is the last word: sent, and the leg finished — which
        // the test observes as the sender closing after the frame.
        let (refusing, feed, mut refused_sent) = scripted_tunnel();
        drop(feed); // The relay's half is already gone; finish must not hang.
        refusing
            .refuse(ForwardRefusalReason::Unbacked, "backer gone".to_owned())
            .await;
        assert_eq!(
            within("the refusal frame", refused_sent.recv())
                .await
                .expect("a frame"),
            refusal_frame(ForwardRefusalReason::Unbacked, "backer gone")
        );
        assert!(
            within("the leg finishes after refusing", refused_sent.recv())
                .await
                .is_none(),
            "the refusal consumes the tunnel; nothing else may follow"
        );

        drop(sent);
        assert!(
            tunnel.open("fwd-1", "c2").await.is_err(),
            "a dead leg is TunnelGone, never a silent success"
        );
    }

    /// The confirmation's count bound, frame-exact: sixteen non-answers are
    /// tolerated and the seventeenth is left unread. A widened bound or a
    /// broken counter consumes it — or never returns at all.
    #[tokio::test]
    async fn confirmation_reads_exactly_its_frame_budget() {
        let (mut tunnel, feed, _sent) = scripted_tunnel();
        // Fed concurrently: the scripted channel is shallower than the
        // budget plus one, so a sequential feed would deadlock against the
        // consumer it is arranging.
        let feeding = tokio::spawn(async move {
            for ordinal in 0..=u8::try_from(super::CONFIRM_FRAME_BOUND).expect("small bound") {
                if feed.send(Ok(data_frame(vec![ordinal]))).await.is_err() {
                    return;
                }
            }
            // Held open so the leftover frame stays readable below.
            std::future::pending::<()>().await;
        });

        let gave_up = within("the bound gives up", confirm_opened(&mut tunnel))
            .await
            .expect_err("babble is not an answer");
        assert!(matches!(gave_up, ConfirmEnd::NoAnswer));

        let leftover = within("the frame past the budget", tunnel.from_relay.next())
            .await
            .expect("a frame")
            .expect("ok");
        assert_eq!(
            leftover,
            data_frame(vec![
                u8::try_from(super::CONFIRM_FRAME_BOUND).expect("small bound")
            ]),
            "exactly the budget was consumed, not one frame more"
        );
        feeding.abort();
    }

    /// All four corners: only a named backer matching a named principal
    /// backs here. The mixed rows are what an `||` or a `!=` typo would get
    /// wrong, and the server's refusal of backerless injects makes them
    /// unreachable end-to-end.
    #[test]
    fn an_inject_is_served_only_by_its_own_principal() {
        use super::backs_here;
        assert!(backs_here(Some("laptop"), Some("laptop")));
        assert!(!backs_here(Some("laptop"), Some("desk")));
        assert!(!backs_here(Some("laptop"), None));
        assert!(!backs_here(None, Some("laptop")));
        assert!(!backs_here(None, None));
    }

    /// The serve plan splits by direction and keeps only this principal's
    /// injects — every client realizes every expose, no coordination.
    #[test]
    fn a_serve_plan_realizes_exposes_and_backs_only_its_own_injects() {
        use horde_proto::v1::{
            ClientId, ForwardDirection, ForwardId, ForwardIntent, ForwardSpec, ForwardStatus,
        };
        let status =
            |name: &str, direction: ForwardDirection, backer: Option<&str>| ForwardStatus {
                intent: Some(ForwardIntent {
                    forward_id: Some(ForwardId::new(format!("fwd-{name}"))),
                    session_id: None,
                    agent_id: None,
                    spec: Some(ForwardSpec {
                        direction: direction as i32,
                        name: name.to_owned(),
                        worker_port: 5173,
                        client_port: 5432,
                    }),
                    backer_client_id: backer.map(ClientId::new),
                }),
                worker_reachable: true,
                backed: false,
            };

        let plan = super::ServePlan::from_statuses(
            &[
                status("vite", ForwardDirection::Expose, None),
                status("db", ForwardDirection::Inject, Some("laptop")),
                status("other", ForwardDirection::Inject, Some("desk")),
            ],
            Some("laptop"),
        );

        assert_eq!(plan.exposes.len(), 1);
        assert_eq!(plan.exposes[0].name, "vite");
        let injects = plan.injects.read().expect("readable");
        assert_eq!(injects.len(), 1, "only this principal's inject is backed");
        assert_eq!(injects["fwd-db"].client_port, 5432);
    }

    #[tokio::test]
    async fn a_taken_port_falls_back_to_ephemeral_and_says_so() {
        // Bind our own listener on :0 — the port we then contest is one this
        // test owns, never an assertion about anyone else's.
        let occupant = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind the occupant");
        let taken = occupant.local_addr().expect("addr").port();

        let bound = bind_with_fallback(taken).await.expect("fallback binds");
        assert!(bound.fell_back, "the requested port was taken");
        let fallback_port = bound.listener.local_addr().expect("addr").port();
        assert_ne!(fallback_port, taken);

        // And a free request is honored without claiming a fallback: the
        // occupant's port becomes free once dropped, but rather than reuse
        // it (a race), ask for :0 semantics through the fallback flag.
        let fresh = bind_with_fallback(0).await.expect("ephemeral binds");
        assert!(
            !fresh.fell_back,
            "an ephemeral request got exactly what it asked for"
        );
    }

    #[tokio::test]
    async fn confirmation_accepts_refuses_and_gives_up_by_count() {
        let (mut tunnel, feed, _sent) = scripted_tunnel();
        feed.send(Ok(accepted_frame())).await.expect("feed");
        within("accept", confirm_opened(&mut tunnel))
            .await
            .expect("accepted confirms");

        let (mut tunnel, feed, _sent) = scripted_tunnel();
        feed.send(Ok(refusal_frame(
            ForwardRefusalReason::WorkerUnreachable,
            "worker w1 has no live forward stream",
        )))
        .await
        .expect("feed");
        let refused = within("refuse", confirm_opened(&mut tunnel))
            .await
            .expect_err("a refusal is an error");
        match refused {
            ConfirmEnd::Refused(refusal) => {
                assert_eq!(
                    refusal.reason,
                    ForwardRefusalReason::WorkerUnreachable as i32
                );
            }
            other => panic!("expected Refused, got {other:?}"),
        }

        // A closed stream is no answer.
        let (mut tunnel, feed, _sent) = scripted_tunnel();
        drop(feed);
        let ended = within("ended", confirm_opened(&mut tunnel))
            .await
            .expect_err("a closed stream cannot confirm");
        assert!(matches!(ended, ConfirmEnd::NoAnswer));

        // A peer babbling non-answers exhausts the count bound, not a clock.
        let (mut tunnel, feed, _sent) = scripted_tunnel();
        let flood = tokio::spawn(async move {
            // One more than the bound; sends stop failing only if the
            // confirmation stopped reading, which is the point.
            for _ in 0..=super::CONFIRM_FRAME_BOUND {
                if feed.send(Ok(data_frame(vec![0]))).await.is_err() {
                    return;
                }
            }
        });
        let babbled = within("babble", confirm_opened(&mut tunnel))
            .await
            .expect_err("data is not an answer");
        assert!(matches!(babbled, ConfirmEnd::NoAnswer));
        flood.abort();
    }

    /// A pump over a real connected pair, with the test holding the local
    /// peer socket and the scripted relay side — the arrangement all three
    /// pump tests share.
    async fn pumped_pair() -> (
        tokio::net::TcpStream,
        tokio::sync::mpsc::Sender<Result<horde_proto::v1::ForwardTunnelFrame, tonic::Status>>,
        tokio::sync::mpsc::Receiver<horde_proto::v1::ForwardTunnelFrame>,
        tokio::task::JoinHandle<PumpEnd>,
    ) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let dialed = tokio::net::TcpStream::connect(addr);
        let (accepted, dialed) = tokio::join!(listener.accept(), dialed);
        let (accepted, _) = accepted.expect("accept");
        let local = dialed.expect("dial");

        let (tunnel, feed, sent) = scripted_tunnel();
        let pump = tokio::spawn(pump_tcp(accepted, tunnel));
        (local, feed, sent, pump)
    }

    /// The pump, end to end over a real socket pair: bytes both ways, then
    /// EOFs both ways, ending clean.
    #[tokio::test]
    async fn a_pump_carries_bytes_both_ways_and_ends_on_both_eofs() {
        let (mut local, feed, mut sent, pump) = pumped_pair().await;

        // Local peer → relay.
        use tokio::io::AsyncWriteExt;
        local.write_all(b"ping").await.expect("write");
        let frame = within("tcp bytes become a data frame", sent.recv())
            .await
            .expect("a frame");
        assert_eq!(frame, data_frame(b"ping".to_vec()));

        // Relay → local peer.
        feed.send(Ok(data_frame(b"pong".to_vec())))
            .await
            .expect("feed");
        let mut buffer = [0_u8; 4];
        use tokio::io::AsyncReadExt;
        within(
            "relay bytes reach the socket",
            local.read_exact(&mut buffer),
        )
        .await
        .expect("read");
        assert_eq!(&buffer, b"pong");

        // Local half-close becomes an eof frame; relay eof half-closes back.
        local.shutdown().await.expect("shutdown");
        let frame = within("local eof becomes a frame", sent.recv())
            .await
            .expect("a frame");
        assert_eq!(frame, eof_frame());
        feed.send(Ok(eof_frame())).await.expect("feed");

        let end = within("both eofs end the pump", pump)
            .await
            .expect("the pump task must not panic");
        assert!(matches!(end, PumpEnd::Clean), "got {end:?}");

        // And the local socket saw the far EOF.
        let n = within("eof reaches the socket", local.read(&mut buffer))
            .await
            .expect("read");
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn a_mid_stream_refusal_tears_the_pump_down() {
        let (_local, feed, _sent, pump) = pumped_pair().await;
        feed.send(Ok(refusal_frame(ForwardRefusalReason::Unbacked, "gone")))
            .await
            .expect("feed");

        let end = within("refusal ends the pump", pump)
            .await
            .expect("no panic");
        assert!(matches!(end, PumpEnd::Refused(_)), "got {end:?}");
    }

    /// A relay leg dying mid-connection resets the pump — the in-flight
    /// connection is gone, which is the partition contract; the intent
    /// outlives it elsewhere.
    #[tokio::test]
    async fn a_dead_relay_leg_ends_the_pump_as_relay_ended() {
        let (_local, feed, _sent, pump) = pumped_pair().await;
        drop(feed);

        let end = within("a dead leg ends the pump", pump)
            .await
            .expect("no panic");
        assert!(matches!(end, PumpEnd::RelayEnded), "got {end:?}");
    }

    #[test]
    fn lookup_answers_from_the_map_and_only_the_map() {
        let targets: super::InjectTargets = std::sync::Arc::default();
        targets.write().expect("writable").insert(
            "fwd-db".to_owned(),
            super::InjectTarget {
                name: "db".to_owned(),
                client_port: 5432,
            },
        );
        assert_eq!(
            super::lookup_target(&targets, "fwd-db").map(|target| target.client_port),
            Some(5432)
        );
        assert_eq!(super::lookup_target(&targets, "fwd-none"), None);
    }
}

#[cfg(test)]
mod engine_tests {
    //! The realization engines against a real `horde-server` on loopback —
    //! in this package, because a kill in another crate's suite does not
    //! count for this one's mutants.

    use horde_proto::v1::{ForwardDirection, ForwardSpec};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{InjectTarget, InjectTargets, RealizedExpose, realize_expose, serve_backing};

    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(std::time::Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| panic!("{what}: nothing within the ceiling"))
    }

    /// A server whose core the test holds — the registry is how it plays the
    /// far side of the relay.
    async fn served() -> (horde_server::service::ServerCore, crate::Client) {
        let core = horde_server::service::ServerCore::new(
            horde_server::EventLog::in_memory().expect("an in-memory log"),
        );
        let endpoint = horde_server_testkit::serve_core(core.clone())
            .await
            .expect("the server binds loopback");
        let client = crate::Client::connect(&endpoint)
            .await
            .expect("the client dials");
        (core, client)
    }

    /// One live placed expose, opened through the real verb.
    async fn opened_expose(
        core: &horde_server::service::ServerCore,
        client: &mut crate::Client,
    ) -> String {
        horde_server_testkit::place_agent(&core.log, "s1", "agent-1", "w1", "serve")
            .await
            .expect("the placement seeds");
        let opened = client
            .open_forward(
                "s1",
                "agent-1",
                ForwardSpec {
                    direction: ForwardDirection::Expose as i32,
                    name: "echo".to_owned(),
                    worker_port: 4242,
                    client_port: 0,
                },
                "open-1",
            )
            .await
            .expect("the intent journals");
        opened.forward_id.expect("an id").to_string()
    }

    /// The client engine end to end, with the test playing the worker leg
    /// through the registry: accept → tunnel open → dispatch → pair →
    /// bytes both ways — then stop, and the listener is provably gone.
    #[tokio::test]
    async fn a_realized_expose_carries_bytes_and_stops_reaped() {
        let (core, mut client) = served().await;
        let forward_id = opened_expose(&core, &mut client).await;
        let (_epoch, mut lane) = core.forwards.register_worker("w1");

        let realized = realize_expose(&client, &forward_id, "echo", 4242, 0)
            .await
            .expect("the listener binds");
        let local_addr = realized.local_addr;

        let mut conn = within(
            "dial the realized listener",
            tokio::net::TcpStream::connect(local_addr),
        )
        .await
        .expect("accepts");

        // The accept loop's carry opened a tunnel: the dispatch proves it.
        let dispatched = within("the worker lane hears the dispatch", lane.recv())
            .await
            .expect("a connection request");
        assert_eq!(
            dispatched.forward_id.as_ref().map(ToString::to_string),
            Some(forward_id.clone())
        );

        // Answer as the worker would: a second tunnel echoing the id. The
        // plaintext channel claims as Ambient, which may answer anything.
        let mut answering = client
            .open_forward_tunnel()
            .await
            .expect("the answering leg opens");
        answering
            .open(&forward_id, &dispatched.connection_id)
            .await
            .expect("the answer names the connection");

        conn.write_all(b"ping").await.expect("the write lands");
        let crossed = within("bytes reach the answering leg", async {
            use tokio_stream::StreamExt as _;
            loop {
                match answering.from_relay.next().await {
                    Some(Ok(frame)) => {
                        if let Some(horde_proto::v1::forward_tunnel_frame::Frame::Data(bytes)) =
                            frame.frame
                        {
                            return bytes;
                        }
                    }
                    other => panic!("the leg ended before the bytes: {other:?}"),
                }
            }
        })
        .await;
        assert_eq!(crossed, b"ping");

        answering
            .to_relay
            .send(horde_proto::forward::data_frame(b"pong".to_vec()))
            .await
            .expect("the answer sends");
        let mut echoed = [0_u8; 4];
        within("bytes reach the local socket", conn.read_exact(&mut echoed))
            .await
            .expect("the read lands");
        assert_eq!(&echoed, b"pong");

        // Stop is reaped, and provably: the port stops answering.
        realized.stop().await;
        assert!(
            tokio::net::TcpStream::connect(local_addr).await.is_err(),
            "a stopped realization must not keep its port"
        );
    }

    /// Dropping a realization (not just stopping it) aborts the accept
    /// loop: the guard task pattern, on the loop the listener owns.
    #[tokio::test]
    async fn dropping_a_realization_reaps_its_accept_loop() {
        let (guard, reaped) = tokio::sync::oneshot::channel::<()>();
        let accept_task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        drop(RealizedExpose {
            forward_id: "fwd-1".to_owned(),
            name: "echo".to_owned(),
            local_addr: "127.0.0.1:1".parse().expect("an address"),
            fell_back: false,
            accept_task: Some(accept_task),
        });

        let witnessed = within("the drop reaps", reaped).await;
        assert!(
            witnessed.is_err(),
            "the guard must be dropped by cancellation, never sent"
        );
    }

    /// The backer's inject answer, end to end: the parked pairing resolves,
    /// the local target is dialed, and bytes echo through it. A missing
    /// target refuses instead — the code the worker branches on.
    #[tokio::test]
    async fn an_inject_answer_dials_its_target_and_echoes() {
        let (core, client) = served().await;
        let (echo_addr, _echo) = horde_server_testkit::echo_server()
            .await
            .expect("the echo binds");

        let targets: InjectTargets = std::sync::Arc::default();
        targets.write().expect("writable").insert(
            "fwd-db".to_owned(),
            InjectTarget {
                name: "db".to_owned(),
                client_port: echo_addr.port(),
            },
        );

        let paired = core.forwards.park_pending(
            "c9",
            horde_server::forward::registry::PassiveTarget::Client("anyone".to_owned()),
        );
        let answer = tokio::spawn(super::answer_inject_connection(
            client.clone(),
            targets.clone(),
            horde_proto::v1::ForwardConnectionRequest {
                forward_id: Some(horde_proto::v1::ForwardId::new("fwd-db")),
                connection_id: "c9".to_owned(),
            },
        ));

        let mut leg = within("the pairing resolves", paired)
            .await
            .expect("the answer claimed the connection");
        leg.to_passive
            .send(horde_proto::forward::data_frame(b"ping".to_vec()))
            .await
            .expect("the relay side sends");
        let echoed = within("the echo returns through the leg", leg.from_passive.recv())
            .await
            .expect("a frame");
        assert_eq!(echoed, horde_proto::forward::data_frame(b"ping".to_vec()));
        answer.abort();

        // The refusal side: an id this client is not serving.
        let paired = core.forwards.park_pending(
            "c10",
            horde_server::forward::registry::PassiveTarget::Client("anyone".to_owned()),
        );
        let answer = tokio::spawn(super::answer_inject_connection(
            client.clone(),
            targets.clone(),
            horde_proto::v1::ForwardConnectionRequest {
                forward_id: Some(horde_proto::v1::ForwardId::new("fwd-none")),
                connection_id: "c10".to_owned(),
            },
        ));
        let mut leg = within("the refusal pairing resolves", paired)
            .await
            .expect("the refusal still answers the connection");
        let refused = within("the refusal frame", leg.from_passive.recv())
            .await
            .expect("a frame");
        let refusal = horde_proto::forward::refusal_in(&refused).expect("a refusal, not a pump");
        assert_eq!(
            refusal.reason,
            horde_proto::v1::ForwardRefusalReason::UnknownForward as i32
        );
        answer.abort();
    }

    /// Backing over a channel that names no principal is refused by the
    /// server, and the engine reports it rather than pretending to back.
    #[tokio::test]
    async fn serving_without_an_identity_reports_the_refusal() {
        let (_core, client) = served().await;
        let outcome = within(
            "the unauthenticated serve returns",
            serve_backing(&client, std::sync::Arc::default()),
        )
        .await;
        assert!(
            outcome.is_err(),
            "an unbackable serve must not report success"
        );
    }
}
