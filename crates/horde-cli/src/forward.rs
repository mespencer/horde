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

    /// Sends a terminal refusal — the passive side's "I could not carry this".
    pub async fn refuse(&mut self, reason: ForwardRefusalReason, detail: String) {
        let _ = self.to_relay.send(refusal_frame(reason, detail)).await;
    }
}

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
    format!("conn-{nanos:x}-{:x}", ORDINAL.fetch_add(1, Ordering::Relaxed))
}

/// How many consecutive accept failures end a listener's loop. A count
/// bound, because a clock cannot bound a loop that fails without waiting.
pub const ACCEPT_FAILURE_BOUND: u32 = 32;

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
    while consecutive_failures < ACCEPT_FAILURE_BOUND {
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
    if tunnel.open(&forward_id, &request.connection_id).await.is_err() {
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

    let dialed = tokio::time::timeout(
        DIAL_DEADLINE,
        TcpStream::connect(("127.0.0.1", target.client_port)),
    )
    .await;
    match dialed {
        Ok(Ok(tcp)) => {
            let _ = pump_tcp(tcp, tunnel).await;
        }
        Ok(Err(error)) => {
            tunnel
                .refuse(
                    ForwardRefusalReason::TargetDialFailed,
                    format!("dialing 127.0.0.1:{} failed: {error}", target.client_port),
                )
                .await;
        }
        Err(_elapsed) => {
            tunnel
                .refuse(
                    ForwardRefusalReason::TargetDialFailed,
                    format!(
                        "127.0.0.1:{} did not accept within {DIAL_DEADLINE:?}",
                        target.client_port
                    ),
                )
                .await;
        }
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
    use tokio_stream::wrappers::ReceiverStream;

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
        assert_eq!(requested_local_port(5173, 0), 5173, "zero means same-as-worker");
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
        assert!(!pump_is_done(true, false), "the response may still be coming");
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

    /// The pump, end to end over a real socket pair: bytes both ways, then
    /// EOFs both ways, ending clean.
    #[tokio::test]
    async fn a_pump_carries_bytes_both_ways_and_ends_on_both_eofs() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let dialed = tokio::net::TcpStream::connect(addr);
        let (accepted, dialed) = tokio::join!(listener.accept(), dialed);
        let (accepted, _) = accepted.expect("accept");
        let mut local = dialed.expect("dial");

        let (tunnel, feed, mut sent) = scripted_tunnel();
        let pump = tokio::spawn(pump_tcp(accepted, tunnel));

        // Local peer → relay.
        use tokio::io::AsyncWriteExt;
        local.write_all(b"ping").await.expect("write");
        let frame = within("tcp bytes become a data frame", sent.recv())
            .await
            .expect("a frame");
        assert_eq!(frame, data_frame(b"ping".to_vec()));

        // Relay → local peer.
        feed.send(Ok(data_frame(b"pong".to_vec()))).await.expect("feed");
        let mut buffer = [0_u8; 4];
        use tokio::io::AsyncReadExt;
        within("relay bytes reach the socket", local.read_exact(&mut buffer))
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
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let dialed = tokio::net::TcpStream::connect(addr);
        let (accepted, dialed) = tokio::join!(listener.accept(), dialed);
        let (accepted, _) = accepted.expect("accept");
        let _local = dialed.expect("dial");

        let (tunnel, feed, _sent) = scripted_tunnel();
        let pump = tokio::spawn(pump_tcp(accepted, tunnel));
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
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let dialed = tokio::net::TcpStream::connect(addr);
        let (accepted, dialed) = tokio::join!(listener.accept(), dialed);
        let (accepted, _) = accepted.expect("accept");
        let _local = dialed.expect("dial");

        let (tunnel, feed, _sent) = scripted_tunnel();
        let pump = tokio::spawn(pump_tcp(accepted, tunnel));
        drop(feed);

        let end = within("a dead leg ends the pump", pump)
            .await
            .expect("no panic");
        assert!(matches!(end, PumpEnd::RelayEnded), "got {end:?}");
    }
}
