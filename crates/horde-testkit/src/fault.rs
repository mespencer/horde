//! The fault injector: a programmable TCP proxy (docs/TESTING.md §2).
//!
//! Grown from the `SeverableProxy` seeded in `horde-worker`'s connection
//! suite, which knew exactly two faults — cut and heal. This is the real
//! instrument: one [`FaultProxy`] sits in front of any TCP upstream and
//! injects, per connection and while connections are live:
//!
//! * **cut / heal** — every live relay is aborted (both sockets drop, exactly
//!   like a yanked cable) and new connections are refused until healed.
//! * **blackhole** — delivery stops but every socket stays open: the spotty
//!   network's worst face, where nothing errors and nothing arrives. Healing
//!   releases the held bytes on the *same* connections.
//! * **delay** — fixed extra latency added to every delivered chunk.
//! * **throttle** — a bandwidth ceiling, paced per chunk.
//! * **cut after a byte budget** — the proxy forwards exactly N more bytes
//!   (both directions combined) and then severs, which is how a test lands a
//!   partition deterministically *inside* a transfer instead of racing a
//!   clock against it.
//!
//! The proxy is layer 4: it never parses what it carries, so gRPC, TLS and
//! git smart-HTTP all ride it unmodified. For a TLS upstream the dialer keeps
//! verifying the upstream's certificate — the proxy moves bytes, it does not
//! terminate anything.
//!
//! This module is held to production lint policy like the rest of the crate:
//! faults are injected on purpose, but the injector itself may not panic.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// How much one relay read moves at a time. Small enough that throttling and
/// byte-budget cuts land with sub-chunk precision on real traffic.
const CHUNK_BYTES: usize = 8 * 1024;

/// The sentinel for "no byte budget is armed".
const BUDGET_DISARMED: u64 = u64::MAX;

/// What can go wrong standing the proxy up — nothing else here returns errors,
/// because a fault injector's job is to make *other* code's errors happen.
#[derive(Debug, thiserror::Error)]
pub enum FaultError {
    /// The loopback listener could not bind.
    #[error("the fault proxy could not bind a loopback listener: {0}")]
    Bind(std::io::Error),
    /// The upstream string did not contain a socket address.
    #[error(
        "{0:?} is not an upstream address (expected `host:port`, \
         with or without an `http://` or `https://` scheme)"
    )]
    Upstream(String),
}

/// A TCP proxy with programmable faults. See the module docs for the fault
/// vocabulary; every control takes effect immediately, including on
/// connections already carrying traffic.
#[derive(Debug)]
pub struct FaultProxy {
    scheme: &'static str,
    addr: SocketAddr,
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    refuse_new: AtomicBool,
    delay_micros: AtomicU64,
    /// Bytes per second; `0` is unlimited.
    throttle: AtomicU64,
    /// Bytes the proxy may still forward before severing; [`BUDGET_DISARMED`]
    /// when no budget is armed. A mutex rather than a CAS loop: claims are
    /// per-chunk and contention is two relay tasks.
    budget: Mutex<u64>,
    /// `false` while blackholed. A watch channel so a held relay parks on a
    /// wakeup instead of polling a clock.
    forward: watch::Sender<bool>,
    live: Mutex<Vec<tokio::task::AbortHandle>>,
    connections_carried: AtomicU64,
    bytes_forwarded: AtomicU64,
}

impl FaultProxy {
    /// Starts a proxy in front of `upstream` — `host:port`, with or without
    /// an `http://`/`https://` scheme. Binds `127.0.0.1` on an OS-chosen
    /// port: DESIGN.md §3 forbids listening wider before auth exists, and a
    /// fault injector is not an exception.
    ///
    /// # Errors
    ///
    /// [`FaultError::Upstream`] when the address does not parse and
    /// [`FaultError::Bind`] when the listener cannot bind.
    pub async fn front(upstream: &str) -> Result<Self, FaultError> {
        let (scheme, upstream_addr) = split_scheme(upstream);
        let upstream_addr: SocketAddr = upstream_addr
            .parse()
            .map_err(|_| FaultError::Upstream(upstream.to_owned()))?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(FaultError::Bind)?;
        let addr = listener.local_addr().map_err(FaultError::Bind)?;

        let (forward, _) = watch::channel(true);
        let shared = Arc::new(Shared {
            refuse_new: AtomicBool::new(false),
            delay_micros: AtomicU64::new(0),
            throttle: AtomicU64::new(0),
            budget: Mutex::new(BUDGET_DISARMED),
            forward,
            live: Mutex::new(Vec::new()),
            connections_carried: AtomicU64::new(0),
            bytes_forwarded: AtomicU64::new(0),
        });

        let accept_shared = Arc::clone(&shared);
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                if accept_shared.refuse_new.load(Ordering::SeqCst) {
                    // The network is down; the connection dies at once.
                    continue;
                }
                accept_shared.carry(inbound, upstream_addr);
            }
        });

        Ok(Self {
            scheme,
            addr,
            shared,
        })
    }

    /// The endpoint to dial instead of the upstream, in the upstream's own
    /// scheme (`http://127.0.0.1:port` for an `http://` upstream).
    #[must_use]
    pub fn endpoint(&self) -> String {
        format!("{}{}", self.scheme, self.addr)
    }

    /// The proxy's bound address.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Severs every live connection and refuses new ones until [`heal`].
    ///
    /// [`heal`]: FaultProxy::heal
    pub fn cut(&self) {
        self.shared.cut();
    }

    /// Stops delivering bytes without closing anything. Sockets stay open,
    /// writes appear to succeed, nothing arrives — until [`heal`] releases
    /// the held bytes on the same connections.
    ///
    /// [`heal`]: FaultProxy::heal
    pub fn blackhole(&self) {
        // `send_replace`, not `send`: relays subscribe per chunk, so there may
        // be no receiver alive at this instant — and `send` on a receiverless
        // watch drops the value on the floor.
        self.shared.forward.send_replace(false);
    }

    /// Adds `extra` latency to every delivered chunk.
    pub fn delay(&self, extra: Duration) {
        let micros = u64::try_from(extra.as_micros()).unwrap_or(u64::MAX);
        self.shared.delay_micros.store(micros, Ordering::SeqCst);
    }

    /// Caps delivery at `bytes_per_second`, paced per chunk. Zero is refused
    /// as a throttle — a link that moves nothing is [`blackhole`], and the
    /// two must not blur.
    ///
    /// [`blackhole`]: FaultProxy::blackhole
    pub fn throttle(&self, bytes_per_second: u64) {
        self.shared
            .throttle
            .store(bytes_per_second.max(1), Ordering::SeqCst);
    }

    /// Arms a byte budget: the proxy forwards exactly `bytes` more (both
    /// directions combined), then severs as [`cut`] does.
    ///
    /// [`cut`]: FaultProxy::cut
    pub fn cut_after(&self, bytes: u64) {
        *self.shared.lock_budget() = bytes;
    }

    /// Restores the healthy network: connections are accepted, held bytes are
    /// released, and every delay, throttle and byte budget is cleared.
    /// Connections severed by [`cut`] stay severed — healing repairs the
    /// road, not the crashes.
    ///
    /// [`cut`]: FaultProxy::cut
    pub fn heal(&self) {
        self.shared.refuse_new.store(false, Ordering::SeqCst);
        self.shared.delay_micros.store(0, Ordering::SeqCst);
        self.shared.throttle.store(0, Ordering::SeqCst);
        *self.shared.lock_budget() = BUDGET_DISARMED;
        self.shared.forward.send_replace(true);
    }

    /// How many connections the proxy has carried (refused ones not counted).
    #[must_use]
    pub fn connections_carried(&self) -> u64 {
        self.shared.connections_carried.load(Ordering::SeqCst)
    }

    /// How many bytes the proxy has forwarded, both directions combined.
    #[must_use]
    pub fn bytes_forwarded(&self) -> u64 {
        self.shared.bytes_forwarded.load(Ordering::SeqCst)
    }
}

impl Shared {
    /// The budget, with a poisoned lock healed rather than propagated: the
    /// value is a `u64` and every write to it is a single store, so the
    /// poison carries no torn state worth dying over.
    fn lock_budget(&self) -> std::sync::MutexGuard<'_, u64> {
        self.budget.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Severs under the `live` lock, so a relay registering concurrently
    /// either lands before the drain (and is aborted by it) or observes
    /// `refuse_new` (and aborts itself). No interleaving lets a connection
    /// slip through a cut.
    fn cut(&self) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        self.refuse_new.store(true, Ordering::SeqCst);
        for relay in live.drain(..) {
            relay.abort();
        }
    }

    /// Registers relay tasks as severable — or aborts them at once when the
    /// network is already cut. Finished tasks left in the list are inert:
    /// aborting one is a no-op.
    fn register(&self, handles: &[tokio::task::AbortHandle]) {
        let mut live = self.live.lock().unwrap_or_else(PoisonError::into_inner);
        if self.refuse_new.load(Ordering::SeqCst) {
            for handle in handles {
                handle.abort();
            }
            return;
        }
        live.extend(handles.iter().cloned());
    }

    /// Spawns the two directional relays for one accepted connection.
    ///
    /// Both directions are joined, not raced: a peer that half-closes (EOF on
    /// one direction, the other still answering) is served exactly as a real
    /// network would, with each pump propagating its own EOF as a shutdown.
    fn carry(self: &Arc<Self>, inbound: TcpStream, upstream: SocketAddr) {
        self.connections_carried.fetch_add(1, Ordering::SeqCst);
        let shared = Arc::clone(self);
        let relay = tokio::spawn(async move {
            let Ok(outbound) = TcpStream::connect(upstream).await else {
                return;
            };
            let (in_read, in_write) = inbound.into_split();
            let (out_read, out_write) = outbound.into_split();
            let up = tokio::spawn(Arc::clone(&shared).pump(in_read, out_write));
            let down = tokio::spawn(Arc::clone(&shared).pump(out_read, in_write));
            // The pumps move the bytes; cut() severs by aborting them, so
            // they must be registered — aborting only this parent task would
            // leave both running with the sockets they own.
            shared.register(&[up.abort_handle(), down.abort_handle()]);
            let _ = tokio::join!(up, down);
        });
        self.register(&[relay.abort_handle()]);
    }

    /// Moves bytes one way until EOF, an error, a sever, or an abort.
    async fn pump(self: Arc<Self>, mut from: OwnedReadHalf, mut to: OwnedWriteHalf) {
        let mut buffer = [0_u8; CHUNK_BYTES];
        while let Ok(read) = from.read(&mut buffer).await {
            if read == 0 || self.deliver(&mut to, &buffer[..read]).await.is_err() {
                break;
            }
        }
        let _ = to.shutdown().await;
    }

    /// Delivers one chunk under the current faults. `Err` means the relay is
    /// over — the budget severed it, the peer is gone, or the blackhole gate
    /// closed for good.
    async fn deliver(&self, to: &mut OwnedWriteHalf, chunk: &[u8]) -> Result<(), Undeliverable> {
        let extra = Duration::from_micros(self.delay_micros.load(Ordering::SeqCst));
        if !extra.is_zero() {
            tokio::time::sleep(extra).await;
        }
        let pace = pacing(chunk.len(), self.throttle.load(Ordering::SeqCst));
        if !pace.is_zero() {
            tokio::time::sleep(pace).await;
        }

        // The blackhole gate. Parked on a wakeup, never a poll: the wait ends
        // when `heal` flips the watch or the proxy is dropped.
        let mut forward = self.forward.subscribe();
        if forward.wait_for(|open| *open).await.is_err() {
            return Err(Undeliverable);
        }

        let (allowed, sever) = self.claim(chunk.len());
        if allowed > 0 {
            if to.write_all(&chunk[..allowed]).await.is_err() {
                return Err(Undeliverable);
            }
            self.bytes_forwarded
                .fetch_add(allowed as u64, Ordering::SeqCst);
        }
        if sever {
            self.cut();
            return Err(Undeliverable);
        }
        Ok(())
    }

    /// Claims up to `wanted` bytes from the budget.
    fn claim(&self, wanted: usize) -> (usize, bool) {
        let mut budget = self.lock_budget();
        let (allowed, sever, remaining) = budget_claim(*budget, wanted);
        *budget = remaining;
        (allowed, sever)
    }
}

/// The relay's "this connection is over" marker.
struct Undeliverable;

/// What one chunk may take from the byte budget:
/// `(bytes to forward, sever afterwards, budget left)`.
///
/// A disarmed budget forwards everything; a chunk that reaches the budget's
/// edge forwards exactly the remainder and severs — so the peer observes a
/// clean prefix of the stream, never a byte past the budget.
fn budget_claim(budget: u64, wanted: usize) -> (usize, bool, u64) {
    if budget == BUDGET_DISARMED {
        return (wanted, false, BUDGET_DISARMED);
    }
    let wanted_u64 = wanted as u64;
    if wanted_u64 < budget {
        return (wanted, false, budget - wanted_u64);
    }
    let allowed = usize::try_from(budget).unwrap_or(usize::MAX);
    (allowed, true, 0)
}

/// How long delivering `bytes` must take under a `bytes_per_second` ceiling.
/// Zero means unthrottled.
fn pacing(bytes: usize, bytes_per_second: u64) -> Duration {
    if bytes_per_second == 0 {
        return Duration::ZERO;
    }
    // Micros rather than float: (bytes * 1e6) / rate, saturating.
    let micros = (bytes as u64)
        .saturating_mul(1_000_000)
        .checked_div(bytes_per_second)
        .unwrap_or(0);
    Duration::from_micros(micros)
}

/// Splits a known scheme prefix off an endpoint, keeping it for
/// [`FaultProxy::endpoint`].
fn split_scheme(endpoint: &str) -> (&'static str, &str) {
    if let Some(rest) = endpoint.strip_prefix("http://") {
        ("http://", rest)
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        ("https://", rest)
    } else {
        ("", endpoint)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use super::{BUDGET_DISARMED, FaultProxy, budget_claim, pacing, split_scheme};

    /// A ceiling for every wait: a fault test that hangs has found a worse
    /// bug than any it was written for.
    async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(10), future)
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
    }

    /// An echo server on a loopback port; every accepted connection echoes
    /// until EOF.
    async fn echo_upstream() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let address = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    while let Ok(read) = socket.read(&mut buffer).await {
                        if read == 0 || socket.write_all(&buffer[..read]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("http://{address}")
    }

    /// A sink server that collects everything it receives, for byte-exact
    /// assertions about what crossed the proxy.
    async fn sink_upstream() -> (String, Arc<std::sync::Mutex<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let address = listener.local_addr().expect("a bound address");
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&received);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let sink = Arc::clone(&sink);
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 1024];
                    while let Ok(read) = socket.read(&mut buffer).await {
                        if read == 0 {
                            return;
                        }
                        sink.lock()
                            .expect("an unpoisoned sink")
                            .extend_from_slice(&buffer[..read]);
                    }
                });
            }
        });
        (format!("http://{address}"), received)
    }

    async fn dial(proxy: &FaultProxy) -> TcpStream {
        TcpStream::connect(proxy.addr())
            .await
            .expect("the proxy accepts")
    }

    async fn round_trip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
        stream.write_all(payload).await.expect("the write carries");
        let mut echoed = vec![0_u8; payload.len()];
        stream
            .read_exact(&mut echoed)
            .await
            .expect("the echo returns");
        echoed
    }

    #[tokio::test]
    async fn a_healthy_proxy_carries_bytes_and_counts_them() {
        let upstream = echo_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");
        assert_eq!(
            proxy.endpoint(),
            format!("http://{}", proxy.addr()),
            "the endpoint keeps the upstream's scheme"
        );

        let mut stream = dial(&proxy).await;
        let echoed = within("the echo", round_trip(&mut stream, b"ping")).await;
        assert_eq!(&echoed, b"ping");
        assert_eq!(proxy.connections_carried(), 1);
        assert_eq!(
            proxy.bytes_forwarded(),
            8,
            "four bytes each way crossed the proxy"
        );
    }

    /// The positive control the seed proxy had, kept: a proxy that cannot be
    /// shown to sever proves nothing about partition behavior.
    #[tokio::test]
    async fn cut_severs_live_connections_and_refuses_new_ones_until_heal() {
        let upstream = echo_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");

        let mut live = dial(&proxy).await;
        within("the pre-cut echo", round_trip(&mut live, b"up")).await;

        proxy.cut();
        let severed = within("the severed read", async {
            let mut buffer = [0_u8; 8];
            live.read(&mut buffer).await
        })
        .await;
        assert!(
            matches!(severed, Ok(0) | Err(_)),
            "a cut connection must end, not linger: {severed:?}"
        );

        // New connections die at once: accepted by the listener, never
        // carried. The refusal is that no byte ever returns.
        let mut refused = dial(&proxy).await;
        let outcome = within("the refused connection to end", async {
            refused.write_all(b"hello?").await.ok();
            let mut buffer = [0_u8; 8];
            refused.read(&mut buffer).await
        })
        .await;
        assert!(
            matches!(outcome, Ok(0) | Err(_)),
            "a connection through a cut proxy must be dead: {outcome:?}"
        );
        assert_eq!(
            proxy.connections_carried(),
            1,
            "refused connections are not carried"
        );

        proxy.heal();
        let mut healed = dial(&proxy).await;
        let echoed = within("the healed echo", round_trip(&mut healed, b"back")).await;
        assert_eq!(&echoed, b"back");
    }

    /// What separates a blackhole from a cut: nothing arrives, but nothing
    /// closes either — and healing releases the held bytes on the *same*
    /// connection, which a severed socket could never do.
    #[tokio::test]
    async fn a_blackhole_holds_bytes_that_heal_releases_on_the_same_connection() {
        let upstream = echo_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");
        let mut stream = dial(&proxy).await;
        within("the pre-blackhole echo", round_trip(&mut stream, b"ok")).await;

        proxy.blackhole();
        stream
            .write_all(b"held")
            .await
            .expect("a blackholed write still succeeds locally");

        // Nothing may arrive while the hole is open. A bounded quiet window
        // is the only honest way to observe absence; the positive control is
        // the release below, which proves the same bytes were merely held.
        let mut buffer = [0_u8; 8];
        let held = tokio::time::timeout(Duration::from_millis(300), stream.read(&mut buffer)).await;
        assert!(
            held.is_err(),
            "a blackholed link must deliver nothing: {held:?}"
        );

        proxy.heal();
        let mut echoed = vec![0_u8; 4];
        within("the released echo", stream.read_exact(&mut echoed))
            .await
            .expect("the held bytes arrive after heal");
        assert_eq!(&echoed, b"held", "the very bytes written into the hole");
    }

    /// The deterministic mid-transfer partition: exactly the budget crosses,
    /// then the line goes dead — no racing a clock against a transfer.
    #[tokio::test]
    async fn cut_after_forwards_exactly_the_budget_and_then_severs() {
        let (upstream, received) = sink_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");
        proxy.cut_after(10);

        let mut stream = dial(&proxy).await;
        // More than the budget, in one write.
        stream.write_all(&[7_u8; 64]).await.expect("the write");

        // The connection must die: read EOF or error within the ceiling.
        let ended = within("the severed connection to end", async {
            let mut buffer = [0_u8; 8];
            stream.read(&mut buffer).await
        })
        .await;
        assert!(matches!(ended, Ok(0) | Err(_)), "got {ended:?}");

        let delivered = received.lock().expect("an unpoisoned sink").clone();
        assert_eq!(
            delivered.len(),
            10,
            "exactly the budgeted bytes crossed, not one more"
        );
        assert_eq!(proxy.bytes_forwarded(), 10);

        // The sever is a cut: new connections are refused until heal, and
        // heal also disarms the budget, so a retry can then run to the end.
        proxy.heal();
        let mut retry = dial(&proxy).await;
        retry.write_all(&[9_u8; 64]).await.expect("the retry write");
        retry.shutdown().await.expect("the retry closes");
        within("the retry to be fully delivered", async {
            let mut polls = 0_u32;
            loop {
                if received.lock().expect("an unpoisoned sink").len() == 74 {
                    return;
                }
                polls += 1;
                assert!(polls < 100, "the healed retry never delivered 64 bytes");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }

    /// The slow link is slow: a delay defers arrival, a throttle paces it.
    /// Lower bounds only — an upper bound would measure the machine.
    #[tokio::test]
    async fn delay_and_throttle_defer_delivery() {
        let upstream = echo_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");

        proxy.delay(Duration::from_millis(200));
        let mut stream = dial(&proxy).await;
        let started = std::time::Instant::now();
        within("the delayed echo", round_trip(&mut stream, b"slow")).await;
        assert!(
            started.elapsed() >= Duration::from_millis(400),
            "four bytes each way, 200ms per chunk each way: {:?}",
            started.elapsed()
        );

        proxy.heal();
        proxy.throttle(4096);
        let payload = vec![3_u8; 2048];
        let started = std::time::Instant::now();
        within("the throttled echo", round_trip(&mut stream, &payload)).await;
        assert!(
            started.elapsed() >= Duration::from_millis(500),
            "2 KiB at 4 KiB/s each way cannot beat half a second: {:?}",
            started.elapsed()
        );
    }

    /// `heal` clears *every* fault. Each fault here is set so severe that a
    /// heal which failed to clear it would blow the test's own ceiling —
    /// which is how the reset of each field is observable without asserting
    /// an upper bound on a healthy machine.
    #[tokio::test]
    async fn heal_clears_delay_throttle_and_budget() {
        let upstream = echo_upstream().await;
        let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");

        proxy.delay(Duration::from_secs(3600));
        proxy.throttle(1); // 100 bytes would take 100 seconds.
        proxy.cut_after(3); // 100 bytes would sever mid-payload.
        proxy.heal();

        let mut stream = dial(&proxy).await;
        let echoed = within("the healed echo", round_trip(&mut stream, &[5_u8; 100])).await;
        assert_eq!(
            echoed.len(),
            100,
            "a heal that left any fault armed could not have delivered this"
        );
    }

    /// The budget arithmetic, as a truth table.
    #[test]
    fn the_budget_claim_forwards_a_clean_prefix_and_severs_at_the_edge() {
        // Disarmed: everything flows, forever.
        assert_eq!(
            budget_claim(BUDGET_DISARMED, 100),
            (100, false, BUDGET_DISARMED)
        );

        // Under budget: the chunk flows, the budget shrinks.
        assert_eq!(budget_claim(50, 10), (10, false, 40));

        // Exactly at the edge: the whole chunk flows, then the line dies.
        assert_eq!(budget_claim(10, 10), (10, true, 0));

        // Over the edge: the remainder flows, not one byte more.
        assert_eq!(budget_claim(10, 64), (10, true, 0));

        // Exhausted: nothing flows and the sever holds.
        assert_eq!(budget_claim(0, 64), (0, true, 0));
    }

    #[test]
    fn pacing_charges_time_per_byte_and_zero_means_unthrottled() {
        assert_eq!(pacing(2048, 0), Duration::ZERO);
        assert_eq!(pacing(0, 4096), Duration::ZERO);
        assert_eq!(pacing(4096, 4096), Duration::from_secs(1));
        assert_eq!(pacing(1024, 4096), Duration::from_millis(250));
    }

    #[test]
    fn schemes_split_off_and_come_back_on_the_endpoint() {
        assert_eq!(split_scheme("http://1.2.3.4:5"), ("http://", "1.2.3.4:5"));
        assert_eq!(split_scheme("https://1.2.3.4:5"), ("https://", "1.2.3.4:5"));
        assert_eq!(split_scheme("1.2.3.4:5"), ("", "1.2.3.4:5"));
    }

    #[tokio::test]
    async fn an_unparseable_upstream_is_refused_by_name() {
        let refused = FaultProxy::front("http://nowhere.example:80").await;
        let message = match refused {
            Err(error) => error.to_string(),
            Ok(_) => panic!("a hostname is not a socket address"),
        };
        assert!(
            message.contains("nowhere.example"),
            "the refusal names what was given: {message}"
        );
    }
}
