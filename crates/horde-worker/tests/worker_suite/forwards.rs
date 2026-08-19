//! Port forwards end to end over real sockets (ADR-0006).
//!
//! Every test here carries real TCP bytes through the whole path — client
//! socket → client leg → server relay → worker leg → worker socket — with a
//! real `horde-server` on a loopback port and the production worker plane
//! (`horde_worker::forward`) and client engine (`horde_cli::forward`) on
//! either side. Refusals are asserted through the code's own surfaces: a
//! refused connection is a socket that closes with nothing read, and
//! reachability/backing are `ListForwards`' overlay — never a probe of a
//! port this suite does not own. Every listener is bound on `:0` and its
//! address passed around.

use horde_cli::Client;
use horde_proto::v1::worker_service_client::WorkerServiceClient;
use horde_proto::v1::{ForwardDirection, ForwardSpec};
use horde_server::EventLog;
use horde_server::service::ServerCore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::support::{place_agent, serve, within};

/// The shared testkit echo server: plays the agent's workspace process
/// (expose) or the service on the human's machine (inject).
async fn echo_server() -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    horde_server_testkit::echo_server()
        .await
        .expect("the echo server binds :0")
}

fn expose_spec(name: &str, worker_port: u16) -> ForwardSpec {
    ForwardSpec {
        direction: ForwardDirection::Expose as i32,
        name: name.to_owned(),
        worker_port: u32::from(worker_port),
        client_port: 0,
    }
}

/// Polls `ListForwards` (bounded by count) until `holds` accepts the answer.
async fn forwards_reach(
    client: &mut Client,
    holds: impl Fn(&[horde_proto::v1::ForwardStatus]) -> bool,
) -> Vec<horde_proto::v1::ForwardStatus> {
    let mut last = Vec::new();
    for _ in 0..250 {
        last = within("list forwards", client.list_forwards(None))
            .await
            .expect("the list answers");
        if holds(&last) {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the forwards overlay never reached the expected state: {last:?}");
}

/// One round trip through a connected socket, asserted byte for byte.
async fn echo_round_trip(conn: &mut TcpStream, payload: &[u8]) {
    within("write through the tunnel", conn.write_all(payload))
        .await
        .expect("the write lands");
    let mut echoed = vec![0_u8; payload.len()];
    within("read the echo back", conn.read_exact(&mut echoed))
        .await
        .expect("the echo returns");
    assert_eq!(echoed, payload);
}

/// Dials a worker-loopback port until its listener answers, bounded by
/// count: the worker binds inject listeners on reconcile, which follows the
/// intent push asynchronously, so the first dials can race the bind.
async fn dial_until_accepted(port: u16) -> TcpStream {
    for _ in 0..250 {
        if let Ok(conn) = TcpStream::connect(("127.0.0.1", port)).await {
            return conn;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("the worker-side listener on {port} never came up");
}

/// A relayed expose ready to realize: fresh server, echo bound, agent
/// placed on `w1`, intent journaled — the arrangement the expose tests
/// share. The echo task rides along so its listener lives as long as the
/// harness does.
struct ExposedEcho {
    endpoint: String,
    client: Client,
    forward_id: String,
    echo_port: u16,
    _echo: tokio::task::JoinHandle<()>,
}

async fn exposed_echo(session: &str) -> ExposedEcho {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = serve(core.clone()).await;
    let (echo_addr, echo) = echo_server().await;
    place_agent(&core.log, session, "agent-1", "w1", "serve the echo").await;

    let mut client = Client::connect(&endpoint).await.expect("the client dials");
    let opened = within(
        "open the expose",
        client.open_forward(
            session,
            "agent-1",
            expose_spec("echo", echo_addr.port()),
            "open-1",
        ),
    )
    .await
    .expect("the intent journals");
    ExposedEcho {
        endpoint,
        client,
        forward_id: opened.forward_id.expect("an id").to_string(),
        echo_port: echo_addr.port(),
        _echo: echo,
    }
}

/// A worker forward plane serving `w1` over its own channel.
async fn worker_plane(endpoint: &str) -> tokio::task::JoinHandle<()> {
    let worker_client = WorkerServiceClient::connect(endpoint.to_owned())
        .await
        .expect("the worker dials");
    tokio::spawn(async move {
        let _ = horde_worker::forward::serve(worker_client, "w1").await;
    })
}

/// Waits for the overlay to report the placed worker (un)reachable.
async fn reachability_reaches(client: &mut Client, wanted: bool) {
    forwards_reach(client, |forwards| {
        forwards
            .iter()
            .any(|status| status.worker_reachable == wanted)
    })
    .await;
}

/// The harness's expose realized on this client — requesting the echo's
/// own port, so the ephemeral fallback is what actually binds.
async fn realized_echo(harness: &ExposedEcho) -> horde_cli::forward::RealizedExpose {
    horde_cli::forward::realize_expose(
        &harness.client,
        &harness.forward_id,
        "echo",
        u32::from(harness.echo_port),
        0,
    )
    .await
    .expect("the listener binds")
}

/// Serving IS backing: one spawned backing task over these targets.
fn spawn_backing(
    client: &Client,
    targets: &horde_cli::forward::InjectTargets,
) -> tokio::task::JoinHandle<()> {
    let client = client.clone();
    let targets = targets.clone();
    tokio::spawn(async move {
        let _ = horde_cli::forward::serve_backing(&client, targets).await;
    })
}

/// A refused connection: closes with nothing readable, within the ceiling.
async fn assert_refused_fast(conn: &mut TcpStream) {
    let mut buffer = [0_u8; 1];
    let read = within("a refusal closes the connection", conn.read(&mut buffer)).await;
    match read {
        Ok(0) => {}
        Ok(n) => panic!("a refused connection must carry no bytes, got {n}"),
        Err(_reset) => {} // A reset is a refusal too.
    }
}

/// The whole expose path: intent journaled, worker plane subscribed, client
/// listener realized (on the ephemeral fallback, since the requested port is
/// the echo server's own), and a TCP echo carried both ways — twice, because
/// the second connection proves the relay outlives its first.
#[tokio::test]
async fn an_expose_carries_a_tcp_echo_end_to_end() {
    let mut harness = exposed_echo("fwd-e2e").await;
    let plane = worker_plane(&harness.endpoint).await;
    reachability_reaches(&mut harness.client, true).await;

    let realized = realized_echo(&harness).await;
    assert!(
        realized.fell_back,
        "the requested port is the echo server's own, so the client falls \
         back to an ephemeral one and prints the mapping"
    );

    for payload in [b"ping over the relay".as_slice(), b"and again".as_slice()] {
        let mut conn = within(
            "dial the realized listener",
            TcpStream::connect(realized.local_addr),
        )
        .await
        .expect("the realized listener accepts");
        echo_round_trip(&mut conn, payload).await;
        // Full close, both ways, ends clean rather than hanging.
        within("shutdown", conn.shutdown()).await.expect("shutdown");
        let mut rest = Vec::new();
        let _ = within("drain to eof", conn.read_to_end(&mut rest)).await;
    }

    realized.stop().await;
    plane.abort();
}

/// The fast-refusal contract: with no worker plane connected, a realized
/// listener still accepts — and closes each connection with nothing written,
/// while the overlay says why.
#[tokio::test]
async fn an_unreachable_worker_refuses_connections_fast() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = serve(core.clone()).await;
    place_agent(
        &core.log,
        "fwd-refuse",
        "agent-1",
        "w-gone",
        "nothing serves",
    )
    .await;

    let mut client = Client::connect(&endpoint).await.expect("the client dials");
    let opened = within(
        "open the expose",
        client.open_forward("fwd-refuse", "agent-1", expose_spec("dead", 1), "open-1"),
    )
    .await
    .expect("the intent journals — reachability is realization, not intent");
    let forward_id = opened.forward_id.expect("an id").to_string();

    let listed = forwards_reach(&mut client, |forwards| !forwards.is_empty()).await;
    assert!(
        !listed[0].worker_reachable,
        "the overlay says the worker is unreachable"
    );

    let realized = horde_cli::forward::realize_expose(&client, &forward_id, "dead", 1, 0)
        .await
        .expect("the listener binds regardless — intent outlives reachability");
    let mut conn = within("dial the listener", TcpStream::connect(realized.local_addr))
        .await
        .expect("the listener accepts");
    assert_refused_fast(&mut conn).await;
    realized.stop().await;
}

/// Partition mid-stream: in-flight forwards reset, the intent survives, and
/// a reconnected worker plane re-realizes without re-declaration.
#[tokio::test]
async fn a_partition_resets_in_flight_forwards_and_reattach_re_realizes() {
    let mut harness = exposed_echo("fwd-part").await;
    let plane = worker_plane(&harness.endpoint).await;
    reachability_reaches(&mut harness.client, true).await;

    let realized = realized_echo(&harness).await;

    // A connection alive mid-partition.
    let mut conn = within("dial", TcpStream::connect(realized.local_addr))
        .await
        .expect("accepts");
    echo_round_trip(&mut conn, b"before the partition").await;

    // The partition: the worker plane dies, taking its tunnel legs with it.
    plane.abort();

    // The in-flight connection resets rather than hanging idle.
    let mut buffer = [0_u8; 1];
    let torn = within(
        "the in-flight connection dies with the worker",
        conn.read(&mut buffer),
    )
    .await;
    assert!(
        matches!(torn, Ok(0) | Err(_)),
        "a dead worker leg must reset the connection, got {torn:?}"
    );

    // The durable intent survives the dead realization.
    let survived = forwards_reach(&mut harness.client, |forwards| {
        forwards.iter().any(|status| !status.worker_reachable)
    })
    .await;
    assert_eq!(survived.len(), 1, "the intent is still journaled");

    // Reattach: a fresh plane re-realizes from the re-sent intent set, and
    // the same still-bound client listener carries bytes again.
    let plane = worker_plane(&harness.endpoint).await;
    reachability_reaches(&mut harness.client, true).await;

    let mut conn = within(
        "dial after reattach",
        TcpStream::connect(realized.local_addr),
    )
    .await
    .expect("the listener stayed bound");
    echo_round_trip(&mut conn, b"after the reattach").await;

    realized.stop().await;
    plane.abort();
}

/// The inject backer lifecycle over real mTLS: accept→back→serve, backer
/// disconnect→unbacked fast refusal, backer return→re-backed — with the
/// backer elected by identity, which is why this test runs on the secured
/// harness rather than plaintext.
#[tokio::test]
async fn an_inject_backs_serves_unbacks_and_re_backs() {
    let harness = horde_server_testkit::secure::serve_secured()
        .await
        .expect("the secured server starts");
    let client_identity = harness
        .enroll(horde_identity::PrincipalRole::Client, "laptop")
        .await
        .expect("the client enrolls");
    let worker_identity = harness
        .enroll(horde_identity::PrincipalRole::Worker, "w1")
        .await
        .expect("the worker enrolls");

    // The service only the client's machine can see.
    let (target_addr, _echo) = echo_server().await;
    // The worker-loopback port the agent will dial: reserved by binding :0
    // and releasing — the one honest way to pick a concrete free port for a
    // spec that must journal it.
    let reserved = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("reserve a port");
    let worker_port = reserved.local_addr().expect("addr").port();
    drop(reserved);

    place_agent(&harness.log, "fwd-inj", "agent-1", "w1", "reach the db").await;

    let mut client = harness
        .client(&client_identity)
        .await
        .expect("the enrolled client dials");
    let opened = within(
        "open the inject",
        client.open_forward(
            "fwd-inj",
            "agent-1",
            ForwardSpec {
                direction: ForwardDirection::Inject as i32,
                name: "db".to_owned(),
                worker_port: u32::from(worker_port),
                client_port: u32::from(target_addr.port()),
            },
            "open-1",
        ),
    )
    .await
    .expect("the initiator is the backer");
    let forward_id = opened.forward_id.expect("an id").to_string();

    // The worker plane, over its own mTLS channel.
    let worker_client = harness
        .worker_client(&worker_identity)
        .await
        .expect("the worker dials");
    let _plane = tokio::spawn(async move {
        let _ = horde_worker::forward::serve(worker_client, "w1").await;
    });

    // Back it: serving IS backing.
    let targets: horde_cli::forward::InjectTargets = std::sync::Arc::default();
    if let Ok(mut map) = targets.write() {
        map.insert(
            forward_id.clone(),
            horde_cli::forward::InjectTarget {
                name: "db".to_owned(),
                client_port: target_addr.port(),
            },
        );
    }
    let backing = spawn_backing(&client, &targets);
    forwards_reach(&mut client, |forwards| {
        forwards
            .iter()
            .any(|status| status.backed && status.worker_reachable)
    })
    .await;

    // The agent's side: dial the worker-loopback port, echo through the
    // backer's machine. The first dials may race the reconcile that binds
    // the listener, so this dial retries, bounded.
    let mut conn = dial_until_accepted(worker_port).await;
    echo_round_trip(&mut conn, b"through the backer").await;
    drop(conn);

    // The backer detaches: the intent survives, the overlay says unbacked,
    // and a fresh agent-side connection refuses fast with nothing written.
    backing.abort();
    forwards_reach(&mut client, |forwards| {
        forwards.iter().any(|status| !status.backed)
    })
    .await;
    let mut refused = within(
        "the listener still accepts while unbacked",
        TcpStream::connect(("127.0.0.1", worker_port)),
    )
    .await
    .expect("the worker-side listener stays bound");
    assert_refused_fast(&mut refused).await;

    // The owning client's return re-backs without re-declaration.
    let backing = spawn_backing(&client, &targets);
    forwards_reach(&mut client, |forwards| {
        forwards.iter().any(|status| status.backed)
    })
    .await;
    let mut conn = within(
        "the agent dials again after the re-back",
        TcpStream::connect(("127.0.0.1", worker_port)),
    )
    .await
    .expect("accepts");
    echo_round_trip(&mut conn, b"re-backed").await;

    backing.abort();
}
