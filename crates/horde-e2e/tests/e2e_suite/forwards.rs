//! Port forwards over the whole real system (ADR-0006): a raw TCP echo in
//! the fake-adapter agent's workspace, exposed by the operator's own CLI and
//! carried client socket → `horde` process → server relay → worker daemon →
//! echo — every hop a real process on a real socket under a real
//! certificate. Then the worker-kill fault: connections refuse fast with the
//! overlay naming the loss, the intent survives, and a restarted daemon
//! re-realizes without re-declaration.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::support::System;

/// One bounded poll interval for overlay waits.
const POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// How many polls an overlay wait may spend before it is a failure.
const POLLS: u32 = 300;

/// Polls `forward ls` until the single row's `worker_reachable` matches.
async fn reachability_reaches(system: &System, wanted: bool) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    for _ in 0..POLLS {
        last = system.cli_json(&["forward", "ls"]).await;
        let reachable = last
            .as_array()
            .and_then(|rows| rows.first())
            .and_then(|row| row["worker_reachable"].as_bool());
        if reachable == Some(wanted) {
            return last;
        }
        tokio::time::sleep(POLL).await;
    }
    panic!("forward ls never reported worker_reachable={wanted}; last: {last}");
}

/// One echo round trip through a realized local address.
async fn echo_through(local_addr: &str, payload: &[u8]) {
    let mut conn = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::net::TcpStream::connect(local_addr),
    )
    .await
    .expect("the dial must finish")
    .expect("the realized listener accepts");
    conn.write_all(payload).await.expect("the write lands");
    let mut echoed = vec![0_u8; payload.len()];
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        conn.read_exact(&mut echoed),
    )
    .await
    .expect("the echo must return rather than hang")
    .expect("the echo reads");
    assert_eq!(echoed, payload);
}

#[tokio::test]
async fn an_exposed_port_relays_raw_tcp_and_survives_a_worker_kill() {
    let system = System::launch().await;
    let mut worker = system
        .spawn_worker("w-fwd", &system.state.path().join("w-fwd"), &[])
        .await;
    let (session_id, root) = system.placed_session("forward e2e").await;

    // The agent's workspace process: a raw TCP echo on a port this test
    // owns, bound on :0 and passed around by address.
    let echo = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("the echo binds :0");
    let echo_port = echo.local_addr().expect("a bound address").port();
    tokio::spawn(async move {
        while let Ok((socket, _peer)) = echo.accept().await {
            tokio::spawn(async move {
                let (mut read, mut write) = socket.into_split();
                let _ = tokio::io::copy(&mut read, &mut write).await;
            });
        }
    });

    // The operator exposes it: a foreground CLI process that journals the
    // intent, realizes the listener, and prints the mapping.
    let expose = system.spawn_cli(
        "horde[forward-expose]",
        &[
            "forward",
            "expose",
            &session_id,
            &root,
            &echo_port.to_string(),
            "--as",
            "echo",
        ],
    );
    let printed = expose
        .await_stdout("the realized mapping", |out| out.contains("local_addr"))
        .await
        .expect("the expose realizes");
    let mapping: serde_json::Value = printed
        .lines()
        .find(|line| line.contains("local_addr"))
        .and_then(|line| serde_json::from_str(line).ok())
        .expect("the mapping line is one JSON value");
    let local_addr = mapping["local_addr"]
        .as_str()
        .expect("the mapping names the bound address")
        .to_owned();
    assert_eq!(
        mapping["fell_back"],
        serde_json::Value::Bool(true),
        "the requested port is the echo server's own, so the printed mapping \
         is the ephemeral fallback: {mapping}"
    );

    // The overlay agrees before any byte flows.
    let listed = reachability_reaches(&system, true).await;
    assert_eq!(listed[0]["name"], "echo", "{listed}");
    assert_eq!(listed[0]["direction"], "expose", "{listed}");

    // Raw TCP, end to end, twice — the second connection proves the relay
    // outlives the first.
    echo_through(&local_addr, b"through every real process").await;
    echo_through(&local_addr, b"and once more").await;

    // The fault: kill the worker daemon outright.
    worker.kill().await.expect("the worker dies on demand");
    reachability_reaches(&system, false).await;

    // The listener stays bound and refuses fast: connected, then closed
    // with nothing readable.
    let mut refused = tokio::net::TcpStream::connect(&local_addr)
        .await
        .expect("the client listener stays bound");
    let mut buffer = [0_u8; 1];
    let read = tokio::time::timeout(std::time::Duration::from_secs(5), refused.read(&mut buffer))
        .await
        .expect("the refusal must be fast, not a hang");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "a refused connection carries no bytes: {read:?}"
    );

    // Revival: the same worker state directory rejoins, the re-sent intent
    // set re-realizes, and the same local address carries bytes again — no
    // re-declaration anywhere.
    let _worker = system
        .spawn_worker("w-fwd", &system.state.path().join("w-fwd"), &[])
        .await;
    reachability_reaches(&system, true).await;
    echo_through(&local_addr, b"after the revival").await;

    let mut expose = expose;
    expose.terminate().await.expect("the expose ends cleanly");
}
