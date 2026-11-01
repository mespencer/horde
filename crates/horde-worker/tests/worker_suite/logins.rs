//! The worker's login plane against a real server on loopback: the dispatch
//! stream, the tunnel, the PTY, and the reap — driven through the same
//! client library the `horde` binary uses, so a login plane hollowed out
//! anywhere along that path cannot answer.

use std::sync::Arc;

use horde_server::EventLog;
use horde_server::service::ServerCore;

use crate::support::serve;

/// One bounded poll interval for cross-process waits.
const POLL: std::time::Duration = std::time::Duration::from_millis(50);

/// How many polls a wait may spend before it is a failure.
const POLLS: u32 = 200;

/// An announce sink the test reads back — the worker's operator lines are
/// the owned witness the reap assertions rest on. A tokio channel, because
/// the login plane runs on this test's own runtime and a blocking receive
/// would starve the very tasks it waits on.
fn announcing() -> (
    horde_worker::login::Announce,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (
        Arc::new(move |line| {
            let _ = tx.send(line);
        }),
        rx,
    )
}

/// Polls (bounded) until the server holds a login lane for `worker`.
async fn lane_registers(core: &ServerCore, worker: &str) {
    for _ in 0..POLLS {
        if core.logins.lane_to(worker).is_some() {
            return;
        }
        tokio::time::sleep(POLL).await;
    }
    panic!("worker {worker} never registered its login stream");
}

/// Drains announce lines (ceilinged, count-bounded) until one holds,
/// keeping what was seen.
async fn announce_reaches(
    lines: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
    holds: impl Fn(&str) -> bool,
) {
    let mut seen = Vec::new();
    let drained = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        for _ in 0..POLLS {
            let Some(line) = lines.recv().await else {
                return false;
            };
            let matched = holds(&line);
            seen.push(line);
            if matched {
                return true;
            }
        }
        false
    })
    .await;
    assert!(
        matches!(drained, Ok(true)),
        "the expected announce line never landed; saw: {seen:?}"
    );
}

/// The whole worker half over the real spine: the plane registers its
/// dispatch stream, a client's open reaches it, the PTY spawns and attaches
/// (the client hears accepted — a handle hollowed to nothing leaves the
/// pairing to die at its deadline), and the client vanishing kills and
/// provably reaps the command, announced only after the wait returned.
#[tokio::test]
async fn a_real_login_dispatch_is_answered_and_a_vanished_client_reaped() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = serve(core.clone()).await;

    let (announce, mut lines) = announcing();
    let mut recipes = std::collections::BTreeMap::new();
    recipes.insert(
        "fake".to_owned(),
        horde_adapter::LoginRecipe::terminal_only(
            "/bin/sh",
            vec!["-c".to_owned(), "read line; printf ok".to_owned()],
        ),
    );
    let plane = Arc::new(horde_worker::login::LoginPlane::new(recipes, announce));

    let worker_client =
        horde_proto::v1::worker_service_client::WorkerServiceClient::connect(endpoint.clone())
            .await
            .expect("the worker spine dials");
    let serving = tokio::spawn(async move {
        let _ = horde_worker::login::serve(worker_client, "w-login", plane).await;
    });
    lane_registers(&core, "w-login").await;

    // The operator's half, through the same library the binary uses.
    let mut client = horde_cli::Client::connect(&endpoint)
        .await
        .expect("the client dials");
    let opened = horde_cli::login::open_login(&mut client, "w-login", "fake", None, 80, 24)
        .await
        .expect("the worker's plane must attach: an accepted answer is the proof");
    assert!(!opened.login_id.is_empty());
    announce_reaches(&mut lines, |line| line.contains("login spawned")).await;

    // The fault: the client vanishes mid-login. The worker's reap line is
    // the owned witness — printed only after wait() returned on the killed
    // child — never an assertion about OS process state.
    drop(opened);
    drop(client);
    announce_reaches(&mut lines, |line| {
        line.contains("login reaped") && line.contains("client-vanished")
    })
    .await;

    serving.abort();
}
