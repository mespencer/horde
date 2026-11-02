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

/// A server on loopback and a login plane whose one provider, `fake`, signs
/// in with `recipe`, its operator lines readable by the test.
async fn plane_over_a_server(
    recipe: horde_adapter::LoginRecipe,
) -> (
    ServerCore,
    String,
    Arc<horde_worker::login::LoginPlane>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
) {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = serve(core.clone()).await;
    let (announce, lines) = announcing();
    let recipes = std::collections::BTreeMap::from([("fake".to_owned(), recipe)]);
    let plane = Arc::new(horde_worker::login::LoginPlane::new(recipes, announce));
    (core, endpoint, plane, lines)
}

/// The whole worker half over the real spine: the plane registers its
/// dispatch stream, a client's open reaches it, the PTY spawns and attaches
/// (the client hears accepted — a handle hollowed to nothing leaves the
/// pairing to die at its deadline), and the client vanishing kills and
/// provably reaps the command, announced only after the wait returned.
#[tokio::test]
async fn a_real_login_dispatch_is_answered_and_a_vanished_client_reaped() {
    let (core, endpoint, plane, mut lines) =
        plane_over_a_server(horde_adapter::LoginRecipe::terminal_only(
            "/bin/sh",
            vec!["-c".to_owned(), "read line; printf ok".to_owned()],
        ))
        .await;

    let worker_client =
        horde_proto::v1::worker_service_client::WorkerServiceClient::connect(endpoint.clone())
            .await
            .expect("the worker spine dials");
    let serving = tokio::spawn(async move {
        let registered = horde_proto::v1::RegisterWorkerRequest::default();
        let _ = horde_worker::login::serve(worker_client, "w-login", &registered, plane).await;
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

/// The latest placement state the log records for `agent`, replayed over
/// the control plane.
async fn placement_of(endpoint: &str, session_id: &str, agent: &str) -> Option<i32> {
    let mut client = horde_cli::Client::connect(endpoint)
        .await
        .expect("the client dials");
    let mut state = None;
    client
        .sync(
            &horde_cli::client::SyncOptions::new(session_id.to_owned()),
            |envelope| {
                if let Some(horde_proto::v1::event::Kind::PlacementChanged(changed)) = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                    && let Some(placement) = &changed.placement
                    && placement
                        .agent_id
                        .as_ref()
                        .map(ToString::to_string)
                        .as_deref()
                        == Some(agent)
                {
                    state = Some(placement.state);
                }
            },
            |_| {},
        )
        .await
        .expect("the replay finishes");
    state
}

/// The account a login signs in is one placement can match, end to end:
/// a spawn requiring the account `work` queues against a Worker that holds
/// no sign-in for it; `horde worker login --account work` runs, its command
/// writes into the account's profile directory and exits cleanly; the
/// Worker re-advertises what it now holds — from that directory, the one an
/// agent bound to the account launches under — and the queued spawn places.
#[tokio::test]
async fn an_account_login_makes_a_spawn_requiring_that_account_placeable() {
    let profile = tempfile::tempdir().expect("a profile root");
    let (core, endpoint, plane, mut lines) = plane_over_a_server(
        horde_adapter::LoginRecipe::terminal_only(
            "/bin/sh",
            vec![
                "-c".to_owned(),
                r#"printf signed > "$FAKE_CONFIG_DIR/credentials""#.to_owned(),
            ],
        )
        .with_accounts("FAKE_CONFIG_DIR", profile.path()),
    )
    .await;
    assert!(
        plane.held_accounts().is_empty(),
        "nothing is signed in yet, so nothing is advertised"
    );

    // The worker registers holding no account, the way a dial would.
    let mut worker_client = horde_worker::client::worker_service(
        tonic::transport::Endpoint::try_from(endpoint.clone())
            .expect("an endpoint")
            .connect()
            .await
            .expect("the worker spine dials"),
    );
    let mut advertised = horde_worker::capabilities::advertise(
        [],
        None,
        &horde_worker::capabilities::DeclaredFacts::default(),
    );
    advertised.provider_accounts = plane.held_accounts();
    let registered = horde_proto::v1::RegisterWorkerRequest {
        hello: Some(horde_worker::capabilities::hello("w-acct")),
        worker_id: Some(horde_proto::v1::WorkerId::new("w-acct")),
        capabilities: Some(advertised),
        last_local_journal_sequence: None,
    };
    worker_client
        .register_worker(registered.clone())
        .await
        .expect("the worker registers");

    let mut client = horde_cli::Client::connect(&endpoint)
        .await
        .expect("the client dials");
    let (summary, _root) = client
        .create_session("accounts", "")
        .await
        .expect("a session");
    let session_id = summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .expect("a session id");
    let spawned = client
        .spawn_agent_with_spec(
            &session_id,
            None,
            horde_proto::v1::AgentSpec {
                placement: Some(horde_proto::v1::PlacementConstraints {
                    required_provider_accounts: vec!["work".to_owned()],
                    ..horde_proto::v1::PlacementConstraints::default()
                }),
                ..horde_proto::v1::AgentSpec::default()
            },
            "needs-work",
        )
        .await
        .expect("the spawn is accepted");
    let child = spawned
        .agent_id
        .as_ref()
        .map(ToString::to_string)
        .expect("an agent id");
    let queued = spawned.placement.expect("a placement");
    assert_eq!(
        queued.state,
        horde_proto::v1::PlacementState::Queued as i32,
        "no worker holds `work` yet"
    );
    assert!(
        queued.reason.contains("lacks provider accounts [`work`]"),
        "{}",
        queued.reason
    );

    let serving = tokio::spawn(async move {
        let _ = horde_worker::login::serve(worker_client, "w-acct", &registered, plane).await;
    });
    lane_registers(&core, "w-acct").await;
    let opened = horde_cli::login::open_login(&mut client, "w-acct", "fake", Some("work"), 80, 24)
        .await
        .expect("the worker's plane attaches");
    announce_reaches(&mut lines, |line| {
        line.contains("readvertised provider_accounts=1")
    })
    .await;

    let mut placed = None;
    for _ in 0..POLLS {
        placed = placement_of(&endpoint, &session_id, &child).await;
        if placed == Some(horde_proto::v1::PlacementState::Placed as i32) {
            break;
        }
        tokio::time::sleep(POLL).await;
    }
    assert_eq!(
        placed,
        Some(horde_proto::v1::PlacementState::Placed as i32),
        "the re-advertised account places the queued spawn"
    );

    drop(opened);
    serving.abort();
}
