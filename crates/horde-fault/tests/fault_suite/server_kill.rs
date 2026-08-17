//! Scenario (c): `kill -9` the server mid-append, restart it, and the
//! log is the only survivor.
//!
//! The server is a real OS process running the production startup stack —
//! `horde_server::startup`'s parse, serve and announce, via the
//! `fault-server` fixture (see its module docs for why the shipped
//! binary's `[[bin]]` cannot be relied on across packages) — serving mTLS
//! on fixed ports over a SQLite log on disk, with a client enrolled
//! through the bootstrap code on its stdout and a worker enrolled through
//! a minted code and running as its own process. The kill lands while a turn
//! is journaling — the victim turn's stall guarantees its completion happens
//! *while the server is dead*, so recovery has real work to do:
//!
//! * **the log survives**: everything acked before the kill replays after;
//! * **workers reconcile**: the runner redials the same endpoint, resumes
//!   from its own watermarks, and delivers the turn that finished in the
//!   dark — exactly once, no respawn;
//! * **clients resync**: a fresh connection replays a consistent history
//!   and new work flows end to end.

use std::time::Duration;

use horde_cli::Client;
use horde_cli::client::SyncOptions;
use horde_identity::{JoinCode, PrincipalRole};
use horde_proto::v1::Event;
use horde_proto::v1::event::Kind;
use horde_server_testkit::secure::hello;

use horde_fault::{ServerConfig, ServerProcess, TurnSpec, WorkerProcess, WorkerRunnerConfig};

use crate::support;

/// Joins over the real enrollment listener with `code`, returning the
/// stored identity.
async fn join_as(
    enrollment_endpoint: &str,
    code: &str,
    role: PrincipalRole,
    name: &str,
) -> horde_identity::StoredIdentity {
    let code = JoinCode::parse(code).expect("a parseable join code");
    let enrolled = support::within(
        "the enrollment join",
        horde_identity::join(enrollment_endpoint, &code, hello(role, name)),
    )
    .await
    .expect("the join succeeds");
    enrolled.identity
}

/// Connects as `identity`, retrying while a just-(re)started listener comes
/// up. Bounded by attempts, each attempt bounded by tonic's own connect.
async fn connect_client(endpoint: &str, identity: &horde_identity::StoredIdentity) -> Client {
    for _ in 0_u32..support::MAX_POLLS {
        match Client::connect_with_identity(endpoint, identity).await {
            Ok(client) => return client,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    panic!("the server at {endpoint} never accepted an enrolled connection");
}

/// Polls `session`'s replay through the client until `arrived` holds — the
/// only window this scenario has, since the server is a separate process.
/// Bounded by poll count; the failure dumps what the log actually held,
/// because "it never arrived" is not a diagnosis.
async fn follow_until(
    client: &mut Client,
    session: &str,
    what: &str,
    arrived: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut last_view = Vec::new();
    for _ in 0_u32..support::MAX_POLLS {
        let events = replayed_events(client, session).await;
        if arrived(&events) {
            return events;
        }
        last_view = events;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let kinds: Vec<&str> = last_view
        .iter()
        .map(|event| {
            event
                .kind
                .as_ref()
                .and_then(horde_proto::kind_field_name)
                .unwrap_or("?")
        })
        .collect();
    panic!(
        "{what} never arrived; chunks={:?}; kinds={kinds:?}",
        support::chunk_texts(&last_view)
    );
}

/// [`follow_until`] an output chunk says `text`.
async fn follow_until_chunk(client: &mut Client, session: &str, text: &str) {
    follow_until(client, session, &format!("the chunk {text:?}"), |events| {
        support::chunks_saying(events, text) >= 1
    })
    .await;
}

/// The full session history, as decoded events, through the client.
async fn replayed_events(client: &mut Client, session: &str) -> Vec<Event> {
    let mut events = Vec::new();
    client
        .sync(
            &SyncOptions::new(session),
            |envelope| {
                if let Some(event) = &envelope.event {
                    events.push(event.clone());
                }
            },
            |_| {},
        )
        .await
        .expect("the replay succeeds");
    events
}

#[tokio::test]
async fn a_server_killed_mid_append_restarts_on_its_log_and_everyone_reconciles() {
    let dirs = tempfile::tempdir().expect("a tempdir");
    let config = ServerConfig {
        database: dirs.path().join("horde.db"),
        state_dir: dirs.path().join("state"),
        // Fixed ports, so the restart answers where everyone already dials.
        bind: support::free_loopback_port().await,
        enrollment_bind: support::free_loopback_port().await,
    };

    // — First life: the real binary, from nothing. —
    let server = support::within(
        "the real server starts",
        ServerProcess::spawn(&config, true),
    )
    .await
    .expect("the server spawns and announces");
    let announced = server.announcement().clone();
    let bootstrap = announced
        .bootstrap_code
        .as_deref()
        .expect("a fresh server minted the first client code");

    // Enroll the operator client through the bootstrap code, and a worker
    // through a code that client mints — the production issuance paths.
    let client_identity = join_as(
        &announced.enrollment_endpoint(),
        bootstrap,
        PrincipalRole::Client,
        "fault-client",
    )
    .await;
    let mut client = connect_client(&announced.main_endpoint(), &client_identity).await;
    let minted = support::within(
        "the worker code mints",
        client.mint_enrollment_code(horde_proto::v1::Role::Worker, "worker-a", Some(5)),
    )
    .await
    .expect("the mint succeeds");
    let worker_identity = join_as(
        &announced.enrollment_endpoint(),
        &minted.code,
        PrincipalRole::Worker,
        "worker-a",
    )
    .await;
    let identity_dir = dirs.path().join("worker-identity");
    worker_identity
        .save(&identity_dir)
        .expect("the identity persists for the runner");

    let (session_summary, root) = support::within(
        "the session creates",
        client.create_session("fault-restart", "session-key"),
    )
    .await
    .expect("the session creates");
    let session = session_summary
        .session_id
        .as_ref()
        .map(ToString::to_string)
        .expect("a created session has an id");
    let root = root.to_string();

    // The worker: a real process, enrolled, dialing the fixed endpoint.
    // Turn bravo stalls, so its completion is guaranteed to happen while
    // the server is dead.
    let runner = WorkerRunnerConfig {
        endpoint: announced.main_endpoint(),
        worker_id: "worker-a".to_owned(),
        journal: dirs.path().join("journal.sqlite"),
        turns: vec![
            TurnSpec::answering("alpha-answer"),
            TurnSpec::opening_then_stalling("bravo-open", 1_500, "bravo-answer"),
            TurnSpec::answering("charlie-answer"),
        ],
        identity_dir: Some(identity_dir),
    };
    let worker = WorkerProcess::spawn(&runner).expect("the runner spawns");

    // A message to an unplaced agent is the scheduler's queue problem, not
    // the command stream's (DESIGN §6): the fold deliberately issues nothing
    // for it. So the client waits for the root to be *placed and started*
    // before assigning — which is also what a real client sees in its UI.
    follow_until(
        &mut client,
        &session,
        "the root agent's placement on the worker",
        |events| support::lifecycle_count(events, &root, "placed on this worker") >= 1,
    )
    .await;

    // A full healthy round trip first, so the kill interrupts a system in
    // flight rather than one still assembling.
    support::within(
        "the first assignment answers",
        client.send_assignment(
            &root,
            "do alpha",
            horde_proto::v1::DeliveryMode::TurnBoundary,
            "assign-alpha",
        ),
    )
    .await
    .expect("the assignment is acked");
    follow_until_chunk(&mut client, &session, "alpha-answer").await;

    // Bravo opens — its append is acked, its stall holds the turn — and the
    // kill lands mid-append, mid-turn, mid-everything.
    support::within(
        "the second assignment is acked",
        client.send_assignment(
            &root,
            "do bravo",
            horde_proto::v1::DeliveryMode::TurnBoundary,
            "assign-bravo",
        ),
    )
    .await
    .expect("the assignment is acked");
    follow_until_chunk(&mut client, &session, "bravo-open").await;
    server.kill_now().await.expect("the kill lands");

    // — Second life: same database, same state dir, same ports. The log on
    //   disk is the only thing connecting the two lives. —
    let server = support::within(
        "the server restarts on its surviving log",
        ServerProcess::spawn(&config, false),
    )
    .await
    .expect("the restart comes up");
    assert_eq!(
        server.announcement().main,
        announced.main,
        "the restart answers on the address everyone already dials"
    );

    // Clients resync: a fresh connection replays a consistent history —
    // everything acked before the kill is there, exactly once.
    let mut client = connect_client(&announced.main_endpoint(), &client_identity).await;
    let replayed = replayed_events(&mut client, &session).await;
    assert_eq!(
        support::chunks_saying(&replayed, "alpha-answer"),
        1,
        "acked history survives the kill exactly once"
    );
    let assignments = replayed
        .iter()
        .filter(|event| matches!(event.kind, Some(Kind::AssignmentSent(_))))
        .count();
    assert_eq!(assignments, 2, "both acked assignments survived");

    // Workers reconcile: the turn that finished in the dark arrives through
    // the runner's redial — delivered once, not re-run.
    follow_until_chunk(&mut client, &session, "bravo-answer").await;

    // And the system is whole: new work flows end to end.
    support::within(
        "the post-restart assignment is acked",
        client.send_assignment(
            &root,
            "do charlie",
            horde_proto::v1::DeliveryMode::TurnBoundary,
            "assign-charlie",
        ),
    )
    .await
    .expect("the assignment is acked");
    follow_until_chunk(&mut client, &session, "charlie-answer").await;

    let history = replayed_events(&mut client, &session).await;
    for marker in [
        "alpha-answer",
        "bravo-open",
        "bravo-answer",
        "charlie-answer",
    ] {
        assert_eq!(
            support::chunks_saying(&history, marker),
            1,
            "{marker} must appear exactly once across the kill and the replay"
        );
    }
    support::assert_usage_exactly_once(&history, "three turns across two server lives");
    assert_eq!(
        support::lifecycle_count(&history, &root, "placed on this worker"),
        1,
        "the worker reconciled; it did not re-run the spawn"
    );

    worker.kill_now().await.expect("the cleanup kill lands");
    server.kill_now().await.expect("the cleanup kill lands");
}
