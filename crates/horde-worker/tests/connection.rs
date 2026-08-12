//! The worker↔server connection, end to end over real sockets.
//!
//! A real `horde-server` serves `WorkerService` on a loopback TCP port, and a
//! real `horde-worker` dials it — the dependency points this way only
//! (`horde-server` knows nothing about the worker), the shape `horde-cli`'s
//! tests already use. TCP rather than a duplex pair because the partition test
//! needs a transport that can be *severed*: the [`harness::SeverableProxy`]
//! sits in front of the listener with `cut()`/`heal()`, and is the seed of
//! TESTING.md §2's fault injector.
//!
//! The two headline tests here are the ones the design documents by name:
//! `a_real_worker_reconciles_its_applied_commands_after_a_restart` (marked as
//! a seam in `horde-server/tests/services.rs`) and the partition → journal →
//! reconcile scenario from DESIGN.md §2.

use std::sync::Arc;

use horde_server::EventLog;
use horde_server::service::ServerCore;
use horde_worker::{Connection, Journal, Runtime};

/// Test-only fixtures, under `cfg(test)` so the `expect_used` exemption
/// applies to helpers as well as to `#[test]` fns.
#[cfg(test)]
mod harness {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use horde_adapter::Adapter;
    use horde_proto::prost::Message;
    use horde_proto::v1::worker_service_server::WorkerServiceServer;
    use horde_proto::v1::{
        AgentId, AgentSpawned, AgentSpec, Assignment, Event, Placement, PlacementChanged,
        PlacementState, TurnInterrupted, WorkerId, event::Kind,
    };
    use horde_server::EventLog;
    use horde_server::log::PendingEvent;
    use horde_server::service::{ServerCore, WorkerFacing};
    use horde_worker::{DialConfig, JournalEntry, Runtime, capabilities};
    use tokio::net::{TcpListener, TcpStream};
    use tokio_stream::wrappers::TcpListenerStream;

    /// Serves the worker-facing service on a loopback port the OS picks.
    ///
    /// `127.0.0.1` only: DESIGN.md §3 forbids binding wider before auth
    /// exists, and tests are not an exception to that.
    pub(crate) async fn serve(core: ServerCore) -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback port");
        let address = listener.local_addr().expect("a bound address");
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(WorkerServiceServer::new(WorkerFacing::new(core)))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        format!("http://{address}")
    }

    /// A TCP proxy that can be severed and healed — the fault the partition
    /// tests inject, and the seed of TESTING.md §2's network fault injector.
    ///
    /// `cut()` aborts every live relay (both sockets drop, so each side sees
    /// the peer vanish mid-stream, exactly like a yanked cable) and refuses
    /// new connections until `heal()`.
    pub(crate) struct SeverableProxy {
        endpoint: String,
        severed: Arc<AtomicBool>,
        live: Arc<std::sync::Mutex<Vec<tokio::task::AbortHandle>>>,
    }

    impl SeverableProxy {
        /// Starts a proxy in front of `upstream` (an `http://addr` endpoint).
        pub(crate) async fn front(upstream: &str) -> SeverableProxy {
            let upstream: SocketAddr = upstream
                .trim_start_matches("http://")
                .parse()
                .expect("an upstream socket address");
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("a loopback port");
            let address = listener.local_addr().expect("a bound address");

            let severed = Arc::new(AtomicBool::new(false));
            let live = Arc::new(std::sync::Mutex::new(Vec::new()));
            let accept_severed = Arc::clone(&severed);
            let accept_live = Arc::clone(&live);
            tokio::spawn(async move {
                loop {
                    let Ok((mut inbound, _)) = listener.accept().await else {
                        return;
                    };
                    if accept_severed.load(Ordering::SeqCst) {
                        // The network is down; the connection dies at once.
                        continue;
                    }
                    let relay = tokio::spawn(async move {
                        let Ok(mut outbound) = TcpStream::connect(upstream).await else {
                            return;
                        };
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    });
                    accept_live
                        .lock()
                        .expect("an unpoisoned proxy")
                        .push(relay.abort_handle());
                }
            });

            SeverableProxy {
                endpoint: format!("http://{address}"),
                severed,
                live,
            }
        }

        pub(crate) fn endpoint(&self) -> String {
            self.endpoint.clone()
        }

        /// Severs every live connection and refuses new ones.
        pub(crate) fn cut(&self) {
            self.severed.store(true, Ordering::SeqCst);
            for relay in self.live.lock().expect("an unpoisoned proxy").drain(..) {
                relay.abort();
            }
        }

        /// Lets new connections through again.
        pub(crate) fn heal(&self) {
            self.severed.store(false, Ordering::SeqCst);
        }
    }

    /// Appends the `AgentSpawned` + `PlacementChanged` pair the control plane
    /// writes when the scheduler places an agent — the two events the server's
    /// command fold turns into this worker's `SpawnAgentCommand`, with the
    /// implicit first assignment riding the spec.
    pub(crate) async fn place_agent(
        log: &EventLog,
        session: &str,
        agent: &str,
        worker: &str,
        first_message: &str,
    ) {
        let spawned = Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: format!("{agent}:spawned"),
            kind: Some(Kind::AgentSpawned(AgentSpawned {
                parent_agent_id: None,
                spawned_by: None,
                spec: Some(AgentSpec {
                    initial_assignment: Some(Assignment {
                        assignment_id: None,
                        text: first_message.to_owned(),
                        payload: None,
                    }),
                    ..AgentSpec::default()
                }),
                tree_path: Vec::new(),
            })),
        };
        let placed = Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: format!("{agent}:placed"),
            kind: Some(Kind::PlacementChanged(PlacementChanged {
                placement: Some(Placement {
                    agent_id: Some(AgentId::new(agent)),
                    state: PlacementState::Placed.into(),
                    worker_id: Some(WorkerId::new(worker)),
                    reason: String::new(),
                }),
            })),
        };
        for event in [spawned, placed] {
            log.append(PendingEvent {
                session_id: session.to_owned(),
                idempotency_key: event.idempotency_key.clone(),
                payload: event.encode_to_vec(),
                ..PendingEvent::default()
            })
            .await
            .expect("the seed event appends");
        }
    }

    /// Dials as `config` and runs the connection over `journal`, driving
    /// `runtime` — the whole worker, in one line of test.
    ///
    /// Returns the running task (abort it to crash the worker) and the
    /// registration, whose resume positions are what reconciliation tests
    /// assert on.
    pub(crate) async fn dial_and_run(
        config: &DialConfig,
        journal: &horde_worker::Journal,
        runtime: Runtime,
    ) -> (
        tokio::task::JoinHandle<Result<(), horde_worker::WorkerError>>,
        horde_proto::v1::RegisterWorkerResponse,
    ) {
        let (mut connection, registration) = horde_worker::Connection::dial(config)
            .await
            .expect("the dial succeeds");
        let journal = journal.clone();
        let task = tokio::spawn(async move {
            let mut runtime = runtime;
            connection.run(&journal, &mut runtime).await
        });
        (task, registration)
    }

    /// A dial config advertising this runtime's adapters.
    pub(crate) fn config_for(endpoint: &str, worker_id: &str, runtime: &Runtime) -> DialConfig {
        DialConfig {
            endpoint: endpoint.to_owned(),
            worker_id: worker_id.to_owned(),
            hello: capabilities::hello(worker_id),
            capabilities: capabilities::advertise(runtime.adapters()),
        }
    }

    /// A dial config for a worker with no adapters at all.
    pub(crate) fn bare_config(endpoint: &str, worker_id: &str) -> DialConfig {
        DialConfig {
            endpoint: endpoint.to_owned(),
            worker_id: worker_id.to_owned(),
            hello: capabilities::hello(worker_id),
            capabilities: capabilities::advertise(std::iter::empty::<&dyn Adapter>()),
        }
    }

    /// An event the sink journals, with a marker that survives to the server.
    pub(crate) fn marker_entry(session: &str, marker: &str) -> JournalEntry {
        JournalEntry {
            session_id: session.to_owned(),
            agent_id: Some("agent-1".to_owned()),
            // Empty: exercises the journal's auto-minted keys on the same
            // path the MCP endpoint will use.
            idempotency_key: String::new(),
            kind: Kind::TurnInterrupted(TurnInterrupted {
                requested_by: None,
                turn_id: String::new(),
                reason: marker.to_owned(),
            }),
        }
    }

    /// Waits until `session`'s server log reaches `minimum`, returning the
    /// head it reached. Bounded by poll count and by a ceiling per park.
    pub(crate) async fn server_log_at_least(log: &EventLog, session: &str, minimum: u64) -> u64 {
        let mut notifications = log.subscribe();
        let mut polls = 0_u32;
        loop {
            let head = log.head(session).await.expect("a readable log");
            if head >= minimum {
                return head;
            }
            polls += 1;
            assert!(polls < 256, "the server log stalled below {minimum}");
            let _ = tokio::time::timeout(core::time::Duration::from_secs(5), notifications.recv())
                .await
                .expect("the server log must keep moving");
        }
    }

    /// Waits until the worker journal's truncation floor reaches `floor` —
    /// which only an ack from the server can cause. Bounded by poll count.
    pub(crate) async fn journal_truncated_to(journal: &horde_worker::Journal, floor: u64) {
        for _ in 0_u32..250 {
            if let Err(horde_worker::WorkerError::TruncatedPast { floor: reached, .. }) =
                journal.read_from(1, 1).await
                && reached >= floor
            {
                return;
            }
            tokio::time::sleep(core::time::Duration::from_millis(20)).await;
        }
        panic!("the journal never truncated to {floor}; no ack can have arrived");
    }

    /// Every event of `session`'s server log, decoded.
    pub(crate) async fn decoded_session_log(log: &EventLog, session: &str) -> Vec<Event> {
        log.read_all(session)
            .await
            .expect("a readable log")
            .iter()
            .map(|stored| Event::decode(stored.payload.as_slice()).expect("a decodable event"))
            .collect()
    }

    /// Waits until the decoded session log satisfies `arrived`, returning it.
    ///
    /// The live smoke's waiter: a real harness produces an unknowable number
    /// of events, so the bound is a predicate plus a poll-count ceiling with a
    /// per-park timeout — generous because a real model is on the other end.
    pub(crate) async fn server_log_until(
        log: &EventLog,
        session: &str,
        what: &str,
        arrived: impl Fn(&[Event]) -> bool,
    ) -> Vec<Event> {
        let mut notifications = log.subscribe();
        for _ in 0_u32..240 {
            let events = decoded_session_log(log, session).await;
            if arrived(&events) {
                return events;
            }
            let _ = tokio::time::timeout(core::time::Duration::from_secs(1), notifications.recv())
                .await;
        }
        panic!("{what}");
    }
}

/// The seam named in `horde-server/tests/services.rs`: a real worker applies a
/// real spawn command, crashes without ceremony, and its next life reconciles
/// what it durably applied against what the server would replay — re-running
/// nothing, renumbering nothing.
#[tokio::test]
async fn a_real_worker_reconciles_its_applied_commands_after_a_restart() {
    use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn};

    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = harness::serve(core.clone()).await;
    let dir = tempfile::tempdir().expect("a tempdir");
    let journal_path = dir.path().join("journal.sqlite");

    // The scheduler places agent-1 here, with its implicit first assignment.
    harness::place_agent(&core.log, "s1", "agent-1", "worker-a", "say hello").await;

    // First life: apply the spawn, run the turn, push and get acked.
    {
        let journal = Journal::open(&journal_path).expect("a fresh journal");
        let runtime = Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(
            Script::of(vec![ScriptedTurn::saying("hello")]),
        )));
        let config = harness::config_for(&endpoint, "worker-a", &runtime);
        let (run, _) = harness::dial_and_run(&config, &journal, runtime).await;

        // Two seed events, then the worker's four: starting, established,
        // one output chunk, one usage record.
        let head = harness::server_log_at_least(&core.log, "s1", 6).await;
        assert_eq!(head, 6, "exactly the four worker events, no duplicates");

        // The ack came back and the journal truncated on it.
        harness::journal_truncated_to(&journal, 5).await;

        // A fully synced worker parks; a finished run task means a pump or
        // cursor defect errored out quietly — the kind of mutant that
        // otherwise passes every functional assertion while spinning.
        assert!(!run.is_finished(), "the synced connection must stay parked");

        // Crash: no shutdown, no goodbye. The journal file is what survives.
        run.abort();
    }

    // While the worker was down, the scheduler placed a second agent on it.
    harness::place_agent(&core.log, "s1", "agent-2", "worker-a", "say more").await;

    // Second life, over the same journal file and nothing else.
    let journal = Journal::open(&journal_path).expect("the same journal");
    assert_eq!(
        journal
            .applied_command_sequence()
            .await
            .expect("a watermark"),
        1,
        "the first life durably applied command 1"
    );
    assert_eq!(
        journal.last_local_sequence().await.expect("a head"),
        4,
        "and journaled four events before dying"
    );

    let runtime =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("more"),
        ]))));
    let config = harness::config_for(&endpoint, "worker-a", &runtime);
    let (run, registration) = harness::dial_and_run(&config, &journal, runtime).await;
    assert_eq!(
        registration.resume_from_journal_sequence, 5,
        "the server remembered everything the first life pushed"
    );

    // Only agent-2's spawn is replayed: its two seed events plus its four
    // worker events, on top of the six already there.
    let head = harness::server_log_at_least(&core.log, "s1", 12).await;
    assert_eq!(head, 12, "nothing about agent-1 was re-run or re-pushed");

    assert_eq!(
        journal
            .applied_command_sequence()
            .await
            .expect("a watermark"),
        2,
        "the second life applied only the command it had not applied"
    );
    assert_eq!(
        journal.last_local_sequence().await.expect("a head"),
        8,
        "numbering continued across the restart; nothing was renumbered"
    );

    // The sharpest form of "re-ran nothing": agent-1 started exactly once in
    // the whole history.
    let events = harness::decoded_session_log(&core.log, "s1").await;
    let agent_1_starts = events
        .iter()
        .filter(|event| {
            event.agent_id.as_ref().map(ToString::to_string) == Some("agent-1".to_owned())
                && matches!(
                    &event.kind,
                    Some(horde_proto::v1::event::Kind::AgentLifecycleChanged(changed))
                        if changed.reason == "placed on this worker"
                )
        })
        .count();
    assert_eq!(agent_1_starts, 1, "a restart must not respawn applied work");

    assert!(
        !run.is_finished(),
        "the reconciled connection must stay parked, not die quietly"
    );
    run.abort();
}

/// DESIGN.md §2's partition behavior, held over a real severed transport: a
/// worker that loses the server keeps journaling locally, and reconciliation
/// on reconnect delivers exactly what the server missed — no gaps, no
/// duplicates, no renumbering.
#[tokio::test]
async fn a_partitioned_worker_journals_locally_and_reconciles_on_reconnect() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let upstream = harness::serve(core.clone()).await;
    let proxy = harness::SeverableProxy::front(&upstream).await;
    let journal = Journal::in_memory().expect("an in-memory journal");
    let sink = journal.sink();

    // Connected: one event flows end to end.
    let config = harness::bare_config(&proxy.endpoint(), "worker-a");
    sink.record(harness::marker_entry("s1", "before the cut"))
        .await
        .expect("the journal accepts");
    let (run, _) = harness::dial_and_run(&config, &journal, Runtime::new(journal.clone())).await;
    harness::server_log_at_least(&core.log, "s1", 1).await;

    // The cable is yanked mid-connection.
    proxy.cut();
    let outcome = tokio::time::timeout(core::time::Duration::from_secs(10), run)
        .await
        .expect("a severed connection must surface, not hang")
        .expect("the run task joins");
    drop(outcome); // Err(transport) or Ok(server hung up): both are detection.

    // Partitioned: the worker keeps journaling. Anything requiring the
    // server would block only the agent that needs it — the journal never
    // does.
    sink.record(harness::marker_entry("s1", "during the cut 1"))
        .await
        .expect("a partitioned journal still accepts");
    sink.record(harness::marker_entry("s1", "during the cut 2"))
        .await
        .expect("a partitioned journal still accepts");
    assert_eq!(
        core.log.head("s1").await.expect("a readable log"),
        1,
        "nothing can have reached the server through a severed proxy"
    );

    // Heal, redial, reconcile.
    proxy.heal();
    let (run, registration) =
        harness::dial_and_run(&config, &journal, Runtime::new(journal.clone())).await;
    assert_eq!(
        registration.resume_from_journal_sequence, 2,
        "the server asks for exactly what it missed"
    );

    let head = harness::server_log_at_least(&core.log, "s1", 3).await;
    assert_eq!(
        head, 3,
        "exactly the two partition events arrived, once each"
    );

    let events = harness::decoded_session_log(&core.log, "s1").await;
    let markers: Vec<String> = events
        .iter()
        .filter_map(|event| match &event.kind {
            Some(horde_proto::v1::event::Kind::TurnInterrupted(interrupted)) => {
                Some(interrupted.reason.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        markers,
        vec!["before the cut", "during the cut 1", "during the cut 2"],
        "order survives the partition"
    );

    // And the journal was told: the ack truncates through the healed link.
    harness::journal_truncated_to(&journal, 4).await;
    assert!(
        !run.is_finished(),
        "the reconciled connection must stay parked, not die quietly"
    );
    run.abort();
}

/// The live half of the seam: the same wire path as everything above, driven
/// through the **real** `claude` binary instead of the fake. Manual only —
/// costs tokens, needs a login, and runs on the harness's default model
/// because the command fold does not carry a binding yet.
///
/// ```sh
/// cargo test -p horde-worker --test connection -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "spends real tokens and needs a claude login"]
async fn the_real_claude_path_streams_a_turn_into_the_server_log() {
    use horde_proto::v1::event::Kind;

    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let endpoint = harness::serve(core.clone()).await;
    harness::place_agent(
        &core.log,
        "s1",
        "agent-1",
        "worker-a",
        "Reply with exactly the token HORDE_WORKER_OK and nothing else.",
    )
    .await;

    let journal = Journal::in_memory().expect("an in-memory journal");
    let runtime =
        Runtime::new(journal.clone()).with_adapter(Arc::new(horde_worker::ClaudeAdapter::new()));
    let config = harness::config_for(&endpoint, "worker-a", &runtime);
    let (run, _) = harness::dial_and_run(&config, &journal, runtime).await;

    let events = harness::server_log_until(
        &core.log,
        "s1",
        "the live turn never recorded its usage in the server log",
        |events| {
            events.iter().any(|event| {
                matches!(&event.kind, Some(Kind::UsageRecorded(recorded))
                    if recorded.record.as_ref().is_some_and(|record| !record.turn_id.is_empty()))
            })
        },
    )
    .await;

    let streamed: String = events
        .iter()
        .filter_map(|event| match &event.kind {
            Some(Kind::AgentOutputChunk(chunk)) => Some(chunk.text.clone()),
            _ => None,
        })
        .collect();
    assert!(
        streamed.contains("HORDE_WORKER_OK"),
        "the answer must arrive through the journal and the push pump: {streamed}"
    );
    assert!(
        events.iter().any(|event| matches!(&event.kind,
            Some(Kind::AgentLifecycleChanged(changed))
                if changed.reason.contains("provider session"))),
        "the established-session lifecycle must reach the server"
    );

    run.abort();
}

/// The positive control for the fault injector itself: a proxy that cannot be
/// shown to sever proves nothing about partition behavior.
#[tokio::test]
async fn the_proxy_severs_on_cut_and_carries_again_on_heal() {
    let core = ServerCore::new(EventLog::in_memory().expect("an in-memory log"));
    let upstream = harness::serve(core.clone()).await;
    let proxy = harness::SeverableProxy::front(&upstream).await;
    let config = harness::bare_config(&proxy.endpoint(), "worker-a");

    Connection::dial(&config)
        .await
        .expect("an intact proxy carries the dial");

    proxy.cut();
    let severed = Connection::dial(&config).await;
    assert!(
        severed.is_err(),
        "a dial through a severed proxy must fail, or cut() proves nothing"
    );

    proxy.heal();
    Connection::dial(&config)
        .await
        .expect("a healed proxy carries the dial again");
}
