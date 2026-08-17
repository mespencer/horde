//! The fault suite's toolbox: bounded waits, log inspection, and the
//! in-process worker supervisor the network-only scenarios drive.
//!
//! Everything here is observation and arrangement. Nothing reimplements a
//! production decision: seeding goes through `horde_server_testkit`, the
//! worker is the production `Runtime`/`Connection`, and the log is read
//! back through `EventLog` itself.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use horde_proto::prost::Message;
use horde_proto::v1::event::Kind;
use horde_proto::v1::{Event, PlacementState};
use horde_server::EventLog;
use horde_worker::{Connection, DialConfig, Journal, Runtime, capabilities};

/// Every wait's ceiling. Process scenarios cover binary startup and redial
/// loops, so this is generous — and still finite, because this suite exists
/// to create the conditions for hangs.
pub(crate) const CEILING: Duration = Duration::from_secs(30);

/// How many parks a poll loop may take before it is a hang, independent of
/// the clock: progress is bounded by count, per QUALITY_GATES.md.
pub(crate) const MAX_POLLS: u32 = 600;

/// Runs `future` under [`CEILING`], failing the test by name if it hangs.
pub(crate) async fn within<F: Future>(what: &str, future: F) -> F::Output {
    match tokio::time::timeout(CEILING, future).await {
        Ok(output) => output,
        Err(_) => panic!("{what} must finish rather than hang"),
    }
}

/// Every event of `session`'s server log, decoded.
pub(crate) async fn decoded_log(log: &EventLog, session: &str) -> Vec<Event> {
    let stored = log.read_all(session).await.expect("a readable log");
    stored
        .iter()
        .map(|row| Event::decode(row.payload.as_slice()).expect("a decodable event"))
        .collect()
}

/// Polls `session`'s log until `arrived` holds, parking on the log's own
/// notifications between reads. Bounded by [`MAX_POLLS`] and a per-park
/// ceiling; panics with `what` when the log stops moving first.
pub(crate) async fn log_until(
    log: &EventLog,
    session: &str,
    what: &str,
    arrived: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut notifications = log.subscribe();
    let mut last_view = Vec::new();
    for _ in 0_u32..MAX_POLLS {
        let events = decoded_log(log, session).await;
        if arrived(&events) {
            return events;
        }
        last_view = events;
        let _ = tokio::time::timeout(Duration::from_millis(250), notifications.recv()).await;
    }
    let kinds: Vec<String> = last_view
        .iter()
        .map(|event| {
            let kind = event
                .kind
                .as_ref()
                .and_then(horde_proto::kind_field_name)
                .unwrap_or("?");
            let agent = event
                .agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default();
            format!("{agent}:{kind}")
        })
        .collect();
    panic!(
        "{what} (after {MAX_POLLS} polls); chunks={:?}; kinds={kinds:?}",
        chunk_texts(&last_view)
    );
}

/// The texts of every assistant-visible output chunk, in log order.
pub(crate) fn chunk_texts(events: &[Event]) -> Vec<String> {
    let mut texts = Vec::new();
    for event in events {
        if let Some(Kind::AgentOutputChunk(chunk)) = &event.kind {
            texts.push(chunk.text.clone());
        }
    }
    texts
}

/// How many output chunks say exactly `text` — the duplicate detector for
/// marker chunks.
pub(crate) fn chunks_saying(events: &[Event], text: &str) -> usize {
    chunk_texts(events).iter().filter(|t| *t == text).count()
}

/// How many usage records each turn id carries. "Usage counted exactly once"
/// is every value in this map being 1.
pub(crate) fn usage_counts_by_turn(events: &[Event]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        if let Some(Kind::UsageRecorded(recorded)) = &event.kind
            && let Some(record) = &recorded.record
        {
            *counts.entry(record.turn_id.clone()).or_insert(0) += 1;
        }
    }
    counts
}

/// Asserts that no completed turn billed twice — and that at least one
/// billed at all, so a log with usage silently missing cannot pass.
pub(crate) fn assert_usage_exactly_once(events: &[Event], context: &str) {
    let counts = usage_counts_by_turn(events);
    assert!(
        !counts.is_empty(),
        "{context}: no usage was recorded at all, so 'exactly once' is vacuous"
    );
    for (turn, count) in &counts {
        assert_eq!(
            *count, 1,
            "{context}: turn {turn} billed {count} times: {counts:?}"
        );
    }
}

/// How many lifecycle transitions `agent` journaled whose reason contains
/// `reason` — "started exactly once" is this being 1 for "placed on this
/// worker".
pub(crate) fn lifecycle_count(events: &[Event], agent: &str, reason: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            event.agent_id.as_ref().map(ToString::to_string) == Some(agent.to_owned())
                && matches!(
                    &event.kind,
                    Some(Kind::AgentLifecycleChanged(changed)) if changed.reason.contains(reason)
                )
        })
        .count()
}

/// `agent`'s placement history, in log order.
pub(crate) fn placement_states(events: &[Event], agent: &str) -> Vec<PlacementState> {
    events
        .iter()
        .filter_map(|event| match &event.kind {
            Some(Kind::PlacementChanged(changed)) => changed
                .placement
                .as_ref()
                .filter(|placement| {
                    placement.agent_id.as_ref().map(ToString::to_string) == Some(agent.to_owned())
                })
                .map(|placement| placement.state()),
            _ => None,
        })
        .collect()
}

/// Runs an in-process worker the way the worker-runner binary does: dial,
/// run, redial — over the same journal and the same `Runtime`, so agents and
/// resume positions survive every reconnect. Abort the handle to end it.
pub(crate) fn supervise_worker(
    endpoint: String,
    worker_id: String,
    journal: Journal,
    runtime: Runtime,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut runtime = runtime;
        for _ in 0_u32..MAX_POLLS {
            let config = DialConfig::insecure(
                endpoint.clone(),
                worker_id.clone(),
                capabilities::hello(&worker_id),
                capabilities::advertise(
                    runtime.adapters(),
                    runtime.workspaces().map(|_| Vec::new()),
                ),
            );
            if let Ok((mut connection, _)) = Connection::dial(&config).await {
                let _ = connection.run(&journal, &mut runtime).await;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

/// Polls the worker journal until `arrived` holds over its decoded events —
/// the partitioned-side counterpart of [`log_until`]. Acked events truncate
/// away, so the read chases the floor rather than expecting sequence one to
/// still exist.
pub(crate) async fn journal_until(
    journal: &Journal,
    what: &str,
    arrived: impl Fn(&[Event]) -> bool,
) -> Vec<Event> {
    let mut from = 1_u64;
    for _ in 0_u32..MAX_POLLS {
        match journal.read_from(from, 1024).await {
            Ok(journaled) => {
                let events: Vec<Event> = journaled
                    .iter()
                    .filter_map(|entry| entry.event.clone())
                    .collect();
                if arrived(&events) {
                    return events;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(horde_worker::WorkerError::TruncatedPast { floor, .. }) => {
                from = floor.max(1);
            }
            Err(other) => panic!("the worker journal must stay readable: {other}"),
        }
    }
    panic!("{what} (after {MAX_POLLS} polls of the worker journal)");
}

/// A loopback port the OS just handed out — bound, read, released — so a
/// restarted server can come back on the address its peers already dial.
pub(crate) async fn free_loopback_port() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    listener.local_addr().expect("a bound address")
}

/// Deterministic filler for blob payloads: patterned, not random, so a rerun
/// is byte-for-byte the same transfer.
pub(crate) fn patterned_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

/// A `ServerCore` over its own in-memory log.
pub(crate) fn fresh_core() -> horde_server::ServerCore {
    horde_server::ServerCore::new(EventLog::in_memory().expect("an in-memory log"))
}

/// A served core fronted by the injector — every network scenario's opening
/// move: real server spines on a real socket, with the proxy in the path.
pub(crate) async fn proxied_core() -> (horde_server::ServerCore, horde_testkit::FaultProxy) {
    let core = fresh_core();
    let upstream = horde_server_testkit::serve_core(core.clone())
        .await
        .expect("the core serves");
    let proxy = horde_testkit::FaultProxy::front(&upstream)
        .await
        .expect("the proxy binds");
    (core, proxy)
}

/// Appends one already-shaped event to `session`, keyed by the event's own
/// idempotency key — the arrangement half of "an observed fact is on the
/// log", shared so the suites do not each restate the append envelope.
pub(crate) async fn append_event(log: &EventLog, session: &str, event: Event) {
    let pending = horde_server::log::PendingEvent {
        session_id: session.to_owned(),
        idempotency_key: event.idempotency_key.clone(),
        payload: event.encode_to_vec(),
        ..horde_server::log::PendingEvent::default()
    };
    log.append(pending).await.expect("the event appends");
}

/// A runtime driving one scripted FakeAdapter over `journal`.
pub(crate) fn scripted_runtime(journal: &Journal, script: horde_adapter_fake::Script) -> Runtime {
    Runtime::new(journal.clone())
        .with_adapter(Arc::new(horde_adapter_fake::FakeAdapter::new(script)))
}
