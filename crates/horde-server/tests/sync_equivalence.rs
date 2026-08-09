//! The property ADR-0001 rests on: **sync-from-offset ≡ full replay**.
//!
//! Every resilience feature Horde claims — reconnect, attach, scrollback,
//! crash recovery — is the same call with a different offset. If a client that
//! resumes from the middle can observe a different history than one that reads
//! from the beginning, all of them are broken at once, and the failure is
//! silent: the client simply never learns about an event.
//!
//! These are property tests rather than examples because the interesting cases
//! are interleavings, and interleavings are exactly what a hand-written test
//! fails to imagine. The generators below produce arbitrary sequences of
//! appends and reconnects across several sessions and several simulated
//! workers.
//!
//! Each test builds its own current-thread runtime rather than using
//! `#[tokio::test]`, because proptest drives the closure synchronously.

use horde_server::EventLog;
use horde_server::log::{PendingEvent, StoredEvent};
use proptest::prelude::*;

/// One thing a generated scenario does.
#[derive(Debug, Clone)]
enum Step {
    /// Append to session `session % sessions`.
    Append { session: usize, worker: usize },
    /// Drop the follower and resume from where it had got to.
    Reconnect,
}

fn step_strategy(sessions: usize, workers: usize) -> impl Strategy<Value = Step> {
    prop_oneof![
        8 => (0..sessions, 0..workers)
            .prop_map(|(session, worker)| Step::Append { session, worker }),
        1 => Just(Step::Reconnect),
    ]
}

fn scenario() -> impl Strategy<Value = (usize, usize, Vec<Step>)> {
    (1_usize..4, 1_usize..4).prop_flat_map(|(sessions, workers)| {
        (
            Just(sessions),
            Just(workers),
            prop::collection::vec(step_strategy(sessions, workers), 1..40),
        )
    })
}

/// Returns the runtime rather than unwrapping it here: `expect` is only allowed
/// inside `#[test]` functions, and clippy's in-test allowance does not reach
/// free helpers in an integration test file.
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

fn session_name(index: usize) -> String {
    format!("session-{index}")
}

/// A worker-originated event, numbered in that worker's own journal space.
fn worker_event(session: &str, worker: usize, journal_sequence: u64) -> PendingEvent {
    PendingEvent {
        session_id: session.to_owned(),
        origin_worker_id: format!("worker-{worker}"),
        journal_sequence: Some(journal_sequence),
        idempotency_key: String::new(),
        payload: format!("{worker}:{journal_sequence}").into_bytes(),
    }
}

fn sequences(events: &[StoredEvent]) -> Vec<u64> {
    events.iter().map(|event| event.sequence).collect()
}

fn payloads(events: &[StoredEvent]) -> Vec<Vec<u8>> {
    events.iter().map(|event| event.payload.clone()).collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    /// A follower that starts at an arbitrary offset, is dropped and resumed
    /// arbitrarily often, and reads the rest live, must end up with exactly the
    /// suffix a full replay would have given it — same events, same order, no
    /// gaps, no repeats.
    #[test]
    fn resuming_from_any_offset_yields_the_same_suffix_as_a_full_replay(
        (session_count, worker_count, steps) in scenario(),
        start_offset in 1_u64..6,
    ) {
        let runtime = runtime().expect("a current-thread runtime must build");
        runtime.block_on(async move {
            let log = EventLog::in_memory().expect("in-memory log");
            let watched = session_name(0);

            // Subscribe before anything is written, exactly as a live client
            // would, then follow across reconnects.
            let (history, mut tail) = log.tail(&watched, start_offset).await.expect("tail");
            let mut observed = history;

            let mut journal = vec![0_u64; worker_count];

            for step in steps {
                match step {
                    Step::Append { session, worker } => {
                        journal[worker] = journal[worker].saturating_add(1);
                        let target = session_name(session % session_count);
                        log.append(worker_event(&target, worker, journal[worker]))
                            .await
                            .expect("append");
                    }
                    Step::Reconnect => {
                        // A reconnect is just "tail again from where I got to",
                        // which is the whole point of the offset design. Doing it
                        // while notifications are still buffered is deliberate:
                        // that is the state a real client reconnects in.
                        let resume = tail.delivered().saturating_add(1);
                        let (catch_up, resumed) =
                            log.tail(&watched, resume).await.expect("re-tail");
                        observed.extend(catch_up);
                        tail = resumed;
                    }
                }
            }

            // Drain to the known head. Count-driven rather than timeout-driven:
            // the head is a fact, so waiting on it is deterministic and costs no
            // wall-clock time when there is nothing outstanding.
            // Bounded, and bounded *tightly*: proptest runs this case ~96 times,
            // so a generous per-wait timeout multiplies into a suite that hangs
            // instead of reporting. Everything here is in-memory, so half a
            // second is already orders of magnitude of headroom.
            let head = log.head(&watched).await.expect("head");
            while observed.last().map_or(start_offset.saturating_sub(1), |event| event.sequence)
                < head
            {
                let batch = tokio::time::timeout(
                    std::time::Duration::from_millis(500),
                    tail.next_batch(),
                )
                .await
                .map_err(|_| TestCaseError::fail("the tail stalled below the known head"))?
                .ok_or_else(|| TestCaseError::fail("the tail closed before reaching the head"))?
                .map_err(|error| TestCaseError::fail(format!("tail read failed: {error}")))?;
                observed.extend(batch);
            }

            let full = log.read_all(&watched).await.expect("full replay");
            let expected: Vec<StoredEvent> = full
                .into_iter()
                .filter(|event| event.sequence >= start_offset)
                .collect();

            prop_assert_eq!(
                sequences(&observed),
                sequences(&expected),
                "a resumed follower must observe exactly the suffix a full replay contains"
            );
            prop_assert_eq!(payloads(&observed), payloads(&expected));
            Ok(())
        })?;
    }

    /// Sequences within a session must be dense and 1-based no matter how many
    /// workers appended concurrently. A gap would make "resume from N+1"
    /// silently skip an event; a repeat would make replay ambiguous.
    #[test]
    fn sequences_stay_dense_under_concurrent_appends_from_many_workers(
        (session_count, worker_count, steps) in scenario(),
    ) {
        let runtime = runtime().expect("a current-thread runtime must build");
        runtime.block_on(async move {
            let log = EventLog::in_memory().expect("in-memory log");

            // Fire every append concurrently so they genuinely contend for the
            // head of their session.
            let mut journal = vec![0_u64; worker_count];
            let mut handles = Vec::new();

            for step in steps {
                let Step::Append { session, worker } = step else {
                    continue;
                };
                journal[worker] = journal[worker].saturating_add(1);
                let target = session_name(session % session_count);
                let event = worker_event(&target, worker, journal[worker]);
                let log = log.clone();
                handles.push(tokio::spawn(async move { log.append(event).await }));
            }

            let mut appended = 0_usize;
            for handle in handles {
                handle.await.expect("append task").expect("append");
                appended += 1;
            }

            let mut total = 0_usize;
            for index in 0..session_count {
                let name = session_name(index);
                let events = log.read_all(&name).await.expect("replay");
                total += events.len();

                let expected: Vec<u64> = (1..=events.len() as u64).collect();
                prop_assert_eq!(
                    sequences(&events),
                    expected,
                    "session {} must be dense and 1-based",
                    name
                );
            }

            prop_assert_eq!(total, appended, "every append must appear exactly once");
            Ok(())
        })?;
    }

    /// A worker that replays its journal after a partition must not duplicate
    /// anything. Replaying from an arbitrary earlier point is the realistic
    /// case: the worker only knows what the server acked, which may lag.
    #[test]
    fn replaying_a_worker_journal_never_duplicates_events(
        appends in 1_usize..12,
        replay_from in 1_usize..12,
    ) {
        let runtime = runtime().expect("a current-thread runtime must build");
        runtime.block_on(async move {
            let log = EventLog::in_memory().expect("in-memory log");
            let session = session_name(0);

            for journal_sequence in 1..=appends as u64 {
                log.append(worker_event(&session, 0, journal_sequence))
                    .await
                    .expect("append");
            }
            let head_before = log.head(&session).await.expect("head");

            // The worker reconnects and replays from a position it believed was
            // unacked. Everything at or below the true watermark is a duplicate.
            for journal_sequence in (replay_from.min(appends) as u64)..=appends as u64 {
                let appended = log
                    .append(worker_event(&session, 0, journal_sequence))
                    .await
                    .expect("replayed append");
                prop_assert!(
                    appended.deduplicated,
                    "journal sequence {} was already stored and must be deduplicated",
                    journal_sequence
                );
            }

            prop_assert_eq!(
                log.head(&session).await.expect("head"),
                head_before,
                "a replayed journal must not advance the session log"
            );
            prop_assert_eq!(
                log.worker_journal_watermark("worker-0").await.expect("watermark"),
                appends as u64
            );
            Ok(())
        })?;
    }
}

/// `tail`'s postcondition under concurrent writes: the history it returns is
/// contiguous from the requested offset, and the tail resumes exactly at its
/// end.
///
/// **Labelled limitation.** This does *not* discriminate the subscribe-window
/// hazard described in `log`'s module docs. That window is only observable when
/// the final append of a session lands between the replay read and the
/// subscribe, and no arrangement of this test lost that race across 250 rounds
/// on a 4-thread runtime — removing either the ordering or the sweep leaves it
/// green. It is kept because contiguity and tail positioning are real
/// invariants it does catch, and it is documented rather than deleted so nobody
/// reads it as proof the window is covered. Closing that gap needs an
/// injectable seam in `tail`, which is wave-3 work rather than something to
/// fake here (docs/TESTING.md §1: a test that cannot yet reach a defect ships
/// labelled).
#[test]
fn tail_returns_a_history_that_reaches_the_head_it_observed() {
    const ROUNDS: usize = 200;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("a multi-thread runtime must build");

    runtime.block_on(async {
        for round in 0..ROUNDS {
            let log = EventLog::in_memory().expect("in-memory log");
            let session = session_name(0);

            for journal_sequence in 1..=8_u64 {
                log.append(worker_event(&session, 0, journal_sequence))
                    .await
                    .expect("append");
            }

            // Write concurrently with the replay, so the sweep has something to
            // find on at least some rounds.
            let writer = log.clone();
            let target = session.clone();
            let writing =
                tokio::spawn(async move { writer.append(worker_event(&target, 0, 9)).await });

            let (history, tail) = log.tail(&session, 1).await.expect("tail");
            writing.await.expect("writer task").expect("append");

            // Whatever the replay saw, it must be contiguous from 1 and the tail
            // must be positioned exactly at its end.
            let observed = sequences(&history);
            let expected: Vec<u64> = (1..=observed.len() as u64).collect();
            assert_eq!(
                observed, expected,
                "round {round}: history must be contiguous"
            );
            assert_eq!(
                tail.delivered(),
                observed.last().copied().unwrap_or(0),
                "round {round}: the tail must resume exactly where the history ended"
            );
            assert!(
                observed.len() >= 8,
                "round {round}: the replay must at least cover what existed before it started"
            );
        }
    });
}
