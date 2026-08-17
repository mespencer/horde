//! Scenario (a): a partition lands in the middle of an active turn.
//!
//! DESIGN.md §2's claim under attack: "a worker that loses the server keeps
//! its agents running and journals events locally, syncing on reconnect."
//! The worker suite already proves the bare journal reconciles; this
//! scenario cuts the cable while a *turn is in flight* and holds the cut
//! until the whole rest of the turn — output, turn end, usage — exists only
//! in the worker's journal. The recovery under assertion: after heal, the
//! server log contains every event exactly once, in order, and the turn's
//! usage is billed exactly once. Idempotency keys, not luck, are what a
//! reconnect replay leans on, because the lock-step pump can lose at most
//! one unacked batch and re-sends it whole.

use std::time::Duration;

use horde_adapter_fake::{Script, ScriptedTurn, Step};
use horde_proto::v1::event::Kind;
use horde_worker::Journal;

use crate::support;

/// One scripted turn that announces itself, stalls mid-flight long enough
/// for a fault to land deterministically inside it, then finishes.
fn stalling_turn(opener: &str, tail: &str, answer: &str) -> Script {
    Script::of(vec![ScriptedTurn {
        steps: vec![
            Step::Say(opener.to_owned()),
            Step::Stall(Duration::from_millis(400)),
            Step::Say(tail.to_owned()),
        ],
        ..ScriptedTurn::saying(answer)
    }])
}

/// Both tests' opening move: a proxied server with agent-1 placed on
/// worker-a, and an in-process worker supervising `script` through the
/// proxy. Returns the stage: core, proxy, the worker's journal, and the
/// supervisor handle to abort.
async fn staged_worker(
    script: Script,
) -> (
    horde_server::ServerCore,
    horde_testkit::FaultProxy,
    Journal,
    tokio::task::JoinHandle<()>,
) {
    let (core, proxy) = support::proxied_core().await;
    horde_server_testkit::place_agent(&core.log, "s1", "agent-1", "worker-a", "begin")
        .await
        .expect("the seed appends");
    let journal = Journal::in_memory().expect("an in-memory journal");
    let runtime = support::scripted_runtime(&journal, script);
    let worker = support::supervise_worker(
        proxy.endpoint(),
        "worker-a".to_owned(),
        journal.clone(),
        runtime,
    );
    (core, proxy, journal, worker)
}

#[tokio::test]
async fn a_turn_cut_mid_flight_reaches_the_server_exactly_once_after_heal() {
    let (core, proxy, journal, worker) = staged_worker(stalling_turn(
        "before-the-cut",
        "during-the-partition",
        "the-turn-answer",
    ))
    .await;

    // The turn opens over the healthy link: its first chunk crosses.
    support::log_until(
        &core.log,
        "s1",
        "the turn never opened at the server",
        |events| support::chunks_saying(events, "before-the-cut") == 1,
    )
    .await;

    // The cable is yanked mid-turn — the 400ms stall is still holding the
    // turn open, so everything after this line happens partitioned.
    proxy.cut();
    let frozen_head = core.log.head("s1").await.expect("a readable log");

    // Held cut until the ENTIRE rest of the turn exists only worker-side:
    // the partition chunk, and the usage record that closes the turn. This
    // is what makes the mid-turn window deterministic rather than a race.
    support::within(
        "the partitioned worker journals the rest of the turn",
        support::journal_until(
            &journal,
            "the rest of the turn never reached the worker journal",
            |events| {
                support::chunks_saying(events, "during-the-partition") == 1
                    && !support::usage_counts_by_turn(events).is_empty()
            },
        ),
    )
    .await;
    assert_eq!(
        core.log.head("s1").await.expect("a readable log"),
        frozen_head,
        "nothing can have crossed a severed proxy"
    );

    // Heal. The supervisor redials on its own; reconciliation must deliver
    // exactly what the server missed — no gaps, no duplicates.
    proxy.heal();
    let events = support::log_until(
        &core.log,
        "s1",
        "the partitioned turn never reconciled into the server log",
        |events| {
            support::chunks_saying(events, "during-the-partition") == 1
                && !support::usage_counts_by_turn(events).is_empty()
        },
    )
    .await;

    assert_eq!(
        support::chunks_saying(&events, "before-the-cut"),
        1,
        "the pre-cut chunk must not be re-pushed by the replay"
    );
    support::assert_usage_exactly_once(&events, "the partitioned turn");

    // Order survives the partition: the turn's chunks arrive in the order
    // the agent produced them, with the partition marker after the opener.
    let texts = support::chunk_texts(&events);
    let opener = texts.iter().position(|t| t == "before-the-cut");
    let partitioned = texts.iter().position(|t| t == "during-the-partition");
    assert!(
        opener < partitioned,
        "order must survive the partition: {texts:?}"
    );

    // And the lifecycle tells one coherent story: the agent started exactly
    // once — a reconnect is a resync, never a respawn.
    assert_eq!(
        support::lifecycle_count(&events, "agent-1", "placed on this worker"),
        1,
        "a partition must not re-run the spawn"
    );

    // The healed link also carries the ack back: the worker journal
    // truncates, which only a server ack can cause. Bounded by poll count.
    support::within("the ack truncates the worker journal", async {
        for _ in 0_u32..support::MAX_POLLS {
            if matches!(
                journal.read_from(1, 1).await,
                Err(horde_worker::WorkerError::TruncatedPast { .. })
            ) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("no ack ever truncated the journal through the healed link");
    })
    .await;

    worker.abort();
}

/// The same wire, attacked at the byte level: the cut is armed by *budget*,
/// so the partition lands somewhere inside a gRPC frame nobody chose — and
/// recovery must still be exact. This is the injector's deterministic
/// mid-transfer sever doing to the event stream what a flaky network does.
#[tokio::test]
async fn a_byte_budget_cut_inside_the_push_stream_still_reconciles_exactly() {
    let (core, proxy, journal, worker) = staged_worker(stalling_turn(
        "budget-opener",
        "budget-tail",
        "the-budget-answer",
    ))
    .await;

    // Healthy through registration and the turn's opener…
    support::log_until(
        &core.log,
        "s1",
        "the turn never opened at the server",
        |events| support::chunks_saying(events, "budget-opener") == 1,
    )
    .await;

    // …then a tiny byte budget, armed while the stall holds the turn open.
    // The tail's push will cross it and die somewhere inside a frame nobody
    // chose. Wherever it lands, the assertion is the same: exactly-once.
    proxy.cut_after(100);

    // The worker finishes the turn locally regardless — journaling is
    // local-first and owes the network nothing.
    support::within(
        "the worker finishes the turn locally",
        support::journal_until(
            &journal,
            "the turn never finished in the worker journal",
            |events| support::chunks_saying(events, "budget-tail") == 1,
        ),
    )
    .await;

    proxy.heal();
    let events = support::log_until(
        &core.log,
        "s1",
        "the turn never reconciled after the byte-budget cut",
        |events| {
            support::chunks_saying(events, "budget-tail") == 1
                && !support::usage_counts_by_turn(events).is_empty()
        },
    )
    .await;

    assert_eq!(
        support::chunks_saying(&events, "budget-opener"),
        1,
        "the replay of the possibly-lost batch must deduplicate, not append"
    );
    support::assert_usage_exactly_once(&events, "the budget-cut turn");
    assert_eq!(
        support::lifecycle_count(&events, "agent-1", "placed on this worker"),
        1,
        "however the bytes fell, the spawn ran once"
    );
    // No duplicate of any marker chunk anywhere in the log.
    let session_established = events
        .iter()
        .filter(|event| {
            matches!(&event.kind, Some(Kind::AgentLifecycleChanged(changed))
                if changed.reason.contains("provider session"))
        })
        .count();
    assert_eq!(
        session_established, 1,
        "the established-session event must not duplicate under replay"
    );

    worker.abort();
}
