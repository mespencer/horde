//! Scenario (b): `kill -9` a worker mid-turn — a real process, really killed.
//!
//! The machinery under attack, and the recovery asserted of each piece:
//!
//! * **Placement re-queue.** An agent placed on the dead worker that never
//!   started is re-queued when the loss is detected, and re-placed when the
//!   worker returns — with the fold's cancel command settling the
//!   at-least-once race exactly as `placement.rs` documents.
//! * **Revival.** The agent that was mid-turn stays placed (a disconnect is
//!   a partition, not a death), and the assignment that was waiting for the
//!   worker revives it in its next life — same provider session, turn
//!   numbering continued past the half-journaled turn.
//! * **Usage.** Exactly once per completed turn, across both lives and the
//!   command replay.
//!
//! The kill window is made deterministic, not raced: the victim turn stalls
//! by script, the network is blackholed *before* the second agent is seeded
//! (so its spawn command can never have been applied), and only then does
//! SIGKILL land.

use horde_proto::prost::Message;
use horde_proto::v1::event::Kind;
use horde_proto::v1::{AgentId, Assignment, AssignmentSent, Event, PlacementState};
use horde_server::EventLog;
use horde_server::log::PendingEvent;
use horde_testkit::FaultProxy;

use horde_fault::{TurnSpec, WorkerProcess, WorkerRunnerConfig};

use crate::support;

/// Appends the `AssignmentSent` a control-plane send would journal — the
/// message that both (a) triggers loss detection when its command cannot be
/// sent, and (b) revives the agent when the worker returns.
async fn send_assignment_event(log: &EventLog, session: &str, agent: &str, text: &str) {
    let event = Event {
        agent_id: Some(AgentId::new(agent)),
        occurred_at: None,
        idempotency_key: format!("{agent}:assign:{text}"),
        kind: Some(Kind::AssignmentSent(AssignmentSent {
            from: None,
            assignment: Some(Assignment {
                assignment_id: None,
                text: text.to_owned(),
                payload: None,
            }),
            delivery: 0,
        })),
    };
    log.append(PendingEvent {
        session_id: session.to_owned(),
        idempotency_key: event.idempotency_key.clone(),
        payload: event.encode_to_vec(),
        ..PendingEvent::default()
    })
    .await
    .expect("the assignment appends");
}

/// The runner's script. Index 0 is the victim turn (opens, then stalls far
/// past the test — it dies half-journaled, and its ordinal is the one
/// revival skips); index 1 is the revived turn's answer.
fn runner_turns() -> Vec<TurnSpec> {
    vec![
        TurnSpec::opening_then_stalling("t1-open", 120_000, "t1-answer"),
        TurnSpec::answering("revived-answer"),
    ]
}

#[tokio::test]
async fn a_worker_killed_mid_turn_requeues_the_unstarted_and_revives_the_running() {
    let core = support::fresh_core();
    let upstream = horde_server_testkit::serve_core(core.clone())
        .await
        .expect("the core serves");
    let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");
    let journal_dir = tempfile::tempdir().expect("a tempdir");
    let runner = WorkerRunnerConfig {
        endpoint: proxy.endpoint(),
        worker_id: "worker-a".to_owned(),
        journal: journal_dir.path().join("journal.sqlite"),
        turns: runner_turns(),
        identity_dir: None,
    };

    // agent-1 is seeded queued, so the *scheduler* places it when the worker
    // registers — the production placement path, not a seeded shortcut.
    horde_server_testkit::seed_agent(&core.log, "s1", "agent-1", None, "begin")
        .await
        .expect("the seed appends");

    let first_life = WorkerProcess::spawn(&runner).expect("the runner spawns");

    // The victim turn is open: its first chunk crossed, and the script's
    // stall now holds it mid-turn for minutes — the kill window.
    let placed = support::log_until(
        &core.log,
        "s1",
        "the scheduler never placed agent-1 or its turn never opened",
        |events| support::chunks_saying(events, "t1-open") == 1,
    )
    .await;
    assert_eq!(
        support::placement_states(&placed, "agent-1"),
        vec![PlacementState::Queued, PlacementState::Placed],
        "the scheduler answered the queued spawn when the worker appeared"
    );

    // Blackhole first: whatever is seeded from here can reach the log but
    // never the worker, so agent-2's spawn is guaranteed never-started.
    proxy.blackhole();
    horde_server_testkit::place_agent(&core.log, "s2", "agent-2", "worker-a", "queued work")
        .await
        .expect("the seed appends");

    // SIGKILL, mid-turn. Then sever the held sockets so the transport is
    // gone, not merely quiet.
    first_life.kill_now().await.expect("the kill lands");
    proxy.cut();

    // The revival assignment. Its DeliverMessage command is also the next
    // send on worker-a's command stream — the send whose failure is how the
    // server learns the worker is lost (nothing here runs on a timer).
    send_assignment_event(&core.log, "s1", "agent-1", "are you alive?").await;

    // Loss detected: the never-started agent is re-queued. The mid-turn
    // agent must NOT be — a disconnect is a partition, not a death.
    let requeued = support::log_until(
        &core.log,
        "s2",
        "the lost worker's unstarted agent was never re-queued",
        |events| {
            support::placement_states(events, "agent-2")
                .last()
                .copied()
                == Some(PlacementState::Queued)
        },
    )
    .await;
    assert_eq!(
        support::placement_states(&requeued, "agent-2"),
        vec![PlacementState::Placed, PlacementState::Queued],
        "the re-queue answers the seeded placement, exactly once"
    );
    let s1_after_loss = support::decoded_log(&core.log, "s1").await;
    assert_eq!(
        support::placement_states(&s1_after_loss, "agent-1")
            .last()
            .copied(),
        Some(PlacementState::Placed),
        "the mid-turn agent stays placed through the loss"
    );

    // The second life: same journal, same identity, healed network.
    proxy.heal();
    let second_life = WorkerProcess::spawn(&runner).expect("the runner respawns");

    // Recovery, piece by piece. The revived turn answers — through the
    // command replay delivering the assignment to a process whose agents map
    // started empty, over the provider session the journal remembered.
    let s1_events = support::log_until(
        &core.log,
        "s1",
        "the revival never answered through the second life",
        |events| support::chunks_saying(events, "revived-answer") == 1,
    )
    .await;
    assert_eq!(
        support::lifecycle_count(&s1_events, "agent-1", "revived"),
        1,
        "the revival is journaled as a lifecycle transition, once"
    );
    assert_eq!(
        support::lifecycle_count(&s1_events, "agent-1", "placed on this worker"),
        1,
        "the mid-turn agent was revived, never respawned"
    );
    support::assert_usage_exactly_once(&s1_events, "agent-1 across two lives");

    // And the re-queued agent lands back on the returned worker and actually
    // starts: placement ends Placed, and its turn opens.
    let s2_events = support::log_until(
        &core.log,
        "s2",
        "the re-queued agent never started on the returned worker",
        |events| {
            let states = support::placement_states(events, "agent-2");
            states.last().copied() == Some(PlacementState::Placed)
                && support::chunks_saying(events, "t1-open") >= 1
        },
    )
    .await;
    let states = support::placement_states(&s2_events, "agent-2");
    assert_eq!(
        states,
        vec![
            PlacementState::Placed,
            PlacementState::Queued,
            PlacementState::Placed,
        ],
        "the full placement story: placed, lost, re-placed — nothing more"
    );

    second_life.kill_now().await.expect("the cleanup kill lands");
}
