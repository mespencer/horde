//! The orchestrator verbs against a real server.
//!
//! The pair worth watching here is `report_result` and `wait_for_agent`. They
//! were the two ends of a pipe that did not exist: nothing in the codebase
//! wrote `ResultReported`, so the read side would have blocked forever against
//! a writer that was never built. These tests are what say the pipe now joins
//! up, and they would fail if either half regressed.

mod common;

use common::{Setup, harness, identity_for, verbs_for_one_agent};
use horde_mcp::verbs::{MessageKind, Verbs};
use horde_proto::v1::ResultStatus;

#[tokio::test]
async fn a_reported_result_is_what_a_waiting_parent_receives() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let reporter = Verbs::new(harness.context().await?);
    let waiter = Verbs::new(harness.context().await?);

    // The parent starts waiting before the child reports, which is the ordering
    // that actually happens and the one a naive implementation gets wrong.
    let agent_id = harness.agent_id.clone();
    let waiting = tokio::spawn(async move { waiter.wait_for_agent(&agent_id).await });

    reporter
        .report_result(
            &identity,
            ResultStatus::Partial,
            "got halfway",
            "result-halfway",
        )
        .await?;

    let result = waiting.await??;
    assert_eq!(result.summary, "got halfway");
    assert_eq!(
        result.status,
        ResultStatus::Partial as i32,
        "partial is a real answer and must not be flattened into success"
    );
    Ok(())
}

/// A result already in the log answers a wait that starts afterwards. Without
/// this, a parent that was slow to call `wait_for_agent` would hang forever on
/// a child that had already finished.
#[tokio::test]
async fn a_result_reported_before_the_wait_still_answers_it() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let reporter = Verbs::new(harness.context().await?);
    reporter
        .report_result(
            &identity,
            ResultStatus::Success,
            "done early",
            "result-early",
        )
        .await?;

    let waiter = Verbs::new(harness.context().await?);
    let result = waiter.wait_for_agent(&harness.agent_id).await?;

    assert_eq!(result.summary, "done early");
    Ok(())
}

/// A second assignment owes a second result. The wait used to replay the log
/// from zero and hand back the *first* result it met, so a parent that
/// re-assigned a child was told its new task was done before it started.
/// Both waits share one context — the second reads from the cursor the first
/// left, which is the path production takes.
#[tokio::test]
async fn a_wait_after_a_second_assignment_is_not_answered_by_the_first_result() -> Setup<()> {
    let (harness, identity, reporter) = verbs_for_one_agent().await?;
    let parent = std::sync::Arc::new(Verbs::new(harness.context().await?));

    reporter
        .report_result(&identity, ResultStatus::Success, "first", "result-first")
        .await?;
    assert_eq!(
        parent.wait_for_agent(&harness.agent_id).await?.summary,
        "first"
    );

    parent
        .send_message(
            &harness.agent_id,
            MessageKind::Assign,
            "now the second thing",
            None,
            false,
            "assign-second",
        )
        .await?;

    let waiter = std::sync::Arc::clone(&parent);
    let agent_id = harness.agent_id.clone();
    let mut waiting = tokio::spawn(async move { waiter.wait_for_agent(&agent_id).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut waiting)
            .await
            .is_err(),
        "the first result is stale once a second assignment was sent"
    );

    reporter
        .report_result(&identity, ResultStatus::Success, "second", "result-second")
        .await?;
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), waiting).await???;
    assert_eq!(result.summary, "second");
    Ok(())
}

/// A wait survives the link to the server being cut: the result lands during
/// the partition, and the wait reads it once the link heals — by redialing
/// from its cursor, not by failing the verb as an internal error.
#[tokio::test]
async fn a_wait_survives_a_severed_link_and_reads_the_result_after_it_heals() -> Setup<()> {
    let (harness, identity, reporter) = verbs_for_one_agent().await?;
    let proxy = horde_testkit::fault::FaultProxy::front(&harness.endpoint).await?;
    let through_proxy = horde_mcp::context::AgentContext::new(
        harness.journal(),
        horde_cli::client::Client::connect(&proxy.endpoint()).await?,
        harness.session_id.clone(),
    );
    let waiter = Verbs::new(through_proxy);
    let agent_id = harness.agent_id.clone();
    let mut waiting = tokio::spawn(async move { waiter.wait_for_agent(&agent_id).await });

    // Let the follow open, then cut it mid-wait.
    let opened = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while proxy.connections_carried() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(opened.is_ok(), "the wait must dial the server");
    proxy.cut();

    reporter
        .report_result(
            &identity,
            ResultStatus::Success,
            "landed in the partition",
            "r-cut",
        )
        .await?;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut waiting)
            .await
            .is_err(),
        "nothing can arrive while the link is cut, and the wait must not fail"
    );

    proxy.heal();
    let result = tokio::time::timeout(std::time::Duration::from_secs(10), waiting).await???;
    assert_eq!(result.summary, "landed in the partition");
    Ok(())
}

/// The payload is the Worker's to stamp. An endpoint that let an agent declare
/// its own commits would be the declaring path DESIGN §5 rules out, so this
/// asserts the absence deliberately rather than by accident.
#[tokio::test]
async fn a_reported_result_carries_no_agent_declared_payload() -> Setup<()> {
    let (harness, identity, reporter) = verbs_for_one_agent().await?;
    reporter
        .report_result(&identity, ResultStatus::Success, "done", "result-done")
        .await?;

    let waiter = Verbs::new(harness.context().await?);
    let result = waiter.wait_for_agent(&harness.agent_id).await?;

    assert!(
        result.payload.is_none(),
        "commits are auto-detected by the worker, never claimed by the agent"
    );
    Ok(())
}

/// A spawn never blocks. With no worker registered there is nowhere to run,
/// and the right answer is a queued placement carrying the reason — not an
/// error, and not a wait. The task the spawner named rides the spawn as the
/// child's initial assignment — the parameter routing the conformance audit
/// found dropped on the floor (fix list a2).
#[tokio::test]
async fn a_spawn_with_nowhere_to_run_comes_back_queued_rather_than_failing() -> Setup<()> {
    let (harness, identity, verbs) = verbs_for_one_agent().await?;
    let outcome = verbs
        .spawn_agent(&identity, "map the atlas", "spawn-1")
        .await?;

    assert!(
        !outcome.agent_id.is_empty(),
        "the agent exists whether or not it is placed"
    );
    assert!(
        !outcome.placed,
        "no worker is registered, so this cannot have been placed"
    );

    // The child's `AgentSpawned` carries the task as its initial assignment:
    // that is what a worker will fold into the first turn input, so a spawn
    // that lost it would place an agent with nothing to do.
    let mut client = harness.client().await?;
    let mut task = None;
    client
        .sync(
            &horde_cli::client::SyncOptions::new(harness.session_id.clone()),
            |envelope| {
                let spawned_child = envelope
                    .event
                    .as_ref()
                    .filter(|event| {
                        event.agent_id.as_ref().map(ToString::to_string)
                            == Some(outcome.agent_id.clone())
                    })
                    .and_then(|event| match &event.kind {
                        Some(horde_proto::v1::event::Kind::AgentSpawned(spawned)) => {
                            spawned.spec.as_ref()
                        }
                        _ => None,
                    });
                if let Some(spec) = spawned_child {
                    task = spec
                        .initial_assignment
                        .as_ref()
                        .map(|assignment| assignment.text.clone());
                }
            },
            |_| {},
        )
        .await?;
    assert_eq!(
        task.as_deref(),
        Some("map the atlas"),
        "the task must ride the spawn as the initial assignment"
    );
    Ok(())
}

/// Assignments and notes both reach the log, and the distinction is the
/// sender's to declare.
#[tokio::test]
async fn assignments_and_notes_both_land_and_report_where() -> Setup<()> {
    let harness = harness().await?;

    let verbs = Verbs::new(harness.context().await?);

    let assigned = verbs
        .send_message(
            &harness.agent_id,
            MessageKind::Assign,
            "fix the build",
            None,
            false,
            "assign-1",
        )
        .await?;
    let noted = verbs
        .send_message(
            &harness.agent_id,
            MessageKind::Note,
            "for context",
            None,
            false,
            "note-1",
        )
        .await?;

    assert!(
        noted > assigned,
        "each message reports the position it actually wrote at: {assigned} then {noted}"
    );
    Ok(())
}

/// The regression test for the bug this crate's tests found the hard way.
///
/// `harness()` seeds its session with idempotency key `key-1`. Reusing that key
/// for a result used to make the result a duplicate of the session's
/// `agent_spawned` — acked, returned `Ok`, and silently absent from the log, so
/// the wait in the tests above hung with no error anywhere to explain it.
///
/// It is now refused at the write, which is the whole point: the complaint
/// arrives at the caller that caused it rather than as a hang in a reader
/// nobody has connected to it yet.
#[tokio::test]
async fn reusing_another_events_idempotency_key_is_refused_rather_than_swallowed() -> Setup<()> {
    let (_harness, identity, verbs) = verbs_for_one_agent().await?;

    let error = verbs
        .report_result(&identity, ResultStatus::Success, "done", "key-1")
        .await
        .expect_err("key-1 already belongs to this session's agent_spawned");

    let rendered = error.to_string();
    assert!(
        rendered.contains("agent_spawned") && rendered.contains("result_reported"),
        "the message must name both kinds so the collision is obvious: {rendered}"
    );
    assert!(
        rendered.contains("key-1"),
        "the message must name the key: {rendered}"
    );
    Ok(())
}

/// An unwritable journal must surface rather than look like a reported result.
#[tokio::test]
async fn a_result_that_cannot_be_recorded_is_an_error() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let verbs = Verbs::new(harness.context_with_dead_journal().await?);

    let error = verbs
        .report_result(&identity, ResultStatus::Success, "done", "result-done")
        .await
        .expect_err("an unwritable journal must surface");

    assert!(
        matches!(error, horde_mcp::verbs::VerbError::NotRecorded { .. }),
        "a parent waiting on this agent must not be told it succeeded: {error}"
    );
    Ok(())
}
