//! The orchestrator verbs against a real server.
//!
//! The pair worth watching here is `report_result` and `wait_for_agent`. They
//! were the two ends of a pipe that did not exist: nothing in the codebase
//! wrote `ResultReported`, so the read side would have blocked forever against
//! a writer that was never built. These tests are what say the pipe now joins
//! up, and they would fail if either half regressed.

mod common;

use common::{Setup, harness, identity_for};
use horde_mcp::verbs::{MessageKind, Verbs};
use horde_proto::v1::ResultStatus;

#[tokio::test]
async fn a_reported_result_is_what_a_waiting_parent_receives() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let reporter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    let mut waiter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );

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

    let reporter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    reporter
        .report_result(
            &identity,
            ResultStatus::Success,
            "done early",
            "result-early",
        )
        .await?;

    let mut waiter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    let result = waiter.wait_for_agent(&harness.agent_id).await?;

    assert_eq!(result.summary, "done early");
    Ok(())
}

/// The payload is the Worker's to stamp. An endpoint that let an agent declare
/// its own commits would be the declaring path DESIGN §5 rules out, so this
/// asserts the absence deliberately rather than by accident.
#[tokio::test]
async fn a_reported_result_carries_no_agent_declared_payload() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let reporter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    reporter
        .report_result(&identity, ResultStatus::Success, "done", "result-done")
        .await?;

    let mut waiter = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    let result = waiter.wait_for_agent(&harness.agent_id).await?;

    assert!(
        result.payload.is_none(),
        "commits are auto-detected by the worker, never claimed by the agent"
    );
    Ok(())
}

/// A spawn never blocks. With no worker registered there is nowhere to run,
/// and the right answer is a queued placement carrying the reason — not an
/// error, and not a wait.
#[tokio::test]
async fn a_spawn_with_nowhere_to_run_comes_back_queued_rather_than_failing() -> Setup<()> {
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let mut verbs = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );
    let outcome = verbs.spawn_agent(&identity, "spawn-1").await?;

    assert!(
        !outcome.agent_id.is_empty(),
        "the agent exists whether or not it is placed"
    );
    assert!(
        !outcome.placed,
        "no worker is registered, so this cannot have been placed"
    );
    Ok(())
}

/// Assignments and notes both reach the log, and the distinction is the
/// sender's to declare.
#[tokio::test]
async fn assignments_and_notes_both_land_and_report_where() -> Setup<()> {
    let harness = harness().await?;

    let mut verbs = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );

    let assigned = verbs
        .send_message(
            &harness.agent_id,
            MessageKind::Assign,
            "fix the build",
            false,
            "assign-1",
        )
        .await?;
    let noted = verbs
        .send_message(
            &harness.agent_id,
            MessageKind::Note,
            "for context",
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
    let harness = harness().await?;
    let identity = identity_for(&harness.agent_id)?;

    let verbs = Verbs::new(
        harness.journal(),
        harness.client().await?,
        harness.session_id.clone(),
    );

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

    let verbs = Verbs::new(
        std::sync::Arc::new(common::ServerJournal::new("http://127.0.0.1:1")),
        harness.client().await?,
        harness.session_id.clone(),
    );

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
