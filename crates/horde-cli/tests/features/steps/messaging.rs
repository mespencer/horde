//! Assignments, notes, and the result an assignment is owed.
//!
//! Sends and spawns go through the real binary (the ordinary "the user runs"
//! step). The child's *result* is an observed fact of execution: the control
//! plane cannot author one by design (DESIGN.md §5), so the step journals it
//! exactly the way a Worker's journal push does — through the worker spine.

use cucumber::{then, when};
use horde_proto::v1::{AgentId, AgentResult, Event, ResultReported, ResultStatus, event::Kind};

use crate::world::{HordeWorld, StepResult};

/// The spawn renders "<agent-id>  queued  (…)"; the id is the first word.
#[then("the spawned child's id is on stdout")]
fn the_spawned_childs_id_is_on_stdout(world: &mut HordeWorld) -> StepResult {
    let stdout = world.last_run()?.stdout.clone();
    let child = stdout
        .split_whitespace()
        .next()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| format!("no agent id on stdout: {stdout:?}"))?;
    world.notes.insert("child".to_owned(), child.to_owned());
    Ok(())
}

#[then("stdout names the assignment it opened")]
fn stdout_names_the_assignment(world: &mut HordeWorld) -> StepResult {
    let stdout = &world.last_run()?.stdout;
    if !stdout.contains("(assignment ") {
        return Err(format!("no assignment id on stdout: {stdout:?}").into());
    }
    Ok(())
}

#[then("stdout does not mention an assignment")]
fn stdout_does_not_mention_an_assignment(world: &mut HordeWorld) -> StepResult {
    let stdout = &world.last_run()?.stdout;
    if stdout.contains("assignment") {
        return Err(format!("a note must not open an assignment: {stdout:?}").into());
    }
    Ok(())
}

#[when(expr = "the child reports success {string}, journaled the way a worker journals it")]
async fn the_child_reports_success(world: &mut HordeWorld, summary: String) -> StepResult {
    let child = world.note("child")?.to_owned();
    let event = Event {
        agent_id: Some(AgentId::new(&child)),
        occurred_at: None,
        idempotency_key: format!("result-{child}"),
        kind: Some(Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: None,
                status: ResultStatus::Success as i32,
                summary,
                payload: None,
            }),
        })),
    };
    horde_server_testkit::journal_event(world.endpoint()?, world.session()?, event).await?;
    Ok(())
}

#[then(expr = "the result {string} is on the log")]
async fn the_result_is_on_the_log(world: &mut HordeWorld, summary: String) -> StepResult {
    world
        .await_event("the result to be journaled", move |envelope| {
            let Kind::ResultReported(reported) = envelope.event.as_ref()?.kind.as_ref()? else {
                return None;
            };
            (reported.result.as_ref()?.summary == summary).then_some(())
        })
        .await
}

/// The user's view of it: the replay carries the reported result. The CLI
/// keeps event details shallow, so what a user sees is the fact that a
/// result arrived, not its summary — the TUI owns the richer view.
#[then("the session replay shows a reported result")]
async fn the_replay_shows_a_reported_result(world: &mut HordeWorld) -> StepResult {
    crate::steps::cli::replay_shows(world, "result_reported").await
}
