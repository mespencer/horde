//! Watching an escalation arrive and reading the decision back off the log.
//!
//! The *answering* happens through the ordinary "the user runs" step — the
//! `approve` verb of the real binary. What lives here is the waiting and the
//! decision assertions, made through the client library because the CLI's
//! replay deliberately keeps event details shallow.

use cucumber::{then, when};
use horde_proto::v1::{PermissionDecision, PermissionOutcome, event::Kind};

use crate::world::{HordeWorld, StepResult};

/// Waits until the agent's proposed call is on the log, and notes its
/// request id — later steps reference it as `<request>`.
#[when(expr = "the escalation to run {string} reaches the log")]
async fn the_escalation_reaches_the_log(world: &mut HordeWorld, command: String) -> StepResult {
    let request_id = world
        .await_event("the escalation to arrive", move |envelope| {
            let Kind::PermissionRequested(requested) = envelope.event.as_ref()?.kind.as_ref()?
            else {
                return None;
            };
            let request = requested.request.as_ref()?;
            // The proposed call must actually carry the arguments the feature
            // named — a request for some other call must not satisfy this.
            if !format!("{:?}", request.tool_call).contains(&command) {
                return None;
            }
            request.request_id.as_ref().map(ToString::to_string)
        })
        .await?;
    world.notes.insert("request".to_owned(), request_id);
    Ok(())
}

/// Approves with `--update-input`: the allow that clamps the call's
/// arguments rather than merely permitting them (DESIGN §10).
#[when(expr = "the user approves {string} narrowing the command to {string}")]
async fn the_user_approves_narrowing(
    world: &mut HordeWorld,
    request: String,
    narrowed: String,
) -> StepResult {
    let request_id = world.substituted(&request);
    let update = serde_json::json!({ "command": narrowed }).to_string();
    let endpoint = world.endpoint()?.to_owned();
    world
        .run_horde(
            &endpoint,
            &["approve", &request_id, "--update-input", &update],
        )
        .await
}

/// The first decision on the log, once there is one.
async fn decision(world: &HordeWorld) -> StepResult<PermissionDecision> {
    world
        .await_event("the decision to be journaled", |envelope| {
            let Kind::PermissionDecided(decided) = envelope.event.as_ref()?.kind.as_ref()? else {
                return None;
            };
            decided.decision.clone()
        })
        .await
}

#[then(expr = "the decision on the log is {string}")]
async fn the_decision_is(world: &mut HordeWorld, outcome: String) -> StepResult {
    let expected = match outcome.as_str() {
        "allowed" => PermissionOutcome::Allow,
        "denied" => PermissionOutcome::Deny,
        other => return Err(format!("unknown outcome {other:?} in the feature file").into()),
    };
    let decided = decision(world).await?;
    if decided.outcome != expected as i32 {
        return Err(format!("expected {outcome}, got {decided:?}").into());
    }
    Ok(())
}

#[then(expr = "the decision's reason is {string}")]
async fn the_decisions_reason_is(world: &mut HordeWorld, reason: String) -> StepResult {
    let decided = decision(world).await?;
    if decided.reason != reason {
        return Err(format!("expected reason {reason:?}, got {:?}", decided.reason).into());
    }
    Ok(())
}

#[then(expr = "the decision on the log carries the narrowed command {string}")]
async fn the_decision_carries_the_narrowing(
    world: &mut HordeWorld,
    narrowed: String,
) -> StepResult {
    let decided = decision(world).await?;
    let updated = decided
        .updated_input
        .as_ref()
        .ok_or("the decision carries no updated input, so the original call would run")?;
    let command = updated.fields.get("command").and_then(|value| {
        if let Some(horde_proto::prost_types::value::Kind::StringValue(text)) = &value.kind {
            Some(text.as_str())
        } else {
            None
        }
    });
    if command != Some(narrowed.as_str()) {
        return Err(format!("expected the narrowed command {narrowed:?}, got {updated:?}").into());
    }
    Ok(())
}
