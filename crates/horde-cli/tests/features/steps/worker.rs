//! A real `horde-worker` joining the scenario's server, supervising the
//! FakeAdapter (docs/TESTING.md §3) — real dial, real registration, real
//! journal, real command stream. Only the provider harness is scripted.

use std::sync::Arc;

use cucumber::given;
use horde_adapter::{TokenUsage, TurnStatus};
use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
use horde_proto::v1::{PlacementState, event::Kind};

use crate::world::{HordeWorld, RunningWorker, StepResult};

/// The token bill every scripted turn declares, so the usage display has a
/// number worth asserting on ("in 1200" is a fact from this line).
fn scripted_usage() -> TokenUsage {
    TokenUsage {
        input_tokens: 1200,
        output_tokens: 345,
        ..TokenUsage::default()
    }
}

#[given(expr = "a worker joins running a scripted agent that answers {string}")]
async fn a_worker_joins_answering(world: &mut HordeWorld, answer: String) -> StepResult {
    let turn = ScriptedTurn::saying(answer).with_usage(scripted_usage(), 0.02);
    join_with(world, Script::of(vec![turn])).await
}

#[given(
    expr = "a worker joins running a scripted agent that must ask before running {string}, \
            then answers {string}"
)]
async fn a_worker_joins_asking(
    world: &mut HordeWorld,
    command: String,
    answer: String,
) -> StepResult {
    // The ask comes *before* the answer, so nothing the user later sees can
    // have been emitted while the agent was still blocked on the decision.
    let turn = ScriptedTurn {
        steps: vec![
            Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: serde_json::json!({ "command": command }),
            },
            Step::Say(answer.clone()),
        ],
        text: Some(answer),
        status: TurnStatus::Completed,
        usage: scripted_usage(),
        cost_usd: 0.02,
    };
    join_with(world, Script::of(vec![turn])).await
}

/// Dials the scenario's server as a real worker running `script`, then waits
/// for the registration's effect: the queued root agent placed on it.
async fn join_with(world: &mut HordeWorld, script: Script) -> StepResult {
    let endpoint = world.endpoint()?.to_owned();
    let journal_dir = tempfile::tempdir()?;
    let journal = horde_worker::Journal::open(&journal_dir.path().join("journal.sqlite"))?;
    let runtime = horde_worker::Runtime::new(journal.clone())
        .with_adapter(Arc::new(FakeAdapter::new(script)));

    let config = horde_worker::DialConfig {
        endpoint,
        worker_id: "scripted-box".to_owned(),
        hello: horde_worker::capabilities::hello("scripted-box"),
        capabilities: horde_worker::capabilities::advertise(
            runtime.adapters(),
            None,
            &horde_worker::capabilities::DeclaredFacts::default(),
        ),
        identity: None,
    };
    let (mut connection, _registration) = horde_worker::Connection::dial(&config).await?;
    let task = tokio::spawn(async move {
        let mut runtime = runtime;
        let logins = std::sync::Arc::new(horde_worker::login::LoginPlane::from_adapters(
            runtime.adapters(),
            std::sync::Arc::new(|_line| {}),
        ));
        connection.run(&journal, &mut runtime, logins).await
    });
    world.worker = Some(RunningWorker::new(task, journal_dir));

    // Registration is what places the queued root agent; nothing later in the
    // scenario is meaningful until the log says that happened.
    world
        .await_event("the root agent to be placed on the worker", |envelope| {
            let Kind::PlacementChanged(changed) = envelope.event.as_ref()?.kind.as_ref()? else {
                return None;
            };
            let placement = changed.placement.as_ref()?;
            (placement.state == PlacementState::Placed as i32).then_some(())
        })
        .await
}
