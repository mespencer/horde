use std::sync::Arc;

use horde_adapter::{Capabilities, Capability};
use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
use horde_proto::prost_types::Struct;
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    AgentId, ArchiveAgentCommand, Assignment, DeliverMessageCommand,
    DeliverPermissionDecisionCommand, DeliveryMode, MaterializeWorkspaceCommand, PermissionOutcome,
    PermissionRequestId, SessionId, SpawnAgentCommand, StreamCommandsResponse,
    deliver_message_command, event::Kind,
};
use serde_json::json;

use super::Runtime;
use super::shared::carried_payload;
use crate::client::ApplyCommand;
use crate::journal::{AgentBookkeeping, Journal};

/// Waits until every accepted command has completed — bounded, so a
/// lane a mutant starved fails here in seconds instead of hanging.
async fn settle(runtime: &Runtime) {
    tokio::time::timeout(core::time::Duration::from_secs(15), runtime.settled())
        .await
        .unwrap_or_else(|_| panic!("the accepted commands never completed"));
}

fn runtime_with(script: Script) -> Runtime {
    Runtime::new(Journal::in_memory().expect("an in-memory journal"))
        .with_adapter(Arc::new(FakeAdapter::new(script)))
}

/// A runtime whose adapter advertises only pre-authorization — the shape
/// of a provider that can neither rewrite inputs nor take an interrupt.
fn preauthorize_only_runtime(script: Script) -> Runtime {
    let adapter = FakeAdapter::new(script)
        .with_capabilities(Capabilities::none().with(Capability::PreAuthorizeToolCalls));
    Runtime::new(Journal::in_memory().expect("an in-memory journal"))
        .with_adapter(Arc::new(adapter))
}

fn spawn_command(sequence: u64, agent: &str, first_message: &str) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: sequence,
        command_id: format!("s1:{sequence}"),
        log_position: sequence,
        command: Some(Command::SpawnAgent(SpawnAgentCommand {
            session_id: Some(SessionId::new("s1")),
            agent_id: Some(AgentId::new(agent)),
            spec: Some(horde_proto::v1::AgentSpec {
                initial_assignment: Some(Assignment {
                    assignment_id: None,
                    text: first_message.to_owned(),
                    payload: None,
                }),
                ..horde_proto::v1::AgentSpec::default()
            }),
            ..SpawnAgentCommand::default()
        })),
    }
}

fn decision_command(
    sequence: u64,
    agent: &str,
    request_id: &str,
    updated_input: Option<Struct>,
) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: sequence,
        command_id: format!("s1:{}", sequence + 100),
        log_position: 0,
        command: Some(Command::DeliverPermissionDecision(
            DeliverPermissionDecisionCommand {
                agent_id: Some(AgentId::new(agent)),
                decision: Some(horde_proto::v1::PermissionDecision {
                    request_id: Some(PermissionRequestId::new(request_id)),
                    outcome: PermissionOutcome::Allow.into(),
                    updated_input,
                    ..horde_proto::v1::PermissionDecision::default()
                }),
            },
        )),
    }
}

/// Waits until the journal satisfies `accept`, returning what it holds.
///
/// The wait is on a *fact* about the journal, never on an event count:
/// the pump journals each session event in its own transaction with its
/// own head announcement, so a watcher can legitimately wake between two
/// appends of the same logical step and read a journal that is mid-way
/// through it. A count-based wait turns that interleaving into a flake
/// (`a_permission_decision_unblocks_the_waiting_agent`, twice); a
/// fact-based wait just parks again.
///
/// Bounded twice: by iteration count (a hot loop cannot spin past 64
/// polls) and by a ceiling on each park (a journal that stops moving fails
/// in seconds, not never).
async fn journal_satisfying(
    runtime: &Runtime,
    waiting_for: &str,
    accept: impl Fn(&[horde_proto::v1::JournaledEvent]) -> bool,
) -> Vec<horde_proto::v1::JournaledEvent> {
    let journal = runtime.shared.journal.clone();
    let mut head = journal.watch_head();
    let mut polls = 0_u32;
    // One ceiling for the whole wait rather than 60s per wake: a
    // journal that has genuinely stopped (a pump a mutant never
    // started, say) must fail in seconds — under cargo-mutants, a
    // 60s-per-wake wait across a suite reads as TIMEOUT, a mutant
    // nobody evaluated. 15s total is still 3× the wake latency that
    // flaked the old 5s bound, and a passing run never spends it.
    let deadline = tokio::time::Instant::now() + core::time::Duration::from_secs(15);
    loop {
        let events = journal.read_from(1, 100).await.expect("a readable journal");
        if accept(&events) {
            return events;
        }
        polls += 1;
        assert!(polls < 64, "the journal never reached {waiting_for}");
        // A positive wait: something must arrive, and the assertion is
        // on what arrived — the deadline is a ceiling, never a duration
        // anything is expected to take.
        tokio::time::timeout_at(deadline, head.changed())
            .await
            .unwrap_or_else(|_| panic!("the journal stopped moving before {waiting_for}"))
            .expect("the journal outlives the wait");
    }
}

/// Waits until the journal holds at least `minimum` events.
///
/// Only for waits whose assertion is genuinely about arrival order up to
/// a fixed prefix; a wait for "the event I am about to assert on" belongs
/// on [`journal_satisfying`] with that fact as the predicate.
async fn journal_at_least(
    runtime: &Runtime,
    minimum: usize,
) -> Vec<horde_proto::v1::JournaledEvent> {
    journal_satisfying(runtime, &format!("{minimum} events"), |events| {
        events.len() >= minimum
    })
    .await
}

fn kinds(events: &[horde_proto::v1::JournaledEvent]) -> Vec<&'static str> {
    events
        .iter()
        .map(|journaled| {
            match journaled
                .event
                .as_ref()
                .expect("an event")
                .kind
                .as_ref()
                .expect("a kind")
            {
                Kind::AgentLifecycleChanged(_) => "lifecycle",
                Kind::AgentOutputChunk(_) => "output",
                Kind::UsageRecorded(_) => "usage",
                Kind::PermissionRequested(_) => "permission-requested",
                Kind::PermissionDecided(_) => "permission-decided",
                Kind::TurnInterrupted(_) => "interrupted",
                _ => "other",
            }
        })
        .collect()
}

/// The whole ADR-0004 wire, in-process: the Runtime serves the Session's
/// endpoint, mints the agent's identity pair, hands both to the harness —
/// and the scripted harness dials back over real HTTP to report its
/// result, which lands in the journal through the stamping sink under
/// the *authenticated* agent id.
#[tokio::test]
async fn an_agent_reports_its_result_through_its_own_mcp_endpoint() {
    // The control-plane channel dials nowhere: report_result is journal-
    // plane, which is exactly the half this test pins.
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut runtime = runtime_with(Script::of(vec![
        ScriptedTurn::saying("wrapping up").with_step(Step::UseHordeTool {
            tool_call_id: "call-r".to_owned(),
            tool_name: "report_result".to_owned(),
            input: json!({ "status": "success", "summary": "the work is done" }),
        }),
    ]))
    .with_agent_endpoints(channel, "w-test");

    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");

    let reported = |events: &[horde_proto::v1::JournaledEvent]| {
        events.iter().any(|journaled| {
            journaled.event.as_ref().is_some_and(|event| {
                matches!(
                    event.kind.as_ref(),
                    Some(Kind::ResultReported(reported))
                        if reported.result.as_ref()
                            .is_some_and(|result| result.summary == "the work is done")
                ) && event.agent_id.as_ref().map(ToString::to_string) == Some("agent-1".to_owned())
            })
        })
    };
    journal_satisfying(&runtime, "the endpoint-reported result", reported).await;
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_spawned_agent_streams_its_turn_into_the_journal() {
    let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi there")]));
    runtime
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    settle(&runtime).await;
    assert_eq!(runtime.running_agents(), 1);

    // starting, established(running), output, usage.
    let events = journal_at_least(&runtime, 4).await;
    assert_eq!(
        kinds(&events),
        vec!["lifecycle", "lifecycle", "output", "usage"],
        "the whole turn reaches the journal in order"
    );
    assert_eq!(
        runtime
            .shared
            .journal
            .applied_command_sequence()
            .await
            .expect("a watermark"),
        1
    );
    runtime.shutdown().await;
}

/// The at-least-once contract: a redelivered spawn must not start a second
/// harness or append a second effect.
#[tokio::test]
async fn a_redelivered_spawn_neither_respawns_nor_rejournals() {
    let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
    runtime
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 4).await.len();

    runtime
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("a redelivery is not an error");
    settle(&runtime).await;

    assert_eq!(runtime.running_agents(), 1, "one agent, not two");
    let after = runtime
        .shared
        .journal
        .read_from(1, 100)
        .await
        .expect("a readable journal");
    assert_eq!(after.len(), settled, "nothing was re-journaled");
    runtime.shutdown().await;
}

/// A turn that proposes `Bash ls` as tool call `call-1` and blocks on the
/// decision — the shape both permission tests drive.
///
/// The stall behind the ask pins the interleaving that made the
/// unblocking test flake: an answered permission releases the tool
/// result *before* the turn's ending, as two separate journal appends,
/// and the stall holds that gap open for real time. A wait that infers
/// "the turn ended" from an event count reads the journal inside the gap
/// and fails deterministically here, instead of only under a loaded
/// scheduler. The refusal test never reaches the stall — a session
/// waiting on a decision plays no further steps — so it costs nothing
/// there.
fn asking_permission() -> Script {
    Script::of(vec![
        ScriptedTurn::saying("checking")
            .with_step(Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: json!({ "command": "ls" }),
            })
            .with_step(Step::Stall(core::time::Duration::from_millis(250))),
    ])
}

#[tokio::test]
async fn a_permission_decision_unblocks_the_waiting_agent() {
    let mut runtime = runtime_with(asking_permission());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");

    // The request must reach the journal, or nobody can ever answer it.
    journal_satisfying(&runtime, "the permission request", |events| {
        kinds(events).contains(&"permission-requested")
    })
    .await;

    runtime
        .apply(decision_command(2, "agent-1", "call-1", None))
        .await
        .expect("the decision applies");

    // The turn resumes and closes: a usage record arrives. Waited for as
    // a fact, not a count — the resumed turn journals its tool result
    // ahead of its ending, so "five events" is a journal that may not
    // have ended anything yet (the interleaving the script's stall pins).
    let events = journal_satisfying(&runtime, "a usage record", |events| {
        kinds(events).contains(&"usage")
    })
    .await;
    assert!(
        !kinds(&events).contains(&"permission-decided"),
        "the decision is the Server's event; journaling it here doubles it"
    );
    runtime.shutdown().await;
}

/// The reject-don't-drop rule, end to end: a narrowing decision toward an
/// adapter that cannot rewrite is never silently widened — the call is
/// denied and the log says why — and it is that command's failure, not
/// the Worker's: the command completes, the turn ends, and the agent
/// lives on.
#[tokio::test]
async fn a_narrowing_decision_is_refused_when_the_adapter_cannot_rewrite() {
    let mut runtime = preauthorize_only_runtime(asking_permission());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    journal_at_least(&runtime, 4).await;

    runtime
        .apply(decision_command(
            2,
            "agent-1",
            "call-1",
            Some(Struct::default()),
        ))
        .await
        .expect("an unappliable decision is the command's failure, not the loop's");

    let events = journal_satisfying(&runtime, "the denial and the turn's end", |events| {
        diagnostic_saying(events, "denied tool call call-1") && kinds(events).contains(&"usage")
    })
    .await;
    assert!(
        !failed_with_reason(&events, |_| true),
        "a refused decision does not fail the agent: {events:?}"
    );
    settle(&runtime).await;
    assert_eq!(runtime.running_agents(), 1);
    runtime.shutdown().await;
}

/// Whether any journaled event is a harness diagnostic containing `needle`.
fn diagnostic_saying(events: &[horde_proto::v1::JournaledEvent], needle: &str) -> bool {
    events.iter().any(|journaled| {
        matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentOutputChunk(chunk))
                if chunk.channel
                    == i32::from(horde_proto::v1::AgentOutputChannel::HarnessDiagnostic)
                    && chunk.text.contains(needle)
        )
    })
}

/// A decision missing its request id cannot be applied, and is journaled
/// against its agent as a diagnostic naming the command — while the
/// Worker keeps running every agent, this one included.
#[tokio::test]
async fn a_malformed_command_is_journaled_against_its_agent() {
    let mut runtime = runtime_with(asking_permission());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    journal_satisfying(&runtime, "the permission request", |events| {
        kinds(events).contains(&"permission-requested")
    })
    .await;

    let mut malformed = decision_command(2, "agent-1", "call-1", None);
    if let Some(Command::DeliverPermissionDecision(deliver)) = &mut malformed.command
        && let Some(decision) = &mut deliver.decision
    {
        decision.request_id = None;
    }
    runtime
        .apply(malformed)
        .await
        .expect("a malformed command is not fatal");
    journal_satisfying(&runtime, "the diagnostic", |events| {
        diagnostic_saying(events, "could not apply command s1:102")
            && diagnostic_saying(events, "permission request id")
    })
    .await;

    // The agent still answers to a well-formed decision.
    runtime
        .apply(decision_command(3, "agent-1", "call-1", None))
        .await
        .expect("the decision applies");
    journal_satisfying(&runtime, "the turn's end", |events| {
        kinds(events).contains(&"usage")
    })
    .await;
    runtime.shutdown().await;
}

/// A command for an agent this Worker is not running is a race with the
/// agent ending, not a failure: it applies (advancing the ack) and
/// forwards to nobody.
#[tokio::test]
async fn a_command_for_an_unknown_agent_still_applies() {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 1,
            command_id: "s1:9".to_owned(),
            log_position: 0,
            command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                agent_id: Some(AgentId::new("nobody")),
                reason: "cleanup".to_owned(),
            })),
        })
        .await
        .expect("applies with nobody to tell");

    assert_eq!(
        runtime
            .shared
            .journal
            .applied_command_sequence()
            .await
            .expect("a watermark"),
        1,
        "the ack must advance or the server redelivers forever"
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn archiving_ends_the_agent_and_journals_the_transition() {
    let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
    runtime
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    journal_at_least(&runtime, 4).await;

    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 2,
            command_id: "s1:8".to_owned(),
            log_position: 0,
            command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                agent_id: Some(AgentId::new("agent-1")),
                reason: "done with it".to_owned(),
            })),
        })
        .await
        .expect("the archive applies");
    settle(&runtime).await;

    assert_eq!(runtime.running_agents(), 0);
    let events = journal_at_least(&runtime, 5).await;
    let archived = events
        .iter()
        .filter_map(|journaled| journaled.event.as_ref()?.kind.as_ref())
        .any(|kind| {
            matches!(
                kind,
                Kind::AgentLifecycleChanged(changed)
                    if changed.state
                        == i32::from(horde_proto::v1::AgentLifecycleState::Archived)
                        && changed.reason == "done with it"
            )
        });
    assert!(archived, "the archive is an event like everything else");
    // Shutdown must complete even though the pump was already retired.
    runtime.shutdown().await;
}

fn deliver_command(
    sequence: u64,
    agent: &str,
    body: deliver_message_command::Body,
    delivery: DeliveryMode,
) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: sequence,
        command_id: format!("s1:{}", sequence + 200),
        log_position: 0,
        command: Some(Command::DeliverMessage(DeliverMessageCommand {
            agent_id: Some(AgentId::new(agent)),
            body: Some(body),
            delivery: delivery.into(),
        })),
    }
}

/// An `agent-1` mid-conversation: spawned with an echoing script, its
/// opening turn settled. Returns the runtime and the settled event count.
async fn spawned_echoing_agent() -> (Runtime, usize) {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(spawn_command(1, "agent-1", "first"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 4).await.len();
    (runtime, settled)
}

/// The delivered text must reach the agent — the fake echoes it, and the
/// echo can only get back to the journal through the whole pump.
#[tokio::test]
async fn a_delivered_note_reaches_the_agent_and_its_echo_the_journal() {
    let (mut runtime, settled) = spawned_echoing_agent().await;

    runtime
        .apply(deliver_command(
            2,
            "agent-1",
            deliver_message_command::Body::Note(horde_proto::v1::Note {
                text: "a mid-life note".to_owned(),
                payload: None,
            }),
            DeliveryMode::TurnBoundary,
        ))
        .await
        .expect("the delivery applies");

    let events = journal_at_least(&runtime, settled + 2).await;
    let echoed = events.iter().any(|journaled| {
        matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("a mid-life note")
        )
    });
    assert!(
        echoed,
        "the note must reach the agent, not just the journal"
    );
    runtime.shutdown().await;
}

/// Did any journaled event mark the agent Failed, with a reason this
/// predicate accepts? The loud-failure tests differ only in the reason.
fn failed_with_reason(
    events: &[horde_proto::v1::JournaledEvent],
    reason: impl Fn(&str) -> bool,
) -> bool {
    events.iter().any(|journaled| {
        matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentLifecycleChanged(changed))
                if changed.state == i32::from(horde_proto::v1::AgentLifecycleState::Failed)
                    && reason(&changed.reason)
        )
    })
}

/// The delivery mode is load-bearing: an interrupt-mode message toward a
/// provider that cannot interrupt must fail the agent loudly (the adapter
/// refuses it), never quietly downgrade to a queued delivery. This is the
/// observable difference between the Interrupt arm existing and not.
#[tokio::test]
async fn an_interrupt_delivery_is_refused_by_an_uninterruptible_provider() {
    let mut runtime = preauthorize_only_runtime(Script::echoing());
    runtime
        .apply(spawn_command(1, "agent-1", ""))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 1).await.len();

    runtime
        .apply(deliver_command(
            2,
            "agent-1",
            deliver_message_command::Body::Assignment(Assignment {
                assignment_id: None,
                text: "stop and do this".to_owned(),
                payload: None,
            }),
            DeliveryMode::Interrupt,
        ))
        .await
        .expect("the command applies; the refusal is the agent's failure");

    let events = journal_at_least(&runtime, settled + 1).await;
    let refused = failed_with_reason(&events, |reason| reason.contains("does not support"));
    assert!(
        refused,
        "an interrupt the provider cannot honour must fail loudly, not \
             quietly queue: {events:?}"
    );
    runtime.shutdown().await;
}

/// An interrupt command journals the interruption as its effect, with the
/// reason and the agent's session — the audit trail of who stopped what.
#[tokio::test]
async fn an_interrupt_command_journals_the_interruption() {
    let (mut runtime, settled) = spawned_echoing_agent().await;

    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 2,
            command_id: "s1:300".to_owned(),
            log_position: 0,
            command: Some(Command::InterruptAgent(
                horde_proto::v1::InterruptAgentCommand {
                    agent_id: Some(AgentId::new("agent-1")),
                    reason: "the user pressed escape".to_owned(),
                },
            )),
        })
        .await
        .expect("the interrupt applies");

    let events = journal_at_least(&runtime, settled + 1).await;
    let interrupted = events.iter().find_map(|journaled| {
        let event = journaled.event.as_ref()?;
        match event.kind.as_ref()? {
            Kind::TurnInterrupted(interrupted) => Some((
                interrupted.reason.clone(),
                journaled.session_id.as_ref().map(ToString::to_string),
            )),
            _ => None,
        }
    });
    let (reason, session) = interrupted.expect("the interruption is journaled");
    assert_eq!(reason, "the user pressed escape");
    assert_eq!(
        session.as_deref(),
        Some("s1"),
        "the effect lands in the agent's own session log"
    );
    runtime.shutdown().await;
}

/// A decision for a call nobody is waiting on is a race, not a crash: the
/// pump logs it and the agent lives on.
#[tokio::test]
async fn a_decision_for_no_outstanding_call_leaves_the_agent_alive() {
    let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    journal_at_least(&runtime, 4).await;

    runtime
        .apply(decision_command(2, "agent-1", "no-such-call", None))
        .await
        .expect("a stray decision is not an error");
    settle(&runtime).await;

    assert_eq!(runtime.running_agents(), 1, "the agent must survive");
    assert_eq!(
        runtime.shutdown().await,
        1,
        "and its pump is still reapable"
    );
}

/// A harness that died between turns is revived to take the message
/// that found it dead — and when the revived harness cannot take it
/// either, the agent fails with a journaled reason instead of reviving
/// forever. Silence here would leave a permanently hung agent that looks
/// merely idle.
#[tokio::test]
async fn a_message_after_the_harness_died_fails_the_agent_loudly() {
    let mut runtime =
        runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]).ending_after_the_script());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 4).await.len();

    // The script is spent and the fake behaves as a dead harness — in
    // every incarnation. The delivery must surface that, not hang.
    runtime
        .apply(note_command(2, "anyone home?"))
        .await
        .expect("the delivery applies; the death is the agent's failure");

    let events = journal_satisfying(&runtime, "the revival and its failure", |events| {
        failed_with_reason(&events[settled..], |_| true)
    })
    .await;
    let revived = events[settled..].iter().any(|journaled| {
        matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentLifecycleChanged(changed)) if changed.reason.contains("revived")
        )
    });
    assert!(revived, "the idle death was revived first: {events:?}");
    runtime.shutdown().await;
}

/// The EPIPE case (ADR-0012): a harness that died between turns is found
/// dead by the next send, revived, and handed the same message — which
/// the new incarnation answers. Nothing is lost, and nothing fails.
#[tokio::test]
async fn a_message_to_a_harness_that_died_idle_reaches_its_revival() {
    let mut runtime = runtime_with(Script::of(vec![
        ScriptedTurn::saying("last words").with_step(Step::Exit),
        // The ordinal a revival skips (see `resume_session`).
        ScriptedTurn::saying("never played"),
        ScriptedTurn::saying("back from the dead"),
    ]));
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 4).await.len();

    runtime
        .apply(note_command(2, "are you there?"))
        .await
        .expect("the delivery applies");

    let events = journal_satisfying(&runtime, "the revived turn", |events| {
        events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("back from the dead")
            )
        })
    })
    .await;
    assert!(
        !failed_with_reason(&events[settled..], |_| true),
        "an idle death is not the agent's failure: {events:?}"
    );
    assert!(
        runtime
            .shared
            .journal
            .handed_deliveries("agent-1")
            .await
            .expect("readable")
            .is_empty(),
        "the message was settled once the revived harness took it"
    );
    runtime.shutdown().await;
}

/// Steering held for a turn boundary that never comes — the Worker goes
/// down mid-turn — is not lost: the next run replays it from the inbox,
/// and it revives the agent to reach it.
#[tokio::test]
async fn a_held_message_survives_a_worker_restart() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    let mut runtime =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(Script::of(vec![
            ScriptedTurn::saying("working").with_step(Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: json!({ "command": "ls" }),
            }),
        ]))));
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    journal_satisfying(&runtime, "the permission request", |events| {
        kinds(events).contains(&"permission-requested")
    })
    .await;
    runtime
        .apply(note_command(2, "held for the boundary"))
        .await
        .expect("the delivery applies");
    settle(&runtime).await;
    assert_eq!(
        journal
            .handed_deliveries("agent-1")
            .await
            .expect("readable")
            .len(),
        1,
        "handed to the pump, not yet taken by the harness"
    );
    // The Worker goes down with the message still held.
    runtime.shutdown().await;

    // The next run replays the inbox: the held message is a command
    // again, and it revives the agent to reach it.
    let mut second =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(Script::echoing())));
    assert_eq!(second.replay_inbox().await.expect("replays"), 1);
    assert_eq!(second.replay_inbox().await.expect("idempotent"), 0);
    journal_satisfying(&second, "the held message's echo", |events| {
        events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("held for the boundary")
            )
        })
    })
    .await;
    second.shutdown().await;
}

/// The registered adapters flow into the advertisement — an empty
/// iterator here would advertise a worker no harness can run on.
#[tokio::test]
async fn the_registered_adapters_are_what_the_advertisement_sees() {
    let runtime = runtime_with(Script::echoing());
    let advertised = crate::capabilities::advertise(
        runtime.adapters(),
        runtime.workspaces().map(|_| Vec::new()),
        &crate::capabilities::DeclaredFacts::default(),
    );
    assert_eq!(advertised.harnesses.len(), 1);
    assert_eq!(advertised.harnesses[0].provider, "fake");
}

/// Shutdown reports how many pumps it reaped — with two agents running,
/// anything but two means an agent leaked past its shutdown.
#[tokio::test]
async fn shutdown_reaps_every_running_agent() {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(spawn_command(1, "agent-1", "one"))
        .await
        .expect("the first spawn applies");
    runtime
        .apply(spawn_command(2, "agent-2", "two"))
        .await
        .expect("the second spawn applies");
    settle(&runtime).await;

    assert_eq!(runtime.running_agents(), 2);
    assert_eq!(
        runtime.shutdown().await,
        2,
        "both pumps must be waited for, or an agent outlives its worker"
    );
}

/// `Debug` on a runtime is what an operator reads when a worker looks
/// wedged: which adapters it holds and which agents it is running are the
/// two facts that diagnose it — same precedent as the server's views.
#[tokio::test]
async fn the_debug_view_names_the_adapters_and_the_agents() {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    settle(&runtime).await;

    let view = format!("{runtime:?}");
    assert!(view.contains("Runtime"), "got {view}");
    assert!(
        view.contains("fake"),
        "the adapters are half the diagnosis: {view}"
    );
    assert!(
        view.contains("agent-1"),
        "the agents are the other half: {view}"
    );
    runtime.shutdown().await;
}

/// The wire spawn maps onto the adapter spec field by field; a defaulted
/// spec would run every agent on the default model under the ambient
/// account with no overlay, silently.
#[test]
fn the_domain_spec_carries_binding_model_account_overlay_and_mode() {
    let spawn = SpawnAgentCommand {
        binding: Some(horde_proto::v1::ModelBinding {
            tier: String::new(),
            provider: "fake".to_owned(),
            model: "fake-deep".to_owned(),
            provider_account: "work".to_owned(),
        }),
        prompt_overlay: "be brief".to_owned(),
        spec: Some(horde_proto::v1::AgentSpec {
            permission_mode: "cautious".to_owned(),
            ..horde_proto::v1::AgentSpec::default()
        }),
        ..SpawnAgentCommand::default()
    };

    let spec = super::shared::domain_spec(&spawn, None, None);
    assert_eq!(spec.model.as_deref(), Some("fake-deep"));
    assert_eq!(
        spec.account,
        Some(horde_adapter::ProviderAccount::new("work"))
    );
    assert_eq!(spec.append_system_prompt.as_deref(), Some("be brief"));
    assert_eq!(
        spec.permission_mode.as_deref(),
        Some("cautious"),
        "the mode is what a sandboxed-at-spawn provider derives its tier from"
    );
}

/// The empty-string cases are decisions, not accidents: an empty model or
/// account means "the harness's default", and protobuf cannot say `None`.
#[test]
fn empty_wire_strings_mean_absent_not_empty() {
    let spec = super::shared::domain_spec(&SpawnAgentCommand::default(), None, None);
    assert_eq!(spec.model, None, "no binding is not an empty model name");
    assert_eq!(spec.account, None, "no account is the ambient default");
    assert_eq!(
        spec.append_system_prompt, None,
        "no overlay is no flag at all, not an empty one"
    );
    assert_eq!(
        spec.permission_mode, None,
        "an empty mode name is a spawn that named none, not a mode called \"\""
    );
}

// -- Steering, interrupt, and revival --------------------------------------

/// The four corners of the boundary-hold predicate. Inline as
/// `state != Idle && delivery == TurnBoundary` the corners are not
/// separately reachable in most runs, which is exactly how a flipped
/// operator survives an end-to-end suite.
#[test]
fn steering_holds_only_boundary_messages_during_a_turn() {
    use super::pump::{PumpState, should_hold_for_boundary};
    use horde_adapter::TurnInput;

    let queued = TurnInput::queued("later");
    let interrupting = TurnInput::interrupting("now");

    assert!(should_hold_for_boundary(&queued, PumpState::Streaming));
    assert!(should_hold_for_boundary(
        &queued,
        PumpState::AwaitingDecision
    ));
    assert!(
        !should_hold_for_boundary(&queued, PumpState::Idle),
        "an idle agent takes its message immediately"
    );
    assert!(
        !should_hold_for_boundary(&interrupting, PumpState::Streaming),
        "an interrupting message never waits — waiting is what it exists to not do"
    );
}

/// The interrupt state table. The AwaitingDecision arm is the load-bearing
/// one: an interrupt aborts the turn a decision was blocking, so a pump
/// still parked on that decision would wait forever for an answer nobody
/// owes — while Idle must NOT start polling a harness that owes nothing.
#[test]
fn an_interrupt_unparks_only_a_decision_wait() {
    use super::pump::{PumpState, state_after_interrupt};

    assert_eq!(
        state_after_interrupt(PumpState::AwaitingDecision),
        PumpState::Streaming
    );
    assert_eq!(
        state_after_interrupt(PumpState::Streaming),
        PumpState::Streaming
    );
    assert_eq!(state_after_interrupt(PumpState::Idle), PumpState::Idle);
}

/// The send-side ledger. The unreachable corners are pinned here because
/// the pump's own discipline (queue mid-turn, idle means nothing owed)
/// keeps every live run away from them.
#[test]
fn a_send_owes_one_ending_unless_it_also_aborted_a_turn() {
    use super::pump::{PumpState, turns_owed_after_send};

    // The interrupting-send-into-a-live-turn case: two endings owed.
    assert_eq!(turns_owed_after_send(true, PumpState::Streaming, 1), 2);
    assert_eq!(
        turns_owed_after_send(true, PumpState::AwaitingDecision, 1),
        2
    );
    // A queued send owes one, whatever was in flight before it.
    assert_eq!(
        turns_owed_after_send(false, PumpState::Streaming, 1),
        1,
        "a queued send aborts nothing and must not inflate the ledger"
    );
    // An idle interrupting send has nothing to abort.
    assert_eq!(turns_owed_after_send(true, PumpState::Idle, 0), 1);
}

/// The turn-ending state table: an ending is the boundary only when no
/// other turn still owes its ending — the interrupting-send case, where
/// the aborted turn ends while the steered turn is still running.
#[test]
fn a_turn_ending_is_the_boundary_only_when_nothing_else_is_in_flight() {
    use core::time::Duration;

    use horde_adapter::{TurnId, TurnOutcome, TurnStatus, UsageDelta};

    use super::pump::{PumpState, state_after};

    let ended = horde_adapter::AgentEvent::TurnEnded(TurnOutcome {
        turn_id: TurnId::new("t1"),
        status: TurnStatus::Interrupted,
        text: None,
        usage: UsageDelta::default(),
        denials: Vec::new(),
        duration: Duration::from_millis(1),
    });

    assert_eq!(state_after(&ended, 0), PumpState::Idle);
    assert_eq!(
        state_after(&ended, 1),
        PumpState::Streaming,
        "the steered turn is still running; going idle would strand it unread"
    );
}

/// An `agent-1` blocked mid-turn on permission call `call-1`, plus the
/// settled journal length — the preamble every steering and interrupt
/// test shares.
async fn blocked_agent() -> (Runtime, usize) {
    let mut runtime = runtime_with(asking_permission());
    runtime
        .apply(spawn_command(1, "agent-1", "go"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&runtime, 4).await.len();
    (runtime, settled)
}

/// A turn-boundary note to `agent-1` — the routine delivery the revival
/// and steering tests keep sending.
fn note_command(sequence: u64, text: &str) -> StreamCommandsResponse {
    deliver_command(
        sequence,
        "agent-1",
        deliver_message_command::Body::Note(horde_proto::v1::Note {
            text: text.to_owned(),
            payload: None,
        }),
        DeliveryMode::TurnBoundary,
    )
}

/// A restarted Worker: a fresh runtime over `journal` with an echoing
/// adapter, that has just been asked to deliver a note to `agent-1`.
async fn restarted_runtime_delivering(journal: &Journal, sequence: u64) -> Runtime {
    let mut second =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(Script::echoing())));
    second
        .apply(note_command(sequence, "hello?"))
        .await
        .expect("the delivery applies");
    settle(&second).await;
    second
}

/// Steering, end to end: a turn-boundary message that arrives while the
/// agent is blocked mid-turn is held by the pump and delivered at the turn
/// boundary — after the blocked turn ends, never into the middle of it.
#[tokio::test]
async fn a_boundary_message_during_a_blocked_turn_waits_for_the_boundary() {
    let (mut runtime, _settled) = blocked_agent().await;

    // Mid-turn, blocked on the decision: steer.
    runtime
        .apply(note_command(2, "steered note"))
        .await
        .expect("the delivery applies");

    // Only the decision releases the turn; then the held note plays.
    runtime
        .apply(decision_command(3, "agent-1", "call-1", None))
        .await
        .expect("the decision applies");

    let events = journal_at_least(&runtime, 8).await;
    let all = kinds(&events);
    let first_usage = all
        .iter()
        .position(|kind| *kind == "usage")
        .expect("the blocked turn must end: {all:?}");
    let echo = events
        .iter()
        .position(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("steered note")
            )
        })
        .expect("the held note must reach the agent after the boundary");
    assert!(
        echo > first_usage,
        "the steered note must not enter the blocked turn: {all:?}"
    );
    runtime.shutdown().await;
}

/// The interrupt path out of a blocked turn: the turn ends (usage lands)
/// and the agent takes the next message — a pump still parked on the
/// now-dead decision would hang both.
#[tokio::test]
async fn an_interrupt_releases_an_agent_blocked_on_a_decision() {
    let (mut runtime, settled) = blocked_agent().await;

    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 2,
            command_id: "s1:301".to_owned(),
            log_position: 0,
            command: Some(Command::InterruptAgent(
                horde_proto::v1::InterruptAgentCommand {
                    agent_id: Some(AgentId::new("agent-1")),
                    reason: "escape".to_owned(),
                },
            )),
        })
        .await
        .expect("the interrupt applies");

    // interrupted + usage: the aborted turn still closes.
    let events = journal_at_least(&runtime, settled + 2).await;
    assert!(
        kinds(&events).contains(&"usage"),
        "the aborted turn must end and bill: {:?}",
        kinds(&events)
    );

    // And the agent is idle, not wedged: a follow-up message echoes.
    runtime
        .apply(note_command(3, "after the interrupt"))
        .await
        .expect("the delivery applies");
    let events = journal_at_least(&runtime, settled + 4).await;
    assert!(
        events.iter().any(|journaled| matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("after the interrupt")
        )),
        "the interrupted agent must remain steerable"
    );
    runtime.shutdown().await;
}

/// An interrupt-mode delivery cuts through a blocked turn immediately:
/// the blocked turn ends, the message starts its own.
#[tokio::test]
async fn an_interrupting_message_cuts_through_a_blocked_turn() {
    let (mut runtime, settled) = blocked_agent().await;

    runtime
        .apply(deliver_command(
            2,
            "agent-1",
            deliver_message_command::Body::Assignment(Assignment {
                assignment_id: None,
                text: "drop that and do this".to_owned(),
                payload: None,
            }),
            DeliveryMode::Interrupt,
        ))
        .await
        .expect("the delivery applies");

    let events = journal_at_least(&runtime, settled + 3).await;
    let all = kinds(&events);
    assert!(
        all.contains(&"usage"),
        "the aborted turn still closes: {all:?}"
    );
    assert!(
        events.iter().any(|journaled| matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("drop that and do this")
        )),
        "the interrupting message must start its own turn: {all:?}"
    );
    runtime.shutdown().await;
}

/// Revival, the headline path: the Worker restarts, its agents map is
/// empty, and a message for a remembered agent revives the conversation —
/// with the turn numbering continued, so nothing the revived turn journals
/// collides with keys the first incarnation already used.
#[tokio::test]
async fn a_message_after_a_worker_restart_revives_the_agent() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    let script = || {
        Script::of(vec![
            ScriptedTurn::saying("turn one"),
            ScriptedTurn::saying("turn two"),
            ScriptedTurn::saying("turn three"),
        ])
    };

    let mut first =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(script())));
    first
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&first, 4).await.len();
    first.shutdown().await;

    // The restarted Worker: same journal, fresh runtime, no agents.
    let mut second =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(script())));
    assert_eq!(second.running_agents(), 0);

    second
        .apply(note_command(2, "are you still there?"))
        .await
        .expect("the delivery applies by reviving");
    settle(&second).await;

    assert_eq!(second.running_agents(), 1, "the agent lives again");
    let events = journal_at_least(&second, settled + 4).await;

    let revived = journaled_lifecycle(
        &second,
        settled + 4,
        horde_proto::v1::AgentLifecycleState::Starting,
        "revived",
    )
    .await;
    assert!(
        revived,
        "the revival is journaled as a lifecycle transition"
    );

    let continued = events.iter().find_map(|journaled| {
        match journaled
            .event
            .as_ref()
            .and_then(|event| event.kind.as_ref())
        {
            Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("turn three") => {
                Some(chunk.turn_id.clone())
            }
            _ => None,
        }
    });
    assert_eq!(
        continued.as_deref(),
        Some("fake-session-1#3"),
        "one incarnation's turns continue the other's numbering, one \
             ordinal skipped for the turn that may have died half-journaled"
    );
    second.shutdown().await;
}

/// Context dies at archive: an archived agent must not be revivable, even
/// though its spawn was once remembered.
#[tokio::test]
async fn an_archived_agent_cannot_be_revived() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    let mut first =
        Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(Script::echoing())));
    first
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    journal_at_least(&first, 4).await;
    first
        .apply(archive_command(2, "s1:8", "agent-1"))
        .await
        .expect("the archive applies");
    let settled = journal_at_least(&first, 5).await.len();
    first.shutdown().await;

    let second = restarted_runtime_delivering(&journal, 3).await;

    assert_eq!(
        second.running_agents(),
        0,
        "archive discarded the context; nothing may rise from it"
    );
    assert_eq!(
        second
            .shared
            .journal
            .read_from(1, 100)
            .await
            .expect("readable")
            .len(),
        settled,
        "and nothing new is journaled for a message with no recipient"
    );
    second.shutdown().await;
}

/// A revival that cannot resume — the harness never established a provider
/// session — fails the agent loudly instead of leaving the sender waiting
/// on an agent that looks merely slow.
#[tokio::test]
async fn reviving_an_agent_that_never_established_fails_loudly() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    // A misbehaving script that never announces its session: nothing to
    // resume ever gets recorded.
    let mut first = Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(
        Script::of(vec![ScriptedTurn::saying("hi")])
            .misbehaving(horde_adapter_fake::Misbehaviour::SkipSessionEstablished),
    )));
    first
        .apply(spawn_command(1, "agent-1", "hello"))
        .await
        .expect("the spawn applies");
    let settled = journal_at_least(&first, 3).await.len();
    first.shutdown().await;

    let second = restarted_runtime_delivering(&journal, 2).await;

    let events = journal_at_least(&second, settled + 1).await;
    assert!(
        failed_with_reason(&events, |reason| reason.contains("no provider session")),
        "an unrevivable agent must be journaled as failed: {events:?}"
    );
    assert_eq!(second.running_agents(), 0);
    second.shutdown().await;
}

/// This Worker never advertised workspace support, so the command means
/// the handshake promised something it cannot honour. That is reported
/// as a DEGRADED instance naming why — the Server stops preferring this
/// Worker — and never stops the Worker or acks work as done.
#[tokio::test]
async fn materialize_workspace_is_refused_loudly() {
    let mut runtime = runtime_with(Script::echoing());
    let mut command = materialize_command(1, std::path::Path::new("/nowhere"));
    command.command_id = "s1:7".to_owned();
    runtime
        .apply(command)
        .await
        .expect("an unsupported command is not fatal to the Worker");
    let events = journal_at_least(&runtime, 1).await;
    match events[0]
        .event
        .as_ref()
        .and_then(|event| event.kind.as_ref())
    {
        Some(Kind::WorkspaceInstanceChanged(changed)) => {
            let report = changed.report.as_ref().expect("a report");
            assert_eq!(
                report.state,
                horde_proto::v1::WorkspaceInstanceState::Degraded as i32
            );
            assert!(
                report.detail.contains("materialize workspace"),
                "the report names what this worker cannot do: {}",
                report.detail
            );
        }
        other => panic!("expected a degraded instance report, got {other:?}"),
    }
}

/// A materialize command missing what the protocol requires cannot be
/// applied, and with no agent to journal against it is logged — and
/// acked, because redelivering it forever would not make it whole.
#[tokio::test]
async fn an_incomplete_materialize_is_acked_not_fatal() {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 1,
            command_id: "s1:7".to_owned(),
            log_position: 0,
            command: Some(Command::MaterializeWorkspace(
                MaterializeWorkspaceCommand::default(),
            )),
        })
        .await
        .expect("an incomplete command is not fatal");
    settle(&runtime).await;
    assert_eq!(
        runtime
            .shared
            .journal
            .applied_command_sequence()
            .await
            .expect("a watermark"),
        1
    );
    assert!(
        runtime
            .shared
            .journal
            .replay_inbox()
            .await
            .expect("readable")
            .is_empty(),
        "the refused command completed; nothing is left to replay"
    );
}

/// A single-commit origin, built with git itself.
fn tiny_origin(root: &std::path::Path) -> std::path::PathBuf {
    let origin = root.join("origin");
    std::fs::create_dir_all(&origin).expect("origin dir");
    for arguments in [
        ["init", "-q", "-b", "main"].as_slice(),
        [
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            // No detached auto-maintenance left racing the clone.
            "-c",
            "maintenance.auto=false",
            "commit",
            "-qm",
            "seed",
            "--allow-empty",
        ]
        .as_slice(),
    ] {
        let status = std::process::Command::new("git")
            .args(arguments)
            .current_dir(&origin)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {arguments:?} failed");
    }
    origin
}

/// A materialize command for one definition cloning `origin`.
fn materialize_command(sequence: u64, origin: &std::path::Path) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: sequence,
        command_id: format!("s1:{sequence}"),
        log_position: 0,
        command: Some(Command::MaterializeWorkspace(MaterializeWorkspaceCommand {
            definition: Some(horde_proto::v1::WorkspaceDefinition {
                workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new("ws-1")),
                name: "atlas".to_owned(),
                repos: vec![horde_proto::v1::WorkspaceRepo {
                    name: "atlas".to_owned(),
                    remote_url: origin.display().to_string(),
                    default_branch: "main".to_owned(),
                    layout_path: String::new(),
                }],
                unmanaged_paths: Vec::new(),
                created_at: None,
                etag: "1".to_owned(),
            }),
            session_id: Some(SessionId::new("s1")),
        })),
    }
}

/// A workspace-supporting runtime whose `ws-1` instance is already
/// materialized from a fresh tiny origin. The tempdir rides back so the
/// checkouts live as long as the test.
async fn materialized_runtime(script: Script) -> (tempfile::TempDir, std::path::PathBuf, Runtime) {
    let temp = tempfile::tempdir().expect("a tempdir");
    let origin = tiny_origin(temp.path());
    let workspaces_root = temp.path().join("workspaces");
    let mut runtime = runtime_with(script).with_workspaces(
        crate::workspace::WorkspaceManager::new("worker-a", workspaces_root.clone()),
    );
    runtime
        .apply(materialize_command(1, &origin))
        .await
        .expect("the materialize applies");
    settle(&runtime).await;
    (temp, workspaces_root, runtime)
}

/// The full workspace path through the runtime: materialize journals what
/// landed on disk, and the spawn that follows runs in its checkout.
#[tokio::test]
async fn a_materialize_then_spawn_runs_the_agent_in_its_checkout() {
    let (_temp, workspaces_root, mut runtime) =
        materialized_runtime(Script::of(vec![ScriptedTurn::saying("hi")])).await;

    let events = journal_at_least(&runtime, 1).await;
    match events[0]
        .event
        .as_ref()
        .and_then(|event| event.kind.as_ref())
    {
        Some(Kind::WorkspaceInstanceChanged(changed)) => {
            let report = changed.report.as_ref().expect("a report");
            assert_eq!(
                report.state,
                horde_proto::v1::WorkspaceInstanceState::Ready as i32,
                "detail: {}",
                report.detail
            );
        }
        other => panic!("expected a workspace instance report, got {other:?}"),
    }

    let spawn = workspace_spawn(2, "agent-1", "ws-1");
    runtime.apply(spawn).await.expect("the spawn applies");
    settle(&runtime).await;

    assert_eq!(runtime.running_agents(), 1);
    assert!(
        workspaces_root
            .join("ws-1")
            .join(".horde-agents")
            .join("agent-1")
            .join("atlas")
            .join(".git")
            .is_file(),
        "the agent's worktree checkout was composed before the harness started"
    );
    runtime.shutdown().await;
}

/// Only a payload carrying something is worth obtaining: git refs,
/// attachments, or both. An attachments-only payload skipped here would
/// silently deliver a message naming files that never arrive.
#[test]
fn a_payload_is_carried_when_it_holds_refs_or_attachments() {
    use horde_proto::v1::{Attachment, GitRef, Payload};
    let refs_only = Payload {
        git_refs: vec![GitRef::default()],
        attachments: Vec::new(),
    };
    let attachments_only = Payload {
        git_refs: Vec::new(),
        attachments: vec![Attachment::default()],
    };
    let empty = Payload::default();

    assert!(carried_payload(Some(&refs_only)).is_some());
    assert!(carried_payload(Some(&attachments_only)).is_some());
    assert!(
        carried_payload(Some(&empty)).is_none(),
        "an empty payload obtains nothing"
    );
    assert!(carried_payload(None).is_none());
}

/// The receiving half of the attachment seam, through the whole
/// runtime: a delivered note's inline attachment becomes a verified
/// file under the agent's checkout root before the text reaches it.
#[tokio::test]
async fn a_delivered_attachment_is_materialized_into_the_agents_checkout() {
    let (_temp, workspaces_root, mut runtime) = materialized_runtime(Script::echoing()).await;
    runtime
        .apply(workspace_spawn(2, "agent-1", "ws-1"))
        .await
        .expect("the spawn applies");
    settle(&runtime).await;
    assert_eq!(runtime.running_agents(), 1);

    let content = b"the failing assertion".to_vec();
    let attachment = horde_proto::v1::Attachment {
        name: "failing-test.log".to_owned(),
        blob_digest: horde_proto::blob::BlobDigest::of(&content).to_string(),
        size_bytes: content.len() as u64,
        media_type: "text/plain".to_owned(),
        inline: content.clone(),
    };
    let digest_prefix: String = horde_proto::blob::BlobDigest::of(&content)
        .hex()
        .chars()
        .take(12)
        .collect();
    runtime
        .apply(deliver_command(
            3,
            "agent-1",
            deliver_message_command::Body::Note(horde_proto::v1::Note {
                text: "see the attached log".to_owned(),
                payload: Some(horde_proto::v1::Payload {
                    git_refs: Vec::new(),
                    attachments: vec![attachment],
                }),
            }),
            DeliveryMode::TurnBoundary,
        ))
        .await
        .expect("the delivery applies");
    settle(&runtime).await;

    let materialized = workspaces_root
        .join("ws-1")
        .join(".horde-agents")
        .join("agent-1")
        .join(crate::blob::ATTACHMENTS_DIR)
        .join(format!("{digest_prefix}-failing-test.log"));
    assert_eq!(
        std::fs::read(&materialized).expect("the attachment file exists"),
        content,
        "the file is the verified content, in place before the agent reads the note"
    );
    runtime.shutdown().await;
}

/// An attachment for an agent with nowhere to put it fails the agent
/// loudly — delivering text that names files nobody materialized would
/// be a silent drop with a delayed symptom.
#[tokio::test]
async fn an_attachment_for_a_checkoutless_agent_fails_the_delivery_honestly() {
    let (mut runtime, settled) = spawned_echoing_agent().await;

    runtime
        .apply(deliver_command(
            2,
            "agent-1",
            deliver_message_command::Body::Note(horde_proto::v1::Note {
                text: "see the attachment".to_owned(),
                payload: Some(horde_proto::v1::Payload {
                    git_refs: Vec::new(),
                    attachments: vec![horde_proto::v1::Attachment {
                        name: "shot.png".to_owned(),
                        blob_digest: horde_proto::blob::BlobDigest::of(b"px").to_string(),
                        size_bytes: 2,
                        media_type: String::new(),
                        inline: b"px".to_vec(),
                    }],
                }),
            }),
            DeliveryMode::TurnBoundary,
        ))
        .await
        .expect("the command applies; the agent is what fails");

    let failed = journaled_lifecycle(
        &runtime,
        settled + 1,
        horde_proto::v1::AgentLifecycleState::Failed,
        "no checkout to receive",
    )
    .await;
    assert!(failed, "the failure must be journaled with its reason");
    runtime.shutdown().await;
}

/// Whether the journal (grown to `at_least` events) holds a lifecycle
/// transition into `state` whose reason contains `needle` — the shape
/// every "did the runtime say why" assertion shares.
async fn journaled_lifecycle(
    runtime: &Runtime,
    at_least: usize,
    state: horde_proto::v1::AgentLifecycleState,
    needle: &str,
) -> bool {
    let events = journal_at_least(runtime, at_least).await;
    events.iter().any(|journaled| {
        matches!(
            journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentLifecycleChanged(changed))
                if changed.state == i32::from(state) && changed.reason.contains(needle)
        )
    })
}

/// The advertisement builder reads workspace support from this accessor,
/// so it has to report what the builder actually installed — a `None`
/// here would un-advertise a capability the runtime honours.
#[test]
fn workspace_support_is_what_the_builder_installed() {
    let bare = Runtime::new(Journal::in_memory().expect("a journal"));
    assert!(bare.workspaces().is_none());

    let temp = tempfile::tempdir().expect("a tempdir");
    let with = Runtime::new(Journal::in_memory().expect("a journal")).with_workspaces(
        crate::workspace::WorkspaceManager::new("worker-a", temp.path()),
    );
    assert!(with.workspaces().is_some());
}

/// A spawn command whose spec names `workspace`, under the worktree
/// policy — the shape the server sends once placement chose this worker.
fn workspace_spawn(sequence: u64, agent: &str, workspace: &str) -> StreamCommandsResponse {
    let mut spawn = spawn_command(sequence, agent, "hello");
    if let Some(Command::SpawnAgent(command)) = &mut spawn.command
        && let Some(spec) = &mut command.spec
    {
        spec.workspace_definition_id = Some(horde_proto::v1::WorkspaceDefinitionId::new(workspace));
        spec.checkout_policy = horde_proto::v1::CheckoutPolicy::Worktree as i32;
    }
    spawn
}

/// Applies a spawn that must fail the *agent* and not the command loop,
/// returning the journaled failure's reason for the caller to inspect.
async fn failed_spawn_reason(runtime: &mut Runtime, command: StreamCommandsResponse) -> String {
    runtime
        .apply(command)
        .await
        .expect("the command applies; the agent is what failed");
    settle(runtime).await;

    assert_eq!(runtime.running_agents(), 0);
    let events = journal_at_least(runtime, 1).await;
    match events[0]
        .event
        .as_ref()
        .and_then(|event| event.kind.as_ref())
    {
        Some(Kind::AgentLifecycleChanged(changed)) => {
            assert_eq!(
                changed.state,
                i32::from(horde_proto::v1::AgentLifecycleState::Failed)
            );
            changed.reason.clone()
        }
        other => panic!("expected a failed lifecycle, got {other:?}"),
    }
}

/// A spawn naming a workspace on a Worker with no workspace root fails
/// the *agent* honestly rather than the command loop.
#[tokio::test]
async fn a_workspace_spawn_without_support_fails_the_agent_honestly() {
    let mut runtime = runtime_with(Script::echoing());
    let reason = failed_spawn_reason(&mut runtime, workspace_spawn(1, "agent-1", "ws-1")).await;
    assert!(
        reason.contains("workspace"),
        "the reason names the missing half: {reason}"
    );
}

/// `command`, bound to a provider this test Runtime has no adapter for.
fn bound_to_codex(mut command: StreamCommandsResponse) -> StreamCommandsResponse {
    if let Some(Command::SpawnAgent(spawn)) = &mut command.command {
        spawn.binding = Some(horde_proto::v1::ModelBinding {
            provider: "codex".to_owned(),
            ..horde_proto::v1::ModelBinding::default()
        });
    }
    command
}

/// An archive of `agent`.
fn archive_command(sequence: u64, command_id: &str, agent: &str) -> StreamCommandsResponse {
    StreamCommandsResponse {
        command_sequence: sequence,
        command_id: command_id.to_owned(),
        log_position: 0,
        command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
            agent_id: Some(AgentId::new(agent)),
            reason: "done".to_owned(),
        })),
    }
}

/// A spawn that cannot start fails the *agent*, not the command loop: the
/// command applies with the failure as its journaled effect.
#[tokio::test]
async fn a_spawn_naming_an_unknown_provider_fails_the_agent_not_the_loop() {
    let mut runtime = runtime_with(Script::echoing());
    let command = bound_to_codex(spawn_command(1, "agent-1", "hello"));

    let reason = failed_spawn_reason(&mut runtime, command).await;
    assert!(
        reason.contains("codex"),
        "the reason names the missing adapter: {reason}"
    );
}

/// A spawn that minted an endpoint and composed a checkout but never got
/// a harness gives both back: the Session's endpoint (bound for nobody
/// else) is released, and the checkout is gone from disk — the agent is
/// not remembered, so nothing would ever reuse or release either.
#[tokio::test]
async fn a_spawn_that_cannot_start_releases_what_it_minted() {
    let (_temp, workspaces_root, runtime) = materialized_runtime(Script::echoing()).await;
    let channel = tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut runtime = runtime.with_agent_endpoints(channel, "w-test");
    let command = bound_to_codex(workspace_spawn(2, "agent-1", "ws-1"));

    runtime
        .apply(command)
        .await
        .expect("the command applies; the agent is what failed");
    settle(&runtime).await;
    assert_eq!(runtime.running_agents(), 0);
    // events[0] is the materialize report; the spawn's failure follows.
    let events = journal_at_least(&runtime, 2).await;
    assert!(
        matches!(
            events[1].event.as_ref().and_then(|event| event.kind.as_ref()),
            Some(Kind::AgentLifecycleChanged(changed))
                if changed.state == i32::from(horde_proto::v1::AgentLifecycleState::Failed)
                    && changed.reason.contains("codex")
        ),
        "the spawn failed the agent: {:?}",
        events[1]
    );

    assert!(
        !workspaces_root
            .join("ws-1")
            .join(".horde-agents")
            .join("agent-1")
            .exists(),
        "the failed spawn's checkout is released"
    );
    let endpoints = runtime
        .shared
        .endpoints
        .clone()
        .expect("endpoints are wired");
    assert_eq!(
        endpoints.shutdown().await,
        0,
        "the endpoint bound for the failed spawn was already released"
    );
}

/// An agent archived with no pump — here, after a Worker restart, so
/// nothing is running it — still gives its checkout back (ADR-0010):
/// HEAD pinned under `refs/horde/archived/<agent>`, then the worktree
/// removed. A shutdown alone keeps the checkout, for the revival.
#[tokio::test]
async fn archiving_an_agent_with_no_pump_still_gives_its_checkout_back() {
    let (_temp, workspaces_root, mut first) =
        materialized_runtime(Script::of(vec![ScriptedTurn::saying("hi")])).await;
    first
        .apply(workspace_spawn(2, "agent-1", "ws-1"))
        .await
        .expect("the spawn applies");
    settle(&first).await;
    let journal = first.shared.journal.clone();
    first.shutdown().await;
    let checkout = workspaces_root
        .join("ws-1")
        .join(".horde-agents")
        .join("agent-1");
    assert!(checkout.exists(), "a shutdown is not an archive");

    let mut second = Runtime::new(journal)
        .with_adapter(Arc::new(FakeAdapter::new(Script::echoing())))
        .with_workspaces(crate::workspace::WorkspaceManager::new(
            "worker-a",
            workspaces_root.clone(),
        ));
    second
        .apply(archive_command(3, "s1:3", "agent-1"))
        .await
        .expect("the archive applies");
    settle(&second).await;
    assert!(
        !checkout.exists(),
        "the pumpless archived agent's checkout is released"
    );
    let pinned = std::process::Command::new("git")
        .args(["rev-parse", "--verify", "refs/horde/archived/agent-1"])
        .current_dir(workspaces_root.join("ws-1").join("atlas"))
        .output()
        .expect("git runs");
    assert!(pinned.status.success(), "HEAD was pinned before removal");
    second.shutdown().await;
}

/// A long effect on one lane holds up nobody else's: while a
/// materialize is stuck on a remote that never answers, another agent's
/// spawn and message run to completion — and a spawn naming the stuck
/// workspace waits behind it, as the one sequential loop made it.
#[tokio::test]
async fn a_slow_materialize_blocks_only_what_depends_on_it() {
    let temp = tempfile::tempdir().expect("a tempdir");
    let mut runtime = runtime_with(Script::echoing()).with_workspaces(
        crate::workspace::WorkspaceManager::new("worker-a", temp.path().join("workspaces")),
    );
    // A remote that accepts and then says nothing, until released.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let port = listener.local_addr().expect("an address").port();
    let (connected, git_connected) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let Ok((socket, _)) = listener.accept().await else {
            return;
        };
        let _ = connected.send(());
        let _ = released.await;
        drop(socket);
    });
    let mut materialize = materialize_command(
        1,
        std::path::Path::new(&format!("http://127.0.0.1:{port}/stuck.git")),
    );
    materialize.command_id = "s1:m".to_owned();
    runtime.apply(materialize).await.expect("accepted");
    tokio::time::timeout(core::time::Duration::from_secs(15), git_connected)
        .await
        .expect("git dials the stuck remote")
        .expect("the remote saw it");

    runtime
        .apply(workspace_spawn(2, "agent-a", "ws-1"))
        .await
        .expect("accepted");
    runtime
        .apply(spawn_command(3, "agent-b", "first"))
        .await
        .expect("accepted");
    runtime
        .apply(deliver_command(
            4,
            "agent-b",
            deliver_message_command::Body::Note(horde_proto::v1::Note {
                text: "right behind the spawn".to_owned(),
                payload: None,
            }),
            DeliveryMode::TurnBoundary,
        ))
        .await
        .expect("accepted");

    let events = journal_satisfying(&runtime, "agent-b's message echoed", |events| {
        events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("right behind the spawn")
            )
        })
    })
    .await;
    let about = |agent: &str| {
        events.iter().any(|journaled| {
            journaled
                .event
                .as_ref()
                .and_then(|event| event.agent_id.as_ref())
                .is_some_and(|id| id.to_string() == agent)
        })
    };
    assert!(
        !about("agent-a"),
        "the spawn naming the stuck workspace waits for it: {events:?}"
    );
    assert!(
        !events.iter().any(|journaled| matches!(
            journaled
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref()),
            Some(Kind::WorkspaceInstanceChanged(_))
        )),
        "the materialize is still stuck while agent-b runs"
    );

    release.send(()).expect("the remote is holding");
    let events = journal_satisfying(&runtime, "agent-a's spawn, after the report", |events| {
        events.iter().any(|journaled| {
            journaled
                .event
                .as_ref()
                .and_then(|event| event.agent_id.as_ref())
                .is_some_and(|id| id.to_string() == "agent-a")
        })
    })
    .await;
    let report = events.iter().position(|journaled| {
        matches!(
            journaled
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref()),
            Some(Kind::WorkspaceInstanceChanged(_))
        )
    });
    let spawned = events.iter().position(|journaled| {
        journaled
            .event
            .as_ref()
            .and_then(|event| event.agent_id.as_ref())
            .is_some_and(|id| id.to_string() == "agent-a")
    });
    assert!(report < spawned && report.is_some(), "{events:?}");
    runtime.shutdown().await;
}

/// A command with no body at all is accepted, acked and completed — not
/// redelivered forever, and not fatal.
#[tokio::test]
async fn a_command_with_no_body_is_acked_and_dropped() {
    let mut runtime = runtime_with(Script::echoing());
    runtime
        .apply(StreamCommandsResponse {
            command_sequence: 1,
            command_id: "s1:empty".to_owned(),
            log_position: 5,
            command: None,
        })
        .await
        .expect("not fatal");
    settle(&runtime).await;
    assert_eq!(
        runtime
            .shared
            .journal
            .applied_command_position()
            .await
            .expect("a watermark"),
        5
    );
    assert!(
        runtime
            .shared
            .journal
            .replay_inbox()
            .await
            .expect("readable")
            .is_empty()
    );
    runtime.shutdown().await;
}

/// Archive gives a running agent's checkout back once its pump (and so
/// its harness group) has stopped — and shutdown joins that release.
#[tokio::test]
async fn archiving_a_workspace_agent_gives_its_checkout_back() {
    let (_temp, workspaces_root, mut runtime) =
        materialized_runtime(Script::of(vec![ScriptedTurn::saying("hi")])).await;
    runtime
        .apply(workspace_spawn(2, "agent-1", "ws-1"))
        .await
        .expect("the spawn applies");
    settle(&runtime).await;
    let checkout = workspaces_root
        .join("ws-1")
        .join(".horde-agents")
        .join("agent-1");
    assert!(checkout.exists(), "setup: the checkout was composed");

    runtime
        .apply(archive_command(3, "s1:3", "agent-1"))
        .await
        .expect("the archive applies");
    runtime.shutdown().await;

    assert!(
        !checkout.exists(),
        "the archived agent's checkout is released"
    );
}

/// Finished retired tasks are dropped as new ones arrive, so the list
/// does not grow by one per archive for the Worker's whole life.
#[tokio::test]
async fn finished_retired_tasks_do_not_accumulate() {
    let mut retired = Vec::new();
    super::lane::keep(&mut retired, finished_task().await);
    super::lane::keep(&mut retired, finished_task().await);
    super::lane::keep(&mut retired, finished_task().await);
    assert_eq!(
        retired.len(),
        1,
        "only the newest (maybe unfinished) task is kept"
    );
    super::lane::keep(&mut retired, tokio::spawn(std::future::pending::<()>()));
    super::lane::keep(&mut retired, tokio::spawn(std::future::pending::<()>()));
    assert_eq!(retired.len(), 2, "unfinished tasks are always kept");
    retired.iter().for_each(tokio::task::JoinHandle::abort);
}

/// A task that has provably finished — waited for as a fact, not a sleep.
async fn finished_task() -> tokio::task::JoinHandle<()> {
    let task = tokio::spawn(async {});
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    task
}

/// A pump around `journal`, for driving
/// [`super::pump::Pump::journal_event`] directly at its boundary. The
/// session is a real (scripted) one; the paths under test never reach it.
async fn journaling_pump(journal: Journal) -> super::pump::Pump {
    use horde_adapter::Adapter as _;
    let session = FakeAdapter::default()
        .spawn(&horde_adapter::AgentSpec::default())
        .await
        .expect("a fake session");
    let mut pump = super::pump::Pump::new(
        session,
        journal,
        crate::translate::AgentContext {
            session_id: "s1".to_owned(),
            agent_id: "agent-1".to_owned(),
            binding: None,
            permission_mode: String::new(),
            tree_path: Vec::new(),
        },
    );
    pump.state = super::pump::PumpState::Streaming;
    pump.turns_in_flight = 1;
    pump
}

/// One boundary of `journal_event`'s early-continue guard: an event that
/// yields entries or progress must reach the journal — here a
/// `SessionEstablished`, whose lifecycle entry and revival identity
/// commit in one batch.
#[tokio::test]
async fn an_event_carrying_progress_journals_it_with_its_entries() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    journal
        .apply_command(
            "cmd-1",
            1,
            Vec::new(),
            AgentBookkeeping::Remember {
                agent_id: "agent-1".to_owned(),
                session_id: "s1".to_owned(),
                spawn: Box::new(horde_proto::v1::SpawnAgentCommand::default()),
            },
        )
        .await
        .expect("the agent is remembered");
    let mut pump = journaling_pump(journal.clone()).await;

    let flow = pump
        .journal_event(&horde_adapter::AgentEvent::SessionEstablished {
            provider_session_id: horde_adapter::ProviderSessionId::new("fake-session-9"),
            model: "m1".to_owned(),
        })
        .await;

    assert!(matches!(flow, super::pump::Flow::Continue));
    let events = journal.read_from(1, 100).await.expect("a readable journal");
    assert_eq!(kinds(&events), vec!["lifecycle"]);
    let revivable = journal
        .revivable_agent("agent-1")
        .await
        .expect("a readable agent")
        .expect("a remembered agent");
    assert_eq!(
        revivable.provider_session_id.as_deref(),
        Some("fake-session-9"),
        "the progress must land with the entry, not be skipped by the guard"
    );
}

/// The other boundary: an event yielding neither entries nor progress —
/// a `ToolCompleted`, per the recorded translation decisions — must not
/// touch the journal at all. "At all" includes the head watch: a
/// spurious empty-batch commit would still announce a head and wake the
/// push pump for nothing, so the watch staying quiet is the observable
/// proof the guard took the early continue.
#[tokio::test]
async fn an_event_with_nothing_to_journal_leaves_the_journal_untouched() {
    let journal = Journal::in_memory().expect("an in-memory journal");
    let mut pump = journaling_pump(journal.clone()).await;
    let mut head = journal.watch_head();
    head.mark_unchanged();

    let flow = pump
        .journal_event(&horde_adapter::AgentEvent::ToolCompleted {
            turn_id: horde_adapter::TurnId::new("s1#1"),
            tool_call_id: horde_adapter::ToolCallId::new("call-1"),
            is_error: false,
        })
        .await;

    assert!(matches!(flow, super::pump::Flow::Continue));
    assert!(
        !head.has_changed().expect("the journal outlives the check"),
        "a no-op event must not announce a journal head"
    );
    let events = journal.read_from(1, 100).await.expect("a readable journal");
    assert!(events.is_empty(), "nothing may be appended: {events:?}");
}
