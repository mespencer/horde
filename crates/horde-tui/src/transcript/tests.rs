//! The transcript fold, driven through `SessionState::apply` — the same
//! entry point the App uses — so every block here came from real wire
//! events through the production fold.

use horde_proto::v1::{
    Actor, AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentResult,
    AgentSpawned, AgentSpec, Answer, Assignment, AssignmentSent, ClientId, Note, NoteSent,
    PermissionDecided, PermissionDecision, PermissionOutcome, PermissionRequestId,
    PermissionRequested, Placement, PlacementChanged, PlacementState, ResultReported,
    ResultStatus, ServerActor, TurnInterrupted, WorkspaceInstanceChanged,
    WorkspaceInstanceReport, actor, event::Kind,
};

use super::{Block, Settled, Speaker, TRANSCRIPT_CAPACITY, Tone};
use crate::fixtures::{chunk, decided, envelope, requested, spawned};
use crate::state::SessionState;

/// A session with a root (`root`, "lead") and a child (`kid`, "helper").
fn family() -> SessionState {
    let mut state = SessionState::new("s1");
    state.apply(&envelope(1, "root", spawned(None, &["lead"])));
    state.apply(&envelope(2, "kid", spawned(Some("root"), &["lead", "helper"])));
    state
}

/// Folds `kinds` about `agent` at consecutive positions after `family`'s.
fn fold(state: &mut SessionState, agent: &str, kinds: Vec<Kind>) {
    let start = state.stream_head + 1;
    for (offset, kind) in (0..).zip(kinds) {
        state.apply(&envelope(start + offset, agent, kind));
    }
}

fn blocks(state: &SessionState, agent: &str) -> Vec<Block> {
    state
        .transcripts
        .get(agent)
        .map(|transcript| transcript.blocks.iter().cloned().collect())
        .unwrap_or_default()
}

fn assistant(turn: &str, text: &str) -> Block {
    Block::Assistant {
        turn: turn.to_owned(),
        text: text.to_owned(),
    }
}

fn notice(tone: Tone, text: &str) -> Block {
    Block::Notice {
        tone,
        text: text.to_owned(),
    }
}

fn from(actor: actor::Actor) -> Option<Actor> {
    Some(Actor { actor: Some(actor) })
}

#[test]
fn streamed_text_merges_within_a_turn_and_splits_across_turns() {
    let mut state = family();
    fold(
        &mut state,
        "root",
        vec![
            chunk(AgentOutputChannel::Assistant, "t1", "Hel"),
            chunk(AgentOutputChannel::Assistant, "t1", "lo"),
            chunk(AgentOutputChannel::Reasoning, "t1", "hm"),
            chunk(AgentOutputChannel::Reasoning, "t1", "m"),
            chunk(AgentOutputChannel::Assistant, "t1", "again"),
            chunk(AgentOutputChannel::Unspecified, "t2", "next"),
            chunk(AgentOutputChannel::Reasoning, "t2", "x"),
            chunk(AgentOutputChannel::Reasoning, "t3", "y"),
            chunk(AgentOutputChannel::HarnessDiagnostic, "t3", "retrying"),
        ],
    );
    assert_eq!(
        blocks(&state, "root")[1..],
        [
            assistant("t1", "Hello"),
            Block::Thinking {
                turn: "t1".to_owned(),
                text: "hmm".to_owned()
            },
            assistant("t1", "again"),
            assistant("t2", "next"),
            Block::Thinking {
                turn: "t2".to_owned(),
                text: "x".to_owned()
            },
            Block::Thinking {
                turn: "t3".to_owned(),
                text: "y".to_owned()
            },
        ],
        "the first block is the child's spawn; diagnostics never land"
    );
}

#[test]
fn results_attach_to_their_calls_in_order_within_the_turn() {
    let mut state = family();
    fold(
        &mut state,
        "kid",
        vec![
            chunk(AgentOutputChannel::ToolCall, "t1", "Read {\"file_path\":\"a\"}"),
            chunk(AgentOutputChannel::ToolCall, "t1", "Grep"),
            chunk(AgentOutputChannel::ToolResult, "t1", "contents of a"),
            chunk(AgentOutputChannel::ToolResult, "t2", "stray"),
        ],
    );
    let tool = |name: &str, input: &str, result: Option<&str>, turn: &str| Block::Tool {
        turn: turn.to_owned(),
        name: name.to_owned(),
        input: input.to_owned(),
        result: result.map(ToOwned::to_owned),
    };
    assert_eq!(
        blocks(&state, "kid"),
        vec![
            tool("Read", "{\"file_path\":\"a\"}", Some("contents of a"), "t1"),
            tool("Grep", "", None, "t1"),
            tool("", "", Some("stray"), "t2"),
        ],
        "the oldest waiting call of the same turn takes the result; a result \
         from another turn stands alone"
    );
    fold(&mut state, "kid", vec![chunk(AgentOutputChannel::ToolResult, "t1", "matches")]);
    assert!(
        matches!(&blocks(&state, "kid")[1], Block::Tool { result: Some(text), .. } if text == "matches")
    );
}

#[test]
fn messages_name_who_sent_them() {
    let mut state = family();
    let assignment = |from: Option<Actor>, text: &str| {
        Kind::AssignmentSent(AssignmentSent {
            from,
            assignment: Some(Assignment {
                text: text.to_owned(),
                ..Assignment::default()
            }),
            delivery: 0,
        })
    };
    fold(
        &mut state,
        "kid",
        vec![
            assignment(from(actor::Actor::ClientId(ClientId::new("laptop"))), "do it"),
            assignment(from(actor::Actor::AgentId(AgentId::new("root"))), "from lead"),
            assignment(from(actor::Actor::Server(ServerActor {})), "revived"),
            assignment(None, "anon"),
            Kind::AssignmentSent(AssignmentSent::default()),
            Kind::NoteSent(NoteSent {
                from: None,
                note: Some(Note {
                    text: "fyi".to_owned(),
                    payload: None,
                }),
                delivery: 0,
            }),
            Kind::NoteSent(NoteSent::default()),
        ],
    );
    let message = |from: Speaker, text: &str, note: bool| Block::Message {
        from,
        text: text.to_owned(),
        note,
    };
    assert_eq!(
        blocks(&state, "kid"),
        vec![
            message(Speaker::Human, "do it", false),
            message(Speaker::Agent("lead".to_owned()), "from lead", false),
            message(Speaker::Server, "revived", false),
            message(Speaker::Human, "anon", false),
            message(Speaker::Human, "", false),
            message(Speaker::Human, "fyi", true),
            message(Speaker::Human, "", true),
        ]
    );
}

#[test]
fn a_spawn_shows_in_the_parent_and_opens_the_child_with_its_assignment() {
    let mut state = SessionState::new("s1");
    state.apply(&envelope(1, "root", spawned(None, &["lead"])));
    let born = Kind::AgentSpawned(AgentSpawned {
        parent_agent_id: Some(AgentId::new("root")),
        spawned_by: from(actor::Actor::AgentId(AgentId::new("root"))),
        spec: Some(AgentSpec {
            initial_assignment: Some(Assignment {
                text: "review it".to_owned(),
                ..Assignment::default()
            }),
            ..AgentSpec::default()
        }),
        tree_path: vec!["lead".to_owned(), "reviewer".to_owned()],
    });
    state.apply(&envelope(2, "kid", born));
    let empty_brief = Kind::AgentSpawned(AgentSpawned {
        spec: Some(AgentSpec::default()),
        ..AgentSpawned::default()
    });
    state.apply(&envelope(3, "solo", empty_brief));
    assert_eq!(
        blocks(&state, "root"),
        vec![Block::Child {
            text: "spawned reviewer".to_owned()
        }]
    );
    assert_eq!(
        blocks(&state, "kid"),
        vec![Block::Message {
            from: Speaker::Agent("lead".to_owned()),
            text: "review it".to_owned(),
            note: false
        }]
    );
    assert!(blocks(&state, "solo").is_empty(), "no brief, no opening message");
}

#[test]
fn a_result_lands_in_the_agent_and_its_parent() {
    let mut state = family();
    let reported = Kind::ResultReported(ResultReported {
        result: Some(AgentResult {
            status: ResultStatus::Success as i32,
            summary: "all green".to_owned(),
            ..AgentResult::default()
        }),
    });
    fold(&mut state, "kid", vec![reported]);
    fold(&mut state, "root", vec![Kind::ResultReported(ResultReported::default())]);
    assert_eq!(
        blocks(&state, "kid"),
        vec![Block::Reported {
            status: "success",
            summary: "all green".to_owned()
        }]
    );
    assert_eq!(
        blocks(&state, "root"),
        vec![
            Block::Child {
                text: "spawned helper".to_owned()
            },
            Block::Child {
                text: "helper reported success: all green".to_owned()
            },
            Block::Reported {
                status: "unspecified",
                summary: String::new()
            },
        ],
        "the root has no parent to tell"
    );
}

#[test]
fn prompts_record_how_they_were_settled() {
    let mut state = family();
    let answered = Kind::PermissionDecided(PermissionDecided {
        decision: Some(PermissionDecision {
            request_id: Some(PermissionRequestId::new("q")),
            answers: [
                ("size".to_owned(), Answer { values: vec!["L".to_owned()] }),
                ("colour".to_owned(), Answer { values: vec!["Red".to_owned(), "Blue".to_owned()] }),
            ]
            .into(),
            ..PermissionDecision::default()
        }),
    });
    fold(
        &mut state,
        "kid",
        vec![
            requested("a", "rm a"),
            requested("b", "rm b"),
            requested("q", "AskUserQuestion"),
            requested("open", "ls"),
            Kind::PermissionRequested(PermissionRequested::default()),
            decided("a", PermissionOutcome::Allow, ""),
            decided("b", PermissionOutcome::Deny, "no"),
            answered,
            decided("never-asked", PermissionOutcome::Allow, ""),
            Kind::PermissionDecided(PermissionDecided::default()),
            Kind::PermissionDecided(PermissionDecided {
                decision: Some(PermissionDecision::default()),
            }),
        ],
    );
    let prompt = |id: &str, summary: &str, settled: Option<Settled>| Block::Prompt {
        request_id: id.to_owned(),
        summary: summary.to_owned(),
        settled,
    };
    assert_eq!(
        blocks(&state, "kid"),
        vec![
            prompt("a", "rm a", Some(Settled::Allowed)),
            prompt("b", "rm b", Some(Settled::Denied)),
            prompt(
                "q",
                "AskUserQuestion",
                Some(Settled::Answered("colour → Red, Blue; size → L".to_owned()))
            ),
            prompt("open", "ls", None),
        ],
        "a bodyless request adds nothing; stray decisions change nothing"
    );
}

#[test]
fn the_machinery_worth_reading_becomes_notices() {
    let mut state = family();
    let lifecycle = |state: AgentLifecycleState, reason: &str| {
        Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: 0,
            state: state as i32,
            reason: reason.to_owned(),
        })
    };
    let placement = |state: PlacementState, reason: &str| {
        Kind::PlacementChanged(PlacementChanged {
            placement: Some(Placement {
                state: state as i32,
                reason: reason.to_owned(),
                ..Placement::default()
            }),
            adapter_capabilities: Vec::new(),
        })
    };
    let workspace = |detail: &str| {
        Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
            report: Some(WorkspaceInstanceReport {
                detail: detail.to_owned(),
                ..WorkspaceInstanceReport::default()
            }),
            progress: None,
        })
    };
    fold(
        &mut state,
        "kid",
        vec![
            lifecycle(AgentLifecycleState::Failed, "the provider refused"),
            lifecycle(AgentLifecycleState::Archived, ""),
            lifecycle(AgentLifecycleState::Running, "ignored"),
            placement(PlacementState::Queued, "no worker"),
            placement(PlacementState::Lost, ""),
            placement(PlacementState::Expired, "timeout"),
            placement(PlacementState::Cancelled, ""),
            placement(PlacementState::Placed, "ignored"),
            Kind::PlacementChanged(PlacementChanged::default()),
            Kind::TurnInterrupted(TurnInterrupted {
                reason: "by you".to_owned(),
                ..TurnInterrupted::default()
            }),
            Kind::TurnInterrupted(TurnInterrupted::default()),
            workspace("disk full"),
            workspace(""),
            Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
            Kind::UsageRecorded(horde_proto::v1::UsageRecorded::default()),
        ],
    );
    assert_eq!(
        blocks(&state, "kid"),
        vec![
            notice(Tone::Error, "Failed · the provider refused"),
            notice(Tone::Info, "Archived"),
            notice(Tone::Warn, "Queued · no worker"),
            notice(Tone::Error, "LOST with its worker"),
            notice(Tone::Error, "Placement expired · timeout"),
            notice(Tone::Warn, "Placement cancelled"),
            notice(Tone::Warn, "Interrupted · by you"),
            notice(Tone::Warn, "Interrupted"),
            notice(Tone::Info, "workspace: disk full"),
        ]
    );
}

#[test]
fn a_transcript_keeps_only_its_newest_blocks() {
    let mut state = family();
    let total = TRANSCRIPT_CAPACITY + 2;
    let kinds = (0..total)
        .map(|index| chunk(AgentOutputChannel::Assistant, &format!("t{index}"), "x"))
        .collect();
    fold(&mut state, "kid", kinds);
    let kept = blocks(&state, "kid");
    assert_eq!(kept.len(), TRANSCRIPT_CAPACITY);
    assert_eq!(kept[0], assistant("t2", "x"), "the two oldest fell off");
}

#[test]
fn an_event_without_a_kind_transcribes_nothing() {
    let mut state = family();
    let mut bare = envelope(3, "kid", Kind::TurnInterrupted(TurnInterrupted::default()));
    if let Some(event) = bare.event.as_mut() {
        event.kind = None;
    }
    state.apply(&bare);
    assert!(blocks(&state, "kid").is_empty());
}
