//! Event builders the unit tests share, so every test folds real wire
//! values through the production fold — and so the builders exist once.

use horde_proto::v1::{
    AgentId, AgentOutputChannel, AgentOutputChunk, AgentSpawned, Event, EventEnvelope,
    PermissionDecided, PermissionDecision, PermissionOutcome, PermissionRequest,
    PermissionRequestId, PermissionRequested, ProposedToolCall, Question, QuestionOption,
    SessionId, event::Kind,
};

/// One event at a log position in session `s1`, about one agent (none when
/// `agent` is empty).
pub(crate) fn envelope(sequence: u64, agent: &str, kind: Kind) -> EventEnvelope {
    envelope_in("s1", sequence, agent, kind)
}

/// One event at a log position in a named session.
pub(crate) fn envelope_in(session: &str, sequence: u64, agent: &str, kind: Kind) -> EventEnvelope {
    let event = Event {
        kind: Some(kind),
        agent_id: (!agent.is_empty()).then(|| AgentId::new(agent)),
        ..Event::default()
    };
    EventEnvelope {
        sequence,
        event: Some(event),
        session_id: Some(SessionId::new(session)),
        ..EventEnvelope::default()
    }
}

pub(crate) fn spawned(parent: Option<&str>, tree_path: &[&str]) -> Kind {
    Kind::AgentSpawned(AgentSpawned {
        tree_path: tree_path.iter().map(ToString::to_string).collect(),
        parent_agent_id: parent.map(AgentId::new),
        ..AgentSpawned::default()
    })
}

pub(crate) fn chunk(channel: AgentOutputChannel, turn: &str, text: &str) -> Kind {
    Kind::AgentOutputChunk(AgentOutputChunk {
        channel: channel as i32,
        text: text.to_owned(),
        turn_id: turn.to_owned(),
        chunk_index: 0,
    })
}

pub(crate) fn requested(request_id: &str, rendered: &str) -> Kind {
    let tool_call = ProposedToolCall {
        rendered: rendered.to_owned(),
        ..ProposedToolCall::default()
    };
    Kind::PermissionRequested(PermissionRequested {
        request: Some(PermissionRequest {
            permission_mode: "manual".to_owned(),
            tree_path: vec!["ci-fix".to_owned(), "reviewer".to_owned()],
            tool_call: Some(tool_call),
            request_id: Some(PermissionRequestId::new(request_id)),
            ..PermissionRequest::default()
        }),
    })
}

/// An offered option with no description.
fn choice(label: &str) -> QuestionOption {
    QuestionOption {
        label: label.to_owned(),
        ..QuestionOption::default()
    }
}

/// A question request: one question per `(id, options)` pair.
pub(crate) fn asked(request_id: &str, questions: &[(&str, &[&str])]) -> Kind {
    let tool_call = ProposedToolCall {
        tool_name: "AskUserQuestion".to_owned(),
        rendered: "AskUserQuestion".to_owned(),
        questions: questions
            .iter()
            .map(|(id, options)| Question {
                id: (*id).to_owned(),
                question: format!("{id}?"),
                options: options.iter().copied().map(choice).collect(),
                ..Question::default()
            })
            .collect(),
        ..ProposedToolCall::default()
    };
    Kind::PermissionRequested(PermissionRequested {
        request: Some(PermissionRequest {
            request_id: Some(PermissionRequestId::new(request_id)),
            tool_call: Some(tool_call),
            ..PermissionRequest::default()
        }),
    })
}

pub(crate) fn decided(request_id: &str, outcome: PermissionOutcome, reason: &str) -> Kind {
    Kind::PermissionDecided(PermissionDecided {
        decision: Some(PermissionDecision {
            reason: reason.to_owned(),
            outcome: outcome as i32,
            request_id: Some(PermissionRequestId::new(request_id)),
            ..PermissionDecision::default()
        }),
    })
}

/// An assignment from a human client.
pub(crate) fn assignment(text: &str) -> Kind {
    Kind::AssignmentSent(horde_proto::v1::AssignmentSent {
        from: None,
        assignment: Some(horde_proto::v1::Assignment {
            text: text.to_owned(),
            ..horde_proto::v1::Assignment::default()
        }),
        delivery: 0,
    })
}

/// A placement outcome, on a worker when one is named.
pub(crate) fn placement(state: horde_proto::v1::PlacementState, worker: Option<&str>) -> Kind {
    Kind::PlacementChanged(horde_proto::v1::PlacementChanged {
        placement: Some(horde_proto::v1::Placement {
            state: state as i32,
            worker_id: worker.map(horde_proto::v1::WorkerId::new),
            ..horde_proto::v1::Placement::default()
        }),
        adapter_capabilities: Vec::new(),
    })
}

/// A turn-end usage record carrying a binding.
pub(crate) fn bound(turn: &str, provider: &str, model: &str) -> Kind {
    Kind::UsageRecorded(horde_proto::v1::UsageRecorded {
        record: Some(horde_proto::v1::UsageRecord {
            binding: Some(horde_proto::v1::ModelBinding {
                provider: provider.to_owned(),
                model: model.to_owned(),
                ..horde_proto::v1::ModelBinding::default()
            }),
            turn_id: turn.to_owned(),
            ..horde_proto::v1::UsageRecord::default()
        }),
    })
}
