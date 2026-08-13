//! The event log is the resilience mechanism (ADR-0001) and "everything that
//! matters must be an event". These tests hold the `Event.kind` oneof to the
//! vocabulary in CONTEXT.md from both directions: the compiled schema and the
//! generated Rust enum must each carry exactly this set and no other.

use horde_proto::FILE_DESCRIPTOR_SET;
use horde_proto::prost::Message;
use horde_proto::prost_types::{DescriptorProto, FileDescriptorSet};
use horde_proto::v1::{
    AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentOutputChunk,
    AgentResult, AgentSpawned, Assignment, AssignmentId, AssignmentSent, EnrollmentCodeMinted,
    EnrollmentRevoked, Event, EventEnvelope, IdentityEnrolled, Note, NoteSent, Payload,
    PermissionDecided, PermissionDecision, PermissionRequest, PermissionRequested, Placement,
    PlacementChanged, PlacementState, ResultReported, ResultStatus, Role, SessionId,
    SessionLifecycleChanged, SessionLifecycleState, TurnInterrupted, UsageRecord, UsageRecorded,
    WorkerId, WorkspaceDefinition, WorkspaceDefinitionChanged, WorkspaceDefinitionId,
    WorkspaceInstanceChanged, WorkspaceInstanceReport, WorkspacePrewarmRequested, event,
};

/// The event vocabulary, as named in CONTEXT.md and DESIGN.md:
///
/// * Agent output, spawn ancestry, the Assignment/Note split, and the Result
///   an Assignment is owed (`AgentResult` on the wire).
/// * Permission escalation and its decision — human approval is "an ordinary
///   event in the tree", which is what makes phone approval work.
/// * Usage records, per Agent, attributable across a tree.
/// * Workspace Instance state, including materialization progress.
/// * Lifecycle transitions for Agents and Sessions, and Placement — including
///   queued-with-reason, because spawn never blocks the caller.
/// * Turn interruption, distinct from a message queued to a turn boundary.
/// * The Workspace Definition catalog and prewarm requests — Definitions are
///   Server-owned state, and ADR-0001 admits no store for them but the log.
/// * Enrollment (DESIGN §3): codes minted, identities issued, certificates
///   revoked — journaled through the log like everything else, in a reserved
///   stream like the Definition catalog's.
const EVENT_VOCABULARY: &[&str] = &[
    "agent_output_chunk",
    "agent_spawned",
    "assignment_sent",
    "note_sent",
    "result_reported",
    "permission_requested",
    "permission_decided",
    "usage_recorded",
    "workspace_instance_changed",
    "agent_lifecycle_changed",
    "session_lifecycle_changed",
    "placement_changed",
    "turn_interrupted",
    "workspace_definition_changed",
    "workspace_prewarm_requested",
    "enrollment_code_minted",
    "identity_enrolled",
    "enrollment_revoked",
];

/// Maps a generated variant back to its proto field name.
///
/// Exhaustive on purpose: adding an arm to the `Event.kind` oneof without
/// deciding what it is called here stops compiling.
fn kind_field_name(kind: &event::Kind) -> &'static str {
    match kind {
        event::Kind::AgentOutputChunk(_) => "agent_output_chunk",
        event::Kind::AgentSpawned(_) => "agent_spawned",
        event::Kind::AssignmentSent(_) => "assignment_sent",
        event::Kind::NoteSent(_) => "note_sent",
        event::Kind::ResultReported(_) => "result_reported",
        event::Kind::PermissionRequested(_) => "permission_requested",
        event::Kind::PermissionDecided(_) => "permission_decided",
        event::Kind::UsageRecorded(_) => "usage_recorded",
        event::Kind::WorkspaceInstanceChanged(_) => "workspace_instance_changed",
        event::Kind::AgentLifecycleChanged(_) => "agent_lifecycle_changed",
        event::Kind::SessionLifecycleChanged(_) => "session_lifecycle_changed",
        event::Kind::PlacementChanged(_) => "placement_changed",
        event::Kind::TurnInterrupted(_) => "turn_interrupted",
        event::Kind::WorkspaceDefinitionChanged(_) => "workspace_definition_changed",
        event::Kind::WorkspacePrewarmRequested(_) => "workspace_prewarm_requested",
        event::Kind::EnrollmentCodeMinted(_) => "enrollment_code_minted",
        event::Kind::IdentityEnrolled(_) => "identity_enrolled",
        event::Kind::EnrollmentRevoked(_) => "enrollment_revoked",
    }
}

/// One populated instance of every event kind. Populated rather than default so
/// the round-trip test actually exercises nested messages, enums, and payloads
/// instead of encoding thirteen empty bodies.
fn every_kind() -> Vec<event::Kind> {
    vec![
        event::Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant.into(),
            text: "running the test suite".to_owned(),
            turn_id: "turn-3".to_owned(),
            chunk_index: 7,
        }),
        event::Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: Some(AgentId::new("agent-root")),
            spawned_by: None,
            spec: None,
            tree_path: vec!["ci-fix".to_owned(), "reviewer".to_owned()],
        }),
        event::Kind::AssignmentSent(AssignmentSent {
            from: None,
            assignment: Some(Assignment {
                assignment_id: Some(AssignmentId::new("assign-1")),
                text: "review the diff".to_owned(),
                payload: Some(Payload::default()),
            }),
            delivery: 0,
        }),
        event::Kind::NoteSent(NoteSent {
            from: None,
            note: Some(Note {
                text: "heads up, main moved".to_owned(),
                payload: None,
            }),
            delivery: 0,
        }),
        event::Kind::ResultReported(ResultReported {
            result: Some(AgentResult {
                assignment_id: Some(AssignmentId::new("assign-1")),
                status: ResultStatus::Partial.into(),
                summary: "two of three tests fixed".to_owned(),
                payload: None,
            }),
        }),
        event::Kind::PermissionRequested(PermissionRequested {
            request: Some(PermissionRequest {
                tree_path: vec!["ci-fix".to_owned(), "test-runner".to_owned()],
                ..PermissionRequest::default()
            }),
        }),
        event::Kind::PermissionDecided(PermissionDecided {
            decision: Some(PermissionDecision::default()),
        }),
        event::Kind::UsageRecorded(UsageRecorded {
            record: Some(UsageRecord {
                turn_id: "turn-3".to_owned(),
                ..UsageRecord::default()
            }),
        }),
        event::Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
            report: Some(WorkspaceInstanceReport::default()),
            progress: None,
        }),
        event::Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: AgentLifecycleState::Running.into(),
            state: AgentLifecycleState::Idle.into(),
            reason: "reported its result".to_owned(),
        }),
        event::Kind::SessionLifecycleChanged(SessionLifecycleChanged {
            previous_state: SessionLifecycleState::Active.into(),
            state: SessionLifecycleState::Archived.into(),
            reason: "archived by the user".to_owned(),
        }),
        event::Kind::PlacementChanged(PlacementChanged {
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-9")),
                state: PlacementState::Queued.into(),
                worker_id: None,
                reason: "no online worker with tag `gpu`".to_owned(),
            }),
        }),
        event::Kind::TurnInterrupted(TurnInterrupted {
            requested_by: None,
            turn_id: "turn-3".to_owned(),
            reason: "user pressed escape".to_owned(),
        }),
        event::Kind::WorkspaceDefinitionChanged(WorkspaceDefinitionChanged {
            definition: Some(WorkspaceDefinition {
                workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
                name: "atlas".to_owned(),
                ..WorkspaceDefinition::default()
            }),
            deleted: false,
        }),
        event::Kind::WorkspacePrewarmRequested(WorkspacePrewarmRequested {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            worker_id: Some(WorkerId::new("worker-a")),
        }),
        event::Kind::EnrollmentCodeMinted(EnrollmentCodeMinted {
            role: Role::Worker.into(),
            name: "mac-studio".to_owned(),
            code_hash: "b5bb9d8014a0f9b1d61e21e796d78dcc".to_owned(),
            expires_at: None,
        }),
        event::Kind::IdentityEnrolled(IdentityEnrolled {
            role: Role::Worker.into(),
            name: "mac-studio".to_owned(),
            certificate_serial: "0a1b2c3d".to_owned(),
            code_hash: "b5bb9d8014a0f9b1d61e21e796d78dcc".to_owned(),
        }),
        event::Kind::EnrollmentRevoked(EnrollmentRevoked {
            role: Role::Worker.into(),
            name: "mac-studio".to_owned(),
            certificate_serial: "0a1b2c3d".to_owned(),
            reason: "laptop stolen".to_owned(),
        }),
    ]
}

/// The `Event.kind` oneof as the compiled schema declares it: `(field name,
/// field number)`, in declaration order.
///
/// Returns empty rather than panicking if the descriptor cannot be read; an
/// empty set fails every assertion below, which is the same verdict with a
/// clearer diff.
fn kind_fields_from_descriptor() -> Vec<(String, i32)> {
    let Ok(set) = FileDescriptorSet::decode(FILE_DESCRIPTOR_SET) else {
        return Vec::new();
    };

    let event_message: Option<DescriptorProto> = set
        .file
        .into_iter()
        .find(|f| f.name() == "horde/v1/event.proto")
        .into_iter()
        .flat_map(|f| f.message_type)
        .find(|m| m.name() == "Event");

    let Some(event_message) = event_message else {
        return Vec::new();
    };

    let kind_index = event_message
        .oneof_decl
        .iter()
        .position(|o| o.name() == "kind")
        .map(|index| i32::try_from(index).unwrap_or(-1));

    event_message
        .field
        .iter()
        .filter(|f| f.oneof_index.is_some() && f.oneof_index == kind_index)
        .map(|f| (f.name().to_owned(), f.number()))
        .collect()
}

fn sorted(mut names: Vec<String>) -> Vec<String> {
    names.sort();
    names
}

fn vocabulary() -> Vec<String> {
    sorted(EVENT_VOCABULARY.iter().map(|s| (*s).to_owned()).collect())
}

#[test]
fn schema_event_kinds_are_exactly_the_context_vocabulary() {
    let names = kind_fields_from_descriptor()
        .into_iter()
        .map(|(name, _tag)| name)
        .collect();
    assert_eq!(sorted(names), vocabulary());
}

#[test]
fn generated_event_kinds_are_exactly_the_context_vocabulary() {
    let kinds = every_kind();
    let from_rust = kinds.iter().map(kind_field_name).map(str::to_owned);
    assert_eq!(sorted(from_rust.collect()), vocabulary());
}

/// The production name lookup reads the descriptor; this exhaustive match is
/// the hand-written oracle it is held against, kind by kind. A drift between
/// schema and lookup — a misdecoded tag, a missed oneof field — shows up here
/// as the exact kind that misnamed itself.
#[test]
fn the_descriptor_lookup_names_every_kind_like_the_schema_does() {
    for kind in every_kind() {
        assert_eq!(
            horde_proto::kind_field_name(&kind),
            Some(kind_field_name(&kind)),
            "the descriptor-derived name must match the declared field"
        );
    }
}

#[test]
fn every_event_kind_survives_an_envelope_round_trip() {
    for kind in every_kind() {
        let name = kind_field_name(&kind);

        let envelope = EventEnvelope {
            session_id: Some(SessionId::new("ci-fix")),
            sequence: 42,
            recorded_at: None,
            event: Some(Event {
                agent_id: Some(AgentId::new("agent-9")),
                occurred_at: None,
                idempotency_key: "worker-1/journal/117".to_owned(),
                kind: Some(kind),
            }),
        };

        let encoded = envelope.encode_to_vec();
        let decoded = EventEnvelope::decode(encoded.as_slice())
            .unwrap_or_else(|e| panic!("{name} envelope should decode: {e}"));

        assert_eq!(decoded, envelope, "{name} envelope did not round-trip");
    }
}

#[test]
fn event_kind_tags_are_stable() {
    // Field numbers are the contract. Renumbering a shipped kind silently
    // reinterprets every event already in a Session log or a Worker journal, so
    // pin the assignment here as well as in buf's breaking check.
    let expected: [(&str, i32); 18] = [
        ("agent_output_chunk", 10),
        ("agent_spawned", 11),
        ("assignment_sent", 12),
        ("note_sent", 13),
        ("result_reported", 14),
        ("permission_requested", 15),
        ("permission_decided", 16),
        ("usage_recorded", 17),
        ("workspace_instance_changed", 18),
        ("agent_lifecycle_changed", 19),
        ("session_lifecycle_changed", 20),
        ("placement_changed", 21),
        ("turn_interrupted", 22),
        ("workspace_definition_changed", 23),
        ("workspace_prewarm_requested", 24),
        ("enrollment_code_minted", 25),
        ("identity_enrolled", 26),
        ("enrollment_revoked", 27),
    ];

    let expected: Vec<(String, i32)> = expected
        .into_iter()
        .map(|(name, tag)| (name.to_owned(), tag))
        .collect();

    assert_eq!(kind_fields_from_descriptor(), expected);
}
