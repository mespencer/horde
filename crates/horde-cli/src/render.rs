//! The CLI's two presentations of a wire answer.
//!
//! `--json` is here from the first commit rather than retrofitted, because a
//! scriptable verb whose only output is prose is not scriptable, and adding
//! machine output later means every existing script has already grown a parser
//! for the human format.
//!
//! Rendering is deliberately total: an event kind this build does not model
//! prints as its kind name rather than vanishing. A client that silently drops
//! events it cannot pretty-print is worse than one that admits it saw
//! something.

use horde_proto::v1::{
    EventEnvelope, PlacementState, SessionLifecycleState, SessionSummary, event::Kind,
};
use serde_json::{Value, json};

/// How to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Aligned prose for a terminal.
    #[default]
    Human,
    /// One JSON value per answer, for scripts.
    Json,
}

impl Format {
    #[must_use]
    pub fn from_flag(json: bool) -> Self {
        if json { Self::Json } else { Self::Human }
    }
}

/// Renders one session summary.
#[must_use]
pub fn session(summary: &SessionSummary, format: Format) -> String {
    match format {
        Format::Json => json!({
            "session_id": id(summary.session_id.as_ref().map(ToString::to_string)),
            "title": summary.title,
            "root_agent_id": id(summary.root_agent_id.as_ref().map(ToString::to_string)),
            "state": session_state(summary.state),
            "head_sequence": summary.head_sequence,
            "agent_count": summary.agent_count,
        })
        .to_string(),
        Format::Human => format!(
            "{}  {}  agents={} head={} {}",
            summary
                .session_id
                .as_ref()
                .map_or_else(|| "-".to_owned(), ToString::to_string),
            session_state(summary.state),
            summary.agent_count,
            summary.head_sequence,
            if summary.title.is_empty() {
                String::new()
            } else {
                summary.title.clone()
            }
        )
        .trim_end()
        .to_owned(),
    }
}

/// Renders a list of sessions, including the empty case.
#[must_use]
pub fn sessions(summaries: &[SessionSummary], format: Format) -> String {
    match format {
        Format::Json => {
            let rows: Vec<Value> = summaries
                .iter()
                .filter_map(|summary| serde_json::from_str(&session(summary, Format::Json)).ok())
                .collect();
            json!({ "sessions": rows }).to_string()
        }
        Format::Human => {
            if summaries.is_empty() {
                // Said rather than printed as a blank line: an empty result and
                // a swallowed error look identical otherwise.
                return "no sessions".to_owned();
            }
            summaries
                .iter()
                .map(|summary| session(summary, Format::Human))
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
}

/// Renders one event from a sync stream.
#[must_use]
pub fn event(envelope: &EventEnvelope, format: Format) -> String {
    let sequence = envelope.sequence;
    let session_id = envelope
        .session_id
        .as_ref()
        .map_or_else(String::new, ToString::to_string);
    let inner = envelope.event.as_ref();
    let agent_id = inner
        .and_then(|event| event.agent_id.as_ref())
        .map(ToString::to_string);
    let kind_name = inner.and_then(|event| event.kind.as_ref()).map_or(
        // An event with no kind is still a position in the log, and hiding it
        // would make the printed sequence look gapped.
        "unknown", kind_name,
    );

    match format {
        Format::Json => json!({
            "sequence": sequence,
            "session_id": session_id,
            "agent_id": agent_id,
            "kind": kind_name,
            "detail": detail(inner.and_then(|event| event.kind.as_ref())),
        })
        .to_string(),
        Format::Human => {
            let detail = detail(inner.and_then(|event| event.kind.as_ref()));
            let rendered = detail.as_str().unwrap_or_default();
            format!(
                "{sequence:>6}  {kind_name:<24} {}{}",
                agent_id.unwrap_or_else(|| "-".to_owned()),
                if rendered.is_empty() {
                    String::new()
                } else {
                    format!("  {rendered}")
                }
            )
        }
    }
}

/// The event kind's wire name.
fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::AgentOutputChunk(_) => "agent_output_chunk",
        Kind::AgentSpawned(_) => "agent_spawned",
        Kind::AssignmentSent(_) => "assignment_sent",
        Kind::NoteSent(_) => "note_sent",
        Kind::ResultReported(_) => "result_reported",
        Kind::PermissionRequested(_) => "permission_requested",
        Kind::PermissionDecided(_) => "permission_decided",
        Kind::UsageRecorded(_) => "usage_recorded",
        Kind::WorkspaceInstanceChanged(_) => "workspace_instance_changed",
        Kind::AgentLifecycleChanged(_) => "agent_lifecycle_changed",
        Kind::SessionLifecycleChanged(_) => "session_lifecycle_changed",
        Kind::PlacementChanged(_) => "placement_changed",
        Kind::TurnInterrupted(_) => "turn_interrupted",
    }
}

/// The one piece of an event worth showing beside its kind.
///
/// Deliberately shallow: a client that dumps whole payloads is unreadable, and
/// `--json` already carries everything a script needs.
fn detail(kind: Option<&Kind>) -> Value {
    match kind {
        Some(Kind::AgentOutputChunk(chunk)) => Value::String(chunk.text.clone()),
        Some(Kind::AssignmentSent(sent)) => Value::String(
            sent.assignment
                .as_ref()
                .map(|assignment| assignment.text.clone())
                .unwrap_or_default(),
        ),
        Some(Kind::NoteSent(sent)) => Value::String(
            sent.note
                .as_ref()
                .map(|note| note.text.clone())
                .unwrap_or_default(),
        ),
        Some(Kind::PlacementChanged(changed)) => Value::String(
            changed
                .placement
                .as_ref()
                .map(|placement| {
                    let state = placement_state(placement.state);
                    if placement.reason.is_empty() {
                        state.to_owned()
                    } else {
                        format!("{state}: {}", placement.reason)
                    }
                })
                .unwrap_or_default(),
        ),
        Some(Kind::TurnInterrupted(interrupted)) => Value::String(interrupted.reason.clone()),
        _ => Value::String(String::new()),
    }
}

fn session_state(state: i32) -> &'static str {
    match SessionLifecycleState::try_from(state) {
        Ok(SessionLifecycleState::Active) => "active",
        Ok(SessionLifecycleState::Archived) => "archived",
        _ => "unspecified",
    }
}

fn placement_state(state: i32) -> &'static str {
    match PlacementState::try_from(state) {
        Ok(PlacementState::Placed) => "placed",
        Ok(PlacementState::Queued) => "queued",
        Ok(PlacementState::Expired) => "expired",
        Ok(PlacementState::Cancelled) => "cancelled",
        _ => "unspecified",
    }
}

fn id(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
}

/// Renders the server handshake.
#[must_use]
pub fn hello(server: &horde_proto::v1::ServerHello, format: Format) -> String {
    match format {
        Format::Json => json!({
            "semver": server.semver,
            "min_supported_semver": server.min_supported_semver,
            "capability_flags": server.capability_flags,
            "principal": server.principal,
        })
        .to_string(),
        Format::Human => {
            let flags = if server.capability_flags.is_empty() {
                "none".to_owned()
            } else {
                server.capability_flags.join(",")
            };
            format!(
                "horde-server {} (min supported client {})\ncapabilities: {flags}",
                server.semver, server.min_supported_semver
            )
        }
    }
}

/// Renders a freshly created session and the root agent that came with it.
#[must_use]
pub fn created(
    summary: &SessionSummary,
    root_agent_id: &horde_proto::v1::AgentId,
    format: Format,
) -> String {
    match format {
        Format::Json => {
            let mut value: Value =
                serde_json::from_str(&session(summary, Format::Json)).unwrap_or_else(|_| json!({}));
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "root_agent_id".to_owned(),
                    Value::String(root_agent_id.to_string()),
                );
            }
            value.to_string()
        }
        // The root agent id is what the next command needs, so it goes on the
        // line the user will copy from.
        Format::Human => format!(
            "{}\nroot agent: {root_agent_id}",
            session(summary, Format::Human)
        ),
    }
}

/// Renders the outcome of a send.
#[must_use]
pub fn sent(response: &horde_proto::v1::SendMessageResponse, format: Format) -> String {
    let assignment_id = response.assignment_id.as_ref().map(ToString::to_string);

    match format {
        Format::Json => json!({
            "sequence": response.sequence,
            "session_id": id(response.session_id.as_ref().map(ToString::to_string)),
            "assignment_id": id(assignment_id),
        })
        .to_string(),
        Format::Human => match assignment_id {
            // Only assignments get an id, because only they owe a result.
            Some(id) => format!("sent at {} (assignment {id})", response.sequence),
            None => format!("sent at {}", response.sequence),
        },
    }
}

/// Renders a spawn, placed or queued.
#[must_use]
pub fn spawned(response: &horde_proto::v1::SpawnAgentResponse, format: Format) -> String {
    let agent_id = response
        .agent_id
        .as_ref()
        .map_or_else(|| "-".to_owned(), ToString::to_string);
    let placement = response.placement.as_ref();
    let state = placement.map_or("unspecified", |placement| placement_state(placement.state));
    let reason = placement.map_or("", |placement| placement.reason.as_str());
    let worker =
        placement.and_then(|placement| placement.worker_id.as_ref().map(ToString::to_string));

    match format {
        Format::Json => json!({
            "agent_id": agent_id,
            "placement": state,
            "worker_id": id(worker),
            "reason": reason,
        })
        .to_string(),
        // A queued spawn without its reason is not actionable, so the reason is
        // never dropped from the human form.
        Format::Human => {
            let mut line = format!("{agent_id}  {state}");
            if let Some(worker) = worker {
                line.push_str(&format!(" on {worker}"));
            }
            if !reason.is_empty() {
                line.push_str(&format!("  ({reason})"));
            }
            line
        }
    }
}

/// Renders an acknowledgement that carries only a log position.
#[must_use]
pub fn sequence(verb: &str, sequence: u64, format: Format) -> String {
    match format {
        Format::Json => json!({ "result": verb, "sequence": sequence }).to_string(),
        // The sequence is the point: a caller can sync from exactly here
        // instead of racing its own write.
        Format::Human => format!("{verb} at {sequence}"),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentOutputChannel, AgentOutputChunk, Event, EventEnvelope, Placement,
        PlacementChanged, PlacementState, SessionId, SessionLifecycleState, SessionSummary,
        event::Kind,
    };

    use super::{Format, created, event, hello, sent, sequence, session, sessions, spawned};

    fn summary() -> SessionSummary {
        SessionSummary {
            session_id: Some(SessionId::new("s1")),
            title: "ci-fix".to_owned(),
            root_agent_id: Some(AgentId::new("root")),
            workspace_definition_id: None,
            state: SessionLifecycleState::Active as i32,
            created_at: None,
            head_sequence: 12,
            agent_count: 3,
        }
    }

    fn envelope(kind: Option<Kind>) -> EventEnvelope {
        EventEnvelope {
            session_id: Some(SessionId::new("s1")),
            sequence: 7,
            recorded_at: None,
            event: Some(Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind,
            }),
        }
    }

    #[test]
    fn json_session_output_parses_and_carries_every_field_a_script_needs() {
        let rendered = session(&summary(), Format::Json);
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).expect("--json must emit valid JSON");

        assert_eq!(parsed["session_id"], "s1");
        assert_eq!(parsed["state"], "active");
        assert_eq!(parsed["head_sequence"], 12);
        assert_eq!(parsed["agent_count"], 3);
    }

    #[test]
    fn human_session_output_leads_with_the_id_so_it_can_be_copied() {
        let rendered = session(&summary(), Format::Human);
        assert!(rendered.starts_with("s1"), "got {rendered:?}");
        assert!(rendered.contains("active"));
    }

    /// An empty list and a swallowed error look identical if the empty case
    /// prints nothing.
    #[test]
    fn an_empty_session_list_says_so_rather_than_printing_nothing() {
        assert_eq!(sessions(&[], Format::Human), "no sessions");

        let parsed: serde_json::Value =
            serde_json::from_str(&sessions(&[], Format::Json)).expect("valid JSON");
        assert_eq!(parsed["sessions"].as_array().map(Vec::len), Some(0));
    }

    #[test]
    fn an_event_renders_its_kind_and_sequence() {
        let chunk = envelope(Some(Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::Assistant as i32,
            text: "hello".to_owned(),
            turn_id: "t1".to_owned(),
            chunk_index: 0,
        })));

        let human = event(&chunk, Format::Human);
        assert!(human.contains('7') && human.contains("agent_output_chunk"));
        assert!(human.contains("hello"), "the text is the point: {human:?}");

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&chunk, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["sequence"], 7);
        assert_eq!(parsed["kind"], "agent_output_chunk");
        assert_eq!(parsed["detail"], "hello");
    }

    /// Hiding an event this build cannot pretty-print would make the printed
    /// sequence look gapped, which is worse than an ugly line.
    #[test]
    fn an_event_with_no_kind_still_prints_its_position() {
        let unknown = envelope(None);

        let human = event(&unknown, Format::Human);
        assert!(human.contains('7'));
        assert!(human.contains("unknown"));

        let parsed: serde_json::Value =
            serde_json::from_str(&event(&unknown, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["kind"], "unknown");
        assert_eq!(parsed["sequence"], 7);
    }

    /// `kind_name` is a 13-arm match, so a partially covered one scores badly
    /// however simple each arm is. Naming every kind here also makes an added
    /// event kind fail this test rather than silently print as `unknown`.
    #[test]
    fn every_event_kind_has_a_wire_name() {
        use horde_proto::v1::{
            AgentLifecycleChanged, AgentSpawned, AssignmentSent, NoteSent, PermissionDecided,
            PermissionRequested, ResultReported, SessionLifecycleChanged, TurnInterrupted,
            UsageRecorded, WorkspaceInstanceChanged,
        };

        let cases: Vec<(Kind, &str)> = vec![
            (
                Kind::AgentOutputChunk(AgentOutputChunk::default()),
                "agent_output_chunk",
            ),
            (Kind::AgentSpawned(AgentSpawned::default()), "agent_spawned"),
            (
                Kind::AssignmentSent(AssignmentSent::default()),
                "assignment_sent",
            ),
            (Kind::NoteSent(NoteSent::default()), "note_sent"),
            (
                Kind::ResultReported(ResultReported::default()),
                "result_reported",
            ),
            (
                Kind::PermissionRequested(PermissionRequested::default()),
                "permission_requested",
            ),
            (
                Kind::PermissionDecided(PermissionDecided::default()),
                "permission_decided",
            ),
            (
                Kind::UsageRecorded(UsageRecorded::default()),
                "usage_recorded",
            ),
            (
                Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged::default()),
                "workspace_instance_changed",
            ),
            (
                Kind::AgentLifecycleChanged(AgentLifecycleChanged::default()),
                "agent_lifecycle_changed",
            ),
            (
                Kind::SessionLifecycleChanged(SessionLifecycleChanged::default()),
                "session_lifecycle_changed",
            ),
            (
                Kind::PlacementChanged(PlacementChanged::default()),
                "placement_changed",
            ),
            (
                Kind::TurnInterrupted(TurnInterrupted::default()),
                "turn_interrupted",
            ),
        ];

        for (kind, expected) in cases {
            let rendered = event(&envelope(Some(kind)), Format::Human);
            assert!(
                rendered.contains(expected),
                "expected {expected:?} in {rendered:?}"
            );
        }
    }

    #[test]
    fn the_handshake_names_the_server_version_in_both_formats() {
        let server = horde_proto::v1::ServerHello {
            semver: "0.1.0".to_owned(),
            capability_flags: vec!["payload.attachments".to_owned()],
            min_supported_semver: "0.1.0".to_owned(),
            server_time: None,
            principal: String::new(),
        };

        let human = hello(&server, Format::Human);
        assert!(human.contains("0.1.0") && human.contains("payload.attachments"));

        let parsed: serde_json::Value =
            serde_json::from_str(&hello(&server, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["semver"], "0.1.0");
    }

    /// A server advertising nothing must not render an empty field that reads
    /// like a truncated line.
    #[test]
    fn a_server_with_no_capability_flags_says_none() {
        let server = horde_proto::v1::ServerHello {
            semver: "0.1.0".to_owned(),
            min_supported_semver: "0.1.0".to_owned(),
            ..horde_proto::v1::ServerHello::default()
        };
        assert!(hello(&server, Format::Human).contains("none"));
    }

    #[test]
    fn a_created_session_carries_the_root_agent_the_next_command_needs() {
        let root = AgentId::new("root-1");

        let human = created(&summary(), &root, Format::Human);
        assert!(human.contains("root-1"), "got {human:?}");

        let parsed: serde_json::Value =
            serde_json::from_str(&created(&summary(), &root, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["root_agent_id"], "root-1");
        assert_eq!(parsed["session_id"], "s1", "the session fields survive too");
    }

    #[test]
    fn an_assignment_reports_its_id_and_a_note_does_not() {
        let assignment = horde_proto::v1::SendMessageResponse {
            assignment_id: Some(horde_proto::v1::AssignmentId::new("a-1")),
            session_id: Some(SessionId::new("s1")),
            sequence: 4,
        };
        let human = sent(&assignment, Format::Human);
        assert!(
            human.contains('4') && human.contains("a-1"),
            "got {human:?}"
        );

        // Only assignments owe a result, so only they get an id.
        let note = horde_proto::v1::SendMessageResponse {
            assignment_id: None,
            session_id: Some(SessionId::new("s1")),
            sequence: 5,
        };
        let human = sent(&note, Format::Human);
        assert!(
            human.contains('5') && !human.contains("assignment"),
            "got {human:?}"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&sent(&note, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["assignment_id"], serde_json::Value::Null);
    }

    #[test]
    fn a_placed_spawn_names_its_worker() {
        let response = horde_proto::v1::SpawnAgentResponse {
            agent_id: Some(AgentId::new("agent-1")),
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-1")),
                state: PlacementState::Placed as i32,
                worker_id: Some(horde_proto::v1::WorkerId::new("worker-a")),
                reason: String::new(),
            }),
        };

        let human = spawned(&response, Format::Human);
        assert!(
            human.contains("agent-1") && human.contains("placed") && human.contains("worker-a")
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&spawned(&response, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["placement"], "placed");
        assert_eq!(parsed["worker_id"], "worker-a");
    }

    /// A queued spawn without its reason is not actionable.
    #[test]
    fn a_queued_spawn_keeps_its_reason_in_both_formats() {
        let response = horde_proto::v1::SpawnAgentResponse {
            agent_id: Some(AgentId::new("agent-1")),
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-1")),
                state: PlacementState::Queued as i32,
                worker_id: None,
                reason: "no worker is connected".to_owned(),
            }),
        };

        let human = spawned(&response, Format::Human);
        assert!(human.contains("queued") && human.contains("no worker is connected"));

        let parsed: serde_json::Value =
            serde_json::from_str(&spawned(&response, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["reason"], "no worker is connected");
        assert_eq!(parsed["worker_id"], serde_json::Value::Null);
    }

    #[test]
    fn a_spawn_with_no_placement_still_renders() {
        let response = horde_proto::v1::SpawnAgentResponse {
            agent_id: None,
            placement: None,
        };
        assert!(spawned(&response, Format::Human).contains("unspecified"));
    }

    #[test]
    fn an_acknowledgement_carries_the_position_a_caller_syncs_from() {
        assert_eq!(
            sequence("interrupted", 9, Format::Human),
            "interrupted at 9"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&sequence("denied", 9, Format::Json)).expect("valid JSON");
        assert_eq!(parsed["result"], "denied");
        assert_eq!(parsed["sequence"], 9);
    }

    #[test]
    fn every_placement_and_session_state_has_a_name() {
        for (state, expected) in [
            (PlacementState::Placed, "placed"),
            (PlacementState::Queued, "queued"),
            (PlacementState::Expired, "expired"),
            (PlacementState::Cancelled, "cancelled"),
            (PlacementState::Unspecified, "unspecified"),
        ] {
            assert_eq!(super::placement_state(state as i32), expected);
        }
        assert_eq!(super::placement_state(9999), "unspecified");

        assert_eq!(
            super::session_state(SessionLifecycleState::Archived as i32),
            "archived"
        );
        assert_eq!(super::session_state(9999), "unspecified");
    }

    #[test]
    fn a_session_with_no_title_does_not_render_a_trailing_gap() {
        let untitled = SessionSummary {
            title: String::new(),
            ..summary()
        };
        let human = session(&untitled, Format::Human);
        assert_eq!(human, human.trim_end(), "got {human:?}");
    }

    #[test]
    fn a_queued_placement_shows_the_reason_the_spawner_needs() {
        let queued = envelope(Some(Kind::PlacementChanged(PlacementChanged {
            placement: Some(Placement {
                agent_id: Some(AgentId::new("agent-1")),
                state: PlacementState::Queued as i32,
                worker_id: None,
                reason: "no worker is connected".to_owned(),
            }),
        })));

        let human = event(&queued, Format::Human);
        assert!(
            human.contains("queued") && human.contains("no worker is connected"),
            "a queued spawn is useless without its reason: {human:?}"
        );
    }
}
