//! Round trips for the messages the resilience story rests on: the two log
//! envelopes, the client sync stream, and the paired Worker streams. If any of
//! these lose a field on the wire, reconnect and replay lose work silently.

use horde_proto::prost::Message;
use horde_proto::v1::{
    AgentId, AgentResult, Attachment, Event, EventBatch, EventEnvelope, GitRef, JournalAck,
    JournaledEvent, JournaledEventBatch, Payload, PushEventsAccepted, PushEventsRequest,
    PushEventsResponse, PushEventsStart, ResultStatus, SessionCursor, SessionId,
    StreamCommandsResponse, SyncCaughtUp, SyncEventsRequest, SyncEventsResponse, WorkerId, event,
    push_events_request, push_events_response, stream_commands_response, sync_events_response,
};

/// Encodes, decodes, and asserts the value survived unchanged.
///
/// A decode failure surfaces as `None`, so a broken encoding and a lossy one
/// both fail the same assertion with the message printed.
#[track_caller]
fn round_trip<M: Message + Default + PartialEq + std::fmt::Debug>(message: &M) {
    let decoded = M::decode(message.encode_to_vec().as_slice()).ok();
    assert_eq!(decoded.as_ref(), Some(message));
}

#[test]
fn payload_carries_multi_repo_refs_and_attachments() {
    round_trip(&Payload {
        git_refs: vec![
            GitRef {
                repo_name: "atlas".to_owned(),
                commit_sha: "0f2a1c9e4b6d8a3f5c7e9b1d3f5a7c9e1b3d5f70".to_owned(),
                branch: "mespencer/fix-auth-timeout".to_owned(),
            },
            GitRef {
                repo_name: "atlas-ui".to_owned(),
                commit_sha: "9e1b3d5f70f2a1c9e4b6d8a3f5c7e9b1d3f5a7c9".to_owned(),
                branch: String::new(),
            },
        ],
        attachments: vec![Attachment {
            name: "failing-test.log".to_owned(),
            blob_digest: "sha256:8f14e45fceea167a5a36dedd4bea2543".to_owned(),
            size_bytes: 4096,
            media_type: "text/plain".to_owned(),
            inline: b"assertion failed: left == right".to_vec(),
        }],
    });
}

#[test]
fn journaled_event_and_session_envelope_keep_their_own_sequences() {
    let event = Event {
        agent_id: Some(AgentId::new("agent-9")),
        occurred_at: None,
        idempotency_key: "worker-1/journal/117".to_owned(),
        kind: Some(event::Kind::ResultReported(
            horde_proto::v1::ResultReported {
                result: Some(AgentResult {
                    assignment_id: None,
                    status: ResultStatus::Blocked.into(),
                    summary: "needs credentials for the private remote".to_owned(),
                    payload: None,
                }),
            },
        )),
    };

    let journaled = JournaledEvent {
        journal_sequence: 117,
        session_id: Some(SessionId::new("ci-fix")),
        event: Some(event.clone()),
    };
    round_trip(&journaled);

    let envelope = EventEnvelope {
        session_id: Some(SessionId::new("ci-fix")),
        sequence: 42,
        recorded_at: None,
        event: Some(event),
    };
    round_trip(&envelope);

    // The two numbers are independent spaces; nothing in the schema ties them
    // together, and nothing downstream should assume it does.
    assert_ne!(journaled.journal_sequence, envelope.sequence);
}

#[test]
fn sync_events_request_expresses_resume_from_offset() {
    round_trip(&SyncEventsRequest {
        cursors: vec![SessionCursor {
            session_id: Some(SessionId::new("ci-fix")),
            from_sequence: 42,
        }],
        all_sessions: true,
        subtree_root: Some(AgentId::new("agent-9")),
        follow: true,
        max_batch_events: 256,
    });
}

#[test]
fn sync_events_response_round_trips_every_arm() {
    let arms = vec![
        sync_events_response::Update::Batch(EventBatch {
            events: vec![EventEnvelope {
                session_id: Some(SessionId::new("ci-fix")),
                sequence: 43,
                recorded_at: None,
                event: None,
            }],
        }),
        sync_events_response::Update::CaughtUp(SyncCaughtUp {
            heads: vec![SessionCursor {
                session_id: Some(SessionId::new("ci-fix")),
                from_sequence: 43,
            }],
        }),
        sync_events_response::Update::Heartbeat(horde_proto::v1::SyncHeartbeat {
            server_time: None,
        }),
    ];

    for update in arms {
        round_trip(&SyncEventsResponse {
            update: Some(update),
        });
    }
}

#[test]
fn worker_command_stream_carries_sequence_and_idempotency_key() {
    round_trip(&StreamCommandsResponse {
        command_sequence: 88,
        command_id: "cmd-01JABCD".to_owned(),
        command: Some(stream_commands_response::Command::InterruptAgent(
            horde_proto::v1::InterruptAgentCommand {
                agent_id: Some(AgentId::new("agent-9")),
                reason: "user pressed escape".to_owned(),
            },
        )),
    });
}

#[test]
fn push_events_stream_round_trips_start_batch_and_acks() {
    round_trip(&PushEventsRequest {
        body: Some(push_events_request::Body::Start(PushEventsStart {
            worker_id: Some(WorkerId::new("worker-1")),
            last_local_journal_sequence: 117,
        })),
    });

    round_trip(&PushEventsRequest {
        body: Some(push_events_request::Body::Batch(JournaledEventBatch {
            events: vec![JournaledEvent {
                journal_sequence: 118,
                session_id: Some(SessionId::new("ci-fix")),
                event: None,
            }],
            acked_command_sequence: 88,
        })),
    });

    round_trip(&PushEventsResponse {
        body: Some(push_events_response::Body::Accepted(PushEventsAccepted {
            resume_from_journal_sequence: 118,
        })),
    });

    round_trip(&PushEventsResponse {
        body: Some(push_events_response::Body::Ack(JournalAck {
            acked_journal_sequence: 118,
        })),
    });
}

#[test]
fn unknown_enum_values_survive_decoding() {
    // Protobuf enums are open. A newer peer that adds a ResultStatus must not
    // have it silently rewritten to UNSPECIFIED by an older one, so the wire
    // representation has to preserve the integer verbatim.
    let from_a_newer_peer = AgentResult {
        assignment_id: None,
        status: 99,
        summary: "status this build has never heard of".to_owned(),
        payload: None,
    };

    let decoded = AgentResult::decode(from_a_newer_peer.encode_to_vec().as_slice()).ok();

    assert_eq!(decoded.as_ref().map(|r| r.status), Some(99));
    assert_eq!(decoded.as_ref(), Some(&from_a_newer_peer));
}
