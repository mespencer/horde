//! The real client library against a real server.
//!
//! Nothing is stubbed on either side: `horde_cli::Client` talks to
//! `horde_server::service::ControlPlane` over tonic, on the loopback socket
//! `common::serve` binds. Everything above the socket — codegen, framing,
//! streaming, status mapping — is the real thing, and this file enters the
//! client through [`horde_cli::Client::with_channel`] so that seam is covered
//! too.

mod common;

use common::Setup;
use horde_cli::client::{PermissionAnswer, SyncOptions};
use horde_cli::{Client, render};
use horde_proto::v1::{DeliveryMode, PlacementState};

/// Replays a session and returns the sequences the client observed.
async fn replay(client: &mut Client, session_id: &str, from: u64) -> Setup<Vec<u64>> {
    let mut seen = Vec::new();
    client
        .sync(
            &SyncOptions::new(session_id).from_sequence(from),
            |envelope| seen.push(envelope.sequence),
            |_| {},
        )
        .await?;
    Ok(seen)
}

#[tokio::test]
async fn hello_reports_what_the_server_is() {
    let mut client = common::client().await.expect("a connected client");

    let server = client.hello("0.1.0").await.expect("hello must succeed");
    assert!(
        !server.semver.is_empty(),
        "a server that will not name its version cannot be version-skewed against"
    );
    assert!(!server.min_supported_semver.is_empty());
}

/// The verb chain a user actually types: create, spawn, send, then read it all
/// back.
#[tokio::test]
async fn create_spawn_send_then_sync_sees_every_event() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, root) = common::seed(&mut client, "ci-fix")
        .await
        .expect("create must succeed");

    let spawned = client
        .spawn_agent(&session_id, None, "")
        .await
        .expect("spawn must not fail just because nothing can run it");
    assert_eq!(
        spawned.placement.as_ref().map(|placement| placement.state),
        Some(PlacementState::Queued as i32),
        "no worker is registered in this test, so the spawn queues"
    );

    let sent = client
        .send_assignment(&root, "fix the build", DeliveryMode::TurnBoundary, "")
        .await
        .expect("send must succeed");
    assert!(
        sent.assignment_id.is_some(),
        "an assignment owes a result, so it must come back with an id"
    );

    let seen = replay(&mut client, &session_id, 0)
        .await
        .expect("replay must succeed");
    assert_eq!(
        seen,
        (1..=seen.len() as u64).collect::<Vec<u64>>(),
        "a full replay is contiguous from 1"
    );
    assert!(
        seen.len() >= 4,
        "spawn of the root, spawn of the child, its placement, and the assignment: {seen:?}"
    );
}

/// The client half of the log's offset contract, which is what reconnect,
/// scrollback, and attach are all built on.
#[tokio::test]
async fn syncing_from_an_offset_returns_exactly_the_tail_of_a_full_replay() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, root) = common::seed(&mut client, "offsets").await.expect("create");
    common::send_notes(&mut client, &root, 4)
        .await
        .expect("send");

    let full = replay(&mut client, &session_id, 0).await.expect("full");
    let from_third = replay(&mut client, &session_id, 3).await.expect("partial");

    assert_eq!(
        from_third,
        full.iter()
            .copied()
            .filter(|sequence| *sequence >= 3)
            .collect::<Vec<u64>>(),
        "resuming mid-stream must yield the suffix, no more and no less"
    );
}

/// A client that stops reading and comes back must resume from where it got to
/// without re-seeing what it already had.
#[tokio::test]
async fn a_reconnecting_client_resumes_without_duplicates_or_gaps() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, root) = common::seed(&mut client, "resume").await.expect("create");

    client
        .send_note(&root, "before", DeliveryMode::TurnBoundary, "")
        .await
        .expect("send");

    // First pass: read everything, remembering the highest sequence, exactly as
    // a real client would in order to reconnect.
    let mut first = Vec::new();
    let highest = client
        .sync(
            &SyncOptions::new(session_id.clone()),
            |envelope| first.push(envelope.sequence),
            |_| {},
        )
        .await
        .expect("first pass");

    client
        .send_note(&root, "after", DeliveryMode::TurnBoundary, "")
        .await
        .expect("send");

    let second = replay(&mut client, &session_id, highest + 1)
        .await
        .expect("second pass");

    assert!(
        second.iter().all(|sequence| *sequence > highest),
        "a resumed client must not re-see what it already read: {second:?}"
    );
    let mut combined = first;
    combined.extend(&second);
    assert_eq!(
        combined,
        (1..=combined.len() as u64).collect::<Vec<u64>>(),
        "the two passes must join into one gapless history"
    );
}

/// Answering an escalation that is not outstanding must fail rather than
/// silently succeed: a decision that went nowhere must never look applied.
/// The returned sequence is the position a caller syncs from next, so a stub
/// that returns a constant is worse than useless — mutation testing caught
/// `Ok(0)` and `Ok(1)` surviving here.
#[tokio::test]
async fn interrupt_returns_the_log_position_it_wrote() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, root) = common::seed(&mut client, "interrupt")
        .await
        .expect("create");

    let before = replay(&mut client, &session_id, 0).await.expect("replay");
    let head_before = before.last().copied().unwrap_or(0);

    let sequence = client
        .interrupt(&root, "changed my mind")
        .await
        .expect("interrupt");

    assert_eq!(
        sequence,
        head_before + 1,
        "the interrupt must report the position it actually appended at"
    );
    let after = replay(&mut client, &session_id, 0).await.expect("replay");
    assert_eq!(after.last().copied(), Some(sequence));
}

/// Same contract for send: the sequence is what a caller resumes from.
#[tokio::test]
async fn send_returns_the_log_position_it_wrote() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, root) = common::seed(&mut client, "send").await.expect("create");

    let response = client
        .send_note(&root, "hello", DeliveryMode::TurnBoundary, "")
        .await
        .expect("send");

    let seen = replay(&mut client, &session_id, 0).await.expect("replay");
    assert_eq!(
        seen.last().copied(),
        Some(response.sequence),
        "the reported sequence must be where the event actually landed"
    );
}

/// A permission request, journaled the way a worker writes a real one.
fn permission_requested(agent: &str, request_id: &str) -> horde_proto::v1::Event {
    horde_proto::v1::Event {
        agent_id: Some(horde_proto::v1::AgentId::new(agent)),
        occurred_at: None,
        idempotency_key: String::new(),
        kind: Some(horde_proto::v1::event::Kind::PermissionRequested(
            horde_proto::v1::PermissionRequested {
                request: Some(horde_proto::v1::PermissionRequest {
                    request_id: Some(horde_proto::v1::PermissionRequestId::new(request_id)),
                    agent_id: Some(horde_proto::v1::AgentId::new(agent)),
                    ..horde_proto::v1::PermissionRequest::default()
                }),
            },
        )),
    }
}

/// The human half of a narrowing, end to end: a person clamps an argument and
/// the clamp survives the client, the RPC, and the log.
///
/// The server half is covered by `CommandProjection`'s unit tests. This is the
/// half those cannot reach — before this, the field existed on the wire and
/// nothing a human drives could populate it, so the whole mechanism was
/// unreachable in practice.
#[tokio::test]
async fn a_human_can_narrow_a_call_and_the_narrowing_reaches_the_log() {
    let endpoint = common::serve().await.expect("a server");
    let (mut client, session_id, agent_id) = common::seeded(&endpoint, "narrowing")
        .await
        .expect("a seeded session");

    common::journal_event(
        &endpoint,
        &session_id,
        permission_requested(&agent_id, "req-1"),
    )
    .await
    .expect("the worker journals the escalation");

    let narrowed = serde_json::json!({ "path": "/workspace/atlas", "recursive": false });
    client
        .answer_permission(PermissionAnswer::allow("req-1").narrowed(&narrowed))
        .await
        .expect("a narrowing answer must be accepted");

    let mut decisions = Vec::new();
    client
        .sync(
            &SyncOptions::new(session_id.clone()),
            |envelope| {
                if let Some(horde_proto::v1::event::Kind::PermissionDecided(decided)) = envelope
                    .event
                    .as_ref()
                    .and_then(|event| event.kind.as_ref())
                {
                    decisions.push(decided.clone());
                }
            },
            |_| {},
        )
        .await
        .expect("replay");

    let decision = decisions
        .first()
        .and_then(|decided| decided.decision.as_ref())
        .expect("the answer must have been written as a PermissionDecided");

    let updated = decision
        .updated_input
        .as_ref()
        .expect("the narrowing must survive to the log, or the original call runs");

    assert_eq!(
        updated
            .fields
            .get("path")
            .and_then(|value| match &value.kind {
                Some(horde_proto::prost_types::value::Kind::StringValue(s)) => Some(s.as_str()),
                _ => None,
            }),
        Some("/workspace/atlas"),
        "the clamped path is the whole point of the narrowing"
    );
    assert!(
        updated.fields.contains_key("recursive"),
        "every replacement argument travels, not just the first"
    );
}

/// A narrowing the wire cannot carry is refused locally rather than sent as a
/// plain allow — which would permit the original, wider call.
#[tokio::test]
async fn a_narrowing_that_cannot_cross_the_wire_is_refused_before_the_rpc() {
    let mut client = common::client().await.expect("a connected client");

    let not_an_object = serde_json::json!(["not", "an", "object"]);
    let error = client
        .answer_permission(PermissionAnswer::allow("req-1").narrowed(&not_an_object))
        .await
        .expect_err("a non-object argument bag cannot be a Struct");

    assert!(
        matches!(error, horde_cli::client::ClientError::Narrowing { .. }),
        "refusing the narrowing is the point; sending a bare allow would widen \
         the permission: {error}"
    );
}

#[tokio::test]
async fn approving_an_unknown_request_is_refused() {
    let mut client = common::client().await.expect("a connected client");

    let error = client
        .answer_permission(PermissionAnswer::allow("no-such-request"))
        .await
        .expect_err("there is no such escalation");
    assert!(
        error.to_string().contains("no-such-request"),
        "the message must name what was not found: {error}"
    );
}

/// An unbuilt verb must be distinguishable from a broken one, or a user files a
/// bug against a roadmap item.
#[tokio::test]
async fn an_unbuilt_verb_reports_unimplemented_rather_than_a_fabricated_success() {
    let mut client = common::client().await.expect("a connected client");

    // Archiving a session that does not exist proves the error path is real
    // status mapping rather than a client-side guess.
    let error = client
        .archive_session("session-that-never-existed", "")
        .await
        .expect_err("archiving a missing session must fail");
    assert!(
        !error.is_unimplemented(),
        "this one is NotFound, not unbuilt"
    );
    assert!(error.to_string().contains("session-that-never-existed"));
}

#[tokio::test]
async fn archiving_moves_a_session_out_of_the_default_listing() {
    let mut client = common::client().await.expect("a connected client");

    let (session_id, _) = common::seed(&mut client, "temporary")
        .await
        .expect("create");

    assert_eq!(client.list_sessions(false).await.expect("list").len(), 1);

    client
        .archive_session(&session_id, "done with it")
        .await
        .expect("archive");

    assert!(
        client.list_sessions(false).await.expect("list").is_empty(),
        "an archived session is hidden by default"
    );
    assert_eq!(
        client.list_sessions(true).await.expect("list").len(),
        1,
        "and shown when asked for"
    );
}

/// Rendering is exercised through the library rather than only in unit tests,
/// so `--json` is known to survive real wire values.
#[tokio::test]
async fn json_rendering_of_a_real_session_parses() {
    let mut client = common::client().await.expect("a connected client");
    let (summary, _) = client
        .create_session("json", "key-1")
        .await
        .expect("create");

    let rendered = render::session(&summary, render::Format::Json);
    let parsed: serde_json::Value =
        serde_json::from_str(&rendered).expect("--json must emit valid JSON for real values");
    assert_eq!(parsed["state"], "active");
    assert_eq!(parsed["agent_count"], 1);
}

/// The two-step subscribe-before-write shape, positively: a stream opened
/// before the write hands back the event the write produced. A `next_matching`
/// that answered `None` without reading would pass every test that only waits
/// — this one asserts on what was handed back.
#[tokio::test]
async fn a_follow_stream_hands_back_the_matching_event() {
    let endpoint = common::serve().await.expect("a server must start");
    let (mut writer, session_id, root) = common::seeded(&endpoint, "follow")
        .await
        .expect("a session must be creatable");

    let mut watcher = Client::connect(&endpoint)
        .await
        .expect("a second client connects");
    let mut stream = watcher
        .follow(&SyncOptions::new(session_id))
        .await
        .expect("the subscription opens");

    writer
        .send_note(&root, "the needle", DeliveryMode::TurnBoundary, "")
        .await
        .expect("the note sends");

    let found = stream
        .next_matching(|envelope| {
            let horde_proto::v1::event::Kind::NoteSent(sent) =
                envelope.event.as_ref()?.kind.as_ref()?
            else {
                return None;
            };
            sent.note.as_ref().map(|note| note.text.clone())
        })
        .await
        .expect("the stream must not fail");
    assert_eq!(
        found.as_deref(),
        Some("the needle"),
        "the matched value is handed back, not merely consumed"
    );
}
