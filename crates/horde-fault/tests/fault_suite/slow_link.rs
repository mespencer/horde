//! Scenario (d): the link goes slow, then silently dead — and heartbeats
//! are what tell a quiet stream from a stale one.
//!
//! W5d's machinery under attack: a follow stream owes a `SyncHeartbeat`
//! whenever it has been silent past its period, so "no data" and "no
//! connection" are distinguishable on both ends. The faults are the spotty
//! network's two faces the seed proxy could not stage: added latency (slow
//! but alive — heartbeats keep arriving) and a blackhole (sockets open,
//! nothing moves — heartbeats stop, which IS the staleness signal). The
//! recovery asserted: nothing deadlocks, and the bytes held in the hole —
//! heartbeats and a real event appended mid-blackhole — arrive on the same
//! stream after heal, the event exactly once.

use std::time::Duration;

use horde_proto::prost::Message;
use horde_proto::v1::control_plane_service_client::ControlPlaneServiceClient;
use horde_proto::v1::control_plane_service_server::ControlPlaneServiceServer;
use horde_proto::v1::{
    Event, SessionCursor, SessionId, SyncEventsRequest, SyncEventsResponse, TurnInterrupted,
    sync_events_response::Update,
};
use horde_server::EventLog;
use horde_server::log::PendingEvent;
use horde_server::service::ControlPlane;
use horde_testkit::FaultProxy;
use tokio_stream::wrappers::TcpListenerStream;

use crate::support;

/// The shrunken heartbeat period: long enough to be real traffic, short
/// enough that a stream owing several costs the suite milliseconds.
const HEARTBEAT: Duration = Duration::from_millis(100);

/// Serves only the control plane, with the heartbeat period under the
/// test's control — the one knob `horde_server_testkit::serve_core` does
/// not expose.
async fn serve_with_heartbeat(core: horde_server::ServerCore) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("a loopback port");
    let address = listener.local_addr().expect("a bound address");
    let plane = ControlPlane::new(core).with_heartbeat_period(HEARTBEAT);
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ControlPlaneServiceServer::new(plane))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    format!("http://{address}")
}

/// Appends one marker event the stream should deliver exactly once.
async fn append_marker(log: &EventLog, session: &str, marker: &str) {
    let event = Event {
        agent_id: None,
        occurred_at: None,
        idempotency_key: format!("marker:{marker}"),
        kind: Some(horde_proto::v1::event::Kind::TurnInterrupted(
            TurnInterrupted {
                requested_by: None,
                turn_id: String::new(),
                reason: marker.to_owned(),
            },
        )),
    };
    log.append(PendingEvent {
        session_id: session.to_owned(),
        idempotency_key: event.idempotency_key.clone(),
        payload: event.encode_to_vec(),
        ..PendingEvent::default()
    })
    .await
    .expect("the marker appends");
}

/// The next frame within `ceiling`, or `None` when the ceiling elapsed —
/// which is exactly how a client marks a link stale: not by an error, but
/// by a heartbeat that failed to arrive on time.
async fn frame_within(
    stream: &mut tonic::Streaming<SyncEventsResponse>,
    ceiling: Duration,
) -> Option<Update> {
    match tokio::time::timeout(ceiling, stream.message()).await {
        Err(_elapsed) => None,
        Ok(message) => message
            .expect("the stream itself must not fail")
            .and_then(|response| response.update),
    }
}

/// Reads frames until a heartbeat arrives, bounded by count — batches and
/// catch-up markers in between are fine, silence past the ceiling is not.
async fn heartbeat_within(
    stream: &mut tonic::Streaming<SyncEventsResponse>,
    ceiling: Duration,
    what: &str,
) {
    for _ in 0_u32..32 {
        match frame_within(stream, ceiling).await {
            Some(Update::Heartbeat(beat)) => {
                assert!(
                    beat.server_time.is_some(),
                    "a heartbeat carries the server's clock, or it marks nothing"
                );
                return;
            }
            Some(_) => {}
            None => panic!("{what}: no heartbeat within {ceiling:?}"),
        }
    }
    panic!("{what}: 32 frames and none of them a heartbeat");
}

#[tokio::test]
async fn heartbeats_mark_a_stale_link_and_the_healed_stream_catches_up() {
    let core = support::fresh_core();
    let log = core.log.clone();
    append_marker(&log, "s1", "history").await;
    let upstream = serve_with_heartbeat(core).await;
    let proxy = FaultProxy::front(&upstream).await.expect("the proxy binds");

    let channel = tonic::transport::Endpoint::try_from(proxy.endpoint())
        .expect("the endpoint parses")
        .connect()
        .await
        .expect("the channel connects");
    let mut stream = ControlPlaneServiceClient::new(channel)
        .sync_events(SyncEventsRequest {
            cursors: vec![SessionCursor {
                session_id: Some(SessionId::new("s1")),
                from_sequence: 1,
            }],
            all_sessions: false,
            subtree_root: None,
            follow: true,
            max_batch_events: 0,
        })
        .await
        .expect("the follow stream opens")
        .into_inner();

    // Healthy: the replay arrives, then the quiet stream owes heartbeats.
    let first = support::within("the replayed history", stream.message())
        .await
        .expect("the stream carries")
        .and_then(|response| response.update);
    assert!(
        matches!(first, Some(Update::Batch(_))),
        "history first: {first:?}"
    );
    for _ in 0_u32..3 {
        heartbeat_within(&mut stream, Duration::from_secs(2), "the healthy link").await;
    }

    // Slow is not stale: with real added latency, heartbeats still arrive
    // inside a client's patience — the link is degraded, not dead.
    proxy.delay(Duration::from_millis(120));
    heartbeat_within(&mut stream, Duration::from_secs(2), "the delayed link").await;

    // Blackholed: nothing arrives, nothing errors. The bounded absence of a
    // heartbeat IS the staleness verdict — this wait is the client-side
    // staleness detector running for real, and its positive control is the
    // heal below, which proves the same stream was alive the whole time.
    proxy.heal();
    proxy.blackhole();
    let while_dark = frame_within(&mut stream, Duration::from_millis(700)).await;
    assert!(
        while_dark.is_none(),
        "a blackholed link must deliver nothing — got {while_dark:?}"
    );

    // Something real happens while the link is dark.
    append_marker(&log, "s1", "appended-in-the-dark").await;

    // Heal: the SAME stream drains the hole — heartbeats resume and the
    // dark-time event arrives exactly once, no redial, no deadlock.
    proxy.heal();
    let mut dark_markers = 0_u32;
    for _ in 0_u32..64 {
        match frame_within(&mut stream, Duration::from_secs(2)).await {
            Some(Update::Batch(batch)) => {
                for envelope in &batch.events {
                    let reason = envelope.event.as_ref().and_then(|event| match &event.kind {
                        Some(horde_proto::v1::event::Kind::TurnInterrupted(interrupted)) => {
                            Some(interrupted.reason.clone())
                        }
                        _ => None,
                    });
                    if reason.as_deref() == Some("appended-in-the-dark") {
                        dark_markers += 1;
                    }
                }
            }
            Some(_) => {}
            None => break, // A full quiet period with no more frames: drained.
        }
        if dark_markers > 0 {
            break;
        }
    }
    assert_eq!(
        dark_markers, 1,
        "the event appended during the blackhole arrives exactly once, on the same stream"
    );
    heartbeat_within(&mut stream, Duration::from_secs(2), "the healed link").await;
}
