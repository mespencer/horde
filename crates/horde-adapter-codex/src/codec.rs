//! Newline-delimited JSON-RPC 2.0 in and out.
//!
//! Decoding classifies a line into a [`Frame`]; encoding builds the three
//! requests a Worker ever sends. Request ids are the caller's to allocate,
//! because correlation is the Worker's job — a codec that owned a counter would
//! fight the supervisor that actually multiplexes the stream.

use horde_adapter::{AdapterError, Result};
use serde_json::{Value, json};

use crate::wire::{Frame, Notification, RpcError};

/// Decodes one line of app-server output.
///
/// # Errors
///
/// [`AdapterError::Protocol`] when the line is not JSON, or is JSON that is
/// neither a response, a server request, nor a notification. Unknown *methods*
/// are not an error — see [`Notification::classify`].
pub fn decode_line(line: &str) -> Result<Frame> {
    let value: Value = serde_json::from_str(line).map_err(|source| AdapterError::Protocol {
        message: format!("app-server line is not JSON: {source}"),
        raw: line.to_owned(),
    })?;

    let id = value.get("id").and_then(Value::as_u64);
    let method = value.get("method").and_then(Value::as_str);

    match (id, method) {
        // Both id and method: the server is asking us something.
        (Some(id), Some(method)) => Ok(Frame::ServerRequest {
            id,
            method: method.to_owned(),
            params: value.get("params").cloned().unwrap_or(Value::Null),
        }),
        // id without method: a reply to us.
        (Some(id), None) => Ok(Frame::Response {
            id,
            result: value.get("result").cloned(),
            error: value
                .get("error")
                .and_then(|e| serde_json::from_value::<RpcError>(e.clone()).ok()),
        }),
        // method without id: a notification.
        (None, Some(method)) => Ok(Frame::Notification(Notification::classify(
            method,
            value.get("params").cloned().unwrap_or(Value::Null),
        ))),
        (None, None) => Err(AdapterError::Protocol {
            message: "app-server frame has neither id nor method".to_owned(),
            raw: line.to_owned(),
        }),
    }
}

/// `turn/start` — send a message and begin a turn.
#[must_use]
pub fn encode_turn_start(id: u64, thread_id: &str, text: &str) -> String {
    request(
        id,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": text }],
        }),
    )
}

/// `turn/steer` — deliver a message into a turn already running.
///
/// `expected_turn_id` is a precondition the server enforces: the request fails
/// if that turn is no longer the active one, which is what stops a steer racing
/// a turn that ended while it was in flight.
#[must_use]
pub fn encode_steer(id: u64, thread_id: &str, expected_turn_id: &str, text: &str) -> String {
    request(
        id,
        "turn/steer",
        json!({
            "threadId": thread_id,
            "expectedTurnId": expected_turn_id,
            "input": [{ "type": "text", "text": text }],
        }),
    )
}

/// `turn/interrupt` — cancel the turn in flight.
///
/// The caller must not then wait for `turn/completed`; see [`crate::turn`].
#[must_use]
pub fn encode_interrupt(id: u64, thread_id: &str) -> String {
    request(id, "turn/interrupt", json!({ "threadId": thread_id }))
}

fn request(id: u64, method: &str, params: Value) -> String {
    json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{decode_line, encode_interrupt, encode_steer, encode_turn_start};
    use crate::wire::{Frame, Notification};

    #[test]
    fn a_notification_is_a_method_without_an_id() {
        let frame = decode_line(
            &json!({"method": "turn/started", "params": {"turn": {"id": "t1"}}}).to_string(),
        )
        .expect("recorded notifications must decode");

        match frame {
            Frame::Notification(Notification::TurnStarted(started)) => {
                assert_eq!(started.turn.id, "t1");
            }
            other => panic!("expected a turn/started notification, got {other:?}"),
        }
    }

    #[test]
    fn a_server_request_carries_both_an_id_and_a_method() {
        // Codex 0.145.0 never sent one of these (ADR-0005), but the frame kind
        // exists and must not be mistaken for a response to something we asked.
        let frame = decode_line(
            &json!({"id": 7, "method": "item/commandExecution/requestApproval",
                    "params": {"command": "rm -rf /"}})
            .to_string(),
        )
        .expect("decodes");

        assert!(
            matches!(frame, Frame::ServerRequest { id: 7, .. }),
            "an id plus a method is a request to us, not a reply to us"
        );
    }

    #[test]
    fn a_response_is_an_id_without_a_method() {
        let frame = decode_line(&json!({"id": 3, "result": {"turnId": "t1"}}).to_string())
            .expect("decodes");
        assert!(matches!(frame, Frame::Response { id: 3, .. }));
    }

    #[test]
    fn a_frame_with_neither_id_nor_method_is_a_protocol_error() {
        let error = decode_line(&json!({"jsonrpc": "2.0"}).to_string())
            .expect_err("an unaddressable frame cannot be acted on");
        assert!(error.to_string().contains("neither id nor method"));
    }

    #[test]
    fn a_line_that_is_not_json_is_a_protocol_error() {
        let error = decode_line("not json at all").expect_err("garbage must not parse");
        assert!(error.to_string().contains("not JSON"));
    }

    #[test]
    fn steer_carries_the_precondition_that_stops_it_racing_a_finished_turn() {
        let encoded = encode_steer(4, "thread-1", "turn-9", "stop counting");
        let value: Value = serde_json::from_str(&encoded).expect("we produced this");

        assert_eq!(value["method"], "turn/steer");
        assert_eq!(value["params"]["expectedTurnId"], "turn-9");
        assert_eq!(value["params"]["input"][0]["text"], "stop counting");
    }

    #[test]
    fn the_encoders_produce_addressable_jsonrpc() {
        for encoded in [
            encode_turn_start(1, "thread-1", "hello"),
            encode_interrupt(2, "thread-1"),
        ] {
            let value: Value = serde_json::from_str(&encoded).expect("we produced this");
            assert_eq!(value["jsonrpc"], "2.0");
            assert!(
                value["id"].is_u64(),
                "a request without an id cannot be correlated"
            );
            assert!(value["method"].is_string());
        }
    }
}
