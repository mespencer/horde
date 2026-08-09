//! Decoding harness output and encoding harness input.
//!
//! Pure functions over strings: no process, no I/O, no async. That is what lets
//! the conformance suite drive this layer from recorded bytes and get exactly
//! the behavior a live process would produce.

use horde_adapter::AdapterError;
use serde_json::{Value, json};

use crate::wire::{Envelope, Event};

/// Decodes one line of the harness's stdout.
///
/// An envelope whose `type` this crate does not model decodes to
/// [`Event::Unknown`] with its raw form intact. Only malformed JSON, or an
/// envelope whose known shape fails to parse, is an error — the first means the
/// stream is corrupt, the second means the protocol moved under us, and both
/// need the raw line to diagnose.
pub fn decode_line(line: &str) -> Result<Envelope, AdapterError> {
    let raw: Value = serde_json::from_str(line).map_err(|source| AdapterError::Protocol {
        message: format!("line is not valid JSON: {source}"),
        raw: line.to_owned(),
    })?;

    let event_type = raw
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| AdapterError::Protocol {
            message: "envelope has no `type` field".to_owned(),
            raw: line.to_owned(),
        })?
        .to_owned();

    let event = match event_type.as_str() {
        "assistant" => Event::Assistant(typed(&raw, &event_type, line)?),
        "user" => Event::User(typed(&raw, &event_type, line)?),
        "result" => Event::Result(Box::new(typed(&raw, &event_type, line)?)),
        "control_response" => Event::ControlResponse(typed(&raw, &event_type, line)?),
        "rate_limit_event" => Event::RateLimit(typed(&raw, &event_type, line)?),
        "stream_event" => Event::StreamEvent,
        "system" => match raw.get("subtype").and_then(Value::as_str) {
            Some("init") => Event::Init(Box::new(typed(&raw, &event_type, line)?)),
            other => Event::System {
                subtype: other.unwrap_or_default().to_owned(),
            },
        },
        _ => Event::Unknown { event_type },
    };

    Ok(Envelope { raw, event })
}

/// Deserializes a known envelope shape, keeping the raw line in the error so a
/// protocol change is diagnosable from the report alone.
fn typed<T: serde::de::DeserializeOwned>(
    raw: &Value,
    event_type: &str,
    line: &str,
) -> Result<T, AdapterError> {
    serde_json::from_value(raw.clone()).map_err(|source| AdapterError::Protocol {
        message: format!("malformed `{event_type}` envelope: {source}"),
        raw: line.to_owned(),
    })
}

/// Encodes a user message for the harness's stdin.
#[must_use]
pub fn encode_user_message(text: &str) -> String {
    json!({
        "type": "user",
        "message": { "role": "user", "content": [{ "type": "text", "text": text }] }
    })
    .to_string()
}

/// Encodes an interrupt.
///
/// This is the only mechanism that aborts a turn while leaving the process
/// usable. The spike measured the alternatives: SIGINT aborts the turn but ends
/// the process, and closing stdin does not abort at all — it lets the turn run
/// to completion and only then exits. Both would silently break steering.
#[must_use]
pub fn encode_interrupt(request_id: &str) -> String {
    json!({
        "type": "control_request",
        "request_id": request_id,
        "request": { "subtype": "interrupt" }
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::{decode_line, encode_interrupt, encode_user_message};
    use crate::wire::{ContentBlock, Event};

    #[test]
    fn an_unmodelled_event_type_decodes_rather_than_failing() {
        let line = r#"{"type":"something_new_in_a_later_cli","payload":{"a":1}}"#;
        let envelope = decode_line(line).expect("unknown types must decode");
        assert!(matches!(envelope.event, Event::Unknown { .. }));
        assert_eq!(envelope.raw["payload"]["a"], 1);
    }

    #[test]
    fn malformed_json_reports_the_offending_line() {
        let error = decode_line("{not json").expect_err("must reject");
        assert!(
            error.to_string().contains("{not json"),
            "the raw line is the only way to diagnose a drifted protocol, got: {error}"
        );
    }

    #[test]
    fn an_envelope_without_a_type_is_rejected() {
        let error = decode_line(r#"{"session_id":"abc"}"#).expect_err("must reject");
        assert!(error.to_string().contains("no `type` field"));
    }

    #[test]
    fn init_is_recognized_by_its_subtype_not_its_type() {
        let line =
            r#"{"type":"system","subtype":"init","session_id":"s1","model":"claude-haiku-4-5"}"#;
        let envelope = decode_line(line).expect("init must decode");
        match envelope.event {
            Event::Init(init) => {
                assert_eq!(init.session_id, "s1");
                assert_eq!(init.model, "claude-haiku-4-5");
            }
            other => panic!("expected Init, got {other:?}"),
        }
    }

    #[test]
    fn other_system_subtypes_are_kept_distinct_from_init() {
        let line = r#"{"type":"system","subtype":"thinking_tokens","estimated_tokens":27}"#;
        let envelope = decode_line(line).expect("must decode");
        assert_eq!(
            envelope.event,
            Event::System {
                subtype: "thinking_tokens".to_owned()
            }
        );
    }

    #[test]
    fn a_tool_use_block_keeps_its_input_as_structured_data() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[
            {"type":"tool_use","id":"toolu_01","name":"Write","input":{"file_path":"/tmp/x","content":"hi"}}]}}"#;
        let envelope = decode_line(line).expect("must decode");
        match envelope.event {
            Event::Assistant(message) => match &message.message.content[0] {
                ContentBlock::ToolUse { id, name, input } => {
                    assert_eq!(id, "toolu_01");
                    assert_eq!(name, "Write");
                    assert_eq!(input["file_path"], "/tmp/x");
                }
                other => panic!("expected ToolUse, got {other:?}"),
            },
            other => panic!("expected Assistant, got {other:?}"),
        }
    }

    #[test]
    fn an_unmodelled_content_block_does_not_fail_the_message() {
        let line = r#"{"type":"assistant","message":{"role":"assistant","content":[
            {"type":"future_block_kind","whatever":true}]}}"#;
        let envelope = decode_line(line).expect("must decode");
        match envelope.event {
            Event::Assistant(message) => {
                assert_eq!(message.message.content, vec![ContentBlock::Other]);
            }
            other => panic!("expected Assistant, got {other:?}"),
        }
    }

    #[test]
    fn encoded_messages_are_single_line_because_the_protocol_is_line_delimited() {
        let encoded = encode_user_message("hello\nworld");
        assert!(
            !encoded.contains('\n'),
            "a literal newline would split one message into two: {encoded}"
        );
    }

    #[test]
    fn an_encoded_user_message_round_trips_through_the_decoder() {
        let encoded = encode_user_message("hello");
        let envelope = decode_line(&encoded).expect("our own encoding must decode");
        assert_eq!(envelope.raw["message"]["content"][0]["text"], "hello");
    }

    #[test]
    fn an_interrupt_carries_the_subtype_the_harness_looks_for() {
        let encoded = encode_interrupt("horde-1");
        let value: serde_json::Value = serde_json::from_str(&encoded).expect("valid json");
        assert_eq!(value["type"], "control_request");
        assert_eq!(value["request"]["subtype"], "interrupt");
        assert_eq!(value["request_id"], "horde-1");
    }
}
