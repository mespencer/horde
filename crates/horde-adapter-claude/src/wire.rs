//! Serde types for the `claude` stream-json protocol.
//!
//! Transcribed from real captures (`docs/spikes/transcripts/`), not from
//! documentation — the protocol is undocumented in places, and the spike found
//! at least one flag (`--permission-prompt-tool`) that works while being absent
//! from `--help`.
//!
//! Two rules shape everything here:
//!
//! * **Nothing is dropped.** Every decoded envelope keeps its raw [`Value`]
//!   alongside the typed view. A Worker journals what the harness actually
//!   said, not the subset this crate happened to model, so a protocol change
//!   shows up as an unrecognized event rather than as silence.
//! * **Unknown shapes are a variant, not an error.** A new event type from a
//!   newer CLI must not fail a session.

use serde::Deserialize;
use serde_json::Value;

/// One line of the harness's stdout: the raw envelope plus a typed view.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub raw: Value,
    pub event: Event,
}

impl Envelope {
    /// The harness conversation id carried by nearly every envelope.
    #[must_use]
    pub fn session_id(&self) -> Option<&str> {
        self.raw.get("session_id").and_then(Value::as_str)
    }
}

/// The typed view of an envelope.
///
/// [`Event::Result`] is boxed because it is an order of magnitude larger than
/// every other variant and would otherwise set the size of all of them.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// `system`/`init`. Emitted **once per turn**, not once per process — the
    /// spike saw three of them on a single three-turn process. Treating a
    /// second one as a new process or a protocol violation breaks on turn two.
    Init(Box<Init>),
    /// A message from the model: text, thinking, or a tool call.
    Assistant(Message),
    /// Tool results, echoed back through the user role.
    User(Message),
    /// Closes a turn. Exactly one per user message.
    Result(Box<TurnResult>),
    /// Acknowledges a `control_request`, e.g. an interrupt.
    ControlResponse(ControlResponse),
    /// The provider's own rate-limit state.
    RateLimit(RateLimit),
    /// Token-level deltas, only present with `--include-partial-messages`.
    /// Carries no information the assembled messages do not.
    StreamEvent,
    /// Other `system` subtypes: `thinking_tokens`, `status`, hook lifecycle.
    System { subtype: String },
    /// An envelope this crate does not model. Its raw form is still preserved.
    Unknown { event_type: String },
}

/// `system`/`init`: the turn header.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Init {
    pub session_id: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub cwd: String,
    #[serde(default)]
    pub tools: Vec<String>,
    /// Protocol features this CLI build supports, e.g. `interrupt_receipt_v1`.
    ///
    /// The honest place to negotiate behavior across CLI versions — better than
    /// pinning a version string, since the protocol is not a frozen contract.
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub permission_mode: Option<String>,
}

/// An `assistant` or `user` envelope.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Message {
    pub message: MessageBody,
    /// Set when the message came from a harness-native subagent.
    #[serde(default)]
    pub parent_tool_use_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct MessageBody {
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub content: Vec<ContentBlock>,
}

/// One block inside a message.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    Thinking {
        thinking: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        tool_use_id: String,
        #[serde(default)]
        is_error: bool,
        /// Free-form: a string for most tools, an array of blocks for others.
        #[serde(default)]
        content: Value,
    },
    /// A block type this crate does not model.
    #[serde(other)]
    Other,
}

/// The `result` envelope that closes a turn.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnResult {
    pub subtype: String,
    /// The authoritative success flag.
    ///
    /// `subtype` alone is not sufficient: an unauthenticated run reports
    /// `subtype: "success"` with `is_error: true`. Code that branches on
    /// `subtype` reports a failed agent as a healthy one.
    #[serde(default)]
    pub is_error: bool,
    pub session_id: String,
    #[serde(default)]
    pub result: Option<String>,
    /// Cumulative for the process lifetime, not per-turn.
    #[serde(default)]
    pub total_cost_usd: f64,
    /// Wall-clock for this turn alone.
    #[serde(default)]
    pub duration_ms: u64,
    #[serde(default)]
    pub terminal_reason: Option<String>,
    /// Per-turn token counts. Excludes auxiliary models, so it is not the
    /// billing truth — see [`Self::model_usage`].
    #[serde(default)]
    pub usage: ResultUsage,
    /// Per-model running totals, camelCase where [`Self::usage`] is snake_case.
    #[serde(rename = "modelUsage", default)]
    pub model_usage: std::collections::BTreeMap<String, ModelUsage>,
    #[serde(default)]
    pub permission_denials: Vec<WireDenial>,
}

impl TurnResult {
    /// True only when the turn completed *and* carried no error flag.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        !self.is_error && self.subtype == "success"
    }

    /// Whether this result reports the session as unauthenticated.
    ///
    /// The harness signals this as an ordinary-looking result envelope, so it
    /// has to be recognized rather than waited for as a spawn failure.
    #[must_use]
    pub fn is_unauthenticated(&self) -> bool {
        self.is_error && self.terminal_reason.as_deref() == Some("api_error")
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
pub struct ResultUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

/// Running totals for one model. Note the casing: this object is camelCase
/// while its sibling `usage` is snake_case. Getting that wrong silently zeroes
/// every field, since each one defaults.
#[derive(Debug, Clone, Copy, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(rename = "costUSD", default)]
    pub cost_usd: f64,
}

/// A tool call the permission layer refused, as reported at turn end.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WireDenial {
    pub tool_name: String,
    #[serde(default)]
    pub tool_use_id: String,
    #[serde(default)]
    pub tool_input: Value,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ControlResponse {
    pub response: ControlResponseBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ControlResponseBody {
    #[serde(default)]
    pub subtype: String,
    #[serde(default)]
    pub request_id: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RateLimit {
    pub rate_limit_info: RateLimitInfo,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RateLimitInfo {
    #[serde(default)]
    pub status: String,
    #[serde(default, rename = "rateLimitType")]
    pub rate_limit_type: String,
    #[serde(default, rename = "resetsAt")]
    pub resets_at: Option<i64>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::TurnResult;

    /// The conversation id is read from wherever the harness put it.
    ///
    /// Untested until now, and the mutants showed why that mattered: returning
    /// `None`, `Some("")`, or a constant all passed. That id is what makes an
    /// agent revivable, so a wrong one is not a display bug — it is a
    /// conversation that cannot be found again.
    #[test]
    fn the_session_id_comes_from_the_envelope_not_from_a_default() {
        // Decoded the way a live session decodes it, so the test exercises the
        // real path rather than a hand-built struct.
        let envelope = crate::codec::decode_line(
            &json!({"type": "system", "subtype": "init", "session_id": "sess-7", "model": "m"})
                .to_string(),
        )
        .expect("the recorded shape must decode");
        assert_eq!(envelope.session_id(), Some("sess-7"));

        let anonymous = crate::codec::decode_line(&json!({"type": "stream_event"}).to_string())
            .expect("decodes");
        assert_eq!(
            anonymous.session_id(),
            None,
            "an envelope without an id must report none rather than an empty one"
        );
    }

    fn result_with(is_error: bool, terminal_reason: Option<&str>) -> TurnResult {
        let mut value = json!({
            "subtype": "error",
            "is_error": is_error,
            "session_id": "s1",
            "duration_ms": 1,
        });
        if let Some(reason) = terminal_reason {
            value["terminal_reason"] = json!(reason);
        }
        serde_json::from_value(value).expect("the recorded shape must parse")
    }

    /// An unauthenticated session arrives as an ordinary-looking result, so
    /// **both** halves of the check carry weight.
    ///
    /// A failure that is not an auth failure must not be reported as one — the
    /// scheduler's response differs: a bad credential needs a different
    /// account, an ordinary error can be retried where it stands.
    #[test]
    fn only_an_api_error_failure_reads_as_unauthenticated() {
        assert!(
            result_with(true, Some("api_error")).is_unauthenticated(),
            "an errored result with api_error is the unauthenticated shape"
        );
        assert!(
            !result_with(false, Some("api_error")).is_unauthenticated(),
            "a successful turn is never unauthenticated, whatever its reason"
        );
        assert!(
            !result_with(true, Some("max_turns")).is_unauthenticated(),
            "an ordinary failure must not be mistaken for a credential problem"
        );
        assert!(
            !result_with(true, None).is_unauthenticated(),
            "and neither must a failure with no reason at all"
        );
    }
}
