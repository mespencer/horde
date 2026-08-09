//! Serde types for the `codex app-server` protocol, transcribed from the W2c
//! golden transcripts rather than from the schema.
//!
//! The schema (`codex app-server generate-json-schema`) declares 89 client
//! methods and 70 server notifications. Almost none of them matter to a Worker,
//! so this models the frames that actually appeared in a driven session and
//! lets the rest fall into [`Notification::Other`]. That is deliberate: a codec
//! that enumerated all 70 would break on every Codex release, whereas one that
//! ignores what it does not need only breaks when something it *uses* changes.
//!
//! Framing is newline-delimited JSON-RPC 2.0. A frame is a response (has `id`
//! and `result`/`error`), a server→client request (has both `id` and `method`),
//! or a notification (has `method`, no `id`).

use serde::{Deserialize, Serialize};

/// One decoded line from the app-server's stdout.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// A reply to something Horde asked. Correlated by `id`.
    Response {
        id: u64,
        result: Option<serde_json::Value>,
        error: Option<RpcError>,
    },
    /// The server asking Horde something — approvals arrive this way on
    /// harnesses that route them, which Codex 0.145.0 does not (ADR-0005).
    ServerRequest {
        id: u64,
        method: String,
        params: serde_json::Value,
    },
    /// Everything else, and where all turn output lives.
    Notification(Notification),
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

/// The notifications this codec acts on.
///
/// `method` is matched exhaustively into this enum by [`Notification::classify`];
/// anything unrecognized becomes [`Notification::Other`] and is dropped rather
/// than erroring, because a newer Codex emitting more is normal.
#[derive(Debug, Clone, PartialEq)]
pub enum Notification {
    ThreadStarted(ThreadStarted),
    /// `thread/status/changed`. The **only** terminal signal after an
    /// interrupt — `turn/interrupt` produces no `turn/completed` at all.
    ThreadStatusChanged(ThreadStatus),
    TurnStarted(TurnRef),
    TurnCompleted(TurnCompleted),
    AgentMessageDelta(AgentMessageDelta),
    ItemCompleted(ItemCompleted),
    TokenUsageUpdated(TokenUsageUpdated),
    RateLimitsUpdated(RateLimits),
    Other {
        method: String,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ThreadStarted {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(default)]
    pub thread: Option<ThreadRef>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ThreadRef {
    pub id: String,
    #[serde(default)]
    pub model: Option<String>,
}

/// `thread/status/changed`. `idle` is what closes an interrupted turn.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ThreadStatus {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    pub status: StatusBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StatusBody {
    #[serde(rename = "type")]
    pub kind: String,
}

impl ThreadStatus {
    #[must_use]
    pub fn is_idle(&self) -> bool {
        self.status.kind == "idle"
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnRef {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    pub turn: TurnBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnBody {
    pub id: String,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "durationMs", default)]
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnCompleted {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    pub turn: TurnBody,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AgentMessageDelta {
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    #[serde(rename = "itemId", default)]
    pub item_id: Option<String>,
    pub delta: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ItemCompleted {
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    pub item: Item,
}

/// A completed item. `phase` is what distinguishes a turn's final answer from
/// its running commentary — and a steered turn produces *two* items with
/// `phase: "final_answer"`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Item {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub phase: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(rename = "exitCode", default)]
    pub exit_code: Option<i64>,
}

impl Item {
    #[must_use]
    pub fn is_final_answer(&self) -> bool {
        self.kind == "agentMessage" && self.phase.as_deref() == Some("final_answer")
    }

    #[must_use]
    pub fn is_command_execution(&self) -> bool {
        self.kind == "commandExecution"
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TokenUsageUpdated {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    #[serde(rename = "tokenUsage")]
    pub token_usage: TokenUsageBody,
}

/// Codex reports both a per-thread running `total` and a per-turn `last`.
///
/// `last` is the turn, which is why this codec needs no accumulator the way the
/// Claude one does — but resume replays a prior `last`, so idempotence is still
/// required (see [`crate::turn`]).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TokenUsageBody {
    #[serde(default)]
    pub last: Option<TokenCounts>,
    #[serde(default)]
    pub total: Option<TokenCounts>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
pub struct TokenCounts {
    #[serde(rename = "inputTokens", default)]
    pub input_tokens: u64,
    #[serde(rename = "outputTokens", default)]
    pub output_tokens: u64,
    #[serde(rename = "cachedInputTokens", default)]
    pub cached_input_tokens: u64,
    #[serde(rename = "cacheWriteInputTokens", default)]
    pub cache_write_input_tokens: u64,
    #[serde(rename = "reasoningOutputTokens", default)]
    pub reasoning_output_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RateLimits {
    #[serde(rename = "rateLimits")]
    pub rate_limits: RateLimitBody,
}

/// Codex reports a **percentage**, never absolute counts — the finding that
/// forced `horde.v1.RateWindow` to carry both shapes.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RateLimitBody {
    #[serde(rename = "limitId", default)]
    pub limit_id: Option<String>,
    #[serde(default)]
    pub primary: Option<RateWindowBody>,
}

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
pub struct RateWindowBody {
    #[serde(rename = "usedPercent", default)]
    pub used_percent: f64,
    #[serde(rename = "windowDurationMins", default)]
    pub window_duration_mins: Option<u64>,
    #[serde(rename = "resetsAt", default)]
    pub resets_at: Option<i64>,
}

impl Notification {
    /// Routes a raw `(method, params)` pair into a typed notification.
    ///
    /// Unrecognized methods and params that do not fit their method both become
    /// [`Notification::Other`]: a codec that refused either would turn a Codex
    /// upgrade into an outage, and nothing this crate does depends on frames it
    /// cannot read.
    #[must_use]
    pub fn classify(method: &str, params: serde_json::Value) -> Self {
        fn parse<T: for<'de> Deserialize<'de>>(params: serde_json::Value) -> Option<T> {
            serde_json::from_value(params).ok()
        }

        let other = || Self::Other {
            method: method.to_owned(),
        };

        match method {
            "thread/started" => parse(params).map_or_else(other, Self::ThreadStarted),
            "thread/status/changed" => parse(params).map_or_else(other, Self::ThreadStatusChanged),
            "turn/started" => parse(params).map_or_else(other, Self::TurnStarted),
            "turn/completed" => parse(params).map_or_else(other, Self::TurnCompleted),
            "item/agentMessage/delta" => parse(params).map_or_else(other, Self::AgentMessageDelta),
            "item/completed" => parse(params).map_or_else(other, Self::ItemCompleted),
            "thread/tokenUsage/updated" => {
                parse(params).map_or_else(other, Self::TokenUsageUpdated)
            }
            "account/rateLimits/updated" => {
                parse(params).map_or_else(other, Self::RateLimitsUpdated)
            }
            _ => other(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{Item, Notification, ThreadStatus};

    #[test]
    fn an_unknown_method_is_ignored_rather_than_refused() {
        // 70 notification methods exist and this codec reads eight. A Codex
        // release adding a 71st must not break a Worker.
        assert_eq!(
            Notification::classify("hook/started", json!({"anything": true})),
            Notification::Other {
                method: "hook/started".to_owned()
            }
        );
    }

    #[test]
    fn a_known_method_with_unreadable_params_degrades_instead_of_erroring() {
        // Same reasoning one level down: the method is known but the body
        // changed shape. Dropping the frame beats failing the turn.
        assert_eq!(
            Notification::classify("turn/completed", json!({"unexpected": "shape"})),
            Notification::Other {
                method: "turn/completed".to_owned()
            }
        );
    }

    #[test]
    fn idle_is_recognized_as_the_status_that_ends_an_interrupted_turn() {
        let idle: ThreadStatus =
            serde_json::from_value(json!({"threadId": "t", "status": {"type": "idle"}}))
                .expect("the recorded shape must parse");
        assert!(idle.is_idle());

        let active: ThreadStatus = serde_json::from_value(
            json!({"threadId": "t", "status": {"type": "active", "activeFlags": []}}),
        )
        .expect("the recorded shape must parse");
        assert!(!active.is_idle());
    }

    #[test]
    fn only_the_final_answer_phase_counts_as_a_final_answer() {
        let final_answer: Item = serde_json::from_value(
            json!({"type": "agentMessage", "text": "STEERED", "phase": "final_answer"}),
        )
        .expect("parses");
        let commentary: Item = serde_json::from_value(
            json!({"type": "agentMessage", "text": "I'll run that", "phase": "commentary"}),
        )
        .expect("parses");

        assert!(final_answer.is_final_answer());
        assert!(
            !commentary.is_final_answer(),
            "commentary is not the turn's answer"
        );
    }
}
