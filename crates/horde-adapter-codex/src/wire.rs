//! Serde types for the `codex app-server` protocol, transcribed from the W2c
//! golden transcripts rather than from the schema.
//!
//! The schema (`codex app-server generate-json-schema`) declared 89 client
//! methods and 70 server notifications at 0.147; at 0.167 it is 106 client
//! requests, 86 notifications and 10 server requests. Almost none of them matter to a Worker,
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
    /// `item/started`. Read for one reason: the `mcpToolCall` item that opens
    /// *before* its elicitation arrives is where the tool's name and arguments
    /// live — the elicitation itself names only the server (transcript 17).
    ItemStarted(ItemCompleted),
    ItemCompleted(ItemCompleted),
    TokenUsageUpdated(TokenUsageUpdated),
    RateLimitsUpdated(RateLimits),
    /// The `error` notification (0.147.0 schema): a turn-scoped failure with
    /// a `willRetry` bit that separates a hiccup from a verdict.
    Error(ErrorBody),
    /// `serverRequest/resolved` (0.167): codex saying a server request is
    /// settled — after Horde's answer, or on its own (a question's
    /// `autoResolutionMs`, a turn ending under it). An approval Horde still
    /// holds for that request id is stale from here on.
    ServerRequestResolved(ServerRequestResolved),
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
    /// Populated only when `status` is `"failed"` (0.147.0 schema).
    #[serde(default)]
    pub error: Option<TurnErrorBody>,
}

/// The failure a turn or `error` notification carries.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TurnErrorBody {
    pub message: String,
    /// Camel-cased codex error code, e.g. `"unauthorized"`,
    /// `"usageLimitExceeded"`. A string or an object in the schema; only the
    /// string form is read, because only the string form is matched on.
    #[serde(rename = "codexErrorInfo", default)]
    pub codex_error_info: Option<serde_json::Value>,
}

impl TurnErrorBody {
    /// The error code, when codex reported one in its string form.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        self.codex_error_info.as_ref().and_then(|v| v.as_str())
    }
}

/// The `error` notification: `{error, threadId, turnId, willRetry}`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ErrorBody {
    pub error: TurnErrorBody,
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    #[serde(rename = "willRetry", default)]
    pub will_retry: bool,
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
    /// Boxed because [`Item`] is the union of every item kind's fields, and an
    /// unboxed copy would bloat every [`Notification`] a session ever decodes.
    pub item: Box<Item>,
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
    /// `mcpToolCall` items: which configured MCP server the call targets.
    #[serde(default)]
    pub server: Option<String>,
    /// `mcpToolCall` items: the tool's own name.
    #[serde(default)]
    pub tool: Option<String>,
    /// `mcpToolCall` items: the full arguments, before the call runs.
    #[serde(default)]
    pub arguments: Option<serde_json::Value>,
    /// `mcpToolCall` items: why the call failed — a declined elicitation
    /// reads `"user rejected MCP tool call"` here (transcript 18).
    #[serde(default)]
    pub error: Option<ItemError>,
    /// `fileChange` items: the patch, per file, before it is applied — what
    /// a file-change approval is judged on, since the approval itself names
    /// only the item (transcript 36).
    #[serde(default)]
    pub changes: Option<Vec<FileChange>>,
}

/// One file inside a `fileChange` item.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FileChange {
    pub path: String,
    /// `{"type":"add"}`, `{"type":"delete"}`, `{"type":"update", …}` — kept
    /// whole, so a rule or a human sees exactly what codex said.
    #[serde(default)]
    pub kind: serde_json::Value,
    #[serde(default)]
    pub diff: Option<String>,
}

/// `serverRequest/resolved`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServerRequestResolved {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "requestId")]
    pub request_id: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ItemError {
    #[serde(default)]
    pub message: String,
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

    #[must_use]
    pub fn is_mcp_tool_call(&self) -> bool {
        self.kind == "mcpToolCall"
    }

    #[must_use]
    pub fn is_file_change(&self) -> bool {
        self.kind == "fileChange"
    }
}

/// `mcpServer/elicitation/request` — the MCP tool-call interception point.
///
/// Arrives as a server→client *request* before a byte of the call reaches the
/// MCP server (transcripts 17–18; the stub logs prove the ordering). Note what
/// it does **not** carry: the tool's name. That lives on the `mcpToolCall`
/// item started just before, which is why the assembler correlates the two.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct McpElicitation {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    #[serde(rename = "serverName")]
    pub server_name: String,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(rename = "_meta", default)]
    pub meta: Option<McpElicitationMeta>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct McpElicitationMeta {
    /// `"mcp_tool_call"` is the kind this codec acts on; the field exists so
    /// any other kind codex invents is visibly not a tool-call approval.
    #[serde(default)]
    pub codex_approval_kind: Option<String>,
    /// The call's full arguments — Horde sees them before the call runs.
    #[serde(default)]
    pub tool_params: Option<serde_json::Value>,
    #[serde(default)]
    pub tool_description: Option<String>,
}

impl McpElicitation {
    /// Whether this is the MCP tool-call approval, as opposed to some other
    /// elicitation the server routed through the same method.
    #[must_use]
    pub fn is_mcp_tool_call_approval(&self) -> bool {
        self.meta
            .as_ref()
            .and_then(|meta| meta.codex_approval_kind.as_deref())
            == Some("mcp_tool_call")
    }
}

/// The method elicitations arrive on.
pub const MCP_ELICITATION_METHOD: &str = "mcpServer/elicitation/request";

/// The method exec approvals arrive on — the shell/exec interception point.
///
/// Routed to the client *before* the command executes (re-probe transcripts
/// 20–33, codex-cli 0.148.0 and 0.147.0): the exec item starts, parks in
/// `inProgress`, and completes as `declined` or with an exit code only after
/// this request is answered. At 0.147 only the granular approval policy routes
/// it; at 0.148 plain `on-request` does too. At 0.146 and below nothing does.
pub const EXEC_APPROVAL_METHOD: &str = "item/commandExecution/requestApproval";

/// `item/commandExecution/requestApproval` — a shell command awaiting Horde's
/// verdict, as recorded in transcripts 20–33.
///
/// The reply speaks the `decision` vocabulary (never the elicitation
/// `action`): `"accept"`, `"acceptForSession"`, `"decline"`, `"cancel"`, or
/// `{"acceptWithExecpolicyAmendment": {"execpolicy_amendment": [argv…]}}`.
/// The recorded `availableDecisions` list is advisory, not exhaustive —
/// transcript 25's list omits `decline`, yet declining blocked the command.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ExecApprovalRequest {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    /// The commandExecution item this approval gates. Codex numbers its own
    /// items, so this is the tool-call identity downstream.
    #[serde(rename = "itemId", default)]
    pub item_id: Option<String>,
    /// Codex's own words for why it is asking, e.g. "command failed; retry
    /// without sandbox?" — display context, not rule input.
    #[serde(default)]
    pub reason: Option<String>,
    /// The full invocation as codex will run it, shell wrapper included:
    /// `/bin/zsh -lc "printf …"`.
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    /// Codex's parse of the invocation into user-level actions; the inner
    /// command without the shell wrapper lives here.
    #[serde(rename = "commandActions", default)]
    pub command_actions: Vec<CommandAction>,
    /// The execpolicy rule codex proposes to persist on
    /// `acceptWithExecpolicyAmendment`, as an argv (transcript 30 proves the
    /// echo-back shape deserializes and is honored).
    #[serde(rename = "proposedExecpolicyAmendment", default)]
    pub proposed_execpolicy_amendment: Option<Vec<String>>,
}

/// One parsed action inside an [`ExecApprovalRequest`].
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CommandAction {
    /// The action's command text, when codex parsed one (`type: "unknown"`
    /// still carries it — transcript 20).
    #[serde(default)]
    pub command: Option<String>,
}

impl ExecApprovalRequest {
    /// The command deterministic rules should match on: codex's own parse of
    /// the inner command when the invocation reduced to exactly one, else the
    /// raw invocation.
    ///
    /// The raw form is shell-wrapped (`/bin/zsh -lc "git status"`), which no
    /// prefix rule authored as `git ` would ever match; the single parsed
    /// action is codex's statement of what actually runs. Ambiguity — zero or
    /// several actions — falls back to the raw form, because a rule must never
    /// match a fragment of a compound command.
    #[must_use]
    pub fn effective_command(&self) -> Option<&str> {
        match self.command_actions.as_slice() {
            [only] => only.command.as_deref().or(self.command.as_deref()),
            _ => self.command.as_deref(),
        }
    }
}

/// The thread, turn and item a 0.167 server request names — the header every
/// item-scoped request shares.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct RequestScope {
    #[serde(rename = "threadId", default)]
    pub thread_id: Option<String>,
    #[serde(rename = "turnId", default)]
    pub turn_id: Option<String>,
    #[serde(rename = "itemId", default)]
    pub item_id: Option<String>,
}

/// A server request whose params carry a [`RequestScope`].
pub trait Scoped {
    fn scope(&self) -> &RequestScope;
}

impl Scoped for FileChangeApprovalRequest {
    fn scope(&self) -> &RequestScope {
        &self.scope
    }
}

impl Scoped for PermissionsApprovalRequest {
    fn scope(&self) -> &RequestScope {
        &self.scope
    }
}

impl Scoped for UserInputRequest {
    fn scope(&self) -> &RequestScope {
        &self.scope
    }
}

/// The method file-change approvals arrive on (0.167, transcripts 36–37).
///
/// At 0.148 this request was never sent — the thread parked at
/// `waitingOnApproval` instead (transcripts 28, 31). The reply speaks the
/// `decision` vocabulary: `accept`, `acceptForSession`, `decline`, `cancel`.
pub const FILE_CHANGE_APPROVAL_METHOD: &str = "item/fileChange/requestApproval";

/// `item/fileChange/requestApproval`. Names the item, not the patch: the
/// diff rides the `fileChange` item started just before it (transcript 36),
/// which is why the assembler joins the two by item id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct FileChangeApprovalRequest {
    /// Which thread, turn and item the request belongs to.
    #[serde(flatten)]
    pub scope: RequestScope,
    #[serde(default)]
    pub reason: Option<String>,
    /// A root codex asks to be granted write access under for the session.
    #[serde(rename = "grantRoot", default)]
    pub grant_root: Option<String>,
}

/// The method sandbox-widening requests arrive on (0.167, behind the
/// `request_permissions_tool` feature; transcript 39).
pub const PERMISSIONS_APPROVAL_METHOD: &str = "item/permissions/requestApproval";

/// `item/permissions/requestApproval`: the agent asking for more sandbox —
/// network, extra filesystem roots — for the turn or the session. The reply
/// grants a profile (`{"permissions": …, "scope": "turn"|"session"}`); an
/// empty profile grants nothing, which is how a refusal is spelled.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PermissionsApprovalRequest {
    /// Which thread, turn and item the request belongs to.
    #[serde(flatten)]
    pub scope: RequestScope,
    #[serde(default)]
    pub cwd: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    /// `{"network":{"enabled":true},"fileSystem":null}` — kept whole: it is
    /// both what the chain judges and what an allow grants back.
    #[serde(default)]
    pub permissions: serde_json::Value,
}

/// The method agent questions arrive on (0.167, behind the
/// `default_mode_request_user_input` feature outside plan mode; transcript
/// 38).
pub const USER_INPUT_METHOD: &str = "item/tool/requestUserInput";

/// `item/tool/requestUserInput`: questions with options, answered by id.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UserInputRequest {
    /// Which thread, turn and item the request belongs to.
    #[serde(flatten)]
    pub scope: RequestScope,
    #[serde(default)]
    pub questions: Vec<UserInputQuestion>,
}

/// One question inside a [`UserInputRequest`].
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UserInputQuestion {
    pub id: String,
    #[serde(default)]
    pub header: String,
    #[serde(default)]
    pub question: String,
    #[serde(rename = "isOther", default)]
    pub is_other: bool,
    #[serde(rename = "isSecret", default)]
    pub is_secret: bool,
    #[serde(default)]
    pub options: Option<Vec<UserInputOption>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UserInputOption {
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub description: String,
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

/// Codex reports both a per-thread running `total` and a per-call `last`.
///
/// One reading arrives per *model call*, so a turn that calls the model six
/// times sends six (transcript 33): `last` is one call, never the turn. The
/// assembler differences `total` instead, which also makes the reading resume
/// replays (transcript 16) a no-op (see [`crate::turn`]).
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
            "item/started" => parse(params).map_or_else(other, Self::ItemStarted),
            "item/completed" => parse(params).map_or_else(other, Self::ItemCompleted),
            "error" => parse(params).map_or_else(other, Self::Error),
            "serverRequest/resolved" => {
                parse(params).map_or_else(other, Self::ServerRequestResolved)
            }
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

    /// The recorded elicitation shape (transcript 17), byte-for-byte fields:
    /// the arguments ride `_meta.tool_params`, and the *tool name is absent* —
    /// which is why correlation with the started item exists at all.
    #[test]
    fn the_recorded_elicitation_shape_parses_and_identifies_itself() {
        let elicitation: super::McpElicitation = serde_json::from_value(json!({
            "threadId": "t1",
            "turnId": "turn-1",
            "serverName": "spike",
            "mode": "form",
            "_meta": {
                "codex_approval_kind": "mcp_tool_call",
                "persist": ["session", "always"],
                "tool_description": "Echoes the token argument back.",
                "tool_params": {"token": "HORDE_MCP_OK"},
                "tool_params_display": [{"name": "token", "value": "HORDE_MCP_OK", "display_name": "token"}]
            },
            "message": "Allow the spike MCP server to run tool \"horde_probe\"?",
            "requestedSchema": {"type": "object", "properties": {}}
        }))
        .expect("the recorded shape must parse");

        assert!(elicitation.is_mcp_tool_call_approval());
        assert_eq!(elicitation.server_name, "spike");
        assert_eq!(elicitation.turn_id.as_deref(), Some("turn-1"));
        assert_eq!(
            elicitation.meta.and_then(|meta| meta.tool_params),
            Some(json!({"token": "HORDE_MCP_OK"}))
        );
    }

    /// Some other approval kind must not read as a tool-call approval — the
    /// discriminator is the field, not the method.
    #[test]
    fn a_non_tool_call_elicitation_is_not_an_approval() {
        let other: super::McpElicitation = serde_json::from_value(json!({
            "serverName": "spike",
            "_meta": {"codex_approval_kind": "oauth_login"}
        }))
        .expect("parses");
        assert!(!other.is_mcp_tool_call_approval());

        let bare: super::McpElicitation =
            serde_json::from_value(json!({"serverName": "spike"})).expect("parses");
        assert!(!bare.is_mcp_tool_call_approval(), "no _meta is no approval");
    }

    /// The mcpToolCall item as recorded in transcripts 17–18.
    #[test]
    fn an_mcp_tool_call_item_carries_server_tool_and_arguments() {
        let started: Item = serde_json::from_value(json!({
            "type": "mcpToolCall", "id": "exec-45", "server": "spike",
            "tool": "horde_probe", "status": "inProgress",
            "arguments": {"token": "HORDE_MCP_OK"}
        }))
        .expect("parses");
        assert!(started.is_mcp_tool_call());
        assert_eq!(started.tool.as_deref(), Some("horde_probe"));

        let rejected: Item = serde_json::from_value(json!({
            "type": "mcpToolCall", "id": "exec-45", "server": "spike",
            "tool": "horde_probe", "status": "failed",
            "arguments": {"token": "HORDE_MCP_OK"},
            "error": {"message": "user rejected MCP tool call"}
        }))
        .expect("parses");
        assert_eq!(
            rejected.error.map(|error| error.message),
            Some("user rejected MCP tool call".to_owned())
        );
    }

    /// The 0.147.0 `error` notification, and the code the taxonomy keys on.
    #[test]
    fn an_error_notification_parses_with_its_retry_bit_and_code() {
        let classified = Notification::classify(
            "error",
            json!({
                "error": {"message": "401 Unauthorized", "codexErrorInfo": "unauthorized"},
                "threadId": "t1", "turnId": "turn-1", "willRetry": false
            }),
        );
        match classified {
            Notification::Error(body) => {
                assert!(!body.will_retry);
                assert_eq!(body.error.code(), Some("unauthorized"));
                assert_eq!(body.error.message, "401 Unauthorized");
            }
            other => panic!("expected an error notification, got {other:?}"),
        }
    }

    /// The recorded exec-approval shape (transcript 20), field for field: the
    /// raw invocation is shell-wrapped, the parsed action carries the inner
    /// command, and the proposed amendment is an argv.
    #[test]
    fn the_recorded_exec_approval_shape_parses_and_names_its_command() {
        let request: super::ExecApprovalRequest = serde_json::from_value(json!({
            "threadId": "01a01b80-1d2d-7f60-99de-5b47a62a0631",
            "turnId": "01a01b80-1d55-7611-84f5-e65b521eaea9",
            "itemId": "exec-1f1c062e-6400-4e6f-9ba3-b919696784e7",
            "startedAtMs": 1_787_167_782_287_u64,
            "environmentId": "local",
            "reason": "Allow me to create horde-escalation-probe.txt?",
            "command": "/bin/zsh -lc \"printf '%s\\n' ESCALATED > horde-escalation-probe.txt\"",
            "cwd": "/private/tmp/horde-appr-20",
            "commandActions": [
                {"type": "unknown",
                 "command": "printf '%s\n' ESCALATED > horde-escalation-probe.txt"}
            ],
            "proposedExecpolicyAmendment":
                ["/bin/zsh", "-lc", "printf '%s\n' ESCALATED > horde-escalation-probe.txt"],
            "availableDecisions": ["accept",
                {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": []}}, "cancel"]
        }))
        .expect("the recorded shape must parse");

        assert_eq!(
            request.item_id.as_deref(),
            Some("exec-1f1c062e-6400-4e6f-9ba3-b919696784e7")
        );
        assert_eq!(request.cwd.as_deref(), Some("/private/tmp/horde-appr-20"));
        assert_eq!(
            request.effective_command(),
            Some("printf '%s\n' ESCALATED > horde-escalation-probe.txt"),
            "one parsed action means rules match the inner command"
        );
        assert_eq!(
            request
                .proposed_execpolicy_amendment
                .as_deref()
                .map(<[String]>::len),
            Some(3)
        );
    }

    /// The effective command's fallbacks: a compound invocation (several
    /// actions) and an unparsed one (none) both fall back to the raw form —
    /// a rule must never match a fragment of a compound command.
    #[test]
    fn the_effective_command_falls_back_to_the_raw_invocation_on_ambiguity() {
        let parse = |value: serde_json::Value| -> super::ExecApprovalRequest {
            serde_json::from_value(value).expect("parses")
        };

        let compound = parse(json!({
            "command": "/bin/zsh -lc \"cd /x && git push\"",
            "commandActions": [{"command": "cd /x"}, {"command": "git push"}]
        }));
        assert_eq!(
            compound.effective_command(),
            Some("/bin/zsh -lc \"cd /x && git push\"")
        );

        let unparsed = parse(json!({"command": "/bin/zsh -lc true"}));
        assert_eq!(unparsed.effective_command(), Some("/bin/zsh -lc true"));

        let action_without_text = parse(json!({
            "command": "/bin/zsh -lc true",
            "commandActions": [{"type": "unknown"}]
        }));
        assert_eq!(
            action_without_text.effective_command(),
            Some("/bin/zsh -lc true"),
            "a single action with no text still falls back to the raw form"
        );

        let bare = parse(json!({}));
        assert_eq!(bare.effective_command(), None, "nothing to match on");
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
