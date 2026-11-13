//! The words wire values render as, shared by every screen and by the CLI.
//!
//! Two frontends, one vocabulary (DESIGN.md §11): the CLI's `sync` and
//! `agent status` say these same words for the same wire values.

use horde_proto::v1::{
    AgentLifecycleState, AgentOutputChannel, PermissionOutcome, PlacementState, ResultStatus,
    SessionLifecycleState,
};

/// How many characters of an id stand in for it when there is no name, once
/// its kind stem is gone: the short-prefix convention git uses.
pub const SHORT_ID_LEN: usize = 8;

/// The kind stems server-minted ids open with (`session-…`, `agent-…`).
/// Every id of a kind shares its stem, so a prefix that keeps it spends
/// most of its characters saying nothing.
const ID_STEMS: [&str; 2] = ["session-", "agent-"];

/// The first [`SHORT_ID_LEN`] characters of an id after its kind stem —
/// `session-18f3a2b4c5d6-0` shows as `18f3a2b4`. Display only: unlike the
/// CLI's resolvable short ids, nothing parses this back.
#[must_use]
pub fn short_id(id: &str) -> String {
    let unstemmed = ID_STEMS
        .iter()
        .find_map(|stem| id.strip_prefix(stem))
        .unwrap_or(id);
    unstemmed.chars().take(SHORT_ID_LEN).collect()
}

pub(crate) fn channel_name(channel: i32) -> &'static str {
    match AgentOutputChannel::try_from(channel) {
        Ok(AgentOutputChannel::Assistant) => "assistant",
        Ok(AgentOutputChannel::Reasoning) => "reasoning",
        Ok(AgentOutputChannel::ToolCall) => "tool call",
        Ok(AgentOutputChannel::ToolResult) => "tool result",
        Ok(AgentOutputChannel::HarnessDiagnostic) => "harness",
        _ => "output",
    }
}

pub(crate) fn result_status_name(status: i32) -> &'static str {
    match ResultStatus::try_from(status) {
        Ok(ResultStatus::Success) => "success",
        Ok(ResultStatus::Partial) => "partial",
        Ok(ResultStatus::Blocked) => "blocked",
        _ => "unspecified",
    }
}

pub(crate) fn permission_outcome_name(outcome: i32) -> &'static str {
    match PermissionOutcome::try_from(outcome) {
        Ok(PermissionOutcome::Allow) => "allowed",
        Ok(PermissionOutcome::Deny) => "denied",
        _ => "unspecified",
    }
}

/// The word an agent lifecycle state renders as — public for the same
/// one-vocabulary reason as [`placement_state_name`]: the CLI's `sync` and
/// `agent status` say these words too.
#[must_use]
pub fn agent_lifecycle_name(state: i32) -> &'static str {
    match AgentLifecycleState::try_from(state) {
        Ok(AgentLifecycleState::Queued) => "queued",
        Ok(AgentLifecycleState::Starting) => "starting",
        Ok(AgentLifecycleState::Running) => "running",
        Ok(AgentLifecycleState::WaitingOnChild) => "waiting on child",
        Ok(AgentLifecycleState::WaitingOnHuman) => "waiting on human",
        Ok(AgentLifecycleState::Idle) => "idle",
        Ok(AgentLifecycleState::Archived) => "archived",
        Ok(AgentLifecycleState::Failed) => "failed",
        _ => "unspecified",
    }
}

pub(crate) fn session_lifecycle_name(state: i32) -> &'static str {
    match SessionLifecycleState::try_from(state) {
        Ok(SessionLifecycleState::Active) => "active",
        Ok(SessionLifecycleState::Archived) => "archived",
        _ => "unspecified",
    }
}

/// The word a placement state renders as.
///
/// Public because the CLI's renderer says the same words for the same wire
/// values — two frontends, one vocabulary (DESIGN.md §11) — and a private
/// copy over there was a duplicate block the gate rightly counted.
#[must_use]
pub fn placement_state_name(state: i32) -> &'static str {
    match PlacementState::try_from(state) {
        Ok(PlacementState::Placed) => "placed",
        Ok(PlacementState::Queued) => "queued",
        Ok(PlacementState::Expired) => "expired",
        Ok(PlacementState::Cancelled) => "cancelled",
        Ok(PlacementState::Lost) => "lost",
        _ => "unspecified",
    }
}
