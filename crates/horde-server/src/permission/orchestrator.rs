//! Which calls are an agent driving its own subtree (ADR-0009).
//!
//! An agent's orchestrator verbs — `spawn_agent`, `report_result`,
//! `send_message`, `wait_for_agent`, and ADR-0014's `workspace_checkout`,
//! which only adds a worktree inside the caller's own root — are how it does
//! the job Horde exists
//! for. Escalating every one of them to a human, as the escalate-everything
//! default did, turned "agents orchestrating agents" into "a human approving
//! each delegation". The shipped default mode allows them, scoped by the
//! ancestry the log records: spawning and reporting are always the caller's
//! own business (the server makes the caller the parent and the reporter,
//! whatever the arguments say); a message may go to the caller's descendants
//! or its parent; a wait may watch only a descendant. Everything else — the
//! forward verbs included, which have their own chain treatment — is not an
//! orchestrator verb and is left to the rest of the chain.

use horde_proto::prost_types::value::Kind as ValueKind;
use horde_proto::v1::ProposedToolCall;

use crate::state::SessionState;

/// The orchestrator verb a tool name spells, if it spells one — under
/// either harness's spelling, which `horde_mcp::endpoint::horde_tool` owns
/// (ADR-0004, 2026-10-31 amendment).
#[must_use]
pub fn orchestrator_verb(tool_name: &str) -> Option<&str> {
    let verb = horde_mcp::endpoint::horde_tool(tool_name)?;
    matches!(
        verb,
        "spawn_agent" | "report_result" | "send_message" | "wait_for_agent" | "workspace_checkout"
    )
    .then_some(verb)
}

/// Whether `call`, made by `caller`, is one of the caller's own orchestrator
/// verbs within its own subtree.
#[must_use]
pub fn own_orchestrator_verb(call: &ProposedToolCall, caller: &str, state: &SessionState) -> bool {
    match orchestrator_verb(&call.tool_name) {
        Some("spawn_agent" | "report_result" | "workspace_checkout") => true,
        Some("send_message") => string_argument(call, "to_agent_id").is_some_and(|target| {
            is_descendant(state, caller, target) || is_parent(state, caller, target)
        }),
        Some("wait_for_agent") => string_argument(call, "agent_id")
            .is_some_and(|target| is_descendant(state, caller, target)),
        _ => false,
    }
}

/// One string argument of a proposed call.
fn string_argument<'a>(call: &'a ProposedToolCall, field: &str) -> Option<&'a str> {
    match call.input.as_ref()?.fields.get(field)?.kind.as_ref()? {
        ValueKind::StringValue(text) => Some(text.as_str()),
        _ => None,
    }
}

/// Whether `agent` sits strictly below `ancestor` in the session's tree.
///
/// Walks `agent`'s parent chain; bounded by the number of agents, so a
/// malformed history with a parent cycle cannot spin.
fn is_descendant(state: &SessionState, ancestor: &str, agent: &str) -> bool {
    let mut current = state
        .agents
        .get(agent)
        .and_then(|agent| agent.parent_agent_id.as_deref());
    for _ in 0..state.agents.len() {
        match current {
            Some(parent) if parent == ancestor => return true,
            Some(parent) => {
                current = state
                    .agents
                    .get(parent)
                    .and_then(|agent| agent.parent_agent_id.as_deref());
            }
            None => return false,
        }
    }
    false
}

/// Whether `target` is `caller`'s direct parent.
fn is_parent(state: &SessionState, caller: &str, target: &str) -> bool {
    state
        .agents
        .get(caller)
        .and_then(|agent| agent.parent_agent_id.as_deref())
        == Some(target)
}

#[cfg(test)]
mod tests {
    use horde_proto::prost_types::{Struct, Value, value::Kind as ValueKind};
    use horde_proto::v1::ProposedToolCall;

    use super::{orchestrator_verb, own_orchestrator_verb};
    use crate::state::SessionState;
    use crate::state::fixture::{projected, spawned};

    /// root → child → grandchild, plus a sibling under root, projected
    /// through the production fold.
    fn tree() -> SessionState {
        projected(&[
            spawned("root", None),
            spawned("child", Some("root")),
            spawned("grandchild", Some("child")),
            spawned("sibling", Some("root")),
        ])
    }

    /// A malformed history whose parents loop must answer, not spin: the
    /// walk is bounded by the number of agents.
    #[test]
    fn a_parent_cycle_is_walked_to_a_bounded_no() {
        let state = projected(&[spawned("a", Some("b")), spawned("b", Some("a"))]);
        assert!(!own_orchestrator_verb(
            &naming("horde/wait_for_agent", "agent_id", "a"),
            "c",
            &state
        ));
    }

    fn call(tool: &str, field: &str, value: Option<ValueKind>) -> ProposedToolCall {
        let mut input = Struct::default();
        if let Some(kind) = value {
            input
                .fields
                .insert(field.to_owned(), Value { kind: Some(kind) });
        }
        ProposedToolCall {
            tool_name: tool.to_owned(),
            input: Some(input),
            rendered: String::new(),
        }
    }

    fn naming(tool: &str, field: &str, target: &str) -> ProposedToolCall {
        call(tool, field, Some(ValueKind::StringValue(target.to_owned())))
    }

    #[test]
    fn both_harness_spellings_name_the_five_verbs_and_nothing_else() {
        for verb in [
            "spawn_agent",
            "report_result",
            "send_message",
            "wait_for_agent",
            "workspace_checkout",
        ] {
            assert_eq!(orchestrator_verb(&format!("horde/{verb}")), Some(verb));
            assert_eq!(
                orchestrator_verb(&format!("mcp__horde__{verb}")),
                Some(verb)
            );
        }
        for tool in [
            "horde/expose",
            "horde/inject",
            "mcp__horde__approve",
            "spawn_agent",
            "other/spawn_agent",
            "mcp__other__report_result",
            "Bash",
        ] {
            assert_eq!(orchestrator_verb(tool), None, "{tool}");
        }
    }

    #[test]
    fn spawning_and_reporting_are_always_the_callers_own() {
        let state = tree();
        assert!(own_orchestrator_verb(
            &call("horde/spawn_agent", "task", None),
            "grandchild",
            &state
        ));
        assert!(own_orchestrator_verb(
            &call("mcp__horde__workspace_checkout", "repo", None),
            "grandchild",
            &state
        ));
        assert!(own_orchestrator_verb(
            &call("mcp__horde__report_result", "status", None),
            "child",
            &state
        ));
    }

    /// Messages go down the caller's subtree or up to its parent; never
    /// sideways, never past the parent, never to itself or nobody.
    #[test]
    fn a_message_reaches_descendants_and_the_parent_only() {
        let state = tree();
        let send = |target: &str| naming("horde/send_message", "to_agent_id", target);
        assert!(own_orchestrator_verb(&send("child"), "root", &state));
        assert!(own_orchestrator_verb(&send("grandchild"), "root", &state));
        assert!(
            own_orchestrator_verb(&send("root"), "child", &state),
            "up to the parent"
        );
        assert!(
            !own_orchestrator_verb(&send("root"), "grandchild", &state),
            "past the parent"
        );
        assert!(
            !own_orchestrator_verb(&send("sibling"), "child", &state),
            "sideways"
        );
        assert!(
            !own_orchestrator_verb(&send("child"), "child", &state),
            "itself"
        );
        assert!(!own_orchestrator_verb(&send("ghost"), "root", &state));
        assert!(!own_orchestrator_verb(
            &call("horde/send_message", "to_agent_id", None),
            "root",
            &state
        ));
        assert!(!own_orchestrator_verb(
            &call(
                "horde/send_message",
                "to_agent_id",
                Some(ValueKind::NumberValue(1.0))
            ),
            "root",
            &state
        ));
    }

    #[test]
    fn a_wait_watches_descendants_only() {
        let state = tree();
        let wait = |target: &str| naming("mcp__horde__wait_for_agent", "agent_id", target);
        assert!(own_orchestrator_verb(&wait("grandchild"), "root", &state));
        assert!(own_orchestrator_verb(&wait("grandchild"), "child", &state));
        assert!(
            !own_orchestrator_verb(&wait("root"), "child", &state),
            "not the parent"
        );
        assert!(!own_orchestrator_verb(&wait("sibling"), "child", &state));
    }

    #[test]
    fn other_tools_are_not_orchestrator_verbs() {
        let state = tree();
        assert!(!own_orchestrator_verb(
            &naming("Bash", "command", "ls"),
            "root",
            &state
        ));
        assert!(!own_orchestrator_verb(
            &naming("horde/expose", "name", "vite"),
            "root",
            &state
        ));
        let no_input = ProposedToolCall {
            tool_name: "horde/wait_for_agent".to_owned(),
            input: None,
            rendered: String::new(),
        };
        assert!(!own_orchestrator_verb(&no_input, "root", &state));
    }
}
