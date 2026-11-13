//! The control plane's workspace-cleanup verbs and branch claims (ADR-0014).
//!
//! `CollectWorkspaceGarbage`, `RemoveWorkspaceInstance` and
//! `DeleteWorkspaceDefinition{purge}` each resolve their target Workers now —
//! from the Instance cache plus the registry — split them into targeted,
//! offline and unsupported, append one `WorkspaceGcRequested` to the
//! housekeeping stream, and return. The command fold turns that event into
//! one `WorkspaceGcCommand` per targeted or offline Worker, so the fold stays
//! a pure function of the log and an offline Worker acts on reconnect.

use std::ops::ControlFlow;

use horde_proto::v1::{
    AgentId, Event, GcLevel, WorkerId, WorkspaceDefinitionId, WorkspaceGcRequested, event::Kind,
};

use super::shared::{ServerCore, append_server_event, uuid_like};
use crate::error::ServerError;
use crate::state::SessionState;

/// The workspace protocol ADR-0014's Workers advertise.
pub const WORKSPACE_PROTOCOL: u32 = 2;

/// Who a cleanup request reaches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Targets {
    /// Connected and protocol 2: answers will follow.
    pub targeted: Vec<String>,
    /// Not connected: the command waits in their stream.
    pub offline: Vec<String>,
    /// Connected but older: never sent the command.
    pub unsupported: Vec<String>,
}

impl Targets {
    /// Every Worker the request is folded into a command for.
    #[must_use]
    pub fn commanded(&self) -> Vec<String> {
        self.targeted.iter().chain(&self.offline).cloned().collect()
    }
}

/// Splits `workers` by what the registry says of each now. A pure function
/// of the registry's answers, so the split is a truth table.
#[must_use]
pub fn split_targets(workers: Vec<String>, protocol_of: impl Fn(&str) -> Option<u32>) -> Targets {
    let mut targets = Targets::default();
    for worker in workers {
        match protocol_of(&worker) {
            None => targets.offline.push(worker),
            Some(protocol) if protocol >= WORKSPACE_PROTOCOL => targets.targeted.push(worker),
            Some(_) => targets.unsupported.push(worker),
        }
    }
    targets
}

/// The Workers a request names: the ones asked for, or every Worker the
/// cache says holds a matching Instance.
#[must_use]
pub fn workers_for(
    core: &ServerCore,
    asked: &[WorkerId],
    definition_id: Option<&str>,
) -> Vec<String> {
    let mut workers: Vec<String> = if asked.is_empty() {
        core.instances.holders(definition_id)
    } else {
        asked.iter().map(ToString::to_string).collect()
    };
    workers.sort();
    workers.dedup();
    workers
}

/// The split for `workers`, read off the live registry.
#[must_use]
pub fn targets(core: &ServerCore, workers: Vec<String>) -> Targets {
    split_targets(workers, |worker| {
        core.workers
            .capabilities_of(worker)
            .map(|capabilities| capabilities.workspace_protocol)
    })
}

/// What one gc request asks for, before it is appended.
#[derive(Debug, Clone, Default)]
pub struct GcAsk {
    pub definition_id: Option<String>,
    pub agent_ids: Vec<AgentId>,
    pub level: GcLevel,
    pub force: bool,
    pub dry_run: bool,
    pub remove_instance: bool,
    pub include_pins: bool,
    pub requested_by: String,
}

/// Appends one `WorkspaceGcRequested` naming every commanded Worker,
/// returning the request id and its log position.
///
/// # Errors
///
/// The append failing.
pub async fn request_gc(
    core: &ServerCore,
    ask: GcAsk,
    targets: &Targets,
) -> Result<(String, u64), ServerError> {
    let request_id = format!("gc-{}", uuid_like());
    let sequence = append_server_event(
        &core.log,
        horde_proto::WORKSPACE_HOUSEKEEPING_STREAM,
        "",
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspaceGcRequested(WorkspaceGcRequested {
                request_id: request_id.clone(),
                worker_ids: targets.commanded().into_iter().map(WorkerId::new).collect(),
                workspace_definition_id: ask.definition_id.map(WorkspaceDefinitionId::new),
                agent_ids: ask.agent_ids,
                level: ask.level.into(),
                force: ask.force,
                dry_run: ask.dry_run,
                remove_instance: ask.remove_instance,
                include_pins: ask.include_pins,
                requested_by: ask.requested_by,
            })),
        },
    )
    .await?;
    Ok((request_id, sequence))
}

/// The live agent holding `branch` in workspace `definition_id`, as
/// `(agent, session)`: the claim table (ADR-0014), read off the in-memory
/// projection. Live means neither archived nor lost; default names are
/// never claimed, because they cannot collide.
///
/// # Errors
///
/// The projection failing to read.
pub async fn branch_holder(
    core: &ServerCore,
    definition_id: &str,
    branch: &str,
) -> Result<Option<(String, String)>, ServerError> {
    if branch.is_empty() {
        return Ok(None);
    }
    core.sessions
        .walk(
            crate::enrollment::is_session_stream,
            |state| match claimant(state, definition_id, branch) {
                Some(agent) => ControlFlow::Break((agent, state.session_id.clone())),
                None => ControlFlow::Continue(()),
            },
        )
        .await
}

/// The agent of one session holding the claim, if any.
#[must_use]
pub fn claimant(state: &SessionState, definition_id: &str, branch: &str) -> Option<String> {
    if state.lifecycle == horde_proto::v1::SessionLifecycleState::Archived {
        return None;
    }
    state
        .agents
        .values()
        .filter(|agent| agent.lifecycle != horde_proto::v1::AgentLifecycleState::Archived)
        .filter(|agent| agent.placement_state != horde_proto::v1::PlacementState::Lost)
        .find(|agent| {
            agent.spec.as_ref().is_some_and(|spec| {
                spec.branch == branch
                    && spec
                        .workspace_definition_id
                        .as_ref()
                        .is_some_and(|id| id.to_string() == definition_id)
            })
        })
        .map(|agent| agent.agent_id.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn targets_split_by_connection_and_protocol() {
        let targets = split_targets(
            vec!["new".to_owned(), "old".to_owned(), "gone".to_owned()],
            |worker| match worker {
                "new" => Some(2),
                "old" => Some(0),
                _ => None,
            },
        );
        assert_eq!(targets.targeted, vec!["new"]);
        assert_eq!(targets.unsupported, vec!["old"]);
        assert_eq!(targets.offline, vec!["gone"]);
        assert_eq!(
            targets.commanded(),
            vec!["new", "gone"],
            "an older worker is never commanded"
        );
        let newer = split_targets(vec!["v3".to_owned()], |_| Some(3));
        assert_eq!(newer.targeted, vec!["v3"]);
    }

    fn state_with(agent: &str, branch: &str, definition: &str) -> SessionState {
        let mut state = SessionState {
            session_id: "s1".to_owned(),
            ..SessionState::default()
        };
        state.agents.insert(
            agent.to_owned(),
            crate::state::AgentState {
                agent_id: agent.to_owned(),
                parent_agent_id: None,
                lifecycle: horde_proto::v1::AgentLifecycleState::Running,
                tree_path: Vec::new(),
                agent_type: String::new(),
                permission_mode: String::new(),
                worker_id: Some("worker-a".to_owned()),
                spec: Some(horde_proto::v1::AgentSpec {
                    branch: branch.to_owned(),
                    workspace_definition_id: Some(WorkspaceDefinitionId::new(definition)),
                    ..horde_proto::v1::AgentSpec::default()
                }),
                placement_state: horde_proto::v1::PlacementState::Placed,
                queued_at_unix_ms: None,
                rewrites_tool_input: false,
            },
        );
        state
    }

    /// A live agent holds its branch in its workspace only; archive and
    /// loss release it.
    #[test]
    fn a_claim_is_a_live_agents_branch_in_its_own_workspace() {
        let state = state_with("agent-1", "feat/login", "ws-1");
        assert_eq!(
            claimant(&state, "ws-1", "feat/login"),
            Some("agent-1".to_owned())
        );
        assert_eq!(claimant(&state, "ws-2", "feat/login"), None);
        assert_eq!(claimant(&state, "ws-1", "feat/other"), None);

        let mut archived = state.clone();
        if let Some(agent) = archived.agents.get_mut("agent-1") {
            agent.lifecycle = horde_proto::v1::AgentLifecycleState::Archived;
        }
        assert_eq!(claimant(&archived, "ws-1", "feat/login"), None);

        let mut lost = state.clone();
        if let Some(agent) = lost.agents.get_mut("agent-1") {
            agent.placement_state = horde_proto::v1::PlacementState::Lost;
        }
        assert_eq!(claimant(&lost, "ws-1", "feat/login"), None);

        let mut session_archived = state;
        session_archived.lifecycle = horde_proto::v1::SessionLifecycleState::Archived;
        assert_eq!(claimant(&session_archived, "ws-1", "feat/login"), None);
    }
}
