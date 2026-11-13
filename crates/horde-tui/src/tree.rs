//! The agent tree's nodes and the per-agent facts events add to them.

use horde_proto::v1::{
    AgentLifecycleState, AgentOutputChannel, Event, PlacementState, event::Kind,
};

use crate::words::agent_lifecycle_name;

/// One agent's place in the tree, learned from its `AgentSpawned` event,
/// and the facts later events add: lifecycle, placement, binding, and
/// whether a turn is in flight.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentNode {
    /// Unset for the session's top-level agent.
    pub parent: Option<String>,
    /// Ancestry display names, root first, as the spawn event carried them.
    pub tree_path: Vec<String>,
    /// The last `AgentLifecycleState`, as its wire value (0 until told).
    pub lifecycle: i32,
    /// The last `PlacementState`, as its wire value (0 until told).
    pub placement: i32,
    /// The worker it was placed on, once placed.
    pub worker_id: Option<String>,
    /// The harness (provider adapter key) bound, e.g. `claude`.
    pub harness: String,
    /// The model bound — or, before any binding is journaled, what the
    /// spawn asked for (`tier fast`, a concrete model).
    pub model: String,
    /// The provider account bound, when journaled.
    pub account: String,
    /// Whether a turn is in flight: output seen, no turn-end yet.
    pub working: bool,
}

impl AgentNode {
    /// The state word a human reads: `LOST` outranks the lifecycle, because
    /// a lost agent's last lifecycle is a claim nobody can still vouch for.
    #[must_use]
    pub fn state_word(&self) -> &'static str {
        if self.placement == PlacementState::Lost as i32 {
            return "LOST";
        }
        if self.working {
            return "working";
        }
        if self.lifecycle == 0 && self.placement == PlacementState::Queued as i32 {
            return "queued";
        }
        agent_lifecycle_name(self.lifecycle)
    }
}

/// The per-agent facts later events add to a spawned agent's node. An event
/// about an agent the replay never saw spawn changes nothing: a node
/// invented here would claim a place in a tree nobody recorded.
pub(crate) fn note_facts(
    agents: &mut std::collections::BTreeMap<String, AgentNode>,
    event: &Event,
) {
    let Some(node) = event
        .agent_id
        .as_ref()
        .and_then(|id| agents.get_mut(&id.to_string()))
    else {
        return;
    };
    match event.kind.as_ref() {
        Some(Kind::AgentOutputChunk(chunk))
            if chunk.channel != AgentOutputChannel::HarnessDiagnostic as i32 =>
        {
            node.working = true;
        }
        Some(Kind::AgentLifecycleChanged(changed)) => {
            node.lifecycle = changed.state;
            node.working &= !ends_work(changed.state);
        }
        Some(Kind::PlacementChanged(changed)) => note_placement(node, changed.placement.as_ref()),
        Some(Kind::UsageRecorded(recorded)) => note_usage(node, recorded.record.as_ref()),
        Some(Kind::TurnInterrupted(_)) => node.working = false,
        _ => {}
    }
}

/// What a human calls an agent: the last element of its spawn-time tree
/// path (its own display name), or a short id for an agent the replay never
/// saw spawn. Empty for session-scoped events, which have no agent.
#[must_use]
pub fn agent_name(
    agents: &std::collections::BTreeMap<String, AgentNode>,
    agent_id: &str,
) -> String {
    agents
        .get(agent_id)
        .and_then(|node| node.tree_path.last())
        .filter(|name| !name.is_empty())
        .cloned()
        .unwrap_or_else(|| crate::words::short_id(agent_id))
}

/// Whether entering this lifecycle state means no turn is in flight.
fn ends_work(state: i32) -> bool {
    matches!(
        AgentLifecycleState::try_from(state),
        Ok(AgentLifecycleState::Idle
            | AgentLifecycleState::Failed
            | AgentLifecycleState::Archived
            | AgentLifecycleState::WaitingOnHuman)
    )
}

fn note_placement(node: &mut AgentNode, placement: Option<&horde_proto::v1::Placement>) {
    let Some(placement) = placement else {
        return;
    };
    node.placement = placement.state;
    if let Some(worker) = placement.worker_id.as_ref() {
        node.worker_id = Some(worker.to_string());
    }
    if placement.state == PlacementState::Lost as i32 {
        node.working = false;
    }
}

/// A usage record carries the binding; one with a turn id is a turn's end.
fn note_usage(node: &mut AgentNode, record: Option<&horde_proto::v1::UsageRecord>) {
    let Some(record) = record else {
        return;
    };
    if let Some(binding) = record.binding.as_ref() {
        replace_if_set(&mut node.harness, &binding.provider);
        replace_if_set(&mut node.model, &binding.model);
        replace_if_set(&mut node.account, &binding.provider_account);
    }
    if !record.turn_id.is_empty() {
        node.working = false;
    }
}

fn replace_if_set(slot: &mut String, value: &str) {
    if !value.is_empty() {
        value.clone_into(slot);
    }
}

/// What a spawn asked for — `(harness, model)` — before any binding says
/// what it got. A tier names no harness.
pub(crate) fn requested_model(spec: Option<&horde_proto::v1::AgentSpec>) -> (String, String) {
    use horde_proto::v1::model_selection::Selection;
    match spec
        .and_then(|spec| spec.model.as_ref())
        .and_then(|model| model.selection.as_ref())
    {
        Some(Selection::Tier(tier)) => (String::new(), format!("tier {tier}")),
        Some(Selection::Concrete(concrete)) => (concrete.provider.clone(), concrete.model.clone()),
        None => (String::new(), String::new()),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentId, AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentSpawned,
        AgentSpec, ConcreteModel, ModelBinding, ModelSelection, Placement, PlacementChanged,
        PlacementState, TurnInterrupted, UsageRecord, UsageRecorded, WorkerId, event::Kind,
        model_selection::Selection,
    };

    use crate::fixtures::{chunk, envelope, spawned};
    use crate::state::SessionState;

    fn node(state: &SessionState) -> super::AgentNode {
        state.agents.get("a").cloned().unwrap_or_default()
    }

    fn lifecycle(state: AgentLifecycleState) -> Kind {
        Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: 0,
            state: state as i32,
            reason: String::new(),
        })
    }

    fn placed(state: PlacementState, worker: Option<&str>) -> Kind {
        let placement = Placement {
            worker_id: worker.map(WorkerId::new),
            state: state as i32,
            ..Placement::default()
        };
        Kind::PlacementChanged(PlacementChanged {
            placement: Some(placement),
            adapter_capabilities: Vec::new(),
        })
    }

    fn usage(turn: &str, provider: &str, model: &str, account: &str) -> Kind {
        Kind::UsageRecorded(UsageRecorded {
            record: Some(UsageRecord {
                binding: Some(ModelBinding {
                    tier: String::new(),
                    provider: provider.to_owned(),
                    model: model.to_owned(),
                    provider_account: account.to_owned(),
                }),
                turn_id: turn.to_owned(),
                ..UsageRecord::default()
            }),
        })
    }

    fn spawned_asking(selection: Option<Selection>) -> Kind {
        Kind::AgentSpawned(AgentSpawned {
            spec: Some(AgentSpec {
                model: Some(ModelSelection { selection }),
                ..AgentSpec::default()
            }),
            tree_path: vec!["a".to_owned()],
            ..AgentSpawned::default()
        })
    }

    /// Agent `a` spawned, then one chunk of output on `channel` in turn `t1`.
    fn after_output(channel: AgentOutputChannel) -> SessionState {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(1, "a", spawned(None, &["a"])));
        state.apply(&envelope(2, "a", chunk(channel, "t1", "hi")));
        state
    }

    #[test]
    fn output_starts_work_and_every_turn_end_stops_it() {
        let stops = [
            usage("t1", "", "", ""),
            Kind::TurnInterrupted(TurnInterrupted::default()),
            lifecycle(AgentLifecycleState::Idle),
            lifecycle(AgentLifecycleState::Failed),
            lifecycle(AgentLifecycleState::Archived),
            lifecycle(AgentLifecycleState::WaitingOnHuman),
            placed(PlacementState::Lost, None),
        ];
        for stop in stops {
            let mut state = after_output(AgentOutputChannel::Assistant);
            assert!(node(&state).working, "output means a turn is in flight");
            state.apply(&envelope(3, "a", stop.clone()));
            assert!(!node(&state).working, "{stop:?} ends the turn");
        }
    }

    #[test]
    fn diagnostics_unbound_usage_and_running_do_not_end_work() {
        let mut state = after_output(AgentOutputChannel::HarnessDiagnostic);
        assert!(
            !node(&state).working,
            "the harness talking to itself is not a turn"
        );
        state.apply(&envelope(
            3,
            "a",
            chunk(AgentOutputChannel::ToolCall, "t", "Bash"),
        ));
        for (sequence, kind) in [
            (4, usage("", "", "", "")),
            (5, lifecycle(AgentLifecycleState::Running)),
            (6, placed(PlacementState::Placed, None)),
            (7, Kind::UsageRecorded(UsageRecorded::default())),
            (8, Kind::PlacementChanged(PlacementChanged::default())),
        ] {
            state.apply(&envelope(sequence, "a", kind));
        }
        assert!(node(&state).working);
        assert_eq!(node(&state).lifecycle, AgentLifecycleState::Running as i32);
    }

    #[test]
    fn placement_and_usage_fill_in_worker_and_binding() {
        let mut state = SessionState::new("s1");
        state.apply(&envelope(
            1,
            "a",
            spawned_asking(Some(Selection::Tier("fast".to_owned()))),
        ));
        assert_eq!(
            (node(&state).harness.as_str(), node(&state).model.as_str()),
            ("", "tier fast")
        );
        state.apply(&envelope(
            2,
            "a",
            placed(PlacementState::Placed, Some("w1")),
        ));
        state.apply(&envelope(3, "a", placed(PlacementState::Queued, None)));
        assert_eq!(
            node(&state).worker_id.as_deref(),
            Some("w1"),
            "an unset worker keeps the last"
        );
        assert_eq!(node(&state).placement, PlacementState::Queued as i32);
        state.apply(&envelope(4, "a", usage("t", "claude", "opus", "work")));
        state.apply(&envelope(5, "a", usage("t", "", "", "")));
        let bound = node(&state);
        assert_eq!(
            (
                bound.harness.as_str(),
                bound.model.as_str(),
                bound.account.as_str()
            ),
            ("claude", "opus", "work"),
            "an empty binding field keeps what was known"
        );
    }

    #[test]
    fn a_concrete_spawn_names_its_harness_and_an_unknown_agent_is_untouched() {
        let mut state = SessionState::new("s1");
        let concrete = Selection::Concrete(ConcreteModel {
            provider: "codex".to_owned(),
            model: "gpt-9".to_owned(),
            provider_account: String::new(),
        });
        state.apply(&envelope(1, "a", spawned_asking(Some(concrete))));
        assert_eq!(
            (node(&state).harness.as_str(), node(&state).model.as_str()),
            ("codex", "gpt-9")
        );
        state.apply(&envelope(2, "a", spawned_asking(None)));
        assert_eq!(node(&state).model, "", "no selection asks for nothing");

        state.apply(&envelope(
            3,
            "ghost",
            chunk(AgentOutputChannel::Assistant, "t", "boo"),
        ));
        assert!(
            !state.agents.contains_key("ghost"),
            "facts never invent a node"
        );
        let mut orphan = envelope(4, "", lifecycle(AgentLifecycleState::Idle));
        orphan
            .event
            .iter_mut()
            .for_each(|event| event.agent_id = Some(AgentId::new("nobody")));
        state.apply(&orphan);
        assert_eq!(state.agents.len(), 1);
    }

    #[test]
    fn the_state_word_ranks_lost_over_working_over_lifecycle() {
        let word = |lifecycle: i32, placement: i32, working: bool| {
            super::AgentNode {
                lifecycle,
                placement,
                working,
                ..super::AgentNode::default()
            }
            .state_word()
        };
        let lost = PlacementState::Lost as i32;
        let queued = PlacementState::Queued as i32;
        let idle = AgentLifecycleState::Idle as i32;
        assert_eq!(word(idle, lost, true), "LOST");
        assert_eq!(word(idle, 0, true), "working");
        assert_eq!(word(0, queued, false), "queued");
        assert_eq!(
            word(idle, queued, false),
            "idle",
            "a lifecycle outranks the queue"
        );
        assert_eq!(word(idle, 0, false), "idle");
        assert_eq!(word(0, 0, false), "unspecified");
    }
}
