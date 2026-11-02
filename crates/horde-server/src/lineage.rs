//! What a child inherits from its parent, and what it may never exceed.
//!
//! An agent that spawns a child through its MCP endpoint says only what the
//! child is for, and perhaps a type or a tier. Everything else is its
//! parent's: the same workspace (whose checkout then starts from the parent's
//! committed HEAD — DESIGN §5, worked out on the worker), the same agent type,
//! the same model selection (DESIGN §6: "unspecified = inherit the spawner's
//! tier"), the same permission mode. The parent's spec read here is the one
//! the log recorded for it — the server's projection, never anything the
//! spawning agent asserted.
//!
//! What the child may *change* is bounded by ADR-0009's attenuation: an
//! agent-originated spawn may not name another workspace, another permission
//! mode, or a type that sees an MCP server its parent cannot. The checkout
//! policy is deliberately not inherited: a child's default is a worktree of
//! its own (DESIGN §7), whatever its parent occupies.

use horde_proto::v1::AgentSpec;

use crate::mcp::McpServerCatalog;

/// `child` with every field it left unset filled from `parent`'s spec.
///
/// Inherited: the workspace, the agent type, the model selection, and the
/// permission mode. Not inherited: the checkout policy, placement
/// constraints, budget, and initial assignment — each belongs to the child.
#[must_use]
pub fn inherited(parent: &AgentSpec, mut child: AgentSpec) -> AgentSpec {
    if child
        .workspace_definition_id
        .as_ref()
        .is_none_or(|id| id.value.is_empty())
    {
        child
            .workspace_definition_id
            .clone_from(&parent.workspace_definition_id);
    }
    if child.agent_type.is_empty() {
        child.agent_type.clone_from(&parent.agent_type);
    }
    if child.model.is_none() {
        child.model.clone_from(&parent.model);
    }
    if child.permission_mode.is_empty() {
        child.permission_mode.clone_from(&parent.permission_mode);
    }
    child
}

/// Why `child` holds more than `parent` does, or `None` when it holds no
/// more (ADR-0009). Asked of an agent-originated spawn after inheritance, so
/// an unset field has already become the parent's own.
#[must_use]
pub fn exceeds(
    parent: &AgentSpec,
    child: &AgentSpec,
    catalog: &McpServerCatalog,
) -> Option<String> {
    let workspace_of = |spec: &AgentSpec| {
        spec.workspace_definition_id
            .as_ref()
            .map(|id| id.value.clone())
            .unwrap_or_default()
    };
    if workspace_of(child) != workspace_of(parent) {
        return Some(format!(
            "a child may not leave its parent's workspace ({:?}) for {:?}",
            workspace_of(parent),
            workspace_of(child)
        ));
    }
    if child.permission_mode != parent.permission_mode {
        return Some(format!(
            "a child may not change its parent's permission mode ({:?}) to {:?}",
            parent.permission_mode, child.permission_mode
        ));
    }
    let local_names = |agent_type: &str| -> Vec<String> {
        catalog
            .local_servers_for(agent_type)
            .into_iter()
            .map(|config| config.name)
            .collect()
    };
    let parent_sees = [
        local_names(&parent.agent_type),
        catalog.visible_hub_servers_for(&parent.agent_type),
    ]
    .concat();
    let wider: Vec<String> = [
        local_names(&child.agent_type),
        catalog.visible_hub_servers_for(&child.agent_type),
    ]
    .concat()
    .into_iter()
    .filter(|name| !parent_sees.contains(name))
    .collect();
    (!wider.is_empty()).then(|| {
        format!(
            "agent type {:?} would see MCP servers its parent's type {:?} cannot: {}",
            child.agent_type,
            parent.agent_type,
            wider.join(", ")
        )
    })
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        Assignment, CheckoutPolicy, McpServerConfig, McpServerConfigChanged, ModelSelection,
        WorkspaceDefinitionId, model_selection::Selection,
    };

    use super::{AgentSpec, McpServerCatalog, exceeds, inherited};

    fn tier(name: &str) -> Option<ModelSelection> {
        Some(ModelSelection {
            selection: Some(Selection::Tier(name.to_owned())),
        })
    }

    fn parent() -> AgentSpec {
        AgentSpec {
            agent_type: "reviewer".to_owned(),
            model: tier("deep"),
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            checkout_policy: CheckoutPolicy::Shared as i32,
            permission_mode: "careful".to_owned(),
            ..AgentSpec::default()
        }
    }

    /// The catalog `exceeds` judges types against: one server for everyone,
    /// one hub server only `reviewer` sees, one local server only `admin`
    /// sees.
    fn catalog() -> McpServerCatalog {
        let config = |name: &str, local: bool, allowed: &[&str]| McpServerConfig {
            name: name.to_owned(),
            local,
            allowed_agent_types: allowed.iter().map(|s| (*s).to_owned()).collect(),
            ..McpServerConfig::default()
        };
        let mut catalog = McpServerCatalog::default();
        for (sequence, config) in [
            config("everyone", false, &[]),
            config("review-hub", false, &["reviewer"]),
            config("admin-local", true, &["admin"]),
        ]
        .into_iter()
        .enumerate()
        {
            catalog.fold_change(
                u64::try_from(sequence).unwrap_or_default(),
                McpServerConfigChanged {
                    config: Some(config),
                    ..McpServerConfigChanged::default()
                },
            );
        }
        catalog
    }

    #[test]
    fn an_empty_child_becomes_its_parents_workspace_type_model_and_mode() {
        let child = inherited(
            &parent(),
            AgentSpec {
                initial_assignment: Some(Assignment {
                    text: "the task".to_owned(),
                    ..Assignment::default()
                }),
                ..AgentSpec::default()
            },
        );
        assert_eq!(
            child,
            AgentSpec {
                agent_type: "reviewer".to_owned(),
                model: tier("deep"),
                workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
                permission_mode: "careful".to_owned(),
                // Its own: a worktree by default, not the parent's shared one.
                checkout_policy: CheckoutPolicy::Unspecified as i32,
                initial_assignment: Some(Assignment {
                    text: "the task".to_owned(),
                    ..Assignment::default()
                }),
                ..AgentSpec::default()
            }
        );
    }

    #[test]
    fn what_the_child_names_is_kept_and_an_empty_workspace_id_is_unset() {
        let named = AgentSpec {
            agent_type: "fixer".to_owned(),
            model: tier("fast"),
            workspace_definition_id: Some(WorkspaceDefinitionId::new("")),
            permission_mode: "loose".to_owned(),
            ..AgentSpec::default()
        };
        let child = inherited(&parent(), named);
        assert_eq!(child.agent_type, "fixer");
        assert_eq!(child.model, tier("fast"));
        assert_eq!(child.permission_mode, "loose");
        assert_eq!(
            child.workspace_definition_id,
            Some(WorkspaceDefinitionId::new("ws-1")),
            "an empty id names no workspace, so the parent's is inherited"
        );

        let elsewhere = AgentSpec {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-2")),
            ..AgentSpec::default()
        };
        assert_eq!(
            inherited(&parent(), elsewhere).workspace_definition_id,
            Some(WorkspaceDefinitionId::new("ws-2"))
        );
    }

    #[test]
    fn an_inherited_child_exceeds_nothing() {
        let child = inherited(&parent(), AgentSpec::default());
        assert_eq!(exceeds(&parent(), &child, &catalog()), None);
    }

    #[test]
    fn another_workspace_or_mode_exceeds_the_parent() {
        let elsewhere = AgentSpec {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-2")),
            ..parent()
        };
        let reason = exceeds(&parent(), &elsewhere, &catalog()).unwrap_or_default();
        assert!(reason.contains("workspace"), "{reason}");

        let unbound = AgentSpec {
            workspace_definition_id: None,
            ..parent()
        };
        assert!(
            exceeds(&parent(), &unbound, &catalog()).is_some(),
            "leaving the workspace for none is leaving it too"
        );

        let looser = AgentSpec {
            permission_mode: "loose".to_owned(),
            ..parent()
        };
        let reason = exceeds(&parent(), &looser, &catalog()).unwrap_or_default();
        assert!(reason.contains("permission mode"), "{reason}");
    }

    #[test]
    fn a_type_that_sees_more_servers_exceeds_and_one_that_sees_fewer_does_not() {
        let admin = AgentSpec {
            agent_type: "admin".to_owned(),
            ..parent()
        };
        let reason = exceeds(&parent(), &admin, &catalog()).unwrap_or_default();
        assert!(reason.contains("admin-local"), "{reason}");
        assert!(
            !reason.contains("everyone"),
            "a server both see is no widening: {reason}"
        );

        let untyped = AgentSpec {
            agent_type: String::new(),
            ..parent()
        };
        assert_eq!(
            exceeds(&parent(), &untyped, &catalog()),
            None,
            "an untyped child sees only the unrestricted servers"
        );

        let reviewer_from_untyped = exceeds(&untyped, &parent(), &catalog()).unwrap_or_default();
        assert!(
            reviewer_from_untyped.contains("review-hub"),
            "hub visibility counts too: {reviewer_from_untyped}"
        );
    }
}
