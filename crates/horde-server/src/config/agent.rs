//! The per-agent keys: which layer decides them for an agent a client
//! creates (ADR-0016).
//!
//! Most specific first: the spawn's own flag, the session's setting, the
//! Workspace Definition's, then the server's (its `--permission-mode` or
//! file, already folded into the registry's default). An agent-originated
//! spawn never reaches here with an empty mode — it inherited its parent's
//! (ADR-0009, `lineage.rs`) — so these layers can never widen a child past
//! its parent.

use horde_config::values::{show_duration, show_string};
use horde_config::{Entry, Origin, Provenance};
use horde_proto::v1::WorkspaceDefinition;

use crate::permission::modes::PermissionModes;
use crate::workspace_policy::WorkspacePolicyConfig;

/// The origin a spawn's own `--permission-mode` is reported under.
pub const SPAWN_FLAG: &str = "spawn --permission-mode";

/// What the session layer holds for one session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionLayer<'a> {
    pub id: &'a str,
    pub permission_mode: &'a str,
}

/// The mode an agent a client creates runs under, and the layer that chose
/// it. `server` is the origin of `permissions.default_mode`.
#[must_use]
pub fn agent_mode(
    spawn: &str,
    session: Option<SessionLayer<'_>>,
    workspace: Option<&WorkspaceDefinition>,
    modes: &PermissionModes,
    server: &Origin,
) -> (String, Origin) {
    if !spawn.is_empty() {
        return (spawn.to_owned(), Origin::Flag(SPAWN_FLAG.to_owned()));
    }
    if let Some(session) = session.filter(|session| !session.permission_mode.is_empty()) {
        return (
            session.permission_mode.to_owned(),
            Origin::Session(session.id.to_owned()),
        );
    }
    if let Some(workspace) = workspace.filter(|workspace| !workspace.permission_mode.is_empty()) {
        return (
            workspace.permission_mode.clone(),
            Origin::Workspace(workspace.name.clone()),
        );
    }
    (modes.default_name().to_owned(), server.clone())
}

/// The per-agent entries `GetEffectiveConfig` adds for an agent a client
/// would create in `workspace` and `session`: its mode, and the two cleanup
/// keys a Definition may override.
#[must_use]
pub fn agent_entries(
    server: &Provenance,
    modes: &PermissionModes,
    policy: &WorkspacePolicyConfig,
    session: Option<SessionLayer<'_>>,
    workspace: Option<&WorkspaceDefinition>,
) -> Vec<Entry> {
    let origin_of = |key: &str| {
        server
            .get(key)
            .map_or(Origin::Default, |entry| entry.origin.clone())
    };
    let (mode, mode_origin) = agent_mode(
        "",
        session,
        workspace,
        modes,
        &origin_of("permissions.default_mode"),
    );
    let mut entries = Provenance::default();
    entries.record("agent.permission_mode", show_string(&mode), mode_origin);
    let cleanup = workspace.and_then(|workspace| workspace.cleanup.as_ref());
    for (key, server_value, overridden) in [
        (
            "workspace.idle_reclaim_after",
            policy.idle_reclaim_after,
            cleanup.and_then(|cleanup| cleanup.idle_reclaim_after),
        ),
        (
            "workspace.archived_grace",
            policy.archived_grace,
            cleanup.and_then(|cleanup| cleanup.archived_grace),
        ),
    ] {
        let (value, origin) = match (overridden, workspace) {
            (Some(wire), Some(workspace)) => (
                std::time::Duration::from_secs(u64::try_from(wire.seconds).unwrap_or(0)),
                Origin::Workspace(workspace.name.clone()),
            ),
            _ => (server_value, origin_of(key)),
        };
        entries.record(format!("agent.{key}"), show_duration(&value), origin);
    }
    entries.entries().to_vec()
}

#[cfg(test)]
mod tests {
    use horde_config::{Origin, Provenance};
    use horde_proto::v1::{WorkspaceCleanupPolicy, WorkspaceDefinition};

    use super::{SPAWN_FLAG, SessionLayer, agent_entries, agent_mode};
    use crate::permission::modes::PermissionModes;
    use crate::workspace_policy::{WorkspacePolicyConfig, wire};

    fn workspace(mode: &str) -> WorkspaceDefinition {
        WorkspaceDefinition {
            name: "api".to_owned(),
            permission_mode: mode.to_owned(),
            ..WorkspaceDefinition::default()
        }
    }

    fn session(mode: &str) -> SessionLayer<'_> {
        SessionLayer {
            id: "s-1",
            permission_mode: mode,
        }
    }

    /// The truth table: each layer wins exactly when every more specific
    /// one is empty.
    #[test]
    fn the_most_specific_layer_with_a_mode_decides() {
        let modes = PermissionModes::shipped();
        let server = Origin::File("/s.toml".into());
        let api = workspace("ws-mode");
        let unset = workspace("");
        let cases = [
            (
                "spawn",
                Some(session("s")),
                Some(&api),
                "spawn",
                Origin::Flag(SPAWN_FLAG.to_owned()),
            ),
            (
                "",
                Some(session("s")),
                Some(&api),
                "s",
                Origin::Session("s-1".to_owned()),
            ),
            (
                "",
                Some(session("")),
                Some(&api),
                "ws-mode",
                Origin::Workspace("api".to_owned()),
            ),
            (
                "",
                None,
                Some(&api),
                "ws-mode",
                Origin::Workspace("api".to_owned()),
            ),
            (
                "",
                Some(session("")),
                Some(&unset),
                "bypass",
                server.clone(),
            ),
            ("", None, None, "bypass", server.clone()),
        ];
        for (spawn, session, workspace, wanted, origin) in cases {
            assert_eq!(
                agent_mode(spawn, session, workspace, &modes, &server),
                (wanted.to_owned(), origin),
                "{spawn:?} {session:?} {workspace:?}"
            );
        }
    }

    #[test]
    fn effective_entries_report_the_agents_mode_and_cleanup_with_their_layers() {
        let modes = PermissionModes::shipped();
        let policy = WorkspacePolicyConfig::default();
        let mut server = Provenance::default();
        server.record(
            "permissions.default_mode",
            "\"bypass\"",
            Origin::Flag("--permission-mode".to_owned()),
        );
        server.record(
            "workspace.archived_grace",
            "1d",
            Origin::File("/s.toml".into()),
        );

        let none = agent_entries(&server, &modes, &policy, None, None);
        let shown: Vec<(&str, &str, String)> = none
            .iter()
            .map(|entry| {
                (
                    entry.key.as_str(),
                    entry.value.as_str(),
                    entry.origin.to_string(),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                (
                    "agent.permission_mode",
                    "\"bypass\"",
                    "flag --permission-mode".to_owned()
                ),
                (
                    "agent.workspace.archived_grace",
                    "1d",
                    "file /s.toml".to_owned()
                ),
                (
                    "agent.workspace.idle_reclaim_after",
                    "4h",
                    "default".to_owned()
                ),
            ]
        );

        let mut api = workspace("ask");
        api.cleanup = Some(WorkspaceCleanupPolicy {
            idle_reclaim_after: Some(wire(std::time::Duration::from_secs(60))),
            archived_grace: None,
        });
        let entries = agent_entries(&server, &modes, &policy, Some(session("")), Some(&api));
        let shown: Vec<(&str, &str, String)> = entries
            .iter()
            .map(|entry| {
                (
                    entry.key.as_str(),
                    entry.value.as_str(),
                    entry.origin.to_string(),
                )
            })
            .collect();
        assert_eq!(
            shown,
            [
                (
                    "agent.permission_mode",
                    "\"ask\"",
                    "workspace api".to_owned()
                ),
                (
                    "agent.workspace.archived_grace",
                    "1d",
                    "file /s.toml".to_owned()
                ),
                (
                    "agent.workspace.idle_reclaim_after",
                    "1m",
                    "workspace api".to_owned()
                ),
            ]
        );
        let sessioned = agent_entries(
            &server,
            &modes,
            &policy,
            Some(session("bypass")),
            Some(&api),
        );
        assert_eq!(sessioned[0].origin, Origin::Session("s-1".to_owned()));
    }
}
