//! ADR-0016 at the Server, over the wire: each configuration layer decides
//! an agent's permission mode in order — default, file, flag, workspace,
//! session, spawn — the stamped mode and `GetEffectiveConfig` agree on
//! which layer decided, and a configured tier catalog replaces the shipped
//! one at the RPC.

use horde_proto::v1::{
    AgentId, AgentSpec, CreateSessionRequest, CreateWorkspaceDefinitionRequest, Event,
    GetEffectiveConfigRequest, ModelSelection, SessionId, SessionSettings, SpawnAgentRequest,
    UpdateWorkspaceDefinitionRequest, WorkspaceDefinitionId, WorkspaceRepo, event::Kind,
    model_selection::Selection,
};
use horde_server::config::{ServerFlags, ServerSettings};
use horde_server::service::ServerCore;
use prost::Message;

use crate::common::{Harness, Setup, connect_to, core};

/// A server whose config file says `text`, started with `flags`.
fn configured(text: &str, flags: &ServerFlags) -> Setup<(ServerCore, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.toml");
    std::fs::write(&path, text)?;
    let settings = ServerSettings::load(Some(&path), &path, flags, |_| None)?;
    Ok((core()?.with_settings(&settings), dir))
}

const FILE: &str = r#"
[permissions]
default_mode = "ask"
[permissions.modes.careful]
deny = [{ tool = "Bash", reason = "no shell" }]
[permissions.modes.loose]
bypass = true
[tiers.only]
candidates = [{ provider = "claude", model = "opus" }]
"#;

async fn workspace(harness: &mut Harness, name: &str, mode: &str) -> Setup<String> {
    Ok(harness
        .control
        .create_workspace_definition(CreateWorkspaceDefinitionRequest {
            name: name.to_owned(),
            repos: vec![WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: "https://example.com/atlas.git".to_owned(),
                default_branch: "main".to_owned(),
                ..WorkspaceRepo::default()
            }],
            permission_mode: mode.to_owned(),
            ..CreateWorkspaceDefinitionRequest::default()
        })
        .await?
        .into_inner()
        .definition
        .and_then(|definition| definition.workspace_definition_id)
        .map(|id| id.to_string())
        .unwrap_or_default())
}

/// Creates a session, returning its id and its root agent's id.
async fn session(
    harness: &mut Harness,
    key: &str,
    workspace: Option<&str>,
    session_mode: &str,
    spawn_mode: &str,
) -> Setup<(String, String)> {
    let created = harness
        .control
        .create_session(CreateSessionRequest {
            title: key.to_owned(),
            workspace_definition_id: workspace.map(WorkspaceDefinitionId::new),
            root_agent: Some(AgentSpec {
                permission_mode: spawn_mode.to_owned(),
                ..AgentSpec::default()
            }),
            idempotency_key: key.to_owned(),
            settings: Some(SessionSettings {
                permission_mode: session_mode.to_owned(),
            }),
        })
        .await?
        .into_inner();
    let id = created
        .session
        .and_then(|summary| summary.session_id)
        .map(|id| id.to_string())
        .ok_or("a session id")?;
    let root = created
        .root_agent_id
        .map(|id| id.to_string())
        .ok_or("a root agent")?;
    Ok((id, root))
}

/// The permission mode `agent`'s spawn recorded.
async fn stamped(core: &ServerCore, session: &str, agent: &str) -> Setup<String> {
    for stored in core.log.read_all(session).await? {
        let event = Event::decode(stored.payload.as_slice())?;
        if let Some(Kind::AgentSpawned(spawned)) = event.kind
            && event.agent_id.map(|id| id.to_string()).as_deref() == Some(agent)
        {
            return Ok(spawned.spec.unwrap_or_default().permission_mode);
        }
    }
    Err(format!("no spawn of {agent}").into())
}

/// `GetEffectiveConfig`'s `(value, origin)` for `key`.
async fn effective(
    harness: &mut Harness,
    workspace: &str,
    session: Option<&str>,
    key: &str,
) -> Setup<(String, String)> {
    let entries = harness
        .control
        .get_effective_config(GetEffectiveConfigRequest {
            workspace: workspace.to_owned(),
            session_id: session.map(SessionId::new),
        })
        .await?
        .into_inner()
        .entries;
    entries
        .into_iter()
        .find(|entry| entry.key == key)
        .map(|entry| (entry.value, entry.origin))
        .ok_or_else(|| format!("no {key}").into())
}

/// The headline: with every layer present, each one decides exactly when
/// every more specific layer is silent — and the stamped spawn and the
/// effective config name the same layer.
#[tokio::test]
async fn each_layer_decides_an_agents_mode_in_order() -> Setup<()> {
    let (core, dir) = configured(FILE, &ServerFlags::default())?;
    let file_origin = format!("file {}", dir.path().join("config.toml").display());
    let mut harness = connect_to(core.clone()).await?;
    let careful = workspace(&mut harness, "careful-ws", "careful").await?;
    let plain = workspace(&mut harness, "plain-ws", "").await?;

    // file: nothing more specific says anything.
    let (filed, root) = session(&mut harness, "filed", Some(&plain), "", "").await?;
    assert_eq!(stamped(&core, &filed, &root).await?, "ask");
    assert_eq!(
        effective(&mut harness, &plain, Some(&filed), "agent.permission_mode").await?,
        ("\"ask\"".to_owned(), file_origin.clone())
    );

    // workspace over file.
    let (in_workspace, root) = session(&mut harness, "ws", Some(&careful), "", "").await?;
    assert_eq!(stamped(&core, &in_workspace, &root).await?, "careful");
    assert_eq!(
        effective(
            &mut harness,
            "careful-ws",
            Some(&in_workspace),
            "agent.permission_mode"
        )
        .await?,
        ("\"careful\"".to_owned(), "workspace careful-ws".to_owned())
    );

    // session over workspace — for the root and for a client spawn later.
    let (sessioned, root) = session(&mut harness, "sess", Some(&careful), "loose", "").await?;
    assert_eq!(stamped(&core, &sessioned, &root).await?, "loose");
    assert_eq!(
        effective(
            &mut harness,
            &careful,
            Some(&sessioned),
            "agent.permission_mode"
        )
        .await?,
        ("\"loose\"".to_owned(), format!("session {sessioned}"))
    );
    let child = spawn(&mut harness, &sessioned, &careful, "").await?;
    assert_eq!(stamped(&core, &sessioned, &child).await?, "loose");

    // spawn over session.
    let flagged = spawn(&mut harness, &sessioned, &careful, "bypass").await?;
    assert_eq!(stamped(&core, &sessioned, &flagged).await?, "bypass");
    let (rooted, root) =
        session(&mut harness, "rooted", Some(&careful), "loose", "careful").await?;
    assert_eq!(stamped(&core, &rooted, &root).await?, "careful");
    Ok(())
}

async fn spawn(harness: &mut Harness, session: &str, workspace: &str, mode: &str) -> Setup<String> {
    Ok(harness
        .control
        .spawn_agent(SpawnAgentRequest {
            session_id: Some(SessionId::new(session)),
            spec: Some(AgentSpec {
                workspace_definition_id: Some(WorkspaceDefinitionId::new(workspace)),
                permission_mode: mode.to_owned(),
                ..AgentSpec::default()
            }),
            idempotency_key: format!("{session}-{mode}-spawn"),
            ..SpawnAgentRequest::default()
        })
        .await?
        .into_inner()
        .agent_id
        .map(|id: AgentId| id.to_string())
        .ok_or("a spawned agent id")?)
}

/// Below the file: with no file the built-in default (bypass) decides, and
/// `--permission-mode` beats the file.
#[tokio::test]
async fn the_server_flag_beats_the_file_and_the_default_is_bypass() -> Setup<()> {
    let bare = core()?.with_settings(&ServerSettings::resolve(
        None,
        &ServerFlags::default(),
        |_| None,
    )?);
    let mut harness = connect_to(bare.clone()).await?;
    let (id, root) = session(&mut harness, "bare", None, "", "").await?;
    assert_eq!(stamped(&bare, &id, &root).await?, "bypass");
    assert_eq!(
        effective(&mut harness, "", Some(&id), "agent.permission_mode").await?,
        ("\"bypass\"".to_owned(), "default".to_owned())
    );

    let flags = ServerFlags {
        permission_mode: Some("loose".to_owned()),
        ..ServerFlags::default()
    };
    let (flagged, _dir) = configured(FILE, &flags)?;
    let mut harness = connect_to(flagged.clone()).await?;
    let (id, root) = session(&mut harness, "flagged", None, "", "").await?;
    assert_eq!(stamped(&flagged, &id, &root).await?, "loose");
    assert_eq!(
        effective(&mut harness, "", None, "permissions.default_mode").await?,
        ("\"loose\"".to_owned(), "flag --permission-mode".to_owned())
    );
    Ok(())
}

/// A workspace's mode is set at create, replaced by update, kept by an
/// update that names none, and cleared only by the explicit flag.
#[tokio::test]
async fn a_workspace_mode_is_kept_replaced_and_cleared_by_update() -> Setup<()> {
    let (core, _dir) = configured(FILE, &ServerFlags::default())?;
    let mut harness = connect_to(core).await?;
    let id = workspace(&mut harness, "ws", "careful").await?;
    let update = |mode: &str, clear: bool, etag: String| UpdateWorkspaceDefinitionRequest {
        workspace_definition_id: Some(WorkspaceDefinitionId::new(&id)),
        etag,
        permission_mode: mode.to_owned(),
        clear_permission_mode: clear,
        ..UpdateWorkspaceDefinitionRequest::default()
    };
    let current = |harness: &mut Harness| {
        let mut control = harness.control.clone();
        let id = id.clone();
        async move {
            control
                .get_workspace_definition(horde_proto::v1::GetWorkspaceDefinitionRequest {
                    workspace_definition_id: Some(WorkspaceDefinitionId::new(&id)),
                    name: String::new(),
                })
                .await
                .map(|response| response.into_inner().definition.unwrap_or_default())
        }
    };
    let mut modes = Vec::new();
    for (mode, clear) in [("", false), ("loose", false), ("", false), ("", true)] {
        let etag = current(&mut harness).await?.etag;
        let updated = harness
            .control
            .update_workspace_definition(update(mode, clear, etag))
            .await?
            .into_inner()
            .definition
            .unwrap_or_default();
        modes.push(updated.permission_mode);
    }
    assert_eq!(modes, ["careful", "loose", "loose", ""]);
    Ok(())
}

/// A configured `[tiers]` replaces the shipped catalog at the RPC: a
/// shipped tier is refused naming the configured ones, a configured one
/// binds.
#[tokio::test]
async fn a_configured_tier_catalog_replaces_the_shipped_one() -> Setup<()> {
    let (core, _dir) = configured(FILE, &ServerFlags::default())?;
    let mut harness = connect_to(core).await?;
    let tiered = |tier: &str| AgentSpec {
        model: Some(ModelSelection {
            selection: Some(Selection::Tier(tier.to_owned())),
        }),
        ..AgentSpec::default()
    };
    let refused = harness
        .control
        .create_session(CreateSessionRequest {
            title: "t".to_owned(),
            root_agent: Some(tiered("fast")),
            idempotency_key: "fast".to_owned(),
            ..CreateSessionRequest::default()
        })
        .await
        .expect_err("a shipped tier the file replaced");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert!(
        refused.message().contains("\"fast\"") && refused.message().contains("known tiers: only"),
        "{refused:?}"
    );
    harness
        .control
        .create_session(CreateSessionRequest {
            title: "o".to_owned(),
            root_agent: Some(tiered("only")),
            idempotency_key: "only".to_owned(),
            ..CreateSessionRequest::default()
        })
        .await?;
    Ok(())
}

/// The effective config refuses a workspace it cannot find, and without a
/// workspace or session it reports only the server's own keys.
#[tokio::test]
async fn effective_config_reports_the_servers_keys_and_refuses_an_unknown_workspace() -> Setup<()> {
    let (core, _dir) = configured(FILE, &ServerFlags::default())?;
    let mut harness = connect_to(core).await?;
    let refused = harness
        .control
        .get_effective_config(GetEffectiveConfigRequest {
            workspace: "nope".to_owned(),
            session_id: None,
        })
        .await
        .expect_err("an unknown workspace");
    assert_eq!(refused.code(), tonic::Code::NotFound);
    let entries = harness
        .control
        .get_effective_config(GetEffectiveConfigRequest::default())
        .await?
        .into_inner()
        .entries;
    assert!(entries.iter().any(|entry| entry.key == "tiers.only"));
    assert!(entries.iter().all(|entry| !entry.key.starts_with("agent.")));
    Ok(())
}
