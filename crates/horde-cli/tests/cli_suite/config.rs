//! ADR-0016 from the client: `horde config show` prints this client's file
//! with origins without any server, `--server` with a workspace and a
//! session prints the server's per-agent resolution, the client file's
//! session default reaches a new session, `--permission-mode` on `new`
//! beats it, and a broken file refuses by path. The binary runs for real
//! where stdout is the subject; `cli::run` in-process elsewhere.

use horde_cli::Client;
use horde_proto::v1::{CreateWorkspaceDefinitionRequest, WorkspaceRepo};
use horde_server::config::{ServerFlags, ServerSettings};
use horde_server::service::ServerCore;

use crate::common::{Setup, invoke, serve};

const HORDE: &str = env!("CARGO_BIN_EXE_horde");

/// Runs the `horde` binary with `HOME` pointed at `home`.
async fn horde(home: &std::path::Path, arguments: &[&str]) -> Setup<std::process::Output> {
    horde_server_testkit::otlp_scrubbed_run(
        HORDE,
        arguments,
        &[("HOME", &home.to_string_lossy()), ("HORDE_LOG", "")],
    )
    .await
}

/// A server running with the settings `text` configures.
async fn configured_server(text: &str) -> Setup<(String, tempfile::TempDir)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.toml");
    std::fs::write(&path, text)?;
    let document = horde_config::load(horde_config::Role::Server, Some(&path), &path)?;
    let settings = ServerSettings::resolve(document.as_ref(), &ServerFlags::default(), |_| None)?;
    let core = ServerCore::new(horde_server::EventLog::in_memory()?).with_settings(&settings);
    Ok((horde_server_testkit::serve_core(core).await?, dir))
}

#[tokio::test]
async fn config_show_prints_the_home_file_with_origins_and_needs_no_server() -> Setup<()> {
    let home = tempfile::tempdir()?;
    let dir = home.path().join(".horde/client");
    std::fs::create_dir_all(&dir)?;
    let file = dir.join("config.toml");
    std::fs::write(&file, "[session]\npermission_mode = \"ask\"\n")?;

    let output = horde(home.path(), &["config", "show", "--origin"]).await?;
    assert!(output.status.success(), "{output:?}");
    let shown = String::from_utf8_lossy(&output.stdout);
    for line in [
        format!(
            "session.permission_mode = \"ask\"  # file {}",
            file.display()
        ),
        "logs.max_files = 2  # default".to_owned(),
    ] {
        assert!(shown.contains(&line), "{line} in:\n{shown}");
    }

    std::fs::write(&file, "[session]\nmode = \"ask\"\n")?;
    let output = horde(home.path(), &["config", "show"]).await?;
    assert!(!output.status.success());
    let refusal = String::from_utf8_lossy(&output.stderr);
    assert!(
        refusal.contains(&format!("client config {}", file.display()))
            && refusal.contains("unknown key session.mode"),
        "{refusal}"
    );
    Ok(())
}

#[tokio::test]
async fn config_show_server_prints_the_per_agent_resolution() -> Setup<()> {
    let (endpoint, _dir) = configured_server(
        "[permissions]\ndefault_mode = \"ask\"\n[permissions.modes.careful]\nown_orchestrator_verbs = true\n",
    )
    .await?;
    let mut client = Client::connect(&endpoint).await?;
    client
        .create_workspace_definition(CreateWorkspaceDefinitionRequest {
            name: "careful-ws".to_owned(),
            repos: vec![WorkspaceRepo {
                name: "atlas".to_owned(),
                remote_url: "https://example.com/atlas.git".to_owned(),
                default_branch: "main".to_owned(),
                ..WorkspaceRepo::default()
            }],
            permission_mode: "careful".to_owned(),
            ..CreateWorkspaceDefinitionRequest::default()
        })
        .await?;
    let home = tempfile::tempdir()?;
    let output = horde(
        home.path(),
        &[
            "--endpoint",
            &endpoint,
            "config",
            "show",
            "--workspace",
            "careful-ws",
            "--origin",
        ],
    )
    .await?;
    assert!(output.status.success(), "{output:?}");
    let shown = String::from_utf8_lossy(&output.stdout);
    for line in [
        "agent.permission_mode = \"careful\"  # workspace careful-ws",
        "permissions.modes.careful = { bypass = false, own_orchestrator_verbs = true, deny = 0 rules, allow = 0 rules }",
        "tiers.fast = ",
    ] {
        assert!(shown.contains(line), "{line} in:\n{shown}");
    }
    let json = horde(
        home.path(),
        &[
            "--endpoint",
            &endpoint,
            "--json",
            "config",
            "show",
            "--server",
        ],
    )
    .await?;
    let entries: serde_json::Value = serde_json::from_slice(&json.stdout)?;
    let default_mode = entries
        .as_array()
        .and_then(|all| {
            all.iter()
                .find(|entry| entry["key"] == "permissions.default_mode")
        })
        .cloned();
    assert_eq!(
        default_mode.map(|entry| entry["value"].clone()),
        Some(serde_json::json!("\"ask\""))
    );
    Ok(())
}

/// The client file's `session.permission_mode` becomes a new session's
/// layer; `new --permission-mode` beats it; the server reports which.
#[tokio::test]
async fn the_client_files_session_default_reaches_a_new_session_and_the_flag_beats_it() -> Setup<()>
{
    let endpoint = serve().await?;
    let dir = tempfile::tempdir()?;
    let file = dir.path().join("client.toml");
    std::fs::write(&file, "[session]\npermission_mode = \"ask\"\n")?;
    let config = file.to_string_lossy().into_owned();
    invoke(&endpoint, &["--config", &config, "session", "new", "filed"]).await?;
    invoke(
        &endpoint,
        &[
            "--config",
            &config,
            "session",
            "new",
            "flagged",
            "--permission-mode",
            "bypass",
        ],
    )
    .await?;

    let mut client = Client::connect(&endpoint).await?;
    let mut modes = Vec::new();
    for title in ["filed", "flagged"] {
        let session = horde_cli::resolve::session_id(&mut client, title, false).await?;
        let entry = client
            .effective_config("", Some(&session))
            .await?
            .into_iter()
            .find(|entry| entry.key == "agent.permission_mode")
            .ok_or("an agent entry")?;
        assert_eq!(entry.origin, format!("session {session}"), "{title}");
        modes.push(entry.value);
    }
    assert_eq!(modes, ["\"ask\"", "\"bypass\""]);

    let missing = invoke(
        &endpoint,
        &["--config", "/nonexistent/client.toml", "hello"],
    )
    .await
    .expect_err("a named file must exist");
    assert!(
        missing.to_string().contains("/nonexistent/client.toml"),
        "{missing}"
    );
    Ok(())
}
