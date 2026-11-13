//! Workspaces (ADR-0014): real origin repos made with the real `git`, and a
//! real `horde-worker` runtime with a workspace root, dialing the scenario's
//! server. Everything the steps assert comes back through the `horde`
//! binary or the session's log.

use std::path::Path;
use std::sync::Arc;

use cucumber::{given, then, when};
use horde_adapter_fake::{FakeAdapter, Script};
use horde_proto::v1::event::Kind;

use crate::world::{HordeWorld, StepResult};

/// Runs one git command in `dir`, failing the step with git's own words.
fn git(dir: &Path, args: &[&str]) -> StepResult<String> {
    let output = std::process::Command::new("git")
        .args(["-c", "user.name=horde", "-c", "user.email=horde@test"])
        .args(args)
        .current_dir(dir)
        .output()?;
    if !output.status.success() {
        return Err(format!("git {args:?}: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[given(expr = "an origin repo {string} with one commit")]
async fn an_origin_repo(world: &mut HordeWorld, name: String) -> StepResult {
    let dir = tempfile::tempdir()?;
    let origin = dir.path().join(&name);
    std::fs::create_dir_all(&origin)?;
    git(&origin, &["init", "-q", "-b", "main"])?;
    std::fs::write(origin.join("README.md"), format!("{name}\n"))?;
    git(&origin, &["add", "."])?;
    git(&origin, &["commit", "-qm", "seed"])?;
    world.notes.insert(name, origin.display().to_string());
    world.keep.push(dir);
    Ok(())
}

#[given(expr = "the origin repo {string} gains a commit after the instance was cloned")]
async fn the_origin_gains_a_commit(world: &mut HordeWorld, name: String) -> StepResult {
    let origin = std::path::PathBuf::from(world.notes.get(&name).ok_or("no such origin")?);
    std::fs::write(origin.join("later.txt"), "later\n")?;
    git(&origin, &["add", "."])?;
    git(&origin, &["commit", "-qm", "later work"])?;
    let tip = git(&origin, &["rev-parse", "--short=7", "HEAD"])?;
    world.notes.insert("tip".to_owned(), tip);
    Ok(())
}

#[given(expr = "a workspace {string} over the origin repo {string}")]
async fn a_workspace_over(world: &mut HordeWorld, workspace: String, origin: String) -> StepResult {
    let url = world.notes.get(&origin).cloned().ok_or("no such origin")?;
    let endpoint = world.endpoint()?.to_owned();
    let repo = format!("{origin}={url}");
    world
        .run_horde(
            &endpoint,
            &["--json", "workspace", "create", &workspace, "--repo", &repo],
        )
        .await?;
    let run = world.last_run()?;
    if !run.success {
        return Err(format!("workspace create failed: {}", run.stderr).into());
    }
    let created: serde_json::Value = serde_json::from_str(run.stdout.trim())?;
    let id = created["workspace_definition_id"]
        .as_str()
        .ok_or("the create printed no id")?
        .to_owned();
    world.notes.insert(format!("workspace:{workspace}"), id);
    Ok(())
}

#[given("a workspace worker joins")]
async fn a_workspace_worker_joins(world: &mut HordeWorld) -> StepResult {
    let journal_dir = tempfile::tempdir()?;
    let journal = horde_worker::Journal::open(&journal_dir.path().join("journal.sqlite"))?;
    let root = journal_dir.path().join("workspaces");
    let runtime = horde_worker::Runtime::new(journal.clone())
        .with_adapter(Arc::new(FakeAdapter::new(Script::echoing())))
        .with_workspaces(horde_worker::WorkspaceManager::new("ws-box", root));
    let warm = match runtime.workspaces() {
        Some(workspaces) => Some(workspaces.warm_instances().await),
        None => None,
    };
    crate::steps::worker::dial(world, runtime, journal, journal_dir, "ws-box", warm).await
}

#[when(expr = "the user starts a session in workspace {string}")]
async fn starts_a_session(world: &mut HordeWorld, workspace: String) -> StepResult {
    start(world, &workspace, "").await
}

#[when(expr = "the user starts a session in workspace {string} with {string}")]
async fn starts_a_session_with(
    world: &mut HordeWorld,
    workspace: String,
    flags: String,
) -> StepResult {
    start(world, &workspace, &flags).await
}

async fn start(world: &mut HordeWorld, workspace: &str, flags: &str) -> StepResult {
    let endpoint = world.endpoint()?.to_owned();
    let id = world
        .notes
        .get(&format!("workspace:{workspace}"))
        .cloned()
        .ok_or("no such workspace was created")?;
    let mut args = vec!["--json", "session", "new", "workspaces", "--workspace", &id];
    args.extend(flags.split_whitespace());
    world.run_horde(&endpoint, &args).await?;
    let run = world.last_run()?;
    if !run.success {
        return Err(format!("session new failed: {}", run.stderr).into());
    }
    let created: serde_json::Value = serde_json::from_str(run.stdout.trim())?;
    world.session_id = created["session_id"].as_str().map(str::to_owned);
    world.root_agent = created["root_agent_id"].as_str().map(str::to_owned);
    Ok(())
}

#[then(expr = "the user eventually sees the checkout start {string} at the origin's tip")]
async fn sees_the_checkout_at_the_tip(world: &mut HordeWorld, resolution: String) -> StepResult {
    let tip = world
        .notes
        .get("tip")
        .cloned()
        .ok_or("no tip was recorded")?;
    sees_checkout(world, &format!("atlas@{tip} {resolution}")).await
}

#[then(expr = "the user eventually sees the checkout start {string} on {string}")]
async fn sees_the_checkout_on(
    world: &mut HordeWorld,
    resolution: String,
    branch: String,
) -> StepResult {
    sees_checkout(world, &format!("{resolution} on {branch}")).await
}

/// Waits for the root agent's `checkout_prepared`, then replays the session
/// through the real binary and finds `needle` on its human line.
async fn sees_checkout(world: &mut HordeWorld, needle: &str) -> StepResult {
    world
        .await_event("the checkout's provenance", |envelope| {
            matches!(
                envelope.event.as_ref()?.kind.as_ref()?,
                Kind::CheckoutPrepared(_)
            )
            .then_some(())
        })
        .await?;
    crate::steps::cli::replay_shows(world, needle).await
}
