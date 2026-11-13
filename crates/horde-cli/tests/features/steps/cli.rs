//! The generic CLI steps: servers, sessions, running the real binary, and
//! asserting on what landed where a user would look.

use cucumber::{given, then, when};
use horde_proto::v1::event::Kind;

use crate::world::{HordeWorld, StepResult};

#[given("a running server")]
async fn a_running_server(world: &mut HordeWorld) -> StepResult {
    world.endpoint = Some(horde_server_testkit::serve().await?);
    Ok(())
}

#[given("a running server with a blob store")]
async fn a_server_with_a_blob_store(world: &mut HordeWorld) -> StepResult {
    let root = tempfile::tempdir()?;
    world.endpoint = Some(horde_server_testkit::serve_with_blob_store(root.path()).await?);
    world.keep.push(root);
    Ok(())
}

#[given(expr = "a session titled {string}")]
async fn a_session_titled(world: &mut HordeWorld, title: String) -> StepResult {
    let (_, session_id, root) = horde_server_testkit::seeded(world.endpoint()?, &title).await?;
    world.session_id = Some(session_id);
    world.root_agent = Some(root);
    Ok(())
}

/// Splits a feature-file command line into argv words, honouring the
/// `\"…\"` quoting Gherkin strings use for arguments with spaces.
pub(crate) fn command_words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in line.replace("\\\"", "\"").chars() {
        if character == '"' {
            quoted = !quoted;
        } else if character.is_whitespace() && !quoted {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        } else {
            current.push(character);
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

#[when(expr = "the user runs {string}")]
async fn the_user_runs(world: &mut HordeWorld, command: String) -> StepResult {
    let words = command_words(&world.substituted(&command));
    let args: Vec<&str> = words.iter().map(String::as_str).collect();
    let endpoint = world.endpoint()?.to_owned();
    world.run_horde(&endpoint, &args).await
}

#[then("the command succeeds")]
fn the_command_succeeds(world: &mut HordeWorld) -> StepResult {
    let run = world.last_run()?;
    if !run.success {
        return Err(format!("the command failed; stderr: {}", run.stderr).into());
    }
    Ok(())
}

#[then("the command fails")]
fn the_command_fails(world: &mut HordeWorld) -> StepResult {
    let run = world.last_run()?;
    if run.success {
        return Err(format!("the command succeeded; stdout: {}", run.stdout).into());
    }
    Ok(())
}

#[then(expr = "stdout contains {string}")]
fn stdout_contains(world: &mut HordeWorld, needle: String) -> StepResult {
    let needle = world.substituted(&needle);
    let stdout = &world.last_run()?.stdout;
    if !stdout.contains(&needle) {
        return Err(format!("stdout does not contain {needle:?}: {stdout:?}").into());
    }
    Ok(())
}

#[then(expr = "stdout says {string}")]
fn stdout_says(world: &mut HordeWorld, exactly: String) -> StepResult {
    let stdout = world.last_run()?.stdout.trim().to_owned();
    if stdout != exactly {
        return Err(format!("expected exactly {exactly:?} on stdout, got {stdout:?}").into());
    }
    Ok(())
}

#[then("stdout names the root agent")]
fn stdout_names_the_root_agent(world: &mut HordeWorld) -> StepResult {
    let stdout = &world.last_run()?.stdout;
    if !stdout.contains("root agent: ") {
        return Err(format!("no root agent line on stdout: {stdout:?}").into());
    }
    Ok(())
}

/// Waits (ceilinged) until the log carries the text, then replays the session
/// through the real binary — the observation a user actually makes — and
/// asserts the text is on its stdout.
#[then(expr = "the user eventually sees {string} in the session replay")]
async fn eventually_sees_in_replay(world: &mut HordeWorld, needle: String) -> StepResult {
    let wanted = needle.clone();
    world
        .await_event(&format!("the log to carry {needle:?}"), move |envelope| {
            let text = match envelope.event.as_ref()?.kind.as_ref()? {
                Kind::AgentOutputChunk(chunk) => &chunk.text,
                Kind::NoteSent(sent) => &sent.note.as_ref()?.text,
                Kind::AssignmentSent(sent) => &sent.assignment.as_ref()?.text,
                _ => return None,
            };
            text.contains(&wanted).then_some(())
        })
        .await?;

    replay_shows(world, &needle).await
}

/// Replays the session through the real binary and checks `needle` is on
/// its stdout — the observation a user makes.
pub(crate) async fn replay_shows(world: &mut HordeWorld, needle: &str) -> StepResult {
    let endpoint = world.endpoint()?.to_owned();
    let session = world.session()?.to_owned();
    world.run_horde(&endpoint, &["sync", &session]).await?;
    let run = world.last_run()?;
    if !(run.success && run.stdout.contains(needle)) {
        return Err(format!(
            "the replay does not show {needle:?}: stdout {:?}, stderr {:?}",
            run.stdout, run.stderr
        )
        .into());
    }
    Ok(())
}
