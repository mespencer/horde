//! Enrollment and revocation over the secured harness: real CA, real TLS on
//! real sockets, identities issued only through the production join path.

use cucumber::{given, then, when};
use horde_identity::PrincipalRole;

use crate::world::{HordeWorld, Secured, StepResult, within};

#[given("a secured server")]
async fn a_secured_server(world: &mut HordeWorld) -> StepResult {
    let harness = horde_server_testkit::secure::serve_secured().await?;
    world.secure = Some(Secured::new(harness, tempfile::tempdir()?));
    Ok(())
}

/// An operator: a client identity enrolled through the production path and
/// saved to disk, where `--identity` can point at it. The bootstrap mint goes
/// through the authority handle — the same shape as the real server minting
/// on its own stdout before any client exists.
#[given(expr = "an enrolled operator {string}")]
async fn an_enrolled_operator(world: &mut HordeWorld, name: String) -> StepResult {
    let secure = world
        .secure
        .as_mut()
        .ok_or("no secured server is running")?;
    let identity = secure.harness.enroll(PrincipalRole::Client, &name).await?;
    let dir = secure.base().join(&name);
    std::fs::create_dir_all(&dir)?;
    identity.save(&dir)?;
    secure.identity_dirs.insert(name.clone(), dir);
    world.notes.insert("operator".to_owned(), name);
    Ok(())
}

/// Like "the user runs", but speaking mTLS as the enrolled operator.
#[when(expr = "the operator runs {string}")]
async fn the_operator_runs(world: &mut HordeWorld, command: String) -> StepResult {
    let operator = world.note("operator")?.to_owned();
    let secure = world.secure()?;
    let endpoint = secure.harness.main_endpoint();
    let identity_dir = secure.identity_dir(&operator)?.display().to_string();
    let words = super::cli::command_words(&world.substituted(&command));
    let args: Vec<&str> = words.iter().map(String::as_str).collect();
    world.run_horde_as(&endpoint, &identity_dir, &args).await
}

/// "code <secret.fingerprint>" — the artifact a human carries to the new
/// machine. Noted for the join steps.
#[then("stdout prints a one-time code")]
fn stdout_prints_a_code(world: &mut HordeWorld) -> StepResult {
    let stdout = world.last_run()?.stdout.clone();
    let code = stdout
        .lines()
        .find_map(|line| line.strip_prefix("code "))
        .map(str::trim)
        .filter(|code| !code.is_empty())
        .ok_or_else(|| format!("no code line on stdout: {stdout:?}"))?;
    world.notes.insert("code".to_owned(), code.to_owned());
    Ok(())
}

/// The client join, exactly as a new machine does it: the real binary, a
/// fresh identity directory, the enrollment endpoint, and the pasted code.
#[when("a new machine joins with the printed code")]
async fn a_new_machine_joins(world: &mut HordeWorld) -> StepResult {
    let code = world.note("code")?.to_owned();
    let secure = world.secure()?;
    let enrollment = secure.harness.enrollment_endpoint();
    let dir = secure.base().join("new-machine");
    let dir_flag = dir.display().to_string();
    world
        .run_horde_as(
            &enrollment,
            &dir_flag,
            &["enroll", "join", &enrollment, &code],
        )
        .await?;
    let run = world.last_run()?;
    if !run.success {
        return Err(format!("the join failed: {}", run.stderr).into());
    }
    world.notes.insert("joined-dir".to_owned(), dir_flag);
    Ok(())
}

#[then("the new machine's identity opens the control plane")]
async fn the_new_identity_opens_the_plane(world: &mut HordeWorld) -> StepResult {
    let dir = world.note("joined-dir")?.to_owned();
    let endpoint = world.secure()?.harness.main_endpoint();
    world.run_horde_as(&endpoint, &dir, &["hello"]).await?;
    let run = world.last_run()?;
    if !(run.success && run.stdout.contains("horde-server")) {
        return Err(format!(
            "the joined identity was refused: stdout {:?}, stderr {:?}",
            run.stdout, run.stderr
        )
        .into());
    }
    Ok(())
}

/// The worker join: the same production `join` the `horde-worker` binary
/// calls, introducing itself in the Worker role.
#[when(expr = "a worker machine joins as {string} with the printed code")]
async fn a_worker_machine_joins(world: &mut HordeWorld, name: String) -> StepResult {
    let code_text = world.note("code")?.to_owned();
    let secure = world
        .secure
        .as_mut()
        .ok_or("no secured server is running")?;
    let code =
        horde_identity::JoinCode::parse(&code_text).ok_or("the printed code is not a join code")?;
    let enrolled = within("the worker join", async {
        Ok(horde_identity::join(
            &secure.harness.enrollment_endpoint(),
            &code,
            horde_server_testkit::secure::hello(PrincipalRole::Worker, &name),
        )
        .await?)
    })
    .await?;
    world.notes.insert(
        "worker-principal".to_owned(),
        enrolled.principal.to_string(),
    );
    secure.identities.insert(name, enrolled.identity);
    Ok(())
}

#[then(expr = "the worker is enrolled as {string}")]
fn the_worker_is_enrolled_as(world: &mut HordeWorld, principal: String) -> StepResult {
    let enrolled = world.note("worker-principal")?;
    if enrolled != principal {
        return Err(format!("expected {principal:?}, got {enrolled:?}").into());
    }
    Ok(())
}

/// Revocation's effect, not its exit code: the stored identity that used to
/// open the plane now cannot.
#[then("the worker's identity no longer opens the control plane")]
async fn the_workers_identity_no_longer_opens(world: &mut HordeWorld) -> StepResult {
    let secure = world.secure()?;
    let identity = secure
        .identities
        .values()
        .next()
        .ok_or("no worker identity was enrolled")?;
    let refused = within("the revoked dial to be refused", async {
        let Ok(endpoint) = secure.harness.main_endpoint_as(identity) else {
            return Ok(true);
        };
        let Ok(channel) = endpoint.connect().await else {
            return Ok(true);
        };
        Ok(horde_cli::Client::with_channel(channel)
            .hello("0.1.0")
            .await
            .is_err())
    })
    .await?;
    if !refused {
        return Err("a revoked identity still opens the control plane".into());
    }
    Ok(())
}

/// No identity at all against the mTLS door.
#[when(expr = "an unenrolled user tries {string} against the secured server")]
async fn an_unenrolled_user_tries(world: &mut HordeWorld, command: String) -> StepResult {
    let endpoint = world.secure()?.harness.main_endpoint();
    let words = super::cli::command_words(&command);
    let args: Vec<&str> = words.iter().map(String::as_str).collect();
    world.run_horde(&endpoint, &args).await
}
