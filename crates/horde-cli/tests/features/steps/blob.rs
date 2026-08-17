//! Attachment content through the real blob verbs, asserted on disk.

use cucumber::{given, then, when};
use horde_proto::blob::BlobDigest;

use crate::world::{HordeWorld, StepResult};

/// Where this scenario's files live, creating the directory on first use.
fn workdir(world: &mut HordeWorld) -> StepResult<std::path::PathBuf> {
    if let Some(dir) = world.notes.get("workdir") {
        return Ok(std::path::PathBuf::from(dir));
    }
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_path_buf();
    world
        .notes
        .insert("workdir".to_owned(), path.display().to_string());
    world.keep.push(dir);
    Ok(path)
}

#[given(expr = "a work file {string} containing {string}")]
fn a_work_file(world: &mut HordeWorld, name: String, content: String) -> StepResult {
    let path = workdir(world)?.join(&name);
    std::fs::write(&path, content)?;
    world.notes.insert(name, path.display().to_string());
    Ok(())
}

#[when(expr = "the user stashes {string}")]
async fn the_user_stashes(world: &mut HordeWorld, name: String) -> StepResult {
    let path = world.note(&name)?.to_owned();
    let endpoint = world.endpoint()?.to_owned();
    world.run_horde(&endpoint, &["blob", "stash", &path]).await
}

/// "stashed N bytes at sha256:…" — the address is the last word, and it is
/// what every later step (and every payload attachment) carries.
#[then("stdout prints the blob's address")]
fn stdout_prints_the_address(world: &mut HordeWorld) -> StepResult {
    let stdout = world.last_run()?.stdout.clone();
    let address = stdout
        .split_whitespace()
        .last()
        .filter(|word| word.starts_with("sha256:"))
        .ok_or_else(|| format!("no content address on stdout: {stdout:?}"))?;
    world.notes.insert("address".to_owned(), address.to_owned());
    Ok(())
}

#[when(expr = "the user fetches that address into {string}")]
async fn the_user_fetches_into(world: &mut HordeWorld, name: String) -> StepResult {
    let address = world.note("address")?.to_owned();
    fetch(world, &address, &name).await
}

#[when(expr = "the user fetches the address of the missing content {string} into {string}")]
async fn the_user_fetches_missing(
    world: &mut HordeWorld,
    content: String,
    name: String,
) -> StepResult {
    // The address of bytes nobody stashed: well-formed, verifiable, absent.
    let address = BlobDigest::of(content.as_bytes()).to_string();
    fetch(world, &address, &name).await
}

async fn fetch(world: &mut HordeWorld, address: &str, name: &str) -> StepResult {
    let out = workdir(world)?.join(name);
    world
        .notes
        .insert(name.to_owned(), out.display().to_string());
    world
        .notes
        .insert("out".to_owned(), out.display().to_string());
    let endpoint = world.endpoint()?.to_owned();
    let out = out.display().to_string();
    world
        .run_horde(&endpoint, &["blob", "fetch", address, "--out", &out])
        .await
}

#[then(expr = "the fetched file {string} holds {string}")]
fn the_fetched_file_holds(world: &mut HordeWorld, name: String, content: String) -> StepResult {
    let path = world.note(&name)?;
    let read = std::fs::read_to_string(path)?;
    if read != content {
        return Err(format!("expected {content:?} in {path}, got {read:?}").into());
    }
    Ok(())
}

#[then("no output file was written")]
fn no_output_file_was_written(world: &mut HordeWorld) -> StepResult {
    let out = world.note("out")?;
    if std::path::Path::new(out).exists() {
        return Err(format!("a failed fetch must not leave {out} behind").into());
    }
    Ok(())
}
