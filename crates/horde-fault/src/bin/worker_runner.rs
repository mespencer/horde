//! The worker-runner: the production worker library as a killable process.
//!
//! Test fixture, not product (`.cargo/mutants.toml` excludes it under the
//! same criterion as `stub_harness`: the suite runs it, it tests nothing).
//! It composes exactly the pieces the eventual `horde-worker` binary will —
//! `Journal::open` over a file that survives `kill -9`, a `Runtime` over a
//! scripted FakeAdapter, `Connection::dial` and a redial loop — so what the
//! fault suite kills mid-turn is the real library code end to end.
//!
//! ```text
//! worker-runner --endpoint <url> --worker-id <id> --journal <path>
//!               [--turn <answer | opener>stall_ms>answer>]...
//!               [--identity <enrolled identity dir>]
//! ```

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
use horde_worker::{Connection, DialConfig, Journal, Runtime, capabilities};

/// A parse or startup refusal: printed, then exit non-zero.
type Refusal = String;

struct Invocation {
    endpoint: String,
    worker_id: String,
    journal: PathBuf,
    turns: Vec<ScriptedTurn>,
    identity: Option<horde_identity::StoredIdentity>,
}

fn main() -> ExitCode {
    let invocation = match parse(std::env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(refusal) => {
            eprintln!("worker-runner: {refusal}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("worker-runner: no tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(invocation)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(refusal) => {
            eprintln!("worker-runner: {refusal}");
            ExitCode::FAILURE
        }
    }
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Invocation, Refusal> {
    let mut endpoint = None;
    let mut worker_id = None;
    let mut journal = None;
    let mut turns = Vec::new();
    let mut identity_dir: Option<PathBuf> = None;

    while let Some(flag) = args.next() {
        let mut value = |flag: &str| args.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--endpoint" => endpoint = Some(value("--endpoint")?),
            "--worker-id" => worker_id = Some(value("--worker-id")?),
            "--journal" => journal = Some(PathBuf::from(value("--journal")?)),
            "--turn" => turns.push(decode_turn(&value("--turn")?)?),
            "--identity" => identity_dir = Some(PathBuf::from(value("--identity")?)),
            other => return Err(format!("unknown flag {other}")),
        }
    }

    let identity = match identity_dir {
        None => None,
        Some(dir) => Some(
            horde_identity::StoredIdentity::load(&dir)
                .map_err(|error| format!("could not load the identity at {dir:?}: {error}"))?,
        ),
    };

    Ok(Invocation {
        endpoint: endpoint.ok_or("--endpoint is required")?,
        worker_id: worker_id.ok_or("--worker-id is required")?,
        journal: journal.ok_or("--journal is required")?,
        turns,
        identity,
    })
}

/// `answer`, or `opener>stall_ms>answer`: a turn that announces itself and
/// then hangs in flight — the shape a kill or a cut is aimed into.
fn decode_turn(spec: &str) -> Result<ScriptedTurn, Refusal> {
    let parts: Vec<&str> = spec.split('>').collect();
    match parts.as_slice() {
        [answer] => Ok(ScriptedTurn::saying(*answer)),
        [opener, stall_ms, answer] => {
            let stall_ms: u64 = stall_ms
                .parse()
                .map_err(|_| format!("{stall_ms:?} is not a stall in milliseconds"))?;
            Ok(ScriptedTurn {
                steps: vec![
                    Step::Say((*opener).to_owned()),
                    Step::Stall(Duration::from_millis(stall_ms)),
                ],
                ..ScriptedTurn::saying(*answer)
            })
        }
        _ => Err(format!(
            "{spec:?} is not a turn (want `answer` or `opener>stall_ms>answer`)"
        )),
    }
}

/// Dial, run, redial: the worker's supervision loop, bounded by attempt count
/// as a safety net — the suite ends this process by SIGKILL, never by wait.
async fn run(invocation: Invocation) -> Result<(), Refusal> {
    let journal = Journal::open(&invocation.journal)
        .map_err(|error| format!("could not open the journal: {error}"))?;
    let mut runtime = Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(
        Script::of(invocation.turns.clone()),
    )));

    for _ in 0_u32..10_000 {
        let config = DialConfig {
            endpoint: invocation.endpoint.clone(),
            worker_id: invocation.worker_id.clone(),
            hello: capabilities::hello(&invocation.worker_id),
            capabilities: capabilities::advertise(
                runtime.adapters(),
                runtime.workspaces().map(|_| Vec::new()),
            ),
            identity: invocation.identity.clone(),
        };
        if let Ok((mut connection, registration)) = Connection::dial(&config).await {
            println!(
                "runner: registered resume_from={}",
                registration.resume_from_journal_sequence
            );
            // Ok means the server hung up; Err means the transport died or a
            // batch was refused. Either way the connection is over and the
            // resume rules make the next dial exact.
            let outcome = connection.run(&journal, &mut runtime).await;
            eprintln!("runner: connection over: {outcome:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err("the redial safety net ran out; the suite should have killed this process".to_owned())
}
