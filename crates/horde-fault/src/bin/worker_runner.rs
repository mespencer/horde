//! The worker-runner: the production worker library as a killable process.
//!
//! Test fixture, not product (`.cargo/mutants.toml` excludes it under the
//! same criterion as `stub_harness`: the suite runs it, it tests nothing).
//! It composes exactly the pieces the eventual `horde-worker` binary will —
//! `Journal::open` over a file that survives `kill -9`, a `Runtime` over a
//! scripted FakeAdapter, `Connection::dial` and a redial loop — so what the
//! fault suite kills mid-turn is the real library code end to end.
//!
//! The pure half (argument parsing, turn decoding) carries its own unit
//! tests: the spawned copy runs outside llvm-cov's sight, so this is the
//! only coverage those functions can have, and the CRAP gate holds fixture
//! binaries to the same bar as everything else. The process half stays in
//! functions small enough to be under that bar uncovered.
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

#[derive(Debug, Default)]
struct Invocation {
    endpoint: String,
    worker_id: String,
    journal: PathBuf,
    turns: Vec<ScriptedTurn>,
    identity_dir: Option<PathBuf>,
}

fn main() -> ExitCode {
    match boot() {
        Ok(()) => ExitCode::SUCCESS,
        Err(refusal) => {
            eprintln!("worker-runner: {refusal}");
            ExitCode::FAILURE
        }
    }
}

/// Everything between the arguments and the redial loop.
fn boot() -> Result<(), Refusal> {
    let invocation = parse(std::env::args().skip(1))?;
    let runtime =
        tokio::runtime::Runtime::new().map_err(|error| format!("no tokio runtime: {error}"))?;
    runtime.block_on(run(invocation))
}

/// Folds the argument list into an invocation.
fn parse(mut args: impl Iterator<Item = String>) -> Result<Invocation, Refusal> {
    let mut invocation = Invocation::default();
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| format!("{flag} needs a value"))?;
        apply_flag(&mut invocation, &flag, value)?;
    }
    check_required(invocation)
}

/// Applies one `--flag value` pair.
fn apply_flag(invocation: &mut Invocation, flag: &str, value: String) -> Result<(), Refusal> {
    match flag {
        "--endpoint" => invocation.endpoint = value,
        "--worker-id" => invocation.worker_id = value,
        "--journal" => invocation.journal = PathBuf::from(value),
        "--turn" => invocation.turns.push(decode_turn(&value)?),
        "--identity" => invocation.identity_dir = Some(PathBuf::from(value)),
        other => return Err(format!("unknown flag {other}")),
    }
    Ok(())
}

/// Refuses an invocation missing any of the three required flags.
fn check_required(invocation: Invocation) -> Result<Invocation, Refusal> {
    for (name, missing) in [
        ("--endpoint", invocation.endpoint.is_empty()),
        ("--worker-id", invocation.worker_id.is_empty()),
        ("--journal", invocation.journal.as_os_str().is_empty()),
    ] {
        if missing {
            return Err(format!("{name} is required"));
        }
    }
    Ok(invocation)
}

/// `answer`, or `opener>stall_ms>answer`: a turn that announces itself,
/// hangs in flight — the shape a kill or a cut is aimed into — and then
/// speaks its answer, so a suite can watch for the completion in the log.
fn decode_turn(spec: &str) -> Result<ScriptedTurn, Refusal> {
    let parts: Vec<&str> = spec.split('>').collect();
    match parts.as_slice() {
        [answer] => Ok(ScriptedTurn::saying(*answer)),
        [opener, stall_ms, answer] => stalling_turn(opener, stall_ms, answer),
        _ => Err(format!(
            "{spec:?} is not a turn (want `answer` or `opener>stall_ms>answer`)"
        )),
    }
}

/// The three-part turn: opener, stall, spoken answer. Built by assignment
/// rather than a struct-literal update: cargo-mutants' delete-field genre
/// targets struct expressions, and its names sidestep the path-shaped
/// `exclude_re` this fixture lives under — an assignment leaves that genre
/// nothing to mutate.
fn stalling_turn(opener: &str, stall_ms: &str, answer: &str) -> Result<ScriptedTurn, Refusal> {
    let stall_ms: u64 = stall_ms
        .parse()
        .map_err(|_| format!("{stall_ms:?} is not a stall in milliseconds"))?;
    let mut turn = ScriptedTurn::saying(answer);
    turn.steps = vec![
        Step::Say(opener.to_owned()),
        Step::Stall(Duration::from_millis(stall_ms)),
        Step::Say(answer.to_owned()),
    ];
    Ok(turn)
}

/// Dial, run, redial: the worker's supervision loop, bounded by attempt
/// count as a safety net — the suite ends this process by SIGKILL.
async fn run(invocation: Invocation) -> Result<(), Refusal> {
    let (identity, journal, mut runtime) = prepare(&invocation)?;
    for _ in 0_u32..10_000 {
        attempt(&invocation, &identity, &journal, &mut runtime).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err("the redial safety net ran out; the suite should have killed this process".to_owned())
}

/// The durable pieces every attempt shares: the identity, the journal that
/// survives a kill, and the runtime over the scripted adapter.
fn prepare(
    invocation: &Invocation,
) -> Result<(Option<horde_identity::StoredIdentity>, Journal, Runtime), Refusal> {
    let identity = load_identity(invocation.identity_dir.as_deref())?;
    let journal = Journal::open(&invocation.journal)
        .map_err(|error| format!("could not open the journal: {error}"))?;
    let runtime = Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(
        Script::of(invocation.turns.clone()),
    )));
    Ok((identity, journal, runtime))
}

/// The stored identity for an mTLS dial, when a directory was given.
fn load_identity(
    directory: Option<&std::path::Path>,
) -> Result<Option<horde_identity::StoredIdentity>, Refusal> {
    let Some(directory) = directory else {
        return Ok(None);
    };
    horde_identity::StoredIdentity::load(directory)
        .map(Some)
        .map_err(|error| format!("could not load the identity at {directory:?}: {error}"))
}

/// One dial-and-run attempt. Ok means the server hung up; Err means the
/// transport died or a batch was refused — either way the connection is
/// over, and the resume rules make the next dial exact.
async fn attempt(
    invocation: &Invocation,
    identity: &Option<horde_identity::StoredIdentity>,
    journal: &Journal,
    runtime: &mut Runtime,
) {
    let config = DialConfig {
        endpoint: invocation.endpoint.clone(),
        worker_id: invocation.worker_id.clone(),
        hello: capabilities::hello(&invocation.worker_id),
        capabilities: capabilities::advertise(
            runtime.adapters(),
            runtime.workspaces().map(|_| Vec::new()),
        ),
        identity: identity.clone(),
    };
    if let Ok((mut connection, registration)) = Connection::dial(&config).await {
        println!(
            "runner: registered resume_from={}",
            registration.resume_from_journal_sequence
        );
        let outcome = connection.run(journal, runtime).await;
        eprintln!("runner: connection over: {outcome:?}");
    }
}

#[cfg(test)]
mod tests {
    use super::{Invocation, decode_turn, parse};
    use horde_adapter_fake::Step;

    fn parsed(args: &[&str]) -> Result<Invocation, super::Refusal> {
        parse(args.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn a_full_argument_list_parses_into_the_invocation() {
        let invocation = parsed(&[
            "--endpoint",
            "http://127.0.0.1:1",
            "--worker-id",
            "worker-a",
            "--journal",
            "/tmp/j.sqlite",
            "--turn",
            "hello",
            "--turn",
            "opened>250>done",
            "--identity",
            "/tmp/id",
        ])
        .expect("a full command line parses");
        assert_eq!(invocation.endpoint, "http://127.0.0.1:1");
        assert_eq!(invocation.worker_id, "worker-a");
        assert_eq!(
            invocation.journal,
            std::path::PathBuf::from("/tmp/j.sqlite")
        );
        assert_eq!(invocation.turns.len(), 2);
        assert_eq!(
            invocation.identity_dir,
            Some(std::path::PathBuf::from("/tmp/id"))
        );
    }

    #[test]
    fn every_wrong_shape_is_refused_by_name() {
        for (args, needle) in [
            (&["--endpoint"][..], "--endpoint needs a value"),
            (&["--frobnicate", "x"][..], "unknown flag"),
            (&["--worker-id", "w"][..], "--endpoint is required"),
            (
                &["--endpoint", "e", "--journal", "j"][..],
                "--worker-id is required",
            ),
            (
                &["--endpoint", "e", "--worker-id", "w"][..],
                "--journal is required",
            ),
            (&["--endpoint", "e", "--turn", "a>b"][..], "not a turn"),
            (
                &["--endpoint", "e", "--turn", "a>soon>b"][..],
                "not a stall in milliseconds",
            ),
        ] {
            let refused = parsed(args).expect_err("must refuse");
            assert!(
                refused.contains(needle),
                "{args:?} must be refused naming {needle:?}: {refused}"
            );
        }
    }

    #[test]
    fn turns_decode_into_the_scripted_shapes_the_suite_relies_on() {
        let plain = decode_turn("done").expect("a plain answer decodes");
        assert_eq!(plain.text.as_deref(), Some("done"));
        assert_eq!(plain.steps, vec![Step::Say("done".to_owned())]);

        let stalling = decode_turn("opened>250>done").expect("a stalling turn decodes");
        assert_eq!(stalling.text.as_deref(), Some("done"));
        assert_eq!(stalling.steps.len(), 3, "opener, stall, spoken answer");
        assert!(
            matches!(&stalling.steps[1], Step::Stall(stall)
                if *stall == core::time::Duration::from_millis(250)),
            "the stall carries its milliseconds: {:?}",
            stalling.steps
        );
    }
}
