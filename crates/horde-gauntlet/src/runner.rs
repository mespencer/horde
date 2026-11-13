//! Selecting, ordering and running gates.

use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

use crate::command::CommandRunner;
use crate::gate::{GateContext, GateResult, RegisteredGate};
use crate::logs::{RecordingRunner, write_log};
use crate::registry::find;

/// Where a run leaves its machine-readable results.
const RESULTS_FILE: &str = "results.json";

/// Expands a gate selection into the order the gates will run in.
///
/// Selecting a gate pulls in whatever it consumes — asking for `crap` runs
/// `coverage` first, because scoring against a stale LCOV is not a measurement.
/// An unknown name is refused rather than ignored: a typo that silently runs
/// nothing is the quietest way for a quality layer to disappear.
///
/// # Errors
///
/// Returns a message naming the unknown gate and listing the registered ones.
pub fn resolve<'a>(
    gates: &'a [RegisteredGate],
    requested: &[String],
) -> Result<Vec<&'a RegisteredGate>, String> {
    if requested.is_empty() {
        return Ok(gates.iter().collect());
    }
    let mut needed: Vec<&str> = Vec::new();
    for name in requested {
        let gate = find(gates, name).ok_or_else(|| unknown_gate(gates, name))?;
        collect(gates, gate.spec.name, &mut needed);
    }
    Ok(gates
        .iter()
        .filter(|gate| needed.contains(&gate.spec.name))
        .collect())
}

/// Removes the gates named in `skipped` from a selection.
///
/// A skip is refused when it names no gate — a typo would otherwise run the
/// gate it meant to skip — or when a gate still selected consumes the skipped
/// one's output, because that gate would then read a stale artifact.
///
/// # Errors
///
/// Returns a message naming the unknown gate or the dependency.
pub fn without<'a>(
    gates: &[RegisteredGate],
    selected: Vec<&'a RegisteredGate>,
    skipped: &[String],
) -> Result<Vec<&'a RegisteredGate>, String> {
    if let Some(name) = skipped.iter().find(|name| find(gates, name).is_none()) {
        return Err(unknown_gate(gates, name));
    }
    let kept: Vec<&RegisteredGate> = selected
        .into_iter()
        .filter(|gate| !skipped.iter().any(|name| name == gate.spec.name))
        .collect();
    for gate in &kept {
        if let Some(dependency) = gate
            .spec
            .depends_on
            .iter()
            .find(|dependency| skipped.iter().any(|name| name == *dependency))
        {
            return Err(format!(
                "cannot skip `{dependency}`: `{}` reads its output; skip both or neither",
                gate.spec.name
            ));
        }
    }
    Ok(kept)
}

fn collect<'a>(gates: &'a [RegisteredGate], name: &'a str, needed: &mut Vec<&'a str>) {
    if needed.contains(&name) {
        return;
    }
    needed.push(name);
    if let Some(gate) = find(gates, name) {
        for dependency in gate.spec.depends_on {
            collect(gates, dependency, needed);
        }
    }
}

fn unknown_gate(gates: &[RegisteredGate], name: &str) -> String {
    let known: Vec<&str> = gates.iter().map(|gate| gate.spec.name).collect();
    format!(
        "unknown gate `{name}`; registered gates are: {}",
        known.join(", ")
    )
}

/// Runs `selected` in order, writing a log per gate and results as it goes.
///
/// Results are written after every gate, so a run that dies part-way still
/// leaves a report in which the gates that never reported are visible as
/// `no result` rows rather than absent.
///
/// # Errors
///
/// Returns an I/O error if a log or the results file cannot be written.
pub fn run(
    selected: &[&RegisteredGate],
    ctx: &GateContext,
    runner: &dyn CommandRunner,
) -> io::Result<Vec<GateResult>> {
    let mut results: Vec<GateResult> = selected
        .iter()
        .map(|gate| GateResult::pending(&gate.spec))
        .collect();
    save_results(&ctx.log_dir, &results)?;
    for (position, gate) in selected.iter().enumerate() {
        let recorder = RecordingRunner::new(runner);
        let started = Instant::now();
        let outcome = gate.gate.run(ctx, &recorder);
        let millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        write_log(&ctx.log_dir, gate.spec.name, &recorder.transcript())?;
        if let Some(result) = results.get_mut(position) {
            result.outcome = Some(outcome);
            result.millis = Some(millis);
        }
        save_results(&ctx.log_dir, &results)?;
    }
    Ok(results)
}

/// Where a run's machine-readable results live.
pub fn results_path(log_dir: &Path) -> PathBuf {
    log_dir.join(RESULTS_FILE)
}

/// Writes the results of a run.
///
/// # Errors
///
/// Returns an I/O error if the directory or file cannot be written.
pub fn save_results(log_dir: &Path, results: &[GateResult]) -> io::Result<PathBuf> {
    std::fs::create_dir_all(log_dir)?;
    let path = results_path(log_dir);
    let body = serde_json::to_string_pretty(results).map_err(io::Error::other)?;
    std::fs::write(&path, body)?;
    Ok(path)
}

/// Reads the results of the last run.
///
/// # Errors
///
/// Returns an I/O error if no run has been recorded or the file is unreadable.
pub fn load_results(log_dir: &Path) -> io::Result<Vec<GateResult>> {
    let path = results_path(log_dir);
    let body = std::fs::read_to_string(&path)?;
    serde_json::from_str(&body).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::{Enforcement, Gate, GateOutcome, GateSpec};
    use crate::registry::registry;
    use crate::testing::ScriptedRunner;

    struct Fixed(GateOutcome);

    impl Gate for Fixed {
        fn run(&self, _ctx: &GateContext, _runner: &dyn CommandRunner) -> GateOutcome {
            self.0.clone()
        }
    }

    /// A gate that runs one command, so the transcript has something to hold.
    struct Talkative;

    impl Gate for Talkative {
        fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
            let spec = crate::command::CommandSpec::new("cargo", &["noop"], &ctx.workspace_root);
            match runner.run(&spec) {
                Ok(_) => GateOutcome::pass("ran"),
                Err(message) => GateOutcome::error(message),
            }
        }
    }

    fn entry(
        name: &'static str,
        depends_on: &'static [&'static str],
        gate: Box<dyn Gate>,
    ) -> RegisteredGate {
        RegisteredGate {
            spec: GateSpec {
                name,
                enforcement: Enforcement::Blocking,
                depends_on,
                summary: "test gate",
            },
            gate,
        }
    }

    fn names(gates: &[&RegisteredGate]) -> Vec<&'static str> {
        gates.iter().map(|gate| gate.spec.name).collect()
    }

    fn pass(name: &'static str) -> RegisteredGate {
        entry(name, &[], Box::new(Fixed(GateOutcome::pass("clean"))))
    }

    #[test]
    fn an_empty_selection_runs_every_gate_in_registry_order() {
        let gates = registry();
        let resolved = resolve(&gates, &[]).expect("no names to be wrong");
        assert_eq!(names(&resolved).len(), gates.len());
        assert_eq!(names(&resolved)[0], "fmt");
    }

    #[test]
    fn a_skipped_gate_is_left_out_of_the_run() {
        let gates = registry();
        let selected = resolve(&gates, &[]).expect("everything");
        let kept = without(&gates, selected, &["mutants".to_owned()]).expect("mutants is a gate");
        assert_eq!(kept.len(), gates.len() - 1);
        assert!(!names(&kept).contains(&"mutants"), "{:?}", names(&kept));
    }

    #[test]
    fn skipping_nothing_keeps_the_selection() {
        let gates = registry();
        let selected = resolve(&gates, &["fmt".to_owned()]).expect("registered");
        assert_eq!(
            names(&without(&gates, selected, &[]).expect("no skips")),
            vec!["fmt"]
        );
    }

    #[test]
    fn skipping_a_gate_that_does_not_exist_is_refused() {
        let gates = registry();
        let selected = resolve(&gates, &[]).expect("everything");
        let message = without(&gates, selected, &["mutnats".to_owned()]).expect_err("typo");
        assert!(message.contains("unknown gate `mutnats`"), "{message}");
    }

    #[test]
    fn skipping_a_gate_another_selected_gate_reads_is_refused() {
        let gates = registry();
        let selected = resolve(&gates, &[]).expect("everything");
        let message =
            without(&gates, selected, &["coverage".to_owned()]).expect_err("crap reads it");
        assert!(message.contains("`crap` reads its output"), "{message}");
        let selected = resolve(&gates, &[]).expect("everything");
        let both = without(
            &gates,
            selected,
            &["coverage".to_owned(), "crap".to_owned()],
        )
        .expect("skipping both is coherent");
        assert!(!names(&both).contains(&"crap"));
    }

    #[test]
    fn selecting_one_gate_runs_only_that_gate() {
        let gates = registry();
        let resolved = resolve(&gates, &["clippy".to_owned()]).expect("registered");
        assert_eq!(names(&resolved), vec!["clippy"]);
    }

    #[test]
    fn selecting_crap_pulls_in_the_coverage_run_it_reads() {
        let gates = registry();
        let resolved = resolve(&gates, &["crap".to_owned()]).expect("registered");
        assert_eq!(names(&resolved), vec!["coverage", "crap"]);
    }

    #[test]
    fn dependencies_run_first_however_the_selection_was_ordered() {
        let gates = registry();
        let resolved =
            resolve(&gates, &["crap".to_owned(), "coverage".to_owned()]).expect("registered");
        assert_eq!(names(&resolved), vec!["coverage", "crap"]);
    }

    #[test]
    fn a_gate_named_twice_runs_once() {
        let gates = registry();
        let resolved = resolve(&gates, &["fmt".to_owned(), "fmt".to_owned()]).expect("registered");
        assert_eq!(names(&resolved), vec!["fmt"]);
    }

    #[test]
    fn a_dependency_chain_is_followed_transitively() {
        let gates = vec![
            pass("first"),
            entry(
                "second",
                &["first"],
                Box::new(Fixed(GateOutcome::pass("x"))),
            ),
            entry(
                "third",
                &["second"],
                Box::new(Fixed(GateOutcome::pass("x"))),
            ),
        ];
        let resolved = resolve(&gates, &["third".to_owned()]).expect("registered");
        assert_eq!(names(&resolved), vec!["first", "second", "third"]);
    }

    #[test]
    fn an_unknown_gate_is_refused_and_the_message_lists_the_real_ones() {
        let gates = registry();
        let message = resolve(&gates, &["clipy".to_owned()]).expect_err("typo");
        assert!(message.contains("unknown gate `clipy`"), "{message}");
        assert!(message.contains("clippy"), "{message}");
    }

    #[test]
    fn a_run_records_an_outcome_and_a_duration_for_every_gate() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        let gates = [
            pass("alpha"),
            entry("beta", &[], Box::new(Fixed(GateOutcome::fail("nope")))),
        ];
        let selected: Vec<&RegisteredGate> = gates.iter().collect();
        let results = run(&selected, &ctx, &ScriptedRunner::new(Vec::new())).expect("io");

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].name, "alpha");
        assert_eq!(results[0].outcome, Some(GateOutcome::pass("clean")));
        assert_eq!(results[1].outcome, Some(GateOutcome::fail("nope")));
        assert!(results.iter().all(|result| result.millis.is_some()));
    }

    #[test]
    fn a_run_leaves_one_log_per_gate_holding_what_that_gate_ran() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        let gates = [entry("alpha", &[], Box::new(Talkative))];
        let selected: Vec<&RegisteredGate> = gates.iter().collect();
        let runner = ScriptedRunner::always(crate::command::CommandOutput::new(Some(0), "hi", ""));
        run(&selected, &ctx, &runner).expect("io");

        let log = std::fs::read_to_string(ctx.log_dir.join("alpha.log")).expect("log exists");
        assert!(log.contains("$ cargo noop"), "{log}");
        assert!(log.contains("hi"), "{log}");
    }

    #[test]
    fn results_survive_a_round_trip_through_disk() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        let gates = [pass("alpha")];
        let selected: Vec<&RegisteredGate> = gates.iter().collect();
        let written = run(&selected, &ctx, &ScriptedRunner::new(Vec::new())).expect("io");
        assert_eq!(load_results(&ctx.log_dir).expect("saved"), written);
    }

    #[test]
    fn results_are_saved_before_any_gate_runs_so_a_crash_leaves_no_result_rows() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        let pending = vec![GateResult::pending(&pass("alpha").spec)];
        save_results(&ctx.log_dir, &pending).expect("io");
        let loaded = load_results(&ctx.log_dir).expect("saved");
        assert_eq!(loaded[0].outcome, None);
        assert_eq!(loaded[0].name, "alpha");
    }

    #[test]
    fn reading_results_before_any_run_is_an_error_not_an_empty_report() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert!(load_results(dir.path()).is_err());
    }

    #[test]
    fn a_corrupt_results_file_is_an_error_not_an_empty_report() {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(results_path(dir.path()), "not json").expect("write");
        assert!(load_results(dir.path()).is_err());
    }
}
