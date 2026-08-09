//! Copy-paste detection: jscpd over the Rust sources.
//!
//! Agents duplicate code far more readily than they abstract it: asked to add a
//! variant, the cheapest correct answer is to copy the neighbouring function and
//! change three tokens. jscpd's default detection size (50 tokens / 5 lines) is
//! left exactly as it ships, and the threshold is zero blocks — tuning the
//! window to make an existing duplicate disappear would be gaming the gate.

use serde::Deserialize;

use crate::command::{CommandRunner, CommandSpec};
use crate::gate::{Gate, GateContext, GateOutcome};
use crate::gates::{invoke, read_artifact, unexpected_exit};
use crate::tool::{ToolPin, check_pin};

/// The jscpd version the gate is calibrated against.
///
/// npx resolves the exact version named in the command, so the pin is enforced
/// twice: by the `jscpd@…` specifier and by this probe.
pub const PIN: ToolPin = ToolPin {
    program: "npx",
    version_args: &["--yes", "jscpd@5.0.14", "--version"],
    expected: "5.0.14",
    install: "install Node.js 18+ so `npx` can fetch jscpd@5.0.14",
};

/// Directory name under `target/quality` for jscpd's report.
const REPORT_DIR: &str = "jscpd";

/// Detects copy-pasted blocks across the workspace's Rust sources.
#[derive(Debug, Clone, Copy, Default)]
pub struct DuplicationGate;

impl Gate for DuplicationGate {
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
        if let Err(reason) = check_pin(runner, &PIN, &ctx.workspace_root) {
            return GateOutcome::error(reason);
        }
        let spec = spec(ctx);
        let output = match invoke(runner, &spec) {
            Ok(output) => output,
            Err(outcome) => return outcome,
        };
        if !output.success() {
            return unexpected_exit(&spec, &output);
        }
        match read_artifact(&report_path(ctx), "jscpd") {
            Err(outcome) => outcome,
            Ok(json) => match parse_report(&json) {
                Err(reason) => GateOutcome::error(reason),
                Ok(report) => outcome_of(&report),
            },
        }
    }
}

/// Where jscpd writes its JSON report.
pub fn report_path(ctx: &GateContext) -> std::path::PathBuf {
    ctx.log_dir.join(REPORT_DIR).join("jscpd-report.json")
}

/// The command this gate runs.
///
/// Detection settings live in the committed `.jscpd.json`, not here, because
/// jscpd reads them itself when it walks the tree.
pub fn spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "npx",
        vec![
            "--yes".to_owned(),
            "jscpd@5.0.14".to_owned(),
            "--config".to_owned(),
            ctx.workspace_root.join(".jscpd.json").display().to_string(),
            "--reporters".to_owned(),
            "json".to_owned(),
            "--output".to_owned(),
            ctx.log_dir.join(REPORT_DIR).display().to_string(),
            "--silent".to_owned(),
            ctx.workspace_root.display().to_string(),
        ],
        &ctx.workspace_root,
    )
}

/// One duplicated block, as jscpd reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Duplicate {
    /// The earlier occurrence.
    #[serde(rename = "firstFile")]
    pub first: Location,
    /// The later occurrence.
    #[serde(rename = "secondFile")]
    pub second: Location,
    /// How many lines the block spans.
    pub lines: u64,
}

/// Where one half of a duplicated block lives.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Location {
    /// Path to the file.
    pub name: String,
    /// First line of the block.
    pub start: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct Statistics {
    total: Totals,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct Totals {
    clones: u64,
    sources: u64,
    lines: u64,
}

/// What jscpd found.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Report {
    duplicates: Vec<Duplicate>,
    statistics: Statistics,
}

impl Report {
    /// Number of duplicated blocks found.
    pub fn clones(&self) -> u64 {
        self.statistics.total.clones
    }

    /// Number of files jscpd actually looked at.
    pub fn sources(&self) -> u64 {
        self.statistics.total.sources
    }

    /// Total lines scanned.
    pub fn lines(&self) -> u64 {
        self.statistics.total.lines
    }
}

/// Reads jscpd's JSON report.
///
/// # Errors
///
/// Returns a message when the file is not a jscpd report.
pub fn parse_report(json: &str) -> Result<Report, String> {
    serde_json::from_str(json).map_err(|err| format!("could not read jscpd's JSON report: {err}"))
}

/// Turns a jscpd report into a verdict.
///
/// Scanning zero files is `skipped`, not `pass`: "no duplication" and "nothing
/// was looked at" are different facts, and a misconfigured `.jscpd.json` that
/// silently matched nothing is exactly the failure this distinction catches.
pub fn outcome_of(report: &Report) -> GateOutcome {
    if report.sources() == 0 {
        return GateOutcome::skipped("jscpd scanned no files — check .jscpd.json");
    }
    if report.clones() == 0 {
        return GateOutcome::pass(format!(
            "no duplicated blocks in {} file(s), {} line(s)",
            report.sources(),
            report.lines()
        ));
    }
    let mut detail = format!("{} duplicated block(s)", report.clones());
    for duplicate in &report.duplicates {
        detail.push_str(&format!(
            "\n  {} line(s): {}:{} and {}:{}",
            duplicate.lines,
            duplicate.first.name,
            duplicate.first.start,
            duplicate.second.name,
            duplicate.second.start
        ));
    }
    GateOutcome::fail(detail)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::ScriptedRunner;

    const CLEAN: &str = include_str!("../../fixtures/jscpd-report.json");
    const DIRTY: &str = include_str!("../../fixtures/jscpd-report-duplicated.json");

    fn ctx() -> GateContext {
        GateContext::new(Path::new("/repo"), Vec::new())
    }

    #[test]
    fn the_command_pins_the_version_and_points_at_the_committed_config() {
        let line = spec(&ctx()).display();
        assert!(line.contains("jscpd@5.0.14"), "{line}");
        assert!(line.contains("--config /repo/.jscpd.json"), "{line}");
        assert!(
            line.contains("--output /repo/target/quality/jscpd"),
            "{line}"
        );
    }

    #[test]
    fn the_report_is_read_from_where_the_command_writes_it() {
        assert_eq!(
            report_path(&ctx()),
            Path::new("/repo/target/quality/jscpd/jscpd-report.json")
        );
    }

    #[test]
    fn a_real_clean_report_is_read_correctly() {
        let report = parse_report(CLEAN).expect("the fixture is real jscpd output");
        assert_eq!(report.clones(), 0);
        assert_eq!(report.sources(), 2);
        assert_eq!(report.lines(), 15);
    }

    #[test]
    fn a_real_report_with_duplication_is_read_correctly() {
        let report = parse_report(DIRTY).expect("the fixture is real jscpd output");
        assert_eq!(report.clones(), 1);
        assert_eq!(report.duplicates.len(), 1);
    }

    #[test]
    fn output_that_is_not_a_report_is_an_error() {
        let message = parse_report("{}").expect_err("missing fields");
        assert!(message.contains("could not read jscpd"), "{message}");
    }

    #[test]
    fn a_clean_report_passes_and_says_how_much_was_scanned() {
        let report = parse_report(CLEAN).expect("valid");
        let GateOutcome::Pass { detail } = outcome_of(&report) else {
            panic!("zero clones over real sources is a pass");
        };
        assert!(detail.contains("2 file(s)"), "{detail}");
        assert!(detail.contains("15 line(s)"), "{detail}");
    }

    #[test]
    fn scanning_nothing_is_skipped_not_passed() {
        let json = r#"{"duplicates":[],"statistics":{"total":{"clones":0,"sources":0,"lines":0}}}"#;
        let report = parse_report(json).expect("valid");
        assert_eq!(outcome_of(&report).label(), "skipped");
    }

    #[test]
    fn any_duplication_at_all_fails_and_names_both_locations() {
        let report = parse_report(DIRTY).expect("valid");
        let GateOutcome::Fail { detail } = outcome_of(&report) else {
            panic!("the threshold is zero blocks");
        };
        assert!(detail.starts_with("1 duplicated block(s)"), "{detail}");
        assert!(detail.contains("src/one.rs"), "{detail}");
        assert!(detail.contains("src/two.rs"), "{detail}");
    }

    #[test]
    fn a_version_mismatch_stops_the_gate_before_it_scans() {
        let runner = ScriptedRunner::new(vec![CommandOutput::new(Some(0), "4.0.0", "")]);
        let GateOutcome::Error { .. } = DuplicationGate.run(&ctx(), &runner) else {
            panic!("a different jscpd is a different detector");
        };
        assert_eq!(runner.calls().len(), 1);
    }

    #[test]
    fn a_missing_npx_is_an_instrument_error_with_an_install_hint() {
        let runner = ScriptedRunner::failing("could not launch `npx`");
        let GateOutcome::Error { reason } = DuplicationGate.run(&ctx(), &runner) else {
            panic!("an absent detector did not measure");
        };
        assert!(reason.contains("Node.js"), "{reason}");
    }

    #[test]
    fn the_gate_reads_the_report_jscpd_actually_wrote() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        std::fs::create_dir_all(ctx.log_dir.join(REPORT_DIR)).expect("report dir");
        std::fs::write(report_path(&ctx), CLEAN).expect("report");
        let runner = ScriptedRunner::new(vec![
            CommandOutput::new(Some(0), "5.0.14", ""),
            CommandOutput::new(Some(0), "", ""),
        ]);
        assert_eq!(DuplicationGate.run(&ctx, &runner).label(), "pass");
    }

    #[test]
    fn a_missing_report_is_an_instrument_error_even_when_jscpd_exited_zero() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = GateContext::new(dir.path(), Vec::new());
        let runner = ScriptedRunner::new(vec![
            CommandOutput::new(Some(0), "5.0.14", ""),
            CommandOutput::new(Some(0), "", ""),
        ]);
        let GateOutcome::Error { reason } = DuplicationGate.run(&ctx, &runner) else {
            panic!("no report means no measurement");
        };
        assert!(reason.contains("jscpd-report.json"), "{reason}");
    }
}
