//! The vocabulary every gate speaks: outcomes, enforcement, and run context.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::command::CommandRunner;

/// How much authority a gate's verdict carries.
///
/// docs/TESTING.md §4 describes a rollout ladder (observe → ratchet → hard
/// gate). Horde starts from zero code, so every gate is registered at
/// [`Enforcement::Blocking`] on day one; the other rungs exist so a future
/// gate can be introduced without inventing the concept under pressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Enforcement {
    /// Reported, never fails the run.
    Observe,
    /// Compared against a committed baseline; may not get worse.
    Ratchet,
    /// A `fail` verdict reds the run.
    Blocking,
}

impl Enforcement {
    /// The word used in reports.
    pub fn label(self) -> &'static str {
        match self {
            Self::Observe => "observe",
            Self::Ratchet => "ratchet",
            Self::Blocking => "blocking",
        }
    }
}

/// What a gate concluded.
///
/// The four variants are deliberately not collapsible:
///
/// * `Pass` — measured, and the measurement is clean.
/// * `Fail` — measured, and the code does not meet the bar. A verdict about
///   code; only blocking gates turn it into a red run.
/// * `Skipped` — there was nothing to measure. Distinct from `Pass`, because a
///   zero must say which of its two meanings it has.
/// * `Error` — the tooling could not measure. Always fatal, for every gate at
///   every enforcement level: a gate that could not run did not pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GateOutcome {
    /// Measured clean.
    Pass {
        /// What was measured, e.g. `"37 files formatted"`.
        detail: String,
    },
    /// Measured, and the code does not meet the bar.
    Fail {
        /// What the verdict is and where to look.
        detail: String,
    },
    /// Nothing to measure.
    Skipped {
        /// Why there was nothing to measure.
        reason: String,
    },
    /// The instrument itself broke.
    Error {
        /// What broke, and how to fix the environment.
        reason: String,
    },
}

impl GateOutcome {
    /// Measured clean.
    pub fn pass(detail: impl Into<String>) -> Self {
        Self::Pass {
            detail: detail.into(),
        }
    }

    /// A verdict about the code.
    pub fn fail(detail: impl Into<String>) -> Self {
        Self::Fail {
            detail: detail.into(),
        }
    }

    /// Nothing to measure.
    pub fn skipped(reason: impl Into<String>) -> Self {
        Self::Skipped {
            reason: reason.into(),
        }
    }

    /// The instrument broke.
    pub fn error(reason: impl Into<String>) -> Self {
        Self::Error {
            reason: reason.into(),
        }
    }

    /// The single word shown in the result column.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Pass { .. } => "pass",
            Self::Fail { .. } => "fail",
            Self::Skipped { .. } => "skipped",
            Self::Error { .. } => "error",
        }
    }

    /// The explanatory text carried by whichever variant this is.
    pub fn detail(&self) -> &str {
        match self {
            Self::Pass { detail } | Self::Fail { detail } => detail,
            Self::Skipped { reason } | Self::Error { reason } => reason,
        }
    }

    /// Whether this outcome reds the run at the given enforcement level.
    ///
    /// `Error` reds unconditionally — an unmeasured gate is a broken gate, and
    /// a broken instrument tells you nothing about advisory gates either.
    pub fn reds_run(&self, enforcement: Enforcement) -> bool {
        match self {
            Self::Error { .. } => true,
            Self::Fail { .. } => enforcement == Enforcement::Blocking,
            Self::Pass { .. } | Self::Skipped { .. } => false,
        }
    }
}

/// Everything a gate needs to know about the run it is part of.
#[derive(Debug, Clone)]
pub struct GateContext {
    /// Absolute path to the cargo workspace root.
    pub workspace_root: PathBuf,
    /// Packages the run was narrowed to; empty means the whole workspace.
    pub packages: Vec<String>,
    /// Directory for artifacts and per-gate logs (`target/quality`).
    pub log_dir: PathBuf,
    /// Whether the caller asked to measure the whole tree rather than the
    /// change under test. Only the `mutants` gate scopes by change today.
    pub full: bool,
    /// Whether the caller asked to measure what changed since the last green
    /// whole-tree sweep, rather than since the branch point. Only `mutants`
    /// reads it.
    pub incremental: bool,
}

impl GateContext {
    /// Builds a context rooted at `workspace_root`, logging to `target/quality`.
    pub fn new(workspace_root: &Path, packages: Vec<String>) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            log_dir: workspace_root.join("target").join("quality"),
            packages,
            full: false,
            incremental: false,
        }
    }

    /// Asks for the whole tree rather than the change under test.
    #[must_use]
    pub fn measuring_everything(mut self, full: bool) -> Self {
        self.full = full;
        self
    }

    /// Asks for what changed since the last green whole-tree sweep.
    #[must_use]
    pub fn since_last_full_sweep(mut self, incremental: bool) -> Self {
        self.incremental = incremental;
        self
    }

    /// Cargo's package selection flags for this run.
    ///
    /// Returns `["--workspace"]` when nothing was narrowed, so a gate never
    /// silently measures only the default members.
    pub fn package_args(&self) -> Vec<String> {
        if self.packages.is_empty() {
            return vec!["--workspace".to_owned()];
        }
        let mut args = Vec::with_capacity(self.packages.len() * 2);
        for package in &self.packages {
            args.push("-p".to_owned());
            args.push(package.clone());
        }
        args
    }

    /// Where the coverage gate writes LCOV and the crap gate reads it.
    pub fn lcov_path(&self) -> PathBuf {
        self.log_dir.join("lcov.info")
    }
}

/// A quality gate: one measurement, one verdict.
///
/// Implementations own their tool, flags, exclusions and thresholds — gate
/// definitions live in Rust, not YAML (docs/TESTING.md §1), so the laptop and
/// CI run byte-identical commands.
pub trait Gate: Send + Sync {
    /// Measures, and reports what was measured.
    ///
    /// Never returns `Result`: a gate's inability to measure is itself one of
    /// the four outcomes ([`GateOutcome::Error`]), not an error to propagate.
    fn run(&self, ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome;
}

/// Static facts about a gate, held by the registry rather than the gate.
///
/// Keeping name, enforcement and dependencies as registry *data* means
/// promoting a gate is a one-line reviewable diff in one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GateSpec {
    /// Stable identifier used on the command line and in reports.
    pub name: &'static str,
    /// Authority of this gate's verdict.
    pub enforcement: Enforcement,
    /// Gates that must run first because this one consumes their output.
    pub depends_on: &'static [&'static str],
    /// One line shown by `gauntlet list`.
    pub summary: &'static str,
}

/// What one gate did during one run.
///
/// `outcome` is an `Option` on purpose: a gate that never reported — because
/// the process died mid-run, or because it was never reached — must still
/// appear in the report, as a `no result` row. A gate silently vanishing is how
/// a quality layer dies without anyone noticing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateResult {
    /// Gate name, matching its registry entry.
    pub name: String,
    /// Enforcement level at the time of the run.
    pub enforcement: Enforcement,
    /// What the gate concluded, or `None` if it never reported.
    pub outcome: Option<GateOutcome>,
    /// Wall-clock duration, or `None` if the gate never finished.
    pub millis: Option<u64>,
}

impl GateResult {
    /// A row for a gate that has been selected but has not reported yet.
    pub fn pending(spec: &GateSpec) -> Self {
        Self {
            name: spec.name.to_owned(),
            enforcement: spec.enforcement,
            outcome: None,
            millis: None,
        }
    }

    /// Whether this row reds the run.
    ///
    /// A missing outcome reds it: a gate that could not run did not pass.
    pub fn reds_run(&self) -> bool {
        match &self.outcome {
            Some(outcome) => outcome.reds_run(self.enforcement),
            None => true,
        }
    }
}

/// A gate plus its registry facts.
pub struct RegisteredGate {
    /// Static facts.
    pub spec: GateSpec,
    /// The measurement itself.
    pub gate: Box<dyn Gate>,
}

impl std::fmt::Debug for RegisteredGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredGate")
            .field("spec", &self.spec)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enforcement_labels_are_stable() {
        assert_eq!(Enforcement::Observe.label(), "observe");
        assert_eq!(Enforcement::Ratchet.label(), "ratchet");
        assert_eq!(Enforcement::Blocking.label(), "blocking");
    }

    #[test]
    fn outcome_labels_are_stable() {
        assert_eq!(GateOutcome::pass("x").label(), "pass");
        assert_eq!(GateOutcome::fail("x").label(), "fail");
        assert_eq!(GateOutcome::skipped("x").label(), "skipped");
        assert_eq!(GateOutcome::error("x").label(), "error");
    }

    #[test]
    fn outcome_detail_reads_whichever_field_the_variant_carries() {
        assert_eq!(GateOutcome::pass("clean").detail(), "clean");
        assert_eq!(GateOutcome::fail("two over").detail(), "two over");
        assert_eq!(GateOutcome::skipped("no diff").detail(), "no diff");
        assert_eq!(GateOutcome::error("no binary").detail(), "no binary");
    }

    #[test]
    fn error_reds_the_run_at_every_enforcement_level() {
        for enforcement in [
            Enforcement::Observe,
            Enforcement::Ratchet,
            Enforcement::Blocking,
        ] {
            assert!(
                GateOutcome::error("broken").reds_run(enforcement),
                "error must be fatal at {}",
                enforcement.label()
            );
        }
    }

    #[test]
    fn fail_reds_the_run_only_for_blocking_gates() {
        let fail = GateOutcome::fail("two functions over threshold");
        assert!(fail.reds_run(Enforcement::Blocking));
        assert!(!fail.reds_run(Enforcement::Ratchet));
        assert!(!fail.reds_run(Enforcement::Observe));
    }

    #[test]
    fn pass_and_skipped_never_red_the_run() {
        for enforcement in [
            Enforcement::Observe,
            Enforcement::Ratchet,
            Enforcement::Blocking,
        ] {
            assert!(!GateOutcome::pass("clean").reds_run(enforcement));
            assert!(!GateOutcome::skipped("nothing").reds_run(enforcement));
        }
    }

    #[test]
    fn skipped_is_not_pass() {
        assert_ne!(
            GateOutcome::skipped("nothing to measure"),
            GateOutcome::pass("nothing to measure")
        );
    }

    const SPEC: GateSpec = GateSpec {
        name: "fmt",
        enforcement: Enforcement::Blocking,
        depends_on: &[],
        summary: "formatting",
    };

    #[test]
    fn a_registered_gate_debugs_as_its_spec() {
        struct Nothing;
        impl Gate for Nothing {
            fn run(&self, _ctx: &GateContext, _runner: &dyn CommandRunner) -> GateOutcome {
                GateOutcome::pass("nothing")
            }
        }
        let registered = RegisteredGate {
            spec: SPEC,
            gate: Box::new(Nothing),
        };
        let rendered = format!("{registered:?}");
        assert!(rendered.contains("RegisteredGate"), "{rendered}");
        assert!(rendered.contains("fmt"), "{rendered}");
        assert!(rendered.contains("Blocking"), "{rendered}");
    }

    #[test]
    fn a_pending_row_carries_the_gate_identity_but_no_outcome() {
        let result = GateResult::pending(&SPEC);
        assert_eq!(result.name, "fmt");
        assert_eq!(result.enforcement, Enforcement::Blocking);
        assert_eq!(result.outcome, None);
        assert_eq!(result.millis, None);
    }

    #[test]
    fn a_row_with_no_outcome_reds_the_run() {
        assert!(GateResult::pending(&SPEC).reds_run());
    }

    #[test]
    fn a_row_delegates_to_its_outcome_when_it_has_one() {
        let mut result = GateResult::pending(&SPEC);
        result.outcome = Some(GateOutcome::pass("clean"));
        assert!(!result.reds_run());
        result.outcome = Some(GateOutcome::fail("dirty"));
        assert!(result.reds_run());
        result.enforcement = Enforcement::Observe;
        assert!(!result.reds_run());
        result.outcome = Some(GateOutcome::error("broken"));
        assert!(result.reds_run());
    }

    #[test]
    fn whole_workspace_runs_select_the_whole_workspace_explicitly() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        assert_eq!(ctx.package_args(), vec!["--workspace".to_owned()]);
    }

    #[test]
    fn narrowed_runs_pass_one_dash_p_per_package() {
        let ctx = GateContext::new(
            Path::new("/repo"),
            vec!["horde-gauntlet".to_owned(), "horde-proto".to_owned()],
        );
        assert_eq!(
            ctx.package_args(),
            vec!["-p", "horde-gauntlet", "-p", "horde-proto"]
        );
    }

    #[test]
    fn a_context_measures_the_change_under_test_unless_asked_otherwise() {
        assert!(!GateContext::new(Path::new("/repo"), Vec::new()).full);
        assert!(
            GateContext::new(Path::new("/repo"), Vec::new())
                .measuring_everything(true)
                .full
        );
    }

    #[test]
    fn a_context_measures_since_the_branch_point_unless_asked_otherwise() {
        assert!(!GateContext::new(Path::new("/repo"), Vec::new()).incremental);
        assert!(
            GateContext::new(Path::new("/repo"), Vec::new())
                .since_last_full_sweep(true)
                .incremental
        );
    }

    #[test]
    fn artifacts_live_under_target_quality() {
        let ctx = GateContext::new(Path::new("/repo"), Vec::new());
        assert_eq!(ctx.log_dir, Path::new("/repo/target/quality"));
        assert_eq!(ctx.lcov_path(), Path::new("/repo/target/quality/lcov.info"));
    }
}
