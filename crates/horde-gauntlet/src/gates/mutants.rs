//! Mutation testing: cargo-gamma, zero missed mutants.
//!
//! Coverage says a line ran; a mutant says the line's behaviour was *checked*.
//! Agent-written tests are unusually good at the first and unusually bad at the
//! second — asserting that a call returned `Ok`, that a vector is non-empty,
//! that no panic occurred. Every one of those passes when the code under it is
//! quietly wrong, and every one of them is exactly what a surviving mutant
//! points at. A mutation that stays green is a finding, not a relief.
//!
//! cargo-gamma compiles every selected mutant into one set of test binaries
//! and switches between them at run time, so a whole workspace is mutated
//! without rebuilding it once per mutant, and it runs only the tests that
//! reach each mutant. The gate supplies scope, parallelism, memory limits and
//! lint capping; `gamma.toml` holds the exclusions and timeouts; the verdict is
//! read from `gamma-report.json`.

pub mod audit;
pub mod diff;
pub mod report;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::command::CommandRunner;
use crate::gates::prelude::*;
use crate::gates::{invoke, read_artifact, unexpected_exit};

use self::audit::Listed;
use self::report::Tally;

/// The cargo-gamma version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["gamma", "--version"],
    expected: "0.2.1",
    install: INSTALLER,
};

/// Installs the patched cargo-gamma, and with `--check` confirms it.
///
/// The version alone cannot tell the patched build from a stock one, so the
/// gate also runs the installer's check, which compares the digests stamped
/// beside the binary at install time with the binary and the patch.
pub const INSTALLER: &str = "tools/cargo-gamma/install.sh";

/// Branch the diff is taken against.
pub const TRUNK: &str = "main";

/// The cargo profile mutation builds use; `gamma.toml` selects it.
///
/// `[profile.mutants]` inherits `test` and turns debuginfo off: the build is
/// paid once per run, debuginfo is the bulk of its link cost, and nothing in
/// a mutation run reads it.
pub const PROFILE: &str = "mutants";

/// How many mutants run at once, when set. Unset: half the cores, at most
/// [`MAX_DEFAULT_JOBS`].
pub const JOBS_ENV: &str = "HORDE_MUTANTS_JOBS";

/// The most one test binary's process tree may use, in MiB; `0` or `off`
/// for no limit. Unset: [`DEFAULT_MEMORY_MIB`].
pub const MEMORY_ENV: &str = "HORDE_MUTANTS_MEMORY_MIB";

/// Where gamma keeps its workspace copy and build, when set. Unset: gamma's
/// own per-workspace cache under `~/.cache/cargo-gamma`.
pub const CACHE_ENV: &str = "HORDE_MUTANTS_CACHE_DIR";

/// The default never runs more mutants at once than this.
///
/// Two cores per concurrent mutant: gamma splits the cores between the
/// concurrent test harnesses, and the suites here spawn processes and servers
/// of their own, so a job per core turns into contention for the
/// timing-sensitive tests rather than speed. Sixteen is what the 32-core
/// development machine sustains; [`DEFAULT_MEMORY_MIB`] is sized with it.
pub const MAX_DEFAULT_JOBS: usize = 16;

/// The default ceiling for one test binary's process tree.
///
/// Sixteen binaries at this ceiling are 64 GiB, which leaves the 94 GiB
/// development machine room for the build, the kernel and other work.
pub const DEFAULT_MEMORY_MIB: u64 = 4096;

const MIB: u64 = 1024 * 1024;

/// Gamma holds each mutant to `max(peak × multiplier, peak + headroom)` of its
/// binary's baseline peak. Both are stated rather than inherited because the
/// baseline guard in [`memory_args`] is derived from them.
const MEMORY_MULTIPLIER: u64 = 2;
const MEMORY_HEADROOM: u64 = 128 * MIB;

/// One binary GiB, in bytes.
const GIB: u64 = 1024 * MIB;

/// Free space below which a mutation run is refused rather than started.
///
/// Gamma keeps a copy of the workspace and a whole instrumented build of it in
/// its cache, and a cold full sweep grows that to tens of gigabytes before the
/// first mutant runs. A run that dies of a full disk mid-build has spent its
/// most expensive part for nothing, so the gate reports a broken instrument
/// instead of starting it.
pub const MIN_FREE_BYTES: u64 = 40 * GIB;

/// Where gamma's reports land, under `target/quality`.
pub const ARTIFACTS: &str = "mutants";

/// The commit of the last green, unnarrowed full sweep, under `target/quality`.
pub const LAST_FULL_SWEEP: &str = "mutants-last-green-full-sweep";

/// What a diff-scoped run is measured against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Since {
    /// The merge-base with `main` (on `main`: the tip commit's first parent).
    BranchPoint,
    /// The commit of the last green full sweep (`--incremental`).
    LastFullSweep(String),
}

/// Which mutants a run covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Only code changed since a baseline, with the diff written here.
    Diff {
        /// The narrowed diff gamma reads.
        path: PathBuf,
        /// What it was taken against.
        since: Since,
    },
    /// Every mutable function in the tree.
    FullTree,
}

impl Scope {
    /// How the scope is described in the gate's detail line.
    pub fn label(&self) -> String {
        match self {
            Self::Diff {
                since: Since::BranchPoint,
                ..
            } => "changed code".to_owned(),
            Self::Diff {
                since: Since::LastFullSweep(commit),
                ..
            } => format!(
                "code changed since the last green full sweep ({})",
                commit.get(..12).unwrap_or(commit)
            ),
            Self::FullTree => "the whole tree".to_owned(),
        }
    }

    fn diff(&self) -> Option<&Path> {
        match self {
            Self::Diff { path, .. } => Some(path),
            Self::FullTree => None,
        }
    }
}

/// How much memory one test binary's process tree may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryCap {
    /// At most this many bytes; see [`memory_args`].
    Limit(u64),
    /// Uncapped, because someone asked for it in so many words.
    Disabled,
}

/// The run's machine-dependent settings, read from the environment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Mutants tested at once.
    pub jobs: usize,
    /// The per-binary memory ceiling.
    pub memory: MemoryCap,
    /// A cache directory other than gamma's default, if one was named.
    pub cache_dir: Option<PathBuf>,
    /// `CARGO_ENCODED_RUSTFLAGS` for the instrumented build.
    pub rustflags: String,
}

impl Settings {
    /// Reads the settings through `var`, with `cores` available.
    ///
    /// # Errors
    ///
    /// Returns a message when a variable is set to something unreadable. A typo
    /// is not quietly replaced by a default: an uncapped run that takes the
    /// machine down leaves no log to read.
    pub fn from_env(
        var: impl Fn(&str) -> Option<String>,
        cores: Option<usize>,
    ) -> Result<Self, String> {
        Ok(Self {
            jobs: jobs(var(JOBS_ENV), cores)?,
            memory: memory_cap(var(MEMORY_ENV))?,
            cache_dir: set(var(CACHE_ENV)).map(PathBuf::from),
            rustflags: cap_lints(var("CARGO_ENCODED_RUSTFLAGS"), var("RUSTFLAGS")),
        })
    }
}

/// A variable's value, when it is set to something other than blank.
fn set(raw: Option<String>) -> Option<String> {
    raw.map(|raw| raw.trim().to_owned())
        .filter(|raw| !raw.is_empty())
}

/// How many mutants to test at once.
///
/// # Errors
///
/// Returns a message when [`JOBS_ENV`] is not a whole number of at least 1.
pub fn jobs(raw: Option<String>, cores: Option<usize>) -> Result<usize, String> {
    let Some(raw) = set(raw) else {
        return Ok(cores.map_or(1, |cores| (cores / 2).clamp(1, MAX_DEFAULT_JOBS)));
    };
    match raw.parse::<usize>() {
        Ok(jobs) if jobs >= 1 => Ok(jobs),
        _ => Err(format!(
            "`{JOBS_ENV}={raw}` is not a whole number of parallel mutants (at least 1)"
        )),
    }
}

/// The per-binary memory ceiling.
///
/// # Errors
///
/// Returns a message when [`MEMORY_ENV`] is neither a number of MiB nor `off`.
pub fn memory_cap(raw: Option<String>) -> Result<MemoryCap, String> {
    let Some(raw) = set(raw) else {
        return Ok(MemoryCap::Limit(DEFAULT_MEMORY_MIB * MIB));
    };
    if raw.eq_ignore_ascii_case("off") {
        return Ok(MemoryCap::Disabled);
    }
    let mib: u64 = raw.parse().map_err(|_| {
        format!(
            "`{MEMORY_ENV}={raw}` is not a whole number of MiB. Set it to a size in MiB, or to \
             `0` or `off` to run uncapped; it is not ignored on the way to a default, because an \
             uncapped run that exhausts the machine leaves no log to read"
        )
    })?;
    match mib.checked_mul(MIB) {
        Some(0) => Ok(MemoryCap::Disabled),
        Some(bytes) => Ok(MemoryCap::Limit(bytes)),
        None => Err(format!(
            "`{MEMORY_ENV}={raw}` is more MiB than a byte count can hold"
        )),
    }
}

/// `CARGO_ENCODED_RUSTFLAGS` with every lint capped at `allow`, keeping the
/// flags already set.
///
/// The workspace denies `unwrap`, `expect` and `panic!` — exactly the
/// constructs some mutators produce — and an ambient `-D warnings` would fail
/// any mutant that compiles with a warning. Capping lints scores those mutants
/// instead of withdrawing them as unviable. `allow`, not `warn`: rustc honours
/// the first `--cap-lints` it is given, and at `warn` every instrumented
/// compile renders thousands of warnings about gamma's own guard code.
pub fn cap_lints(encoded: Option<String>, plain: Option<String>) -> String {
    const SEPARATOR: &str = "\u{1f}";
    const CAP_LINTS: &str = "--cap-lints=allow";
    match (encoded, plain) {
        (Some(flags), _) if flags.is_empty() => CAP_LINTS.to_owned(),
        (Some(flags), _) => format!("{flags}{SEPARATOR}{CAP_LINTS}"),
        (None, Some(flags)) => flags
            .split(' ')
            .filter(|flag| !flag.is_empty())
            .chain(std::iter::once(CAP_LINTS))
            .collect::<Vec<_>>()
            .join(SEPARATOR),
        (None, None) => CAP_LINTS.to_owned(),
    }
}

/// Gamma's memory flags for a cap.
///
/// Mutants get gamma's own ceiling, derived from their binary's baseline peak,
/// rather than one flat limit, so a runaway mutant is stopped near what its
/// suite really uses. That ceiling is unbounded unless the peak is, so the
/// baseline is held to `cap / multiplier` and the headroom to no more than
/// that; then `max(peak × multiplier, peak + headroom)` cannot exceed the cap,
/// and `jobs` binaries cannot exceed `jobs × cap`. A baseline that breaches its
/// guard fails the measurement; a mutant that breaches its ceiling is reported
/// out of memory, and counted missed.
pub fn memory_args(cap: MemoryCap) -> Vec<String> {
    match cap {
        MemoryCap::Limit(bytes) => {
            let baseline = bytes / MEMORY_MULTIPLIER;
            vec![
                "--memory".to_owned(),
                "enforce".to_owned(),
                "--baseline-memory-limit".to_owned(),
                format!("{baseline}B"),
                "--memory-multiplier".to_owned(),
                MEMORY_MULTIPLIER.to_string(),
                "--memory-headroom".to_owned(),
                format!("{}B", MEMORY_HEADROOM.min(baseline)),
            ]
        }
        MemoryCap::Disabled => vec!["--memory".to_owned(), "off".to_owned()],
    }
}

/// Runs cargo-gamma and requires that no mutant is missed.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        nothing_to_measure: None,
        measure: mutate,
    }
}

/// Reads the process's settings, then measures.
fn mutate(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    let cores = std::thread::available_parallelism().ok().map(usize::from);
    match Settings::from_env(|key| std::env::var(key).ok(), cores) {
        Ok(settings) => measure(ctx, runner, &settings),
        Err(reason) => GateOutcome::error(reason),
    }
}

/// Everything after the version probe: the patch check, the disk pre-flight,
/// the audit, the scope, the run and the verdict.
pub fn measure(ctx: &GateContext, runner: &dyn CommandRunner, settings: &Settings) -> GateOutcome {
    match measured(ctx, runner, settings) {
        Ok(outcome) | Err(outcome) => outcome,
    }
}

fn measured(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    settings: &Settings,
) -> Result<GateOutcome, GateOutcome> {
    check_patch(ctx, runner).map_err(GateOutcome::error)?;
    check_disk(ctx, runner).map_err(GateOutcome::error)?;
    let mutable = audited(ctx, runner)?;
    let scope = match scope(ctx, runner, &mutable)? {
        Scoped::Measure(scope) => scope,
        Scoped::Nothing(reason) => return Ok(GateOutcome::skipped(reason)),
    };
    let listed = listing(
        runner,
        &list_spec(ctx, &selection(&ctx.packages), scope.diff(), Config::Read),
    )?;
    audit::refuse_dangerous(&listed, "the run's own listing").map_err(GateOutcome::error)?;
    if listed.is_empty() {
        return Ok(GateOutcome::skipped(format!(
            "nothing mutable in {}: gamma lists no mutant there",
            scope.label()
        )));
    }
    let packages = if ctx.packages.is_empty() && scope.diff().is_some() {
        audit::packages_of(&listed)
    } else {
        ctx.packages.clone()
    };
    let tally = run(
        ctx,
        runner,
        &run_spec(ctx, &selection(&packages), &scope, settings),
    )?;
    let outcome = outcome_of(&scope, &tally);
    if scope == Scope::FullTree && ctx.packages.is_empty() && outcome.label() == "pass" {
        return Ok(record_full_sweep(ctx, runner, outcome));
    }
    Ok(outcome)
}

/// Refuses a cargo-gamma that is not the patched build.
fn check_patch(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<(), String> {
    let spec = patch_check_spec(&ctx.workspace_root);
    let output = runner.run(&spec).map_err(|error| error.to_string())?;
    if output.success() {
        return Ok(());
    }
    Err(format!(
        "cargo-gamma {} is installed, but not the patched build the gate is calibrated against: \
         {}\ninstall with: {INSTALLER}",
        PIN.expected,
        output.combined().trim()
    ))
}

/// The installer's check that the cargo-gamma on this machine is patched.
pub fn patch_check_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("bash", &[INSTALLER, "--check"], root)
}

/// Lists the workspace with and without the configuration, and audits every
/// difference; returns the files that can hold a mutant.
fn audited(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<BTreeSet<String>, GateOutcome> {
    let workspace = selection(&[]);
    let configured = listing(runner, &list_spec(ctx, &workspace, None, Config::Read))?;
    audit::refuse_dangerous(&configured, "the workspace listing").map_err(GateOutcome::error)?;
    let unconfigured = listing(runner, &list_spec(ctx, &workspace, None, Config::Ignore))?;
    let config =
        std::fs::read_to_string(ctx.workspace_root.join(audit::CONFIG)).unwrap_or_default();
    let entries = audit::configured_exclusions(&config).map_err(GateOutcome::error)?;
    let root = ctx.workspace_root.clone();
    audit::verify_exclusions(&entries, &configured, &unconfigured, |path| {
        root.join(path).exists()
    })
    .map_err(GateOutcome::error)?;
    let directives = audit::scan_directives(&ctx.workspace_root).map_err(GateOutcome::error)?;
    audit::verify_suppressions(&configured, &directives).map_err(GateOutcome::error)?;
    Ok(audit::files_of(&unconfigured))
}

/// One gamma listing, read.
fn listing(runner: &dyn CommandRunner, spec: &CommandSpec) -> Result<Vec<Listed>, GateOutcome> {
    let output = invoke(runner, spec)?;
    if !output.success() {
        return Err(unexpected_exit(spec, &output));
    }
    audit::parse_listing(&output.stdout).map_err(GateOutcome::error)
}

/// Whether a listing reads `gamma.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Config {
    /// As a run would.
    Read,
    /// `--no-config`: the population before any exclusion.
    Ignore,
}

/// Gamma's package selection: the named packages, or the whole workspace.
///
/// Without either, gamma would mutate only the workspace's default members.
pub fn selection(packages: &[String]) -> Vec<String> {
    if packages.is_empty() {
        return vec!["--workspace".to_owned()];
    }
    packages
        .iter()
        .flat_map(|package| ["--package".to_owned(), package.clone()])
        .collect()
}

/// The arguments a listing and a run share, so a listing describes exactly
/// the population the run will test.
fn shared_args(operation: &str, selection: &[String], diff: Option<&Path>) -> Vec<String> {
    let mut args: Vec<String> = [
        "gamma",
        operation,
        "--color",
        "never",
        "--progress",
        "never",
    ]
    .map(str::to_owned)
    .to_vec();
    if let Some(diff) = diff {
        args.push("--in-diff".to_owned());
        args.push(diff.display().to_string());
    }
    args.extend(selection.iter().cloned());
    args
}

/// The command that lists mutants without building anything.
pub fn list_spec(
    ctx: &GateContext,
    selection: &[String],
    diff: Option<&Path>,
    config: Config,
) -> CommandSpec {
    let mut args = shared_args("list", selection, diff);
    args.push("--json".to_owned());
    if config == Config::Ignore {
        args.push("--no-config".to_owned());
    }
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// Where gamma writes its reports.
pub fn artifact_dir(ctx: &GateContext) -> PathBuf {
    ctx.log_dir.join(ARTIFACTS)
}

/// The command that builds and tests the mutants.
pub fn run_spec(
    ctx: &GateContext,
    selection: &[String],
    scope: &Scope,
    settings: &Settings,
) -> CommandSpec {
    let mut args = shared_args("run", selection, scope.diff());
    args.push("--artifact-dir".to_owned());
    args.push(artifact_dir(ctx).display().to_string());
    if let Some(cache) = &settings.cache_dir {
        args.push("--cache-dir".to_owned());
        args.push(cache.display().to_string());
    }
    args.push("--jobs".to_owned());
    args.push(settings.jobs.to_string());
    args.extend(memory_args(settings.memory));
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
        .with_env("CARGO_ENCODED_RUSTFLAGS", settings.rustflags.clone())
}

/// Runs gamma and reads its report.
///
/// The artifact directory is emptied first, so a run that dies part-way cannot
/// leave the previous run's report to be read as this one's.
fn run(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    spec: &CommandSpec,
) -> Result<Tally, GateOutcome> {
    let artifacts = artifact_dir(ctx);
    match std::fs::remove_dir_all(&artifacts) {
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(GateOutcome::error(format!(
                "could not clear {}: {error}",
                artifacts.display()
            )));
        }
        _ => {}
    }
    let output = invoke(runner, spec)?;
    if !output.success() {
        return Err(unexpected_exit(spec, &output));
    }
    let text = read_artifact(&artifacts.join("gamma-report.json"), "cargo gamma run")?;
    let tally = report::parse(&text).map_err(GateOutcome::error)?;
    let missed = artifacts.join("missed.txt");
    let mut lines = tally.findings.join("\n");
    if !lines.is_empty() {
        lines.push('\n');
    }
    std::fs::write(&missed, lines).map_err(|error| {
        GateOutcome::error(format!("could not write {}: {error}", missed.display()))
    })?;
    Ok(tally)
}

/// Turns a tally into a verdict.
///
/// Everything no test assertion rejected is missed: a survivor, and also a
/// timeout, an out-of-memory and an uncovered mutant. A timeout is not a kill;
/// the same mutant can time out on a loaded machine and survive on an idle
/// one, so it is counted against the change and listed for someone to make
/// killable by a bounded test.
pub fn outcome_of(scope: &Scope, tally: &Tally) -> GateOutcome {
    let where_ = scope.label();
    if tally.total == 0 {
        return GateOutcome::skipped(format!("gamma reported no mutant in {where_}"));
    }
    if tally.scored() == 0 {
        return GateOutcome::skipped(format!(
            "no mutant in {where_} produced a verdict ({} unviable, {} excluded)",
            tally.unviable, tally.excluded
        ));
    }
    if tally.missed > 0 {
        return GateOutcome::fail(format!(
            "{} mutant(s) missed in {where_} ({} survived, {} timed out, {} out of memory, \
             {} uncovered); see target/quality/{ARTIFACTS}/missed.txt ({} caught, {} unviable, \
             {} flaky)",
            tally.missed,
            tally.survived,
            tally.timeout,
            tally.out_of_memory,
            tally.uncovered,
            tally.caught,
            tally.unviable,
            tally.flaky
        ));
    }
    GateOutcome::pass(format!(
        "{} mutant(s) caught in {where_}, none missed ({} unviable, {} excluded, {} flaky)",
        tally.caught, tally.unviable, tally.excluded, tally.flaky
    ))
}

/// Records `HEAD` as the last green full sweep, when the tree was clean.
///
/// A sweep measures the working tree, so it vouches for a commit only when the
/// working tree *was* that commit. A dirty tree is reported, not recorded.
fn record_full_sweep(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    outcome: GateOutcome,
) -> GateOutcome {
    let detail = outcome.detail().to_owned();
    let unrecorded = |why: String| {
        GateOutcome::pass(format!(
            "{detail}; not recorded as the last green full sweep: {why}"
        ))
    };
    let status = match runner.run(&status_spec(&ctx.workspace_root)) {
        Ok(status) if status.success() => status,
        Ok(status) => {
            return unrecorded(format!("`git status` failed: {}", status.combined().trim()));
        }
        Err(error) => return unrecorded(error),
    };
    if !status.stdout.trim().is_empty() {
        return unrecorded("the working tree had uncommitted changes".to_owned());
    }
    let head = match runner.run(&head_spec(&ctx.workspace_root)) {
        Ok(head) if head.success() && !head.stdout.trim().is_empty() => {
            head.stdout.trim().to_owned()
        }
        _ => return unrecorded("`git rev-parse HEAD` named no commit".to_owned()),
    };
    let marker = ctx.log_dir.join(LAST_FULL_SWEEP);
    match std::fs::write(&marker, format!("{head}\n")) {
        Ok(()) => GateOutcome::pass(format!(
            "{detail}; recorded {} as the last green full sweep",
            head.get(..12).unwrap_or(&head)
        )),
        Err(error) => unrecorded(format!("could not write {}: {error}", marker.display())),
    }
}

/// The command that says whether tracked files differ from `HEAD`.
pub fn status_spec(root: &Path) -> CommandSpec {
    CommandSpec::new(
        "git",
        &["status", "--porcelain", "--untracked-files=no"],
        root,
    )
}

/// The commit recorded by the last green full sweep, if there is one.
pub fn last_full_sweep(ctx: &GateContext) -> Option<String> {
    let text = std::fs::read_to_string(ctx.log_dir.join(LAST_FULL_SWEEP)).ok()?;
    let commit = text.trim();
    (!commit.is_empty()).then(|| commit.to_owned())
}

/// Refuses to start a mutation run on a volume too full to hold one.
fn check_disk(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<(), String> {
    let spec = free_space_spec(&ctx.workspace_root);
    let output = runner.run(&spec).map_err(|error| error.to_string())?;
    if !output.success() {
        return Err(format!(
            "`{}` failed, so free space could not be checked before mutating: {}",
            spec.display(),
            output.combined().trim()
        ));
    }
    enough_disk(parse_free_bytes(&output.stdout)?)
}

/// The command that reports free space on the workspace's volume.
///
/// `-P` is POSIX portable format — one line per filesystem, fixed columns —
/// and `-k` fixes the unit at 1024-byte blocks, so the output parses the same
/// on macOS and Linux.
pub fn free_space_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("df", &["-P", "-k", "."], root)
}

/// Free bytes on the volume, read from `df -P -k` output.
///
/// # Errors
///
/// Returns a message when the output does not carry an `Available` column this
/// gate can read. An unreadable measurement is refused rather than assumed
/// healthy: a guard that shrugs is a guard that is not there.
pub fn parse_free_bytes(text: &str) -> Result<u64, String> {
    let line = text
        .lines()
        .rfind(|line| !line.trim().is_empty())
        .ok_or_else(|| "`df` printed nothing, so free space could not be checked".to_owned())?;
    let kib: u64 = line
        .split_whitespace()
        .nth(3)
        .ok_or_else(|| format!("`df` printed no Available column to read: `{line}`"))?
        .parse()
        .map_err(|_| format!("`df`'s Available column is not a number: `{line}`"))?;
    Ok(kib * 1024)
}

/// Whether `free` bytes of disk is enough to start a mutation run.
///
/// # Errors
///
/// Returns a message naming the threshold, the observed free space, and the
/// usual places the missing gigabytes are hiding.
pub fn enough_disk(free: u64) -> Result<(), String> {
    if free >= MIN_FREE_BYTES {
        return Ok(());
    }
    Err(format!(
        "only {} GiB free on the workspace's volume, below the {} GiB a mutation run needs. \
         cargo-gamma keeps a copy of the workspace and a whole instrumented build of it in its \
         cache (~/.cache/cargo-gamma/<workspace>, or {CACHE_ENV}). The usual culprits: worktree \
         target/ directories, target/llvm-cov-target, and gamma caches of worktrees that no \
         longer exist (`cargo gamma clean` in a workspace removes its own)",
        gib(free),
        MIN_FREE_BYTES / GIB
    ))
}

/// Bytes rendered as GiB, to one decimal place.
fn gib(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / GIB as f64)
}

/// The command that finds where this branch left `main`.
pub fn merge_base_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["merge-base", "HEAD", TRUNK], root)
}

/// The command that produces the diff the run is scoped to.
pub fn diff_spec(root: &Path, base: &str) -> CommandSpec {
    CommandSpec::owned(
        "git",
        vec!["diff".to_owned(), "--no-color".to_owned(), base.to_owned()],
        root,
    )
}

/// The command that names the current commit.
pub fn head_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["rev-parse", "HEAD"], root)
}

/// The command that names the commit this one was built on.
///
/// For a merge commit that is the trunk it landed on, so the diff against it is
/// what the merge brought in.
pub fn first_parent_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["rev-parse", "HEAD^1"], root)
}

/// Where the narrowed diff is written for gamma to read.
pub fn diff_path(ctx: &GateContext) -> PathBuf {
    ctx.log_dir.join("mutants.diff")
}

/// A scope to measure, or the reason there is nothing to.
enum Scoped {
    Measure(Scope),
    Nothing(String),
}

/// Decides what the run covers.
///
/// Falling back to the whole tree when there is no baseline is the *stricter*
/// choice, so an absent `main` or an empty diff can never quietly shrink the
/// gate to nothing. A diff that changes nothing gamma can mutate — tests, docs,
/// build scripts — is different: there is a change, and nothing in it to
/// measure, which is `skipped`.
fn scope(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    mutable: &BTreeSet<String>,
) -> Result<Scoped, GateOutcome> {
    if ctx.full {
        return Ok(Scoped::Measure(Scope::FullTree));
    }
    let (base, since) = if ctx.incremental {
        match last_full_sweep(ctx) {
            Some(commit) => (commit.clone(), Since::LastFullSweep(commit)),
            None => return Ok(Scoped::Measure(Scope::FullTree)),
        }
    } else {
        match baseline(runner, &ctx.workspace_root)? {
            Some(base) => (base, Since::BranchPoint),
            None => return Ok(Scoped::Measure(Scope::FullTree)),
        }
    };
    let spec = diff_spec(&ctx.workspace_root, &base);
    let raw = invoke(runner, &spec)?;
    if !raw.success() {
        return Err(unexpected_exit(&spec, &raw));
    }
    let path = diff_path(ctx);
    let scope = Scope::Diff {
        path: path.clone(),
        since,
    };
    if raw.stdout.trim().is_empty() {
        return Ok(match scope {
            Scope::Diff {
                since: Since::LastFullSweep(_),
                ..
            } => Scoped::Nothing(format!("nothing changed: {} is empty", scope.label())),
            _ => Scoped::Measure(Scope::FullTree),
        });
    }
    let narrowed = diff::mutable_sections(&raw.stdout, mutable);
    if narrowed.is_empty() {
        return Ok(Scoped::Nothing(format!(
            "nothing mutable in {}: the diff touches only tests, benches, examples, build \
             scripts, docs or excluded files, none of which gamma mutates",
            scope.label()
        )));
    }
    write_diff(&path, &narrowed)?;
    Ok(Scoped::Measure(scope))
}

/// What the change under test is measured against.
///
/// On a branch that is the merge-base with `main`, so the scope is the branch's
/// own work. On `main` the merge-base *is* `HEAD`, which would leave nothing to
/// compare against, so the baseline becomes `HEAD`'s first parent — for a merge
/// commit that is precisely what the merge brought in.
fn baseline(runner: &dyn CommandRunner, root: &Path) -> Result<Option<String>, GateOutcome> {
    let merge_base = single_revision(runner, &merge_base_spec(root))?;
    let head = single_revision(runner, &head_spec(root))?;
    match (merge_base, head) {
        // Not on trunk: the branch point.
        (Some(merge_base), Some(head)) if merge_base != head => Ok(Some(merge_base)),
        // On trunk: what the tip commit introduced.
        (Some(_), Some(_)) => single_revision(runner, &first_parent_spec(root)),
        // No `main`, or no commits at all.
        _ => Ok(None),
    }
}

/// Resolves one revision, or `None` when git cannot name it.
fn single_revision(
    runner: &dyn CommandRunner,
    spec: &CommandSpec,
) -> Result<Option<String>, GateOutcome> {
    let output = invoke(runner, spec)?;
    if !output.success() {
        return Ok(None);
    }
    let revision = output.stdout.trim().to_owned();
    Ok((!revision.is_empty()).then_some(revision))
}

fn write_diff(path: &Path, diff: &str) -> Result<(), GateOutcome> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            GateOutcome::error(format!("could not create {}: {err}", parent.display()))
        })?;
    }
    std::fs::write(path, diff)
        .map_err(|err| GateOutcome::error(format!("could not write {}: {err}", path.display())))
}

#[cfg(test)]
mod tests;
