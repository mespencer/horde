//! Mutation testing: cargo-mutants, zero missed mutants.
//!
//! Coverage says a line ran; a mutant says the line's behaviour was *checked*.
//! Agent-written tests are unusually good at the first and unusually bad at the
//! second — asserting that a call returned `Ok`, that a vector is non-empty,
//! that no panic occurred. Every one of those passes when the code under it is
//! quietly wrong, and every one of them is exactly what a surviving mutant
//! points at. A mutation that stays green is a finding, not a relief.

use std::path::{Path, PathBuf};

use crate::command::CommandRunner;
use crate::gates::prelude::*;
use crate::gates::{invoke, labelled_count, read_output, unexpected_exit};

/// The cargo-mutants version the gate is calibrated against.
pub const PIN: ToolPin = ToolPin {
    program: "cargo",
    version_args: &["mutants", "--version"],
    expected: "27.1.0",
    install: "cargo install --locked cargo-mutants@27.1.0",
};

/// Branch the diff is taken against.
pub const TRUNK: &str = "main";

/// How many mutants to build and test at once.
///
/// Almost all of a mutant's cost is rebuilding the crate, which is largely
/// single-threaded, so running several at once is close to a linear speed-up.
/// A gate nobody can afford to run is a gate nobody runs. This changes only how
/// long the measurement takes, never what it measures; the timeouts in
/// `.cargo/mutants.toml` are generous enough that the extra load does not turn
/// into spurious ones.
pub const JOBS: &str = "4";

/// The cargo profile mutation builds use, defined in the workspace manifest.
///
/// `[profile.mutants]` inherits `test` and turns debuginfo off. Every mutant
/// pays an incremental rebuild and relink of the mutated package's test
/// binaries, debuginfo is the bulk of that link cost, and nothing in a
/// mutation run reads it. Measured on one horde-server shard
/// (`--shard 0/32 --jobs 4`, idle machine): 116.1s wall without the profile,
/// 97.0s with it — a 1.20x speed-up, with an identical verdict both ways
/// (23 caught, 1 unviable). This changes only how long the measurement
/// takes, never what it measures.
pub const PROFILE: &str = "mutants";

/// Where cargo-mutants' configuration, and so its exclusions, live.
pub const CONFIG: &str = ".cargo/mutants.toml";

/// One binary GiB, in bytes.
const GIB: u64 = 1024 * 1024 * 1024;

/// Free space below which a mutation run is refused rather than started.
///
/// A run at `--jobs 4` clones a ~2–3 GB scratch tree per job under the system
/// temp directory and grows each clone's incremental `target/` from there; two
/// full sweeps have died of disk exhaustion mid-run, and a near-full APFS
/// volume also silently serialized a jobs=4 run to ~1.1× parallelism before
/// anything failed. Below this line the measurement cannot safely run, so the
/// gate reports a broken instrument instead of starting it.
pub const MIN_FREE_BYTES: u64 = 40 * GIB;

/// Which mutants a run covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// Only code changed since the merge-base with `main`.
    Diff(PathBuf),
    /// Every mutable function in the tree.
    FullTree,
}

impl Scope {
    /// How the scope is described in the gate's detail line.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Diff(_) => "changed code",
            Self::FullTree => "the whole tree",
        }
    }
}

/// Runs cargo-mutants and requires that nothing survived.
pub fn gate() -> PinnedGate {
    PinnedGate {
        pin: &PIN,
        nothing_to_measure: None,
        measure: mutate,
    }
}

/// Scopes the run, then mutates.
fn mutate(ctx: &GateContext, runner: &dyn CommandRunner) -> GateOutcome {
    if let Err(reason) = check_disk(ctx, runner) {
        return GateOutcome::error(reason);
    }
    if let Err(reason) = check_exclusions(ctx, runner) {
        return GateOutcome::error(reason);
    }
    let scope = match scope(ctx, runner) {
        Ok(scope) => scope,
        Err(outcome) => return outcome,
    };
    let spec = spec(ctx, &scope);
    read_output(runner, &spec, |output| {
        let text = output.combined();
        if let Some(reason) = nothing_to_mutate(&text, &scope) {
            return GateOutcome::skipped(reason);
        }
        match parse_summary(&text) {
            None => unexpected_exit(&spec, output),
            Some(counts) => outcome_of(&scope, counts),
        }
    })
}

/// One way cargo-mutants says a run found no work, and how the row says it.
struct NothingToMutate {
    /// What cargo-mutants printed.
    marker: &'static str,
    /// How the row describes it, before the scope is named.
    reason: &'static str,
    /// What is worth adding after the scope, if anything.
    note: &'static str,
}

/// Why cargo-mutants had nothing to do, when it ran fine and had nothing.
///
/// The work can run out at four different stages, and cargo-mutants words each
/// differently: no Rust in the diff at all, no mutants generated, none left
/// after filtering, none on the changed lines. Every one of them is the same
/// fact to this gate — the run completed and there was nothing to measure.
///
/// `No mutants were viable` reads like a fifth and is deliberately absent.
/// cargo-mutants' own wording continues "perhaps there is a problem with
/// building in a scratch directory", which is the instrument failing rather
/// than a change with nothing in it. That stays an `error`, and
/// [`a_build_that_produced_no_viable_mutants_is_still_an_error`] holds the line
/// — the two messages are one word apart, which is exactly how a broken
/// instrument would come to be reported as a clean sheet.
const NOTHING_TO_MUTATE: &[NothingToMutate] = &[
    NothingToMutate {
        marker: "changes no Rust source files",
        reason: "no Rust changed in",
        note: "",
    },
    NothingToMutate {
        marker: "No mutants to filter",
        reason: "nothing mutable in",
        note: TESTS_ARE_NOT_MUTATED,
    },
    NothingToMutate {
        marker: "diff matched no mutants",
        reason: "nothing mutable in",
        note: TESTS_ARE_NOT_MUTATED,
    },
    NothingToMutate {
        marker: "No mutants found under the active filters",
        reason: "nothing mutable in",
        note: "",
    },
    NothingToMutate {
        marker: "No mutants were generated",
        reason: "nothing mutable in",
        note: "",
    },
];

/// Why a diff full of Rust can still produce no mutants at all.
const TESTS_ARE_NOT_MUTATED: &str =
    " (cargo-mutants does not mutate #[cfg(test)] code, so a tests-only change generates none)";

/// Why there was nothing to mutate, if there was nothing.
///
/// A change that touches no Rust — documentation, a proto file, a config —
/// generates no mutants and cargo-mutants prints no tally. Neither does a
/// change that touches plenty of Rust but only *test* Rust: cargo-mutants does
/// not mutate `#[cfg(test)]` code or `tests/`, so a diff that is all test files
/// is a diff with nothing mutable in it. Both are `skipped`, not `error`:
/// nothing was measured because there was nothing to measure, which is a
/// different fact from an instrument that broke, and the row has to say which.
///
/// Neither is a `pass` either. A zero has to say which of its two meanings it
/// has, and "no mutant survived because none was ever built" must never read as
/// "this change is covered".
pub fn nothing_to_mutate(text: &str, scope: &Scope) -> Option<String> {
    NOTHING_TO_MUTATE
        .iter()
        .find(|nothing| text.contains(nothing.marker))
        .map(|nothing| format!("{} {}{}", nothing.reason, scope.label(), nothing.note))
}

/// Refuses to start a mutation run on a volume too full to hold one.
///
/// This pre-flight belongs to the mutants gate alone: the fast gates neither
/// clone the tree nor build anything a normal `cargo test` would not, so a
/// guard there would only teach people that gates red for reasons unrelated to
/// their measurement. A failure here is an `error`, never a `fail` — the code
/// was not judged; the instrument had nowhere to run.
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
/// The decision is a pure function of one number so the threshold can be
/// exercised exactly — at the line, one byte under it, and well clear of it —
/// without a nearly-full volume to stage it on.
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
         Each of the {JOBS} parallel jobs clones a ~2-3 GB scratch tree (cargo-mutants-* under \
         the system temp directory, /var/folders on macOS) and grows its own incremental \
         target/ from there; a near-full volume has also silently serialized a --jobs {JOBS} \
         run to ~1.1x parallelism. The usual culprits: worktree target/ directories, \
         target/llvm-cov-target, and stale cargo-mutants-* scratch trees",
        gib(free),
        MIN_FREE_BYTES / GIB
    ))
}

/// Bytes rendered as GiB, to one decimal place.
fn gib(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / GIB as f64)
}

/// Confirms the configured exclusions are real and took effect.
///
/// Costs nothing when there are none, which is the usual case: the listing runs
/// only happen once something is excluded.
///
/// Two listings are taken, one with the configuration and one without. Their
/// difference is exactly what the exclusions removed, which is the only way to
/// ask the question that matters — did each entry remove what it claims, and
/// nothing else? — without reimplementing cargo-mutants' regex matching and
/// hoping the imitation agrees.
fn check_exclusions(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<(), String> {
    let config = ctx.workspace_root.join(CONFIG);
    let Ok(text) = std::fs::read_to_string(&config) else {
        return Ok(());
    };
    let entries = configured_exclusions(&text)?;
    if entries.is_empty() {
        return Ok(());
    }
    let listed = listing(runner, &list_spec(ctx))?;
    let unexcluded = listing(runner, &unexcluded_list_spec(ctx))?;
    let removed: Vec<String> = unexcluded
        .into_iter()
        .filter(|mutant| !listed.contains(mutant))
        .collect();
    let root = ctx.workspace_root.clone();
    verify_exclusions(&entries, &listed, &removed, |path| root.join(path).exists())
}

/// The mutants one listing run names.
fn listing(runner: &dyn CommandRunner, spec: &CommandSpec) -> Result<Vec<String>, String> {
    let output = runner.run(spec).map_err(|error| error.to_string())?;
    if !output.success() {
        return Err(format!(
            "`{}` failed, so the exclusions could not be checked: {}",
            spec.display(),
            output.combined().trim()
        ));
    }
    Ok(output.stdout.lines().map(str::to_owned).collect())
}

/// The command that finds where this branch left `main`.
pub fn merge_base_spec(root: &Path) -> CommandSpec {
    CommandSpec::new("git", &["merge-base", "HEAD", TRUNK], root)
}

/// The command that produces the diff cargo-mutants will be scoped to.
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

/// Where the diff is written for cargo-mutants to read.
pub fn diff_path(ctx: &GateContext) -> PathBuf {
    ctx.log_dir.join("mutants.diff")
}

/// Decides whether to mutate the diff or the whole tree.
///
/// Falling back to the whole tree is the *stricter* choice, so an absent `main`
/// or an empty diff can never quietly shrink the gate to nothing.
fn scope(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<Scope, GateOutcome> {
    if ctx.full {
        return Ok(Scope::FullTree);
    }
    let Some(base) = baseline(ctx, runner)? else {
        return Ok(Scope::FullTree);
    };
    let spec = diff_spec(&ctx.workspace_root, &base);
    let diff_output = invoke(runner, &spec)?;
    if !diff_output.success() {
        return Err(unexpected_exit(&spec, &diff_output));
    }
    if diff_output.stdout.trim().is_empty() {
        return Ok(Scope::FullTree);
    }
    let path = diff_path(ctx);
    write_diff(&path, &diff_output.stdout)?;
    Ok(Scope::Diff(path))
}

/// What the change under test is measured against.
///
/// On a branch that is the merge-base with `main`, so the scope is the branch's
/// own work. On `main` the merge-base *is* `HEAD`, which would leave nothing to
/// compare against, so the baseline becomes `HEAD`'s first parent — for a merge
/// commit that is precisely what the merge brought in, and for a plain commit
/// it is that commit. Either way the question is the same one the branch
/// answered: does *this change* leave behaviour no test checks?
///
/// The whole tree remains available, but by asking (`--full`) rather than by
/// accident. That distinction matters: a scope that only ever looks at diffs
/// never re-examines old code, which is exactly how 92 surviving mutants sat
/// unnoticed in crates that had never been measured.
fn baseline(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<Option<String>, GateOutcome> {
    let merge_base = single_revision(ctx, runner, &merge_base_spec(&ctx.workspace_root))?;
    let head = single_revision(ctx, runner, &head_spec(&ctx.workspace_root))?;
    match (merge_base, head) {
        // Not on trunk: the branch point.
        (Some(merge_base), Some(head)) if merge_base != head => Ok(Some(merge_base)),
        // On trunk: what the tip commit introduced.
        (Some(_), Some(_)) => single_revision(ctx, runner, &first_parent_spec(&ctx.workspace_root)),
        // No `main`, or no commits at all.
        _ => Ok(None),
    }
}

/// Resolves one revision, or `None` when git cannot name it.
fn single_revision(
    ctx: &GateContext,
    runner: &dyn CommandRunner,
    spec: &CommandSpec,
) -> Result<Option<String>, GateOutcome> {
    let _ = ctx;
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

/// The command this gate runs.
///
/// Timeouts and exclusions live in the committed `.cargo/mutants.toml`, which
/// cargo-mutants reads itself.
pub fn spec(ctx: &GateContext, scope: &Scope) -> CommandSpec {
    let mut args = vec![
        "mutants".to_owned(),
        "--no-shuffle".to_owned(),
        "--colors".to_owned(),
        "never".to_owned(),
        "--jobs".to_owned(),
        JOBS.to_owned(),
        format!("--profile={PROFILE}"),
        "--output".to_owned(),
        ctx.log_dir.display().to_string(),
    ];
    if let Scope::Diff(path) = scope {
        args.push("--in-diff".to_owned());
        args.push(path.display().to_string());
    }
    for package in &ctx.packages {
        args.push("--package".to_owned());
        args.push(package.clone());
    }
    CommandSpec::owned("cargo", args, &ctx.workspace_root)
}

/// One `exclude_re` entry, with the comment that justifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exclusion {
    /// The regex, exactly as cargo-mutants will see it.
    pub pattern: String,
    /// The comment written above or beside it.
    pub why: String,
}

/// The `exclude_re` entries configured for cargo-mutants.
///
/// # Errors
///
/// Returns a message when the key is present but unreadable. A configuration
/// this gate cannot read is an instrument it cannot vouch for, so it refuses
/// rather than assuming there are no exclusions.
pub fn configured_exclusions(config: &str) -> Result<Vec<Exclusion>, String> {
    let Some(body) = exclusion_body(config)? else {
        return Ok(Vec::new());
    };
    Ok(entries(&body))
}

/// The body of the `exclude_re` array, comments and all.
///
/// Comments are kept, rather than stripped as they used to be, because an
/// equivalence claim is worth no more than the reason written beside it. That
/// means the closing `]` has to be found the way TOML finds it — in code, not
/// inside a comment that happens to mention one.
fn exclusion_body(config: &str) -> Result<Option<String>, String> {
    let Some(start) = config
        .lines()
        .position(|line| is_exclusion_key(line.trim()))
    else {
        return Ok(None);
    };
    let tail: String = config.lines().skip(start).collect::<Vec<_>>().join("\n");
    let opened = tail.split_once('[').map_or("", |(_, rest)| rest);
    let mut body = String::new();
    for line in opened.lines() {
        let (code, comment) = split_comment(line);
        let Some((last, _)) = code.split_once(']') else {
            body.push_str(line);
            body.push('\n');
            continue;
        };
        // A comment after the closing bracket still explains the entry beside
        // it, so it travels with the body rather than being dropped.
        body.push_str(last);
        if let Some(comment) = comment {
            body.push_str(" #");
            body.push_str(comment);
        }
        return Ok(Some(body));
    }
    Err(format!("{CONFIG}: `exclude_re` is missing its closing `]`"))
}

/// A line split into its TOML code and the comment trailing it.
///
/// A `#` inside a quoted string is content rather than a comment, so the split
/// tracks which quote is open — the same care [`toml_strings`] takes over
/// quoting styles, for the same reason.
fn split_comment(line: &str) -> (&str, Option<&str>) {
    let mut quote: Option<char> = None;
    for (at, character) in line.char_indices() {
        match (quote, character) {
            (Some(open), _) if open == character => quote = None,
            (Some(_), _) => {}
            (None, '"' | '\'') => quote = Some(character),
            (None, '#') => return (&line[..at], Some(line[at + 1..].trim())),
            (None, _) => {}
        }
    }
    (line, None)
}

/// The entries in an array body, each carrying the comment that explains it.
///
/// A comment above an entry belongs to it, and so does one beside it. Comment
/// lines accumulate until an entry claims them, which is how a reason spanning
/// several lines stays attached to the one pattern it argues for.
fn entries(body: &str) -> Vec<Exclusion> {
    let mut found = Vec::new();
    let mut pending: Vec<&str> = Vec::new();
    for line in body.lines() {
        let (code, comment) = split_comment(line);
        let patterns = toml_strings(code);
        if patterns.is_empty() {
            pending.extend(comment);
            continue;
        }
        let why = pending
            .iter()
            .copied()
            .chain(comment)
            .collect::<Vec<_>>()
            .join(" ");
        found.extend(patterns.into_iter().map(|pattern| Exclusion {
            pattern,
            why: why.clone(),
        }));
        pending.clear();
    }
    found
}

fn is_exclusion_key(line: &str) -> bool {
    // A commented-out key does not start with the key, so `#` needs no test of
    // its own; the caller trims first.
    let Some(rest) = line.strip_prefix("exclude_re") else {
        return false;
    };
    rest.trim_start().starts_with('=')
}

/// The TOML strings in an array body, in either quoting style.
///
/// A regex in TOML wants a literal string — `'stub\\.rs'` — because a basic
/// string would need the backslash doubled. Both are read, and a basic string's
/// `\\\\` is folded back to one backslash so the pattern is what cargo-mutants
/// will see rather than what TOML spelled.
fn toml_strings(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = text;
    // `find` over a character set already means "whichever quote comes first".
    while let Some(open) = rest.find(['"', '\'']) {
        let Some(quote) = rest.get(open..).and_then(|tail| tail.chars().next()) else {
            break;
        };
        let Some(after) = rest.get(open + 1..) else {
            break;
        };
        let Some(close) = after.find(quote) else {
            break;
        };
        if let Some(value) = after.get(..close) {
            found.push(if quote == '"' {
                value.replace("\\\\", "\\")
            } else {
                value.to_owned()
            });
        }
        rest = after.get(close + 1..).unwrap_or("");
    }
    found
}

/// What an exclusion takes out of scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A file, whose every mutant is out of scope.
    File(String),
    /// One function, whose mutant is provably equivalent to the original.
    Function(String),
}

impl Target {
    /// Whether this mutant is one the exclusion claims.
    ///
    /// A file claims what is written in it. A function claims mutants that name
    /// *it*, not ones that merely spell its letters somewhere: `none` does not
    /// name `announced.is_none()`. That distinction is what stops an over-broad
    /// entry from adopting the mutants it removed by accident — they surface as
    /// unclaimed instead.
    pub fn claims(&self, mutant: &str) -> bool {
        match self {
            Self::File(path) => mutant.contains(path),
            Self::Function(name) => names_function(mutant, name),
        }
    }
}

/// Whether a mutant's description names this function, rather than one whose
/// name contains it.
pub fn names_function(mutant: &str, function: &str) -> bool {
    mutant.match_indices(function).any(|(at, _)| {
        let before = mutant[..at].chars().next_back();
        let after = mutant[at + function.len()..].chars().next();
        !before.is_some_and(part_of_a_path) && !after.is_some_and(part_of_a_path)
    })
}

fn part_of_a_path(character: char) -> bool {
    character.is_alphanumeric() || character == '_' || character == ':'
}

/// What an exclusion pattern names.
///
/// Only two shapes are allowed, and both can be checked by looking rather than
/// by reimplementing a regex engine and hoping the imitation agrees with
/// cargo-mutants' own:
///
/// * a plain path, with `\.` for a dot, which excludes a file;
/// * a plain Rust path — `Capabilities::none` — which excludes one function.
///
/// # Errors
///
/// Returns a message when the pattern is neither.
pub fn target(pattern: &str) -> Result<Target, String> {
    let literal = literal_path(pattern)?;
    if literal.contains('/') || literal.ends_with(".rs") {
        return Ok(Target::File(literal));
    }
    if is_rust_path(&literal) {
        return Ok(Target::Function(literal));
    }
    Err(shape_refusal(pattern))
}

/// Whether this is an identifier, or several joined by `::`.
fn is_rust_path(literal: &str) -> bool {
    !literal.is_empty()
        && literal.split("::").all(|segment| {
            !segment.is_empty()
                && segment
                    .chars()
                    .all(|character| character.is_alphanumeric() || character == '_')
        })
}

/// The literal text an exclusion pattern matches, with `\.` read as a dot.
///
/// # Errors
///
/// Returns a message when the pattern carries regex syntax, which would make
/// what it excludes a question only a regex engine could answer.
pub fn literal_path(pattern: &str) -> Result<String, String> {
    let mut literal = String::with_capacity(pattern.len());
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' if characters.next() == Some('.') => literal.push('.'),
            '\\' => return Err(shape_refusal(pattern)),
            '*' | '?' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                return Err(shape_refusal(pattern));
            }
            other => literal.push(other),
        }
    }
    Ok(literal)
}

fn shape_refusal(pattern: &str) -> String {
    format!(
        "{CONFIG}: `{pattern}` is neither a plain path nor a function name. An exclusion \
         must name one path, with `\\.` for a dot, or one function such as \
         `Capabilities::none`, so the gate can check what it actually removed"
    )
}

/// Confirms every exclusion names something, removed it, and removed only it.
///
/// Four ways an exclusion lies, and all four are checked:
///
/// * *It names nothing.* An exclusion for a path no longer in the tree is
///   indistinguishable from a live one, so it silently misstates what is being
///   measured — the same reason the module-cycle baseline ratchets both ways.
/// * *It claims equivalence without arguing it.* A function is excluded only
///   when the mutated expression is identical to the original, and that is an
///   argument, not an observation. It has to be written down.
/// * *It was configured but had no effect.* cargo-mutants matches `exclude_re`
///   against a mutant's description, and an anchoring mistake makes an
///   exclusion quietly do nothing; the mutants it was meant to skip are tested
///   anyway. Configured is not the same as applied, so this checks applied.
/// * *It removed something it does not name.* The two listings differ by
///   exactly what the configuration took out, so anything missing that no entry
///   claims means the configuration is quietly wider than it reads.
///
/// # Errors
///
/// Returns a message naming the exclusion and which of the four it is.
pub fn verify_exclusions(
    entries: &[Exclusion],
    listed: &[String],
    removed: &[String],
    exists: impl Fn(&str) -> bool,
) -> Result<(), String> {
    let targets = entries
        .iter()
        .map(|entry| target(&entry.pattern))
        .collect::<Result<Vec<_>, _>>()?;
    for (entry, target) in entries.iter().zip(&targets) {
        check_entry(entry, target, listed, removed, &exists)?;
    }
    unclaimed(removed, &targets)
}

/// Everything one entry has to prove about itself.
fn check_entry(
    entry: &Exclusion,
    target: &Target,
    listed: &[String],
    removed: &[String],
    exists: &impl Fn(&str) -> bool,
) -> Result<(), String> {
    let pattern = &entry.pattern;
    match target {
        Target::File(path) if !exists(path) => {
            return Err(format!(
                "{CONFIG}: `{pattern}` excludes {path}, which is not in the tree — \
                 an exclusion that names nothing misstates what is measured"
            ));
        }
        Target::Function(name) if entry.why.trim().is_empty() => {
            return Err(format!(
                "{CONFIG}: `{pattern}` excludes the function {name} with no reason beside it. \
                 A function is excluded only when its mutant is provably equivalent — the \
                 mutated expression is identical to the original, so no test could tell them \
                 apart — and that argument belongs in a comment, not in someone's memory"
            ));
        }
        _ => {}
    }
    if let Some(mutant) = listed.iter().find(|mutant| target.claims(mutant)) {
        return Err(format!(
            "{CONFIG}: `{pattern}` did not take — cargo-mutants still lists `{mutant}`. \
             Configured is not applied"
        ));
    }
    let claimed = removed
        .iter()
        .filter(|mutant| target.claims(mutant))
        .count();
    if claimed == 0 {
        return Err(format!(
            "{CONFIG}: `{pattern}` removed no mutant at all — an exclusion nothing depends on \
             reads exactly like one that is holding something back"
        ));
    }
    if let Target::Function(name) = target
        && claimed > 1
    {
        return Err(format!(
            "{CONFIG}: `{pattern}` removed {claimed} mutants of {name}, and equivalence is a \
             claim about one mutated expression. If the function grew, the argument has to be \
             made again for what grew rather than widened to cover it"
        ));
    }
    Ok(())
}

/// Refuses a difference between the listings that no entry accounts for.
///
/// An over-broad pattern removes mutants it does not name, and so does a
/// configuration key this gate does not read. Both look identical from inside
/// the excluded listing, and both are caught here.
fn unclaimed(removed: &[String], targets: &[Target]) -> Result<(), String> {
    let Some(mutant) = removed
        .iter()
        .find(|mutant| !targets.iter().any(|target| target.claims(mutant)))
    else {
        return Ok(());
    };
    Err(format!(
        "{CONFIG}: `{mutant}` was excluded, but no entry names it — the configuration takes \
         more out of scope than it says"
    ))
}

/// The command that lists the mutants a run would test, without testing them.
pub fn list_spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec!["mutants".to_owned(), "--list".to_owned()],
        &ctx.workspace_root,
    )
}

/// The same listing with the configuration ignored.
///
/// What this listing has and the configured one does not is precisely what the
/// exclusions removed — measured, rather than inferred from the patterns.
pub fn unexcluded_list_spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec![
            "mutants".to_owned(),
            "--list".to_owned(),
            "--no-config".to_owned(),
        ],
        &ctx.workspace_root,
    )
}

/// What a cargo-mutants run concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MutantCounts {
    /// Mutants no test noticed.
    pub missed: usize,
    /// Mutants some test failed on.
    pub caught: usize,
    /// Mutants that did not compile.
    pub unviable: usize,
    /// Mutants whose test run hung.
    pub timeout: usize,
}

impl MutantCounts {
    /// Mutants that were built and tested.
    pub fn tested(self) -> usize {
        self.missed + self.caught + self.timeout
    }
}

/// Reads cargo-mutants' final tally.
///
/// Returns `None` when there is no tally, which means the run did not finish —
/// an instrument error, not a clean sheet.
pub fn parse_summary(text: &str) -> Option<MutantCounts> {
    let line = text.lines().find(|line| line.contains("mutants tested"))?;
    let tokens: Vec<&str> = line.split_whitespace().collect();
    Some(MutantCounts {
        missed: labelled_count(&tokens, "missed").unwrap_or(0),
        caught: labelled_count(&tokens, "caught").unwrap_or(0),
        unviable: labelled_count(&tokens, "unviable").unwrap_or(0),
        timeout: labelled_count(&tokens, "timeouts")
            .or_else(|| labelled_count(&tokens, "timeout"))
            .unwrap_or(0),
    })
}

/// Turns a tally into a verdict.
///
/// A timeout counts as caught — the mutant changed behaviour enough to hang the
/// suite — but the number is reported so a creeping timeout count is visible
/// rather than hidden inside a green row.
pub fn outcome_of(scope: &Scope, counts: MutantCounts) -> GateOutcome {
    let where_ = scope.label();
    if counts.missed > 0 {
        return GateOutcome::fail(format!(
            "{} mutant(s) survived in {where_}; see mutants.out/missed.txt ({} caught, {} unviable)",
            counts.missed, counts.caught, counts.unviable
        ));
    }
    if counts.tested() == 0 {
        return GateOutcome::skipped(format!(
            "no viable mutants in {where_} ({} unviable)",
            counts.unviable
        ));
    }
    let timeouts = match counts.timeout {
        0 => String::new(),
        count => format!(", {count} timed out"),
    };
    GateOutcome::pass(format!(
        "{} mutant(s) caught in {where_}, none survived ({} unviable{timeouts})",
        counts.caught, counts.unviable
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::CommandOutput;
    use crate::testing::{ScriptedRunner, ctx_at, exited, failed, repo_ctx, run_gate};

    const CLEAN: &str = include_str!("../../fixtures/cargo-mutants-clean.txt");

    /// Real `df -P -k` output from the machine this gate was written on, with
    /// 75 GiB available — comfortably over the threshold.
    const DF: &str = include_str!("../../fixtures/df-portable.txt");

    /// The same shape with 5 GiB available. A genuinely full volume cannot be
    /// captured without filling one, so the dirty case varies only the number.
    const DF_NEARLY_FULL: &str = "Filesystem   1024-blocks      Used Available Capacity  Mounted on\n\
         /dev/disk3s5   971350180 966107300   5242880    99%    /System/Volumes/Data\n";

    fn version() -> CommandOutput {
        exited(0, "cargo-mutants 27.1.0")
    }

    /// The disk pre-flight finding plenty of room.
    fn roomy_disk() -> CommandOutput {
        exited(0, DF)
    }

    /// The patterns a config configures, with their comments set aside.
    fn patterns(config: &str) -> Result<Vec<String>, String> {
        configured_exclusions(config)
            .map(|found| found.into_iter().map(|entry| entry.pattern).collect())
    }

    /// The reason written beside the one entry a config configures.
    fn why(config: &str) -> String {
        let found = configured_exclusions(config).expect("a readable config");
        let [entry] = found.as_slice() else {
            panic!("expected exactly one entry, got {found:?}");
        };
        entry.why.clone()
    }

    #[test]
    fn a_config_without_exclusions_has_none() {
        let config = "# a comment\nminimum_test_timeout = 60\n";
        assert_eq!(configured_exclusions(config), Ok(Vec::new()));
    }

    #[test]
    fn an_empty_exclusion_list_is_no_exclusions() {
        assert_eq!(configured_exclusions("exclude_re = []\n"), Ok(Vec::new()));
    }

    #[test]
    fn exclusions_are_read_inline_or_across_lines() {
        assert_eq!(
            patterns("exclude_re = [\"a\", \"b\"]\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            patterns("exclude_re = [\n    \"a\",\n    \"b\",\n]\ntimeout = 1\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
    }

    #[test]
    fn a_comment_above_an_entry_is_that_entry_s_reason() {
        assert_eq!(
            why("exclude_re = [\n    # it is default\n    'A::b',\n]\n"),
            "it is default"
        );
    }

    #[test]
    fn a_comment_beside_an_entry_is_its_reason_too() {
        assert_eq!(
            why("exclude_re = [\n    'A::b', # it is default\n]\n"),
            "it is default"
        );
        assert_eq!(
            why("exclude_re = ['A::b'] # it is default\n"),
            "it is default"
        );
    }

    #[test]
    fn a_reason_spanning_several_lines_stays_whole() {
        assert_eq!(
            why("exclude_re = [\n    # it is\n    # default\n    'A::b', # really\n]\n"),
            "it is default really"
        );
    }

    #[test]
    fn a_reason_belongs_only_to_the_entry_it_was_written_above() {
        let found = configured_exclusions(
            "exclude_re = [\n    # only about a\n    'A::a',\n    'B::b',\n]\n",
        )
        .expect("a readable config");
        assert_eq!(found[0].why, "only about a");
        assert_eq!(found[1].why, "", "{found:?}");
    }

    #[test]
    fn an_entry_with_no_comment_anywhere_has_no_reason() {
        assert_eq!(why("exclude_re = ['A::b']\n"), "");
    }

    #[test]
    fn a_hash_inside_a_pattern_is_content_not_a_comment() {
        assert_eq!(
            patterns("exclude_re = ['a#b']\n"),
            Ok(vec!["a#b".to_owned()])
        );
    }

    #[test]
    fn a_bracket_inside_a_comment_does_not_end_the_list() {
        // The array is closed by the `]` TOML sees, not by the first one in the
        // file — and comments are no longer stripped before the search.
        assert_eq!(
            patterns("exclude_re = [\n    # see [1] for why\n    'A::b',\n]\n"),
            Ok(vec!["A::b".to_owned()])
        );
    }

    #[test]
    fn a_commented_out_exclusion_is_not_an_exclusion() {
        let config = "# exclude_re = [\"ghost\"]\nminimum_test_timeout = 60\n";
        assert_eq!(configured_exclusions(config), Ok(Vec::new()));
    }

    #[test]
    fn an_unterminated_exclusion_list_is_refused_rather_than_read_as_empty() {
        let message = configured_exclusions("exclude_re = [\"a\",\n")
            .expect_err("a config the gate cannot read is one it cannot vouch for");
        assert!(message.contains("closing"), "{message}");
    }

    #[test]
    fn a_table_header_before_the_key_is_not_mistaken_for_it() {
        // The first `[` in the file belongs to a table header, not to the
        // array, so finding the key by name has to actually find the key.
        let config = "[mutants]\nminimum_test_timeout = 60\nexclude_re = ['a']\n";
        assert_eq!(patterns(config), Ok(vec!["a".to_owned()]));
    }

    #[test]
    fn a_key_that_merely_starts_with_the_name_is_not_the_key() {
        let config = "exclude_reasoning = 'because'\nexclude_re = ['a']\n";
        assert_eq!(patterns(config), Ok(vec!["a".to_owned()]));
    }

    #[test]
    fn quoting_styles_may_be_mixed_in_either_order() {
        assert_eq!(
            patterns("exclude_re = [\"a\", 'b']\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            patterns("exclude_re = ['a', \"b\"]\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
    }

    #[test]
    fn a_quote_inside_the_other_quoting_style_is_content_not_a_delimiter() {
        assert_eq!(
            patterns("exclude_re = ['it\"s']\n"),
            Ok(vec!["it\"s".to_owned()])
        );
    }

    #[test]
    fn a_regex_reads_the_same_in_either_toml_quoting_style() {
        // A literal string is the idiom for a regex; a basic string has to
        // double the backslash. Both must reach cargo-mutants as `\\.`.
        let literal = patterns("exclude_re = ['stub\\.rs']\n").expect("valid");
        let basic = patterns("exclude_re = [\"stub\\\\.rs\"]\n").expect("valid");
        assert_eq!(literal, basic);
        assert_eq!(literal, vec!["stub\\.rs".to_owned()]);
    }

    #[test]
    fn a_plain_path_exclusion_yields_the_path_it_names() {
        assert_eq!(
            literal_path("horde-worker/src/bin/stub_harness\\.rs"),
            Ok("horde-worker/src/bin/stub_harness.rs".to_owned())
        );
    }

    #[test]
    fn a_pattern_that_is_neither_a_path_nor_a_function_is_refused() {
        for pattern in [
            "src/bin/.*",
            "src/bin/**",
            "a(b)c",
            "a|b",
            "^a$",
            "a\\d",
            "a b",
            "A::",
            "",
        ] {
            let message =
                target(pattern).expect_err("the gate can only vouch for exclusions it can check");
            assert!(message.contains("plain path"), "{pattern}: {message}");
        }
    }

    #[test]
    fn a_pattern_naming_a_file_targets_the_file_and_one_naming_a_function_the_function() {
        assert_eq!(
            target("horde-worker/src/bin/stub_harness\\.rs"),
            Ok(Target::File(
                "horde-worker/src/bin/stub_harness.rs".to_owned()
            ))
        );
        // A bare file name has no slash, so `.rs` is what marks it a file.
        assert_eq!(
            target("stub_harness\\.rs"),
            Ok(Target::File("stub_harness.rs".to_owned()))
        );
        assert_eq!(
            target("Capabilities::none"),
            Ok(Target::Function("Capabilities::none".to_owned()))
        );
        assert_eq!(target("main"), Ok(Target::Function("main".to_owned())));
    }

    #[test]
    fn a_function_claims_mutants_that_name_it_and_no_others() {
        let target = Target::Function("none".to_owned());
        assert!(target.claims("x.rs:1:1: replace none -> Self with Default::default()"));
        assert!(
            !target.claims("x.rs:1:1: replace match guard a.is_none() with true in check"),
            "`none` inside `is_none` is a different function"
        );
        assert!(
            !target.claims("x.rs:1:1: replace Caps::none_of -> Self with Default::default()"),
            "`none` inside `none_of` is a different function"
        );
        assert!(
            !target.claims("x.rs:1:1: delete ! in Caps::none"),
            "`none` inside `Caps::none` is a different function"
        );
    }

    #[test]
    fn a_file_claims_everything_written_in_it() {
        let target = Target::File("crates/w/src/bin/stub.rs".to_owned());
        assert!(target.claims("crates/w/src/bin/stub.rs:9:5: replace main with ()"));
        assert!(!target.claims("crates/w/src/lib.rs:9:5: replace main with ()"));
    }

    /// An entry whose reason is written beside it.
    fn justified(pattern: &str) -> Exclusion {
        Exclusion {
            pattern: pattern.to_owned(),
            why: "the mutated expression is the original".to_owned(),
        }
    }

    fn owned(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|line| (*line).to_owned()).collect()
    }

    const STUB: &str = "crates/w/src/bin/stub.rs:9:5: replace main with ()";
    const EQUIVALENT: &str =
        "crates/a/src/c.rs:9:5: replace Caps::none -> Self with Default::default()";
    const ALSO_NONE: &str = "crates/a/src/c.rs:10:9: delete ! in Caps::none";

    #[test]
    fn an_exclusion_that_took_is_accepted() {
        let entries = [justified("crates/w/src/bin/stub\\.rs")];
        let listed = owned(&["crates/other/src/lib.rs:1:1: replace f with ()"]);
        let removed = owned(&[STUB]);
        assert_eq!(
            verify_exclusions(&entries, &listed, &removed, |_| true),
            Ok(())
        );
    }

    #[test]
    fn an_exclusion_naming_nothing_is_refused() {
        // A dead exclusion looks exactly like a live one, so it silently
        // misstates the measured scope — the module-cycle baseline is
        // bidirectional for the same reason.
        let entries = [justified("crates/gone/src/bin/stub\\.rs")];
        let message = verify_exclusions(&entries, &[], &[], |_| false)
            .expect_err("the path it excludes is not in the tree");
        assert!(message.contains("not in the tree"), "{message}");
    }

    #[test]
    fn an_exclusion_that_was_configured_but_did_not_take_is_refused() {
        // Observed for real: `--exclude 'src/bin/**'` was accepted and the stub
        // binary's mutants were tested anyway, because exclude_re matches a
        // mutant's description rather than a path glob. Configured is not
        // applied, so the gate checks applied.
        let entries = [justified("crates/w/src/bin/stub\\.rs")];
        let listed = owned(&[STUB]);
        let message = verify_exclusions(&entries, &listed, &[], |_| true)
            .expect_err("cargo-mutants still lists what the exclusion claims to remove");
        assert!(message.contains("did not take"), "{message}");
    }

    #[test]
    fn an_exclusion_that_removed_nothing_is_refused() {
        // The path is in the tree and nothing it names is listed, and still the
        // entry is doing nothing: the two listings are identical. Only the
        // difference between them can tell that apart from a live exclusion.
        let entries = [justified("crates/w/src/bin/stub\\.rs")];
        let message = verify_exclusions(&entries, &[], &[], |_| true)
            .expect_err("an exclusion that removes nothing is holding nothing back");
        assert!(message.contains("removed no mutant"), "{message}");
    }

    #[test]
    fn a_function_excluded_without_a_written_reason_is_refused() {
        let entries = [Exclusion {
            pattern: "Caps::none".to_owned(),
            why: "   ".to_owned(),
        }];
        let message = verify_exclusions(&entries, &[], &owned(&[EQUIVALENT]), |_| true)
            .expect_err("equivalence is an argument, and an argument has to be written down");
        assert!(message.contains("no reason beside it"), "{message}");
    }

    #[test]
    fn a_file_needs_no_written_reason() {
        // A file exclusion is checked against the filesystem and reads for
        // itself; an equivalence claim is checkable by nothing but the reader.
        let entries = [Exclusion {
            pattern: "crates/w/src/bin/stub\\.rs".to_owned(),
            why: String::new(),
        }];
        assert_eq!(
            verify_exclusions(&entries, &[], &owned(&[STUB]), |_| true),
            Ok(())
        );
    }

    #[test]
    fn a_function_exclusion_that_argues_its_equivalence_is_accepted() {
        // The whole point of the mechanism, and the case the rest of these
        // tests only approach from the outside: a reason written down, one
        // mutant removed, nothing of it left in the listing.
        let entries = [justified("Caps::none")];
        assert_eq!(
            verify_exclusions(&entries, &[], &owned(&[EQUIVALENT]), |_| true),
            Ok(())
        );
    }

    #[test]
    fn a_function_exclusion_that_removed_more_than_one_mutant_is_refused() {
        // Equivalence is a claim about one mutated expression. A function that
        // grew a second mutant has outgrown the argument made for it, and
        // widening the entry to cover it silently excludes untested behaviour.
        let entries = [justified("Caps::none")];
        let removed = owned(&[EQUIVALENT, ALSO_NONE]);
        let message = verify_exclusions(&entries, &[], &removed, |_| true)
            .expect_err("two mutants, one argument");
        assert!(message.contains("removed 2 mutants"), "{message}");
    }

    #[test]
    fn a_mutant_removed_that_no_entry_names_is_refused() {
        // Either the pattern is wider than it reads, or some other key in the
        // config is quietly narrowing scope. The gate vouches for neither.
        let entries = [justified("crates/w/src/bin/stub\\.rs")];
        let removed = owned(&[STUB, "crates/product/src/lib.rs:3:1: replace go with ()"]);
        let message = verify_exclusions(&entries, &[], &removed, |_| true)
            .expect_err("the listings differ by more than the entries account for");
        assert!(message.contains("no entry names it"), "{message}");
    }

    /// A workspace root whose `.cargo/mutants.toml` says `config`.
    fn root_configured(config: &str) -> (tempfile::TempDir, GateContext) {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join(".cargo")).expect("cargo dir");
        std::fs::write(dir.path().join(CONFIG), config).expect("config");
        let ctx = ctx_at(dir.path());
        (dir, ctx)
    }

    /// A run with no trunk to compare against: the version probe, the disk
    /// pre-flight, any extra responses the test needs first, then the git
    /// probes and a clean result.
    fn whole_tree_run(after_version: Vec<CommandOutput>) -> Vec<CommandOutput> {
        let mut script = vec![version(), roomy_disk()];
        script.extend(after_version);
        script.extend([
            failed(128, "fatal: no main"),
            exited(0, "tip\n"),
            exited(0, CLEAN),
        ]);
        script
    }

    #[test]
    fn no_exclusions_means_no_listing_run_at_all() {
        let (_dir, ctx) = root_configured("minimum_test_timeout = 60\n");
        let runner = ScriptedRunner::new(whole_tree_run(Vec::new()));
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
        assert!(
            !runner
                .command_lines()
                .iter()
                .any(|line| line.contains("--list")),
            "checking nothing must cost nothing: {:?}",
            runner.command_lines()
        );
    }

    #[test]
    fn an_absent_config_is_not_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(whole_tree_run(Vec::new()));
        assert_eq!(
            run_gate(&gate(), &ctx_at(dir.path()), &runner).label(),
            "pass"
        );
    }

    /// The config used by the gate-level exclusion tests: one entry naming a
    /// file that really is in the throwaway root.
    const EXCLUDING_THE_CONFIG: &str = "exclude_re = ['.cargo/mutants\\.toml']\n";

    /// What the excluded listing shows, and what the unexcluded one adds.
    const ELSEWHERE: &str = "crates/other/src/lib.rs:1:1: replace f with ()";
    const IN_THE_CONFIG: &str = ".cargo/mutants.toml:1:1: replace thing with ()";

    #[test]
    fn a_live_exclusion_that_took_lets_the_run_proceed() {
        let (_dir, ctx) = root_configured(EXCLUDING_THE_CONFIG);
        let runner = ScriptedRunner::new(whole_tree_run(vec![
            exited(0, ELSEWHERE),
            exited(0, &format!("{ELSEWHERE}\n{IN_THE_CONFIG}")),
        ]));
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
        let lines = runner.command_lines();
        assert!(lines[2].contains("--list"), "{lines:?}");
        assert!(
            lines[3].contains("--no-config"),
            "the difference between the listings is the measurement: {lines:?}"
        );
    }

    /// A function-scoped equivalence entry with its argument written beside it.
    ///
    /// The reason is what separates an equivalence from a survivor somebody got
    /// tired of, so the gate-level tests carry a real one rather than a token.
    const EXCLUDING_A_FUNCTION: &str = "exclude_re = [\n    \
         # `Caps::none()` is written `Self::default()` over a type deriving\n    \
         # `Default`, so the mutated expression is the original.\n    \
         'Caps::none',\n]\n";

    /// Why the gate refuses `config`, given what the two listings say.
    ///
    /// Every unverifiable configuration is refused the same way and has to cost
    /// the same nothing, so both halves of that live here rather than being
    /// restated once per lie: the caller is left with the one thing that differs
    /// between these tests, which is which lie was told.
    fn refusal(config: &str, listed: &str, unexcluded: &str) -> String {
        let (_dir, ctx) = root_configured(config);
        let runner = ScriptedRunner::new(vec![
            version(),
            roomy_disk(),
            exited(0, listed),
            exited(0, unexcluded),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("an exclusion the gate cannot vouch for must stop it");
        };
        assert_eq!(
            runner.calls().len(),
            4,
            "no mutation run should start on an unverifiable configuration"
        );
        reason
    }

    #[test]
    fn an_exclusion_naming_nothing_stops_the_gate_before_it_mutates() {
        let reason = refusal("exclude_re = ['crates/gone/src/bin/stub\\.rs']\n", "", "");
        assert!(reason.contains("not in the tree"), "{reason}");
    }

    #[test]
    fn an_exclusion_that_did_not_take_stops_the_gate() {
        let reason = refusal(EXCLUDING_THE_CONFIG, IN_THE_CONFIG, IN_THE_CONFIG);
        assert!(reason.contains("did not take"), "{reason}");
    }

    #[test]
    fn an_exclusion_that_removed_nothing_stops_the_gate() {
        // Both listings agree, so the entry is inert. Nothing in the excluded
        // listing can show that; only the comparison can.
        let reason = refusal(EXCLUDING_THE_CONFIG, ELSEWHERE, ELSEWHERE);
        assert!(reason.contains("removed no mutant"), "{reason}");
    }

    #[test]
    fn a_function_exclusion_that_proves_itself_lets_the_run_proceed() {
        // The mechanism end to end: the entry names a function, the two
        // listings differ by exactly that function's one mutant, and the
        // argument for its equivalence is written where the next reader is.
        let (_dir, ctx) = root_configured(EXCLUDING_A_FUNCTION);
        let runner = ScriptedRunner::new(whole_tree_run(vec![
            exited(0, ELSEWHERE),
            exited(0, &format!("{ELSEWHERE}\n{EQUIVALENT}")),
        ]));
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
    }

    #[test]
    fn a_planted_entry_naming_a_mutant_that_does_not_exist_reds_the_gate() {
        // Nothing in the tree is called `Caps::none`, so both listings agree.
        // A function entry has no path to check against the filesystem, which
        // is exactly why "it removed something" is the check that catches this.
        let reason = refusal(EXCLUDING_A_FUNCTION, ELSEWHERE, ELSEWHERE);
        assert!(reason.contains("removed no mutant"), "{reason}");
    }

    #[test]
    fn a_planted_entry_whose_mutant_was_not_removed_reds_the_gate() {
        // The pattern is configured and the mutant is still there — an
        // anchoring mistake, or a name that does not match what cargo-mutants
        // prints. Configured is not applied, for functions as much as files.
        let reason = refusal(EXCLUDING_A_FUNCTION, EQUIVALENT, EQUIVALENT);
        assert!(reason.contains("did not take"), "{reason}");
    }

    #[test]
    fn a_function_excluded_with_no_reason_written_down_stops_the_gate() {
        // It removes exactly what it names, and it is still refused: nothing
        // but a reader can check an equivalence, and a reader needs the reason.
        let reason = refusal(
            "exclude_re = ['Caps::none']\n",
            ELSEWHERE,
            &format!("{ELSEWHERE}\n{EQUIVALENT}"),
        );
        assert!(reason.contains("no reason beside it"), "{reason}");
    }

    #[test]
    fn a_listing_that_fails_is_an_instrument_error() {
        let (_dir, ctx) = root_configured(EXCLUDING_THE_CONFIG);
        let runner = ScriptedRunner::new(vec![
            version(),
            roomy_disk(),
            failed(1, "error: bad config"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("exclusions that could not be checked were not checked");
        };
        assert!(reason.contains("bad config"), "{reason}");
    }

    #[test]
    fn an_unexcluded_listing_that_fails_is_an_instrument_error_too() {
        let (_dir, ctx) = root_configured(EXCLUDING_THE_CONFIG);
        let runner = ScriptedRunner::new(vec![
            version(),
            roomy_disk(),
            exited(0, ELSEWHERE),
            failed(1, "error: no such option --no-config"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("half a comparison measures nothing");
        };
        assert!(reason.contains("no such option"), "{reason}");
    }

    #[test]
    fn the_listing_commands_do_not_test_anything() {
        assert_eq!(
            list_spec(&repo_ctx()).display(),
            "cargo mutants --list",
            "checking exclusions must not cost a mutation run"
        );
        assert_eq!(
            unexcluded_list_spec(&repo_ctx()).display(),
            "cargo mutants --list --no-config"
        );
    }

    #[test]
    fn the_diff_is_taken_against_the_merge_base_with_main() {
        assert_eq!(
            merge_base_spec(Path::new("/repo")).display(),
            "git merge-base HEAD main"
        );
        assert_eq!(
            diff_spec(Path::new("/repo"), "abc123").display(),
            "git diff --no-color abc123"
        );
    }

    #[test]
    fn a_full_tree_run_names_no_diff() {
        let spec = spec(&repo_ctx(), &Scope::FullTree);
        assert_eq!(
            spec.display(),
            "cargo mutants --no-shuffle --colors never --jobs 4 --profile=mutants \
             --output /repo/target/quality"
        );
    }

    #[test]
    fn a_diff_scoped_run_points_cargo_mutants_at_the_diff() {
        let spec = spec(
            &repo_ctx(),
            &Scope::Diff(PathBuf::from("/repo/target/quality/mutants.diff")),
        );
        assert!(
            spec.display()
                .ends_with("--in-diff /repo/target/quality/mutants.diff"),
            "{}",
            spec.display()
        );
    }

    #[test]
    fn package_narrowing_is_passed_through() {
        let ctx = GateContext::new(Path::new("/repo"), vec!["horde-gauntlet".to_owned()]);
        assert!(
            spec(&ctx, &Scope::FullTree)
                .display()
                .ends_with("--package horde-gauntlet"),
            "{}",
            spec(&ctx, &Scope::FullTree).display()
        );
    }

    #[test]
    fn a_real_clean_run_is_read_correctly() {
        let counts = parse_summary(CLEAN).expect("the fixture is real cargo-mutants output");
        assert_eq!(counts.missed, 0);
        assert!(counts.caught > 0, "{counts:?}");
    }

    #[test]
    fn a_run_with_survivors_is_read_correctly() {
        let text = "858 mutants tested in 5m 3s: 12 missed, 800 caught, 40 unviable, 6 timeouts\n";
        assert_eq!(
            parse_summary(text),
            Some(MutantCounts {
                missed: 12,
                caught: 800,
                unviable: 40,
                timeout: 6,
            })
        );
    }

    #[test]
    fn a_singular_timeout_is_read_too() {
        let text = "3 mutants tested: 0 missed, 2 caught, 0 unviable, 1 timeout\n";
        assert_eq!(parse_summary(text).map(|counts| counts.timeout), Some(1));
    }

    #[test]
    fn output_without_a_tally_means_the_run_did_not_finish() {
        assert_eq!(
            parse_summary("error: failed to build in the clean tree"),
            None
        );
    }

    #[test]
    fn a_surviving_mutant_fails_the_gate_and_says_where_to_look() {
        let counts = MutantCounts {
            missed: 2,
            caught: 10,
            unviable: 1,
            timeout: 0,
        };
        let GateOutcome::Fail { detail } = outcome_of(&Scope::FullTree, counts) else {
            panic!("a mutation that stays green is a finding");
        };
        assert!(
            detail.starts_with("2 mutant(s) survived in the whole tree"),
            "{detail}"
        );
        assert!(detail.contains("missed.txt"), "{detail}");
    }

    #[test]
    fn a_clean_run_passes_and_reports_the_timeout_count() {
        let counts = MutantCounts {
            missed: 0,
            caught: 10,
            unviable: 1,
            timeout: 3,
        };
        let GateOutcome::Pass { detail } = outcome_of(&Scope::FullTree, counts) else {
            panic!("nothing survived");
        };
        assert!(detail.contains("10 mutant(s) caught"), "{detail}");
        assert!(detail.contains("3 timed out"), "{detail}");
    }

    #[test]
    fn tested_counts_every_mutant_that_was_built_and_run() {
        let counts = MutantCounts {
            missed: 2,
            caught: 10,
            unviable: 5,
            timeout: 3,
        };
        assert_eq!(
            counts.tested(),
            15,
            "unviable mutants were never run; everything else was"
        );
        assert_eq!(MutantCounts::default().tested(), 0);
    }

    #[test]
    fn a_run_whose_mutants_only_timed_out_is_a_pass_not_a_skip() {
        let counts = MutantCounts {
            missed: 0,
            caught: 0,
            unviable: 0,
            timeout: 3,
        };
        let GateOutcome::Pass { detail } = outcome_of(&Scope::FullTree, counts) else {
            panic!("a mutant that hung the suite did change behaviour, so it was caught");
        };
        assert!(detail.contains("3 timed out"), "{detail}");
    }

    #[test]
    fn a_change_touching_no_rust_is_skipped_rather_than_erroring() {
        // cargo-mutants exits 0 and prints no tally, which is indistinguishable
        // from a crash unless the gate reads the reason.
        assert_eq!(
            nothing_to_mutate(
                " INFO Diff changes no Rust source files\n",
                &Scope::FullTree
            ),
            Some("no Rust changed in the whole tree".to_owned())
        );
        assert_eq!(
            nothing_to_mutate("858 mutants tested: 0 missed", &Scope::FullTree),
            None
        );
    }

    #[test]
    fn a_tests_only_change_is_skipped_rather_than_erroring() {
        // The exact message from `main`: the merge at c5a31e7 brought in four
        // files, all of them test files, so cargo-mutants filtered every mutant
        // away, printed this on stderr, and exited 0 with no tally at all.
        assert_eq!(
            nothing_to_mutate(" INFO No mutants to filter\n", &Scope::Diff(PathBuf::new())),
            Some(format!(
                "nothing mutable in changed code{TESTS_ARE_NOT_MUTATED}"
            ))
        );
    }

    #[test]
    fn every_way_cargo_mutants_reports_no_work_is_skipped() {
        // Four stages the work can run out at, four wordings. Reading only one
        // of them is how this gate erred on `main` after already having been
        // fixed for the docs-only case.
        for message in [
            " INFO Diff changes no Rust source files",
            " INFO No mutants to filter",
            " INFO diff matched no mutants",
            "No mutants found under the active filters",
            "No mutants were generated",
        ] {
            assert!(
                nothing_to_mutate(message, &Scope::FullTree).is_some(),
                "a completed run with no work is not an instrument failure: {message}"
            );
        }
    }

    #[test]
    fn a_build_that_produced_no_viable_mutants_is_still_an_error() {
        // One word from "No mutants were generated", and the opposite meaning:
        // cargo-mutants says this when it could not build in its scratch
        // directory. A broken instrument tells you nothing, so it must not be
        // absorbed into the skipped family by a loose substring match.
        assert_eq!(
            nothing_to_mutate(
                "No mutants were viable: perhaps there is a problem with building \
                 in a scratch directory.",
                &Scope::FullTree
            ),
            None
        );
    }

    #[test]
    fn a_tests_only_change_reports_skipped_through_the_whole_gate() {
        // The repro end to end: a diff naming only test files, and the run that
        // follows it saying it had nothing to filter.
        let run = run_after_with(
            vec![
                exited(0, "branchpoint\n"),
                exited(0, "tip\n"),
                exited(
                    0,
                    "diff --git a/crates/horde-server/tests/contracts.rs \
                     b/crates/horde-server/tests/contracts.rs\n",
                ),
            ],
            failed(0, " INFO No mutants to filter\n"),
        );
        assert_eq!(run.outcome.label(), "skipped", "{:?}", run.outcome);
        assert!(
            run.outcome.detail().contains("nothing mutable"),
            "{}",
            run.outcome.detail()
        );
        assert!(
            run.outcome.detail().contains("cfg(test)"),
            "the row has to say why a diff full of Rust produced no mutants: {}",
            run.outcome.detail()
        );
    }

    #[test]
    fn a_docs_only_change_reports_skipped_through_the_whole_gate() {
        let run = run_after_with(
            vec![
                exited(0, "branchpoint\n"),
                exited(0, "tip\n"),
                exited(0, "diff --git a/README.md b/README.md\n"),
            ],
            failed(0, " INFO Diff changes no Rust source files\n"),
        );
        assert_eq!(run.outcome.label(), "skipped", "{:?}", run.outcome);
        assert!(
            run.outcome.detail().contains("no Rust changed"),
            "{}",
            run.outcome.detail()
        );
    }

    #[test]
    fn a_run_with_nothing_to_mutate_is_skipped_not_passed() {
        let counts = MutantCounts {
            missed: 0,
            caught: 0,
            unviable: 4,
            timeout: 0,
        };
        assert_eq!(
            outcome_of(&Scope::Diff(PathBuf::new()), counts).label(),
            "skipped"
        );
    }

    #[test]
    fn the_scope_is_named_in_the_detail() {
        assert_eq!(Scope::FullTree.label(), "the whole tree");
        assert_eq!(Scope::Diff(PathBuf::new()).label(), "changed code");
    }

    /// What one scripted run of the gate produced.
    struct Run {
        _dir: tempfile::TempDir,
        ctx: GateContext,
        outcome: GateOutcome,
        lines: Vec<String>,
    }

    impl Run {
        /// The cargo-mutants invocation, which is always the last command.
        fn mutation_command(&self) -> &str {
            self.lines.last().map_or("", String::as_str)
        }
    }

    /// Runs the gate against a throwaway root, scripting the git probes and
    /// then a clean cargo-mutants run.
    fn run_after(git: Vec<CommandOutput>) -> Run {
        run_after_with(git, exited(0, CLEAN))
    }

    /// As [`run_after`], with the cargo-mutants result chosen by the caller.
    fn run_after_with(git: Vec<CommandOutput>, result: CommandOutput) -> Run {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = ctx_at(dir.path());
        let mut script = vec![version(), roomy_disk()];
        script.extend(git);
        script.push(result);
        let runner = ScriptedRunner::new(script);
        let outcome = run_gate(&gate(), &ctx, &runner);
        Run {
            _dir: dir,
            ctx,
            outcome,
            lines: runner.command_lines(),
        }
    }

    #[test]
    fn on_a_branch_the_scope_is_everything_since_the_branch_point() {
        let run = run_after(vec![
            exited(0, "branchpoint\n"),        // merge-base
            exited(0, "tip\n"),                // HEAD
            exited(0, "diff --git a/x b/x\n"), // the branch's work
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(
            run.mutation_command().contains("--in-diff"),
            "{:?}",
            run.lines
        );
        assert_eq!(run.lines[2], "git merge-base HEAD main");
    }

    #[test]
    fn on_trunk_the_scope_is_what_the_tip_commit_brought_in() {
        // The merge-base is HEAD, so there is nothing behind it to diff
        // against; the first parent is the trunk the merge landed on, which
        // makes the diff exactly what the merge introduced.
        let run = run_after(vec![
            exited(0, "tip\n"),      // merge-base == HEAD
            exited(0, "tip\n"),      // HEAD
            exited(0, "previous\n"), // HEAD^1
            exited(0, "diff --git a/x b/x\n"),
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert_eq!(run.lines[4], "git rev-parse HEAD^1");
        assert!(
            run.mutation_command().contains("--in-diff"),
            "{:?}",
            run.lines
        );
        assert!(
            run.outcome.detail().contains("changed code"),
            "{}",
            run.outcome.detail()
        );
    }

    #[test]
    fn a_root_commit_on_trunk_falls_back_to_the_whole_tree() {
        let run = run_after(vec![
            exited(0, "tip\n"),
            exited(0, "tip\n"),
            failed(128, "fatal: ambiguous argument 'HEAD^1'"),
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(
            run.outcome.detail().contains("whole tree"),
            "nothing to compare against means measure everything: {}",
            run.outcome.detail()
        );
    }

    #[test]
    fn asking_for_everything_skips_the_scoping_entirely() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ctx = ctx_at(dir.path()).measuring_everything(true);
        let runner = ScriptedRunner::new(vec![version(), roomy_disk(), exited(0, CLEAN)]);
        let outcome = run_gate(&gate(), &ctx, &runner);

        assert_eq!(outcome.label(), "pass");
        assert!(
            outcome.detail().contains("whole tree"),
            "{}",
            outcome.detail()
        );
        assert!(
            !runner
                .command_lines()
                .iter()
                .any(|line| line.starts_with("git")),
            "an explicit --full needs no git at all: {:?}",
            runner.command_lines()
        );
    }

    #[test]
    fn an_absent_trunk_branch_falls_back_to_the_whole_tree() {
        let run = run_after(vec![
            failed(128, "fatal: Not a valid object name main"),
            exited(0, "tip\n"),
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(
            !run.mutation_command().contains("--in-diff"),
            "{:?}",
            run.lines
        );
    }

    #[test]
    fn an_empty_diff_falls_back_to_the_whole_tree() {
        let run = run_after(vec![
            exited(0, "branchpoint\n"),
            exited(0, "tip\n"),
            exited(0, ""),
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(
            !run.mutation_command().contains("--in-diff"),
            "{:?}",
            run.lines
        );
    }

    #[test]
    fn a_real_diff_scopes_the_run_and_is_written_to_disk() {
        let diff = "diff --git a/src/lib.rs b/src/lib.rs\n";
        let run = run_after(vec![
            exited(0, "branchpoint\n"),
            exited(0, "tip\n"),
            exited(0, diff),
        ]);
        assert_eq!(run.outcome.label(), "pass");
        assert!(
            run.mutation_command().contains("--in-diff"),
            "{:?}",
            run.lines
        );
        assert_eq!(
            std::fs::read_to_string(diff_path(&run.ctx)).expect("the diff was written"),
            diff
        );
    }

    #[test]
    fn a_run_that_never_reported_a_tally_is_an_instrument_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(vec![
            version(),
            roomy_disk(),
            failed(128, "fatal: no main"),
            exited(0, "tip\n"),
            failed(4, "error: build failed in an unmutated tree"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
            panic!("a mutation run that did not finish did not pass");
        };
        assert!(reason.contains("build failed"), "{reason}");
    }

    #[test]
    fn the_gate_is_pinned_to_the_calibrated_cargo_mutants() {
        assert_eq!(gate().pin, &PIN);
        assert_eq!(PIN.expected, "27.1.0");
    }

    #[test]
    fn the_profile_the_gate_passes_is_defined_in_the_workspace_manifest() {
        // The gate passes `--profile=mutants` on every run, so the manifest
        // losing the profile would turn every mutation run into an error.
        let manifest = include_str!("../../../../Cargo.toml");
        assert!(
            manifest.contains(&format!("[profile.{PROFILE}]")),
            "the workspace manifest must define the profile the gate builds with"
        );
        assert!(
            manifest.contains("debug = false"),
            "the profile exists to drop debuginfo from mutation builds"
        );
    }

    #[test]
    fn the_disk_probe_asks_df_for_portable_kilobyte_output() {
        let spec = free_space_spec(Path::new("/repo"));
        assert_eq!(spec.display(), "df -P -k .");
        assert_eq!(spec.cwd, Path::new("/repo"));
    }

    #[test]
    fn free_space_is_read_from_real_df_output_in_bytes() {
        // 78666364 KiB available, exactly as captured in the fixture.
        assert_eq!(parse_free_bytes(DF), Ok(78_666_364 * 1024));
    }

    #[test]
    fn df_output_the_gate_cannot_read_is_refused_not_assumed_healthy() {
        for text in [
            "",
            "\n   \n",
            "Filesystem   1024-blocks      Used Available Capacity  Mounted on\n",
            "/dev/disk3s5 mounted\n",
            "/dev/disk3s5   971350180 966107300   lots    99%   /\n",
        ] {
            assert!(
                parse_free_bytes(text).is_err(),
                "a guard that shrugs at unreadable output is not a guard: {text:?}"
            );
        }
    }

    #[test]
    fn the_threshold_is_forty_gib() {
        assert_eq!(MIN_FREE_BYTES, 40 * 1024 * 1024 * 1024);
    }

    #[test]
    fn free_space_at_or_above_the_threshold_is_enough() {
        assert_eq!(enough_disk(MIN_FREE_BYTES), Ok(()));
        assert_eq!(enough_disk(u64::MAX), Ok(()));
    }

    #[test]
    fn one_byte_under_the_threshold_is_not() {
        assert!(
            enough_disk(MIN_FREE_BYTES - 1).is_err(),
            "the line is the line"
        );
    }

    #[test]
    fn the_refusal_names_the_threshold_the_observed_space_and_the_culprits() {
        let message = enough_disk(5 * GIB).expect_err("5 GiB is far under the line");
        assert!(message.contains("only 5.0 GiB free"), "{message}");
        assert!(message.contains("40 GiB"), "{message}");
        assert!(message.contains("worktree target/"), "{message}");
        assert!(message.contains("target/llvm-cov-target"), "{message}");
        assert!(message.contains("cargo-mutants-*"), "{message}");
    }

    #[test]
    fn a_nearly_full_volume_stops_the_gate_before_anything_builds() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(vec![version(), exited(0, DF_NEARLY_FULL)]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
            panic!("a volume too full to hold the run is a broken instrument, not a verdict");
        };
        assert!(reason.contains("only 5.0 GiB free"), "{reason}");
        assert!(reason.contains("40 GiB"), "{reason}");
        assert_eq!(
            runner.calls().len(),
            2,
            "nothing else may run once the volume is known to be too full"
        );
    }

    #[test]
    fn a_disk_probe_that_fails_is_an_instrument_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let runner = ScriptedRunner::new(vec![version(), failed(1, "df: cannot stat")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx_at(dir.path()), &runner) else {
            panic!("free space that could not be checked was not checked");
        };
        assert!(
            reason.contains("free space could not be checked"),
            "{reason}"
        );
        assert!(reason.contains("df: cannot stat"), "{reason}");
    }
}
