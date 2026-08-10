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

/// Where cargo-mutants' configuration, and so its exclusions, live.
pub const CONFIG: &str = ".cargo/mutants.toml";

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

/// Why there was nothing to mutate, if there was nothing.
///
/// A change that touches no Rust — documentation, a proto file, a config —
/// generates no mutants and cargo-mutants prints no tally. That is `skipped`,
/// not `error`: nothing was measured because there was nothing to measure,
/// which is a different fact from an instrument that broke, and the row has to
/// say which.
pub fn nothing_to_mutate(text: &str, scope: &Scope) -> Option<String> {
    text.contains("changes no Rust source files")
        .then(|| format!("no Rust changed in {}", scope.label()))
}

/// Confirms the configured exclusions are real and took effect.
///
/// Costs nothing when there are none, which is the usual case: the listing run
/// only happens once something is excluded.
fn check_exclusions(ctx: &GateContext, runner: &dyn CommandRunner) -> Result<(), String> {
    let config = ctx.workspace_root.join(CONFIG);
    let Ok(text) = std::fs::read_to_string(&config) else {
        return Ok(());
    };
    let patterns = configured_exclusions(&text)?;
    if patterns.is_empty() {
        return Ok(());
    }
    let spec = list_spec(ctx);
    let output = runner.run(&spec).map_err(|error| error.to_string())?;
    if !output.success() {
        return Err(format!(
            "`{}` failed, so the exclusions could not be checked: {}",
            spec.display(),
            output.combined().trim()
        ));
    }
    let listed: Vec<String> = output.stdout.lines().map(str::to_owned).collect();
    let root = ctx.workspace_root.clone();
    verify_exclusions(&patterns, &listed, |path| root.join(path).exists())
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

/// The `exclude_re` patterns configured for cargo-mutants.
///
/// # Errors
///
/// Returns a message when the key is present but unreadable. A configuration
/// this gate cannot read is an instrument it cannot vouch for, so it refuses
/// rather than assuming there are no exclusions.
pub fn configured_exclusions(config: &str) -> Result<Vec<String>, String> {
    let Some(rest) = exclusion_value(config) else {
        return Ok(Vec::new());
    };
    let (list, closed) = match rest.split_once(']') {
        Some((list, _)) => (list, true),
        None => (rest.as_str(), false),
    };
    if !closed {
        return Err(format!("{CONFIG}: `exclude_re` is missing its closing `]`"));
    }
    Ok(toml_strings(list))
}

/// The body of the `exclude_re` array, from just after its opening bracket.
fn exclusion_value(config: &str) -> Option<String> {
    let start = config
        .lines()
        .position(|line| is_exclusion_key(line.trim()))?;
    let tail: String = config
        .lines()
        .skip(start)
        .filter(|line| !line.trim_start().starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    tail.split_once('[').map(|(_, rest)| rest.to_owned())
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

/// The literal path an exclusion names.
///
/// Exclusions here must be plain paths, with `\.` for a dot. That is a real
/// restriction on what may be excluded, and it buys the only thing that makes
/// an exclusion trustworthy: "did this actually exclude anything?" can be
/// answered by looking, instead of by reimplementing a regex engine and hoping
/// the imitation agrees with cargo-mutants' own.
///
/// # Errors
///
/// Returns a message when the pattern is not a plain path.
pub fn literal_path(pattern: &str) -> Result<String, String> {
    let mut literal = String::with_capacity(pattern.len());
    let mut characters = pattern.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' if characters.next() == Some('.') => literal.push('.'),
            '\\' => return Err(escape_refusal(pattern)),
            '*' | '?' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                return Err(escape_refusal(pattern));
            }
            other => literal.push(other),
        }
    }
    Ok(literal)
}

fn escape_refusal(pattern: &str) -> String {
    format!(
        "{CONFIG}: `{pattern}` is not a plain path. Exclusions must name one path, \
         with `\\.` for a dot, so the gate can check the exclusion actually took"
    )
}

/// Confirms every exclusion both names something and removed it.
///
/// Two ways an exclusion lies, and both are checked:
///
/// * It names a path that no longer exists. A dead exclusion is
///   indistinguishable from a live one, so it silently misstates what is being
///   measured — the same reason the module-cycle baseline ratchets both ways.
/// * It is configured but had no effect. cargo-mutants matches `exclude_re`
///   against a mutant's description, and an anchoring mistake makes an
///   exclusion quietly do nothing; the mutants it was meant to skip are tested
///   anyway. Configured is not the same as applied, so this checks applied.
///
/// # Errors
///
/// Returns a message naming the exclusion and which of the two it is.
pub fn verify_exclusions(
    patterns: &[String],
    listed: &[String],
    exists: impl Fn(&str) -> bool,
) -> Result<(), String> {
    for pattern in patterns {
        let literal = literal_path(pattern)?;
        if !exists(&literal) {
            return Err(format!(
                "{CONFIG}: `{pattern}` excludes {literal}, which is not in the tree — \
                 an exclusion that names nothing misstates what is measured"
            ));
        }
        if let Some(mutant) = listed.iter().find(|mutant| mutant.contains(&literal)) {
            return Err(format!(
                "{CONFIG}: `{pattern}` did not take — cargo-mutants still lists `{mutant}`. \
                 Configured is not applied"
            ));
        }
    }
    Ok(())
}

/// The command that lists the mutants a run would test, without testing them.
pub fn list_spec(ctx: &GateContext) -> CommandSpec {
    CommandSpec::owned(
        "cargo",
        vec!["mutants".to_owned(), "--list".to_owned()],
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

    fn version() -> CommandOutput {
        exited(0, "cargo-mutants 27.1.0")
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
            configured_exclusions("exclude_re = [\"a\", \"b\"]\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            configured_exclusions("exclude_re = [\n    \"a\",\n    \"b\",\n]\ntimeout = 1\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
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
        assert_eq!(configured_exclusions(config), Ok(vec!["a".to_owned()]));
    }

    #[test]
    fn a_key_that_merely_starts_with_the_name_is_not_the_key() {
        let config = "exclude_reasoning = 'because'\nexclude_re = ['a']\n";
        assert_eq!(configured_exclusions(config), Ok(vec!["a".to_owned()]));
    }

    #[test]
    fn quoting_styles_may_be_mixed_in_either_order() {
        assert_eq!(
            configured_exclusions("exclude_re = [\"a\", 'b']\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            configured_exclusions("exclude_re = ['a', \"b\"]\n"),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
    }

    #[test]
    fn a_quote_inside_the_other_quoting_style_is_content_not_a_delimiter() {
        assert_eq!(
            configured_exclusions("exclude_re = ['it\"s']\n"),
            Ok(vec!["it\"s".to_owned()])
        );
    }

    #[test]
    fn a_regex_reads_the_same_in_either_toml_quoting_style() {
        // A literal string is the idiom for a regex; a basic string has to
        // double the backslash. Both must reach cargo-mutants as `\\.`.
        let literal = configured_exclusions("exclude_re = ['stub\\.rs']\n").expect("valid");
        let basic = configured_exclusions("exclude_re = [\"stub\\\\.rs\"]\n").expect("valid");
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
    fn a_pattern_that_is_not_a_plain_path_is_refused() {
        for pattern in ["src/bin/.*", "src/bin/**", "a(b)c", "a|b", "^a$", "a\\d"] {
            let message = literal_path(pattern)
                .expect_err("the gate can only vouch for exclusions it can check");
            assert!(message.contains("plain path"), "{pattern}: {message}");
        }
    }

    #[test]
    fn an_exclusion_that_took_is_accepted() {
        let patterns = ["crates/w/src/bin/stub\\.rs".to_owned()];
        let listed = ["crates/other/src/lib.rs:1:1: replace f with ()".to_owned()];
        assert_eq!(verify_exclusions(&patterns, &listed, |_| true), Ok(()));
    }

    #[test]
    fn an_exclusion_naming_nothing_is_refused() {
        // A dead exclusion looks exactly like a live one, so it silently
        // misstates the measured scope — the module-cycle baseline is
        // bidirectional for the same reason.
        let patterns = ["crates/gone/src/bin/stub\\.rs".to_owned()];
        let message = verify_exclusions(&patterns, &[], |_| false)
            .expect_err("the path it excludes is not in the tree");
        assert!(message.contains("not in the tree"), "{message}");
    }

    #[test]
    fn an_exclusion_that_was_configured_but_did_not_take_is_refused() {
        // Observed for real: `--exclude 'src/bin/**'` was accepted and the stub
        // binary's mutants were tested anyway, because exclude_re matches a
        // mutant's description rather than a path glob. Configured is not
        // applied, so the gate checks applied.
        let patterns = ["crates/w/src/bin/stub\\.rs".to_owned()];
        let listed = ["crates/w/src/bin/stub.rs:9:5: replace main with ()".to_owned()];
        let message = verify_exclusions(&patterns, &listed, |_| true)
            .expect_err("cargo-mutants still lists what the exclusion claims to remove");
        assert!(message.contains("did not take"), "{message}");
    }

    /// A workspace root whose `.cargo/mutants.toml` says `config`.
    fn root_configured(config: &str) -> (tempfile::TempDir, GateContext) {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::create_dir_all(dir.path().join(".cargo")).expect("cargo dir");
        std::fs::write(dir.path().join(CONFIG), config).expect("config");
        let ctx = ctx_at(dir.path());
        (dir, ctx)
    }

    /// A run with no trunk to compare against: the version probe, any extra
    /// responses the test needs first, then the git probes and a clean result.
    fn whole_tree_run(after_version: Vec<CommandOutput>) -> Vec<CommandOutput> {
        let mut script = vec![version()];
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

    #[test]
    fn a_live_exclusion_that_took_lets_the_run_proceed() {
        let (_dir, ctx) = root_configured("exclude_re = ['.cargo/mutants\\.toml']\n");
        let runner = ScriptedRunner::new(whole_tree_run(vec![exited(
            0,
            "crates/other/src/lib.rs:1:1: replace f with ()",
        )]));
        assert_eq!(run_gate(&gate(), &ctx, &runner).label(), "pass");
        assert!(
            runner.command_lines()[1].contains("--list"),
            "{:?}",
            runner.command_lines()
        );
    }

    #[test]
    fn an_exclusion_naming_nothing_stops_the_gate_before_it_mutates() {
        let (_dir, ctx) = root_configured("exclude_re = ['crates/gone/src/bin/stub\\.rs']\n");
        let runner = ScriptedRunner::new(vec![version(), exited(0, "")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("an exclusion that names nothing misstates the measured scope");
        };
        assert!(reason.contains("not in the tree"), "{reason}");
        assert_eq!(
            runner.calls().len(),
            2,
            "no mutation run should start on an unverifiable configuration"
        );
    }

    #[test]
    fn an_exclusion_that_did_not_take_stops_the_gate() {
        let (_dir, ctx) = root_configured("exclude_re = ['.cargo/mutants\\.toml']\n");
        let runner = ScriptedRunner::new(vec![
            version(),
            exited(0, ".cargo/mutants.toml:1:1: replace thing with ()"),
        ]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("cargo-mutants still lists what the exclusion claims to remove");
        };
        assert!(reason.contains("did not take"), "{reason}");
    }

    #[test]
    fn a_listing_that_fails_is_an_instrument_error() {
        let (_dir, ctx) = root_configured("exclude_re = ['.cargo/mutants\\.toml']\n");
        let runner = ScriptedRunner::new(vec![version(), failed(1, "error: bad config")]);
        let GateOutcome::Error { reason } = run_gate(&gate(), &ctx, &runner) else {
            panic!("exclusions that could not be checked were not checked");
        };
        assert!(reason.contains("bad config"), "{reason}");
    }

    #[test]
    fn the_listing_command_does_not_test_anything() {
        assert_eq!(
            list_spec(&repo_ctx()).display(),
            "cargo mutants --list",
            "checking exclusions must not cost a mutation run"
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
            "cargo mutants --no-shuffle --colors never --jobs 4 --output /repo/target/quality"
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
        let mut script = vec![version()];
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
        assert_eq!(run.lines[1], "git merge-base HEAD main");
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
        assert_eq!(run.lines[3], "git rev-parse HEAD^1");
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
        let runner = ScriptedRunner::new(vec![version(), exited(0, CLEAN)]);
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
}
