//! Proving what the mutation run leaves out, before it runs.
//!
//! A mutation gate can be hollowed out without a single test changing: widen
//! an exclusion, add a suppression, and the score rises while the coverage it
//! claims shrinks. So nothing that removes a mutant is trusted on its say-so.
//! Every removal is measured — gamma lists the mutants with the configuration
//! and without it, and the difference is what the configuration removed — and
//! every removal has to account for itself.
//!
//! Three things can remove a mutant, and each has its own check:
//!
//! * an `exclude-files` entry in `gamma.toml` (`verify_exclusions`);
//! * a `// #[gamma::skip(..)]` directive in the source (`verify_suppressions`);
//! * and one removal that is not a choice but a hazard: a file whose mutants
//!   must never run at all (`refuse_dangerous`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde::Deserialize;

/// Where cargo-gamma's configuration, and so its exclusions, live.
pub const CONFIG: &str = "gamma.toml";

/// Files a mutant of which must never run.
///
/// `reap.rs` sends real signals to processes chosen at runtime. A mutant of an
/// earlier version flipped `ppid == parent` and SIGKILLed every process on the
/// machine. Gamma compiles every mutant of a file it discovers into the test
/// binary as a runtime switch, so the file must not be discovered at all; an
/// `exclude-files` entry drops it before it is parsed. This list is how the
/// gate holds that line rather than trusting the configuration to: a file
/// here that exists must be excluded by name, and no listing the gate takes may
/// contain a mutant in it, or nothing runs.
pub const DANGEROUS: &[&str] = &["crates/horde-server-testkit/src/reap.rs"];

/// One mutant as `cargo gamma list --json` describes it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Listed {
    /// Workspace-relative path of the mutated file.
    pub file: String,
    /// The package that owns the file.
    pub package: String,
    /// Line the mutated expression starts on.
    pub line: u32,
    /// Column the mutated expression starts at.
    pub column: u32,
    /// The mutator, e.g. `relational.gt_to_ge`.
    pub mutator: String,
    /// The function or item the mutant is in.
    pub item_path: String,
    /// What the expression becomes.
    pub replacement: String,
    /// Why gamma set the mutant aside, when a directive did.
    #[serde(default)]
    pub suppression: Option<Suppression>,
}

impl Listed {
    /// What identifies a mutant across two listings.
    ///
    /// Not gamma's `id`: that hash also covers the mutant's position among its
    /// site's replacements, which shifts when a sibling replacement is not
    /// generated, and the same mutant would then look removed in one listing
    /// and added in the other.
    fn key(&self) -> (&str, u32, u32, &str, &str) {
        (
            &self.file,
            self.line,
            self.column,
            &self.mutator,
            &self.replacement,
        )
    }

    /// The mutant, as a line someone can find.
    pub fn describe(&self) -> String {
        format!(
            "{}:{}:{}: {} in {} -> {}",
            self.file, self.line, self.column, self.mutator, self.item_path, self.replacement
        )
    }
}

/// A source directive's mark on a mutant.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Suppression {
    /// `Comment`, `Attribute` or `Config`.
    pub channel: String,
    /// The reason written in the directive.
    pub reason: Option<String>,
    /// The line the directive is on.
    pub line: Option<usize>,
}

/// Reads `cargo gamma list --json`.
///
/// # Errors
///
/// Returns a message when the output is not a listing this gate understands.
pub fn parse_listing(text: &str) -> Result<Vec<Listed>, String> {
    serde_json::from_str(text).map_err(|error| {
        format!("`cargo gamma list --json` printed something other than a mutant listing: {error}")
    })
}

/// The files a listing puts at least one mutant in.
pub fn files_of(listed: &[Listed]) -> BTreeSet<String> {
    listed.iter().map(|mutant| mutant.file.clone()).collect()
}

/// The packages a listing puts at least one mutant in.
pub fn packages_of(listed: &[Listed]) -> Vec<String> {
    let packages: BTreeSet<&str> = listed
        .iter()
        .map(|mutant| mutant.package.as_str())
        .collect();
    packages.into_iter().map(str::to_owned).collect()
}

/// Refuses a listing that contains a mutant of a [`DANGEROUS`] file.
///
/// # Errors
///
/// Returns a message naming the file and the listing it appeared in.
pub fn refuse_dangerous(listed: &[Listed], which: &str) -> Result<(), String> {
    match listed
        .iter()
        .find(|mutant| DANGEROUS.contains(&mutant.file.as_str()))
    {
        None => Ok(()),
        Some(mutant) => Err(format!(
            "REFUSING TO MUTATE: {which} contains `{}`. {} sends real signals to processes \
             chosen at runtime, and a mutant of it once SIGKILLed every process on the machine; \
             it must stay out of discovery entirely (an `exclude-files` entry in {CONFIG}). \
             Nothing was built or run",
            mutant.describe(),
            mutant.file
        )),
    }
}

/// One `exclude-files` entry, with the comment that justifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exclusion {
    /// The entry, exactly as gamma will see it.
    pub pattern: String,
    /// The comment written above or beside it.
    pub why: String,
}

/// The `exclude-files` entries configured for cargo-gamma.
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

/// The body of the `exclude-files` array, comments and all.
///
/// Comments are kept because an exclusion is worth no more than the reason
/// written beside it. That means the closing `]` has to be found the way TOML
/// finds it — in code, not inside a comment that happens to mention one.
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
    Err(format!(
        "{CONFIG}: `exclude-files` is missing its closing `]`"
    ))
}

/// A line split into its TOML code and the comment trailing it.
///
/// A `#` inside a quoted string is content rather than a comment, so the split
/// tracks which quote is open.
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
/// several lines stays attached to the one path it argues for — and why a
/// reason written once above two entries belongs to both.
fn entries(body: &str) -> Vec<Exclusion> {
    let mut found = Vec::new();
    let mut pending: Vec<&str> = Vec::new();
    let mut last_why = String::new();
    for line in body.lines() {
        let (code, comment) = split_comment(line);
        let patterns = toml_strings(code);
        if patterns.is_empty() {
            pending.extend(comment);
            continue;
        }
        let written = pending
            .iter()
            .copied()
            .chain(comment)
            .collect::<Vec<_>>()
            .join(" ");
        // An entry directly under another, with no comment of its own, shares
        // the reason above them both.
        let why = if written.is_empty() {
            last_why.clone()
        } else {
            written
        };
        found.extend(patterns.into_iter().map(|pattern| Exclusion {
            pattern,
            why: why.clone(),
        }));
        last_why = why;
        pending.clear();
    }
    found
}

fn is_exclusion_key(line: &str) -> bool {
    // A commented-out key does not start with the key, so `#` needs no test of
    // its own; the caller trims first.
    let Some(rest) = line.strip_prefix("exclude-files") else {
        return false;
    };
    rest.trim_start().starts_with('=')
}

/// The TOML strings in an array body, in either quoting style.
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
            found.push(value.to_owned());
        }
        rest = after.get(close + 1..).unwrap_or("");
    }
    found
}

/// The one path an exclusion names.
///
/// `exclude-files` takes globs, and a glob's reach is a question only a glob
/// engine can answer — which is how `src/bin/**` once took a product binary
/// out with a fixture. So an entry here must be one literal path, which the
/// gate can check against the tree and against the listings by looking.
///
/// # Errors
///
/// Returns a message when the entry carries glob syntax.
pub fn literal_path(pattern: &str) -> Result<&str, String> {
    if pattern.is_empty()
        || pattern.contains(['*', '?', '[', ']', '{', '}', '!', '\\'])
        || !pattern.ends_with(".rs")
    {
        return Err(format!(
            "{CONFIG}: `{pattern}` is not one literal path to a Rust file. An exclusion must name \
             exactly one file, written from the workspace root, so the gate can check what it \
             actually removed"
        ));
    }
    Ok(pattern)
}

/// Confirms every exclusion names a real file, says why, and removed exactly
/// that file's mutants.
///
/// `configured` and `unconfigured` are gamma's two listings of the whole
/// workspace, with `gamma.toml` and with `--no-config`. Their difference is
/// what the configuration removed. Five lies are refused:
///
/// * *It names nothing.* An entry for a path no longer in the tree reads like
///   one still doing its job. (Gamma refuses a glob matching nothing too; the
///   gate's own check names the entry.)
/// * *It says nothing.* An exclusion is an argument — this file is a fixture
///   the suite spawns, or this file is dangerous to mutate — and an argument
///   nobody wrote down is one nobody can check.
/// * *It did not take.* A mutant of the file is still listed.
/// * *It removed nothing.* The two listings agree about the file, so the entry
///   is inert, and an inert entry reads exactly like one holding something
///   back.
/// * *Something else was removed.* A mutant outside every excluded file is
///   missing from the configured listing, so the configuration takes more out
///   of scope than it reads — or a mutant was *added*, so it does more than
///   exclude.
///
/// One difference is not a lie, and is accounted for by name: gamma decides
/// whether a type implements `Default` from the declarations it parses, by
/// name, across the workspace. An excluded file is not parsed, so when it
/// declares a same-named type that derives `Default`, gamma stops generating
/// `Default::default()` replacements for the other type, which would not have
/// compiled anyway. Those removals are recognised by their replacement and
/// returned, so the gate can say how many there were.
///
/// # Errors
///
/// Returns a message naming the entry, or the mutant, and which lie it is.
pub fn verify_exclusions(
    entries: &[Exclusion],
    configured: &[Listed],
    unconfigured: &[Listed],
    exists: impl Fn(&str) -> bool,
) -> Result<usize, String> {
    let mut excluded = BTreeSet::new();
    for entry in entries {
        let path = literal_path(&entry.pattern)?;
        check_entry(entry, path, configured, unconfigured, &exists)?;
        excluded.insert(path);
    }
    for path in DANGEROUS {
        if exists(path) && !excluded.contains(path) {
            return Err(format!(
                "REFUSING TO MUTATE: {path} is not excluded in {CONFIG}. It sends real signals to \
                 processes chosen at runtime, and a mutant of it once SIGKILLed every process on \
                 the machine; it must be an `exclude-files` entry of its own"
            ));
        }
    }
    let kept: BTreeSet<_> = configured.iter().map(Listed::key).collect();
    let all: BTreeSet<_> = unconfigured.iter().map(Listed::key).collect();
    if let Some(added) = configured
        .iter()
        .find(|mutant| !all.contains(&mutant.key()))
    {
        return Err(format!(
            "{CONFIG}: `{}` is listed only with the configuration — it adds mutants rather than \
             only excluding them, and the gate vouches for exclusions alone",
            added.describe()
        ));
    }
    let mut inferred = 0;
    for mutant in unconfigured
        .iter()
        .filter(|mutant| !kept.contains(&mutant.key()))
    {
        if excluded.contains(mutant.file.as_str()) {
            continue;
        }
        if mutant.replacement.contains("Default::default()") {
            inferred += 1;
            continue;
        }
        return Err(format!(
            "{CONFIG}: `{}` was excluded, but no entry names its file — the configuration takes \
             more out of scope than it says",
            mutant.describe()
        ));
    }
    Ok(inferred)
}

/// Everything one entry has to prove about itself.
fn check_entry(
    entry: &Exclusion,
    path: &str,
    configured: &[Listed],
    unconfigured: &[Listed],
    exists: &impl Fn(&str) -> bool,
) -> Result<(), String> {
    let pattern = &entry.pattern;
    if !exists(path) {
        return Err(format!(
            "{CONFIG}: `{pattern}` is not in the tree — an exclusion that names nothing \
             misstates what is measured"
        ));
    }
    if entry.why.trim().is_empty() {
        return Err(format!(
            "{CONFIG}: `{pattern}` is excluded with no reason beside it. A file is excluded \
             only when it is a fixture the suite spawns rather than tests, or when a mutant of \
             it is dangerous to run, and which one belongs in a comment"
        ));
    }
    if let Some(mutant) = configured.iter().find(|mutant| mutant.file == path) {
        return Err(format!(
            "{CONFIG}: `{pattern}` did not take — gamma still lists `{}`. Configured is not \
             applied",
            mutant.describe()
        ));
    }
    if !unconfigured.iter().any(|mutant| mutant.file == path) {
        return Err(format!(
            "{CONFIG}: `{pattern}` removed no mutant at all — an exclusion nothing depends on \
             reads exactly like one that is holding something back"
        ));
    }
    Ok(())
}

/// A `gamma::skip` directive found in the source.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Directive {
    /// Workspace-relative path of the file it is in.
    pub file: String,
    /// The line it is on, from 1.
    pub line: usize,
}

/// The marker every source directive carries.
const DIRECTIVE: &str = "#[gamma::skip(";

/// The lines of `text` that carry a `gamma::skip` directive, from 1.
///
/// A directive is a comment — `// #[gamma::skip(..)]`, on its own line or
/// trailing code — or a real attribute on its own line. Text that only quotes
/// one, inside a string literal, is not a directive: an odd number of `"`
/// before the marker means the marker is inside a string.
pub fn directive_lines(text: &str) -> Vec<usize> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| is_directive(line))
        .map(|(index, _)| index + 1)
        .collect()
}

fn is_directive(line: &str) -> bool {
    if line.trim_start().starts_with(DIRECTIVE) {
        return true;
    }
    let Some(at) = line.find(&format!("// {DIRECTIVE}")) else {
        return false;
    };
    // Code before the comment, not a comment already (a doc comment that
    // mentions a directive) and not an open string literal (a test that
    // quotes one).
    let before = &line[..at];
    !before.contains("//") && before.matches('"').count().is_multiple_of(2)
}

/// Every directive in the workspace's sources.
///
/// # Errors
///
/// Returns a message when a source directory or file cannot be read.
pub fn scan_directives(root: &Path) -> Result<Vec<Directive>, String> {
    let mut found = Vec::new();
    let crates = root.join("crates");
    if crates.is_dir() {
        walk(root, &crates, &mut found)?;
    }
    found.sort();
    Ok(found)
}

fn walk(root: &Path, directory: &Path, found: &mut Vec<Directive>) -> Result<(), String> {
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("could not read {}: {error}", directory.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|error| format!("could not read {}: {error}", directory.display()))?
            .path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            walk(root, &path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let text = std::fs::read_to_string(&path)
                .map_err(|error| format!("could not read {}: {error}", path.display()))?;
            let file = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            found.extend(directive_lines(&text).into_iter().map(|line| Directive {
                file: file.clone(),
                line,
            }));
        }
    }
    Ok(())
}

/// Confirms every suppressed mutant was suppressed in the source, with a
/// reason, and every directive still suppresses something.
///
/// A suppression is an equivalence claim — this mutant is the original
/// expression in another spelling, so no test could tell them apart — and
/// three things are refused:
///
/// * *A suppression without a reason.* Equivalence is an argument, and nothing
///   in the tree records it unless the directive does.
/// * *A suppression from the configuration.* `gamma.toml` may exclude files and
///   nothing else; a mutant is suppressed at the expression, where the next
///   person to change the expression sees the claim.
/// * *A directive that suppresses nothing.* The expression it argued about has
///   changed or gone, and a stale claim reads exactly like a live one.
///
/// Returns how many mutants are suppressed.
///
/// # Errors
///
/// Returns a message naming the mutant or the directive.
pub fn verify_suppressions(
    configured: &[Listed],
    directives: &[Directive],
) -> Result<usize, String> {
    let mut used: BTreeMap<(&str, usize), usize> = BTreeMap::new();
    let mut suppressed = 0;
    for mutant in configured {
        let Some(suppression) = &mutant.suppression else {
            continue;
        };
        suppressed += 1;
        if suppression.channel == "Config" {
            return Err(format!(
                "`{}` is suppressed by the configuration. {CONFIG} excludes files only; an \
                 equivalent mutant is suppressed in the source, beside the expression it argues \
                 about",
                mutant.describe()
            ));
        }
        if suppression
            .reason
            .as_deref()
            .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err(format!(
                "`{}` is suppressed with no reason. A mutant is suppressed only when it is \
                 provably the original expression in another spelling, and that argument goes in \
                 the directive's `reason = \"...\"`",
                mutant.describe()
            ));
        }
        if let Some(line) = suppression.line {
            *used.entry((mutant.file.as_str(), line)).or_default() += 1;
        }
    }
    if let Some(stale) = directives
        .iter()
        .find(|directive| !used.contains_key(&(directive.file.as_str(), directive.line)))
    {
        return Err(format!(
            "{}:{}: this gamma::skip directive suppresses no mutant. The expression it argued \
             about has changed or gone (or its file is excluded, or not mutated at all); delete \
             it, or `cargo gamma unsuppress` will",
            stale.file, stale.line
        ));
    }
    Ok(suppressed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real listing: `cargo gamma list --json --package horde-observe`,
    /// trimmed to three mutants.
    const LISTING: &str = include_str!("../../../fixtures/gamma-list.json");

    fn mutant(file: &str, replacement: &str) -> Listed {
        Listed {
            file: file.to_owned(),
            package: "p".to_owned(),
            line: 3,
            column: 5,
            mutator: "fn_value.default".to_owned(),
            item_path: "Thing::make".to_owned(),
            replacement: replacement.to_owned(),
            suppression: None,
        }
    }

    fn suppressed(file: &str, line: usize, reason: Option<&str>, channel: &str) -> Listed {
        Listed {
            suppression: Some(Suppression {
                channel: channel.to_owned(),
                reason: reason.map(str::to_owned),
                line: Some(line),
            }),
            ..mutant(file, "0")
        }
    }

    fn justified(path: &str) -> Exclusion {
        Exclusion {
            pattern: path.to_owned(),
            why: "the suite spawns it".to_owned(),
        }
    }

    /// A tree in which every path exists except the reaper, as in a workspace
    /// that is not this one.
    fn not_the_reaper(path: &str) -> bool {
        path != REAP
    }

    const PRODUCT: &str = "crates/a/src/lib.rs";
    const FIXTURE: &str = "crates/a/src/bin/stub.rs";
    const REAP: &str = "crates/horde-server-testkit/src/reap.rs";

    #[test]
    fn a_real_listing_is_read() {
        let listed = parse_listing(LISTING).expect("the fixture is real gamma output");
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].file, "crates/horde-observe/src/config.rs");
        assert_eq!(listed[0].package, "horde-observe");
        assert!(listed.iter().all(|mutant| mutant.suppression.is_none()));
    }

    #[test]
    fn output_that_is_not_a_listing_is_refused() {
        assert!(parse_listing("{}").is_err());
        assert!(parse_listing("warning: something").is_err());
    }

    #[test]
    fn files_and_packages_are_collected_once_each_in_order() {
        let mut other = mutant(FIXTURE, "()");
        other.package = "a".to_owned();
        let listed = [mutant(PRODUCT, "0"), mutant(PRODUCT, "1"), other];
        assert_eq!(
            files_of(&listed).into_iter().collect::<Vec<_>>(),
            vec![FIXTURE.to_owned(), PRODUCT.to_owned()]
        );
        assert_eq!(packages_of(&listed), vec!["a".to_owned(), "p".to_owned()]);
    }

    #[test]
    fn a_listing_with_a_mutant_of_the_reaper_is_refused() {
        let message = refuse_dangerous(
            &[mutant(PRODUCT, "0"), mutant(REAP, "()")],
            "the run's listing",
        )
        .expect_err("a mutant of the reaper must never run");
        assert!(message.starts_with("REFUSING TO MUTATE"), "{message}");
        assert!(message.contains("the run's listing"), "{message}");
        assert!(message.contains(REAP), "{message}");
    }

    #[test]
    fn a_listing_without_the_reaper_is_accepted() {
        assert_eq!(refuse_dangerous(&[mutant(PRODUCT, "0")], "x"), Ok(()));
    }

    #[test]
    fn the_dangerous_files_exist_in_this_workspace() {
        // A rename would leave the list naming nothing, and the guard with it.
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        for path in DANGEROUS {
            assert!(root.join(path).is_file(), "{path} is no longer in the tree");
        }
    }

    #[test]
    fn the_committed_config_excludes_every_dangerous_file() {
        let config = include_str!("../../../../../gamma.toml");
        let entries = configured_exclusions(config).expect("the committed config is readable");
        for path in DANGEROUS {
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.pattern == *path && !entry.why.is_empty()),
                "{path} must be excluded, with its reason"
            );
        }
    }

    #[test]
    fn exclusions_are_read_with_the_comment_above_them() {
        let config = "profile = \"mutants\"\nexclude-files = [\n    # spawned by the suite\n    \
                      \"a/src/bin/x.rs\",\n    \"a/src/bin/y.rs\", # also spawned\n]\nmin-score = 0.0\n";
        assert_eq!(
            configured_exclusions(config),
            Ok(vec![
                Exclusion {
                    pattern: "a/src/bin/x.rs".to_owned(),
                    why: "spawned by the suite".to_owned()
                },
                Exclusion {
                    pattern: "a/src/bin/y.rs".to_owned(),
                    why: "also spawned".to_owned()
                },
            ])
        );
    }

    #[test]
    fn an_entry_directly_under_another_shares_its_reason() {
        let config = "exclude-files = [\n    # both spawned\n    'a.rs',\n    'b.rs',\n]\n";
        let found = configured_exclusions(config).expect("readable");
        assert_eq!(found[1].why, "both spawned", "{found:?}");
    }

    #[test]
    fn a_config_without_exclusions_has_none() {
        assert_eq!(
            configured_exclusions("profile = \"mutants\"\n"),
            Ok(Vec::new())
        );
        assert_eq!(
            configured_exclusions("exclude-files = []\n"),
            Ok(Vec::new())
        );
        assert_eq!(
            configured_exclusions("# exclude-files = ['ghost.rs']\n"),
            Ok(Vec::new())
        );
    }

    #[test]
    fn an_unterminated_list_is_refused_rather_than_read_as_empty() {
        let message =
            configured_exclusions("exclude-files = [\"a.rs\",\n").expect_err("unreadable");
        assert!(message.contains("closing"), "{message}");
    }

    #[test]
    fn a_bracket_or_hash_inside_a_comment_or_string_is_not_syntax() {
        let config = "exclude-files = [\n    # see [1]\n    'a#b.rs',\n]\n";
        let found = configured_exclusions(config).expect("readable");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pattern, "a#b.rs");
    }

    #[test]
    fn a_key_that_merely_starts_with_the_name_is_not_the_key() {
        let config = "exclude-files-why = 'x'\nexclude-files = ['a.rs']\n";
        let found = configured_exclusions(config).expect("readable");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].pattern, "a.rs");
    }

    #[test]
    fn a_glob_or_a_non_rust_path_is_not_a_literal_path() {
        for pattern in [
            "src/bin/**",
            "src/*.rs",
            "src/[ab].rs",
            "src/{a,b}.rs",
            "!a.rs",
            "src/",
            "",
        ] {
            let message = literal_path(pattern).expect_err("only a literal path can be checked");
            assert!(message.contains("one literal path"), "{pattern}: {message}");
        }
        assert_eq!(literal_path(FIXTURE), Ok(FIXTURE));
    }

    #[test]
    fn an_exclusion_that_took_is_accepted() {
        let configured = [mutant(PRODUCT, "0")];
        let unconfigured = [mutant(PRODUCT, "0"), mutant(FIXTURE, "()")];
        assert_eq!(
            verify_exclusions(
                &[justified(FIXTURE)],
                &configured,
                &unconfigured,
                not_the_reaper
            ),
            Ok(0)
        );
    }

    #[test]
    fn an_exclusion_naming_nothing_is_refused() {
        let message = verify_exclusions(&[justified(FIXTURE)], &[], &[], |_| false)
            .expect_err("not in the tree");
        assert!(message.contains("not in the tree"), "{message}");
    }

    #[test]
    fn an_exclusion_without_a_reason_is_refused() {
        let entry = Exclusion {
            pattern: FIXTURE.to_owned(),
            why: " ".to_owned(),
        };
        let message = verify_exclusions(&[entry], &[], &[mutant(FIXTURE, "()")], not_the_reaper)
            .expect_err("an exclusion is an argument");
        assert!(message.contains("no reason beside it"), "{message}");
    }

    #[test]
    fn an_exclusion_that_did_not_take_is_refused() {
        let listed = [mutant(FIXTURE, "()")];
        let message = verify_exclusions(&[justified(FIXTURE)], &listed, &listed, not_the_reaper)
            .expect_err("still listed");
        assert!(message.contains("did not take"), "{message}");
    }

    #[test]
    fn an_exclusion_that_removed_nothing_is_refused() {
        let listed = [mutant(PRODUCT, "0")];
        let message = verify_exclusions(&[justified(FIXTURE)], &listed, &listed, not_the_reaper)
            .expect_err("inert");
        assert!(message.contains("removed no mutant"), "{message}");
    }

    #[test]
    fn a_removal_no_entry_names_is_refused() {
        let unconfigured = [mutant(FIXTURE, "()"), mutant(PRODUCT, "0")];
        let message = verify_exclusions(&[justified(FIXTURE)], &[], &unconfigured, not_the_reaper)
            .expect_err("wider than it reads");
        assert!(message.contains("no entry names its file"), "{message}");
    }

    #[test]
    fn a_mutant_only_the_configuration_lists_is_refused() {
        let configured = [mutant(PRODUCT, "1")];
        let unconfigured = [mutant(PRODUCT, "0"), mutant(FIXTURE, "()")];
        let message = verify_exclusions(
            &[justified(FIXTURE)],
            &configured,
            &unconfigured,
            not_the_reaper,
        )
        .expect_err("adds rather than excludes");
        assert!(message.contains("adds mutants"), "{message}");
    }

    #[test]
    fn a_default_replacement_lost_to_type_inference_is_accounted_for_and_counted() {
        // Observed for real: excluding the fault suite's worker-runner, which
        // declares an `Invocation` deriving `Default`, stops gamma generating
        // `Ok(Default::default())` for horde-server's unrelated `Invocation`.
        let unconfigured = [
            mutant(FIXTURE, "()"),
            mutant(PRODUCT, "Ok(Default::default())"),
        ];
        assert_eq!(
            verify_exclusions(&[justified(FIXTURE)], &[], &unconfigured, not_the_reaper),
            Ok(1)
        );
    }

    #[test]
    fn the_reaper_must_be_excluded_by_name_when_it_exists() {
        let unconfigured = [mutant(FIXTURE, "()")];
        let message = verify_exclusions(&[justified(FIXTURE)], &[], &unconfigured, |_| true)
            .expect_err("the reaper exists and is not excluded");
        assert!(message.starts_with("REFUSING TO MUTATE"), "{message}");
        assert!(message.contains(REAP), "{message}");
    }

    #[test]
    fn excluding_the_reaper_satisfies_the_guard() {
        let unconfigured = [mutant(REAP, "()")];
        assert_eq!(
            verify_exclusions(&[justified(REAP)], &[], &unconfigured, |_| true),
            Ok(0)
        );
    }

    #[test]
    fn directives_are_found_on_their_own_line_trailing_code_or_as_attributes() {
        // Built from the marker so that this file's own lines carry none.
        let text = [
            "fn a() {}".to_owned(),
            format!("    // {DIRECTIVE}fn_value.default, reason = \"same\")]"),
            format!("let x = 1; // {DIRECTIVE}arith)]"),
            format!("{DIRECTIVE}arith)]"),
            format!("let s = \"// {DIRECTIVE}arith)]\";"),
            format!("/// doc mentioning // {DIRECTIVE}arith)]"),
            format!("//! crate doc mentioning `// {DIRECTIVE}arith)]`"),
        ]
        .join("\n");
        assert_eq!(directive_lines(&text), vec![2, 3, 4]);
    }

    #[test]
    fn directives_are_scanned_from_the_workspace_sources() {
        let dir = tempfile::tempdir().expect("temp dir");
        let src = dir.path().join("crates").join("a").join("src");
        std::fs::create_dir_all(&src).expect("dirs");
        std::fs::create_dir_all(dir.path().join("crates").join("a").join("target")).expect("dirs");
        let directive = format!("// {DIRECTIVE}arith, reason = \"r\")]\n");
        std::fs::write(
            src.join("lib.rs"),
            format!("fn f() {{}}\n{directive}fn g() {{}}\n"),
        )
        .expect("write");
        std::fs::write(src.join("notes.md"), &directive).expect("write");
        std::fs::write(
            dir.path()
                .join("crates")
                .join("a")
                .join("target")
                .join("gen.rs"),
            &directive,
        )
        .expect("write");
        assert_eq!(
            scan_directives(dir.path()),
            Ok(vec![Directive {
                file: "crates/a/src/lib.rs".to_owned(),
                line: 2
            }])
        );
    }

    #[test]
    fn a_workspace_with_no_crates_directory_has_no_directives() {
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(scan_directives(dir.path()), Ok(Vec::new()));
    }

    fn directive(file: &str, line: usize) -> Directive {
        Directive {
            file: file.to_owned(),
            line,
        }
    }

    #[test]
    fn a_reasoned_source_suppression_that_is_in_use_is_accepted() {
        let listed = [
            mutant(PRODUCT, "1"),
            suppressed(PRODUCT, 7, Some("it is the same expression"), "Comment"),
        ];
        assert_eq!(
            verify_suppressions(&listed, &[directive(PRODUCT, 7)]),
            Ok(1)
        );
    }

    #[test]
    fn a_suppression_without_a_reason_is_refused() {
        for reason in [None, Some("  ")] {
            let listed = [suppressed(PRODUCT, 7, reason, "Comment")];
            let message = verify_suppressions(&listed, &[directive(PRODUCT, 7)])
                .expect_err("equivalence is an argument");
            assert!(message.contains("no reason"), "{message}");
        }
    }

    #[test]
    fn a_suppression_from_the_configuration_is_refused() {
        let listed = [suppressed(PRODUCT, 7, Some("why"), "Config")];
        let message = verify_suppressions(&listed, &[]).expect_err("config excludes files only");
        assert!(
            message.contains("suppressed by the configuration"),
            "{message}"
        );
    }

    #[test]
    fn a_directive_that_suppresses_nothing_is_refused() {
        let listed = [suppressed(PRODUCT, 7, Some("why"), "Attribute")];
        let message =
            verify_suppressions(&listed, &[directive(PRODUCT, 7), directive(PRODUCT, 20)])
                .expect_err("stale");
        assert!(message.starts_with("crates/a/src/lib.rs:20:"), "{message}");
        assert!(message.contains("suppresses no mutant"), "{message}");
    }

    #[test]
    fn nothing_suppressed_and_no_directives_is_zero() {
        assert_eq!(verify_suppressions(&[mutant(PRODUCT, "0")], &[]), Ok(0));
    }
}
