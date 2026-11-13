//! Narrowing a diff to the files cargo-gamma can mutate.
//!
//! Gamma refuses a diff that names Rust it does not survey — a file under
//! `tests/`, `benches/` or `examples/`, a `build.rs`, a spike outside the
//! workspace — and refuses one that names nothing it surveys at all, such as a
//! documentation-only change. Both refusals are right for gamma and wrong for
//! this gate: a change to tests or docs is a change with nothing to mutate,
//! which is `skipped`, not a broken instrument.
//!
//! So the gate keeps only the parts of the diff that name a file gamma's own
//! listing puts mutants in. That set comes from gamma rather than from a list
//! of path globs kept here, so it cannot drift from what gamma surveys: a file
//! with no mutants cannot contribute any, and every file that can is kept.

use std::collections::BTreeSet;

/// The parts of `diff` that change one of `mutable`, in their original order.
///
/// A unified diff from `git diff` is a sequence of per-file sections, each
/// opening with a `diff --git a/<path> b/<path>` line. A section is kept when
/// the path it changes *to* is mutable; a deleted file changes to nothing and
/// is dropped, since a file that no longer exists holds no mutants.
pub fn mutable_sections(diff: &str, mutable: &BTreeSet<String>) -> String {
    let mut kept = String::new();
    let mut keeping = false;
    for line in diff.split_inclusive('\n') {
        if let Some(header) = line.strip_prefix("diff --git ") {
            keeping = changed_path(header).is_some_and(|path| mutable.contains(path));
        }
        if keeping {
            kept.push_str(line);
        }
    }
    kept
}

/// The path a `diff --git` header says the file is changed to.
///
/// The header is `a/<old> b/<new>`. Paths with spaces are quoted by git, and
/// no workspace source file has one, so a quoted header is simply not matched
/// — which drops it, the safe direction for a path gamma could not resolve.
fn changed_path(header: &str) -> Option<&str> {
    let header = header.trim_end();
    let (_, new) = header.rsplit_once(" b/")?;
    (!new.starts_with('"')).then_some(new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mutable(paths: &[&str]) -> BTreeSet<String> {
        paths.iter().map(|path| (*path).to_owned()).collect()
    }

    const SOURCE: &str = "diff --git a/crates/a/src/lib.rs b/crates/a/src/lib.rs\n\
        index 1..2 100644\n\
        --- a/crates/a/src/lib.rs\n\
        +++ b/crates/a/src/lib.rs\n\
        @@ -1 +1 @@\n\
        -old\n\
        +new\n";

    const TEST: &str = "diff --git a/crates/a/tests/it.rs b/crates/a/tests/it.rs\n\
        --- a/crates/a/tests/it.rs\n\
        +++ b/crates/a/tests/it.rs\n\
        @@ -1 +1 @@\n\
        +assert!(true);\n";

    const DOCS: &str = "diff --git a/README.md b/README.md\n\
        --- a/README.md\n\
        +++ b/README.md\n\
        @@ -1 +1 @@\n\
        +words\n";

    #[test]
    fn a_section_for_a_mutable_file_is_kept_whole() {
        assert_eq!(
            mutable_sections(SOURCE, &mutable(&["crates/a/src/lib.rs"])),
            SOURCE
        );
    }

    #[test]
    fn sections_for_tests_and_docs_are_dropped_and_the_source_kept() {
        let diff = format!("{DOCS}{SOURCE}{TEST}");
        assert_eq!(
            mutable_sections(&diff, &mutable(&["crates/a/src/lib.rs"])),
            SOURCE
        );
    }

    #[test]
    fn a_diff_with_nothing_mutable_narrows_to_nothing() {
        let diff = format!("{DOCS}{TEST}");
        assert_eq!(
            mutable_sections(&diff, &mutable(&["crates/a/src/lib.rs"])),
            ""
        );
    }

    #[test]
    fn a_renamed_file_is_judged_by_its_new_path() {
        let renamed = "diff --git a/crates/a/src/old.rs b/crates/a/src/new.rs\n\
            similarity index 90%\n\
            rename from crates/a/src/old.rs\n\
            rename to crates/a/src/new.rs\n";
        assert_eq!(
            mutable_sections(renamed, &mutable(&["crates/a/src/new.rs"])),
            renamed
        );
        assert_eq!(
            mutable_sections(renamed, &mutable(&["crates/a/src/old.rs"])),
            ""
        );
    }

    #[test]
    fn a_quoted_path_is_not_matched() {
        let quoted = "diff --git \"a/x y.rs\" \"b/x y.rs\"\n+z\n";
        assert_eq!(mutable_sections(quoted, &mutable(&["x y.rs"])), "");
    }
}
