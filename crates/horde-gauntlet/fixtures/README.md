# Fixtures

Real output from the pinned tools, captured by running each one against a small
throwaway crate in both its clean and its dirty state. The gate tests read these
and call the production reader — no test-side reimplementation of the parsing,
so a tool that changes its output format fails a test here rather than silently
returning zero findings in a real run.

Regenerate a fixture by running the tool at its pinned version (see
[docs/QUALITY_GATES.md](../../../docs/QUALITY_GATES.md)) and replacing the file.
If the numbers change, the assertions that name them have to change too — that
is the point.

Two files are lightly edited, and only in ways that do not touch what is parsed:

- `cargo-fmt-check.txt`, `cargo-clippy-denied.txt`, `cargo-*test*.txt` — the
  capture machine's absolute paths were rewritten to `/repo`.
- `jscpd-report-duplicated.json` — the `fragment` field (the full duplicated
  source text) was replaced with a placeholder to keep the file small, and the
  two file paths were shortened. Every counted field is as jscpd produced it.
