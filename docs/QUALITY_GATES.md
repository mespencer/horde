# Horde — Quality Gates

The gauntlet is Horde's only enforcement mechanism (IMPLEMENTATION.md): there are no
PRs and no hosted CI, so `gauntlet run` on `main` after every merge *is* the quality
bar. This document is the companion to [TESTING.md](./TESTING.md): TESTING.md says why
the suite exists, this says what each gate does and how to read it.

Every gate is **blocking**. This workspace started from zero code, so there is no
existing debt to grandfather and no gate needs an advisory stage to calibrate against.
The rollout ladder in TESTING.md §4 (observe → ratchet → hard gate) still exists in the
code — `Enforcement` has all three rungs — for gates added later against a tree that
already has history.

## Reading a run

```
$ gauntlet run                       # every gate
$ gauntlet run crap                  # one gate, plus whatever it depends on
$ gauntlet run clippy -p horde-proto # narrow to a package
$ gauntlet run --full                # mutants measures the whole tree, not just the change
$ gauntlet list                      # the registry, with enforcement levels
$ gauntlet report --format markdown  # re-render the last run
```

`run` prints a table and exits non-zero if the run is red. Everything each gate ran,
and everything the tools printed, is kept in `target/quality/<gate>.log`; the machine
readable results are in `target/quality/results.json`.

The result column has five values, and the differences between them are the point:

| result | meaning | effect on the run |
|---|---|---|
| `pass` | measured, and clean | — |
| `fail` | measured, and the code does not meet the bar | red (for a blocking gate) |
| `skipped` | there was nothing to measure | — |
| `error` | the tooling could not measure | **red, always** |
| `no result` | the gate never reported at all | **red, always** |

`error` is not a worse `fail`. A `fail` is a verdict about the code; an `error` means
the instrument is broken, and a broken instrument tells you nothing — so it is fatal
regardless of the gate's enforcement level. `skipped` is not a `pass`: a zero has to
say which of its two meanings it has, so "jscpd scanned no files" can never be read as
"there is no duplication". `no result` exists because a gate that silently vanishes is
how a quality layer dies without anyone noticing; results are written to disk before the
first gate runs and again after each one, so a run that dies part-way still renders every
gate it was going to run.

## Pinned tool versions

An unpinned tool silently moves a calibration baseline, so each gate declares the exact
version its thresholds were chosen against and errors — with the install command — on a
mismatch. `cargo fmt`, `cargo clippy` and `cargo test` are pinned by
`rust-toolchain.toml` instead, which is why they carry no separate pin.

| tool | version | install |
|---|---|---|
| Rust toolchain | 1.95.0 | `rust-toolchain.toml` (rustup installs it) |
| cargo-nextest | 0.9.82 | `cargo install --locked cargo-nextest@0.9.82` |
| cargo-llvm-cov | 0.8.7 | `cargo install --locked cargo-llvm-cov@0.8.7` |
| cargo-crap | 0.3.1 | `cargo install --locked cargo-crap@0.3.1` |
| cargo-machete | 0.9.2 | `cargo install --locked cargo-machete@0.9.2` |
| cargo-modules | 0.26.0 | `cargo install --locked cargo-modules@0.26.0` |
| cargo-mutants | 27.1.0 | `cargo install --locked cargo-mutants@27.1.0` |
| jscpd | 5.0.14 | Node.js 18+; `npx` fetches `jscpd@5.0.14` itself |
| buf | 1.69.0 | `brew install bufbuild/buf/buf`, or https://buf.build/docs/installation |

---

## fmt

**What.** Every tracked source file is already rustfmt-clean, at the default rustfmt
configuration (the repo deliberately has no `rustfmt.toml`).

**Why.** Formatting arguments are the cheapest possible review noise, and diff noise is
where real changes hide. Agents in particular reformat surrounding code as a side effect
of editing it; a clean baseline makes that visible instead of invisible.

**Local.** `gauntlet run fmt`, or `cargo fmt --all -- --check`. To fix: `cargo fmt --all`.

**How to read.** A `fail` lists the distinct files rustfmt would rewrite — files, not
hunks, since rustfmt prints one header per hunk. If rustfmt exits non-zero without any
parseable `Diff in …` headers the gate reports `error`, because it cannot tell a
formatting verdict from a broken rustfmt.

## clippy

**What.** `cargo clippy --all-targets --all-features -- -D warnings` across the selected
packages. This is also where the workspace lint policy becomes fatal: `unwrap_used`,
`expect_used`, `panic`, `todo`, `unimplemented` and `dbg_macro` are denied in
`Cargo.toml`, and `clippy.toml` allows `expect`/`panic` in tests while keeping `unwrap`
banned **everywhere, tests included**.

**Why.** `unwrap()` in agent-written code is the single most common way a plausible
diff becomes a crash under an input nobody tried. Denying it in the manifest and
`-D warnings` here means the policy lives in exactly one place each: level in the
manifest, severity in this gate.

**`expect` in integration tests — the friction, and the fix that is not a weakening.**
`allow-expect-in-tests` exempts anything under `#[cfg(test)]`. It is not restricted to
functions carrying `#[test]`, which is the natural guess and is wrong in a way that
matters: helper functions in `tests/*.rs` trip `expect_used` not because they lack a
`#[test]` attribute but because an integration test file's free functions sit outside any
`cfg(test)` module. The fix is to put the helpers in one:

```rust
#[cfg(test)]
mod harness {
    pub(crate) fn thing() -> Thing {
        Thing::new().expect("...")
    }
}
```

That is the canonical fix. It states what the helpers already are, it runs under the
unmodified deny-list, and it costs one line. Rewriting helpers to be assertion-based so
they never need `expect` is an acceptable alternative — it is what W1b did — but it is
not required, and reaching for it because `expect` seems banned is treating a
misdiagnosis as a constraint.

What must **not** happen is `#![allow(clippy::expect_used)]` at the top of the file. The
policy is that `expect` is fine *in tests*; the `cfg(test)` wrapper says exactly that,
whereas a blanket allow also covers any non-test code that later lands in the file.

**Local.** `gauntlet run clippy`, or
`cargo clippy --all-targets --all-features -- -D warnings`.

**How to read.** A `fail` gives the number of diagnostics; the messages themselves are in
`target/quality/clippy.log`. Cargo's own tallies (`could not compile …`, `aborting due
to …`) are excluded from the count so it reflects distinct findings. An exit code other
than 0 or 101 is an `error`, not a lint verdict.

## test

**What.** The whole suite, all features: `cargo nextest run` if nextest is installed at
its pinned version, `cargo test --all-targets` otherwise, plus `cargo test --doc` either
way (nextest cannot run doctests).

**Why.** Obvious as a gate; less obvious as a *measurement*. The gate parses the harness's
own tally rather than trusting the exit code, so "the build failed and nothing ran"
becomes an `error` instead of a green row, and a run with no tests at all becomes
`skipped` instead of `pass`.

**Local.** `gauntlet run test`, or `cargo nextest run --all-features --workspace`.

**How to read.** A `pass` shows the test and doctest counts and names the harness, so a
silent fall back to `cargo test` is visible in the row. Ignored tests are reported in the
detail rather than being folded into the pass. An installed-but-wrong-version nextest is
an `error`; an *absent* nextest is a supported fallback and is not.

## coverage

**What.** `cargo llvm-cov` over the whole workspace, all features, writing LCOV to
`target/quality/lcov.info` — which is also the input the `crap` gate scores against.

**Why.** Two jobs. It produces the artifact CRAP needs, and it records the number so a
threshold can eventually be chosen from evidence rather than from a default.

**Blocking with no threshold.** There is deliberately no minimum percentage yet.
"Blocking" here means the measurement must *happen*: if llvm-cov cannot run, or leaves no
readable LCOV, the gate is an `error` and the run is red. TESTING.md §4 is explicit that
an absolute threshold is chosen from evidence, and on day one there is no evidence. This
is the one gate whose bar is expected to tighten later, and doing so is a deliberate,
reviewable one-line change.

**Local.** `gauntlet run coverage`. For a browsable report:
`cargo llvm-cov --all-features --workspace --html`.

**How to read.** The detail is `lines <pct>% (<hit>/<found>)`. Watch the trend, not the
absolute number.

## crap

**What.** CRAP = `cc + cc² × (1 − coverage)³` for every function, hard threshold **15**,
zero functions allowed over.

**Why.** Coverage alone rewards testing the easy code; complexity alone punishes code that
is complex *and* well tested. CRAP is the product: a threshold of 15 means no function
may exceed cyclomatic complexity 15 even when perfectly covered, and an untested function
may not exceed about three branches. That is exactly the combination agent-written code
drifts toward — a long function with a plausible happy-path test.

**The coverage dependency is load-bearing, not an optimisation.** cargo-crap without
`--lcov` scores every function as 0% covered — its own help calls that "not a real CRAP
run" — and the output still looks like a report. Worse, an LCOV that exists but records
nothing makes cargo-crap exit *zero* while producing exactly the same lie, so nothing
downstream notices. The gate therefore reads the LCOV itself before scoring and reports
`error` if it is missing or records no instrumented lines, rather than passing on a
number that is complexity wearing CRAP's name.

**Narrowing is by directory.** cargo-crap has no `--package`; it analyses `--path`,
which defaults to its working directory. `gauntlet run crap -p <crate>` therefore runs
cargo-crap *in* that crate's directory, resolved through `cargo metadata`. Naming two
packages is refused rather than quietly widened back to the whole workspace — measuring
more than was asked for is how a narrowed run comes to report other people's findings.

**Local.** `gauntlet run crap` (it runs `coverage` first, because scoring against a
missing or stale LCOV is not a measurement). Directly:
`cargo crap --workspace --lcov target/quality/lcov.info --threshold 15`.

**The one exclusion.** `build.rs` is excluded, because a build script runs at compile
time rather than under the test harness, so llvm-cov never emits coverage data for it
and cargo-crap scores it as if it were 0% covered. That is the instrument measuring its
own blind spot: no amount of testing can move a build script's score. "The instrument
cannot see it" is the only argument that earns an exclusion here — "nobody has tested
it yet" is precisely what the gate exists to report, and a test asserts that the
exclusion list stays at exactly one entry.

**How to read.** A `pass` reports the worst score and where it is, so the headroom is
visible before it is gone. A `fail` lists every function over the line with its score,
cyclomatic complexity and coverage — which tells you whether the fix is more tests or a
smaller function. cargo-crap has no package filter, so this gate always scores the whole
workspace and says so; `-p` does not narrow it.

## duplication

**What.** jscpd over the workspace's Rust sources, at its **shipped default detection
size** (50 tokens / 5 lines). Threshold: zero duplicated blocks.

**Why.** Agents duplicate far more readily than they abstract. Asked to add a variant, the
cheapest correct answer is to copy the neighbouring function and change three tokens —
which passes review, passes tests, and doubles the number of places the next bug fix has
to land.

**Not gamed.** The detection window in `.jscpd.json` is jscpd's default, unchanged.
Narrowing it, or adding an `ignore` entry, to make an existing duplicate disappear is
gaming the gate; the fix is to factor the shape the duplicate is pointing at. The only
`ignore` entries are `target/` and `node_modules/`, neither of which contains source.

**Excluded from scanning.** `spikes/**` and `docs/spikes/**`. Spike code is
throwaway-tier by design — it exists to answer one question and then stop — and holding
it to workspace standards would either corrupt the spike or corrupt the standard. The
workspace-scoped gates (clippy, test, machete, module-structure, mutants) skip spikes
automatically because the spike crates carry their own `[workspace]` table and are not
members; only the gates that walk the filesystem need to be told. `crap` needs no
exclusion for the same structural reason: it enumerates workspace members.

**Local.** `gauntlet run duplication`.

**How to read.** A `fail` names both locations of every block, with its length. A `pass`
states how many files and lines were scanned — and scanning *zero* files is reported as
`skipped`, not `pass`, because a misconfigured glob that silently matches nothing is
exactly the failure that distinction exists to catch.

## machete

**What.** `cargo machete`, with dev-dependencies included. Zero unused dependencies.

**Why.** A dependency nobody uses is a supply-chain surface and a compile-time cost for
nothing. Dev-dependencies matter as much as normal ones and for a different reason: a
`proptest` left in `[dev-dependencies]` after the tests that used it were deleted reads,
to anyone auditing the manifest, as a property-testing suite that no longer exists.
cargo-machete checks both tables by default, and a test in the gate asserts that it
still does.

**Invoked as `cargo-machete`, not `cargo machete`.** cargo-machete decides whether its
first argument is a subcommand name or a directory to scan by looking at the `CARGO`
environment variable, which cargo sets when it dispatches a subcommand. Reached any other
way, `cargo machete --version` means "scan the directories `machete` and `--version`",
exits 2, and prints no version at all. That happened on `main`: the gate correctly
reported `error` rather than a finding, and the fix was to call the binary directly. A
version probe should not depend on an environment variable set three processes away
deciding how its own arguments are parsed.

**Local.** `gauntlet run machete`, or `cargo-machete --with-metadata`.

**Build scripts are a blind spot.** cargo-machete scans `src/` and does not read
`build.rs`, so a `[build-dependencies]` entry used only by the build script reads as
unused. This is the same shape as the `crap` gate's `build.rs` exclusion — the
instrument cannot see the file — but the escape hatch lives in the crate's own manifest
rather than in the gate, which is better: the annotation sits next to the dependency it
excuses and is reviewed with it.

```toml
[package.metadata.cargo-machete]
# Used by build.rs, which machete does not scan.
ignored = ["prost-build"]
```

**How to read.** A `fail` names each package and dependency. Genuine false positives (a
crate used only through a macro, say) go in `[package.metadata.cargo-machete] ignored`
in the crate's own manifest, where the exemption is visible in review — not in the gate.
Exit code 2 is cargo-machete failing, and is reported as `error`.

**This gate is stricter than a casual local check.** Running `cargo machete` by hand in a
crate directory, or at the root without `--with-metadata`, answers a narrower question
than the gate does — the gate walks the whole workspace and counts dev-dependencies. A
"it's clean on my machine" that disagrees with the gate is usually the shallow check, not
a gate bug: reproduce with the exact command in the gate's log before concluding
otherwise. That log is the point of `target/quality/machete.log`.

## module-structure

**What.** Two structural checks, one gate:

1. **Orphans** — no `.rs` file under a crate's source tree is unreachable from its crate
   root. Run with `--cfg-test` so `#[cfg(test)] mod tests` is not mistaken for an orphan.
2. **Cycles** — module-level dependency cycles are ratcheted against the committed
   `baselines/module-cycles.txt`.

**Why.** An agent asked to "move that helper out" creates `src/helper.rs` and forgets
`mod helper;`. The file compiles nothing, is never run, and reviews exactly like live
code. Separately, an agent resolving a borrow error by reaching back into the calling
module turns a layered crate into a knot — one edge at a time, each edge locally
reasonable. Neither is a measurement needing calibration, so both are hard from day one.

**The ratchet runs both ways.** A new cycle fails. A cycle listed in the baseline that no
longer exists *also* fails, so the file cannot quietly overstate the debt it licenses and
keep licensing it. There is no `--update` flag: the baseline is updated by hand-copying
the gate's output into it, which is a reviewable diff.

**Local.** `gauntlet run module-structure`. Directly:
`cargo modules orphans --deny --cfg-test -p <pkg> --lib` and
`cargo modules dependencies --lib --cfg-test -p <pkg>`.

**How to read.** A `fail` lists orphaned files by path and module name, new cycles as
`a <-> b`, and disappeared cycles with the line to delete from the baseline.

*Known limitation, stated rather than hidden:* cargo-modules scans a crate's whole source
directory for whichever target it is pointed at, so asking about a binary in a package
that also has a library reports every library module as an orphan. Packages with a
library are therefore checked through the library target alone (which covers everything
under `src/`), and binaries are checked directly only in packages that have no library.

## proto-contract

**What.** `buf lint` over the schema, and `buf breaking` against the merge-base with
`main`. Zero violations of either.

**Why.** The `.proto` files are the one artefact the server, workers and clients all
agree on, and they upgrade independently — so this gate *is* the version-skew guarantee
from DESIGN §2. A renamed field is not a compile error anywhere; it is a runtime
mismatch between two binaries built a week apart.

**The baseline needs no special case.** It is always the merge-base with `main`. On a
branch that is the branch point, so the gate asks "does my work break what I branched
from". On `main` itself the merge-base *is* `main`'s tip, so the same rule asks "does my
working tree break what is committed" — the no-uncommitted-breakage check. `HEAD~` would
be wrong on both. When there is no `main` at all, lint still runs and the row says
compatibility was **unchecked**, so a green row can never be misread as "compatible"
when it only means "not compared".

**Local.** `gauntlet run proto-contract`, or `buf lint` and
`buf breaking --against ".git#ref=$(git merge-base HEAD main)"`.

**How to read.** A `fail` counts lint violations and breaking changes separately and
lists each with its file, line and column. A workspace with no `proto/` directory is
`skipped` — and it skips *before* the version probe, so an absent buf on a machine with
no schema is not reported as a broken instrument.

## mutants

**What.** `cargo mutants`, **zero missed mutants**. Scoped to the diff against the
merge-base with `main` when there is one, and to the whole tree when there is not.

**Why.** Coverage says a line ran. A mutant says the line's behaviour was *checked*.
Agent-written tests are unusually good at the first and unusually bad at the second:
asserting a call returned `Ok`, that a vector is non-empty, that nothing panicked. Every
one of those stays green when the code under it is quietly wrong, and every one of them
is exactly what a surviving mutant points at. A mutation that stays green is a finding,
not a relief.

**Scope: the change under test.** This is the only gate that does not measure
everything, and what it measures is always "what did this change introduce":

| where | baseline | scope |
|---|---|---|
| on a branch | merge-base with `main` | the branch's own work |
| on `main` | `HEAD`'s first parent | what the tip commit (usually a merge) brought in |
| no `main`, or a root commit | — | the whole tree |
| `gauntlet run --full` | — | the whole tree |

On a branch, merging `main` in *moves* the merge-base to `main`'s tip, so the diff stays
the branch's own work and the mutant count tracks what the branch changed rather than how
large the workspace has become. On `main` the merge-base is `HEAD` itself, which would
leave nothing to compare against — so the baseline is the first parent, which for a merge
commit is precisely the trunk it landed on. Same question, asked of the merge.

**What this scope cannot see.** A gate that only ever looks at diffs never re-examines
code no recent diff has touched. Not hypothetical: the first whole-tree run on this
workspace found **92 surviving mutants** in crates that had only ever been measured
through somebody's diff. Treat a green diff-scoped row as "this change is covered", never
as "the workspace is covered" — the row says which it is, in words, for that reason.

**When the whole-tree sweep runs: at every wave boundary.** `--full` is the answer to the
other question, and the thing that failed before was not that the sweep was skipped but
that nobody had ever run it at all. A calendar reminder would be the same promise in a
new costume, so the sweep is pinned to an event this project already has:
IMPLEMENTATION.md's waves. Before a wave is declared finished, whoever is running it does:

```
gauntlet run --full        # ~24 minutes at 1100 mutants; expect to schedule around it
```

and the next wave does not start on a red one. Wave boundaries are the right moment
because that is exactly when the next tranche of work is about to be built on whatever
the last one left behind, and they arrive often enough that a sweep is never archaeology.
Once there is a release process, tagging joins the list.

This is deliberately *not* enforced by a timer inside the gate. A gate that reds because
of the calendar rather than the code teaches people to ignore it, and the gate has no
honest way to know when the last sweep happened — nothing in git records it. Making the
sweep an owned step in an existing ritual is weaker than a machine check and stronger
than a good intention, and it is the strongest thing available here.

Falling back to the whole tree when there is no baseline is deliberate: an absent branch
or a root commit must never quietly shrink the gate to nothing. Timeouts live in
`.cargo/mutants.toml`; `--jobs` and `--full` live in the gate.

**Two kinds of exclusion, and nothing else.** Skipping a function because its mutants
survive is how a mutation gate becomes decorative. If a function cannot be
mutation-tested, the fix is normally to move its logic somewhere that can be — which is
why the gauntlet's own I/O is behind the `CommandRunner` seam and its parsing is in pure
functions. `.cargo/mutants.toml` admits exactly two shapes of `exclude_re` entry, and the
gate refuses anything else outright rather than accepting it and leaving it unverified.

*A file, when the file is test fixture.* A binary that exists **to be spawned by the test
suite** — a stub harness a process-supervision test launches — is fixture wearing a
`[[bin]]`'s clothes, and mutating it measures the fixture rather than the product. The
criterion is *who exercises whom*: a binary the suite runs is excluded; a binary the suite
tests is not. `horde-cli`'s `bin/horde.rs` is a product the suite tests, so it stays in
scope — which is why the entry names one path, with `\.` for a dot, rather than
`src/bin/**`, a glob that would take the product out with the fixture:

```toml
exclude_re = ['crates/horde-worker/src/bin/stub_harness\.rs']
```

The path is written **from the workspace root**, `crates/` and all. Earlier revisions of
this section omitted the `crates/` prefix, and when the exclusion first landed for real the
self-check refused it — *"excludes horde-worker/src/bin/stub_harness.rs, which is not in
the tree"*. The regex would in fact have matched, because `exclude_re` is tested against a
mutant description that begins with the full path; it is the gate's *existence* check that
insists on a path a reader can go and look at. That is the check earning its keep: a
prefix-less path is one directory rename away from silently excluding nothing at all.

*A function, when its mutant is provably equivalent.* Sometimes cargo-mutants' rewrite
produces the expression that was already there. `Capabilities::none()` is written
`Self::default()` over a type deriving `Default`, and the mutation is
`-> Self with Default::default()`: the same expression, character for character. No test
can distinguish them, and a test written to look as if it had would be asserting on
nothing — a hollow test is worse than the survivor it hides, because it also lies about
coverage. Such an entry names the function as a Rust path and **must** carry a comment
arguing the equivalence:

```toml
exclude_re = [
    # `Capabilities::none()` is written `Self::default()`, and `Capabilities`
    # derives `Default`, so the mutated expression is the original.
    'Capabilities::none',
]
```

**The criterion is identity, not difficulty.** An equivalence entry requires the mutated
expression to be *provably identical* to the original. "No test kills it", "it would need
an awkward harness", "the mutant is obviously harmless" are all findings, not
equivalences, and belong in the test suite. The comment is mandatory because equivalence
is an argument rather than an observation: nothing in the tree records it, so the next
reader either finds the reasoning written down or has to re-derive it — and re-deriving it
is exactly the work people skip.

**Every exclusion proves itself, four ways.** Before a mutation run, the gate lists the
mutants twice — `cargo mutants --list` and `cargo mutants --list --no-config`. The
difference between the two listings *is* what the configuration removed, measured rather
than inferred from reading the patterns. Four lies are refused:

* *It names nothing.* An exclusion for a path no longer in the tree is indistinguishable
  from one still doing its job, so it silently misstates what is being measured — the same
  reason `baselines/module-cycles.txt` ratchets in both directions.
* *It was configured but did not apply.* `exclude_re` is a regex over each mutant's
  description, not a path glob, so an anchoring mistake makes an exclusion quietly do
  nothing and the mutants it named get tested anyway. This is not hypothetical: a
  `--exclude 'src/bin/**'` was accepted without complaint and the stub binary's mutants
  were tested regardless. Configured is not applied.
* *It removed nothing.* An entry whose two listings agree is inert, and an inert entry
  reads exactly like one holding something back. Nothing inside the excluded listing can
  tell those apart; only the comparison can.
* *It removed something it does not name.* Anything missing from the excluded listing that
  no entry claims means the configuration is wider than it reads — an over-broad pattern,
  or a key this gate does not parse at all. A bare `none` would match `announced.is_none()`
  in cargo-mutants' regex but not in the gate's idea of "names this function", so its
  collateral surfaces here instead of passing as intended.

An equivalence entry has one extra obligation: it must remove **exactly one** mutant.
Equivalence is a claim about a single mutated expression, so a function that has grown a
second one has outgrown the argument made for it. Widening the entry to cover the new
mutant would silently exclude untested behaviour; the entry reds instead, and the argument
has to be made again for what grew.

Both listings cost nothing when there are no exclusions, which is the usual case: they run
only once something is excluded.

**Local.** `gauntlet run mutants`, or `cargo mutants` directly. Survivors are listed in
`target/quality/mutants.out/missed.txt`.

**How to read.** A `fail` gives the number of survivors and points at `missed.txt`; each
line there is a specific edit that no test noticed. A `pass` reports how many mutants were
caught, how many were unviable (did not compile — legitimately not a finding), and how
many timed out. Timeouts count as caught in the tally — a mutant that hangs the suite did
change behaviour — and the count is surfaced separately so a creeping number cannot hide
inside a green row. Read that number every time, and read it as *unevaluated* rather than
as killed: "A timeout is not a kill" below says why the classification is not stable. A
run that produced no tally at all is an `error`, not a clean sheet — *unless* it said why
it had nothing to do, in which case it is `skipped`.

**A change with nothing mutable in it is `skipped`, and never `pass`.** cargo-mutants
mutates product code, not tests: `#[cfg(test)]` modules and `tests/` are not mutated at
all. So a diff can be full of Rust and still generate zero mutants — which is exactly what
a test-only refactor is — and cargo-mutants then exits 0 having printed no tally. There
are four wordings for it, depending on which stage the work ran out at (`Diff changes no
Rust source files`, `No mutants to filter`, `diff matched no mutants`, `No mutants found
under the active filters` / `No mutants were generated`), and the gate reads all of them.
The row says `skipped` with the reason, because "no mutant survived" and "no mutant was
ever built" are different facts and only one of them is evidence about the change.

One message in that family is **not** in it: `No mutants were viable`, whose own text
continues "perhaps there is a problem with building in a scratch directory". That is the
instrument failing, so it stays an `error`. It is one word away from `No mutants were
generated`, which is precisely how a broken build would come to be reported as a clean
sheet; a test pins the distinction.

---

## Lessons the gates have already taught

Each of these cost someone real time. They are here so it is only once.

**Run the gates *before* `git commit`, not in the same command chain.** A
`cargo test && git commit` that you interrupt, or whose test step you misread,
commits red. Worse, a chain that commits first and measures second leaves the
repository in a state the gate has never approved. Measure, read the result,
then commit.

**Tool debris is part of a tool's output.** cargo-mutants rotates its output
directory to `mutants.out.old`; jscpd writes a report tree; llvm-cov leaves
profraw files. Anything a gate's tool creates belongs under `target/` (already
ignored) or in `.gitignore` — check what a new tool leaves behind the first time
you run it, not the first time it shows up in `git status`.

**A gate that only ever passes has not been shown to work.** Before trusting a
new gate, break something on purpose and watch it go red — an unformatted file,
a duplicated block, a deleted assertion. Every gate here was seen red before it
was seen green, which is why the fixtures in `crates/horde-gauntlet/fixtures/`
include dirty output as well as clean.

**`mod.rs` declares and re-exports; it does not define.** A module that both
re-exports a child and defines what that child imports is a cycle, and
`module-structure` will say so. This has come up twice — in `horde-server`'s
service spines and in `horde-adapter`'s crate root — and the fix is the same
both times: move the shared definition into a sibling module.

**"It did not finish" is a weak assertion, and mutants know it.** A test whose
only claim is that an operation blocked or timed out passes for *any* reason the
operation blocked — including the mutant's. `FakeSession::advance` cost three
attempts to this: the test scripted a `Say` before the `Stall`, so the mutant
that plays straight through a stall still stopped one step later, still held the
caller, and still passed. The killing version asks what the caller was *handed*
before it blocked, not whether it blocked. When a test's assertion is the absence
of something, check that the mutant would have produced something — otherwise
both versions are equally silent and the test is measuring the clock.

**Fixing one instance of a class leaves the class.** The mutants gate learned to treat
a docs-only diff as `skipped` rather than a broken instrument. One merge later it went
red on `main` again, for a tests-only diff — the same bug, one case over, because the fix
had matched a single message string rather than asking what the tool says when it has
nothing to do. The repair was to enumerate the family from `cargo-mutants --help` and the
binary's own strings, and to write the test for each. When a gate misreads a tool, spend
the extra ten minutes finding out how many ways that tool can say the same thing; the
second instance always arrives, and it arrives on `main`.

**A timeout is not a kill; it is a mutant nobody evaluated.** The tally counts a
timed-out mutant as caught, which is the most defensible thing one number can
say — but the classification is not stable, and the same mutant on the same tree
comes back `MISSED` on an idle machine and `TIMEOUT` on a loaded one. The reason
is visible in cargo-mutants' own output: it measures *one* unmutated baseline and
derives the whole run's budget from it (`Auto-set build timeout to 23s`,
`Auto-set test timeout to 60s`), so every mutant is judged against a clock that
was started once, under whatever load the machine happened to be under then.
Nothing about the mutant enters that number. So until cargo-mutants derives a
per-mutant timeout from that mutant's own baseline, read a timeout as
**unevaluated**: it is not evidence that a test noticed the change, and a green
row containing timeouts has not been shown to be green. Re-run them on a quiet
machine before believing the row — and never close out a survivor by observing
that it timed out on the next attempt.

**Bound every wait — but bound *progress* by count or structure, never by clock.**
The way to turn a timeout into a kill is to give each wait a ceiling and assert on
what arrived, so a mutant that stops the thing arriving fails in seconds instead of
stalling the suite. `next_batch_within` in `horde-server`'s log tests and `within`
in `horde-worker`'s supervision tests are the same helper, and every wait against
a child process or a tail goes through one of them.

A timer is not always enough, and the exception is worth knowing before it costs
an afternoon. Two mechanisms have now been measured here, opposite in behaviour and
identical in effect — in both, the deadline reads as if it exists and does not:

*The loop never yields.* `#[tokio::test]` runs on the current-thread runtime, so a
loop whose body never actually awaits — a `read_line` mutated to return
`Ok(Some(""))` instantly, forever — never yields to the scheduler, the timer is
never polled, and a `tokio::time::timeout` wrapped around that loop silently does
not fire. That mutant spun for a full minute while every other test in the file had
already failed, hanging the whole binary on one starved timer.

*The loop yields on every iteration.* The inverse defeats the same clock. A loop
that genuinely awaits each time round — `spawn_blocking` into SQLite — starves the
runtime and the test's own budget with real work rather than with a spin, and
per-test clocks did not bound it either. Nothing is stalled, so nothing looks stuck;
the deadline simply never comes out ahead of the loop. Yielding is not what makes a
timer work, which is why "it awaits, so the timeout will fire" is not an argument.

So the rule is the one both mechanisms point at: **a clock cannot bound a loop that
fails to make progress, whether because it never yields or because it yields so hard
it starves the runtime; bound progress by count or structure, never by time.**

**The fix hierarchy, strongest first.**

1. **Structural** — remove the loop's ability to not progress at all, so there is no
   non-terminating case left to bound. A loop that consumes a finite input, or that
   cannot repeat a state, needs no ceiling and no clock. This is the only fix that
   makes the failure impossible rather than survivable, and it is the one to reach
   for first.
2. **A counted ceiling** — needs no scheduler co-operation, so it holds exactly where
   a clock cannot:

   ```rust
   let mut lines = 0_u32;
   while let Ok(Some(_)) = within("a line arrives", harness.read_line()).await {
       lines += 1;
       assert!(lines < 1024, "the stub's stdout must reach EOF");
   }
   ```

3. **A clock** — legitimate only for a **positive** wait: something must arrive, and
   the assertion is on what arrived. A deadline is evidence about an arrival, never
   about a loop finishing; asking one to establish "this terminates" is asking the
   instrument the one question it cannot answer.

The same trap sits under any "wait for a notification" loop that can be woken
spuriously — `tokio::time::pause` or an observable park signal is the equivalent
fix where the spin is in production code rather than a test.

**A compound condition inside a loop or a stream handler is untestable in place;
extract it as a named predicate.** `while a && b` in a stream handler cannot have
its truth table exercised, because the states that separate `&&` from `||` are
the ones the surrounding machinery makes unreachable: a failed reply is usually
followed by a hang-up, a non-following client also drops the stream, a tail only
ever sees notifications it already delivered. The wrong operator therefore
behaves correctly in every run anyone can stage, and the mutant survives an
end-to-end suite that is genuinely exercising the feature. Lifted out as a free
function the same expression is an ordinary truth table — four cases, all
assertable, no runtime to arrange. Five of these have now been extracted in
`horde-server` for exactly this reason: `needs_sweep`, `sweep_from` and
`is_stale` in the log module, `stop_after_caught_up` in `service/control_plane.rs`
and `should_stop` in `service/worker.rs`. Reaching for a harness clever enough to
force the unreachable state is the wrong repair; the condition being hard to
reach *is* the design feedback.

**A write that is acked but discarded is the worst failure shape there is —
spend an error on it.** The permission and result verbs both key their events
with an idempotency key, and the log deduplicates on `(session_id,
idempotency_key)`. That check originally ignored the event *kind*, so a key
already used by any other event in the session made the new event a duplicate:
the write was acked, the call returned `Ok`, and nothing reached the log. The
symptom then appeared in a completely different component — `wait_for_agent`
blocking forever on a result that had been silently swallowed — with nothing
connecting the hang to the write that caused it. Diagnosing it once meant
dumping every event in the session and noticing an `agent_spawned` sitting where
a `result_reported` should have been.

The repair is `service::shared::refuse_kind_collision`: an exact-kind hit still
deduplicates silently, because at-least-once delivery depends on that, but a hit
whose kind differs is refused with both kinds and the key in the message. Every
legitimate retry replays the same kind, so a kind mismatch is *always* a
collision and never a retry.

The general rule is worth more than the specific fix. When a write path can
discard its input and still answer success, the resulting bug has no local
symptom, cannot be found from the failing component, and survives every test
that does not happen to read back what it wrote. Prefer an error at the write —
even a noisy one, even one that costs a caller a rename — over a silence that
someone else will pay for later, somewhere else, with no thread back to here.

**A faster test tool is a measurement, not an assumption.** Switching cargo-mutants
to `--test-tool=nextest` (stop at the first failing test) read like a free 2× —
most mutants are caught, so most suites should end early. Measured on the identical
2,439-mutant tree, it was **1.8× slower**: 17,681s against 9,680s. nextest pays a
fixed per-mutant cost (about 3s here — its own build/list/run cycle in every
scratch tree), and fail-fast can only refund *test* time, which these suites
barely spend; the per-mutant floor is the incremental build, and no test tool
touches that. The experiment still bought one fact worth keeping: every one of
the 2,438 evaluated mutants dies without doctests, so nextest's inability to run
them would have cost this tree nothing. If the sweep needs to be faster, the
levers that match the physics are a shared compiler cache for the build floor
and `--shard` across machines for the wave boundary — not a different test tool.

## Adding a gate

1. **Write the gate.** One module under `crates/horde-gauntlet/src/gates/`. Split it into
   a function that *builds* the command, a pure function that *reads* the output, and
   nothing else — the running is done by `CommandGate` (one command) or `PinnedGate`
   (several). If neither shape fits, implement `Gate` directly and say why.
2. **Pin the tool.** A `ToolPin` const with the exact version and its install command.
   A mismatch must be an `error`, never a `fail`.
3. **Capture fixtures.** Run the real tool, in both its clean and its dirty state, and
   commit the output under `crates/horde-gauntlet/fixtures/`. Tests read the fixture and
   call the real reader; no test-side reimplementation of the parsing.
4. **Decide the four outcomes explicitly.** In particular: what does this gate measure
   such that there is *nothing* to measure (`skipped`), and what can go wrong with the
   tool such that the measurement did not happen (`error`)?
5. **Register it.** Add a `RegisteredGate` literal to `registry::registry()` with its
   name, enforcement level, dependencies and one-line summary — and update the
   `EXPECTED` list in that module's self-test. That failing test is the point: adding,
   removing or demoting a gate has to be deliberate.
6. **Document it here,** in the same commit, in the same shape: what / why / local
   command / how to read. Add the tool to the pinned-versions table. A gate whose
   documentation lands later is a gate nobody else can act on.
7. **Run the gauntlet on itself.** `gauntlet run` must be green — including the new
   gate's own code passing `crap`, `duplication` and `mutants`.
