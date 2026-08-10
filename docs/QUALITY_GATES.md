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

**Local.** `gauntlet run machete`, or `cargo machete --with-metadata`.

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

**Scope, and why it does not grow with the workspace.** The diff is taken against the
merge-base with `main`, and merging `main` into a branch *moves* that merge-base to
`main`'s tip. So the diff is always only the branch's own work, and the mutant count
tracks what this branch changed rather than how large the workspace has become: nine
crates and 567 tests still produce ~440 mutants of gauntlet code, because that is what
this branch touched. Main growing does not slow this gate down; it only costs more the
day someone edits the grown parts, which is exactly when it should.

The fallback when there is no diff (or no `main`) is the *whole tree*, which is the
stricter choice, so a missing branch can never quietly shrink the gate to nothing.
Timeouts live in `.cargo/mutants.toml`; `--jobs` lives in the gate.

**No exclusions, today.** `.cargo/mutants.toml` has no `exclude_re`. Skipping a function
because its mutants survive is how a mutation gate becomes decorative. If a function
cannot be mutation-tested, the fix is to move its logic somewhere that can be — which is
why the gauntlet's own I/O is behind the `CommandRunner` seam and its parsing is in pure
functions.

**The one exclusion that will be legitimate, and its criterion.** A binary that exists
*to be spawned by the test suite* — a stub harness a process-supervision test launches —
is test fixture wearing a `[[bin]]`'s clothes. Mutating it measures the fixture, not the
product. When such a binary lands, exclude it by its own path and say so:

```toml
# Test fixture: the process suite spawns this binary, so mutating it measures
# the fixture rather than the code under test.
exclude_re = ["horde-worker/src/bin/stub_harness\\.rs"]
```

The criterion is *who exercises whom*: a binary the suite runs is excluded; a binary the
suite tests is not. `horde-cli`'s `bin/horde.rs` is a product the suite tests, so it stays
in scope — which is why the exclusion names one path rather than `src/bin/**`, a glob that
would take the product out with the fixture.

**Exclusions rot, and nothing here catches that yet.** An `exclude_re` that no longer
matches anything is indistinguishable from one still doing its job, exactly like a
`baselines/module-cycles.txt` entry for a cycle that has been fixed — and that file is
ratcheted in both directions for precisely this reason. The mutation gate has no
equivalent check. Doing it properly means asking cargo-mutants what its exclusions
actually removed, because `exclude_re` is a regex over mutant descriptions rather than a
path glob, and a cheaper approximation would be a check that lies. Until that exists, an
exclusion added here is a promise someone has to keep by hand.

**Local.** `gauntlet run mutants`, or `cargo mutants` directly. Survivors are listed in
`target/quality/mutants.out/missed.txt`.

**How to read.** A `fail` gives the number of survivors and points at `missed.txt`; each
line there is a specific edit that no test noticed. A `pass` reports how many mutants were
caught, how many were unviable (did not compile — legitimately not a finding), and how
many timed out. Timeouts count as caught, since a mutant that hangs the suite did change
behaviour, but the number is surfaced so a creeping timeout count does not hide inside a
green row. A run that produced no tally at all is an `error`, not a clean sheet.

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
