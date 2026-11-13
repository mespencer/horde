# Wave 12 — decisions and scope

Agreed with the operator on 2026-11-12/13. This file is the brief every wave-12
teammate reads first. It records *decisions*, not designs; each item's ADR or
docs carry the design.

## Environment notes

- Development machine: Linux x86_64, 32 cores, 94 GB RAM (`aurum-dev-gpu-1`,
  Tailscale `100.105.35.39`). Earlier waves ran on a macOS machine (Tyrion), so
  the gauntlet's pinned tools are installed here under `~/.horde-tools/bin`
  (`PATH="$HOME/.horde-tools/bin:$PATH"`), kept apart from `~/.cargo/bin`, which
  other repos pin differently. `cargo-gamma` 0.2.1 with atlas's
  census-kill-final patch is already in `~/.cargo/bin`.
- Installed harnesses: `claude` 2.1.322, `codex` 0.167.0 (Tyrion has the same).
- Network: workers and clients dial the server outbound, so the server for real
  multi-machine runs lives on this Linux box (Tyrion → here works; here → Tyrion
  TCP is blocked by Tailscale ACLs).
- Branch for the wave: `mespencer/wave-12`. Teammates work in their own
  worktrees and branches; the orchestrator merges into `mespencer/wave-12`
  with real merges and re-runs the fast gauntlet there.

## Ground rules (unchanged from IMPLEMENTATION.md, plus)

- `unwrap()` never; `expect()` only in tests/benchmarks; tests exercise
  production code, no reimplementation of production logic in tests.
- **Mutation testing is blocking but run rarely**: at the end of a big change,
  not per step. Day-to-day handoffs run every gauntlet gate *except* mutants
  (`gauntlet run` with mutants skipped as the gate supports), plus the relevant
  suites. Each item's final handoff runs mutants scoped to its diff.
- Report evidence (gauntlet table, test counts), never claims.

## Items, in order

1. **W12a — Mutation testing on cargo-gamma.** Replace cargo-mutants with
   cargo-gamma 0.2.1 (atlas's patched build; copy atlas's
   `.github/workflow-resources/cargo-gamma/{install.sh,*.patch}` into the repo).
   Keep the gate blocking at zero missed (timeouts and OOM count as missed).
   `gamma.toml` with `exclude-files` replaces `.cargo/mutants.toml`;
   `crates/horde-server-testkit/src/reap.rs` must be excluded from discovery
   entirely (gamma compiles schemata into the test binary) and the gate must
   prove it. Equivalent mutants use `#[gamma::skip(..., reason)]`. Rebuild the
   exclusion audit in gamma terms. Speed: persistent `--cache-dir`, jobs ~16–24
   with a memory cap, `--cap-lints=allow`, and an incremental mode that diffs
   against the last green full sweep. Pilot a full run; record timing vs the old
   2–10 h. Also split `run_workspace` (`crates/horde-cli/src/cli.rs`, CRAP 15.0).
2. **W12b — Docker multi-node test harness.** Compose topology: one server,
   2–3 workers, a client, each container its own network identity; real join
   tokens and mTLS; fake adapter by default, opt-in real `claude`/`codex`.
   Scenarios: placement, git relay, cross-worker share, worker kill/restart,
   network partition. Then a real-machine smoke (server here, worker on Tyrion).
3. **W12c — Harness modernization.** Keep the transports (claude stream-json,
   `codex app-server`; T3 Code uses the same Codex transport and its Claude SDK
   spawns the same CLI). Re-probe claude 2.1.322 and codex 0.167; version
   detection with supported/untested/refused ranges for both; move Claude
   permissions to `--permission-prompt-tool stdio` + `can_use_tool` control
   requests (MCP route as fallback only); surface every prompt type to clients
   (Claude `AskUserQuestion`, codex `item/fileChange/requestApproval`,
   `item/permissions/requestApproval`, `item/tool/requestUserInput`) and make
   them answerable from CLI and TUI; drift tests (codex JSON schema diff vs what
   `wire.rs` uses; claude live smoke against latest CLI); fix stale
   `docs/ADAPTERS.md` "Not here yet".
4. **W12d — Workspaces.**
   - Freshness: at spawn, fetch (throttled ~60 s) and base each agent worktree
     on the latest `origin/<default_branch>`; never mutate the instance working
     tree; on fetch failure fall back to the cached copy with a warning event.
     Children still start from the parent's commit. Spawn / MCP may name a base
     per repo.
   - Lazy checkout: workspace checkout mode `all` | `lazy` (per workspace,
     overridable per spawn). Lazy agents start with named repos (or none) plus a
     manifest; an MCP tool `workspace_checkout(repo)` adds a worktree on demand.
     Only checked-out repos are stamped and dirty-checked in results.
   - Optional `--branch <name>` applied across repos; default `horde/<agent-id>`.
   - Cleanup, two tiers. *Soft* (reclaim): per-repo configurable command
     (Rust repos default `cargo clean`; others none); worktree, branch and
     commits stay. Automatic on archive, after a configurable idle time, and
     oldest-first under disk pressure. *Hard* (remove worktree, branch, optional
     pin): automatic only when provably lossless (clean tree, every commit
     reachable from a pushed/merged branch or a pin); otherwise explicit
     `horde workspace gc --hard --force`. Orphans after worker reset are reported
     and soft-reclaimed at most. Instances removed only explicitly or with
     definition delete `--purge`. Every cleanup has `--dry-run`.
   - Workspaces are deliberate: no auto-registration; at most `horde` offers to
     create one when started outside any workspace.
   - Finish or remove `unmanaged_paths`, `MaterializationProgress`,
     `ReportWorkspaceInstances`.
   - ADR first (ADR-0014).
5. **W12e — Config layering and permissions.** Config file layering per DESIGN
   §13 (defaults → file → workspace → session → flags). Permissions default to
   bypass and are configurable; the existing rule chain stays as an option; no
   judge. Harness permission prompts always reach clients.
6. **W12f — TUI rebuild.** Main interactive view in the style of Claude Code
   (streaming output, compose box, inline approvals/questions, interrupt, slash
   commands) plus session/agent tree, inbox, workspaces and a usage view (display
   only; no budgets).
7. **W12g — Windows support.** Job objects instead of process groups/signals,
   path and shell handling, Windows build/test in the gauntlet; tested with a
   worker on `aurum-dev-gpu-2`.
8. **W12h — Logs and diagnostics.** No OTLP stack. Bounded rotating local logs
   per binary; `horde logs <worker|server> [--follow] [--since]` over the
   existing mTLS streams; an MCP tool so agents can read worker/server logs.
   Logs never enter the session event log. ADR first.
9. **W12i — ssh bootstrap.** `horde worker add user@host`: install binaries,
   join, verify harnesses.
10. **W12j — Small cleanup**, folded into nearby items: wire or delete
    `RevivalIdentity::is_resumable_under`; distinct types for journal vs session
    sequences; actor attribution for archive and workspace/MCP edits; blob GC;
    CONFORMANCE line citation fix.

Deferred: multi-user servers. Dropped: policy judge, budgets/alerts.
