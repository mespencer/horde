# Configuration: one strict TOML file per role, layered under workspace, session and flags, and permissions that default to bypass

No configuration file is read anywhere. Every knob is a flag, an environment
variable, or a constant. Tiers are `TierCatalog::shipped_defaults()`, read
in three places (`validate_model_selection`, `binding_for`,
`requested_provider`). Permission modes are `PermissionModes::shipped()`,
built in `startup.rs`, so nobody can author one. ADR-0014 added fifteen
`workspace.*` settings and ADR-0015 added twelve `logs.*` settings, and each
says "until W12e's config file". DESIGN §13 describes a layering that no
code implements (CONFORMANCE: ABSENT).

Day to day, the permission chain is also too strict. Its shipped default
escalates everything except an agent's own orchestrator verbs, so every
`Read` and every `Bash` waits for a human. The operator decided on
2026-11-13 (docs/WAVE12.md, W12e): permissions default to bypass, the rule
chain stays available as a mode, there is no judge, and harness prompts and
questions still reach clients.

This ADR records how those decisions are built.

## Decision

### Layers

From least to most specific, a later layer wins:

1. **Built-in defaults.** The constants that exist today. They are named in
   one place per role, and `config show` prints them.
2. **The role's config file.** TOML, under `~/.horde`:

   | Role | File | Override |
   |---|---|---|
   | `horde-server` | `~/.horde/server/config.toml` (`<state-dir>/config.toml`) | `--config PATH` |
   | `horde-worker` | `~/.horde/worker/config.toml` (`<state-dir>/config.toml`) | `--config PATH` |
   | `horde` (client) | `~/.horde/client/config.toml` | `--config PATH` |

   A missing default file is no layer. A missing file named by `--config` is
   a refusal: the operator asked for a file that is not there.
3. **Workspace.** Settings on a Workspace Definition (server-side, authored
   with `horde workspace create|update`). They apply to agents in that
   workspace.
4. **Session.** Settings given at `CreateSession`
   (`horde new --permission-mode`, or the client file's
   `session.permission_mode`). They apply to every agent a client creates in
   that session.
5. **Flags.** For a process-wide key, the process's own flag
   (`horde-server --workspace-fetch-throttle 2m`) overrides its file. For a
   per-agent key, the spawn's own flag (`horde spawn --permission-mode ask`)
   overrides every layer above.

Not every key exists at every layer. A process-wide key, such as a scan
interval, has layers 1, 2 and 5. A per-agent key has whichever layers make
sense for it, and the key table below says which. The two kinds never
conflict: a process flag is part of that process's configuration, and the
workspace and session layers are more specific than any one process.

DESIGN §13's "repo" layer (`.horde/` in each repo) is not built. A checked-out
repo is content that agents write (ADR-0014 makes the same point about
reclaim commands), so nothing that grants authority may come from it. Repo
agent types and prompt fragments stay future work.

### Provenance

Every role computes its effective settings once, at startup, together with
one origin for each key: `default`, `file <path>`, `workspace <name>`,
`session <id>`, `flag --<name>`, or `env <VAR>`.

```
horde config show [--origin]                       # this client's settings
horde config show --server [--workspace W] [--session S] [--origin]
horde-server --show-config [--origin] [other flags] # what this command line would serve with
horde-worker --show-config [--origin] [other flags]
```

`--show-config` parses the flags and the file exactly as a start would, then
prints the result and exits. A config error therefore shows up here before
any daemon starts. `horde config show --server` asks the running server
(`ControlPlaneService/GetEffectiveConfig`, any client, ADR-0009 row). With
`--workspace` and `--session` it adds the per-agent keys for an agent a client
would create there, each with the layer that decided it. Output is one
`key = value` line per key, with `# origin` appended under `--origin`.
`--json` gives `{key, value, origin}` objects.

### Strict validation

The file is parsed into a TOML table and walked against the role's key
table. The walk never deserializes leniently:

- An unknown key is refused with its full path and the keys valid at that
  level: `server config /h/.horde/server/config.toml: unknown key
  workspace.fetch_throtle (known here: archived_grace, archived_pin_ttl,
  fetch_throttle, fetch_timeout, idle_reclaim_after, reclaim,
  reclaim_timeout)`.
- A type error names the key, what it expected, and what it found:
  `workspace.fetch_throttle: expected a duration such as "60s" or "4h",
  found integer 60`.
- A value error repeats the parser's reason: `placement.disk_pressure:
  "10%,x": not a size`.
- Cross-key rules are refusals too. The default mode must name a defined
  mode, the high watermark must exceed the low one, a tier needs at least
  one candidate, and `floor` is valid only with `strategy = "ordered"`.
- A key that belongs to another role is refused with that role's name. For
  example, `worker.workspace.scan_interval` in the server file gives
  "belongs in the worker's config file".

A refusal stops startup with exit code 1. Nothing falls back to a default,
because a typo that silently does nothing is the failure this rule exists to
prevent. This is the same rule ADR-0015 applies to `HORDE_LOG`.

The file is read once. Hot reload is out of scope, so a change takes effect
on restart. For server keys stamped into commands, ADR-0014's "next command
that carries policy" still holds after the restart.

### Keys by role

Durations are strings (`"60s"`, `"4h"`, `"0"` where zero means never). Sizes
are strings (`"20GiB"`). Thresholds are `"10%"`, `"20GiB"` or `"10%,20GiB"`.
All of them use `horde_proto::units`, the parser the flags already use.

**Server** (`~/.horde/server/config.toml`):

| Key | Default | Layers | Notes |
|---|---|---|---|
| `permissions.default_mode` | `"bypass"` | file, `--permission-mode`, workspace, session, spawn | The mode an agent runs under. Must name a mode. |
| `permissions.modes.<name>.bypass` | `false` | file | Allow every call the deny rules do not refuse. |
| `permissions.modes.<name>.own_orchestrator_verbs` | `false` | file | ADR-0009. |
| `permissions.modes.<name>.deny` | `[]` | file | `[{tool?, under?, where?, reason}]` |
| `permissions.modes.<name>.allow` | `[]` | file | `[{tool?, under?, where?}]` |
| `tiers.<name>.strategy` | `"headroom"` | file | `headroom`, `ordered` or `weighted` |
| `tiers.<name>.floor` | — | file | Valid only with `ordered`, from 0.0 to 1.0. |
| `tiers.<name>.candidates` | — (required) | file | `[{provider, model = "", weight = 1}]` |
| `workspace.fetch_throttle` | `60s` | file, flag | ADR-0014 |
| `workspace.fetch_timeout` | `30s` | file, flag | |
| `workspace.idle_reclaim_after` | `4h` | file, flag, workspace `cleanup` | `0` = never |
| `workspace.archived_grace` | `24h` | file, flag, workspace `cleanup` | |
| `workspace.archived_pin_ttl` | unset | file, flag | |
| `workspace.reclaim_timeout` | `10m` | file, flag | |
| `workspace.reclaim.auto` | `[{marker = "Cargo.toml", argv = ["cargo", "clean"]}]` | file | Replaces the built-in list. Each entry also takes `timeout` and `output_paths`. |
| `placement.disk_pressure` | `"10%,20GiB"` | file, flag | ADR-0014 resolution 7 |
| `placement.disk_floor` | `"2GiB"` | file, flag | |
| `logs.level` | `info,horde_server=debug,horde_worker=debug` | file, `HORDE_LOG` | ADR-0015 |
| `logs.max_file_bytes` | `16MiB` | file | |
| `logs.max_files` | `8` | file | |
| `logs.max_age` | `14d` | file | `0` = no age limit |
| `logs.agent_reads.default_bytes` | `16KiB` | file | The `logs` MCP verb |
| `logs.agent_reads.max_bytes` | `64KiB` | file | |
| `logs.agent_reads.calls_per_minute` | `6` | file | |
| `logs.agent_reads.burst` | `3` | file | |

The `[tiers]` table, when present, **replaces the shipped catalog
wholesale**. A catalog cannot be partly the operator's and partly the
default without surprising someone, and "which tiers exist" is one fact.
The shipped catalog stays the default: `fast` (claude haiku), `standard`
(claude sonnet, codex default), and `deep` (claude opus, codex default).

Mode names `bypass` and `ask` are built in. A file mode with the same name
replaces the built-in one.

**Worker** (`~/.horde/worker/config.toml`):

| Key | Default | Layers |
|---|---|---|
| `worker.workspace.root` | unset (no workspaces) | file, `--workspace-root` |
| `worker.workspace.scan_interval` | `60s` | file, flag |
| `worker.workspace.pin_scan_interval` | `6h` | file, flag |
| `worker.workspace.report_interval` | `5m` | file, flag |
| `worker.workspace.disk_low_watermark` | `"10%,20GiB"` | file, flag |
| `worker.workspace.disk_high_watermark` | `"15%"` | file, flag |
| `codex.sandbox.<mode>` | `bypass = "workspace-write"` | file |
| `logs.level` | as the server | file, `HORDE_LOG` |
| `logs.max_file_bytes`, `logs.max_files`, `logs.max_age` | `16MiB`, `8`, `14d` | file |
| `logs.agents.max_file_bytes` | `4MiB` | file |
| `logs.agents.max_files` | `2` | file |
| `logs.agents.budget_bytes` | `256MiB` | file |

`worker.workspace.reclaim_concurrency` is listed in ADR-0014, but W12d did
not build concurrent reclaim, so it is not a key. An entry in the file is
refused as unknown rather than accepted and ignored.

`codex.sandbox` maps a permission-mode name to the codex sandbox tier that
agents under that mode get on this worker. Codex cannot be gated per call
(ADR-0005), so its sandbox is the real control. Without a mapping, a
bypass-mode codex agent would run read-only and fail at its first edit.
`workspace-write` is codex's "edit the checkout, no network" tier. A worker
that wants bypass agents read-only says `codex.sandbox.bypass =
"read-only"`. A configured map replaces the built-in one wholesale, as tiers
do.

**Client** (`~/.horde/client/config.toml`):

| Key | Default | Layers |
|---|---|---|
| `session.permission_mode` | unset (the server decides) | file, `horde new --permission-mode` |
| `logs.level`, `logs.max_file_bytes`, `logs.max_files`, `logs.max_age` | as ADR-0015's client (`4MiB`, `2`, `7d`) | file, `HORDE_LOG` |

`logs.*` is defined and validated on every role now. The logs work (W12h,
ADR-0015) is on a parallel branch and reads these values when it merges.
Until then, nothing on this branch consumes them: they appear in
`config show` with their origin and nothing more. Each role exposes them as
`horde_config::LogKeys`, whose field names match ADR-0015's `SETTING_*`
names.

### Permission modes: bypass by default, the rule chain on request, no judge

`PermissionMode` gains `bypass: bool`. The chain's order becomes:

1. **deterministic deny**. It still runs first and still has the last word,
   so a bypass mode with deny rules means "everything except Y";
2. **questions escalate**. A call that asks the human something is never
   settled by any automatic layer (below);
3. inject forwards escalate (ADR-0006, unchanged);
4. remembered allows;
5. **bypass**: allow, `decided_by = DETERMINISTIC_ALLOW`, reason `"bypass
   mode"`;
6. own orchestrator verbs, then allow rules, then escalate.

The built-in modes:

- `bypass`, the default: `{bypass = true}`.
- `ask`: the previous shipped default, with own orchestrator verbs allowed
  and everything else escalated.

A rule mode is any file mode without `bypass`. Its `deny` and `allow` tables
are the existing `Rule` (`tool`, `where` field prefixes, `under` ancestry).
The judge is not configurable: `judge_instructions` stays in the type for
the chain's own tests and the seam, but no key sets it. A mode that cannot
be authored cannot route to a judge that does not exist.

**Prompts still reach clients.** Bypass is evaluated by the chain, not
passed to the harness. Claude still sends its `can_use_tool` prompt, the
worker still journals `PermissionRequested`, and the chain answers with a
journaled `PermissionDecided`. Every attached client sees both events, and
the agent's history records each call it was allowed to make and why. A
harness-side bypass (`--permission-mode bypassPermissions`,
`approvalPolicy: never`) would make the calls invisible, so it is not used.

**Questions are not permissions.** Claude's `AskUserQuestion` reaches the
chain as a prompt for the tool `AskUserQuestion`. Allowing it would let
Claude run the tool with no answer. So a call to a question tool always
escalates to a human, under every mode, with only authored deny able to
refuse it. On this branch the one question tool is Claude's `AskUserQuestion`.
W12c adds `ProposedToolCall.questions`, and
codex's `item/tool/requestUserInput` reaches the chain that way. When the two
branches meet, the rule becomes "a call that asks questions, or is a
question tool, escalates" in the same position. Before W12c, codex's
`requestUserInput` never reaches the chain: the adapter answers it with an
error. That is W12c's surface to fix.

### Which mode an agent runs under

The server resolves the mode when an agent is created and stamps the name
into the `AgentSpawned` spec. The log then records which mode the agent ran
under. The worker reads the name for the codex sandbox mapping, and the
chain reads it for every request:

- An **agent-originated spawn** inherits its parent's stamped mode and may
  not change it (ADR-0009 attenuation, `lineage.rs`, unchanged). The
  workspace and session layers cannot widen a child beyond its parent.
- An agent a **client** creates (a session's root, or `horde spawn`) gets,
  from most to least specific: the spec's own `permission_mode` (the spawn
  flag), then the session's `permission_mode`, then the Workspace
  Definition's `permission_mode`, then the server's
  `--permission-mode`/`permissions.default_mode`, then `bypass`.

The resolution is a pure function (`config::agent_mode`), and
`GetEffectiveConfig` returns its answer and origin. A mode name nobody
defined is not refused at the RPC, because the registry is configuration
and can change between restarts. As before, the chain escalates every
request from an agent whose mode it cannot resolve, and logs a warning.

An agent spawned before this change has an empty mode in its spec. Its
requests resolve the empty name to the configured default mode, as the
unnamed mode always did.

### Wire changes (additive, `buf breaking` clean)

- `WorkspaceDefinition.permission_mode = 9`, with matching fields on
  `CreateWorkspaceDefinitionRequest` (6) and
  `UpdateWorkspaceDefinitionRequest` (8, where empty means unchanged).
  `horde workspace update --permission-mode ""` cannot clear it, so
  `--clear-permission-mode` does.
- `SessionSettings { string permission_mode = 1; }` on
  `CreateSessionRequest.settings = 5` and on the root's
  `AgentSpawned.session = 5`. The projection folds it into
  `SessionState.settings`.
- `rpc GetEffectiveConfig(GetEffectiveConfigRequest) returns
  (GetEffectiveConfigResponse)`. The request takes an optional workspace (id
  or name) and session id. The response is a list of
  `ConfigEntry { key, value, origin }`.

## Consequences

- A fresh install allows every tool call without asking. The operator sees
  each one in the session, can say `permissions.default_mode = "ask"` to go
  back, and can author rule modes. Questions still stop for a human.
- Codex agents under bypass write their checkout. That is a behavior change
  on every worker, and `codex.sandbox.bypass = "read-only"` restores the
  old behavior.
- Three tier call sites and the mode registry read the server's settings.
  `TierCatalog::shipped_defaults()` and `PermissionModes::shipped()` remain
  the defaults those settings start from.
- The `--workspace-*`, `--placement-*` and worker housekeeping flags lose
  their clap `default_value`s. They become the flag layer, and the default
  lives once, in the key table.
- `docs/CONFORMANCE.md` §13 moves from ABSENT to PARTIAL. The repo layer and
  prompt overlays are not built, and the new layers are documented in
  DESIGN §13.

## Tests

- `horde-config`: the reader's refusals (unknown key with its path and
  siblings, type errors, a key from another role), value parsers, provenance
  rendering, and file loading (missing default versus missing explicit
  file).
- Server unit tests: modes, tiers, workspace, placement and `logs.*` parse
  from a file. The order defaults → file → flag holds per key, with origins.
  `agent_mode` has a truth table over spawn, session, workspace, server and
  default.
- Chain: bypass allows and names itself. Deny still refuses under bypass. A
  question escalates under bypass and past an allow rule, a remembered
  allow, and own-orchestrator verbs. The driver, over a real log with the
  shipped modes, still escalates an `AskUserQuestion` request to a human.
- Server suite (in-process server over a real log): an agent's stamped mode
  comes from each layer in turn (default, file, workspace, session, spawn
  flag), and `GetEffectiveConfig` reports the same origin. A configured
  `[tiers]` catalog replaces the shipped one at `SpawnAgent`.
- Worker: file then flag for the housekeeping keys, the codex sandbox map
  from the file, and `--show-config --origin`.
- CLI suite: `horde config show --origin` over a client file, refusal text
  for a bad file, and `horde config show --server --workspace W --session S`
  against a real server.
