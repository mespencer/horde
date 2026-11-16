# Every harness runs under a Worker-owned profile, in its own process group

A supervised harness read whatever configuration it could find, and the
Worker's permission chain was only as strong as that configuration was
empty. Claude **merges** permission allow lists across
`~/.claude/settings.json`, the checked-out repo's `.claude/settings*.json`,
and `--settings`, so an operator's or a repo's allow rule decided a tool call
before `--permission-prompt-tool` ever saw it (ADR-0003's 2026-08-11
re-verification) — and the same files carry hooks and enabled plugins. An
agent with no account ran on the operator's own `~/.claude`. Codex, launched
with the ambient `CODEX_HOME`, applied the operator's `config.toml`, MCP
servers, and trusted projects (a trusted cwd disables approvals). The live
smokes passed `--setting-sources '' --strict-mcp-config`; production did not,
so the tests proved a harness nobody shipped. Separately, the per-agent
bearer token rode argv, where `ps` shows it to every user on the machine, and
stopping a harness killed its pid alone, leaving its Bash commands, dev
servers, and stdio MCP servers running after archive.

**One `HarnessProfile` per adapter owns all of it** (`horde-worker`'s
`harness` module), rooted at `<state-dir>/harness/<provider>`:

- **The config directory is always the Worker's.** `CLAUDE_CONFIG_DIR` /
  `CODEX_HOME` is always set: `<root>/default` for an agent bound to no
  account, `<root>/accounts/<account>` for one that is. Absolute, created
  owner-only (0700), and an account name that is not one plain directory
  component is refused rather than joined — accounts arrive over the wire,
  and the old resolution put them relative to the child's cwd, inside the
  checkout.
- **Setting sources and MCP are closed.** Every Claude launch carries
  `--setting-sources ''` (no user, project, or local settings file — so no
  ambient allowlist, hook, or plugin) and `--strict-mcp-config` (only the
  Worker's own server). A wired chain adds ADR-0003's empty
  `--settings` lists and `--permission-mode manual`. The flags come from one
  builder, `horde_adapter_claude::launch::IsolationFlags`, which the Worker's
  spawn and both live smokes call, so they cannot drift again. Codex gets the
  same effect from the Worker-owned `CODEX_HOME`: no `config.toml`, no
  servers, no trusted projects but what the Worker gives it.
- **Secrets stay off argv.** Claude's MCP config — carrying the endpoint's
  `Authorization` header — is written per launch to an owner-only (0600) file
  under `<root>/run`, held by the harness and deleted when it is dropped; argv
  names the path. Codex's header values travel in the child's environment
  through `mcp_servers.horde.env_http_headers` (verified against codex-cli
  0.165.0), in variables named `HORDE_MCP_HEADER_TOKEN_<n>` so codex's default
  shell-environment policy (which strips `*TOKEN*`) keeps them out of the
  agent's own commands.
- **Each harness leads its own process group.** It is spawned with
  `process_group(0)`; stopping it sends SIGTERM to the group, waits a bounded
  grace (3 s), then SIGKILLs whatever is left. A group id is signalled only
  while it provably cannot have been reissued — before the leader is reaped,
  or straight after while surviving members still hold it. A harness that
  dies on its own has its group swept when it is reaped; a dropped handle
  SIGKILLs the group.
- **Groups survive a crash in the journal.** Each group is written to an
  additive `harness_group (pgid, fingerprint)` table at spawn and erased at
  reap. At startup, before any agent spawns, `reap_leftover_groups` sweeps
  what a crashed run left: a group is killed only when it is provably this
  Worker's — its leader is gone and its orphans still hold the id, or its
  leader is alive with the same `ps -o lstart=` start time recorded at spawn.
  A leader with a different start time is a stranger who was handed the pid
  (after a reboot) and is never signalled.

**Amendment (2026-11-13), Windows (ADR-0017):** on Windows the process
group is a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, reached by
handle (`horde_worker::process_tree`). A stop closes the harness's stdin,
waits the stop grace, and terminates the job; a dropped handle or a crashed
Worker closes the job and kills every member, so no ledger is written there.
"Owner-only" files and directories inherit the user profile's ACL instead of
a mode. Secrets stay off argv on every platform.

## Consequences

- **A Worker-owned config directory starts logged out.** An agent run there
  fails loudly with the harness's own "Not logged in · Please run /login"
  (Claude reports it as a failed turn with `error: authentication_failed`;
  Codex as an auth-classified thread failure) until it is signed in. The login
  recipe each adapter publishes now carries the profile's config directory in
  its environment, so `horde worker login <w> <provider>` signs in exactly the
  directory account-less agents run under. Per-account login needs the login
  request to name an account, which the wire does not yet carry.

  **Amendment (2026-11-02):** the wire now carries the account. The login
  request has `optional string account = 5` (`proto/horde/v1/login.proto`),
  and `horde worker login <w> <provider> --account <a>` sets it. The worker
  then signs in `<root>/accounts/<a>` (`crates/horde-worker/src/login.rs`,
  `LoginPlane::resolve`). The worker also advertises each account directory
  that a sign-in has written into as a provider account. It re-registers
  after a login that exits cleanly, so a spawn that requires the account can
  be placed.
- Operator conveniences no longer reach agents: user `CLAUDE.md`, user hooks,
  plugins, user-scope MCP servers, and codex profiles are all gone. Project
  `CLAUDE.md` in the checkout still loads — that is repo context, not
  operator configuration. Anything an agent needs is injected explicitly.
- An agent's own commands can still read its owner-only MCP config file (same
  uid); the token it holds authorizes nothing the agent could not already do
  through its endpoint. The boundary closed here is other users and other
  agents' `ps` output, not the agent itself.
- Archived agents give their resources back: the endpoint token and `local`
  MCP children (also for spawns and revivals that never got a harness), the
  Session's loopback endpoint when its last agent goes, and the checkout —
  each repo's HEAD pinned under `refs/horde/archived/<agent>` before the
  worktree, its `horde/<agent>` branch, and the directory are removed, so a
  result that names the commit can still be obtained. Uncommitted work in an
  archived checkout is discarded.
- Startup reaping depends on `ps` (present on macOS and Linux). Without it,
  no leader can be fingerprinted, and a leftover group whose leader is still
  alive is left alone rather than guessed at.
