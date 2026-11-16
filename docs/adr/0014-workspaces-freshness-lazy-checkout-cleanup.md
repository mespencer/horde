# Workspaces: agents start from a fresh base, check out only what they need, and disk is reclaimed in two tiers

Workspaces work in their steady state and go wrong over time. An Instance is
cloned once (`git clone --branch <default_branch>`), and after that nothing
moves its HEAD: a repair fetch updates `refs/remotes/origin/*` and nothing
reads it. Every `worktree` checkout without a parent start point branches from
the Instance HEAD, so the hundredth agent on a long-lived Worker starts from
the commit the Instance was cloned at, weeks behind `main`. A child on another
Worker gets the same stale base, because `parent_start_points` reads only the
local `ShareBroker`. Every agent checks out every repo of the Definition,
including repos it never opens, and its result is stamped and dirty-checked
across all of them. The branch is always `horde/<agent-id>`, so an agent asked
to work on a named feature branch cannot. Cleanup does too much and too
little. Archive pins HEAD under `refs/horde/archived/<agent>` and then removes
the worktree with `--force`, discarding uncommitted work. A checkout of a live
but idle agent keeps its `target/` forever, Instances are never deleted, and
nothing reacts when the disk fills. Three wire pieces are half-built:
`unmanaged_paths` is stored and never read, `MaterializationProgress` has no
producer, and the Worker never calls `ReportWorkspaceInstances` (registration
carries `warm_instances`, so the RPC has no caller at all).

The operator settled the shape on 2026-11-12/13. This
ADR turns those decisions into a design concrete enough to implement. It
keeps everything DESIGN §7 and ADR-0002 already say: Definition versus
Instance, the Worker's state file as the truth, Instances never
self-initiated, and the commit as the unit of sharing.

**An agent starts from the latest `origin/<default_branch>`, and the
Instance's working tree is never touched.** When a spawn composes a
`worktree` or `fresh-clone` checkout, the Worker first fetches each repo it is
about to check out into the Instance (`git fetch --prune origin`). The fetch is
throttled per Instance repo (default 60 s since the last successful fetch) and
single-flight, so ten spawns in a burst share one fetch. The base is the
commit `refs/remotes/origin/<default_branch>` names after that fetch
(`refs/remotes/origin/HEAD` when the Definition leaves `default_branch`
empty). The new branch is created at that commit with `git worktree add -b
<branch> <dest> <sha>`. Nothing ever runs `pull`, `reset` or `checkout` in the
Instance's own working tree. That tree belongs to `shared`-policy agents, and
moving it under them would be a silent mutation of a live checkout. A fetch
that fails or times out (default 30 s, run with `GIT_TERMINAL_PROMPT=0` so a
credential prompt cannot hang it) does not fail the spawn. The base falls
back to the cached remote-tracking ref. If there is none, it falls back to
the Instance HEAD. The fallback is recorded as a warning on the checkout's
provenance event (below).

**A child starts from its parent's commit, on any Worker.** The parent's MCP
endpoint is Worker-side and can see the parent's checkout. When the parent
calls `spawn_agent`, the endpoint observes the parent's committed HEAD in each
repo the parent has checked out, the same `observe_head` the stamp uses. It
stages each commit to the Relay when one is configured (`stage_share`, so the
commit is obtainable anywhere) and writes the commits into the child's spec
as `repo_bases` with source `PARENT`. The child's Worker resolves a `PARENT`
base by making the commit present in the Instance repo (`ShareBroker`
routes: already present, then a peer checkout, then the Relay) before
`worktree add`. This replaces `parent_start_points`, which only worked when
parent and child shared a Worker. A repo the parent has not checked out gets
the fresh default base. Uncommitted parent work is not inherited, as today:
"children start from the parent's committed HEAD".

**A spawn may name a base per repo, and a named base that cannot be resolved
fails the spawn.** `--base atlas=release/1.4` (CLI) or `bases: {"atlas":
"release/1.4"}` (MCP) names a commit SHA, a branch on origin, or a tag. The
Worker resolves it after the throttled fetch, in this order: a full or
abbreviated SHA (`rev-parse --verify <ref>^{commit}`), then
`refs/remotes/origin/<ref>`, then `refs/tags/<ref>`. When the ref is not
found locally, it runs one unthrottled targeted fetch (`git fetch origin --
<ref>`) and tries again. If that fails, the spawn fails with the ref named.
The default base may quietly fall back; a base a human or parent asked for
may not. An explicit base overrides a `PARENT` base for the same repo.

**A workspace checks out every repo or only the repos an agent asks for.**
The Definition gains a checkout mode, `ALL` (the default, and today's
behavior) or `LAZY`. Each spawn may override it, and a spawn may name the
repos to check out at start. A lazy agent starts with those repos, or with
none, plus a manifest of every repo it could check out. It calls the MCP tool
`workspace_checkout(repo)` to add one on demand. Only checked-out repos are
fetched, stamped into results, and dirty-checked. A lazy checkout changes
which agent worktrees exist. It does not change the Instance: materialization
still clones every repo of the Definition, so an on-demand checkout costs a
worktree, not a clone.

**One branch name may span every repo, and the Server refuses a name a live
agent already holds.** `--branch feat/login` names the branch the agent
creates in every repo it checks out, lazily added ones included. The default
stays `horde/<agent-id>`. The Server keeps a claim table in its projection:
`(workspace definition, branch)` maps to the live agent holding it. A spawn
naming a held branch is refused before placement, on any Worker. Claims are
released when the agent is archived or declared lost. The Worker keeps git as
the final judge for branches the Server cannot see (see *Branch collisions*).

**Cleanup has two tiers, and only the first ever runs on its own unless
nothing can be lost.** A *soft* reclaim runs a per-repo reclaim command. It
defaults to `cargo clean` for a repo with a root `Cargo.toml` and to nothing
for any other repo. It leaves the worktree, the branch and every commit in
place. It runs automatically when an agent is archived, when a live agent has
been idle longer than a set time, and oldest-first when the disk is low. A
*hard* removal deletes the worktree and its branch, and in a separate step
the archive pin. It runs automatically only when a precise reachability check
proves nothing is lost. Otherwise it waits for `horde workspace gc --hard
--force`. Checkouts left behind by a worker reset (orphans) are reported and
soft-reclaimed at most. An Instance is removed only by an explicit command or
by `horde workspace delete --purge`. Every cleanup has a `--dry-run`, and
every reclaim, removal and refusal is an event.

**Workspaces are deliberate.** Nothing auto-registers a Definition. Bare
`horde`, started inside a git repo that no Definition contains, may offer once
to create one, and only on an interactive terminal.

**The half-built pieces are finished, not removed.** `unmanaged_paths` become
real per-Instance directories, linked into every checkout. The Worker
journals `MaterializationProgress` while it materializes.
`ReportWorkspaceInstances` becomes the Worker's authoritative resync after
every materialize, every cleanup pass and on a timer, carrying per-checkout
and disk-usage facts. The sections below give the details.

## Wire changes

All additive. Field numbers are the next free ones as of `2df0ef0`.

`workspace.proto`:

```proto
enum CheckoutMode {
  CHECKOUT_MODE_UNSPECIFIED = 0;   // reads as ALL
  CHECKOUT_MODE_ALL = 1;
  CHECKOUT_MODE_LAZY = 2;
}

// How a repo's checkouts are soft-reclaimed. Unset reads as `auto`.
message ReclaimPolicy {
  message Auto {}                  // detect by marker file (see Soft reclaim)
  message None {}
  message Command {
    repeated string argv = 1;      // argv[0] a bare program name or absolute path; no shell
    google.protobuf.Duration timeout = 2;  // unset: the resolved default
  }
  oneof policy { Auto auto = 1; None none = 2; Command command = 3; }
}

message WorkspaceRepo {            // + field
  ReclaimPolicy reclaim = 5;
}

// Workspace-layer overrides of the cleanup defaults (W12e layer "workspace").
message WorkspaceCleanupPolicy {
  google.protobuf.Duration idle_reclaim_after = 1;  // unset: inherit; zero: never
  google.protobuf.Duration archived_grace = 2;      // unset: inherit
}

message WorkspaceDefinition {      // + fields
  CheckoutMode checkout_mode = 7;
  WorkspaceCleanupPolicy cleanup = 8;
}

enum CheckoutState {
  CHECKOUT_STATE_UNSPECIFIED = 0;
  CHECKOUT_STATE_ACTIVE = 1;            // live agent, turn in flight or recently active
  CHECKOUT_STATE_IDLE = 2;              // live agent, idle past idle_reclaim_after
  CHECKOUT_STATE_ARCHIVED_RETAINED = 3; // archived; hard removal not provably lossless
  CHECKOUT_STATE_ORPHAN = 4;            // on disk, no agent this Worker knows
}

message CheckoutReport {
  AgentId agent_id = 1;
  SessionId session_id = 2;
  repeated string repos = 3;            // checked-out repo names
  string branch = 4;
  CheckoutState state = 5;
  google.protobuf.Timestamp last_active_at = 6;
  google.protobuf.Timestamp soft_reclaimed_at = 7;
  uint64 bytes = 8;                     // as of sized_at
  string retained_reason = 9;           // why hard removal was refused
}

message WorkspaceInstanceReport {  // + fields
  repeated CheckoutReport checkouts = 8;
  uint64 instance_bytes = 9;            // whole Instance root, checkouts included
  uint64 unmanaged_bytes = 10;
  uint64 filesystem_free_bytes = 11;
  uint64 filesystem_total_bytes = 12;
  google.protobuf.Timestamp sized_at = 13;
}
```

`agent.proto`:

```proto
enum RepoBaseSource {
  REPO_BASE_SOURCE_UNSPECIFIED = 0;
  REPO_BASE_SOURCE_REQUESTED = 1;  // a human or agent named it
  REPO_BASE_SOURCE_PARENT = 2;     // the spawning parent's committed HEAD
}

message RepoBase {
  string repo_name = 1;
  string ref = 2;                  // SHA, origin branch, or tag
  RepoBaseSource source = 3;       // provenance, not authority
}

message AgentSpec {                // + fields
  CheckoutMode checkout_mode = 9;        // unset: the Definition's
  repeated string checkout_repos = 10;   // initial repos under LAZY; non-empty implies LAZY
  repeated RepoBase repo_bases = 11;
  string branch = 12;                    // unset: horde/<agent-id>
}
```

`event.proto`, new `Event.kind` arms:

```proto
// Per agent, in its Session: exactly what each repo of a checkout started from.
// Journaled at spawn (all initial repos) and at each workspace_checkout (one).
message CheckoutPrepared {
  message Repo {
    string repo_name = 1;
    string branch = 2;
    string base_commit_sha = 3;
    BaseResolution resolution = 4;
    string requested_ref = 5;              // for REQUESTED / PARENT
    google.protobuf.Timestamp fetched_at = 6;  // last successful fetch behind the base
    string stale_reason = 7;               // non-empty = the warning
  }
  enum BaseResolution {
    BASE_RESOLUTION_UNSPECIFIED = 0;
    BASE_RESOLUTION_FRESH = 1;          // origin/<default>, fetched within throttle
    BASE_RESOLUTION_CACHED = 2;         // fetch failed; cached origin/<default>
    BASE_RESOLUTION_INSTANCE_HEAD = 3;  // no remote-tracking ref at all, or `shared`
    BASE_RESOLUTION_PARENT = 4;
    BASE_RESOLUTION_REQUESTED = 5;
    BASE_RESOLUTION_REUSED = 6;         // redelivered spawn / revival: checkout already there
  }
  repeated Repo repos = 1;
  CheckoutMode mode = 2;
  repeated string available_repos = 3;  // the manifest's repo set (lazy)
}

enum CleanupTier { CLEANUP_TIER_UNSPECIFIED = 0; CLEANUP_TIER_SOFT = 1; CLEANUP_TIER_HARD = 2;
                   CLEANUP_TIER_UNPIN = 3; CLEANUP_TIER_INSTANCE = 4; }
enum CleanupTrigger { CLEANUP_TRIGGER_UNSPECIFIED = 0; CLEANUP_TRIGGER_ARCHIVE = 1;
                      CLEANUP_TRIGGER_IDLE = 2; CLEANUP_TRIGGER_DISK_PRESSURE = 3;
                      CLEANUP_TRIGGER_EXPLICIT = 4; CLEANUP_TRIGGER_PURGE = 5;
                      CLEANUP_TRIGGER_SCAN = 6; }

// One cleanup decision on one checkout (or pin, or Instance). Housekeeping stream.
message WorkspaceCleanup {
  WorkerId worker_id = 1;
  WorkspaceDefinitionId workspace_definition_id = 2;
  AgentId agent_id = 3;            // empty for UNPIN of a foreign pin / INSTANCE
  SessionId session_id = 4;        // from the checkout ledger; may be empty for orphans
  CleanupTier tier = 5;
  CleanupTrigger trigger = 6;
  string gc_request_id = 7;        // EXPLICIT / PURGE
  bool dry_run = 8;
  message RepoAction {
    string repo_name = 1;
    enum Outcome { OUTCOME_UNSPECIFIED = 0; DONE = 1; WOULD_DO = 2; SKIPPED = 3;
                   REFUSED = 4; FAILED = 5; }
    Outcome outcome = 2;
    string reason = 3;             // "untracked files: 3", "no reclaim command", "timed out after 10m"
    repeated string argv = 4;      // the reclaim command run, if any
    int32 exit_code = 5;
    string output_tail = 6;        // last 4 KiB of stdout+stderr
    uint64 bytes_freed = 7;
    string pinned_commit_sha = 8;  // HARD: what refs/horde/archived/<agent> now holds
  }
  repeated RepoAction repos = 9;
}

// Server-appended in the housekeeping stream by the gc / instance-remove /
// delete --purge RPCs. Targets are resolved at request time, so the command
// fold stays a pure function of the log.
message WorkspaceGcRequested {
  string request_id = 1;
  repeated WorkerId worker_ids = 2;
  WorkspaceDefinitionId workspace_definition_id = 3;  // empty = every Instance on the targets
  repeated AgentId agent_ids = 4;                     // empty = every checkout
  GcLevel level = 5;
  bool force = 6;
  bool dry_run = 7;
  bool remove_instance = 8;
  bool include_pins = 9;
  string requested_by = 10;        // principal, from the certificate (ADR-0009)
}
enum GcLevel { GC_LEVEL_UNSPECIFIED = 0;   // reads as AUTOMATIC
               GC_LEVEL_SOFT = 1;          // soft only
               GC_LEVEL_AUTOMATIC = 2;     // soft + lossless hard: what the triggers would do
               GC_LEVEL_HARD = 3; }        // + orphans, pins, retained (lossless unless force)

// A Worker's answer to one WorkspaceGcRequested, after its WorkspaceCleanup events.
message WorkspaceGcCompleted {
  string request_id = 1;
  WorkerId worker_id = 2;
  uint32 actions_done = 3;
  uint32 actions_refused = 4;
  uint64 bytes_freed = 5;
  string error = 6;                // the whole request failed (e.g. no workspace support)
}

// The disk stayed below the low watermark after every permitted reclaim. One per episode.
message WorkspaceDiskPressure {
  WorkerId worker_id = 1;
  uint64 free_bytes = 2;
  uint64 low_watermark_bytes = 3;
  bool exhausted = 4;              // true: nothing reclaimable left
}
```

These arms are numbered `checkout_prepared = 36`, `workspace_cleanup = 37`,
`workspace_gc_requested = 38`, `workspace_gc_completed = 39` and
`workspace_disk_pressure = 40`. The Server's ingest judge (ADR-0009) admits
them as follows. `CheckoutPrepared` goes through `judge_agent`.
`WorkspaceCleanup`, `WorkspaceGcCompleted` and `WorkspaceDiskPressure` are
admitted only into the new reserved stream `horde-workspace-housekeeping`,
and only when `worker_id` equals the origin Worker. `WorkspaceGcCompleted`
must also answer a `WorkspaceGcRequested` in that stream that targeted the
Worker, the same shape as `judge_instance`'s prewarm set.
`WorkspaceGcRequested` is Server-written only. The housekeeping stream is
added to `enrollment::is_reserved_stream`, which keeps it out of session
listings and placement walks.

`worker_service.proto`:

```proto
// Server-resolved policy; the Worker never reads Server config.
message ResolvedWorkspacePolicy {
  google.protobuf.Duration fetch_throttle = 1;      // default 60s
  google.protobuf.Duration fetch_timeout = 2;       // default 30s
  google.protobuf.Duration idle_reclaim_after = 3;  // default 4h; zero = never
  google.protobuf.Duration archived_grace = 4;      // default 0
  google.protobuf.Duration reclaim_timeout = 5;     // default 10m
  message MarkerRule { string marker_file = 1; ReclaimPolicy.Command command = 2; }
  repeated MarkerRule auto_rules = 6;               // built-in: Cargo.toml -> cargo clean
}

message SpawnAgentCommand        { ResolvedWorkspacePolicy workspace_policy = 10; }
message MaterializeWorkspaceCommand { ResolvedWorkspacePolicy policy = 3; }

message WorkspaceGcCommand {
  string request_id = 1;
  WorkspaceDefinitionId workspace_definition_id = 2;
  repeated AgentId agent_ids = 3;
  GcLevel level = 4;
  bool force = 5;
  bool dry_run = 6;
  bool remove_instance = 7;
  bool include_pins = 8;
  ResolvedWorkspacePolicy policy = 9;
  SessionId carrying_stream = 10;  // always the housekeeping stream today
}
// StreamCommandsResponse.command: + WorkspaceGcCommand workspace_gc = 17;
```

`worker.proto`: `WorkerCapabilities` gains `uint32 workspace_protocol = 9`.
It is `2` for a Worker that implements this ADR and absent (`0`) for older
ones. Placement refuses to put a spawn that names a base, a branch, a lazy
mode or initial repos on a Worker below `2`; the reason is recorded in the
placement, as with any unmet constraint. The gc RPC lists such Workers as
`unsupported` and never sends them a `WorkspaceGcCommand`.

`control_plane.proto`:

```proto
rpc CollectWorkspaceGarbage(CollectWorkspaceGarbageRequest) returns (CollectWorkspaceGarbageResponse);
rpc RemoveWorkspaceInstance(RemoveWorkspaceInstanceRequest) returns (RemoveWorkspaceInstanceResponse);
rpc ListWorkspaceInstances(ListWorkspaceInstancesRequest) returns (ListWorkspaceInstancesResponse);

message CollectWorkspaceGarbageRequest {
  WorkspaceDefinitionId workspace_definition_id = 1;  // optional
  repeated WorkerId worker_ids = 2;                   // empty = every Worker holding an Instance
  repeated AgentId agent_ids = 3;
  GcLevel level = 4; bool force = 5; bool dry_run = 6; bool include_pins = 7;
}
message CollectWorkspaceGarbageResponse {
  string request_id = 1;
  repeated WorkerId targeted = 2;     // online, protocol >= 2: answers will follow
  repeated WorkerId offline = 3;      // command waits in their stream
  repeated WorkerId unsupported = 4;
  uint64 sequence = 5;                // log position of the WorkspaceGcRequested
}
message RemoveWorkspaceInstanceRequest {
  WorkspaceDefinitionId workspace_definition_id = 1; WorkerId worker_id = 2;
  bool force = 3; bool dry_run = 4;
}
message RemoveWorkspaceInstanceResponse { string request_id = 1; uint64 sequence = 2; }
message ListWorkspaceInstancesRequest { WorkerId worker_id = 1; WorkspaceDefinitionId workspace_definition_id = 2; }
message ListWorkspaceInstancesResponse { repeated WorkspaceInstanceReport instances = 1; }

message DeleteWorkspaceDefinitionRequest {  // + fields
  bool purge = 3;     // also remove every Instance (a WorkspaceGcRequested{remove_instance})
  bool force = 4;     // only with purge
  bool dry_run = 5;   // only with purge: plan the purge, delete nothing, keep the Definition
}
message DeleteWorkspaceDefinitionResponse { string purge_request_id = 1; }  // + field
```

`CreateWorkspaceDefinitionRequest` and `UpdateWorkspaceDefinitionRequest`
gain `checkout_mode` and `cleanup`. Per-repo `reclaim` rides the existing
`repos` field: Update may change `reclaim` and `layout_path` but still not
the repo set. The comment at `control_plane.proto:117-118` about
auto-registration from cwd is corrected to say a Definition is always
explicit.

## Worker

### The checkout ledger

Each agent-private checkout gets a ledger record in a file at
`<root>/<def-id>/.horde-agents/.ledger/<agent-id>.pb`. It lives beside the
checkout, outside the agent's cwd, and the leading dot keeps
`agent_private()` from ever mistaking it for an agent. The record holds the
agent and session ids, the checkout policy, the branch, whether the Worker
created the branch or adopted it, each checked-out repo with its base and
resolution, `created_at`, `last_active_at`, `soft_reclaimed_at`,
`archived_at` and `retained_reason`. It stores ids, never paths: every path
is derived from ids through the existing `is_path_safe` and `layout_of`
guards, so an edited ledger cannot point the Worker at anything outside the
Instance.

The ledger is a file and not a journal table because a worker reset
(ADR-0011) replaces the journal. Checkouts that survive a reset must still
say whose they were, so their cleanup events can name a session and the
operator can decide what to do with them. The ledger is written before
`worktree add` with the repo marked `pending` and finalized after it, so a
crash leaves a record the next scan can repair or remove. The pass after the
crash runs `git worktree prune` and then removes the half-built directory.
`last_active_at` is updated at every turn end and every
`workspace_checkout`, written through, at most once a minute per agent.

`ShareBroker`'s `AgentCheckout.repos` becomes exactly the checked-out subset.
`agent_checkout` (spawn and revival) recomposes from the ledger when one
exists, so a revival or redelivered spawn reuses the same repos, branch and
bases and journals `CheckoutPrepared` with `REUSED`. It never re-resolves a
fresh base under a running agent.

### Freshness

`WorkspaceManager` gains `prepare_base(instance_repo, repo, base:
Option<&RepoBase>, policy) -> ResolvedBase`. It is driven by `compose` for
each repo being checked out:

1. A `PARENT` or `REQUESTED` base that is a full SHA already present in the
   Instance repo is used as-is, with no fetch.
2. Otherwise the throttled fetch runs. A per-`(instance, repo)` async mutex
   holds the last successful fetch `Instant`, in memory only; a restart costs
   one extra fetch. The fetch is `git -C <instance repo> fetch --prune
   --no-tags origin` under `fetch_timeout`, with `GIT_TERMINAL_PROMPT=0` and
   `GIT_ASKPASS=` set empty. Spawns waiting on the mutex reuse its outcome.
3. Resolution follows the rules in the decision above. A `PARENT` SHA still
   missing after the fetch goes to `ShareBroker::obtain_into(instance_repo,
   sha)`, a new sibling of `obtain` that targets the Instance repo instead of
   an agent checkout. If that fails, the parent's commit is unavailable. That
   is a spawn failure: the parent said where to start.
4. A fallback's `stale_reason` names the git error, and `fetched_at` is the
   last success, or unset if there was none.

`shared` checkouts skip all of this. They journal `INSTANCE_HEAD` with the
Instance HEAD SHA, and `--base`, `--branch` and lazy mode are refused for
them at the Server, because each would mean mutating the Instance tree.

`fresh-clone` clones from the Instance repo as today. It then runs `git
switch -c <branch> <base>`, always rather than only for a start point, so a
fresh clone also starts from the fresh base.

The Instance repo's local default branch stays at its clone point. Nothing
reads it. It counts as a keeper only through the reachability rules below,
where it is trivially reachable from origin.

### Lazy checkout and the manifest

The mode is `spec.checkout_mode`, then the Definition's, then `ALL`. A
non-empty `checkout_repos` implies `LAZY`. Under `LAZY`, `compose` creates
worktrees only for `checkout_repos`. A name not in the Definition fails the
spawn with the valid names listed.

The manifest is written to `<agent_root>/.horde/workspace.json`. That is
inside the composed root but outside every repo layout, since the Server
refuses a layout beginning `.horde`. `HORDE_WORKSPACE_MANIFEST` in the
harness environment points at it. The file is rewritten atomically
(write-temp-then-rename) on every change:

```json
{ "workspace": "atlas-suite", "mode": "lazy", "branch": "horde/agent-7f3",
  "repos": [ { "name": "atlas", "path": "atlas", "default_branch": "main",
               "checked_out": true, "base": "9c1e…", "resolution": "fresh" },
             { "name": "proto", "path": "proto", "default_branch": "main",
               "checked_out": false, "pending_commits": ["41aa…"] } ] }
```

The prompt overlay the Worker applies at spawn gains a generated paragraph
under `LAZY` only. It lists the repo names and paths and says to call
`workspace_checkout` before touching a repo that is not checked out.

`workspace_checkout(repo, base?)` is served by the endpoint and executed by
the Runtime through a new `WorkspaceAccess` trait, the same seam pattern as
the endpoint's `JournalSink`. It runs as a local message on the agent's lane
(ADR-0012), so it is ordered with that agent's deliveries and archive, and a
checkout racing an archive loses cleanly. The steps are:

1. Fence through the workspace lane, so an Instance mid-materialize or
   mid-removal is waited for or refused.
2. Run `prepare_base`.
3. Run `worktree add` with the agent's branch.
4. Update the ledger, the broker's repo set and the manifest.
5. Journal `CheckoutPrepared` for that one repo.
6. Return `{path, branch, base_commit_sha, resolution, stale_reason}`.

A repo that is already checked out returns its existing path and does
nothing else, so the call is idempotent. An unknown repo, or a call from an
agent with a `shared` policy or no workspace, is an `invalid_params` error
naming the reason. The call is an orchestrator verb like `spawn_agent`, named
`mcp__horde__workspace_checkout` in the permission chain per ADR-0004's
one-name rule. Its default rule is allow, because it only creates a worktree
inside the agent's own root, from its own workspace.

**Stamping and dirty checks** walk `AgentCheckout.repos`, which is now the
checked-out set. A result from a lazy agent carries `GitRef`s for exactly
the repos it checked out. A dirty repo it never checked out cannot exist.

**Receiving a payload into a repo the receiver has not checked out** does
not create a worktree, because the receiver did not ask for one. `obtain`
makes the commit present in the receiver's Instance repo through
`obtain_into`, adds the SHA to that repo's `pending_commits` in the manifest,
and treats the delivery as complete. When the agent later calls
`workspace_checkout(repo, base=<sha>)`, the commit is there. If git's own gc
pruned the unreferenced objects in between, `prepare_base`'s `PARENT` and
`REQUESTED` path re-obtains them from a peer or the Relay. A repo outside the
receiver's Definition is still an error, as today. The sending side changes
by construction: a lazy sender stamps only what it checked out, so it never
ships a `GitRef` for a repo it never touched.

### Branches

The branch is `spec.branch`, or `horde/<agent-id>` when unset. The Server
validates it (below). The Worker also runs `git check-ref-format --branch
<name>` before use and refuses a failure. In each Instance repo:

- **No local branch of that name.** If `refs/remotes/origin/<name>` exists
  and the spawn named no base for this repo, the base becomes that
  remote-tracking ref (resolution `REQUESTED`, `requested_ref` =
  `origin/<name>`). `--branch` on an existing branch continues it. The
  branch is created with `worktree add -b`, and the ledger records
  `created_branch = true`.
- **A local branch exists and is checked out in another worktree.** The
  spawn fails, naming the holder agent from the ledgers or "a worktree Horde
  did not create". The usual case is an archived agent's retained checkout,
  and the error says so: `horde workspace gc --hard --agent <id> [--force]`.
- **A local branch exists and is not checked out anywhere.** If its tip is
  an ancestor of, or equal to, the resolved base, the Worker moves it to the
  base (`branch -f`, lossless by the ancestry check) and adopts it
  (`created_branch = false`). Otherwise the spawn fails: the branch holds
  commits the base does not, and silently branching beside it or resetting
  it would lose or hide them.

A `fresh-clone` checkout has its own refs, so only the Server's claim table
guards its name.

### Soft reclaim

`soft_reclaim(checkout, trigger)` runs on the agent's lane. It never runs
while that agent has a turn in flight; a pending one is deferred until the
turn ends. For each checked-out repo, the command is the repo's
`ReclaimPolicy`:

- **`none`**: skipped, reason "no reclaim command".
- **`command`**: run as given.
- **`auto`** (or unset): the first `auto_rules` entry whose `marker_file`
  exists at the repo root. The built-in, server-supplied rule is `Cargo.toml`
  → `["cargo", "clean"]`, with one guard. The Worker first runs `cargo
  metadata --no-deps --format-version 1 --offline` and reads
  `target_directory`. If that directory is not inside this repo's checkout
  (a shared `CARGO_TARGET_DIR`, or `build.target-dir` in a `.cargo/config`),
  the repo is skipped with that reason, because `cargo clean` would wipe a
  directory other checkouts are using. No matching rule means skipped.

Execution never involves a shell:

- `tokio::process::Command::new(argv[0]).args(&argv[1..])`, with cwd set to
  the repo's checkout directory.
- `env_clear()`, then only `PATH`, `HOME`, `LANG`, and `CARGO_HOME` /
  `RUSTUP_HOME` when set in the Worker's environment. `CARGO_TARGET_DIR` is
  deliberately absent.
- stdin is null. The child runs in its own process group (ADR-0010's
  `HarnessProfile` launcher; a Job Object on Windows under W12g).
- On timeout the whole group gets SIGTERM, then SIGKILL 5 s later.
- `argv[0]` must be a bare name with no path separator, resolved on that
  `PATH`, or an absolute path. A relative path is refused at the Server.
- The last 4 KiB of combined output and the exit code go into the
  `RepoAction`.

Bytes freed are measured by a bounded directory walk of the repo checkout
before and after, run in `spawn_blocking`. Symlinks are not followed, and
the walk is capped at 30 s; past the cap the field is left unset rather than
guessed.

Reclaim commands come only from the Definition, which an enrolled Client
writes through the control plane, or from Server config. They are never read
from the repo (`.horde/`, Makefiles) or from an agent: a checked-out repo is
content agents write. Agent-originated spawns cannot change a Definition
(ADR-0009). The Worker cannot verify what an operator-authored command
deletes. It guarantees only that the command runs in the repo, without a
shell, with a bounded lifetime.

**Triggers:**

- **Archive.** After `ArchiveAgentCommand`, once the pump has stopped:
  soft reclaim. Then, when `archived_grace` is zero, the lossless hard check;
  otherwise the checkout is marked for a scan at `archived_at + grace`. This
  replaces `release_checkout`'s unconditional pin-and-force-remove (ADR-0010,
  ADR-0012 consequence). Uncommitted work is now retained, not discarded.
- **Idle.** A periodic scan (every `scan_interval`, default 60 s) soft
  reclaims live agents idle longer than `idle_reclaim_after` (default 4 h),
  once per idle period. The flag resets when a turn starts.
- **Disk pressure.** The same scan reads `statvfs` on the workspace root
  (`GetDiskFreeSpaceExW` on Windows). Free space below `disk_low_watermark`
  (default the larger of 10 % and 20 GiB) starts an episode. Candidates not
  yet soft-reclaimed since their last activity are taken oldest first:
  archived-retained by `archived_at`, then orphans by directory mtime, then
  idle live agents by `last_active_at`. Busy agents are never candidates.
  Reclaim continues until free space reaches `disk_high_watermark` (default
  15 %, the hysteresis band) or the candidates run out. Lossless hard
  removals of archived checkouts follow, oldest first. Still under the low
  watermark at the end means one `WorkspaceDiskPressure{exhausted: true}`
  per episode. Disk pressure never triggers a forced hard removal.

A `RepoAction` that fails stops nothing else: the next repo and the next
checkout proceed. The checkout's `soft_reclaimed_at` is set only when every
repo was `DONE` or `SKIPPED`.

### Hard removal and the reachability check

Hard removal is per checkout and per repo. It applies only to archived or
orphaned checkouts, never a live agent's, not even with `--force`: a live
agent's working directory is not the operator's to delete out from under it.
To get rid of one, archive the agent first. Instance removal refuses outright
while any live agent occupies the Instance.

Let `I` be the Instance repo, `W` the agent's worktree of it (or its clone,
for `fresh-clone`), `B` the agent's branch, and `A` the agent id.

**`removable(W)`: the worktree and branch can go without losing anything.**
All of the following must hold:

1. **Clean.** `git -C W status --porcelain=v1 -z --untracked-files=all
   --ignore-submodules=none` prints nothing. Untracked non-ignored files
   count as loss. Ignored files do not: they are by definition what a build
   produces and what soft reclaim deletes (see *Open questions*).
2. **No operation in progress.** None of `MERGE_HEAD`, `CHERRY_PICK_HEAD`,
   `REVERT_HEAD`, `BISECT_LOG`, `rebase-merge/` or `rebase-apply/` exists in
   `git -C W rev-parse --git-dir`.
3. **Not locked.** `git -C I worktree list --porcelain` shows no `locked`
   line for `W`.
4. **One pin covers everything the removal deletes.** Let `H` =
   `rev-parse HEAD` in `W` and `T` = the tip of `B`. `T` must equal `H` or be
   an ancestor of it (`merge-base --is-ancestor T H`).
5. **For `fresh-clone` only, the clone holds nothing else.** `git -C W
   rev-list --all --not H` (that is, every local ref, `refs/stash`
   included, minus `H`'s history) prints nothing. A worktree's stash and its
   other branches live in `I`'s common ref store and survive the removal, so
   this test applies to clones only.
6. **Every step of a git probe succeeded.** Any git error makes the answer
   "not provable". It is never treated as "clean".

The removal itself:

1. Pin: `git -C I fetch --no-tags -- W +H:refs/horde/archived/A`. For a
   worktree this is just a ref update, since the objects are shared.
2. Re-run checks 1–2, closing the window between the check and the pin.
3. `git -C I worktree remove W`, without `--force`, so git refuses a dirty
   tree independently. A clone is deleted with `remove_dir_all`.
4. `git -C I branch -D B`, only if `created_branch`. An adopted branch is
   left in place, because it existed before the agent.
5. The agent root is deleted. `remove_dir_all` does not follow symlinks, so
   the unmanaged-path links go and their targets stay.
6. The ledger record is deleted last.

Every step is idempotent, so a crash between any two steps re-runs safely.

**`unpinnable(P)`: an archive pin can go.** `P` =
`refs/horde/archived/<agent>` in `I`. The pin can go when, in this pass,
after a fetch that succeeded within the pass (a failed fetch makes the answer
"not provable"), at least one of these holds:

- `git -C I for-each-ref --contains P refs/remotes/origin` is non-empty:
  the commit was pushed or merged with history preserved.
- There is a Relay, `git ls-remote <relay>/<repo> 'refs/horde/share/*'`
  succeeded, and some listed SHA `S` is present in `I` with
  `merge-base --is-ancestor P S`. The commit was staged to the Relay, whose
  mirror never prunes share refs (`mirror.rs`, `refresh` without `--prune`).

The pin's own commit is the whole question. Squash-merged or rebased-away
work leaves `P` unreachable and the pin stays, which is the safe answer.
Pins are never dropped by age unless `archived_pin_ttl` is set (default:
never), and even then only by `gc --hard --include-pins --force`.

**`instance_removable(I)`: the Instance can go.** All of the following:

- No live agent occupies the Instance.
- Every remaining checkout is `removable` and gets removed in the same pass.
- The Instance working tree is clean by rule 1.
- Every ref under `refs/heads/*` and `refs/horde/*` in every Instance repo
  passes the `unpinnable` reachability test.
- Every unmanaged path is empty. Non-empty unmanaged data cannot be proven
  recoverable, so it always needs `--force`, and `--dry-run` lists it with
  sizes.

The removal journals `WorkspaceInstanceChanged{MISSING}` into the
housekeeping stream, deletes the Instance root, and sends an incremental
`ReportWorkspaceInstances`.

**Automatic hard removal** is exactly `removable` on archived checkouts, at
archive or after the grace period, and `unpinnable` on pins, during a scan
at most every `pin_scan_interval` (default 6 h; each pass costs one fetch per
repo). Orphans are never hard-removed automatically, even when lossless. The
operator's rule is "reported, soft at most", and an orphan's session may
still matter to someone.

### Orphans

An orphan is a ledger record, or a directory under `.horde-agents/` with no
record at all, whose agent the Runtime has no lane or revivable record for.
After a worker reset that is every pre-reset checkout. Orphans appear in
`CheckoutReport` with state `ORPHAN` and in a `WorkspaceCleanup{tier: SOFT,
trigger: SCAN}` the first time they are seen, which records the soft reclaim
or why it was skipped. Only `gc --hard` naming them, explicitly or through a
workspace or worker filter, removes them, under the same `removable` rule or
with `--force`.

### `WorkspaceGcCommand`

The command runs on the workspace lane. It plans first: it walks the
matching checkouts, pins and, with `remove_instance`, the Instance, and
evaluates the predicates above. Then it executes each per-checkout action on
that agent's lane, unless `dry_run`. It journals one `WorkspaceCleanup` per
checkout, pin or Instance acted on or refused, and then one
`WorkspaceGcCompleted`. The levels:

| level | soft | hard (archived, lossless) | orphans | pins (lossless) | non-lossless |
|---|---|---|---|---|---|
| `SOFT` | yes | – | soft | – | – |
| `AUTOMATIC` (default) | yes | yes | soft | – | – |
| `HARD` | yes | yes | yes, if lossless | with `include_pins` | only with `force` |

`dry_run` still fetches, because the reachability proof needs it, and only
remote-tracking refs move. Every other action becomes `WOULD_DO` or
`REFUSED`. Idempotency keys are `gc:<request_id>:<agent|instance>:<repo>:<tier>`,
so a redelivered command journals nothing twice.

### Instance reports

The Worker calls `ReportWorkspaceInstances(authoritative = true)` with the
full `warm_instances()` set at these moments:

- after every materialize (READY or DEGRADED);
- after every cleanup pass that changed anything;
- after an Instance removal;
- every `report_interval` (default 5 min). This interval is what notices
  out-of-band deletion, which no event could.

`warm_instances()` gains:

- `CheckoutReport`s built from the ledgers plus the Runtime's live set;
- the sizes from the last bounded walk;
- `filesystem_*` from `statvfs`;
- the real `head_commit_sha` and `dirty` per Instance repo, which it now
  reports instead of empty values.

The journaled `WorkspaceInstanceChanged` events stay the history. The RPC is
the cache correction, and registration's `warm_instances` remains the
first authoritative set.

### MaterializationProgress

`materialize` takes a progress sink. Before each repo it journals
`WorkspaceInstanceChanged{state: MATERIALIZING, progress: {step: "cloning
atlas" | "fetching atlas" | "creating unmanaged paths", completed_repos,
total_repos}}` into the materialize command's carrying stream, then the final
READY or DEGRADED report as today. That is at most `repos + 2` events per
materialize, deliberately coarse: git's own progress percentages would flood
the log for no decision anyone makes. The existing prewarm judge already
admits these events into that stream.

### Unmanaged paths

The Server validates each path:

- relative, every component `Normal`;
- not beginning `.horde`;
- not equal to, inside, or containing any repo's layout path;
- no two paths nested inside each other.

`materialize` creates `<instance>/<path>` (`create_dir_all`) after the repos.
`compose` links `<agent_root>/<path>` to it: a symlink on Unix, a directory
junction on Windows (W12g). It also adds the paths to
`<agent_root>/.horde/workspace.json` under `"unmanaged"`. `shared` agents see
the real directory. Payload stamping never looks there, because it walks
repos only, and that is now tested rather than incidental. A path that
UpdateWorkspaceDefinition drops is left on disk and listed in the Instance
report's detail. Only Instance removal deletes unmanaged data.

## Server

- **Validation** (`workspace.rs`):
  - `validate_definition` adds the unmanaged-path rules and the reclaim
    `argv` rules: non-empty; `argv[0]` a bare name or an absolute path; no
    NUL byte; at most 64 entries and 4 KiB in total.
  - A `LAZY` Definition refuses nested layouts, where one repo's layout is
    an ancestor of another's. A lazily added outer repo would land on a
    non-empty directory, and `AddWorkspaceRepo` to a `LAZY` Definition
    applies the same check.
- **Spawn validation** (`service/commands.rs`, `lineage.rs`):
  - `branch` must be ≤ 200 bytes, have no leading `-`, no `..`, no ASCII
    control or space characters, no `~^:?*[\`, not end in `.lock` or `/`,
    not be `HEAD`, and not begin `horde/` unless it equals
    `horde/<this agent id>`.
  - Each `repo_bases` entry must name a repo of the Definition, and its
    `ref` must not be option-shaped.
  - Each `checkout_repos` entry must name a repo of the Definition.
  - `shared` combined with any of `branch`, `repo_bases`, `checkout_repos`
    or `LAZY` is refused.
  - `inherited` leaves all four fields to the child, and the endpoint, not
    lineage, fills `PARENT` bases. `exceeds` treats none of them as a
    capability: they choose where in the parent's own workspace to start,
    not what the child may do.
- **Branch claims.** The projection folds `AgentSpawned` and lifecycle
  changes into `claims: (definition id, branch) → agent id`, covering live
  agents only. `SpawnAgent` with a held branch fails `FAILED_PRECONDITION`,
  naming the holder agent and session. Default names are never claimed,
  because they cannot collide.
- **Policy resolution.** The Server resolves `ResolvedWorkspacePolicy` from
  built-in defaults, then the Server config file (W12e), then the
  Definition's `cleanup`. It stamps the result into every
  `SpawnAgentCommand`, `MaterializeWorkspaceCommand` and
  `WorkspaceGcCommand` it folds. A config change takes effect on the next
  command that carries policy. The Worker keeps the last policy it saw per
  Instance, in memory, for its own scans.
- **Gc RPCs.** `CollectWorkspaceGarbage`, `RemoveWorkspaceInstance` and
  `DeleteWorkspaceDefinition{purge}` each resolve their target Workers from
  the `InstanceCache` plus the registry. They split the targets into
  targeted, offline and unsupported, append one `WorkspaceGcRequested` to
  the housekeeping stream, and return. The command fold turns that event
  into one `WorkspaceGcCommand` per targeted or offline Worker
  (`command_id` = `<stream>:<seq>:gc:<worker>`). Offline Workers receive it
  on reconnect. `delete --purge` appends the definition-deleted change first
  and then the gc request, so the purge's commands still carry the last
  Definition, which the fold keeps for deleted ids. `delete --purge
  --dry-run` appends only the gc request, with `dry_run`.
- **Instance cache.** `CheckoutReport`s and disk facts ride the cached
  reports. `GetWorkspaceDefinition` and `ListWorkspaceInstances` serve them.
  Placement does not yet read disk facts (see *Open questions*).

## CLI

| Verb | Change |
|---|---|
| `horde workspace create NAME --repo … [--unmanaged P]… [--checkout-mode all\|lazy] [--reclaim REPO=auto\|none\|JSON-ARGV]… [--reclaim-timeout REPO=DUR]… [--idle-reclaim-after DUR] [--archived-grace DUR]` | new flags; `--reclaim atlas='["make","clean"]'` |
| `horde workspace create --from-cwd [NAME]` | the explicit single-repo shortcut: origin URL, `origin/HEAD` branch, dir name |
| `horde workspace update …` | same new flags (`workspace_update.rs`) |
| `horde workspace get WS` | instances with checkouts (state, branch, size, retained reason) and disk free |
| `horde workspace instance list [WS] [--worker W]` | `ListWorkspaceInstances` |
| `horde workspace instance remove WS --worker W [--force] [--dry-run]` | `RemoveWorkspaceInstance`; waits like gc |
| `horde workspace delete WS --etag E [--purge [--force] [--dry-run]]` | purge flags |
| `horde workspace gc [WS] [--soft\|--hard] [--force] [--dry-run] [--include-pins] [--worker W]… [--agent A]… [--wait DUR] [--json]` | `--force` requires `--hard`; neither flag = `AUTOMATIC` |
| `horde workspace log [WS] [--worker W] [--follow] [--json]` | reads the housekeeping stream |
| `horde workspace prewarm WS W [--wait]` | renders `MaterializationProgress` while waiting |
| `horde new` / `horde agent spawn` | `--checkout-mode`, `--repos a,b` (implies lazy), `--base [REPO=]REF`… (bare `REF` only for a one-repo workspace), `--branch NAME` |

`gc`, `instance remove` and `delete --purge` subscribe to the housekeeping
stream from the returned `sequence`. They render each `WorkspaceCleanup` as
a row (worker, agent, repo, outcome, freed, reason) and stop when every
targeted Worker's `WorkspaceGcCompleted` has arrived or `--wait` (default
120 s) runs out. Exit codes follow ADR-0008's script-readable refusals:

- `0`: every targeted Worker completed and nothing was `FAILED`.
- `3`: some Workers were offline, unsupported or still pending at the
  deadline. They are listed, and offline Workers will still act on
  reconnect.
- `1`: refused or failed.

`--dry-run` output is headed `DRY RUN — nothing was changed`.

**The front door's offer.** Bare `horde` (ADR-0008) makes the offer only
when all of these hold:

- stdin and stdout are terminals and `--no-input` is not set;
- cwd is inside a git work tree with an `origin` remote;
- no live Definition holds a repo whose normalized remote URL matches
  (scheme, credentials, `.git` suffix and trailing `/` stripped,
  scp-style `host:path` folded to `ssh://host/path`);
- the repo root is not in the Client's `declined-workspace-offers` list in
  its identity dir.

It then asks once: `No workspace contains <root> (origin <url>). Create
workspace "<dir>" from it? [y/N]`. Yes calls the `--from-cwd` path and
continues into the front door with that workspace preselected for "new
session". No records the root as declined. A local-path origin adds a
warning that Workers on other machines cannot clone it. A non-TTY, a cwd
outside git, or a declined repo gets no prompt, only a one-line hint naming
`horde workspace create --from-cwd`. When a matching Definition exists, the
front door preselects it for "new session". Nothing is ever created without
the yes.

## MCP

- `spawn_agent` gains optional `repos: [string]`, `bases: {repo: ref}`,
  `branch: string` and `checkout_mode: "all" | "lazy"`. The endpoint adds
  `PARENT` bases for every repo the caller has checked out that `bases` does
  not name, staging each to the Relay first.
- New tool `workspace_checkout {repo: string, base?: string}`. It returns
  `{path, branch, base_commit_sha, resolution, stale_reason}`, or an error
  naming the valid repos.
- The served tool list, `tool_names_for` and its test grow by one name.

## Configuration (W12e)

W12e's layering is defaults → file → workspace → session → flags. This
ADR's settings fall into those layers as follows. Each key is a plain
duration or size string, and the built-in default is listed.

| Key | Layer | Default | Notes |
|---|---|---|---|
| `workspace.fetch_throttle` | server file | `60s` | |
| `workspace.fetch_timeout` | server file | `30s` | |
| `workspace.idle_reclaim_after` | server file → Definition `cleanup` | `4h` | `0` = never |
| `workspace.archived_grace` | server file → Definition `cleanup` | `0s` | |
| `workspace.archived_pin_ttl` | server file | unset (never) | only with `gc --hard --include-pins --force` |
| `workspace.reclaim_timeout` | server file → repo `reclaim.command.timeout` | `10m` | |
| `workspace.reclaim.auto` | server file | `[{marker = "Cargo.toml", argv = ["cargo","clean"]}]` | replaces the built-in list when set |
| `workspace.checkout_mode` | Definition → spawn flag | `all` | no file key: a property of the workspace |
| `worker.workspace.root` | worker file | (existing flag) | |
| `worker.workspace.disk_low_watermark` | worker file | `max(10%, 20GiB)` | a machine property, so worker-local |
| `worker.workspace.disk_high_watermark` | worker file | `15%` | must exceed the low watermark |
| `worker.workspace.scan_interval` | worker file | `60s` | |
| `worker.workspace.pin_scan_interval` | worker file | `6h` | |
| `worker.workspace.report_interval` | worker file | `5m` | |
| `worker.workspace.reclaim_concurrency` | worker file | `2` | concurrent soft reclaims per Worker |

The rule behind the split: what a workspace's agents experience (freshness,
retention, reclaim commands) is Server-resolved and travels in commands,
because the Worker never reads Server config. What depends on the machine
(disk, scan cadence, concurrency) lives in the Worker's file. Until W12e
lands, each key has a matching `--flag` on `horde-server` / `horde-worker`,
and the defaults above apply.

## Failure modes

| Situation | Behavior |
|---|---|
| Fetch fails or times out at spawn | spawn proceeds on the cached `origin/<default>` (else Instance HEAD); `CheckoutPrepared.stale_reason` set |
| Requested base not resolvable after one targeted fetch | spawn fails (the existing spawn-failed path: `FAILED` lifecycle), ref named |
| Parent commit unobtainable (no Relay, parent on another Worker) | spawn fails; reason names `NoRelay` |
| Branch held by a live agent | `SpawnAgent` refused at the Server, holder named |
| Branch checked out by a retained or foreign worktree | spawn fails at the Worker, with the gc hint |
| Local branch has commits the base lacks | spawn fails; nothing reset |
| `workspace_checkout` of an unknown repo, or under `shared` | tool error, nothing changed |
| `workspace_checkout` during archive | the lane orders them; after archive the agent is gone and the call errors |
| Instance removal while agents are live | refused, even with `--force` |
| Reclaim command missing (`ENOENT`), non-zero exit, timeout | `FAILED` RepoAction with exit code and output tail; process group killed on timeout; checkout untouched |
| `cargo metadata` fails | repo skipped ("could not locate target dir"), never a blind `cargo clean` |
| A git error in any reachability probe | "not provable": retained, reason recorded |
| Crash mid hard removal | every step idempotent; the next scan finishes or retains |
| Crash mid worktree add | ledger `pending`; next scan prunes and removes |
| Worker offline during gc | command waits in its stream; CLI exits `3` listing it |
| Worker reset | ledgers survive; checkouts become orphans: reported, soft at most |
| Relay unreachable during gc | Relay keepers absent, so fewer pins provably droppable |
| Disk still low after all reclaim | one `WorkspaceDiskPressure{exhausted}` per episode; nothing forced |
| Definition deleted without `--purge` | Instances stay and are reported against the deleted id; `gc --hard` or `instance remove` can remove them |
| Agent replaced its unmanaged-path symlink with a real dir | that dir is inside the agent root and goes with it; Instance data untouched |
| Old Worker (`workspace_protocol < 2`) | spawns using the new fields are not placed there; gc lists it as unsupported |

## Test plan

Real git, real repos in temp dirs, and production code throughout: per the
ground rules, no test re-implements a predicate to check it.

- **Worker unit tests** (`workspace.rs`, new `cleanup.rs`), each over
  fixture repos built with git:
  - `removable`: the full truth table (untracked file, ignored file, staged
    change, merge in progress, rebase in progress, locked worktree, branch
    tip ahead of a detached HEAD, a fresh clone with an extra branch, a fresh
    clone with a stash);
  - `unpinnable`: pushed, merged, squash-merged (retained), relay-staged,
    fetch failed (retained);
  - `instance_removable`;
  - base resolution order (SHA, origin branch, tag, unknown);
  - branch adoption (not present / ancestor / diverged / checked out
    elsewhere);
  - unmanaged-path link creation;
  - `remove_dir_all` on an agent root leaves the unmanaged target's files
    intact;
  - the auto-rule `target_directory` guard (target inside the repo vs a
    `.cargo/config.toml` pointing outside).
- **Worker suite** (`worker_suite/workspaces.rs`, local bare remotes):
  - **freshness:** the remote advances, so a new agent's base is the new
    tip and the Instance HEAD and working tree are unchanged;
  - **throttle:** two spawns within the throttle produce one fetch, observed
    through `CheckoutPrepared.fetched_at` equality and the remote's
    `upload-pack` invocation count via a wrapper set in
    `remote.origin.uploadpack`;
  - **stale fallback:** the remote is made unreachable, so the spawn
    succeeds with `CACHED` and a `stale_reason`;
  - an explicit bad base fails the spawn;
  - **lazy:** no worktrees at start, manifest contents, `workspace_checkout`
    creates exactly one, a repeat call is idempotent, a result stamps only
    checked-out repos, a payload into a non-checked-out repo leaves no
    worktree and a later `workspace_checkout(base=sha)` lands on it, and
    revival recomposes the same subset;
  - `--branch` across repos;
  - **soft reclaim:** a real `cargo clean` on a fixture crate (`target/`
    gone; worktree, branch and commits intact); a `["sleep","600"]` command
    with a 1 s timeout is killed with its group; `none` is skipped;
  - **archive:** lossless leads to pin plus removal, dirty to retention,
    with the event reasons asserted;
  - **idle:** a 1 s idle TTL reclaims once, then not again until a turn;
  - **disk pressure:** through an injected `FsStats` source (the production
    scan reads its free bytes from it; the test supplies low numbers and
    then real ones). Asserts oldest-first order, stopping at the high
    watermark, never touching a busy agent, and the exhausted event;
  - **orphans:** after a journal reset they are reported and soft-reclaimed
    only; `gc --hard` removes the lossless ones;
  - **gc:** `--dry-run` journals `WOULD_DO` and leaves the filesystem
    byte-for-byte unchanged apart from remote-tracking refs; `--hard`
    refuses non-lossless; `--force` removes; `--include-pins` drops only
    unpinnable pins;
  - **Instance removal:** refused with a live agent; refused with non-empty
    unmanaged data unless `--force`; `MISSING` report;
  - **reports:** `MaterializationProgress` sequence for a three-repo
    materialize; `ReportWorkspaceInstances` called after materialize and
    cleanup (testkit server records calls).
- **Server tests:**
  - the command fold turns `WorkspaceGcRequested` into commands for exactly
    the listed Workers, with stable ids across a refold;
  - the ingest judge admits the new kinds only from the named Worker and
    the gc answer only for a targeting request;
  - the branch-claim fold (claim, release on archive and on lost, refusal
    message);
  - policy resolution layering;
  - validation tables for branch names, reclaim argv, unmanaged paths and
    nested layouts under `LAZY`;
  - placement refuses `workspace_protocol < 2` for spawns that use the new
    fields.
- **MCP:** the tool list includes `workspace_checkout`; argument validation;
  `PARENT` bases filled and staged (endpoint with a real Relay from
  `horde-testkit`).
- **CLI:** flag parsing (`--reclaim` JSON argv, `--base` forms, `--force`
  without `--hard` refused); gc rendering and exit codes against a testkit
  server; the front-door offer appears on a pty and not on a pipe; a
  declined repo is remembered.
- **Multi-node (W12b harness):** a cross-worker child starts from its
  parent's unpushed commit via the Relay; a branch collision across two
  Workers is refused; `delete --purge` removes the Instances on both
  Workers.
- Mutation testing runs scoped to the diff at the item's final handoff
  (the wave's ground rules).

## Considered options

- **Advance the Instance HEAD (fast-forward or reset) and keep branching
  from it.** Rejected: it mutates the tree `shared` agents work in, races
  concurrent spawns, and the operator ruled it out.
- **Fetch on every spawn.** Rejected: a burst of spawns hammers the remote
  for no new information. The throttle plus single-flight gives the same
  freshness to within a minute.
- **Resolve bases on the Server through the Relay mirror.** Rejected for now:
  in the bootstrap phase (DESIGN §8) Workers fetch origin directly and the
  Relay may be absent. Worker-side resolution works in both phases, and the
  `PARENT` path already uses the Relay when it exists.
- **Lazy checkout through sparse-checkout or partial clone.** Rejected:
  sparseness is within one repo. The need is to skip whole repos of a
  multi-repo set, which worktrees already express.
- **Delete by age (TTL hard removal).** Rejected: time says nothing about
  loss. Age only orders soft reclaim under pressure.
- **Keep the checkout ledger in the journal.** Rejected: a worker reset
  replaces the journal, and the orphans it leaves are the ones that most
  need a record.
- **Reclaim commands as shell strings, or read from the repo's `.horde/`.**
  Rejected: shell parsing of operator text is an injection surface, and the
  repo is agent-writable content.
- **Remove `unmanaged_paths`.** Rejected: GPU Workers holding datasets and
  weights are exactly the case DESIGN §7 named. Implementing it costs a
  directory, a link and a validation rule.
- **Remove `ReportWorkspaceInstances`, since ingest already caches journaled
  reports.** Rejected: events record what happened, and only a rescan
  notices what happened out of band (a deleted directory, a filling disk).
- **Auto-register a Definition from cwd** (DESIGN §7's old plan). Rejected
  by the operator: workspaces are deliberate.

## Consequences

- The ADR-0010 and ADR-0012 consequence "a pumpless archive or cancel gives
  back the checkout (pinned)" now means: soft reclaim, then hard removal if
  provably lossless, otherwise retained. Archive no longer discards
  uncommitted work. Disk use per archived agent drops (no `target/`), but
  dirty archived checkouts accumulate until `gc --hard --force`, and
  `horde workspace get` shows them.
- `parent_start_points` and the same-Worker-only parent base go away. Cross-Worker
  children start from the parent's commit, closing the §5 note in
  CONFORMANCE.
- The CONFORMANCE §7 rows on `unmanaged_paths` (dead field), worker-side
  reconciliation, cwd auto-registration and `MaterializationProgress` get
  resolved rows when this is built. The repo-addition permission event stays
  open: this ADR does not touch it.
- Proto changes are additive. Old Clients ignore the new fields. Old Workers
  are fenced by `workspace_protocol`.
- A new reserved stream (`horde-workspace-housekeeping`) joins the catalog,
  enrollment, MCP-catalog and dead-letter streams. Its growth is bounded by
  cleanup actions, roughly one event per checkout per tier.
- DESIGN §7 points here.

## Open questions

1. **`archived_grace` default.** Zero matches today's immediate release. A
   few hours would let a human inspect a finished agent's tree. The ADR
   ships `0s`.
2. **Ignored files and losslessness.** Rule 1 ignores ignored files, so an
   agent-written `.env` or a local DB under an ignored path is lost on a
   lossless hard removal. Alternatives are to treat ignored files outside
   the reclaim command's known outputs as dirty, or to make this a
   per-repo `lossless_ignores` list.
3. **Does the Relay count as "pushed"?** This ADR says yes for pin expiry:
   share refs are never pruned. If the operator intends "pushed" to mean
   the external origin only, drop the Relay keeper.
4. **Pin TTL.** Pins never age out by default. Squash-merge workflows will
   accumulate them; whether `archived_pin_ttl` should default to something
   like 30 d is left open.
5. **Adopting an existing origin branch as the base for `--branch`.** This
   is the natural "continue this branch" reading, but it is a decision the
   brief did not state.
6. **Lazy materialization of the Instance itself** (clone a repo on first
   checkout rather than at materialize). Deferred: it moves minutes-long
   clones into an agent's tool call.
7. **Disk facts in placement.** Workers now report free space; whether
   placement should avoid a Worker in a pressure episode is not designed
   here.
8. **Who may gc.** Any enrolled Client may (ADR-0009 roles). There is no
   per-workspace ACL.
