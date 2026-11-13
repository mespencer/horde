# Logs are local, bounded files read over the spine, never session events; OTLP export is removed

Debugging Horde today means being on the right machine at the right moment.
`horde-server` and `horde-worker` write human-readable lines to stderr and
nowhere else, so a worker started by `horde up`, a systemd unit, or (from
W12i on) an ssh bootstrap keeps its explanation only as long as some
terminal stays open. A harness's stderr is worse off: the worker keeps the
last 256 lines in memory (`process::STDERR_LINES`) and logs them once, when
the harness dies. If the agent is still alive and misbehaving, nobody can
read them. The one structured outlet is the OTLP exporter in
`horde-observe`. It needs a collector nobody runs, exports only RPC-handler
spans, and ships them over plaintext gRPC, which DESIGN §12 already lists as
an open inconsistency with §3's mTLS-everywhere posture. And when an agent
suspects that the *system* is the problem (a worker that stopped
materializing, a harness that rate-limits silently), it cannot look at all.

The operator's intent for W12h is development and
self-debugging, not an observability stack: no OTLP database, bounded
rotating local logs per binary, `horde logs` over the existing mTLS
connections, an MCP tool so agents can read worker and server logs, and logs
never in the session event log. This ADR designs that.

**Logs are not session facts, and the rule says so.** ADR-0001 says
everything that matters is an event and side channels are bugs. Its wave-7
amendment scoped that to *session facts*, meaning anything a client, an
auditor, or a reconnecting component could need to replay. Diagnostic logs
pass beside the session log under two tests, parallel to the amendment's
tests for mutable tables:

1. **Nothing decides from a log.** No fold, placement, permission chain,
   reconnect, or revival path reads a log file or the log tap. Logs are read
   only by people and by agents. Agents act on what they read through the
   verbs, and those actions are events like any other. Code that branches on
   log content is a bug, the same as a table that holds a fact the log lacks.
2. **A log line may repeat a session fact, never originate one.** A harness
   death is still a lifecycle event with its `HARNESS_DIAGNOSTIC` output
   chunk (ADR-0012), a refused journal entry is still a dead letter
   (ADR-0009), and a worker loss is still `LOST`/`FAILED`. The log line
   beside each adds context such as stack traces, raw stderr, and timings.
   If a reviewer finds a log line that is the only record of something a
   client would need, that line is a missing event, and the fix is to add
   the event.

Losing every log file therefore loses no state and breaks no replay, which
is why logs can be bounded, lossy under pressure, rotated away, and kept off
the journal. The same reasoning keeps log *fetches* off the command stream
(below): a log request is ephemeral intent, like a login or a forward
(DESIGN §4, ADR-0006), and not a fact.

**Each daemon writes bounded, rotating JSON lines under its state
directory.** `horde-observe` gains a file layer next to the console layer it
already composes:

| Binary | Directory (default) | Per file | Files kept | Max age |
|---|---|---|---|---|
| `horde-server` | `~/.horde/server/logs/` | 16 MiB | 8 | 14 d |
| `horde-worker` | `~/.horde/worker/logs/` | 16 MiB | 8 | 14 d |
| harness stderr (per agent) | `~/.horde/worker/logs/agents/` | 4 MiB | 2 | until pruned (below) |
| `horde` (opt-in) | `~/.horde/client/logs/` | 4 MiB | 2 | 7 d |

The directory follows `--state-dir`: it is always `<state>/logs`, so two
daemons with separate state directories never share files, and the
exclusive state lock already held covers the logs too. Directories are
`0700` and files `0600`, the same owner-only rule as the rest of the state
directory. Files are named `<binary>.<boot>.<n>.jsonl`. `boot` is a random
id minted at process start, and `n` counts up. A new file is opened when the
current one reaches its size. Files are never renamed, which keeps rotation
safe for a concurrent reader and for Windows (W12g), where renaming an open
file fails. When a new file opens, pruning deletes the oldest files past the
count or the age limit. Retention is a count of bounded files, so disk use
has a hard ceiling: 128 MiB per daemon by default.

Each line is one JSON object written by the tracing JSON formatter:
`ts` (RFC 3339, UTC), `level`, `target`, `msg`, `fields`, `spans` (the span
stack with its fields), plus `boot` and a per-process `seq`. A record longer
than 16 KiB is cut on a char boundary and marked `"truncated": true`. Two
span fields are conventions the reader relies on: `session` and `agent`.
The worker's per-agent pump span and the server's per-RPC spans set them
wherever the request names them.

The file filter is independent of the console's. `RUST_LOG` keeps
controlling stderr. `HORDE_LOG` (until W12e's layering gives it a config key,
`logs.level`) controls the file, with default `info,horde_server=debug,
horde_worker=debug`. Writes go through a non-blocking, bounded channel to a
writer thread, so a slow disk never stalls a request. A full channel drops
the record and counts it, and the writer emits one `log.dropped count=N`
record when it catches up. An I/O failure (disk full, directory removed)
disables the file layer, warns once on stderr, and retries opening a new
file at most once a minute. **Logging is never fatal.** The only startup
refusal is an unparseable `HORDE_LOG` or size setting. This is the same rule
`horde-observe` already applies to typos: a refusal, not a silent no-op.

The file layer also publishes each record to an in-process broadcast tap,
bounded at 4096 records. Follow mode reads it (below). A receiver that lags
gets a gap notice with the count it missed, rather than slowing the writer.

The CLI writes no file unless asked (`HORDE_LOG_FILE=1` or `--log-file`).
Its stdout is the TUI, its stderr is the refusal channel, and most
invocations last a second.

**A harness's stderr goes to a per-agent file as well as the death tail.**
The supervisor's stderr drain (`process.rs`) keeps the in-memory tail for
the death diagnostic and also writes each line to
`agents/<agent-id>.<n>.jsonl` as `{ts, agent, session, stream: "stderr",
line}`. Each agent gets two 4 MiB files, rotated like the daemon logs.
Adapter diagnostics, such as an unparseable stream-json line, an unknown
codex notification, or a version-range warning (W12c), are ordinary tracing
events inside the agent's pump span. They land in the worker log tagged with
the agent. So "everything about agent X" means the worker log filtered by
`agent=X`, merged by timestamp with that agent's stderr file. The `agents/`
directory has its own budget of 256 MiB. Past it, the files of archived
agents are pruned oldest first, then the oldest files of live agents. A live
agent always keeps at least its current file.

**`horde logs` reads any process's logs through the server.**

```
horde logs server               [-f|--follow] [--since D|T] [--level L] [--grep RE] [-n N] [--json]
horde logs worker <NAME>        (same flags)
horde logs agent  <AGENT>       (same flags) [--source all|horde|stderr]
horde logs --dir <PATH>         (same flags; local files, no RPC)
```

`--since` takes a duration (`10m`) or a timestamp. `--level` is the minimum
level. `--grep` is a regular expression (the `regex` crate: linear time,
with a size limit on the compiled pattern) matched against `msg` and the
rendered fields. `-n` is how many of the newest matching records to show
(default 200). `--follow` keeps the stream open after them. The default
output is one human line per record. `--json` passes the redacted records
through as JSON lines. `agent` resolves the agent's current placement
through the projection and reads from that worker. `--dir` is the escape
hatch for a machine that cannot reach the server: same reader, same filters,
run over a local log directory. It also reads a CLI's own `--log-file` logs.

The new RPC is on the control plane:

```proto
rpc ReadLogs(ReadLogsRequest) returns (stream ReadLogsResponse);

message ReadLogsRequest {
  oneof target { ServerTarget server = 1; string worker = 2; AgentId agent = 3; }
  LogFilter filter = 4;          // since, level, grep, agent, source, tail_records
  bool follow = 5;
  optional LogCursor after = 6;  // (boot, seq): resume a dropped follow
  AgentId on_behalf_of = 7;      // required of a worker caller (agent conduit)
  uint32 max_bytes = 8;          // caller's cap; the server clamps it
}
message ReadLogsResponse {
  oneof body { LogRecords records = 1; LogGap gap = 2; LogEnd end = 3; }
}
```

`LogRecords` carries the redacted JSON lines as strings plus a cursor. Logs
are opaque transit, like hub tool payloads, and a typed record would need a
proto change every time a field convention changes. `LogGap` reports
dropped, lagged, or rotated-away records with a count. `LogEnd` gives the
reason the stream ended: done, the byte cap was reached, the worker
disconnected, or the worker predates log serving.

**Worker logs travel on a dedicated stream pair, not the command stream.**
We weighed two routes. A worker command (`ReadLogsCommand` on
`StreamCommands`) would reuse the stream that already exists, but
everything on that stream is a fold over the session log. ADR-0011 resumes
it by log position and ADR-0012 journals each command into the inbox before
acking it. A log request there would have to become an event, which breaks
the brief's "logs never enter the session log", and would be replayed
at-least-once after a reconnect. Replaying a stale follow from yesterday is
exactly the wrong behavior. The login relay already solved the same
problem, so log serving copies its shape:

```proto
// WorkerService
rpc StreamLogRequests(StreamLogRequestsRequest) returns (stream StreamLogRequestsResponse);
rpc WorkerLogTunnel(stream WorkerLogTunnelRequest) returns (stream WorkerLogTunnelResponse);
```

The worker subscribes to `StreamLogRequests` after registering. The server
pushes one `LogQuery` (request id, target, filter, follow, cursor, byte cap)
for each `ReadLogs` aimed at that worker. The worker opens one
`WorkerLogTunnel` for each query. Its first frame echoes the request id,
then it sends record batches, gap notices, and an end frame. If either side
drops the tunnel, the query is cancelled. When a follow's client goes away,
the server drops the tunnel. When the worker disconnects, the server ends
the client's stream with `LogEnd{WORKER_DISCONNECTED}`. Nothing about a
query outlives its streams, and nothing is journaled. The server buffers
nothing and caches no worker's logs. A worker that is not connected gets
`UNAVAILABLE` with its last-seen time, and its files can still be read on
the machine with `--dir`. The worker advertises a `logs.v1` capability flag
in its Hello. Without the flag, the server answers `FAILED_PRECONDITION`
rather than waiting on a stream the worker never opens.

**Who may read what (ADR-0009 rows).** The trust table gains three rows:
`WorkerService/StreamLogRequests` and `WorkerService/WorkerLogTunnel` for
workers, and `ControlPlaneService/ReadLogs` for anyone. The table's
proto-walk test refuses a build that forgets any of them. A client caller
may read every target: Horde is single-user (DESIGN §1), and a client is
the operator. A worker caller is the agent conduit, scoped like
`SpawnAgent`. It must set `on_behalf_of` to an agent the log places on the
calling worker, or the call is refused with `PERMISSION_DENIED`. An agent
may read three things: the server log, any worker's log, and the logs of
agents in its own session. For an agent caller, records whose `session`
field names a different session are filtered out before they leave the
machine, the same scoping ADR-0011 gave a worker's `SyncEvents`. Records
with no session field are process-level and visible. In the other
direction, the worker answers queries only from the server, on a stream it
dialed itself. A worker already runs every command that server sends it, so
the server reading its logs adds no new trust.

**Agents read logs through one MCP verb, with limits.** The agent endpoint
gains `logs`, which harnesses see as `horde/logs` or `mcp__horde__logs`.
Parameters: `target` (`self` is the default, or `server`, `worker:<name>`,
`agent:<id>`), `since`, `level`, `grep`, `max_bytes`, and `cursor`. It
returns `{records, truncated, next_cursor, redacted}`. MCP is
request/response, so there is no follow: an agent pages forward with the
cursor. Limits:

- **Size:** 16 KiB per call by default, 64 KiB at most. The server clamps
  `max_bytes` for any worker caller whatever the endpoint asked for.
- **Rate:** a token bucket per agent (6 calls a minute, burst 3). It is
  enforced at the endpoint, and again at the server for remote targets. An
  exhausted bucket returns a tool error with a retry-after, which the model
  can read, not a protocol error.
- **Concurrency:** one in-flight `logs` call per agent.

`self` and the agent's own worker are served by the endpoint from local
files, not through the server. "Something isn't going right" often means
the server is the thing that's wrong, and an agent must still be able to
read its own harness's stderr during a partition. Remote targets go through
`ReadLogs` on the worker's certificate. Because `logs` is read-only,
`PermissionModes::shipped()` allows it the way it allows the orchestrator
verbs. Deny rules still run first. The call and its result already appear
in the agent's own output as a tool use, so the session log records that the
agent looked. Recording the content as well would put logs into the log.

**Secrets are redacted before a record leaves a machine, and logged less to
begin with.** Harness stderr is uncontrolled text, so redaction is
enforced at egress, in one place: `horde_observe::redact`. The worker's
tunnel serializer, the server's `ReadLogs` serializer, and the endpoint's
local `logs` path all run it. The CLI's `--dir` reads files the operator
already owns and shows them unredacted. The redactor is a pure function
with three parts:

- **Known shapes:** join tokens (`horde://join/…`); PEM private-key blocks;
  `Authorization`/`Bearer` values; URL query values for `code`,
  `access_token`, `refresh_token`, `token`, and `secret`; and the common
  provider and forge key prefixes (`sk-ant-`, `sk-`, `ghp_`, `gho_`,
  `github_pat_`, `xoxb-`/`xoxp-`, `AKIA`).
- **Secret-named fields:** any JSON field whose key contains `token`,
  `secret`, `password`, `passwd`, `credential`, `api_key`, `apikey`,
  `cookie`, or `authorization`, at any depth, has its value replaced.
- **Registered values:** a process registers the exact secrets it holds,
  such as the server's minted join secrets and bootstrap token, and the
  worker's join secret while it holds one. Values of 8 characters or more
  are scrubbed wherever they appear. The set is bounded, and an entry is
  forgotten when its secret is redeemed or expires.

A match becomes `[REDACTED:<kind>]`, and the response reports how many
records it touched. There is no entropy heuristic. It would remove commit
hashes and ids, which are what a debugging reader needs. At the source,
types that hold secrets get hand-written `Debug` impls that elide them, the
way `ServerCredential` and `StoredIdentity` already do. `JoinToken` and
`JoinCode` currently derive `Debug` and must change. The login relay's PTY
bytes and forward tunnel bytes are never logged at any level, and a test
pins that rule.

**The OTLP exporter is removed, not deprecated.** We weighed keeping it as
an optional feature. It would cost a feature-gated pipeline that nothing in
the gauntlet exercises and four exactly pinned OpenTelemetry crates whose
tonic pairing has to move in lockstep with the workspace's own. It would
also keep the plaintext export that contradicts §3, for a mapping of the
agent tree onto spans that was never built. Nobody runs the collector, and
the operator ruled out the stack. W12h therefore deletes `export.rs`,
`pipeline.rs`, and `config.rs` and the `otlp_smoke` suite, drops the
`opentelemetry*` and `tracing-opentelemetry` dependencies, and removes the
testkit's `env_remove` lines. `horde-observe` keeps the subscriber
composition and `ServiceIdentity`, whose name and principal now stamp each
log file's first record, and gains the file layer, the tap, the reader, and
the redactor. A binary started with `HORDE_OTLP_ENDPOINT` set prints one
stderr warning that export was removed by ADR-0015 and starts anyway. A
refusal would stop a daemon over a stale shell variable, and staying silent
would hide the change. The JSON lines keep span names and fields, so a
flame graph can still be rebuilt offline from files if one is ever wanted.
DESIGN §12's OTLP bullet is replaced by a pointer to this ADR.

## Failure modes

- **Disk full or log directory gone:** the file layer disables itself and
  warns once, the console keeps logging, and the layer retries once a
  minute. Requests never stall on log I/O.
- **Writer backlog:** records are dropped and counted, and a `log.dropped`
  record is written later. Readers see the gap as a count, not as silence.
- **Follow outpaced by the producer:** the tap lags, the reader gets
  `LogGap{lagged: N}`, and the follow continues. The CLI's `--follow`
  redials on a transport loss with `after` = the last cursor, with backoff.
  Records written in between are read from the files, and `seq` removes
  duplicates at the file/tap boundary. If `boot` changed (the process
  restarted), the CLI prints a restart marker and continues from the new
  boot's first record.
- **Rotation during a read:** files are never renamed, and a pruned file
  that is still open stays readable through its handle on Unix. On Windows,
  files are opened with share-delete (W12g). A file pruned before the reader
  reached it becomes `LogGap{rotated}`.
- **Worker offline, or older than the feature:** `UNAVAILABLE` with last
  seen, or `FAILED_PRECONDITION` naming the missing `logs.v1`. The server
  never waits indefinitely on a worker.
- **Feedback loop:** the code that serves logs logs at most one summary line
  per query, under a target the tap excludes. A follow therefore never
  produces records that it then follows. This is the same idea as the
  exporter's `SUPPRESSED_TARGETS`.
- **Abusive or looping agent:** the rate, size, and concurrency limits bound
  what one agent can cost. Per-agent buckets mean one agent cannot starve
  another's reads.
- **Redaction miss:** this is the residual risk. It is limited by owner-only
  files, egress-only exposure, secret-eliding `Debug` impls at the source,
  and exact-value scrubbing of the secrets Horde itself mints. A pattern
  that is found missing becomes a redactor test case first.
- **Clock skew:** `--since` is evaluated on the target machine's clock, and
  the records show that clock. The CLI prints the target's `ts` unchanged
  rather than reconciling clocks.

## Test plan

- **`horde-observe` (unit, real files in a temp dir):** rotation at size;
  pruning by count and age with an injected clock; `0700`/`0600`
  permissions; one JSON object per line with `boot`/`seq`; truncation of
  oversized records on a char boundary; the drop counter and its
  `log.dropped` record; the reader's tail, level, since, grep, and agent
  filters over files the real layer wrote; deduplication by `seq` at the
  file/tap boundary; a lagged tap becoming a gap.
- **Redactor:** a table test per pattern, each with a positive case and a
  near miss that must survive (a commit hash, an agent id, `sketch-` as a
  word); secret-named keys at depth; registered values, including
  forgetting them on expiry; the redaction count. Every egress path is
  proven to call it by planting a join token in a real record and reading
  it back through that path. No path gets its own copy of the redaction
  logic.
- **Trust (server):** the proto walk already forces rows for the three new
  methods. Handler tests: a worker caller without `on_behalf_of` is
  refused; an agent placed elsewhere is refused; an agent caller never
  receives another session's records; the byte clamp and the rate limit
  (`RESOURCE_EXHAUSTED` with retry-after) hold for worker callers but not
  for client callers.
- **Worker serving (testkit server, real worker):** query → tunnel →
  records; cancellation when either end drops; `WORKER_DISCONNECTED` when
  the worker is killed mid-follow; `FAILED_PRECONDITION` for a worker
  without `logs.v1`.
- **Harness stderr:** the fake adapter gains a scripted stderr line. The
  line appears in the agent's file and through `horde logs agent`, and the
  death tail behaves as before.
- **End to end (`horde-e2e`):** `horde logs server`; `horde logs worker
  <name> --grep`; `--follow` sees a record emitted after the stream opened;
  `--json` lines parse; `--dir` reads the same files offline; a join token
  in fake-adapter stderr arrives redacted through the CLI and through
  `horde/logs`.
- **MCP:** `logs` is listed (it joins the endpoint's tool-name test);
  paging with `next_cursor`; `truncated` at the cap; the shipped permission
  mode allows it.
- **W12b multi-node:** one compose scenario reads a worker's logs from the
  client container and follows them through a worker restart.
- **OTLP removal:** the warning for a set `HORDE_OTLP_ENDPOINT` is a pure
  function, tested in place. `cargo tree` no longer contains
  `opentelemetry`.
- Mutation testing (W12a gate) covers the redactor, the limits, and the
  rotation arithmetic. These are the mutation-dense parts.

## Consequences

- Every daemon leaves a bounded, structured record on its own disk by
  default: at most 128 MiB of daemon logs per state directory, plus 256 MiB
  of harness stderr on a worker.
- The CLI gains `horde logs` and the MCP endpoint gains `logs`. Proto
  changes are additive: one control-plane RPC, two worker-spine RPCs, a
  `logs.v1` capability flag, and their messages. `buf breaking` passes. A
  newer server with an older worker degrades to `FAILED_PRECONDITION` for
  that worker only.
- ADR-0001 gains a third category beside session facts and permitted
  operational state: diagnostic logs, admitted by the two tests above.
- The OTLP stack and its pinned dependencies leave the workspace. DESIGN
  §12's open mTLS inconsistency closes by removing the exporter.
- `JoinToken`/`JoinCode` stop deriving `Debug`.
- Not decided here: whether `HORDE_LOG` and the size and retention settings
  move under W12e's config layering (expected: yes, as `logs.*` keys); a
  TUI log pane (W12f may add one over the same RPC); and log shipping off
  the machine (out of scope by the operator's decision).
