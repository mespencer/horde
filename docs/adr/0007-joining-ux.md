# Joining is one token, the identity dir remembers the server, and `horde up` stands up a machine

The first real first-run (2026-08-31) hit four traps in a row: the CLI's
default endpoint is `http://…` against a server that has no plaintext mode, so
the default can never connect; the worker's `--join`/`--enrollment` flags had
crossed meanings (the code went in `--join`, the listener in `--enrollment`)
and two careful readers fed the URL to the code parser; enrollment ceremony
demanded a 97-character code, a separately-known listener address, two
exported environment variables, and a ten-minute TTL race; and the errors
diagnosed the paste rather than the actual mistake. None of these are
protocol problems — DESIGN §3's trust story survives untouched. They are
packaging problems, and this ADR repackages joining three ways.

**One-token joining.** Minting emits a single self-contained join token,
`horde://join/<host:port>/<secret>.<fingerprint>` — the enrollment listener,
the secret, and the pinned server fingerprint in one shell-safe string (path
segments only; no `?`/`&` to quote). Redemption takes exactly that token:
`horde enroll join <token>` for clients, `horde-worker --join <token>` for
workers, and the worker's separate `--enrollment` listener flag is deleted
rather than deprecated — with one flag there is nothing left to swap. The
server's first-boot bootstrap line prints the same token form. The
`secret.fingerprint` interior is unchanged, so nothing about code entropy,
pinning, or the TLS-only listener moves; a token is packaging around the
existing `JoinCode`.

**The identity dir remembers the server.** Redeeming a token already teaches
the machine everything it needs forever: the control-plane endpoint, the CA,
its own credential. `enroll join` now persists the server endpoint beside the
credential, and every later invocation reads it from there — `--endpoint` and
`HORDE_ENDPOINT` become overrides, not requirements, and the unusable
plaintext default dies with them. The client identity dir itself defaults to
`~/.horde/id` (workers already have `--state-dir`), so after one join a bare
`horde hello` in a fresh shell just works. Precedence: explicit flag, then
environment, then the identity dir's stored endpoint.

**`horde up` stands up this machine.** One command covers the single-machine
day-one case: start the server if none is running (state under
`~/.horde/server`), enroll the local client if none exists, mint-and-join a
local worker (default name the hostname, adapters from `--adapter`, repeated),
and print one ready line naming the worker and its harnesses. Ctrl-C tears
down what it started. Same-machine trust rides the filesystem instead of a
visible code: the server writes its current bootstrap token to a
user-read-only file in its state dir, and `horde up` — same user, same
machine — reads it there. No code ever appears on a screen for the local
case; the token ceremony surfaces only when a second machine joins, which is
when it earns its keep. `horde up` composes the existing pieces (`--serve`,
join, worker startup) rather than growing a parallel path, and the full mTLS
machinery runs underneath exactly as before.

## Consequences

- The worker's `--enrollment` flag is gone and `--join` changes meaning from
  bare code to token; the repo is unreleased, so both break cleanly instead
  of lingering. The malformed-token error names the expected `horde://join/…`
  shape.
- The identity dir gains one small stored field (the endpoint) and a
  compatibility rule: a dir without it still works, asked-for endpoint
  precedence unchanged.
- The bootstrap-token file is the one new trust surface: user-only
  permissions, rewritten while no client exists, gone once one does — the
  printed stdout line and the file carry the same token, so nothing is
  mintable from the file that was not already on the operator's screen.
- `horde up` owns process lifetimes it started and nothing else: a server it
  found running is left running on exit.
- Sequenced as wave 10, one agent: token type + mint/print/redeem surfaces,
  endpoint persistence + resolution order, `horde up`, and the e2e that walks
  token-join end to end.

## Amendment (2026-10-31): defaults that cannot lie, and an `up` that knows what is running

A whole-project review found wave 10 stopped short in four places. The CLI
still kept `http://127.0.0.1:7777` as a last resort for a machine with no
identity, a default that could never reach a secured server. A dial failure
printed `transport error` and nothing more. `horde up` decided "already
running" by whether a TCP port answered and "ready" by a stdout line. And
`horde-server` and `horde-worker` defaulted their state to the working
directory, which is how stray `horde.db` and `horde-worker-state/` landed in
the repo root.

**No guessed server.** With no `--endpoint`, no `HORDE_ENDPOINT`, and no
stored endpoint, the CLI refuses and names the two ways to get a server:
`horde enroll join <token>` or `horde up`. The worker follows the same
order: `--server`, then `HORDE_SERVER`, then the endpoint its own join
stored, else a refusal. Its `https://127.0.0.1:7777` default is gone as
well. A worker `--join` now stores the endpoint the token taught, so after
it restarts it needs no flags. An endpoint is validated (`https://host:port`,
a real port, not the unspecified address) before anything persists it.

**Errors name their cause.** Every CLI refusal walks the error's source
chain and opens the `io::Error` wrappers that hide rustls's own error. It
then adds a `hint:` in plain words: the server's certificate is from another
CA, it does not cover the dialed name, it has expired, the server refused
this machine's certificate, or nothing is listening.
`horde_identity::diagnosis` owns that vocabulary.

**State dirs are owned, locked, and private.** Each daemon defaults its
state under `~/.horde` (`server/`, `worker/`), never the working directory.
Each holds an exclusive `flock` on its state dir for its whole life
(`server.lock`, `worker.lock`), so a second copy on the same state refuses
and names the holder's pid. Identity, state, and CA directories are created
`0700` and tightened if they already existed. The secret writer re-applies
`0600` on every rewrite, not only on create. `horde-server` requires a mode
by name, either `--serve` or `--check` (the health check that a bare
invocation used to perform). Both daemons parse with clap, so `--help`
exists and a value can never begin with `-`.

**Joining is guarded.** `enroll join` refuses to replace an existing
identity unless `--force` is given, and checks this before it spends the
code. When forced, it first moves the old identity aside as
`<dir>.backup-<unix-time>`. Tokens may come from argv, `--token-file`, or
`HORDE_JOIN_TOKEN` (the worker: `--join`, `--join-file`, or the same
variable). The environment token is *ambient*: it is redeemed only when
nothing is enrolled yet and ignored over an existing identity, so a variable
left set in a supervisor's unit does not fail every restart.

**Tokens name a host the certificate covers.** The server's bootstrap token
used to name the bound address, which is `0.0.0.0` on a wildcard bind. It
now names a host the listener certificate answers to. That is the bound
address when it is loopback or itself a `--san`, otherwise the first
`--san`, otherwise loopback (this machine only, which is honest). A CLI mint
refuses a token that would name the unspecified address. A loopback token
prints with a note that only this machine can redeem it.

**`horde up` acts on facts.**

- It holds `up.lock` on its root, so a second `up` refuses.
- It finds a running server or worker by its state lock and never
  double-starts one.
- It reads the bootstrap token only from a server it started itself. A
  found server with no local identity is a refusal pointing at
  `enroll join`.
- It checks the token's fingerprint against the one the server just
  announced, so a stale file from an earlier run is refused. A redeem
  failure says how to re-mint: stop and re-run `horde up`.
- It checks the identity's CA against the CA in its own server state.
- It proves readiness with a real mTLS `Hello`. A certificate failure stops
  it at once, with the diagnosis. Refused or timed-out connections are
  retried within a ceiling.
- It installs its signal handlers before bring-up, so a ctrl-c mid-start
  still tears down what was started.
- It watches the children it started. A child that dies during a wait fails
  that wait at once, with the child's output. A child that dies after ready
  ends `up` and tears the rest down.
- Once up, it forwards the daemons' stdout with a per-line cap and a capped
  buffer. Forwarding stays off during bring-up, so the bootstrap token never
  reaches a screen.
- It accepts any `registered worker=` line and reports the name the worker
  actually registered under.
- It hands the worker its minted token through the environment, not argv.
- It honors `--identity` and `HORDE_IDENTITY`, and prints
  `export HORDE_IDENTITY=…` when the identity sits where a bare `horde`
  would not look.
- `--san` (repeatable) is passed to the server it starts and advertised on
  the ready line. It is refused when either bind is loopback or the name is
  not dialable.

### Consequences of the amendment

- A bare `horde-server` is now a refusal, and the health check is spelled
  `horde-server --check [db]`.
- No proto change.
- The lock files are advisory and the OS releases them when the holder
  dies, so a crash never leaves a stale lock. The e2e proves `up`'s teardown
  by the locks being free, not by probing a port.
- Not done here: a re-mint verb for a running server's bootstrap token
  (restarting re-mints), and validating `--san` names against DNS at mint
  time. The certificate check at dial time, with its diagnosis, is the
  backstop.
