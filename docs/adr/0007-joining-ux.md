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
