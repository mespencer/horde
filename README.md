# Horde

Horde is a multi-provider agent orchestrator. All agents flow through one control plane: the ones you drive directly and the ones agents spawn themselves. It tracks session trees and spawn ancestry, places agents across machines, runs provider harnesses (Claude Code, Codex) under subscription accounts rather than API keys, and lets any client attach to any session. Every session is an append-only event log on the server, so reconnect, attach, replay and usage attribution are one mechanism. It is single-user per server.

## Binaries

| Binary | Role |
|---|---|
| `horde-server` | Control plane: event log, scheduler, permission chain, git relay, blob store, MCP hub, certificate authority. Needs `--serve`, `--check` (open the log and exit) or `--remint` (re-mint a running server's bootstrap token). |
| `horde-worker` | Agent supervisor: runs harnesses as child processes, materializes workspaces, journals events, and dials the server outbound. |
| `horde` | Client: CLI and TUI. A bare `horde` creates or attaches to a session. |

All state lives under `~/.horde` by default: `server/`, `worker/`, and the client identity in `id/`.

## Quickstart

Build the binaries and put them on `PATH`. `horde up` starts the other two:

```sh
cargo build --release -p horde-cli -p horde-server -p horde-worker
export PATH="$PWD/target/release:$PATH"

horde up --adapter claude
```

`horde up` starts a server if none is running, enrolls this machine's client if it has no identity, proves the pair with a `Hello`, and joins a local worker. It prints one `ready` line and holds until ctrl-c, which stops only what this invocation started. Then, in another terminal, run `horde` to open a session.

### Joining a second machine

Make the server reachable and name the host other machines dial:

```sh
horde up --adapter claude --bind 0.0.0.0:7777 --enrollment-bind 0.0.0.0:7778 --san box.lan
```

Mint a join token through that name. It prints `token horde://join/<host:port>/<secret>.<fingerprint>`:

```sh
horde worker enroll gpu-box --endpoint https://box.lan:7777
```

On the second machine, redeem it. The token file (or `HORDE_JOIN_TOKEN`) keeps the secret out of the process list:

```sh
horde-worker --join-file token.txt --adapter claude
```

The join stores the server's endpoint, so later starts need only `horde-worker --adapter claude`. To add another client machine instead, mint with `horde enroll client <name>` and redeem with `horde enroll join --token-file token.txt`.

## Diagnosing a setup

```sh
horde doctor
```

`horde doctor` checks identity, endpoint, certificates, server reachability, harness binaries (`--harness`, default `claude` and `codex`), and state directories and disk. It says what to fix, and exits non-zero when any check fails. Add `--json` for machine-readable output.

## Building and the quality gauntlet

```sh
cargo build --workspace
cargo build -p horde-gauntlet --release
target/release/gauntlet run
```

`gauntlet run` runs every gate, all of them blocking: fmt, clippy, test, coverage, crap, duplication, machete, module-structure, proto-contract and mutants. Use `gauntlet run <gate>` for one gate and `gauntlet list` for the registry. See [docs/QUALITY_GATES.md](docs/QUALITY_GATES.md).

## Further reading

- [DESIGN.md](DESIGN.md): the end-state design.
- [CONTEXT.md](CONTEXT.md): the glossary.
- [docs/adr/](docs/adr/): architecture decision records.
- [IMPLEMENTATION.md](IMPLEMENTATION.md): the build plan and what each wave delivered.
- [docs/CONFORMANCE.md](docs/CONFORMANCE.md): how the code measures against the design.
