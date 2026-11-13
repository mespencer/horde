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

## Using the TUI

`horde` (or `horde attach <session> [--agent <id>]`, or `horde new [title]`) opens the attached agent's transcript: its messages, streamed replies, tool calls with their results (collapsed to a few lines; ctrl-o expands them), errors and notices. A compose box sits under it, and a status line names the session, agent, harness, model, worker, state and connection. The connection reads `stale` when the stream has said nothing, heartbeats included, for 45 s. Permission prompts and questions open as a card above the compose box and are answered in place. A request with several questions steps through them, and a secret answer is masked. Every action is also a `horde <verb>`, so a script can do whatever the TUI does.

| Key | In the session view |
|---|---|
| enter | Send the message. It queues for the agent's next turn boundary while a turn is running. |
| alt-enter, shift-enter, ctrl-j | New line |
| ctrl-s | Send now: interrupt the turn and deliver at once |
| esc | Leave a prompt card for the compose box, close the command menu, or interrupt a working agent; otherwise clear the box |
| ctrl-x | Interrupt the agent's turn |
| ctrl-o | Show or hide full tool input and output |
| pgup, pgdn, ctrl-end | Scroll the transcript back, forward, to the end |
| tab | Complete a command; move focus between a prompt card and the compose box |
| ? (empty box), f1 | Help, listing every binding |
| ctrl-t | The session and agent tree |
| ctrl-c | Clear the box, or quit when it is empty |
| ctrl-d | Quit |

On a prompt card, ↑↓ and enter choose, y allows a permission, n denies a permission or declines a question, a digit picks an option, space toggles a multi-select option, and ←→ move between questions. Alt-1 to alt-7 switch views anywhere: session, agents, inbox, workspaces, usage, logs and forwards. On a list view, j and k (or ↑↓) move, enter attaches to the selected session or agent (or opens the inbox card), r refreshes, n creates a session, x archives the selected session after a y, esc goes back and q quits. The inbox answers the selected prompt with y, n or a digit, and g goes to the agent that asked.

Type `/` in the compose box for commands: `/attach [session|agent]`, `/new [title]`, `/agents`, `/inbox`, `/workspaces`, `/usage`, `/logs`, `/forwards`, `/interrupt`, `/now <message>`, `/note <message>`, `/details`, `/archive [session]`, `/help` and `/quit`. The agent tree marks LOST agents, the usage view only displays usage, and the logs view is a placeholder until `horde logs` lands.

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
