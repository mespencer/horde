# W1c probe harness

Scratch drivers used to capture the [golden transcripts](../../docs/spikes/transcripts/README.md).
Python because these were written to answer questions fast, not to ship; the Rust
equivalent of the useful half is [`../claude-probe`](../claude-probe/README.md).

| File | Purpose |
|---|---|
| `drive.py` | Spawns `claude`, sends N user messages on one stdin, records both directions to JSONL |
| `interrupt.py` | Starts a long turn, fires an interrupt (`control` / `stdin-close` / `sigint`), then tests whether the process is still usable |
| `permserver.py` | Stand-in stdio MCP server exposing one permission-prompt tool |
| `permhttp.py` | Same tool over streamable HTTP, to prove the endpoint need not be a stdio child |
| `mcp-perm.json`, `mcp-perm-http.json` | `--mcp-config` files pointing at those servers |

## Usage

```sh
# three turns on one process, tools disabled
python3 drive.py out.jsonl 3 "first" "second" "third" --tools ""

# interrupt mid-turn and check the process survives
python3 interrupt.py out.jsonl control
```

`drive.py` passes everything after the prompts through to the CLI. `DRIVE_TIMEOUT`
(seconds, default 180) bounds the wait for each result.

The permission servers are configured by environment:

- `HORDE_PERM_DECISION` — `allow` (default) or `deny`
- `HORDE_PERM_MUTATE` — e.g. `content=REWRITTEN`, to rewrite `updatedInput` and prove
  the permission chain can narrow a call rather than only gate it
- `HORDE_PERM_DELAY` — seconds to stall before deciding, standing in for human approval
- `HORDE_PERM_LOG` — where to append the request/decision log

```sh
HORDE_PERM_DECISION=deny HORDE_PERM_LOG=$PWD/perm.jsonl \
python3 drive.py out.jsonl 1 "write a file" \
  --tools Write --mcp-config ./mcp-perm.json \
  --permission-prompt-tool mcp__horde__permission_prompt
```

`mcp-perm.json` refers to `./permserver.py` relative to this directory, so run from
here or edit the path.

Every capture also passed `--setting-sources '' --strict-mcp-config`. Without those
the operator's own hooks, plugins, and MCP servers load and pollute the stream —
the first probe run of this spike injected an unrelated project's memory into the
transcript before those flags were added.
