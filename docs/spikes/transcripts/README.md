# W1c golden transcripts — `claude` stream-json

Raw captures from spike W1c ([findings](../claude-stream-json.md)). Recorded against
**Claude Code 2.1.226** on macOS (Darwin 25.3.0) on 2026-08-09, model `haiku`
(`claude-haiku-4-5-20251001`) except where noted.

Nothing has been redacted. A scan for credential-shaped strings found none; the
transcripts do contain absolute paths under the spike worktree and base64
`thinking` signatures, both of which are wire-format artifacts worth keeping.

These files exist to seed the adapter conformance suite: the Claude adapter should
be replayable against them without a live API.

## Format

Most files are JSON Lines where each line wraps one wire message with the direction
and a capture timestamp:

```json
{"dir": "in" | "out", "t": 1786...  , "msg": { ...the actual stream-json envelope... }}
```

`dir: "in"` is a message the driver wrote to the child's **stdin**; `dir: "out"` is a
line the child wrote to **stdout**. The one exception is `01-basic-turn.out.jsonl`,
which is unwrapped raw stdout (no envelope) because it was captured with a shell
pipe rather than the Python driver.

`perm-*.jsonl` are logs from the stand-in permission MCP servers
(`spikes/harness/permserver.py` for stdio, `permhttp.py` for HTTP), recording the
JSON-RPC each received and the allow/deny decision it returned.

## Scenarios

| File | Scenario | Key evidence |
|---|---|---|
| `01-basic-turn.out.jsonl` | One user message, one result | Baseline event sequence; unwrapped raw stdout |
| `02-multi-turn.jsonl` | Three user messages on **one** process | Context retained; one `result` per message; stable `session_id`; cumulative `total_cost_usd` |
| `03-tool-use.jsonl` | Prompt that writes a file | `tool_use` block, matching `tool_result` user message, `tool_use_result` sidecar |
| `04a-perm-deny.jsonl` | `--permission-prompt-tool` returns `deny` | Write suppressed; `is_error: true` tool_result; `permission_denials` populated in result |
| `04b-perm-allow.jsonl` | Same, returns `allow` | Tool runs; file created |
| `04c-perm-mutate.jsonl` | `allow` with rewritten `updatedInput` | Tool executes the **server's** input, not the model's |
| `04d-perm-slow-180s.jsonl` | Permission server stalls 180 s before allowing | No timeout; turn completes; proves the human-approval leg |
| `05-resume.jsonl` | `--resume` of the `02` session in a new process | Context survived process death |
| `06-perm-http-deny.jsonl` | Permission tool served over **HTTP** MCP, returns `deny` | Interception is not stdio-only; Horde's per-agent endpoint can be an HTTP service |
| `07a-interrupt-control.jsonl` | `control_request`/`interrupt` mid-turn | `control_response` ack, `aborted_streaming` result, **process stays usable** |
| `07b-resume-after-interrupt.jsonl` | `--resume` of the interrupted session | Partial work retained in context |
| `07c-interrupt-stdinclose.jsonl` | Closing stdin mid-turn | **Does not interrupt** — turn runs to completion, then exits |
| `07d-interrupt-sigint.jsonl` | SIGINT mid-turn | Aborts the turn but also ends the process |
| `08a-append-system-prompt.jsonl` | `--append-system-prompt` | Overlay honored in stream-json mode |
| `08b-model-sonnet.jsonl` | `--model sonnet` | Model selection honored; note the second billed model in `modelUsage` |
| `09-config-isolation.jsonl` | Fresh `CLAUDE_CONFIG_DIR` | Hard auth failure — credentials do not leak from the ambient config |
| `10-partial-messages.jsonl` | `--include-partial-messages` | Token-level `stream_event` deltas for the TUI |
| `11-probe-run.txt` | Console output of `spikes/claude-probe` | The Rust driver doing all of the above |

## Reproducing

The drivers live in `spikes/harness/`. All runs used
`--setting-sources '' --strict-mcp-config` to suppress the operator's ambient
settings, hooks, and MCP servers, so these captures reflect the protocol rather
than one machine's configuration.
