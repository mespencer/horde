# claude-probe

Throwaway spike crate for [W1c](../../docs/spikes/claude-stream-json.md): a Rust
driver for `claude --input-format stream-json --output-format stream-json`, proving
a worker can supervise Claude Code as a plain child process with no Node sidecar and
no Agent SDK.

```
cargo run
```

The demo binary spawns the CLI, runs two turns on one process, interrupts a third
mid-flight and keeps using the same process, then resumes the session in a fresh
process — printing the extracted result and usage at each step.

`src/lib.rs` is the seed of the real Claude adapter. What is worth carrying forward:

- Events are kept as raw JSON with typed accessors layered on top. A worker that
  drops fields it does not recognize cannot journal a session faithfully.
- `ClaudeProbe::interrupt` sends a `control_request`. Closing stdin does **not**
  interrupt a turn and SIGINT kills the process; only the control message aborts a
  turn and leaves the process steerable.
- `ResultMessage::succeeded()` checks `is_error`, not just `subtype` — an
  unauthenticated run reports `subtype: "success"` with `is_error: true`.
- The doc comments on `ResultMessage::total_cost_usd` and `ModelUsage` record the
  accounting traps (cumulative-per-process values, camelCase vs snake_case).

## Scope

Deliberately outside the Horde workspace: `Cargo.toml` carries an empty
`[workspace]` table so it never joins the real build, and it is exempt from the
gauntlet gates. It still honors the project's `unwrap()`/`expect()` ban and passes
`cargo clippy --all-targets -- -D warnings`. Delete the whole `spikes/` tree once
the production adapter lands.
