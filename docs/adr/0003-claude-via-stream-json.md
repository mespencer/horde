# Drive Claude Code via stream-json, not the Agent SDK

The Agent SDK is TypeScript/Python, which would force a Node sidecar per Claude agent inside a Rust worker. The SDK is largely a wrapper over `claude --input-format stream-json --output-format stream-json`, so the worker speaks that protocol directly to the `claude` binary: turn streaming, `--resume` for revival, usage in result messages, and permission prompting via `--permission-prompt-tool` pointed at the horde MCP endpoint. This keeps workers pure Rust and treats Claude like every provider: a supervised child process with a wire protocol.

This is the load-bearing unverified assumption in the design — it is the first spike. If stream-json proves too lossy, the fallback is a thin Agent SDK shim per agent (contained, not preferred).
