# User MCP servers are proxied through a central hub

User MCP servers (Linear, Grafana, …) run at the server and are proxied onto each agent's single horde MCP endpoint, so each server is authenticated exactly once, ever. The alternative — config-layered passthrough with the worker launching MCP servers per agent — was rejected because it requires re-authenticating every MCP server on every worker machine.

## Consequences

- The agent's one-endpoint model (orchestrator verbs + hub tools + worker-local tools) stays intact; the hub rides the endpoint that already exists.
- Machine-bound servers (e.g. browser automation) are marked `local` in config and run worker-side — the pressure valve for anything stateful or misbehaving under multiplexing.
- Hub multiplexing (many agents, one upstream session) is the acknowledged implementation risk.
