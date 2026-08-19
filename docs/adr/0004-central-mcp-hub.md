# User MCP servers are proxied through a central hub

User MCP servers (Linear, Grafana, …) run at the server and are proxied onto each agent's single horde MCP endpoint, so each server is authenticated exactly once, ever. The alternative — config-layered passthrough with the worker launching MCP servers per agent — was rejected because it requires re-authenticating every MCP server on every worker machine.

## Consequences

- The agent's one-endpoint model (orchestrator verbs + hub tools + worker-local tools) stays intact; the hub rides the endpoint that already exists.
- Machine-bound servers (e.g. browser automation) are marked `local` in config and run worker-side — the pressure valve for anything stateful or misbehaving under multiplexing.
- Hub multiplexing (many agents, one upstream session) is the acknowledged implementation risk.

## Implementation note, 2026-08-14 (W6a): built, with the seams named

The hub is built in `horde-mcp::hub` plus the server-side catalog
(`horde-server::mcp`), and the whole path is exercised end to end with
nothing faked (`crates/horde-mcp/tests/hub_e2e.rs`): legacy MCP client →
agent endpoint → permission chain on a real server → hub link over HTTP →
hub endpoint → real rmcp upstream, with a `local` server spawned as a real
child process beside the agent.

- **Config is journaled through the event log** — `McpServerConfigChanged`
  events in the reserved `horde-mcp-catalog` stream, folded last-writer-wins
  exactly like the Workspace Definition catalog, authored via
  `ConfigureMcpServer`/`ListMcpServers`/`DeleteMcpServer` and
  `horde mcp add/list/remove`.
- **Namespacing is `{server}__{tool}`**, split on the *first* `__`; the
  server-name charset (`[a-z0-9-]`, no `_`) is what makes the split
  unambiguous, and validation enforces it at authoring time.
- **The permission chain gates every proxied call at the agent's endpoint**
  (DESIGN §10's placement): each call is raised as an ordinary
  `PermissionRequested`; a deny returns the rule's reason as a readable tool
  error; an allow forwards the decided call, narrowing included. A harness
  that pre-authorizes through `approve` collapses both gates into one
  permission event via its `claudecode/toolUseId`.
- **Per-agent-type allowlists are resolved server-side** at command-fold
  time: `SpawnAgentCommand` carries the agent's `local` configs in full (the
  MaterializeWorkspace rule — no round trip) and its visible hub servers by
  name. Empty allowlist = every agent; non-empty fails closed for untyped
  agents. Invisible hub tools are unlisted *and* unroutable.

Open, deliberately, and flagged rather than solved silently:

- **Secrets in stdio `env` ride the journaled config** and are therefore
  replayable by attached clients. Acceptable single-user; needs a redaction
  or server-side-secret story before multi-user (the enrollment-code-hash
  precedent points the way).
- **Worker-runtime wiring**: the spawn command delivers each agent's MCP
  surface, and the endpoint assembly (`ToolProxy::with_hub_link` +
  `Upstream::connect` per `local` config) is built and tested — but the
  worker does not yet stand up per-agent endpoints at all
  (`AgentSpec.horde_endpoint` is still `None` in `runtime.rs`), so the
  consumption seam stays open alongside it: per-agent token minting, the
  worker-side `JournalSink` impl, and a control-plane channel at the worker.
- **Remote-MCP auth for harness children** stays as wave 5 flagged it: the
  current mode is loopback + per-agent bearer. The hub link accepts a bearer
  and arbitrary headers already; the mTLS door exists on the serving side
  (`EndpointConfig::secured`) but no client-side mTLS dial is wired.
- **Multiplexing under concurrency** remains the standing risk (one upstream
  session, many agents); every e2e here is one agent at a time, same as the
  dual-era spike's caveat.
- **Toolset change notifications** (`tools/list_changed`) are not emitted:
  an upstream's tool list is a connect-time snapshot.

## Amendment, 2026-08-19 (W9b): hub identity travels as worker-forwarded calls

The remote-MCP-auth open item is closed, and closed *against* the shape the
W6a note sketched. Per-agent hub identity does not travel as a credential at
all — not the loopback bearer pair pointed at a served hub endpoint, not a
server-minted per-agent token, not a client-side mTLS dial. It travels as
**worker-forwarded calls**: the Worker forwards each hub tool call to the
Server over its existing authenticated mTLS channel
(`WorkerService.ListHubTools`/`CallHubTool`), with the Agent's identity as
fields within the request — exactly how every other subsystem attributes
agent traffic (a raised permission, a journaled result, a spawn). A second
credential system for one subsystem would have been a second thing to mint,
rotate, revoke, and leak; the channel Horde already trusts is the channel.

What the Server checks on every forwarded call, in order: **attribution** —
the Agent must exist in the named Session and be placed, per the log, on the
calling Worker (the certificate outranks the request's claim), so an
authenticated Worker still cannot speak as an agent it does not hold; then
**visibility** — re-resolved from the journaled catalog and the Agent's type,
never trusted from the request, so invisible hub tools stay unlisted *and*
unroutable even to a Worker whose spawn-time menu has gone stale. The
permission chain stays at the Agent's endpoint (DESIGN §10), where it always
was; the forwarded call is the already-decided one.

Consequences of the ruling, recorded while they are fresh:

- **`HubEndpoint` is not mounted, and that is now a decision rather than a
  gap.** Serving the hub as its own MCP endpoint from `--serve` would be
  theater — nothing in production dials it. The type stays as the
  protocol-level e2e's stand-in for the hub side of the link, and its
  listing/failure-rendering helpers remain the agent endpoint's own.
- **`--serve` wires the hub from the journaled catalog, flaglessly.** Which
  servers exist is `horde mcp add`'s to say; startup warms every hub-placed
  upstream so a misconfiguration fails where the operator is looking, and a
  dead upstream is a reported line plus a per-call retry, never a downed
  control plane. Upstreams are connected once and reconnected only when a
  config's etag moves — "authenticated exactly once" now has a mechanism.
- **Revocation is inherited, not implemented.** The forwarded RPCs pass the
  same `require_enrolled` door as everything else on the channel, so a
  revoked Worker's hub calls die with its channel — proven, not assumed, in
  the worker suite.
- **The endpoint's proxied toolset is per-Agent** (`AgentProxies`): one
  Session endpoint serves many Agents, and `local` children and hub
  visibility are each Agent's own. An unregistered caller gets the verbs and
  nothing else.
- **MCP payloads ride the forwarded wire as JSON text**, deliberately unlike
  `ProposedToolCall`'s `Struct`: nothing Server-side reads the values, and
  `Struct`'s doubles-only numbers would corrupt integer arguments in
  transit.

Still open after this amendment: secrets in journaled stdio `env`,
multiplexing under concurrency (unchanged risk, now with the forwarding hop
in front of the one shared upstream session), and `tools/list_changed`.
