# Port forwards relay through the server, with intent in the log and listeners tied to liveness

A forward lets a worker port reach a client machine (**expose**: an agent's dev
server appears on the client's localhost) or a client port reach a worker
(**inject**: agents reach a database only the client's machine can see). Both
directions relay byte streams through the server over the existing mTLS
channels — client ↔ server ↔ worker, two authenticated hops — because workers
and clients only ever dial the server, and DESIGN §3's rule that nothing but
the server binds beyond localhost survives unchanged: every listener a forward
creates is loopback on somebody's machine. Direct client ↔ worker transport
(both ends already hold horde-identity principals) is an acknowledged later
optimisation, with the server as introducer; it is not the first
implementation.

The state model is **durable intent, ephemeral realization**. The intent —
"agent A exposes 5173 as `vite`" — is journaled (`ForwardOpened` /
`ForwardClosed`) and folded into session state like everything else. The
realization — an actual bound listener — is tied to a live connection and is
deliberately *not* log state: a listener cannot be replayed into existence,
the same reasoning that keeps commands out of tables. Client disconnect kills
that client's listeners and nothing else; reattach re-realizes every intent in
scope. Worker disconnect kills the data plane — client listeners stay bound
and refuse fast, naming the unreachable worker, because a fast explained
refusal beats a hang — and revival re-realizes without re-declaration. Intent
dies with the agent (`Failed`, archive): the process behind the port is gone,
so the forward has nothing left to mean.

Direction decides policy:

- **Expose** is agent-initiated and flows through the permission chain like
  any proposed action; the expected mode auto-allows within a named port/name
  policy and escalates the rest. Multiple attached clients each realize an
  expose independently on their own machine — no coordination, no conflict.
- **Inject** has exactly one **backer**: the client connection whose machine
  serves the traffic. Client-initiated injects have no selection problem —
  the initiator is the backer. Agent-initiated injects surface as an inbox
  escalation on every attached client, and *the first client to accept
  becomes the backer*: acceptance and backing are one act, on the accepter's
  own connection. This makes agent-initiated injects human-gated by physics
  rather than policy — an unbacked inject can do nothing, and only a human's
  client can back one. While unbacked (backer detached), the worker-side
  listener refuses with a distinguishable error and the session state says so;
  the owning client's return re-backs it. A v1 inject listener is
  loopback-wide on the worker — scoping it to one agent's processes needs
  namespaces the workers don't have, and pretending otherwise would be a lie
  in the trust story.

Forwards are named, not numbered: agents declare `expose 5173 as "vite"`
through an MCP verb, and clients render `vite → localhost:5173`. A client that
cannot bind the requested port falls back to an ephemeral port and prints the
mapping — which also makes two horde processes on one machine merely
redundant, never wrong; deduping them via local discovery is a rejected v1
complication. Raw TCP only; UDP and protocol-aware handling are out of scope.

## Consequences

- Two new lifecycle shapes ride existing machinery: expose approval reuses the
  permission chain's modes, inject acceptance reuses the escalation inbox and
  its decided-set race handling, and the journaled decision records which
  principal backed what.
- The server relays bulk bytes for the first time outside the git relay; gRPC
  bidirectional streams inherit HTTP/2 flow control, and the relay cost is
  accepted for v1.
- Sequenced as wave 8, after the conformance audit: proto (tunnel RPCs + two
  event kinds), server relay + folds, worker listeners/dialers, CLI/TUI
  surfaces, one new permission request kind.
