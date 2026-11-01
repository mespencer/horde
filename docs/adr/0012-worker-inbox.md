# A Worker accepts commands into a journaled inbox and runs them per agent

A whole-project review found the Worker's command loop sound when nothing
goes wrong and brittle everywhere else. Commands applied strictly one at a
time, so a spawn cloning a large repo, or a materialize stuck on a slow
remote, held up every other agent's messages, interrupts and permission
decisions behind it. Any error out of a command — a decision missing its
request id, a narrowing the adapter could not apply — went straight to the
daemon's fate table as fatal and stopped the Worker with every agent on it.
And a message was gone the moment it was handed to an agent's pump: held for
a turn boundary when the harness died, or sent into a harness that had died
idle between turns (the broken pipe a Worker only notices when it writes),
it failed the agent and vanished, though the agent was perfectly revivable.

ADR-0011 already made one half of delivery durable: the Server's command fold
buffers a message sent to an agent that is not yet placed and redelivers it
behind every placement until the agent is seen running, and the Worker dedupes
on `command_id`. What it cannot cover is the Worker's own side — after the
Worker acked a command, the Server owes it nothing. This ADR makes that side
durable, without creating a second redelivery path that could double one.

**Applying a command is accept, then complete.** `Runtime::apply` now runs
one transaction: the `command_id` dedupe mark, both ack watermarks
(`applied_command_sequence`, `applied_command_position`), and the encoded
command itself into an additive `inbox` table. That is all the ack ever
claims: the command is this Worker's to finish. It is then handed to a
**lane** — one per agent id, one per workspace definition — which runs its
commands in arrival order and *completes* each in a second transaction: the
command's journaled effects, its revival bookkeeping, and the fate of its
inbox row. A crash between the two loses nothing: the next start replays the
inbox (`Runtime::replay_inbox`, before any dial and again lazily on the first
`apply`) in acceptance order, each row running again as the command it was.
That is the same at-least-once execution a crash before the old single
transaction gave; nothing new can run twice.

**Ordering is per agent; long effects block only their own lane.** An agent's
spawn, the messages the fold buffered behind it, its interrupts and its
archive apply exactly in the order the Server folded them, while another
agent's lane carries on. The one cross-lane order the sequential loop used to
give for free is kept explicitly: a spawn naming a workspace waits on a
*fence* through that workspace's lane, so a materialize dispatched before it
finishes first. A lane whose agent was archived or cancelled, with nothing
queued, retires itself; the next command for that agent starts a successor
that joins it first, so two lanes never run one agent at once.

**A command that cannot be applied is that command's failure.** It completes
like any other, its row settled, with a `HARNESS_DIAGNOSTIC` output chunk
journaled against its agent naming the command and why — an existing kind,
history the Server ingests as-is, and not a lifecycle claim: the agent did not
fail. A permission decision the adapter cannot apply (a narrowing without
`RewritesToolInput`) fails closed: the harness is told *deny* with the reason,
so the call is never silently widened and never left hanging. A materialize
on a Worker without workspace support reports a `DEGRADED` instance naming
it. Only failures that are the Worker's — a journal that cannot record the
acceptance or read the inbox — reach the daemon's fate table, and they stay
fatal there.

**A message is settled when the harness takes it.** A delivery handed to a
live pump completes with its row *handed*, not deleted; the pump deletes it
once `send` succeeded. Whatever is still handed when a harness dies is what
a revival delivers first, oldest first, as turn-boundary input, ahead of the
message that triggered it. A send that fails into a *proven* idle harness —
one that already took a message this incarnation, and has no turn in flight —
is not the agent's failure: the pump ends quietly and its lane revives the
agent to deliver what it was handed. A harness that refuses its very first
message (a revival that dies at once) fails the agent loudly, so no harness
can revive forever. A revival that cannot happen (no provider session, the
adapter refuses the resume) journals `FAILED` and settles every handed message
with it: the failure is their answer.

**Nothing is delivered twice.** The inbox and ADR-0011's buffer cover
disjoint spans of a message's life. The fold buffers until the agent is seen
running and then lets go; the inbox holds only what this Worker accepted.
The places they could meet are closed in the bookkeeping: a cancel or archive
(`AgentBookkeeping::Forget`) deletes the forgotten incarnation's *handed*
rows, because a re-placement — even back onto this Worker, under the fold's
`:placed:<n>` command ids — gets the Server's own redelivery; commands
accepted after the cancel (the re-placement's spawn among them) are kept. A
redelivery under the same `command_id` is refused at acceptance as before.
The one duplicate window left is the at-least-once one every journaled
system has: a crash after the harness took the text and before the pump
deleted the row replays it.

## Consequences

- The ack now means *accepted*, not *done*. Effects of a command may land in
  the journal after later commands' acks; the Server already judges each
  journal entry on its own (ADR-0009, ADR-0011), so nothing reads the two as
  coupled.
- Schema changes are additive (`inbox` table); no proto change is needed for
  the inbox. Old journals start with an empty inbox.
- `Runtime::settled()` resolves once every accepted command has completed —
  what a caller (the tests, chiefly) waits on before reading what a
  command's follow-up sets. `running_agents()` counts started pumps, which
  now trail `apply` returning.
- Riding along in the same review (not decisions of their own): journal
  WAL with `synchronous=NORMAL`; push batches bounded by encoded bytes
  (`horde_proto::limits::PUSH_BATCH_BYTES`, the first row always taken) with
  a deliberate 16 MiB `max_decoding/encoding_message_size` on the Server's
  worker spine and every Worker client of it; output coalesced (output the
  harness has already produced is merged per turn and channel, renumbered
  densely) and chunked at 64 KiB before journaling; jittered exponential
  redial backoff (0.5 s doubling to 10 s, drawn from the upper half,
  reset by a registration); a pumpless archive or cancel gives back the
  checkout (pinned under `refs/horde/archived/<agent>`) and endpoint token,
  closing ADR-0010's last gap; and `LoginOpen`/`LoginSessionRequest` carry
  an optional `account` (`horde worker login <w> <provider> --account <a>`)
  that signs in that account's profile directory.
- Deliberately not done: a single event larger than the 16 MiB ceiling (a
  permission request carrying an enormous write) still cannot cross, since an
  event cannot be split; and handed messages are replayed as plain text — the
  payload they carried was already made present when they were first handed.
