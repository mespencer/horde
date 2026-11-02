# Bare `horde` is the front door, and sessions answer to names

Creating and joining a session today takes three ids nobody should need to
know: `session new <TITLE>` demands a title and prints a session id, `attach
<SESSION_ID>` demands that id back, and `send` wants the id of the root agent
the session was born with. The workflow the ids serve is one motion —
"put me in front of a session" — so the surface becomes one motion too.

**Bare `horde` is the entrypoint.** With no subcommand: no live sessions
means create one, auto-named, and attach; live sessions mean an interactive
picker, newest first, whose top row is "new session". Exactly one live
session skips the picker and attaches. `horde new [title]` is the explicit
spelling (create, then attach; `--detach` for scripting), and `session new`
remains the headless create that prints the name and id without attaching.
Help lives at `--help`, where it always was.

**Sessions are named, thematically.** An omitted title draws a
`adjective-noun` name from a horde-flavored word list (`brisk-warband`,
`quiet-cohort`); a collision redraws rather than suffixing. A user-given
multi-word title slugifies for handle use. Wherever a session id is accepted
— attach, sync, spawn, get, archive, forward — a name or a unique id prefix
resolves to it, client-side against the session list, git-style; no protocol
change. Ambiguity is an error naming the candidates, never a guess.

**A session's name addresses its root agent.** Where an *agent* is expected
(`send`, `interrupt`, subtree scoping), a session name resolves to that
session's root agent — "talk to the session" needs no second id. Child
agents keep their own ids; addressing them stays explicit.

**Prompting discipline.** Pickers exist only where a human does: stdin a
TTY, `--json` absent. Headless or `--json` invocations that would have
prompted fail fast with the candidate list in the error. Archived sessions
stay out of the picker and out of name resolution unless `--all`; the
archived-while-you-chose race gets the fast named refusal, per the ADR-0006
philosophy that an explained refusal beats a hang.

**One rendering.** `session list` and the picker share a single row format:
name, short id, status, agent count, last-activity age. Choosing and
listing look identical.

The TUI itself is deliberately untouched: a full revamp is its own future
design, and nothing here reaches past the moment attach begins.

## Consequences

- `session new`'s title becomes optional everywhere; generated names are
  journaled in the SessionCreated event like any title, so replay and folds
  are unchanged.
- Name resolution is a client-side convenience over `session list`; the
  wire keeps speaking ids, and `--json` output always carries both id and
  name so scripts never re-resolve.
- Bare `horde` changes meaning from "print help" to "enter a session"; help
  remains at `--help`/`help`. This is the only place an existing invocation
  changes behavior, and the repo is unreleased.
- The word list is a fixed in-repo asset: name generation must be
  deterministic under test (seedable) and collision-checked against live
  sessions only.
- Sequenced as wave 10 alongside ADR-0007's `horde up`, which ends its
  stand-up by walking through this same front door.

## Amendment (2026-11-01): resolution that explains itself, and refusals a script can read

Wave 11h's usability pass settles what the front door says when it cannot
do what was asked.

**Resolution asks the server first, and refuses what it cannot find.** A
session reference is first sent through `ListSessions`' exact-name filter
(ADR-0013) — the common case, since generated names are titles — and only a
slug, an id or a prefix falls back to the whole list. A title matches
verbatim as well as by its slug. Because the fallback consults *every*
session, archived included, a reference nothing answers to is now refused
client-side (`no session is named …`) instead of passed through for the
server's less specific "not found". A full id stays exact whatever the
session's state, so nothing that addressed an archived session by id needs
`--all`; a name or prefix that only an archived session answers is refused
naming that session and `--all`. Duplicate titles remain an ambiguity
refusal listing every candidate.

**One flag for "archived too".** `--all` is canonical on every verb that can
look at archived sessions (`session list` included); `--include-archived`
parses everywhere as a hidden alias. `horde help [verb]` works again, as the
original consequences promised.

**Sending to an ended agent is refused, not warned.** ADR-0011 already has
the server refuse a message to a failed, lost or archived agent with
`FAILED_PRECONDITION`; the CLI turns that refusal into one that says what to
do (`horde agent status`, `horde spawn`). A warning-then-send would claim a
delivery that cannot happen — the context died with the agent — and a
client-side pre-check would race the server's own, authoritative one.

**Agent status is a fold, not an RPC.** `horde agent status <agent>` replays
the session (the named one, or each live one) and folds
`AgentLifecycleChanged` and `PlacementChanged` into state, worker and last
change, naming a `LOST` placement as "lost with its worker". `sync` renders
the same lifecycle changes: a transition line in prose, structured states in
`--json`.

**Refusals have a JSON form.** Under `--json`, every refusal — usage errors
included — is one object on stderr (`kind`, gRPC `code`, `message`, `hint`,
`candidates`) with a non-zero exit, and server refusals read as plain text
everywhere (`not found: …`), never tonic's debug dump. `horde doctor` checks
a machine end to end with an actionable fix per line, and `Hello` now echoes
the principal the server derived from the channel's certificate.
