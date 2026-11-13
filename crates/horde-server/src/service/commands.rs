//! The per-worker command stream, folded out of the event log.
//!
//! Commands are at-least-once and carry a dense per-worker sequence, so a
//! worker that reconnects resumes from the last one it durably applied rather
//! than replaying blindly (`worker_service.proto`).
//!
//! # Why this is a projection and not a queue
//!
//! It used to be a queue: an in-memory `VecDeque` per worker, pushed by
//! whichever handler decided a worker should be told something. That lost every
//! undelivered command on restart, which is wrong by ADR-0001's standard —
//! everything that matters should be an event — and the fix was never a
//! `commands` table, because a second table is a second source of truth and the
//! two drift the first time a write path forgets one of them.
//!
//! The observation that makes a table unnecessary: **every command already
//! corresponds to an event that caused it.** A spawn command exists because an
//! agent was placed on a worker, and that placement is a `PlacementChanged`
//! event sitting in the log beside the `AgentSpawned` that describes what to
//! run. So the command stream is not state to be stored, it is a fold over the
//! log — the same shape as session lists, heads and journal watermarks, and
//! recomputed the same way after a restart.
//!
//! # The three sequence spaces
//!
//! `sequence` is per session and `journal_sequence` is per worker's own
//! journal, so neither can order two events from different sessions. Numbering
//! one worker's commands needs a total order over the whole log, which is the
//! append order [`GlobalEvent::row`] carries. The Nth command this fold emits
//! for a worker *is* that worker's command sequence N: dense, 1-based, and
//! monotonic because append order only ever grows.
//!
//! # Why the numbering survives a restart
//!
//! Nothing about the fold depends on when it runs. The same log yields the same
//! commands in the same order with the same sequences, so a worker that acked
//! command 7 before a restart is still talking about the same command 7 after
//! it. That is the property the old queue could not offer at any price, and it
//! is why the worker's ack — not the server's memory — decides where a stream
//! resumes.
//!
//! # ...unless the fold itself changes (ADR-0011)
//!
//! "The same log yields the same sequences" holds for one build of this fold.
//! A build that folds the same history into more or fewer commands — the
//! interrupt-storm fix dropped commands, buffered delivery adds them —
//! renumbers every later command, and a worker resuming by sequence then
//! skips or repeats work. So every command also carries its causing event's
//! log position, which no fold can renumber; a worker that reports the
//! highest position it applied resumes from that position, inclusive, and
//! dedupes the overlap on `command_id`. The sequence remains the resume key
//! only for a worker that has never seen a position.
//!
//! # Messages to agents that are not placed
//!
//! A message sent before its agent is placed — or while a not-yet-started
//! agent waits to be re-placed — is buffered here, durably in the only sense
//! that matters: it is a fold over the log, so it survives every restart. The
//! placement that finally spawns the agent delivers the buffer right behind
//! the spawn, and a re-placement delivers it again to the new worker, because
//! the new incarnation never saw any of it. The buffer is let go once the
//! agent is seen running: from then on it is never re-placed (a lost worker
//! declares it lost instead, ADR-0011), so nobody would ever replay it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use horde_proto::v1::{
    AgentId, ArchiveAgentCommand, CancelPlacementCommand, DeliverMessageCommand,
    DeliverPermissionDecisionCommand, Event, InterruptAgentCommand, MaterializeWorkspaceCommand,
    Placement, PlacementState, SessionId, SpawnAgentCommand, StreamCommandsResponse,
    WorkspaceDefinition, deliver_message_command, event::Kind, stream_commands_response::Command,
};
use prost::Message;
use tokio::sync::broadcast;

use crate::error::ServerError;
use crate::log::{EventLog, GlobalEvent, Notification};

/// How many log rows one fold step reads.
const PAGE: u32 = 512;

/// What an `AgentSpawned` said to run, held until a placement names its worker.
///
/// The two facts a spawn command needs are split across two events: the *what*
/// (`AgentSpawned`, carrying the spec and the parent) and the *where*
/// (`PlacementChanged`, carrying the worker). Neither alone is a command.
#[derive(Debug, Clone, PartialEq)]
struct SpawnDetails {
    session_id: String,
    parent_agent_id: Option<String>,
    spec: Option<horde_proto::v1::AgentSpec>,
    tree_path: Vec<String>,
}

/// One message held for an agent that may still be (re-)placed.
#[derive(Debug, Clone, PartialEq)]
struct Buffered {
    /// The message event's own position — its delivery's identity.
    session_id: String,
    sequence: u64,
    body: deliver_message_command::Body,
    delivery: i32,
}

/// The fold state: everything needed to turn the next log row into commands.
///
/// Deliberately free of any I/O, so the interesting logic is a pure function of
/// a sequence of events. Its tests drive it with hand-built events and never
/// stand up a database.
#[derive(Debug, Default)]
pub struct CommandProjection {
    /// Agents whose `AgentSpawned` has been folded, keyed by agent id.
    spawned: BTreeMap<String, SpawnDetails>,
    /// Which worker each placed agent runs on, keyed by agent id.
    ///
    /// Separate from [`Self::spawned`] because it answers a different question
    /// and outlives the answer to the first: `spawned` holds what a *spawn*
    /// command needs and is only interesting until placement, whereas every
    /// later command addressed to a running agent — a permission decision, an
    /// interrupt — needs to know which worker to reach, and the only event that
    /// ever says so is `PlacementChanged`.
    placed: BTreeMap<String, String>,
    /// The workspace catalog as this fold has seen it, keyed by definition id.
    ///
    /// A `MaterializeWorkspaceCommand` carries the *full* definition — a
    /// worker must be able to materialize without a round trip — and the only
    /// place a fold can get one is the `WorkspaceDefinitionChanged` events
    /// that precede every prewarm request in append order.
    definitions: BTreeMap<String, WorkspaceDefinition>,
    /// Definitions deleted since: a purge's gc commands still carry the last
    /// Definition, so its cleanup policy applies to the Instances it removes
    /// (ADR-0014).
    deleted_definitions: BTreeMap<String, WorkspaceDefinition>,
    /// The Server's workspace defaults, resolved with each Definition into
    /// the policy every spawn, materialize and gc command carries.
    workspace_policy: Arc<crate::workspace_policy::WorkspacePolicyConfig>,
    /// The MCP Hub catalog as this fold has seen it, for the same reason:
    /// a `SpawnAgentCommand` carries the agent's `local` server configs and
    /// its visible hub server names in full, and the only place a fold can
    /// resolve them is the `McpServerConfigChanged` events that precede the
    /// placement in append order.
    mcp_servers: crate::mcp::McpServerCatalog,
    /// How many commands each worker has been issued. This *is* the per-worker
    /// command sequence.
    issued: BTreeMap<String, u64>,
    /// Every message sent to an agent that has not been seen running, in
    /// send order — what a (re-)placement delivers behind its spawn.
    inbox: BTreeMap<String, Vec<Buffered>>,
    /// Agents seen running (or ended): never re-placed, so never buffered.
    started: BTreeSet<String>,
    /// How many journal resets each worker has had (ADR-0011). A stream
    /// watches its own worker's count: commands folded before a reset were
    /// issued to a journal that no longer exists.
    resets: BTreeMap<String, u64>,
}

impl CommandProjection {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves command policies against these Server defaults.
    #[must_use]
    pub fn with_workspace_policy(
        mut self,
        policy: Arc<crate::workspace_policy::WorkspacePolicyConfig>,
    ) -> Self {
        self.workspace_policy = policy;
        self
    }

    /// The policy one Definition's commands carry.
    fn policy_for(&self, definition_id: Option<&str>) -> horde_proto::v1::ResolvedWorkspacePolicy {
        let definition = definition_id.and_then(|id| {
            self.definitions
                .get(id)
                .or_else(|| self.deleted_definitions.get(id))
        });
        crate::workspace_policy::resolve(&self.workspace_policy, definition)
    }

    /// How many commands this worker has been issued so far.
    #[must_use]
    pub fn issued(&self, worker_id: &str) -> u64 {
        self.issued.get(worker_id).copied().unwrap_or(0)
    }

    /// How many journal resets this worker has had so far in the fold.
    #[must_use]
    pub fn resets(&self, worker_id: &str) -> u64 {
        self.resets.get(worker_id).copied().unwrap_or(0)
    }

    /// Folds one log row, returning the commands it causes and whose they are.
    ///
    /// The worker id comes back alongside each command because a fold advanced
    /// by one worker's stream still has to account for every other worker's
    /// commands: skipping them would leave this worker's numbering right and
    /// the fold's state wrong. A `Vec` because one event can command two
    /// workers at once: a re-placement spawns on the new worker *and* cancels
    /// on the old one.
    pub fn advance(&mut self, global: &GlobalEvent) -> Vec<(String, StreamCommandsResponse)> {
        let Ok(event) = Event::decode(global.event.payload.as_slice()) else {
            return Vec::new();
        };
        let agent_id = event.agent_id.as_ref().map(ToString::to_string);

        match (event.kind, agent_id) {
            // Facts the fold remembers but that command nobody by themselves.
            (
                Some(
                    kind @ (Kind::WorkspaceDefinitionChanged(_)
                    | Kind::McpServerConfigChanged(_)
                    | Kind::AgentLifecycleChanged(_)
                    | Kind::WorkerJournalReset(_)),
                ),
                agent_id,
            ) => {
                self.remember(global, kind, agent_id);
                Vec::new()
            }
            (Some(Kind::WorkspacePrewarmRequested(prewarm)), _) => {
                self.materialize_command(global, &prewarm)
            }
            (Some(Kind::WorkspaceGcRequested(requested)), _) => {
                self.gc_commands(global, &requested)
            }
            (Some(Kind::AgentSpawned(spawned)), Some(agent_id)) => {
                self.spawned.insert(
                    agent_id,
                    SpawnDetails {
                        session_id: global.event.session_id.clone(),
                        parent_agent_id: spawned.parent_agent_id.as_ref().map(ToString::to_string),
                        spec: spawned.spec,
                        tree_path: spawned.tree_path,
                    },
                );
                Vec::new()
            }
            (Some(Kind::PlacementChanged(changed)), Some(agent_id)) => {
                self.placement_commands(global, &agent_id, changed)
            }
            // The return leg of the permission chain. A decision is written to
            // the log by whoever answered it — a human through
            // `AnswerPermission`, or a policy layer — and the agent that is
            // blocked on it lives on a worker that reads commands, not the log.
            // Without this arm the decision is durable and invisible: the
            // human's answer lands, and the agent waits forever.
            // Steering: a message to an agent lives in the log, but the agent
            // lives on a worker that reads commands. The delivery mode rides
            // whole — turn-boundary or interrupt is the sender's call, and a
            // fold that dropped it would downgrade every interrupt to a queue.
            (Some(Kind::AssignmentSent(sent)), Some(agent_id)) => self.deliver_commands(
                global,
                &agent_id,
                sent.assignment
                    .map(deliver_message_command::Body::Assignment),
                sent.delivery,
            ),
            (Some(Kind::NoteSent(sent)), Some(agent_id)) => self.deliver_commands(
                global,
                &agent_id,
                sent.note.map(deliver_message_command::Body::Note),
                sent.delivery,
            ),
            // The interrupt verb. The RPC journals the request; the worker
            // holding the agent is the only place it can be carried out.
            //
            // Only a *Server-written* interrupt is a request. The worker
            // acknowledges carrying one out by journaling a `TurnInterrupted`
            // of its own, and folding that acknowledgement into another
            // command made each interrupt an unbounded loop — request, ack,
            // request — that grew the log by thousands of rows a second
            // (ADR-0009). Origin is the discriminator because it is the one
            // fact about the row no journal can forge: the Server stamps it.
            (Some(Kind::TurnInterrupted(interrupted)), Some(agent_id)) if is_request(global) => {
                let Some(worker_id) = self.placed.get(&agent_id).cloned() else {
                    // An unplaced agent has no turn in flight to abort.
                    return Vec::new();
                };
                let command = Command::InterruptAgent(InterruptAgentCommand {
                    agent_id: Some(AgentId::new(agent_id)),
                    reason: interrupted.reason,
                });
                let issued = self.issue(&worker_id, global, command, MAIN);
                vec![(worker_id, issued)]
            }
            // Archive finally has a producer (fix a7): archiving the session
            // folds into an ArchiveAgentCommand for each of its placed
            // agents. Context dies at archive, not at result (DESIGN §5),
            // and before this arm `ArchiveAgentCommand` was honored by every
            // worker and issued by nobody.
            (Some(Kind::SessionLifecycleChanged(changed)), _) => {
                self.archive_commands(global, &changed)
            }
            (Some(Kind::PermissionDecided(decided)), Some(agent_id)) => {
                let (Some(decision), Some(worker_id)) =
                    (decided.decision, self.placed.get(&agent_id).cloned())
                else {
                    return Vec::new();
                };

                let command =
                    Command::DeliverPermissionDecision(DeliverPermissionDecisionCommand {
                        agent_id: Some(AgentId::new(agent_id)),
                        // Forwarded whole. `updated_input` rides inside, and a
                        // decision that narrowed a call is worth nothing to the
                        // worker if the narrowing is left behind here.
                        decision: Some(decision),
                    });
                let issued = self.issue(&worker_id, global, command, MAIN);
                vec![(worker_id, issued)]
            }
            _ => Vec::new(),
        }
    }

    /// Folds a fact that issues no command by itself but shapes later ones.
    fn remember(&mut self, global: &GlobalEvent, kind: Kind, agent_id: Option<String>) {
        match (kind, agent_id) {
            // The catalog half of materialization: a prewarm request can only
            // become a command if the definition it names has been folded.
            (Kind::WorkspaceDefinitionChanged(changed), _) => self.fold_definition(changed),
            // The catalog half of the MCP Hub: a spawn can only carry a
            // server's config if the change that created it has been folded.
            (Kind::McpServerConfigChanged(changed), _) => {
                self.mcp_servers.fold_change(global.event.sequence, changed);
            }
            // An agent seen running is never re-placed again (a lost worker
            // declares it lost instead), so its buffer has no future reader.
            (Kind::AgentLifecycleChanged(changed), Some(agent_id)) => {
                if let Ok(state) = horde_proto::v1::AgentLifecycleState::try_from(changed.state)
                    && crate::placement::survives_only_as_placed(state)
                {
                    self.inbox.remove(&agent_id);
                    self.started.insert(agent_id);
                }
            }
            // An operator reset this worker's journal (ADR-0011): its fresh
            // state applied nothing, so its numbering starts over.
            (Kind::WorkerJournalReset(reset), _) => {
                if let Some(worker_id) = reset.worker_id.as_ref().map(ToString::to_string) {
                    self.issued.insert(worker_id.clone(), 0);
                    *self.resets.entry(worker_id).or_insert(0) += 1;
                }
            }
            _ => {}
        }
    }

    /// Keeps the fold's copy of the workspace catalog current.
    fn fold_definition(&mut self, changed: horde_proto::v1::WorkspaceDefinitionChanged) {
        let Some(definition) = changed.definition else {
            return;
        };
        let Some(id) = definition
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
        else {
            return;
        };
        if changed.deleted {
            self.definitions.remove(&id);
            self.deleted_definitions.insert(id, definition);
        } else {
            self.deleted_definitions.remove(&id);
            self.definitions.insert(id, definition);
        }
    }

    /// A gc request is one `WorkspaceGcCommand` per Worker it names, each
    /// carrying the resolved policy; its id is the request's position
    /// suffixed with the Worker, so a refold issues the same ids (ADR-0014).
    fn gc_commands(
        &mut self,
        global: &GlobalEvent,
        requested: &horde_proto::v1::WorkspaceGcRequested,
    ) -> Vec<(String, StreamCommandsResponse)> {
        let definition_id = requested
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty());
        let policy = self.policy_for(definition_id.as_deref());
        let mut commands = Vec::new();
        for worker in &requested.worker_ids {
            let worker_id = worker.to_string();
            let command = Command::WorkspaceGc(horde_proto::v1::WorkspaceGcCommand {
                request_id: requested.request_id.clone(),
                workspace_definition_id: requested.workspace_definition_id.clone(),
                agent_ids: requested.agent_ids.clone(),
                level: requested.level,
                force: requested.force,
                dry_run: requested.dry_run,
                remove_instance: requested.remove_instance,
                include_pins: requested.include_pins,
                policy: Some(policy.clone()),
                carrying_stream: Some(SessionId::new(global.event.session_id.clone())),
            });
            let issued = self.issue(&worker_id, global, command, &format!(":gc:{worker_id}"));
            commands.push((worker_id, issued));
        }
        commands
    }

    /// A prewarm request for a known definition is a materialize command.
    ///
    /// An unknown definition yields nothing rather than a command with an
    /// empty definition in it: a worker told to materialize nothing would
    /// create an instance of nothing. Control-plane validation makes this
    /// unreachable in a live log; a replay of a partial log makes it real.
    fn materialize_command(
        &mut self,
        global: &GlobalEvent,
        prewarm: &horde_proto::v1::WorkspacePrewarmRequested,
    ) -> Vec<(String, StreamCommandsResponse)> {
        let Some(worker_id) = prewarm.worker_id.as_ref().map(ToString::to_string) else {
            return Vec::new();
        };
        let Some(definition) = prewarm
            .workspace_definition_id
            .as_ref()
            .and_then(|id| self.definitions.get(&id.to_string()).cloned())
        else {
            return Vec::new();
        };

        let policy = crate::workspace_policy::resolve(&self.workspace_policy, Some(&definition));
        let command = Command::MaterializeWorkspace(MaterializeWorkspaceCommand {
            definition: Some(definition),
            session_id: Some(SessionId::new(global.event.session_id.clone())),
            policy: Some(policy),
        });
        let issued = self.issue(&worker_id, global, command, MAIN);
        vec![(worker_id, issued)]
    }

    /// The archive commands one session-archive event causes: every placed
    /// agent of the archived session is archived on its worker.
    ///
    /// Each command's id carries the agent's own suffix — one event causes
    /// as many commands as the session has placed agents, and two commands
    /// under one id would be one command redelivered. The agents leave both
    /// maps: an archived agent must take no later deliveries here, and the
    /// worker's own bookkeeping forgets it on apply.
    fn archive_commands(
        &mut self,
        global: &GlobalEvent,
        changed: &horde_proto::v1::SessionLifecycleChanged,
    ) -> Vec<(String, StreamCommandsResponse)> {
        if changed.state != i32::from(horde_proto::v1::SessionLifecycleState::Archived) {
            return Vec::new();
        }
        let session_id = &global.event.session_id;
        let archived: Vec<String> = self
            .spawned
            .iter()
            .filter(|(_, details)| &details.session_id == session_id)
            .map(|(agent_id, _)| agent_id.clone())
            .collect();

        let mut commands = Vec::new();
        for agent_id in archived {
            self.spawned.remove(&agent_id);
            // Archive is terminal: nothing buffered for it is owed anyone.
            self.inbox.remove(&agent_id);
            self.started.insert(agent_id.clone());
            let Some(worker_id) = self.placed.remove(&agent_id) else {
                continue; // Queued or never placed: nobody is running it.
            };
            let command = Command::ArchiveAgent(ArchiveAgentCommand {
                agent_id: Some(AgentId::new(agent_id.clone())),
                reason: if changed.reason.is_empty() {
                    "the session was archived".to_owned()
                } else {
                    changed.reason.clone()
                },
            });
            let suffix = format!(":archive:{agent_id}");
            let issued = self.issue(&worker_id, global, command, &suffix);
            commands.push((worker_id, issued));
        }
        commands
    }

    /// The commands one placement transition causes.
    ///
    /// A placement that lands an agent issues its spawn command; a placement
    /// that *moves or unseats* an agent also cancels it on the worker that
    /// held it — that cancel is what settles the at-least-once race with a
    /// worker that reconnects and replays a spawn it never applied.
    fn placement_commands(
        &mut self,
        global: &GlobalEvent,
        agent_id: &str,
        changed: horde_proto::v1::PlacementChanged,
    ) -> Vec<(String, StreamCommandsResponse)> {
        let new_worker = changed.placement.as_ref().and_then(placement_worker);
        let previous_worker = self.placed.get(agent_id).cloned();
        let mut commands = Vec::new();

        if let Some(previous) = previous_worker.as_ref()
            && new_worker.as_deref() != Some(previous)
        {
            let cancel = Command::CancelPlacement(CancelPlacementCommand {
                agent_id: Some(AgentId::new(agent_id)),
                reason: "the agent is no longer placed on this worker".to_owned(),
            });
            let issued = self.issue(previous, global, cancel, CANCEL);
            commands.push((previous.clone(), issued));
        }

        match new_worker {
            Some(worker_id) => {
                // Recorded before the spawn details are looked up, because the
                // two can fail independently: an agent may be placed without
                // this fold having seen its `AgentSpawned` (a replay starting
                // mid-log), and losing the placement in that case would leave
                // every later command for the agent unroutable.
                self.placed.insert(agent_id.to_owned(), worker_id.clone());

                // A placement re-stating the same worker is not a new spawn.
                if previous_worker.as_deref() == Some(worker_id.as_str()) {
                    return commands;
                }
                if let Some(details) = self.spawned.get(agent_id) {
                    // The MCP Hub surface this agent gets, resolved here from
                    // the folded catalog and the agent's type: the worker must
                    // spawn `local` servers without a round trip, so full
                    // configs ride the command (the MaterializeWorkspace rule).
                    let agent_type = details
                        .spec
                        .as_ref()
                        .map(|spec| spec.agent_type.as_str())
                        .unwrap_or_default();
                    let spawn = Command::SpawnAgent(SpawnAgentCommand {
                        session_id: Some(SessionId::new(details.session_id.clone())),
                        agent_id: Some(AgentId::new(agent_id)),
                        parent_agent_id: details.parent_agent_id.clone().map(AgentId::new),
                        spec: details.spec.clone(),
                        binding: binding_for(details.spec.as_ref()),
                        tree_path: details.tree_path.clone(),
                        prompt_overlay: String::new(),
                        local_mcp_servers: self.mcp_servers.local_servers_for(agent_type),
                        visible_hub_mcp_servers: self
                            .mcp_servers
                            .visible_hub_servers_for(agent_type),
                        workspace_policy: details
                            .spec
                            .as_ref()
                            .and_then(|spec| spec.workspace_definition_id.as_ref())
                            .map(|id| self.policy_for(Some(&id.to_string()))),
                    });
                    let issued = self.issue(&worker_id, global, spawn, MAIN);
                    commands.push((worker_id.clone(), issued));
                    // Right behind the spawn: everything sent while the agent
                    // had no worker, or before this incarnation existed.
                    commands.extend(self.flush_inbox(global, agent_id, &worker_id));
                }
            }
            None => {
                self.placed.remove(agent_id);
            }
        }
        commands
    }

    /// The command one sent message causes: deliver it to the worker holding
    /// the agent, mode intact — and, while the agent may still be
    /// (re-)placed, keep it for the placement that spawns the next
    /// incarnation (see the module docs).
    ///
    /// A message to an agent this fold never saw spawned, or one already
    /// archived or seen running and now unplaced, has nobody it could ever
    /// reach and issues nothing; the control plane refuses those sends.
    fn deliver_commands(
        &mut self,
        global: &GlobalEvent,
        agent_id: &str,
        body: Option<deliver_message_command::Body>,
        delivery: i32,
    ) -> Vec<(String, StreamCommandsResponse)> {
        let Some(body) = body else {
            return Vec::new();
        };
        if buffers_for(
            self.spawned.contains_key(agent_id),
            self.started.contains(agent_id),
        ) {
            self.inbox
                .entry(agent_id.to_owned())
                .or_default()
                .push(Buffered {
                    session_id: global.event.session_id.clone(),
                    sequence: global.event.sequence,
                    body: body.clone(),
                    delivery,
                });
        }
        let Some(worker_id) = self.placed.get(agent_id).cloned() else {
            return Vec::new();
        };

        let command = deliver(agent_id, body, delivery);
        let issued = self.issue(&worker_id, global, command, MAIN);
        vec![(worker_id, issued)]
    }

    /// The buffered messages a placement owes its new worker, behind the
    /// spawn it just issued.
    ///
    /// Each delivery's id is the message's own position suffixed with the
    /// placement's, so a re-placement back onto a worker that applied an
    /// earlier incarnation's copy is not deduplicated away; its log position
    /// is the placement's, which is where the fold issued it.
    fn flush_inbox(
        &mut self,
        placement: &GlobalEvent,
        agent_id: &str,
        worker_id: &str,
    ) -> Vec<(String, StreamCommandsResponse)> {
        let buffered = self.inbox.get(agent_id).cloned().unwrap_or_default();
        let role = format!(":placed:{}", placement.event.sequence);
        buffered
            .into_iter()
            .map(|message| {
                let id = command_id(&message.session_id, message.sequence, &role);
                let command = deliver(agent_id, message.body, message.delivery);
                let issued = self.issue_with_id(worker_id, placement.row, id, command);
                (worker_id.to_owned(), issued)
            })
            .collect()
    }

    /// Assigns the next per-worker sequence, and the command's stable id.
    fn issue(
        &mut self,
        worker_id: &str,
        cause: &GlobalEvent,
        command: Command,
        role: &str,
    ) -> StreamCommandsResponse {
        // Derived from the event that caused it rather than from the
        // command's own sequence. `(session_id, sequence)` is the log's
        // primary key, so this is unique by construction and identical every
        // time the fold is recomputed — which is what "stable across
        // redelivery" has to mean once a redelivery can follow a restart.
        let id = command_id(&cause.event.session_id, cause.event.sequence, role);
        self.issue_with_id(worker_id, cause.row, id, command)
    }

    /// [`Self::issue`] with the id already chosen.
    fn issue_with_id(
        &mut self,
        worker_id: &str,
        log_position: u64,
        command_id: String,
        command: Command,
    ) -> StreamCommandsResponse {
        let next = self.issued.entry(worker_id.to_owned()).or_insert(0);
        *next = next.saturating_add(1);

        StreamCommandsResponse {
            command_sequence: *next,
            command_id,
            log_position,
            command: Some(command),
        }
    }
}

/// A delivery command for one agent.
fn deliver(agent_id: &str, body: deliver_message_command::Body, delivery: i32) -> Command {
    Command::DeliverMessage(DeliverMessageCommand {
        agent_id: Some(AgentId::new(agent_id)),
        body: Some(body),
        delivery,
    })
}

/// Whether a message is kept for a later (re-)placement: only for an agent
/// this fold saw spawned and has not yet seen running or ended.
///
/// Both halves disqualify alone, and inline they are not separately
/// reachable: a live log never messages an agent it has not spawned, and
/// a started agent is usually still placed, so either operator flipped keeps
/// passing end-to-end while buffering for nobody or re-delivering to a
/// running agent on its next re-placement.
fn buffers_for(spawned: bool, started: bool) -> bool {
    spawned && !started
}

/// The binding a spawn's model selection resolves to (W7b finding 5).
///
/// Deterministic over the spec alone, which is what a fold demands: a
/// concrete request binds as itself; a tier binds its catalog's
/// first-listed candidate. Account-aware selection (`options_for` /
/// `select`, weighing advertised headroom) stays unfed until workers
/// advertise accounts — feeding it a live registry here would make two
/// folds of one log disagree. An unknown tier yields no binding rather
/// than a panic: the RPC already refused unknown names, so hitting one
/// means an older log, and the worker's default provider is the honest
/// fallback.
pub(crate) fn binding_for(
    spec: Option<&horde_proto::v1::AgentSpec>,
) -> Option<horde_proto::v1::ModelBinding> {
    use horde_proto::v1::model_selection::Selection;
    match spec?.model.as_ref()?.selection.as_ref()? {
        Selection::Concrete(concrete) => Some(horde_proto::v1::ModelBinding {
            tier: String::new(),
            provider: concrete.provider.clone(),
            model: concrete.model.clone(),
            provider_account: String::new(),
        }),
        Selection::Tier(tier_name) => {
            let catalog = crate::tiers::TierCatalog::shipped_defaults();
            let candidate = catalog.get(tier_name)?.candidates.first()?.clone();
            Some(horde_proto::v1::ModelBinding {
                tier: tier_name.clone(),
                provider: candidate.provider,
                model: candidate.model,
                provider_account: String::new(),
            })
        }
    }
}

/// Whether a row is a request the Server made, as opposed to a fact a Worker
/// reported. Events the Server appends carry no origin worker; everything a
/// journal delivered carries the journal's worker.
fn is_request(global: &GlobalEvent) -> bool {
    global.origin_worker_id.is_empty()
}

/// The main command an event causes. Its id is the bare log position.
const MAIN: &str = "";

/// A cancel issued *alongside* an event's main command. Suffixed so the two
/// ids stay distinct even though one event caused both.
const CANCEL: &str = ":cancel";

/// The worker a placement names, if it is actually placed on one.
///
/// Extracted rather than written inline as `state == Placed && worker.is_some()`
/// because the two halves are not separately reachable in a live run: the
/// server only ever writes a worker id alongside `Placed`, so an `||` behaves
/// identically in every end-to-end test while being wrong about a queued
/// placement that still carries a stale worker. As a function the combinations
/// are an ordinary truth table.
fn placement_worker(placement: &Placement) -> Option<String> {
    let placed = placement.state == PlacementState::Placed as i32;
    let worker = placement.worker_id.as_ref().map(ToString::to_string);
    match (placed, worker) {
        (true, Some(worker)) => Some(worker),
        _ => None,
    }
}

/// A command's stable identity: the log position of the event that caused it,
/// plus a role suffix when one event causes more than one command.
fn command_id(session_id: &str, sequence: u64, role: &str) -> String {
    format!("{session_id}:{sequence}{role}")
}

/// Where a worker said its command stream resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resume {
    /// The first command sequence not applied — the fallback key.
    pub sequence: u64,
    /// The highest causing-event position applied; zero when unknown. When
    /// known it outranks the sequence (see the module docs).
    pub log_position: u64,
}

impl Resume {
    /// A resume by sequence alone, as every worker before ADR-0011 asks.
    #[must_use]
    pub fn from_sequence(sequence: u64) -> Self {
        Self {
            sequence,
            log_position: 0,
        }
    }

    /// Whether a command at this sequence and position is at or past the
    /// resume point. Inclusive both ways: the position a worker applied may
    /// have caused further commands it never saw, and it dedupes the ones it
    /// did on `command_id`.
    #[must_use]
    pub fn reaches(&self, command: &StreamCommandsResponse) -> bool {
        if self.log_position > 0 {
            command.log_position >= self.log_position
        } else {
            command.command_sequence >= self.sequence
        }
    }
}

/// Whether a folded command is one this stream still owes its worker.
///
/// Two independent reasons to drop one, and inline they are not separately
/// reachable: a stream almost always folds its own worker's commands in
/// ascending order from its resume point, so `&&` keeps behaving correctly
/// while silently delivering another worker's commands the moment two workers
/// share a server. Four cases, all assertable.
fn is_owed(
    worker_id: &str,
    command_worker: &str,
    command: &StreamCommandsResponse,
    resume: Resume,
) -> bool {
    worker_id == command_worker && resume.reaches(command)
}

/// One worker's live command stream.
///
/// Holds its own fold rather than sharing one: a stream is positioned by *its*
/// worker's resume point, and the fold is cheap to rebuild because the log is
/// the only durable state and it is already being read.
pub struct CommandStream {
    log: EventLog,
    worker_id: String,
    resume: Resume,
    projection: CommandProjection,
    /// Append position read up to. Owned here rather than by the fold, because
    /// this is what [`Self::catch_up`]'s loop terminates on and a loop whose
    /// termination lives in another function is a hang waiting for a refactor.
    /// Mutation testing found exactly that: every mutant of the fold that left
    /// the cursor alone turned the loop into an infinite re-read of one page,
    /// and hung the suite instead of failing it.
    cursor: u64,
    notifications: broadcast::Receiver<Notification>,
    parks: Arc<AtomicU64>,
}

impl CommandStream {
    /// Opens a stream for one worker, delivering commands from `resume_from`.
    ///
    /// Subscribes before the first read for the same reason [`EventLog::tail`]
    /// does: from that moment every append is announced, so a command written
    /// during the catch-up read is either in the read or in the channel.
    #[must_use]
    pub fn open(log: &EventLog, worker_id: &str, resume_from: u64) -> Self {
        Self::resuming(log, worker_id, Resume::from_sequence(resume_from))
    }

    /// [`Self::open`], resuming wherever the worker said — by position when
    /// it knows one (ADR-0011).
    /// Stamps commands with policies resolved against these defaults.
    #[must_use]
    pub fn with_workspace_policy(
        mut self,
        policy: Arc<crate::workspace_policy::WorkspacePolicyConfig>,
    ) -> Self {
        self.projection = std::mem::take(&mut self.projection).with_workspace_policy(policy);
        self
    }

    #[must_use]
    pub fn resuming(log: &EventLog, worker_id: &str, resume: Resume) -> Self {
        Self {
            notifications: log.subscribe(),
            log: log.clone(),
            worker_id: worker_id.to_owned(),
            resume: Resume {
                sequence: resume.sequence.max(1),
                log_position: resume.log_position,
            },
            projection: CommandProjection::new(),
            cursor: 0,
            parks: Arc::new(AtomicU64::new(0)),
        }
    }

    /// How many times this stream has waited for the log to move.
    ///
    /// Exposed because "did not spin" is otherwise unobservable. A stream that
    /// busy-loops instead of parking delivers exactly the right commands and
    /// passes every functional test, while burning a core and — the reason this
    /// counter exists — turning a mutation of the waiting path into a suite
    /// that hangs rather than one that fails. A test can assert this moved
    /// exactly once across an idle period: a hot loop drives it to thousands,
    /// and a wait that never waits leaves it at zero.
    #[must_use]
    pub fn parks(&self) -> u64 {
        self.parks.load(Ordering::Relaxed)
    }

    /// The next commands owed to this worker.
    ///
    /// `None` means the log was dropped and no further commands can arrive.
    ///
    /// Every path out of here returns a value rather than merely finishing. That
    /// is deliberate: an `async fn wait(&self)` returning `()` is
    /// indistinguishable from a mutant that returns immediately, so the mutant
    /// does not fail a test, it hangs one — and a timeout is a mutant nobody
    /// evaluated. Waiting is folded into a function that has to produce commands
    /// to be believed.
    pub async fn next_batch(&mut self) -> Option<Result<Vec<StreamCommandsResponse>, ServerError>> {
        loop {
            match self.catch_up().await {
                Ok(batch) if !batch.is_empty() => return Some(Ok(batch)),
                Ok(_) => {}
                Err(error) => return Some(Err(error)),
            }

            self.parks.fetch_add(1, Ordering::Relaxed);
            match self.notifications.recv().await {
                Ok(_) => {}
                // Lagging is survivable for the same reason it is on a tail: the
                // channel is a hint and the log is the truth, so the fold simply
                // carries on from its cursor.
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(
                        missed,
                        worker_id = %self.worker_id,
                        "command stream fell behind; re-reading from the log"
                    );
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }

    /// Folds every log row appended since the last call, collecting whatever
    /// this worker is owed.
    async fn catch_up(&mut self) -> Result<Vec<StreamCommandsResponse>, ServerError> {
        let mut owed = Vec::new();
        loop {
            let page = self.log.read_global_from(self.cursor, PAGE).await?;
            // The cursor comes from the page just read, not from the fold. The
            // page is strictly ascending in `row` and this is the only reader,
            // so the next request always asks for something later and the loop
            // cannot re-read what it just consumed.
            let Some(last) = page.last() else {
                return Ok(owed);
            };
            self.cursor = last.row;

            for global in &page {
                let resets_before = self.projection.resets(&self.worker_id);
                let commands = self.projection.advance(global);
                if self.projection.resets(&self.worker_id) != resets_before {
                    // A journal reset (ADR-0011): everything folded so far was
                    // issued to a journal that no longer exists, and the
                    // fresh one resumes by the restarted numbering.
                    owed.clear();
                    self.resume = Resume::from_sequence(self.resume.sequence.max(1));
                }
                for (worker_id, command) in commands {
                    if is_owed(&self.worker_id, &worker_id, &command, self.resume) {
                        owed.push(command);
                    }
                }
            }
        }
    }
}

impl std::fmt::Debug for CommandStream {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CommandStream")
            .field("worker_id", &self.worker_id)
            .field("resume", &self.resume)
            .field("row", &self.cursor)
            .finish_non_exhaustive()
    }
}

/// How many commands a worker has been issued across the whole log.
///
/// What `RegisterWorker` answers with, and a fold rather than a remembered
/// number for the same reason as everything else here: after a restart the
/// remembered one would be zero, and the worker would be told to replay from
/// the beginning of a stream it has already applied.
pub async fn issued_to(log: &EventLog, worker_id: &str) -> Result<u64, ServerError> {
    let mut projection = CommandProjection::new();
    let mut cursor = 0;
    loop {
        let page = log.read_global_from(cursor, PAGE).await?;
        // Same rule as `CommandStream::catch_up`: the cursor advances from the
        // page, so this loop terminates on what it read rather than on what the
        // fold did with it.
        let Some(last) = page.last() else {
            return Ok(projection.issued(worker_id));
        };
        cursor = last.row;

        for global in &page {
            projection.advance(global);
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        AgentSpec, DeliveryMode, PlacementChanged, TurnInterrupted, WorkerId,
        stream_commands_response::Command,
    };

    use super::*;
    use crate::log::{PendingEvent, StoredEvent};

    /// A log row carrying `event`, at `row` in append order.
    fn row(row: u64, session: &str, sequence: u64, event: Event) -> GlobalEvent {
        GlobalEvent {
            row,
            event: StoredEvent {
                session_id: session.to_owned(),
                sequence,
                recorded_at_unix_ms: 0,
                payload: event.encode_to_vec(),
            },
            origin_worker_id: String::new(),
        }
    }

    use crate::state::fixture::spawned as spawn_event;

    /// The binding truth table (W7b finding 5): a concrete request binds as
    /// itself, a tier binds its catalog's first candidate deterministically,
    /// an unknown tier binds nothing rather than panicking a fold, and no
    /// selection is no binding.
    #[test]
    fn a_spawns_model_selection_resolves_to_its_binding() {
        use horde_proto::v1::model_selection::Selection;
        let with_selection = |selection: Selection| AgentSpec {
            model: Some(horde_proto::v1::ModelSelection {
                selection: Some(selection),
            }),
            ..AgentSpec::default()
        };

        let concrete = super::binding_for(Some(&with_selection(Selection::Concrete(
            horde_proto::v1::ConcreteModel {
                provider: "claude".to_owned(),
                model: "opus".to_owned(),
                provider_account: String::new(),
            },
        ))))
        .expect("a concrete request binds");
        assert_eq!(concrete.provider, "claude");
        assert_eq!(concrete.model, "opus");
        assert_eq!(concrete.tier, "", "a concrete bind names no tier");

        let tiered = super::binding_for(Some(&with_selection(Selection::Tier("deep".to_owned()))))
            .expect("a shipped tier binds");
        assert_eq!(tiered.tier, "deep");
        assert_eq!(
            (tiered.provider.as_str(), tiered.model.as_str()),
            ("claude", "opus"),
            "the tier's first-listed candidate, deterministically"
        );

        assert_eq!(
            super::binding_for(Some(&with_selection(Selection::Tier(
                "imaginary".to_owned()
            )))),
            None,
            "an unknown tier in an older log falls back to the worker default"
        );
        assert_eq!(super::binding_for(Some(&AgentSpec::default())), None);
        assert_eq!(super::binding_for(None), None);
    }

    fn placement_event(agent: &str, worker: Option<&str>, state: PlacementState) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::PlacementChanged(PlacementChanged {
                adapter_capabilities: Vec::new(),
                placement: Some(Placement {
                    agent_id: Some(AgentId::new(agent)),
                    state: state as i32,
                    worker_id: worker.map(WorkerId::new),
                    reason: String::new(),
                }),
            })),
        }
    }

    /// The spawn/placement pair the server writes for one placed agent.
    fn placed_agent(start: u64, session: &str, agent: &str, worker: &str) -> Vec<GlobalEvent> {
        vec![
            row(start, session, start, spawn_event(agent, None)),
            row(
                start + 1,
                session,
                start + 1,
                placement_event(agent, Some(worker), PlacementState::Placed),
            ),
        ]
    }

    /// Folds every row, returning the commands and who they are for.
    fn fold(rows: &[GlobalEvent]) -> Vec<(String, StreamCommandsResponse)> {
        let mut projection = CommandProjection::new();
        rows.iter()
            .flat_map(|global| projection.advance(global))
            .collect()
    }

    /// Archiving a session archives its placed agents where they run (fix
    /// a7) — and only its own: a sibling session's agents, and its own
    /// queued ones, take no command. Each command carries its agent's own
    /// id-suffix so one event's several commands stay several, and a later
    /// message to an archived agent folds into nothing.
    #[test]
    fn archiving_a_session_archives_its_placed_agents_and_only_those() {
        let archive = |row_at: u64, session: &str| {
            row(
                row_at,
                session,
                row_at,
                Event {
                    agent_id: None,
                    occurred_at: None,
                    idempotency_key: String::new(),
                    // Only `state` matters to the fold; the defaulted rest
                    // keeps this fixture from twinning the handler's own
                    // event construction.
                    kind: Some(Kind::SessionLifecycleChanged(
                        horde_proto::v1::SessionLifecycleChanged {
                            state: horde_proto::v1::SessionLifecycleState::Archived as i32,
                            reason: "done with it".to_owned(),
                            ..Default::default()
                        },
                    )),
                },
            )
        };

        let mut rows = placed_agent(1, "s1", "agent-a", "worker-1");
        rows.extend(placed_agent(3, "s1", "agent-b", "worker-2"));
        rows.extend(placed_agent(5, "s2", "agent-c", "worker-1"));
        // A queued agent of s1: spawned, never placed.
        rows.push(row(7, "s1", 7, spawn_event("agent-q", None)));
        rows.push(archive(8, "s1"));
        // A message for an archived agent must fold into nothing.
        rows.push(row(
            9,
            "s1",
            9,
            assignment_event("agent-a", "too late", DeliveryMode::TurnBoundary),
        ));

        let commands = fold(&rows);
        let archives: Vec<(&str, String)> = commands
            .iter()
            .filter_map(|(worker, issued)| match &issued.command {
                Some(Command::ArchiveAgent(archive)) => Some((
                    worker.as_str(),
                    archive.agent_id.as_ref().map(ToString::to_string)?,
                )),
                _ => None,
            })
            .collect();
        assert_eq!(
            archives,
            vec![
                ("worker-1", "agent-a".to_owned()),
                ("worker-2", "agent-b".to_owned()),
            ],
            "each placed agent of s1, on its own worker, and nobody else"
        );
        let ids: std::collections::BTreeSet<&str> = commands
            .iter()
            .filter(|(_, issued)| matches!(&issued.command, Some(Command::ArchiveAgent(_))))
            .map(|(_, issued)| issued.command_id.as_str())
            .collect();
        assert_eq!(ids.len(), 2, "one event, two commands, two ids");

        assert!(
            !commands.iter().any(|(_, issued)| matches!(
                &issued.command,
                Some(Command::DeliverMessage(deliver))
                    if deliver.agent_id.as_ref().map(ToString::to_string)
                        == Some("agent-a".to_owned())
            )),
            "a message after the archive has nobody to reach: {commands:?}"
        );
    }

    /// All four corners. Inline as `state == Placed && worker.is_some()` these
    /// are not separately reachable, because the server never writes a worker
    /// id without `Placed` — so an `||` passes every end-to-end test while
    /// commanding a worker that a queued placement merely mentions.
    #[test]
    fn a_placement_names_a_worker_only_when_it_is_placed_on_one() {
        let placement = |state: PlacementState, worker: Option<&str>| Placement {
            agent_id: Some(AgentId::new("a1")),
            state: state as i32,
            worker_id: worker.map(WorkerId::new),
            reason: String::new(),
        };

        assert_eq!(
            placement_worker(&placement(PlacementState::Placed, Some("w1"))),
            Some("w1".to_owned())
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Placed, None)),
            None,
            "placed nowhere is not placed"
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Queued, Some("w1"))),
            None,
            "a queued placement that still names a worker must not command it"
        );
        assert_eq!(
            placement_worker(&placement(PlacementState::Queued, None)),
            None
        );
    }

    /// Both halves, separately. With `||` a stream would hand one worker
    /// another worker's commands; with the comparison flipped it would either
    /// redeliver what was acked or skip the first command owed.
    #[test]
    fn a_command_is_owed_only_to_its_own_worker_and_only_from_the_resume_point() {
        let at = |sequence: u64| StreamCommandsResponse {
            command_sequence: sequence,
            ..StreamCommandsResponse::default()
        };
        let from = Resume::from_sequence;
        assert!(
            is_owed("w1", "w1", &at(5), from(5)),
            "the resume point is inclusive"
        );
        assert!(is_owed("w1", "w1", &at(6), from(5)));
        assert!(
            !is_owed("w1", "w1", &at(4), from(5)),
            "already applied, so not owed again"
        );
        assert!(
            !is_owed("w1", "w2", &at(5), from(5)),
            "another worker's command must never be delivered here"
        );
        assert!(!is_owed("w1", "w2", &at(4), from(5)));
    }

    /// A known position outranks the sequence, inclusively (ADR-0011): a
    /// renumbering fold may hand an applied command a *higher* sequence than
    /// the worker's watermark, or an unapplied one a lower — the position of
    /// the causing event is the only key no fold can move.
    #[test]
    fn a_known_position_outranks_the_sequence_and_includes_its_own_row() {
        let command = |sequence: u64, log_position: u64| StreamCommandsResponse {
            command_sequence: sequence,
            log_position,
            ..StreamCommandsResponse::default()
        };
        let resume = Resume {
            sequence: 10,
            log_position: 40,
        };
        assert!(
            resume.reaches(&command(3, 41)),
            "an unapplied command the new fold numbered low is still owed"
        );
        assert!(
            resume.reaches(&command(3, 40)),
            "the applied row may have caused further commands; dedupe covers the rest"
        );
        assert!(
            !resume.reaches(&command(99, 39)),
            "an applied command the new fold numbered high is not owed again"
        );
        let by_sequence = Resume::from_sequence(10);
        assert!(by_sequence.reaches(&command(10, 1)));
        assert!(!by_sequence.reaches(&command(9, 1_000)));
    }

    #[test]
    fn a_command_is_identified_by_the_log_position_that_caused_it() {
        assert_eq!(command_id("s1", 7, MAIN), "s1:7");
        assert_eq!(
            command_id("s2", 7, MAIN),
            "s2:7",
            "the same sequence in another session is a different command"
        );
        assert_eq!(
            command_id("s1", 7, CANCEL),
            "s1:7:cancel",
            "a cancel issued beside a main command needs its own identity"
        );
    }

    #[test]
    fn a_placement_on_a_worker_becomes_that_workers_spawn_command() {
        let issued = fold(&placed_agent(1, "s1", "agent-1", "worker-a"));

        assert_eq!(issued.len(), 1, "one placement, one command");
        let (worker, response) = &issued[0];
        assert_eq!(worker, "worker-a");
        assert_eq!(response.command_sequence, 1, "dense and 1-based");
        assert_eq!(
            response.command_id, "s1:2",
            "identified by the placement event that caused it"
        );

        match response.command.as_ref().expect("a command") {
            Command::SpawnAgent(spawn) => {
                assert_eq!(
                    spawn.agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned())
                );
                assert_eq!(
                    spawn.session_id.as_ref().map(ToString::to_string),
                    Some("s1".to_owned()),
                    "the session comes from the log row, not from the placement"
                );
            }
            other => panic!("expected a spawn command, got {other:?}"),
        }
    }

    /// The MCP Hub half of a spawn: `local` configs ride the command in full
    /// (the worker spawns them without a round trip), hub servers ride by
    /// name, and the per-agent-type allowlist is applied here — a scoped
    /// server reaching an agent outside its scope would be a permission
    /// widening nobody authored.
    #[test]
    fn a_spawn_command_carries_the_agents_mcp_surface_from_the_folded_catalog() {
        let mcp_config =
            |name: &str, local: bool, allowed: &[&str]| horde_proto::v1::McpServerConfig {
                name: name.to_owned(),
                transport: Some(horde_proto::v1::mcp_server_config::Transport::Stdio(
                    horde_proto::v1::McpStdioTransport {
                        command: "server-bin".to_owned(),
                        args: Vec::new(),
                        env: std::collections::HashMap::new(),
                    },
                )),
                local,
                allowed_agent_types: allowed.iter().map(|s| (*s).to_owned()).collect(),
                etag: String::new(),
            };
        let changed = |seq: u64, config: horde_proto::v1::McpServerConfig| {
            row(
                seq,
                crate::mcp::MCP_CATALOG_STREAM,
                seq,
                Event {
                    agent_id: None,
                    occurred_at: None,
                    idempotency_key: String::new(),
                    kind: Some(Kind::McpServerConfigChanged(
                        horde_proto::v1::McpServerConfigChanged {
                            config: Some(config),
                            deleted: false,
                        },
                    )),
                },
            )
        };

        let mut typed_spawn = spawn_event("agent-1", None);
        if let Some(Kind::AgentSpawned(spawned)) = typed_spawn.kind.as_mut()
            && let Some(spec) = spawned.spec.as_mut()
        {
            spec.agent_type = "reviewer".to_owned();
        }

        let issued = fold(&[
            changed(1, mcp_config("browser", true, &[])),
            changed(2, mcp_config("linear", false, &["reviewer"])),
            changed(3, mcp_config("payments", false, &["accountant"])),
            row(4, "s1", 1, typed_spawn),
            row(
                5,
                "s1",
                2,
                placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
            ),
        ]);

        assert_eq!(issued.len(), 1);
        match issued[0].1.command.as_ref().expect("a command") {
            Command::SpawnAgent(spawn) => {
                assert_eq!(
                    spawn
                        .local_mcp_servers
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>(),
                    vec!["browser"],
                    "the local config rides in full"
                );
                assert_eq!(
                    spawn.local_mcp_servers[0]
                        .transport
                        .as_ref()
                        .map(|t| matches!(
                            t,
                            horde_proto::v1::mcp_server_config::Transport::Stdio(_)
                        )),
                    Some(true),
                    "in full means the transport too, or the worker cannot spawn it"
                );
                assert_eq!(
                    spawn.visible_hub_mcp_servers,
                    vec!["linear".to_owned()],
                    "hub servers ride by name, and the allowlist holds: \
                     `payments` is scoped to accountants"
                );
            }
            other => panic!("expected a spawn command, got {other:?}"),
        }
    }

    /// The spec and parent live on `AgentSpawned` and the worker lives on
    /// `PlacementChanged`; a command needs both, so losing either half must not
    /// silently produce a command with a default in it.
    #[test]
    fn a_spawn_command_carries_what_the_spawn_event_said_to_run() {
        let rows = vec![
            row(1, "s1", 1, spawn_event("agent-2", Some("agent-1"))),
            row(
                2,
                "s1",
                2,
                placement_event("agent-2", Some("worker-a"), PlacementState::Placed),
            ),
        ];
        let issued = fold(&rows);

        match issued[0].1.command.as_ref().expect("a command") {
            Command::SpawnAgent(spawn) => {
                assert_eq!(
                    spawn.parent_agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned()),
                    "the parent comes from the spawn event"
                );
                assert!(spawn.spec.is_some(), "so does the spec");
            }
            other => panic!("expected a spawn command, got {other:?}"),
        }
    }

    #[test]
    fn a_queued_placement_commands_nobody() {
        let rows = vec![
            row(1, "s1", 1, spawn_event("agent-1", None)),
            row(
                2,
                "s1",
                2,
                placement_event("agent-1", None, PlacementState::Queued),
            ),
        ];
        assert!(
            fold(&rows).is_empty(),
            "a spawn nothing can run is not a command"
        );
    }

    /// A placement for an agent this fold never saw spawned cannot be turned
    /// into a command: half of it is missing. Inventing one with a default spec
    /// would tell a worker to run the wrong thing.
    #[test]
    fn a_placement_without_its_spawn_is_not_guessed_at() {
        let orphan = vec![row(
            1,
            "s1",
            1,
            placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
        )];
        assert!(fold(&orphan).is_empty());
    }

    /// A re-placement moves the spawn and settles the loser: the old worker is
    /// cancelled with its own command id, the new one gets the spawn, and both
    /// stay independently numbered.
    #[test]
    fn a_re_placement_cancels_the_old_worker_and_spawns_on_the_new() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            placement_event("agent-1", None, PlacementState::Queued),
        ));
        rows.push(row(
            4,
            "s1",
            4,
            placement_event("agent-1", Some("worker-b"), PlacementState::Placed),
        ));

        let issued = fold(&rows);
        let summary: Vec<(&str, &str, u64)> = issued
            .iter()
            .map(|(worker, response)| {
                let kind = match response.command.as_ref() {
                    Some(Command::SpawnAgent(_)) => "spawn",
                    Some(Command::CancelPlacement(_)) => "cancel",
                    other => panic!("unexpected command {other:?}"),
                };
                (worker.as_str(), kind, response.command_sequence)
            })
            .collect();

        assert_eq!(
            summary,
            vec![
                ("worker-a", "spawn", 1),
                ("worker-a", "cancel", 2),
                ("worker-b", "spawn", 1),
            ],
            "the queued transition cancels worker-a; the new placement spawns \
             on worker-b under its own numbering"
        );
        assert_eq!(
            issued[1].1.command_id, "s1:3:cancel",
            "the cancel carries its own identity beside the event's main id"
        );
    }

    /// A placement event that re-states the standing worker is not a new
    /// spawn: at-least-once redelivery of the *event* must not become an
    /// at-least-twice delivery of the agent.
    #[test]
    fn a_placement_restating_the_same_worker_issues_nothing() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
        ));

        let issued = fold(&rows);
        assert_eq!(issued.len(), 1, "one spawn, not two: {issued:?}");
    }

    /// An unseating with no new home still cancels: the lost worker may
    /// reconnect and replay a spawn it never applied, and the cancel is what
    /// makes that replay harmless.
    #[test]
    fn an_unseated_agent_is_cancelled_even_before_it_is_replaced() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            placement_event("agent-1", None, PlacementState::Queued),
        ));

        let issued = fold(&rows);
        assert_eq!(issued.len(), 2);
        let (worker, response) = &issued[1];
        assert_eq!(worker, "worker-a");
        assert!(matches!(
            response.command.as_ref(),
            Some(Command::CancelPlacement(_))
        ));
    }

    fn definition(id: &str) -> horde_proto::v1::WorkspaceDefinition {
        horde_proto::v1::WorkspaceDefinition {
            workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(id)),
            name: format!("{id}-name"),
            ..horde_proto::v1::WorkspaceDefinition::default()
        }
    }

    fn definition_event(id: &str, deleted: bool) -> Event {
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspaceDefinitionChanged(
                horde_proto::v1::WorkspaceDefinitionChanged {
                    definition: Some(definition(id)),
                    deleted,
                },
            )),
        }
    }

    fn prewarm_event(id: &str, worker: &str) -> Event {
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspacePrewarmRequested(
                horde_proto::v1::WorkspacePrewarmRequested {
                    workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(id)),
                    worker_id: Some(WorkerId::new(worker)),
                },
            )),
        }
    }

    /// The materialize command must carry the *full* definition, folded from
    /// the catalog event that precedes every prewarm in append order — a
    /// worker must be able to materialize without a round trip.
    #[test]
    fn a_prewarm_for_a_known_definition_commands_a_materialization() {
        let rows = vec![
            row(
                1,
                "horde-workspace-catalog",
                1,
                definition_event("ws-1", false),
            ),
            row(2, "s1", 1, prewarm_event("ws-1", "worker-a")),
        ];
        let issued = fold(&rows);

        assert_eq!(issued.len(), 1);
        let (worker, response) = &issued[0];
        assert_eq!(worker, "worker-a");
        match response.command.as_ref().expect("a command") {
            Command::MaterializeWorkspace(materialize) => {
                assert_eq!(
                    materialize
                        .definition
                        .as_ref()
                        .map(|definition| definition.name.as_str()),
                    Some("ws-1-name"),
                    "the full definition rides the command"
                );
                assert_eq!(
                    materialize.session_id.as_ref().map(ToString::to_string),
                    Some("s1".to_owned()),
                    "progress events belong to the session that asked"
                );
            }
            other => panic!("expected a materialize command, got {other:?}"),
        }
    }

    fn gc_event(request: &str, workers: &[&str], definition: Option<&str>) -> Event {
        Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspaceGcRequested(horde_proto::v1::WorkspaceGcRequested {
                request_id: request.to_owned(),
                worker_ids: workers.iter().map(|w| WorkerId::new(*w)).collect(),
                workspace_definition_id: definition.map(horde_proto::v1::WorkspaceDefinitionId::new),
                level: horde_proto::v1::GcLevel::Hard.into(),
                force: true,
                dry_run: true,
                remove_instance: true,
                include_pins: true,
                ..horde_proto::v1::WorkspaceGcRequested::default()
            })),
        }
    }

    /// A gc request commands exactly the workers it names — one command
    /// each, every field carried over, the policy resolved — under ids a
    /// refold reproduces (ADR-0014).
    #[test]
    fn a_gc_request_commands_exactly_its_workers_with_stable_ids() {
        let rows = vec![
            row(1, "horde-workspace-catalog", 1, definition_event("ws-1", false)),
            row(2, "horde-workspace-housekeeping", 1, gc_event("gc-1", &["worker-a", "worker-b"], Some("ws-1"))),
        ];
        let commands = fold(&rows);
        let workers: Vec<&str> = commands.iter().map(|(worker, _)| worker.as_str()).collect();
        assert_eq!(workers, vec!["worker-a", "worker-b"]);
        let ids: Vec<String> = commands.iter().map(|(_, c)| c.command_id.clone()).collect();
        assert_eq!(
            ids,
            vec![
                "horde-workspace-housekeeping:1:gc:worker-a",
                "horde-workspace-housekeeping:1:gc:worker-b"
            ]
        );
        let Some(Command::WorkspaceGc(gc)) = commands[0].1.command.clone() else {
            panic!("a gc command");
        };
        assert_eq!(gc.request_id, "gc-1");
        assert_eq!(gc.level, i32::from(horde_proto::v1::GcLevel::Hard));
        assert!(gc.force && gc.dry_run && gc.remove_instance && gc.include_pins);
        assert_eq!(gc.workspace_definition_id.map(|id| id.to_string()).as_deref(), Some("ws-1"));
        assert_eq!(gc.carrying_stream.map(|id| id.to_string()).as_deref(), Some("horde-workspace-housekeeping"));
        assert_eq!(
            gc.policy.and_then(|p| p.archived_grace).map(|d| d.seconds),
            Some(86_400),
            "the resolved policy rides the command"
        );
        let refolded: Vec<String> = fold(&rows).into_iter().map(|(_, c)| c.command_id).collect();
        assert_eq!(refolded, ids, "a refold issues the same ids");
        assert!(fold(&[row(1, "horde-workspace-housekeeping", 1, gc_event("gc-2", &[], None))]).is_empty());
    }

    /// A purge's commands still carry the deleted Definition's policy: the
    /// fold keeps the last Definition of a deleted id.
    #[test]
    fn a_purge_after_deletion_still_resolves_the_definitions_policy() {
        let mut overriding = definition("ws-1");
        overriding.cleanup = Some(horde_proto::v1::WorkspaceCleanupPolicy {
            idle_reclaim_after: None,
            archived_grace: Some(horde_proto::prost_types::Duration { seconds: 7, nanos: 0 }),
        });
        let changed = |deleted| Event {
            agent_id: None,
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::WorkspaceDefinitionChanged(horde_proto::v1::WorkspaceDefinitionChanged {
                definition: Some(overriding.clone()),
                deleted,
            })),
        };
        let rows = vec![
            row(1, "horde-workspace-catalog", 1, changed(false)),
            row(2, "horde-workspace-catalog", 2, changed(true)),
            row(3, "horde-workspace-housekeeping", 1, gc_event("gc-1", &["worker-a"], Some("ws-1"))),
        ];
        let commands = fold(&rows);
        let Some(Command::WorkspaceGc(gc)) = commands[0].1.command.clone() else {
            panic!("a gc command");
        };
        assert_eq!(gc.policy.and_then(|p| p.archived_grace).map(|d| d.seconds), Some(7));
    }

    /// A prewarm naming a definition the fold has not seen — or one that was
    /// deleted — yields nothing: a worker told to materialize an empty
    /// definition would create an instance of nothing.
    #[test]
    fn a_prewarm_without_a_live_definition_commands_nothing() {
        let unknown = vec![row(1, "s1", 1, prewarm_event("ws-ghost", "worker-a"))];
        assert!(fold(&unknown).is_empty());

        let deleted = vec![
            row(
                1,
                "horde-workspace-catalog",
                1,
                definition_event("ws-1", false),
            ),
            row(
                2,
                "horde-workspace-catalog",
                2,
                definition_event("ws-1", true),
            ),
            row(3, "s1", 1, prewarm_event("ws-1", "worker-a")),
        ];
        assert!(
            fold(&deleted).is_empty(),
            "a deleted definition is not materializable"
        );
    }

    #[test]
    fn events_that_command_nothing_are_folded_without_issuing() {
        let rows = vec![row(
            1,
            "s1",
            1,
            Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::UsageRecorded(
                    horde_proto::v1::UsageRecorded::default(),
                )),
            },
        )];
        assert!(fold(&rows).is_empty());
    }

    fn assignment_event(agent: &str, text: &str, delivery: DeliveryMode) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::AssignmentSent(horde_proto::v1::AssignmentSent {
                from: None,
                assignment: Some(horde_proto::v1::Assignment {
                    assignment_id: None,
                    text: text.to_owned(),
                    payload: None,
                }),
                delivery: delivery.into(),
            })),
        }
    }

    fn interrupt_event(agent: &str, reason: &str) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::TurnInterrupted(TurnInterrupted {
                requested_by: None,
                turn_id: String::new(),
                reason: reason.to_owned(),
            })),
        }
    }

    /// The steering leg: a message written to the log reaches the worker
    /// holding the agent, body and delivery mode intact. A fold that dropped
    /// the mode would downgrade every interrupt-delivery to a quiet queue.
    #[test]
    fn a_sent_assignment_is_delivered_to_the_placed_worker_mode_intact() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            assignment_event("agent-1", "stop and do this", DeliveryMode::Interrupt),
        ));

        let issued = fold(&rows);
        let (worker, response) = issued.last().expect("the message must be commanded");
        assert_eq!(worker, "worker-a");
        match response.command.as_ref() {
            Some(Command::DeliverMessage(deliver)) => {
                assert_eq!(
                    deliver.agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned())
                );
                assert_eq!(deliver.delivery, i32::from(DeliveryMode::Interrupt));
                match deliver.body.as_ref() {
                    Some(deliver_message_command::Body::Assignment(assignment)) => {
                        assert_eq!(assignment.text, "stop and do this");
                    }
                    other => panic!("expected the assignment to ride whole, got {other:?}"),
                }
            }
            other => panic!("expected a DeliverMessage command, got {other:?}"),
        }
    }

    #[test]
    fn a_sent_note_is_delivered_as_a_note_not_an_assignment() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            Event {
                agent_id: Some(AgentId::new("agent-1")),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(Kind::NoteSent(horde_proto::v1::NoteSent {
                    from: None,
                    note: Some(horde_proto::v1::Note {
                        text: "status?".to_owned(),
                        payload: None,
                    }),
                    delivery: DeliveryMode::TurnBoundary.into(),
                })),
            },
        ));

        let issued = fold(&rows);
        match issued.last().map(|(_, response)| response.command.as_ref()) {
            Some(Some(Command::DeliverMessage(deliver))) => match deliver.body.as_ref() {
                Some(deliver_message_command::Body::Note(note)) => {
                    assert_eq!(note.text, "status?");
                }
                other => panic!("a note must not arrive as an assignment: {other:?}"),
            },
            other => panic!("expected a DeliverMessage command, got {other:?}"),
        }
    }

    /// A message to an agent nobody ever spawned issues nothing: there is no
    /// worker to address and no placement that could ever owe it.
    #[test]
    fn a_message_to_an_unknown_agent_issues_no_command() {
        let rows = vec![row(
            1,
            "s1",
            1,
            assignment_event("agent-1", "hello", DeliveryMode::TurnBoundary),
        )];
        assert!(fold(&rows).is_empty());
    }

    /// The deliveries in `issued` for `worker`: (id, text, log position).
    fn deliveries_to(
        issued: &[(String, StreamCommandsResponse)],
        worker: &str,
    ) -> Vec<(String, String, u64)> {
        issued
            .iter()
            .filter(|(to, _)| to == worker)
            .filter_map(|(_, response)| match &response.command {
                Some(Command::DeliverMessage(DeliverMessageCommand {
                    body: Some(deliver_message_command::Body::Assignment(assignment)),
                    ..
                })) => Some((
                    response.command_id.clone(),
                    assignment.text.clone(),
                    response.log_position,
                )),
                _ => None,
            })
            .collect()
    }

    /// Messages to a spawned-but-unplaced agent wait in the fold and ride
    /// right behind the spawn its placement issues (ADR-0011), in send order,
    /// positioned at the placement that issued them.
    #[test]
    fn messages_before_placement_are_delivered_behind_the_spawn() {
        let rows = vec![
            row(1, "s1", 1, spawn_event("agent-1", None)),
            row(
                2,
                "s1",
                2,
                assignment_event("agent-1", "first", DeliveryMode::TurnBoundary),
            ),
            row(
                3,
                "s1",
                3,
                assignment_event("agent-1", "second", DeliveryMode::TurnBoundary),
            ),
            row(
                4,
                "s1",
                4,
                placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
            ),
        ];
        let issued = fold(&rows);
        assert!(
            matches!(
                issued.first().map(|(_, r)| &r.command),
                Some(Some(Command::SpawnAgent(_)))
            ),
            "the spawn comes first: {issued:?}"
        );
        assert_eq!(
            deliveries_to(&issued, "worker-a"),
            vec![
                ("s1:2:placed:4".to_owned(), "first".to_owned(), 4),
                ("s1:3:placed:4".to_owned(), "second".to_owned(), 4),
            ]
        );
    }

    /// A not-yet-started agent re-placed elsewhere takes its messages along —
    /// the new incarnation never saw them — including back onto a worker
    /// that applied an earlier copy, which a distinct id keeps from
    /// deduplicating away. Once seen running, nothing is replayed again.
    #[test]
    fn buffered_messages_follow_every_re_placement_until_the_agent_runs() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            assignment_event("agent-1", "hello", DeliveryMode::TurnBoundary),
        ));
        rows.push(row(
            4,
            "s1",
            4,
            placement_event("agent-1", None, PlacementState::Queued),
        ));
        rows.push(row(
            5,
            "s1",
            5,
            placement_event("agent-1", Some("worker-b"), PlacementState::Placed),
        ));
        rows.push(row(
            6,
            "s1",
            6,
            placement_event("agent-1", Some("worker-a"), PlacementState::Placed),
        ));
        let issued = fold(&rows);
        assert_eq!(
            deliveries_to(&issued, "worker-a"),
            vec![
                ("s1:3".to_owned(), "hello".to_owned(), 3),
                ("s1:3:placed:6".to_owned(), "hello".to_owned(), 6),
            ],
            "delivered live, then again to the incarnation the return spawned"
        );
        assert_eq!(
            deliveries_to(&issued, "worker-b"),
            vec![("s1:3:placed:5".to_owned(), "hello".to_owned(), 5)]
        );

        // Seen running: a later re-placement replays nothing.
        let mut running = placed_agent(1, "s1", "agent-1", "worker-a");
        running.push(row(
            3,
            "s1",
            3,
            assignment_event("agent-1", "hello", DeliveryMode::TurnBoundary),
        ));
        running.push(row(
            4,
            "s1",
            4,
            crate::state::fixture::lifecycle(
                "agent-1",
                horde_proto::v1::AgentLifecycleState::Running,
            ),
        ));
        running.push(row(
            5,
            "s1",
            5,
            placement_event("agent-1", Some("worker-b"), PlacementState::Placed),
        ));
        assert!(deliveries_to(&fold(&running), "worker-b").is_empty());
    }

    #[test]
    fn only_a_spawned_agent_not_yet_started_buffers() {
        assert!(buffers_for(true, false));
        assert!(!buffers_for(true, true));
        assert!(!buffers_for(false, false));
        assert!(!buffers_for(false, true));
    }

    /// A journal reset restarts the worker's numbering (ADR-0011), and a
    /// stream opened for the fresh journal is owed nothing from before it.
    #[tokio::test]
    async fn a_journal_reset_restarts_the_numbering_and_forgets_what_came_before() {
        let log = EventLog::in_memory().expect("in-memory log");
        let append = |session: &str, event: Event| {
            let log = log.clone();
            let session = session.to_owned();
            async move {
                log.append(PendingEvent {
                    session_id: session,
                    payload: event.encode_to_vec(),
                    ..PendingEvent::default()
                })
                .await
                .expect("append");
            }
        };
        append("s1", spawn_event("old", None)).await;
        append(
            "s1",
            placement_event("old", Some("worker-a"), PlacementState::Placed),
        )
        .await;
        append(
            "horde-worker-journal",
            Event {
                kind: Some(Kind::WorkerJournalReset(
                    horde_proto::v1::WorkerJournalReset {
                        worker_id: Some(WorkerId::new("worker-a")),
                        ..Default::default()
                    },
                )),
                ..Event::default()
            },
        )
        .await;
        append("s1", spawn_event("new", None)).await;
        append(
            "s1",
            placement_event("new", Some("worker-a"), PlacementState::Placed),
        )
        .await;

        let mut stream = CommandStream::open(&log, "worker-a", 1);
        let batch = next_within(&mut stream).await;
        let spawned: Vec<(u64, String)> = batch
            .iter()
            .filter_map(|response| match &response.command {
                Some(Command::SpawnAgent(spawn)) => Some((
                    response.command_sequence,
                    spawn.agent_id.as_ref().map(ToString::to_string)?,
                )),
                _ => None,
            })
            .collect();
        assert_eq!(spawned, vec![(1, "new".to_owned())]);
        assert_eq!(
            super::issued_to(&log, "worker-a").await.expect("fold"),
            1,
            "registration answers the restarted count"
        );
    }

    /// A stream resumed by position delivers what was caused at or after it,
    /// whatever sequence the worker claims — the claim a renumbering fold
    /// would have made a lie (ADR-0011).
    #[tokio::test]
    async fn a_stream_resumed_by_position_ignores_the_sequence() {
        let log = EventLog::in_memory().expect("in-memory log");
        for event in [
            spawn_event("first", None),
            placement_event("first", Some("worker-a"), PlacementState::Placed),
            spawn_event("second", None),
            placement_event("second", Some("worker-a"), PlacementState::Placed),
        ] {
            log.append(PendingEvent {
                session_id: "s1".to_owned(),
                payload: event.encode_to_vec(),
                ..PendingEvent::default()
            })
            .await
            .expect("append");
        }

        let mut stream = CommandStream::resuming(
            &log,
            "worker-a",
            Resume {
                sequence: 99,
                log_position: 4,
            },
        );
        let batch = next_within(&mut stream).await;
        let positions: Vec<(u64, u64)> = batch
            .iter()
            .map(|response| (response.command_sequence, response.log_position))
            .collect();
        assert_eq!(
            positions,
            vec![(2, 4)],
            "only the second spawn, caused at row 4, though its sequence is below 99"
        );
    }

    /// The interrupt verb's missing leg: the journaled request becomes a
    /// command to the worker holding the agent, reason intact.
    #[test]
    fn an_interrupt_reaches_the_placed_worker_with_its_reason() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.push(row(
            3,
            "s1",
            3,
            interrupt_event("agent-1", "the user pressed escape"),
        ));

        let issued = fold(&rows);
        let (worker, response) = issued.last().expect("the interrupt must be commanded");
        assert_eq!(worker, "worker-a");
        match response.command.as_ref() {
            Some(Command::InterruptAgent(interrupt)) => {
                assert_eq!(
                    interrupt.agent_id.as_ref().map(ToString::to_string),
                    Some("agent-1".to_owned())
                );
                assert_eq!(interrupt.reason, "the user pressed escape");
            }
            other => panic!("expected an InterruptAgent command, got {other:?}"),
        }
    }

    /// The storm's regression pin (ADR-0009): the worker acknowledges an
    /// interrupt by journaling a `TurnInterrupted` of its own, and that row
    /// must command nothing. Before the origin check, the ack folded into
    /// another `InterruptAgentCommand`, whose ack folded into another, and
    /// one `horde interrupt` grew the log by thousands of rows a second.
    #[test]
    fn a_worker_acknowledged_interrupt_commands_nothing() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        let mut ack = row(3, "s1", 3, interrupt_event("agent-1", "escape"));
        ack.origin_worker_id = "worker-a".to_owned();
        rows.push(ack);

        let issued = fold(&rows);
        assert!(
            !issued
                .iter()
                .any(|(_, response)| matches!(response.command, Some(Command::InterruptAgent(_)))),
            "an acknowledgement is not a request: {issued:?}"
        );
    }

    /// An unplaced agent has no turn in flight to abort.
    #[test]
    fn an_interrupt_for_an_unplaced_agent_issues_no_command() {
        let rows = vec![row(1, "s1", 1, interrupt_event("agent-1", "escape"))];
        assert!(fold(&rows).is_empty());
    }

    #[test]
    fn each_worker_is_numbered_independently_and_densely() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.extend(placed_agent(3, "s1", "agent-2", "worker-b"));
        rows.extend(placed_agent(5, "s2", "agent-3", "worker-a"));

        let issued = fold(&rows);
        let numbering: Vec<(&str, u64)> = issued
            .iter()
            .map(|(worker, response)| (worker.as_str(), response.command_sequence))
            .collect();

        assert_eq!(
            numbering,
            vec![("worker-a", 1), ("worker-b", 1), ("worker-a", 2)],
            "each worker counts its own commands, across sessions"
        );
    }

    /// The property the whole rewrite exists for. A restarted server rebuilds
    /// the fold from the same log, so a worker that acked command 7 before the
    /// restart must still be talking about the same command 7 afterwards. If
    /// the numbering or the ids depended on anything but the log, this fails.
    #[test]
    fn re_folding_the_same_log_reproduces_every_sequence_and_id() {
        let mut rows = placed_agent(1, "s1", "agent-1", "worker-a");
        rows.extend(placed_agent(3, "s2", "agent-2", "worker-a"));
        rows.extend(placed_agent(5, "s1", "agent-3", "worker-b"));

        let identity = |folded: Vec<(String, StreamCommandsResponse)>| {
            folded
                .into_iter()
                .map(|(worker, response)| (worker, response.command_sequence, response.command_id))
                .collect::<Vec<_>>()
        };

        let before = identity(fold(&rows));
        let after = identity(fold(&rows));

        assert_eq!(before, after, "the fold must not depend on when it runs");
        assert_eq!(before.len(), 3, "a vacuous pass here would prove nothing");
    }

    /// A payload that is not an `Event` must not stop the fold: it is skipped,
    /// or one bad row would wedge every worker's stream.
    #[test]
    fn an_undecodable_payload_is_skipped_rather_than_fatal() {
        let mut projection = CommandProjection::new();
        let garbage = GlobalEvent {
            row: 1,
            event: StoredEvent {
                session_id: "s1".to_owned(),
                sequence: 1,
                recorded_at_unix_ms: 0,
                payload: vec![0xff, 0xff, 0xff],
            },
            origin_worker_id: String::new(),
        };

        assert!(projection.advance(&garbage).is_empty());
    }

    /// The loop in `catch_up` terminates on the page it read, not on what the
    /// fold did with it. Every mutation of the fold that left a cursor alone
    /// used to turn that loop into an infinite re-read of one page — the suite
    /// hung instead of failing, and cargo-mutants recorded five timeouts rather
    /// than five kills. A log whose rows issue no commands at all is the case
    /// that distinguishes the two: it must finish, and finish empty.
    #[tokio::test]
    async fn a_log_that_commands_nothing_still_terminates_the_catch_up() {
        let log = EventLog::in_memory().expect("in-memory log");
        for index in 0..3 {
            log.append(PendingEvent {
                session_id: "s1".to_owned(),
                payload: Event {
                    agent_id: Some(AgentId::new("agent-1")),
                    occurred_at: None,
                    idempotency_key: format!("k{index}"),
                    kind: Some(Kind::TurnInterrupted(TurnInterrupted::default())),
                }
                .encode_to_vec(),
                ..PendingEvent::default()
            })
            .await
            .expect("append");
        }

        let mut stream = CommandStream::open(&log, "worker-a", 1);
        let idle =
            tokio::time::timeout(std::time::Duration::from_millis(500), stream.next_batch()).await;

        assert!(
            idle.is_err(),
            "there is no command in this log, so the stream must park rather than yield one"
        );
        assert_eq!(
            stream.parks(),
            1,
            "and it must have reached the park exactly once: a catch-up that \
             never terminates never parks at all"
        );
    }

    #[test]
    fn a_worker_with_no_commands_has_been_issued_none() {
        let projection = CommandProjection::new();
        assert_eq!(projection.issued("never-placed-anything"), 0);
    }

    // ---------------------------------------------------------------------
    // Against a real log.
    // ---------------------------------------------------------------------

    /// Appends the spawn/placement pair the server writes for a placed agent.
    async fn place(log: &EventLog, session: &str, agent: &str, worker: &str) {
        for event in [
            spawn_event(agent, None),
            placement_event(agent, Some(worker), PlacementState::Placed),
        ] {
            log.append(PendingEvent {
                session_id: session.to_owned(),
                payload: event.encode_to_vec(),
                ..PendingEvent::default()
            })
            .await
            .expect("append");
        }
    }

    /// Waits for the next batch, bounded. An unbounded wait turns a defect into
    /// a hang, and a hung suite reports nothing at all.
    async fn next_within(stream: &mut CommandStream) -> Vec<StreamCommandsResponse> {
        tokio::time::timeout(std::time::Duration::from_secs(5), stream.next_batch())
            .await
            .expect("the stream must produce a batch rather than hang")
            .expect("the log outlives the stream")
            .expect("fold")
    }

    /// A log holding one placed agent, and a stream over it from the start.
    ///
    /// The log comes back with the stream because dropping it closes the
    /// notification channel, which would end the stream rather than park it.
    async fn one_placement() -> (EventLog, CommandStream) {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        let stream = CommandStream::open(&log, "worker-a", 1);
        (log, stream)
    }

    #[tokio::test]
    async fn a_stream_replays_commands_already_in_the_log() {
        let (_log, mut stream) = one_placement().await;
        let batch = next_within(&mut stream).await;

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].command_sequence, 1);
    }

    #[tokio::test]
    async fn a_stream_delivers_only_its_own_workers_commands() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-b").await;

        let mut stream = CommandStream::open(&log, "worker-b", 1);
        let batch = next_within(&mut stream).await;

        assert_eq!(batch.len(), 1, "worker-a's command is not worker-b's");
        assert_eq!(
            batch[0].command_id, "s1:4",
            "worker-b's command was caused by the second placement"
        );
        assert_eq!(
            batch[0].command_sequence, 1,
            "and it is worker-b's first, not the log's second"
        );
    }

    #[tokio::test]
    async fn a_stream_resumes_after_what_the_worker_already_applied() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-a").await;

        let mut stream = CommandStream::open(&log, "worker-a", 2);
        let batch = next_within(&mut stream).await;

        let sequences: Vec<u64> = batch.iter().map(|c| c.command_sequence).collect();
        assert_eq!(sequences, vec![2], "command 1 was already applied");
    }

    /// The live half: a stream with nothing to do must wait, and a placement
    /// appended afterwards must reach it.
    #[tokio::test]
    async fn a_waiting_stream_is_woken_by_a_placement() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);

        let writer = log.clone();
        tokio::spawn(async move {
            place(&writer, "s1", "agent-1", "worker-a").await;
        });

        let batch = next_within(&mut stream).await;
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].command_sequence, 1);
    }

    /// The liveness assertion. A stream with nothing to deliver must *park*,
    /// not spin: a hot loop delivers the right commands and passes every
    /// functional test above while burning a core, and a mutation of the
    /// waiting path shows up as a suite that hangs rather than one that fails.
    /// Parking is counted so the difference is a number a test can read.
    #[tokio::test]
    async fn an_idle_stream_parks_instead_of_spinning() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);
        assert_eq!(stream.parks(), 0, "nothing has waited yet");

        // Nothing will ever arrive, so this must time out *while parked*.
        let idle =
            tokio::time::timeout(std::time::Duration::from_millis(200), stream.next_batch()).await;
        assert!(idle.is_err(), "there is no command, so nothing may arrive");

        assert_eq!(
            stream.parks(),
            1,
            "an idle stream parks exactly once: 0 means it never waited, and a \
             spin drives this into the thousands"
        );
    }

    /// An append that commands another worker must not wake this one into
    /// delivering something that is not its own.
    #[tokio::test]
    async fn an_unrelated_append_does_not_produce_a_command() {
        let log = EventLog::in_memory().expect("in-memory log");
        let mut stream = CommandStream::open(&log, "worker-a", 1);

        place(&log, "s1", "agent-1", "worker-b").await;
        place(&log, "s1", "agent-2", "worker-a").await;

        let batch = next_within(&mut stream).await;
        assert_eq!(batch.len(), 1, "only worker-a's own command");
        assert_eq!(
            batch[0].command_sequence, 1,
            "worker-a numbers from its own first command"
        );
    }

    /// `Debug` on a stream is what an operator reads when one appears stuck,
    /// and the two facts that diagnose it are which worker it belongs to and
    /// how far through the log it has folded. An empty view answers neither —
    /// the same reason `EventLog` and `ServerCore` assert on theirs.
    #[tokio::test]
    async fn the_debug_view_names_the_worker_and_how_far_it_has_read() {
        let (_log, mut stream) = one_placement().await;
        let fresh = format!("{stream:?}");
        assert!(fresh.contains("CommandStream"), "got {fresh}");
        assert!(
            fresh.contains("worker-a"),
            "an operator cannot act on a stream they cannot identify: {fresh}"
        );
        assert!(
            fresh.contains("row: 0"),
            "a stream that has read nothing must say so: {fresh}"
        );

        next_within(&mut stream).await;
        let read = format!("{stream:?}");
        assert!(
            read.contains("row: 2"),
            "the cursor is the whole diagnosis of a stuck stream: {read}"
        );
    }

    #[tokio::test]
    async fn what_a_worker_has_been_issued_is_folded_from_the_log() {
        let log = EventLog::in_memory().expect("in-memory log");
        assert_eq!(
            issued_to(&log, "worker-a").await.expect("fold"),
            0,
            "a worker nothing was placed on has been issued nothing"
        );

        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s1", "agent-2", "worker-a").await;
        place(&log, "s1", "agent-3", "worker-b").await;

        assert_eq!(issued_to(&log, "worker-a").await.expect("fold"), 2);
        assert_eq!(issued_to(&log, "worker-b").await.expect("fold"), 1);
    }

    /// A restart is a new fold over the same log. The commands a fresh stream
    /// produces must be identical to what the previous one produced, or a
    /// worker's ack points at a different command than it did before.
    #[tokio::test]
    async fn a_second_stream_over_the_same_log_produces_identical_commands() {
        let log = EventLog::in_memory().expect("in-memory log");
        place(&log, "s1", "agent-1", "worker-a").await;
        place(&log, "s2", "agent-2", "worker-a").await;

        let mut before = CommandStream::open(&log, "worker-a", 1);
        let first = next_within(&mut before).await;

        // Nothing of the first stream survives; only the log does.
        drop(before);
        let mut after = CommandStream::open(&log, "worker-a", 1);
        let second = next_within(&mut after).await;

        assert_eq!(first, second, "the log is the only state there is");
        assert_eq!(first.len(), 2);
    }

    fn decision_event(agent: &str, decision: horde_proto::v1::PermissionDecision) -> Event {
        Event {
            agent_id: Some(AgentId::new(agent)),
            occurred_at: None,
            idempotency_key: String::new(),
            kind: Some(Kind::PermissionDecided(
                horde_proto::v1::PermissionDecided {
                    decision: Some(decision),
                },
            )),
        }
    }

    fn allow(
        updated_input: Option<horde_proto::prost_types::Struct>,
    ) -> horde_proto::v1::PermissionDecision {
        horde_proto::v1::PermissionDecision {
            request_id: Some(horde_proto::v1::PermissionRequestId::new("req-1")),
            outcome: horde_proto::v1::PermissionOutcome::Allow as i32,
            decided_by: horde_proto::v1::PermissionLayer::Human as i32,
            decided_by_actor: None,
            reason: String::new(),
            remember_for_session: false,
            updated_input,
        }
    }

    /// Without this, a human's answer lands in the log and the agent blocked on
    /// it waits forever — the decision is durable and invisible.
    #[test]
    fn a_permission_decision_becomes_a_command_for_the_agents_worker() {
        let mut projection = CommandProjection::new();
        for global in placed_agent(1, "s1", "agent-1", "worker-a") {
            projection.advance(&global);
        }

        let mut issued =
            projection.advance(&row(3, "s1", 3, decision_event("agent-1", allow(None))));

        assert_eq!(issued.len(), 1, "one decision, one command");
        let (worker, response) = issued.remove(0);
        assert_eq!(
            worker, "worker-a",
            "the decision must reach the worker the agent runs on"
        );
        match response.command {
            Some(Command::DeliverPermissionDecision(command)) => {
                assert_eq!(
                    command
                        .agent_id
                        .as_ref()
                        .map(ToString::to_string)
                        .as_deref(),
                    Some("agent-1")
                );
            }
            other => panic!("expected a permission-decision command, got {other:?}"),
        }
    }

    /// The narrowing is the whole point of `updated_input`: a decision that
    /// clamped a path is worth nothing to the worker if the clamp is left
    /// behind in the projection.
    #[test]
    fn a_narrowing_survives_into_the_command() {
        let mut projection = CommandProjection::new();
        for global in placed_agent(1, "s1", "agent-1", "worker-a") {
            projection.advance(&global);
        }

        let mut narrowed = horde_proto::prost_types::Struct::default();
        narrowed.fields.insert(
            "path".to_owned(),
            horde_proto::prost_types::Value {
                kind: Some(horde_proto::prost_types::value::Kind::StringValue(
                    "/workspace/atlas".to_owned(),
                )),
            },
        );

        let mut issued = projection.advance(&row(
            3,
            "s1",
            3,
            decision_event("agent-1", allow(Some(narrowed.clone()))),
        ));

        assert_eq!(
            issued.len(),
            1,
            "a decision for a placed agent is a command"
        );
        let (_, response) = issued.remove(0);
        match response.command {
            Some(Command::DeliverPermissionDecision(command)) => {
                let decision = command.decision.expect("the command carries the decision");
                assert_eq!(
                    decision.updated_input,
                    Some(narrowed),
                    "dropping the replacement arguments here would run the \
                     original, unnarrowed call"
                );
            }
            other => panic!("expected a permission-decision command, got {other:?}"),
        }
    }

    /// An agent whose placement this fold never saw cannot be addressed. No
    /// command is better than a command sent to the wrong worker.
    #[test]
    fn a_decision_for_an_unplaced_agent_yields_no_command() {
        let mut projection = CommandProjection::new();

        assert!(
            projection
                .advance(&row(1, "s1", 1, decision_event("ghost", allow(None))))
                .is_empty()
        );
    }

    /// Placement is recorded even when the spawn was never folded — a replay
    /// starting mid-log sees the placement and not the spawn, and losing it
    /// would leave every later command for that agent unroutable.
    #[test]
    fn placement_is_remembered_even_without_the_spawn_that_preceded_it() {
        let mut projection = CommandProjection::new();

        // Placement alone produces no spawn command, because the spec is not
        // known — but it must still be remembered.
        assert!(
            projection
                .advance(&row(
                    1,
                    "s1",
                    1,
                    placement_event("agent-1", Some("worker-a"), PlacementState::Placed)
                ))
                .is_empty()
        );

        let issued = projection.advance(&row(2, "s1", 2, decision_event("agent-1", allow(None))));
        assert_eq!(
            issued.first().map(|(worker, _)| worker.as_str()),
            Some("worker-a"),
            "the placement must survive a fold that never saw the spawn"
        );
    }
}
