//! One lane: the commands of one agent — or one workspace — in order.
//!
//! The dispatcher accepts each command into the inbox and hands it to the
//! lane its target names. A lane runs its commands strictly in arrival order,
//! so an agent's spawn, the messages buffered behind it, an interrupt and an
//! archive apply exactly as the Server folded them — while a lane busy with a
//! slow spawn (a clone, a harness start, a hub listing) holds up nobody else.
//!
//! A lane owns its agent outright: the pump's control channel, the pump task,
//! and the tasks retiring earlier incarnations. Running a command is
//! *execute* (decide effects, start nothing), then *complete* (one journal
//! transaction: effects, bookkeeping, and the inbox row's fate), then
//! *follow up* (start the pump, forward control) — so nothing acts on a
//! command whose completion a crash could still undo.
//!
//! # A bad command is that command's failure
//!
//! A command that cannot be applied — a decision missing its request id, a
//! narrowing the adapter cannot apply, a journal read that failed — is
//! journaled against its agent as a harness diagnostic and completes. It
//! never reaches the connection loop: one malformed command used to stop the
//! whole Worker, and every other agent with it.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use horde_adapter::{AgentSession, PermissionDecision, ToolCallId, TurnInput};
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    AgentLifecycleState, ArchiveAgentCommand, DeliverMessageCommand,
    DeliverPermissionDecisionCommand, InterruptAgentCommand, MaterializeWorkspaceCommand,
    SpawnAgentCommand, StreamCommandsResponse, TurnInterrupted, WorkspaceInstanceChanged,
    WorkspaceInstanceReport, WorkspaceInstanceState, event::Kind,
};
use horde_wire::permission_decision_from_wire;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use super::pump::{AgentControl, PumpExit, pump};
use super::shared::{
    Shared, carried_payload, context_of, diagnostic_entry, lifecycle_entry, message_input,
    required_id,
};
use crate::error::WorkerError;
use crate::inbox::InboxDisposition;
use crate::journal::{AgentBookkeeping, JournalEntry, RevivableAgent};
use crate::translate::AgentContext;

/// How many control messages may queue for one agent's pump.
const CONTROL_CAPACITY: usize = 8;

/// What the dispatcher hands a lane.
pub(super) enum LaneMessage {
    /// A command to run, after `after` resolves when it is set — the fence
    /// a spawn waits on so a materialize dispatched before it finishes first.
    Command {
        command: StreamCommandsResponse,
        after: Option<oneshot::Receiver<()>>,
    },
    /// Answer once everything sent to this lane before has run.
    Fence(oneshot::Sender<()>),
}

/// The dispatcher's half of a lane.
pub(super) struct LaneHandle {
    pub(super) sender: mpsc::UnboundedSender<LaneMessage>,
    /// Resolves with how many pumps and retiring tasks the lane reaped.
    pub(super) task: JoinHandle<usize>,
}

/// The facts every lane reports back to the Runtime.
#[derive(Clone)]
pub(super) struct Ledger {
    /// Agents with a pump, for `running_agents` and the debug view.
    pub(super) running: Arc<Mutex<BTreeSet<String>>>,
    /// Commands dispatched and not yet completed.
    pub(super) in_flight: Arc<watch::Sender<usize>>,
}

impl Ledger {
    pub(super) fn new() -> Self {
        Self {
            running: Arc::new(Mutex::new(BTreeSet::new())),
            in_flight: Arc::new(watch::Sender::new(0)),
        }
    }

    pub(super) fn running(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn dispatched(&self) {
        self.in_flight.send_modify(|count| *count += 1);
    }

    fn completed(&self) {
        self.in_flight
            .send_modify(|count| *count = count.saturating_sub(1));
    }
}

/// Starts a lane for `key`. A `predecessor` — the same key's lane that
/// retired itself while this one was being needed — is joined before any
/// command runs, so two lanes never run one agent's commands at once.
pub(super) fn spawn_lane(
    key: String,
    shared: Shared,
    ledger: Ledger,
    predecessor: Option<JoinHandle<usize>>,
) -> LaneHandle {
    let (sender, receiver) = mpsc::unbounded_channel();
    let lane = Lane {
        key,
        shared,
        ledger,
        agent: None,
        retired: Vec::new(),
    };
    LaneHandle {
        sender,
        task: tokio::spawn(lane.run(receiver, predecessor)),
    }
}

/// The lane an incoming command belongs to.
pub(super) fn lane_key(command: &StreamCommandsResponse) -> String {
    let agent = |id: Option<&horde_proto::v1::AgentId>| id.map(ToString::to_string);
    let key = match command.command.as_ref() {
        Some(Command::SpawnAgent(spawn)) => agent(spawn.agent_id.as_ref()),
        Some(Command::DeliverMessage(deliver)) => agent(deliver.agent_id.as_ref()),
        Some(Command::InterruptAgent(interrupt)) => agent(interrupt.agent_id.as_ref()),
        Some(Command::ArchiveAgent(archive)) => agent(archive.agent_id.as_ref()),
        Some(Command::CancelPlacement(cancel)) => agent(cancel.agent_id.as_ref()),
        Some(Command::DeliverPermissionDecision(decision)) => agent(decision.agent_id.as_ref()),
        Some(Command::MaterializeWorkspace(materialize)) => Some(workspace_lane(
            &materialize
                .definition
                .as_ref()
                .and_then(|definition| definition.workspace_definition_id.as_ref())
                .map(ToString::to_string)
                .unwrap_or_default(),
        )),
        None => None,
    };
    key.unwrap_or_default()
}

/// The lane materializations of one workspace definition run on.
pub(super) fn workspace_lane(definition_id: &str) -> String {
    format!("workspace:{definition_id}")
}

/// One running agent, from its lane's side.
struct LiveAgent {
    control: mpsc::Sender<AgentControl>,
    /// The Session whose log this agent's command effects belong in.
    session_id: String,
    /// The adapter's `RewritesToolInput` claim, needed to convert incoming
    /// permission decisions — a narrowing decision must be refused, not
    /// silently widened, when the adapter cannot apply it.
    can_rewrite: bool,
    /// `None` once the pump has ended and its exit was handled.
    pump: Option<JoinHandle<PumpExit>>,
}

struct Lane {
    key: String,
    shared: Shared,
    ledger: Ledger,
    agent: Option<LiveAgent>,
    /// Earlier incarnations' pumps and checkout releases, kept so shutdown
    /// can wait for them.
    retired: Vec<JoinHandle<()>>,
}

/// What executing a command decided, before anything is journaled.
struct Applied {
    effects: Vec<JournalEntry>,
    /// Revival bookkeeping that must commit with the command.
    bookkeeping: AgentBookkeeping,
    inbox: InboxDisposition,
    then: AfterCommit,
}

impl Applied {
    /// A command that applied without effects or follow-up — the race case,
    /// where the agent a command names is already gone.
    fn nothing() -> Self {
        Self::effects(Vec::new())
    }

    fn effects(effects: Vec<JournalEntry>) -> Self {
        Self {
            effects,
            bookkeeping: AgentBookkeeping::None,
            inbox: InboxDisposition::Settle,
            then: AfterCommit::Nothing,
        }
    }
}

/// Work that must wait for a command's completion to commit.
enum AfterCommit {
    Nothing,
    Start {
        agent_id: String,
        session: Box<dyn AgentSession>,
        context: AgentContext,
        can_rewrite: bool,
        /// Delivered in order once the pump is up.
        inputs: Vec<(TurnInput, Option<String>)>,
    },
    Control(AgentControl),
    Retire {
        agent_id: String,
    },
    /// An agent ended with no pump: give back what it still holds.
    Release {
        agent_id: String,
        checkout: Option<std::path::PathBuf>,
    },
}

/// What one lane iteration is about to handle.
enum LaneStep {
    Exited(PumpExit),
    Message(Option<LaneMessage>),
}

impl Lane {
    async fn run(
        mut self,
        mut receiver: mpsc::UnboundedReceiver<LaneMessage>,
        predecessor: Option<JoinHandle<usize>>,
    ) -> usize {
        let mut reaped = match predecessor {
            Some(predecessor) => predecessor.await.unwrap_or_default(),
            None => 0,
        };
        loop {
            let step = tokio::select! {
                biased;
                exit = pump_exit(&mut self.agent) => LaneStep::Exited(exit),
                message = receiver.recv() => LaneStep::Message(message),
            };
            match step {
                LaneStep::Exited(exit) => self.on_pump_exit(exit).await,
                LaneStep::Message(Some(message)) => {
                    let forgotten = self.handle(message).await;
                    if forgotten && self.agent.is_none() && receiver.is_empty() {
                        // An ended agent's lane retires rather than parking
                        // for the Worker's whole life. Closing first makes
                        // the dispatcher's next send fail (it then starts a
                        // successor that joins this one), and anything that
                        // raced in ahead of the close still runs here.
                        receiver.close();
                        while let Ok(message) = receiver.try_recv() {
                            self.handle(message).await;
                        }
                        break;
                    }
                }
                LaneStep::Message(None) => break,
            }
        }
        reaped += self.close().await;
        reaped
    }

    /// Handles one message, reporting whether it forgot the lane's agent.
    async fn handle(&mut self, message: LaneMessage) -> bool {
        match message {
            LaneMessage::Fence(done) => {
                let _ = done.send(());
                false
            }
            LaneMessage::Command { command, after } => {
                if let Some(after) = after {
                    let _ = after.await;
                }
                let forgotten = self.run_command(command).await;
                self.ledger.completed();
                forgotten
            }
        }
    }

    async fn run_command(&mut self, command: StreamCommandsResponse) -> bool {
        let command_id = command.command_id;
        let applied = match command.command {
            Some(kind) => self.execute(kind, &command_id).await,
            None => Err(WorkerError::Incomplete("command")),
        };
        let applied = match applied {
            Ok(applied) => applied,
            Err(error) => self.refused(&command_id, &error).await,
        };
        let forgotten = matches!(applied.bookkeeping, AgentBookkeeping::Forget { .. });
        self.commit(Some(command_id), applied).await;
        forgotten
    }

    /// Completes a command in the journal, then follows up. A completion
    /// that cannot commit follows up with nothing: its row stays in the
    /// inbox and replays at the next start, and a harness it would have
    /// started dies with the dropped session.
    async fn commit(&mut self, command_id: Option<String>, applied: Applied) {
        let completed = self
            .shared
            .journal
            .complete_command(
                command_id,
                applied.effects,
                applied.bookkeeping,
                applied.inbox,
            )
            .await;
        match completed {
            Ok(()) => self.after_commit(applied.then).await,
            Err(error) => {
                tracing::error!(lane = %self.key, %error, "a command's completion could not be journaled; it replays at the next start");
            }
        }
    }

    /// The applied form of a command that could not be applied: a
    /// diagnostic against its agent, when this lane knows the agent's
    /// Session, and a completed row either way.
    async fn refused(&self, command_id: &str, error: &WorkerError) -> Applied {
        tracing::warn!(lane = %self.key, command_id, %error, "a command could not be applied");
        let text = format!("horde-worker could not apply command {command_id}: {error}");
        match self.session_of_lane().await {
            Some(session_id) => {
                Applied::effects(vec![diagnostic_entry(&session_id, &self.key, text)])
            }
            None => Applied::nothing(),
        }
    }

    /// The Session this lane's agent belongs to: its live pump's, or its
    /// remembered spawn's.
    async fn session_of_lane(&self) -> Option<String> {
        if let Some(agent) = &self.agent {
            return Some(agent.session_id.clone());
        }
        match self.shared.journal.revivable_agent(&self.key).await {
            Ok(record) => record.map(|record| record.session_id),
            Err(_) => None,
        }
    }

    /// Executes one command kind, deciding its effects and follow-up.
    async fn execute(
        &mut self,
        command: Command,
        command_id: &str,
    ) -> Result<Applied, WorkerError> {
        match command {
            Command::SpawnAgent(spawn) => self.spawn_agent(spawn).await,
            Command::DeliverMessage(deliver) => self.deliver_message(deliver, command_id).await,
            Command::InterruptAgent(interrupt) => Ok(self.interrupt_agent(&interrupt)),
            Command::ArchiveAgent(archive) => Ok(self.archive_agent(&archive).await),
            Command::CancelPlacement(cancel) => {
                let agent_id = cancel
                    .agent_id
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default();
                // A placement cancelled before its spawn arrived names an
                // agent this Worker never started; one cancelled after is an
                // archive in different clothes. PlacementChanged events are
                // Server-authored, so there is nothing to journal either way
                // — but the agent no longer runs here, so this Worker must
                // not keep the power to revive it.
                Ok(self.end_agent(agent_id, None).await)
            }
            Command::DeliverPermissionDecision(decision) => {
                self.deliver_permission_decision(decision)
            }
            Command::MaterializeWorkspace(materialize) => {
                self.materialize_workspace(materialize).await
            }
        }
    }

    /// Creates or repairs an Instance and journals what is now on disk.
    ///
    /// A Worker with no workspace manager never advertised support, so this
    /// means the handshake promised something it cannot honour: reported as
    /// a `DEGRADED` instance naming why, so the Server stops preferring this
    /// Worker — not a stopped Worker, and not an ack of work that never
    /// happened.
    async fn materialize_workspace(
        &self,
        materialize: MaterializeWorkspaceCommand,
    ) -> Result<Applied, WorkerError> {
        let session_id = required_id(materialize.session_id.as_ref(), "session id")?;
        let definition = materialize
            .definition
            .ok_or(WorkerError::Incomplete("workspace definition"))?;
        let report = match &self.shared.workspaces {
            Some(workspaces) => workspaces.materialize(&definition).await,
            None => WorkspaceInstanceReport {
                workspace_definition_id: definition.workspace_definition_id,
                state: WorkspaceInstanceState::Degraded.into(),
                detail: WorkerError::UnsupportedCommand("materialize workspace").to_string(),
                ..WorkspaceInstanceReport::default()
            },
        };
        Ok(Applied::effects(vec![JournalEntry {
            session_id,
            agent_id: None,
            idempotency_key: String::new(),
            kind: Kind::WorkspaceInstanceChanged(WorkspaceInstanceChanged {
                report: Some(report),
                progress: None,
            }),
        }]))
    }

    async fn spawn_agent(&mut self, spawn: SpawnAgentCommand) -> Result<Applied, WorkerError> {
        let agent_id = required_id(spawn.agent_id.as_ref(), "agent id")?;
        let session_id = required_id(spawn.session_id.as_ref(), "session id")?;
        let context = context_of(&spawn, &session_id, &agent_id);

        // A spawn that cannot start is the *agent* failing: the command still
        // completes, with the failure as its journaled effect.
        let checkout = match self.shared.agent_checkout(&spawn).await {
            Ok(checkout) => checkout,
            Err(error) => return Ok(spawn_failed(&context, &agent_id, &error)),
        };
        let cwd = checkout.as_ref().map(|checkout| checkout.root.clone());
        // The endpoint before the harness: an agent launched without its
        // verbs and permission prompt tool is not degraded, it is a different
        // (and unpermissioned) product — so a bind failure fails the spawn.
        // Its MCP surface rides the same rule.
        let started = match self
            .shared
            .agent_endpoint(&session_id, &agent_id, &spawn)
            .await
        {
            Ok(endpoint) => self.shared.start_session(&spawn, cwd, endpoint).await,
            Err(error) => Err(error),
        };
        let (session, can_rewrite) = match started {
            Ok(started) => started,
            Err(error) => {
                self.shared
                    .abandon_spawn(&agent_id, checkout.as_ref())
                    .await;
                return Ok(spawn_failed(&context, &agent_id, &error));
            }
        };
        // Registered only once the harness actually started: a checkout with
        // no agent behind it must not become a peer-fetch source or a
        // stamping target.
        if let Some(checkout) = checkout {
            self.shared.broker.register(&agent_id, checkout);
        }

        let inputs = spawn
            .spec
            .as_ref()
            .and_then(|spec| spec.initial_assignment.as_ref())
            .map(|assignment| assignment.text.clone())
            .filter(|text| !text.is_empty())
            .map(|text| (TurnInput::queued(text), None))
            .into_iter()
            .collect();

        Ok(Applied {
            effects: vec![lifecycle_entry(
                &context,
                format!("{agent_id}:starting"),
                AgentLifecycleState::Queued,
                AgentLifecycleState::Starting,
                "placed on this worker".to_owned(),
            )],
            // Remembered in the same transaction as the completion: a spawn
            // this Worker completed but cannot rebuild is an agent the Server
            // believes in and nobody can ever revive.
            bookkeeping: AgentBookkeeping::Remember {
                agent_id: agent_id.clone(),
                session_id,
                spawn: Box::new(spawn),
            },
            inbox: InboxDisposition::Settle,
            then: AfterCommit::Start {
                agent_id,
                session,
                context,
                can_rewrite,
                inputs,
            },
        })
    }

    /// Whether a pump is actually alive to take a delivery.
    ///
    /// Presence is not enough: a pump whose harness died has ended, but its
    /// handle lingers until something replaces it. A missing agent and a
    /// closed channel are each an agent nobody is running, and each is a
    /// revival case, never a message to drop.
    fn has_live_pump(&self) -> bool {
        self.agent
            .as_ref()
            .is_some_and(|agent| !agent.control.is_closed())
    }

    async fn deliver_message(
        &mut self,
        deliver: DeliverMessageCommand,
        command_id: &str,
    ) -> Result<Applied, WorkerError> {
        let agent_id = required_id(deliver.agent_id.as_ref(), "agent id")?;
        let Some((input, payload)) = message_input(deliver) else {
            tracing::warn!(
                agent_id,
                "a deliver command carried no message; applied empty"
            );
            return Ok(Applied::nothing());
        };

        if self.has_live_pump() {
            // A Payload's commits and attachments are made present for the
            // receiving agent *before* the message text reaches it — the
            // message names work, and handing the agent a name it cannot
            // open would be the transport becoming the agent's concern.
            let session_id = self.session_of_lane().await.unwrap_or_default();
            if let Some(payload) = carried_payload(payload.as_ref())
                && let Err(error) = self.shared.obtain_payload(&agent_id, payload).await
            {
                return Ok(payload_failed(&session_id, &agent_id, &error));
            }
            // No journal effect: the Server wrote AssignmentSent/NoteSent when
            // the message happened. The row stays, handed, until the pump
            // has actually written the text to the harness.
            return Ok(Applied {
                effects: Vec::new(),
                bookkeeping: AgentBookkeeping::None,
                inbox: InboxDisposition::Hand,
                then: AfterCommit::Control(AgentControl::Deliver {
                    input,
                    command_id: Some(command_id.to_owned()),
                }),
            });
        }

        // Nobody is running the agent — a dead harness, a retired pump, or a
        // Worker that restarted. Sending an agent another message is what
        // revives it (DESIGN.md §5), and the journal remembers how.
        self.revive(
            &agent_id,
            Some(Pending {
                input,
                payload,
                command_id: command_id.to_owned(),
            }),
        )
        .await
    }

    /// Revives a remembered agent to deliver what it was handed and never
    /// sent, then `current` — or journals why it cannot.
    async fn revive(
        &mut self,
        agent_id: &str,
        current: Option<Pending>,
    ) -> Result<Applied, WorkerError> {
        let Some(record) = self.shared.journal.revivable_agent(agent_id).await? else {
            // Never spawned here (or long archived): the race case, applied
            // with nobody to tell — the causing event is already in the log.
            tracing::warn!(
                agent_id,
                "a message for an agent this worker neither runs nor remembers"
            );
            return Ok(Applied::nothing());
        };
        let mut inputs = self.handed_inputs(agent_id).await?;
        if inputs.is_empty() && current.is_none() {
            return Ok(Applied::nothing());
        }

        let context = context_of(&record.spawn, &record.session_id, agent_id);
        if record.provider_session_id.is_none() {
            // The harness died before ever naming its conversation, so there
            // is nothing to resume — said loudly, or the sender reads an
            // unreachable agent as one that is merely slow.
            return Ok(revival_failed(
                &context,
                "cannot revive: no provider session was ever established".to_owned(),
            ));
        }

        match self.shared.resume_session(&record).await {
            Ok((session, can_rewrite, checkout)) => {
                // Re-registered before the payload is obtained: the broker
                // routes the obtain through the receiver's own checkout.
                if let Some(checkout) = checkout {
                    self.shared.broker.register(agent_id, checkout);
                }
                if let Some(Pending {
                    payload: Some(payload),
                    ..
                }) = &current
                    && let Some(payload) = carried_payload(Some(payload))
                    && let Err(error) = self.shared.obtain_payload(agent_id, payload).await
                {
                    // The revived checkout must not linger as a stamping
                    // target for an agent whose pump never started, nor its
                    // fresh endpoint token stay valid with no harness behind it.
                    self.shared.broker.forget(agent_id);
                    self.shared.forget_endpoint(agent_id).await;
                    return Ok(payload_failed(&record.session_id, agent_id, &error));
                }
                inputs.extend(current.map(|pending| (pending.input, Some(pending.command_id))));
                Ok(Applied {
                    effects: vec![lifecycle_entry(
                        &context,
                        // Revivals can repeat; the journal mints each key.
                        String::new(),
                        // The Worker does not track how the last incarnation
                        // ended; claiming one would be invention.
                        AgentLifecycleState::Unspecified,
                        AgentLifecycleState::Starting,
                        "revived to deliver a message".to_owned(),
                    )],
                    bookkeeping: AgentBookkeeping::None,
                    inbox: InboxDisposition::Hand,
                    then: AfterCommit::Start {
                        agent_id: agent_id.to_owned(),
                        session,
                        context,
                        can_rewrite,
                        inputs,
                    },
                })
            }
            Err(error) => {
                // Whatever the failed revival minted — a token, `local`
                // children — must not outlive it.
                self.shared.forget_endpoint(agent_id).await;
                Ok(revival_failed(&context, error.to_string()))
            }
        }
    }

    /// The messages this agent was handed and never sent, as turn-boundary
    /// inputs in their original order. Replayed into a fresh incarnation,
    /// an interrupt has no turn to cut, and a second replayed message must
    /// not cut the first one's.
    async fn handed_inputs(
        &self,
        agent_id: &str,
    ) -> Result<Vec<(TurnInput, Option<String>)>, WorkerError> {
        let handed = self.shared.journal.handed_deliveries(agent_id).await?;
        Ok(handed
            .into_iter()
            .filter_map(|command| {
                let Some(Command::DeliverMessage(deliver)) = command.command else {
                    return None;
                };
                let (input, _payload) = message_input(deliver)?;
                Some((TurnInput::queued(input.text), Some(command.command_id)))
            })
            .collect())
    }

    /// A pump ended. One that found its harness dead between turns had
    /// messages it could not send; the agent is revived to deliver them.
    async fn on_pump_exit(&mut self, exit: PumpExit) {
        if exit != PumpExit::HarnessGone {
            return;
        }
        let agent_id = self.key.clone();
        let applied = match self.revive(&agent_id, None).await {
            Ok(applied) => applied,
            Err(error) => self.refused("(revival)", &error).await,
        };
        self.commit(None, applied).await;
    }

    fn interrupt_agent(&self, interrupt: &InterruptAgentCommand) -> Applied {
        let Some(agent) = self.named_agent(interrupt.agent_id.as_ref()) else {
            return Applied::nothing();
        };
        Applied {
            effects: vec![JournalEntry {
                session_id: agent.session_id.clone(),
                agent_id: Some(self.key.clone()),
                // Interrupts can repeat; the journal mints the key.
                idempotency_key: String::new(),
                kind: Kind::TurnInterrupted(TurnInterrupted {
                    requested_by: None,
                    turn_id: String::new(),
                    reason: interrupt.reason.clone(),
                }),
            }],
            bookkeeping: AgentBookkeeping::None,
            inbox: InboxDisposition::Settle,
            then: AfterCommit::Control(AgentControl::Interrupt),
        }
    }

    /// This lane's agent, if a command names it and it is here.
    ///
    /// `None` is a race, not a bug: the Server sent the command before it
    /// saw the agent end. The command still applies — the causing event is
    /// already in the log — there is just nobody left to forward it to.
    fn named_agent(&self, agent_id: Option<&horde_proto::v1::AgentId>) -> Option<&LiveAgent> {
        let named = agent_id.is_some_and(|id| id.to_string() == self.key);
        let agent = self.agent.as_ref().filter(|_| named);
        if agent.is_none() {
            tracing::warn!(lane = %self.key, "command for an agent this worker is not running");
        }
        agent
    }

    async fn archive_agent(&mut self, archive: &ArchiveAgentCommand) -> Applied {
        let agent_id = archive
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let archived = self.agent.as_ref().map(|agent| JournalEntry {
            session_id: agent.session_id.clone(),
            agent_id: Some(agent_id.clone()),
            idempotency_key: format!("{agent_id}:archived"),
            kind: Kind::AgentLifecycleChanged(horde_proto::v1::AgentLifecycleChanged {
                // The Worker does not track idle-vs-running; claiming a
                // previous state it never observed would be invention.
                previous_state: AgentLifecycleState::Unspecified.into(),
                state: AgentLifecycleState::Archived.into(),
                reason: archive.reason.clone(),
            }),
        });
        self.end_agent(agent_id, archived).await
    }

    /// Ends this lane's agent: forgotten whether or not it is still running
    /// here, because context dies at archive and a revivable-but-idle agent
    /// must stop being revivable even with no pump left to tell. A running
    /// agent is retired (its pump stopped, then its checkout released); one
    /// with no pump still gives back its endpoint and checkout (ADR-0010).
    async fn end_agent(&mut self, agent_id: String, live_effect: Option<JournalEntry>) -> Applied {
        let (effects, then) = if self.agent.is_some() {
            (
                live_effect.into_iter().collect(),
                AfterCommit::Retire {
                    agent_id: agent_id.clone(),
                },
            )
        } else {
            // An unreadable record costs only the checkout release (disk),
            // never the forget: an archived agent must stop being revivable.
            let record = match self.shared.journal.revivable_agent(&agent_id).await {
                Ok(record) => record,
                Err(error) => {
                    tracing::warn!(agent_id, %error, "an ended agent's checkout is left in place");
                    None
                }
            };
            let checkout = record
                .as_ref()
                .and_then(|record: &RevivableAgent| self.shared.remembered_checkout(record));
            (
                Vec::new(),
                AfterCommit::Release {
                    agent_id: agent_id.clone(),
                    checkout,
                },
            )
        };
        Applied {
            effects,
            bookkeeping: AgentBookkeeping::Forget { agent_id },
            inbox: InboxDisposition::Settle,
            then,
        }
    }

    fn deliver_permission_decision(
        &self,
        deliver: DeliverPermissionDecisionCommand,
    ) -> Result<Applied, WorkerError> {
        let Some(agent) = self.named_agent(deliver.agent_id.as_ref()) else {
            return Ok(Applied::nothing());
        };
        let decision = deliver
            .decision
            .ok_or(WorkerError::Incomplete("permission decision"))?;
        let tool_call_id = decision
            .request_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
            .ok_or(WorkerError::Incomplete("permission request id"))?;

        // No journal effect when it applies: the decision is the Server's own
        // event — it ran the chain and journaled PermissionDecided first.
        match permission_decision_from_wire(&decision, agent.can_rewrite) {
            Ok(decision) => Ok(Applied {
                then: AfterCommit::Control(AgentControl::Answer {
                    tool_call_id: ToolCallId::new(tool_call_id),
                    decision,
                }),
                ..Applied::nothing()
            }),
            // A narrowing the adapter cannot apply must never silently widen
            // into a plain allow — and must not hang the harness on a call
            // nobody will answer, nor stop the Worker. It fails closed: the
            // call is denied, and the log says why.
            Err(error) => {
                let reason = format!(
                    "horde-worker denied tool call {tool_call_id}: the decision could not be applied ({error})"
                );
                Ok(Applied {
                    effects: vec![diagnostic_entry(
                        &agent.session_id,
                        &self.key,
                        reason.clone(),
                    )],
                    then: AfterCommit::Control(AgentControl::Answer {
                        tool_call_id: ToolCallId::new(tool_call_id),
                        decision: PermissionDecision::Deny { message: reason },
                    }),
                    ..Applied::nothing()
                })
            }
        }
    }

    /// Runs a command's follow-up after its completion committed.
    async fn after_commit(&mut self, then: AfterCommit) {
        match then {
            AfterCommit::Nothing => {}
            AfterCommit::Start {
                agent_id,
                session,
                context,
                can_rewrite,
                inputs,
            } => {
                self.start(agent_id, session, context, can_rewrite, inputs)
                    .await
            }
            AfterCommit::Control(control) => {
                if let Some(agent) = &self.agent
                    && agent.control.send(control).await.is_err()
                {
                    tracing::warn!(lane = %self.key, "control for an agent whose pump already ended");
                }
            }
            AfterCommit::Retire { agent_id } => self.retire(&agent_id).await,
            AfterCommit::Release { agent_id, checkout } => {
                self.shared.forget_endpoint(&agent_id).await;
                self.shared.broker.forget(&agent_id);
                if let Some(checkout) = checkout {
                    self.shared.release_checkout(&agent_id, &checkout).await;
                }
            }
        }
    }

    async fn start(
        &mut self,
        agent_id: String,
        session: Box<dyn AgentSession>,
        context: AgentContext,
        can_rewrite: bool,
        inputs: Vec<(TurnInput, Option<String>)>,
    ) {
        let (control, control_receiver) = mpsc::channel(CONTROL_CAPACITY);
        let session_id = context.session_id.clone();
        let task = tokio::spawn(pump(
            session,
            self.shared.journal.clone(),
            context,
            control_receiver,
        ));
        for (input, command_id) in inputs {
            let _ = control
                .send(AgentControl::Deliver { input, command_id })
                .await;
        }
        let previous = self.agent.replace(LiveAgent {
            control,
            session_id,
            can_rewrite,
            pump: Some(task),
        });
        if let Some(previous) = previous
            && let Some(task) = previous.pump
        {
            // A revival replacing a dead pump (dropping its control ends one
            // that was somehow still alive): shutdown still owes it a join.
            keep(
                &mut self.retired,
                tokio::spawn(async move {
                    let _ = task.await;
                }),
            );
        }
        self.ledger.running().insert(agent_id);
    }

    async fn retire(&mut self, agent_id: &str) {
        // The checkout registry must not outlive the agent: a stale entry
        // would stamp an archived agent's HEAD, or offer its checkout as a
        // peer-fetch source, forever. Same for the endpoint token.
        self.shared.forget_endpoint(agent_id).await;
        let checkout = self.shared.broker.checkout_of(agent_id);
        self.shared.broker.forget(agent_id);
        self.ledger.running().remove(agent_id);
        let Some(agent) = self.agent.take() else {
            return;
        };
        let _ = agent.control.send(AgentControl::Archive).await;
        // The checkout goes back only once the pump — and so the harness
        // group — has stopped: removing a worktree under a running agent
        // would pull its cwd out from under it.
        let shared = self.shared.clone();
        let agent_id = agent_id.to_owned();
        let pump = agent.pump;
        keep(
            &mut self.retired,
            tokio::spawn(async move {
                if let Some(pump) = pump {
                    let _ = pump.await;
                }
                if let Some(checkout) = checkout {
                    shared.release_checkout(&agent_id, &checkout.root).await;
                }
            }),
        );
    }

    /// Ends the lane: the agent's pump is stopped (its checkout kept — a
    /// shutdown is not an archive, and the next start revives into it) and
    /// every retiring task joined. Returns how many were reaped.
    async fn close(mut self) -> usize {
        let mut reaped = 0;
        if let Some(agent) = self.agent.take() {
            reaped += 1;
            drop(agent.control);
            if let Some(pump) = agent.pump {
                if let Err(error) = pump.await {
                    tracing::warn!(%error, "an agent pump ended abnormally during shutdown");
                }
            }
            self.ledger.running().remove(&self.key);
        }
        for task in std::mem::take(&mut self.retired) {
            reaped += 1;
            let _ = task.await;
        }
        reaped
    }
}

/// A message on its way into a revival.
struct Pending {
    input: TurnInput,
    payload: Option<horde_proto::v1::Payload>,
    command_id: String,
}

/// Resolves when the lane's pump ends; never, while there is none.
async fn pump_exit(agent: &mut Option<LiveAgent>) -> PumpExit {
    let Some(pump) = agent.as_mut().and_then(|agent| agent.pump.as_mut()) else {
        return std::future::pending().await;
    };
    let exit = pump.await.unwrap_or(PumpExit::Ended);
    if let Some(agent) = agent.as_mut() {
        agent.pump = None;
    }
    exit
}

/// Keeps a finished-or-finishing task for shutdown to join, dropping the
/// ones that already finished — the list would otherwise grow by one per
/// incarnation for the lane's whole life.
pub(super) fn keep(retired: &mut Vec<JoinHandle<()>>, task: JoinHandle<()>) {
    retired.retain(|retired| !retired.is_finished());
    retired.push(task);
}

/// The applied form of a spawn that could not start: the command completes,
/// with the agent's failure as its journaled effect.
fn spawn_failed(context: &AgentContext, agent_id: &str, error: &WorkerError) -> Applied {
    // An agent that never started has nothing to revive; remembering it
    // would promise a revival the journal cannot deliver.
    Applied::effects(vec![lifecycle_entry(
        context,
        format!("{agent_id}:spawn-failed"),
        AgentLifecycleState::Queued,
        AgentLifecycleState::Failed,
        error.to_string(),
    )])
}

/// A revival that cannot happen fails the agent loudly, and that failure is
/// the answer to every message it was handed: they are settled with it.
fn revival_failed(context: &AgentContext, reason: String) -> Applied {
    Applied {
        inbox: InboxDisposition::SettleAgent(context.agent_id.clone()),
        ..Applied::effects(vec![lifecycle_entry(
            context,
            String::new(),
            AgentLifecycleState::Unspecified,
            AgentLifecycleState::Failed,
            reason,
        )])
    }
}

/// A delivery whose Payload could not be obtained fails the *agent*,
/// loudly: the message names work the agent cannot see, and delivering the
/// text anyway would have it act on a payload it cannot check out. The
/// command completes, the failure is its journaled effect, and the pump is
/// retired. Failed, not archived: the context survives, so another message
/// (with an obtainable payload) may still revive the agent.
fn payload_failed(session_id: &str, agent_id: &str, error: &WorkerError) -> Applied {
    Applied {
        then: AfterCommit::Retire {
            agent_id: agent_id.to_owned(),
        },
        ..Applied::effects(vec![JournalEntry {
            session_id: session_id.to_owned(),
            agent_id: Some(agent_id.to_owned()),
            idempotency_key: String::new(),
            kind: Kind::AgentLifecycleChanged(horde_proto::v1::AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running.into(),
                state: AgentLifecycleState::Failed.into(),
                reason: format!("a delivered payload could not be obtained: {error}"),
            }),
        }])
    }
}
