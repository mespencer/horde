//! Applying commands: dedupe, adapters, and the per-agent event pump.
//!
//! The Runtime is the [`ApplyCommand`](crate::client::ApplyCommand) half of
//! the Worker: the command feed hands it wire commands, and it turns them into
//! adapter calls, journal entries, and running agents. Its contract with the
//! journal is the one-transaction rule — a command's applied-mark and its
//! effect events commit together — and its contract with the wire is the
//! at-least-once rule: redelivered commands are recognized *before* anything
//! executes, because the side effect of applying a spawn is a live process.
//!
//! # What a command journals, and what it deliberately does not
//!
//! Every command's *causing* event is already in the Session log — that is
//! what the Server's command stream is folded from — so the Runtime journals
//! only what the Worker itself observes: lifecycle transitions it causes
//! (starting, archived, failed) and interruptions it performs. Message
//! deliveries and permission decisions journal nothing here; the Server wrote
//! `AssignmentSent` / `PermissionDecided` when they happened, and a second
//! copy from the Worker would double every one of them in every client.
//!
//! # The pump
//!
//! Each agent runs one task that owns its [`AgentSession`] outright. Commands
//! reach it over a control channel; events leave through
//! [`translate`](crate::translate) into the journal. The select over control
//! and `next_event` is `biased` toward control so a shutdown or an interrupt
//! is never starved by a chatty harness, and it relies on `next_event` being
//! cancel-safe — true of both sessions this Worker drives (tokio's
//! `Lines::next_line` documents it; the fake's only await is a stall sleep).

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use horde_adapter::{
    Adapter, AgentEvent, AgentSession, Capability, PermissionDecision, ProviderAccount, ToolCallId,
    TurnInput,
};
use horde_proto::v1::stream_commands_response::Command;
use horde_proto::v1::{
    AgentLifecycleChanged, AgentLifecycleState, ArchiveAgentCommand, CancelPlacementCommand,
    DeliverMessageCommand, DeliverPermissionDecisionCommand, DeliveryMode, InterruptAgentCommand,
    SpawnAgentCommand, StreamCommandsResponse, TurnInterrupted, deliver_message_command,
    event::Kind,
};
use horde_wire::permission_decision_from_wire;
use tokio::sync::mpsc;

use crate::client::ApplyCommand;
use crate::error::WorkerError;
use crate::journal::{Journal, JournalEntry};
use crate::translate::{AgentContext, journal_entries};

/// How many control messages may queue for one agent's pump.
const CONTROL_CAPACITY: usize = 8;

/// The Worker's command-applying half: adapters in, running agents out.
pub struct Runtime {
    journal: Journal,
    adapters: BTreeMap<String, Arc<dyn Adapter>>,
    /// The provider used when a spawn command names none — the state of the
    /// world before the scheduler resolves bindings.
    default_provider: Option<String>,
    /// Materializes instances and produces per-agent checkouts. `None` means
    /// this Worker does not advertise workspace support and refuses
    /// materialize commands loudly.
    workspaces: Option<crate::workspace::WorkspaceManager>,
    agents: BTreeMap<String, AgentHandle>,
    /// Pumps whose agents were archived, kept so shutdown can wait for them.
    retired: Vec<tokio::task::JoinHandle<()>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Runtime")
            .field("adapters", &self.adapters.keys().collect::<Vec<_>>())
            .field("agents", &self.agents.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// One running agent, from the Runtime's side of the control channel.
struct AgentHandle {
    control: mpsc::Sender<AgentControl>,
    /// The Session whose log this agent's command effects belong in.
    session_id: String,
    /// The adapter's `RewritesToolInput` claim, needed to convert incoming
    /// permission decisions — a narrowing decision must be refused, not
    /// silently widened, when the adapter cannot apply it.
    can_rewrite: bool,
    task: tokio::task::JoinHandle<()>,
}

/// What the Runtime can tell a running pump.
enum AgentControl {
    Deliver(TurnInput),
    Answer {
        tool_call_id: ToolCallId,
        decision: PermissionDecision,
    },
    Interrupt,
    Archive,
}

/// What executing a command decided, before anything is journaled.
struct Applied {
    effects: Vec<JournalEntry>,
    then: AfterCommit,
}

/// Work that must wait for the applied-mark to commit.
///
/// Starting a pump or forwarding control before the commit would act on a
/// command that a crash could then re-deliver as never-applied.
enum AfterCommit {
    Nothing,
    Start {
        agent_id: String,
        session: Box<dyn AgentSession>,
        context: AgentContext,
        can_rewrite: bool,
        first_input: Option<TurnInput>,
    },
    Control {
        agent_id: String,
        control: AgentControl,
    },
    Retire {
        agent_id: String,
    },
}

impl Runtime {
    #[must_use]
    pub fn new(journal: Journal) -> Self {
        Self {
            journal,
            adapters: BTreeMap::new(),
            default_provider: None,
            workspaces: None,
            agents: BTreeMap::new(),
            retired: Vec::new(),
        }
    }

    /// Enables workspace support: materialize commands are applied, spawns
    /// naming a workspace get a checkout, and the advertisement says so.
    #[must_use]
    pub fn with_workspaces(mut self, workspaces: crate::workspace::WorkspaceManager) -> Self {
        self.workspaces = Some(workspaces);
        self
    }

    /// The workspace manager, when this Worker has one — the advertisement
    /// builder reads warmth and support from it.
    #[must_use]
    pub fn workspaces(&self) -> Option<&crate::workspace::WorkspaceManager> {
        self.workspaces.as_ref()
    }

    /// Registers an adapter under its provider key. The first registered
    /// adapter becomes the default for spawn commands that name no provider.
    #[must_use]
    pub fn with_adapter(mut self, adapter: Arc<dyn Adapter>) -> Self {
        let provider = adapter.provider().to_owned();
        if self.default_provider.is_none() {
            self.default_provider = Some(provider.clone());
        }
        self.adapters.insert(provider, adapter);
        self
    }

    /// The registered adapters, for building the capability advertisement.
    pub fn adapters(&self) -> impl Iterator<Item = &dyn Adapter> {
        self.adapters.values().map(AsRef::as_ref)
    }

    /// How many agents this Runtime is currently running.
    #[must_use]
    pub fn running_agents(&self) -> usize {
        self.agents.len()
    }

    /// Ends every agent and waits for their pumps to finish, returning how
    /// many pumps were reaped (running and retired alike).
    ///
    /// Bounded by structure: dropping the control senders ends each pump's
    /// loop, and each shutdown is a kill-by-handle, not a wait-for-exit. The
    /// count is returned rather than logged because "shutdown finished" and
    /// "shutdown actually reaped the agents" are different facts, and only the
    /// second one is evidence.
    pub async fn shutdown(mut self) -> usize {
        let handles: Vec<AgentHandle> = std::mem::take(&mut self.agents).into_values().collect();
        let mut tasks = std::mem::take(&mut self.retired);
        for handle in handles {
            drop(handle.control);
            tasks.push(handle.task);
        }
        let reaped = tasks.len();
        for task in tasks {
            if let Err(error) = task.await {
                tracing::warn!(%error, "an agent pump ended abnormally during shutdown");
            }
        }
        reaped
    }

    /// Executes one command kind, deciding its effects and follow-up.
    async fn execute(&mut self, command: Command) -> Result<Applied, WorkerError> {
        match command {
            Command::SpawnAgent(spawn) => self.spawn_agent(spawn).await,
            Command::DeliverMessage(deliver) => Ok(self.deliver_message(deliver)),
            Command::InterruptAgent(interrupt) => Ok(self.interrupt_agent(&interrupt)),
            Command::ArchiveAgent(archive) => Ok(self.archive_agent(&archive)),
            Command::CancelPlacement(cancel) => Ok(self.cancel_placement(&cancel)),
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
    /// A Worker with no workspace manager never advertised support, so
    /// receiving this means the handshake promised something it cannot
    /// honour — loud beats acking work that never happened. A *degraded*
    /// materialization, by contrast, applies fine: the report carries the
    /// failure, and the Server can stop preferring this Worker.
    async fn materialize_workspace(
        &mut self,
        materialize: horde_proto::v1::MaterializeWorkspaceCommand,
    ) -> Result<Applied, WorkerError> {
        let Some(workspaces) = &self.workspaces else {
            return Err(WorkerError::UnsupportedCommand("materialize workspace"));
        };
        let definition = materialize
            .definition
            .ok_or(WorkerError::Incomplete("workspace definition"))?;
        let session_id = required_id(materialize.session_id.as_ref(), "session id")?;

        let report = workspaces.materialize(&definition).await;
        Ok(Applied {
            effects: vec![JournalEntry {
                session_id,
                agent_id: None,
                idempotency_key: String::new(),
                kind: Kind::WorkspaceInstanceChanged(horde_proto::v1::WorkspaceInstanceChanged {
                    report: Some(report),
                    progress: None,
                }),
            }],
            then: AfterCommit::Nothing,
        })
    }

    async fn spawn_agent(&mut self, spawn: SpawnAgentCommand) -> Result<Applied, WorkerError> {
        let agent_id = required_id(spawn.agent_id.as_ref(), "agent id")?;
        let session_id = required_id(spawn.session_id.as_ref(), "session id")?;

        let context = AgentContext {
            session_id: session_id.clone(),
            agent_id: agent_id.clone(),
            binding: spawn.binding.clone(),
            permission_mode: spawn
                .spec
                .as_ref()
                .map(|spec| spec.permission_mode.clone())
                .unwrap_or_default(),
            tree_path: spawn.tree_path.clone(),
        };

        // A spawn that cannot start is the *agent* failing, not the command
        // loop: the command still applies, with the failure as its journaled
        // effect, and the Server hears an honest lifecycle instead of losing
        // its Worker.
        let (session, can_rewrite) = match self.start_session(&spawn).await {
            Ok(started) => started,
            Err(error) => {
                return Ok(Applied {
                    effects: vec![lifecycle_entry(
                        &context,
                        format!("{agent_id}:spawn-failed"),
                        AgentLifecycleState::Queued,
                        AgentLifecycleState::Failed,
                        error.to_string(),
                    )],
                    then: AfterCommit::Nothing,
                });
            }
        };

        let first_input = spawn
            .spec
            .and_then(|spec| spec.initial_assignment)
            .map(|assignment| assignment.text)
            .filter(|text| !text.is_empty())
            .map(TurnInput::queued);

        Ok(Applied {
            effects: vec![lifecycle_entry(
                &context,
                format!("{agent_id}:starting"),
                AgentLifecycleState::Queued,
                AgentLifecycleState::Starting,
                "placed on this worker".to_owned(),
            )],
            then: AfterCommit::Start {
                agent_id,
                session,
                context,
                can_rewrite,
                first_input,
            },
        })
    }

    /// Resolves the adapter and starts the harness session.
    async fn start_session(
        &self,
        spawn: &SpawnAgentCommand,
    ) -> Result<(Box<dyn AgentSession>, bool), WorkerError> {
        let provider = spawn
            .binding
            .as_ref()
            .map(|binding| binding.provider.clone())
            .filter(|provider| !provider.is_empty())
            .or_else(|| self.default_provider.clone())
            .ok_or_else(|| WorkerError::UnknownProvider(String::new()))?;
        let adapter = self
            .adapters
            .get(&provider)
            .ok_or_else(|| WorkerError::UnknownProvider(provider.clone()))?;

        let cwd = self.agent_checkout(spawn).await?;
        let spec = domain_spec(spawn, cwd);
        let session = adapter.spawn(&spec).await?;
        let can_rewrite = adapter
            .capabilities()
            .supports(Capability::RewritesToolInput);
        Ok((session, can_rewrite))
    }

    /// The working directory a spawn's workspace and Checkout Policy call
    /// for; `None` when the spawn names no workspace and the agent runs
    /// where the Worker runs.
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when a workspace is named but this Worker
    /// has no manager, no materialized instance, or the checkout fails — the
    /// spawn's failure, journaled as a failed lifecycle by the caller.
    async fn agent_checkout(
        &self,
        spawn: &SpawnAgentCommand,
    ) -> Result<Option<std::path::PathBuf>, WorkerError> {
        let Some(spec) = spawn.spec.as_ref() else {
            return Ok(None);
        };
        let Some(definition_id) = spec
            .workspace_definition_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
        else {
            return Ok(None);
        };
        let workspaces = self.workspaces.as_ref().ok_or_else(|| {
            WorkerError::Workspace(format!(
                "the spawn names workspace {definition_id}, but this worker has no workspace root"
            ))
        })?;

        let agent_id = spawn
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let policy = horde_proto::v1::CheckoutPolicy::try_from(spec.checkout_policy)
            .unwrap_or(horde_proto::v1::CheckoutPolicy::Unspecified);
        workspaces
            .checkout(&definition_id, &agent_id, policy)
            .await
            .map(Some)
    }

    fn deliver_message(&mut self, deliver: DeliverMessageCommand) -> Applied {
        let Some(agent_id) = known_agent(&self.agents, deliver.agent_id.as_ref()) else {
            return Applied {
                effects: Vec::new(),
                then: AfterCommit::Nothing,
            };
        };

        let text = match deliver.body {
            Some(deliver_message_command::Body::Assignment(assignment)) => assignment.text,
            Some(deliver_message_command::Body::Note(note)) => note.text,
            None => {
                tracing::warn!(
                    agent_id,
                    "a deliver command carried no message; applied empty"
                );
                return Applied {
                    effects: Vec::new(),
                    then: AfterCommit::Nothing,
                };
            }
        };

        let input = match DeliveryMode::try_from(deliver.delivery) {
            Ok(DeliveryMode::Interrupt) => TurnInput::interrupting(text),
            // Turn-boundary is every provider's safe default, and an
            // unrecognized mode from a newer Server must not become an
            // interrupt nobody asked for.
            _ => TurnInput::queued(text),
        };

        // No journal effect: the Server wrote AssignmentSent/NoteSent when the
        // message happened; that event is why this command exists.
        Applied {
            effects: Vec::new(),
            then: AfterCommit::Control {
                agent_id,
                control: AgentControl::Deliver(input),
            },
        }
    }

    fn interrupt_agent(&mut self, interrupt: &InterruptAgentCommand) -> Applied {
        let Some(agent_id) = known_agent(&self.agents, interrupt.agent_id.as_ref()) else {
            return Applied {
                effects: Vec::new(),
                then: AfterCommit::Nothing,
            };
        };

        Applied {
            effects: vec![JournalEntry {
                session_id: self
                    .agents
                    .get(&agent_id)
                    .map(|handle| handle.session_id.clone())
                    .unwrap_or_default(),
                agent_id: Some(agent_id.clone()),
                // Interrupts can repeat; the journal mints the key.
                idempotency_key: String::new(),
                kind: Kind::TurnInterrupted(TurnInterrupted {
                    requested_by: None,
                    turn_id: String::new(),
                    reason: interrupt.reason.clone(),
                }),
            }],
            then: AfterCommit::Control {
                agent_id,
                control: AgentControl::Interrupt,
            },
        }
    }

    fn archive_agent(&mut self, archive: &ArchiveAgentCommand) -> Applied {
        let Some(agent_id) = known_agent(&self.agents, archive.agent_id.as_ref()) else {
            return Applied {
                effects: Vec::new(),
                then: AfterCommit::Nothing,
            };
        };

        let session_id = self
            .agents
            .get(&agent_id)
            .map(|handle| handle.session_id.clone())
            .unwrap_or_default();
        Applied {
            effects: vec![JournalEntry {
                session_id,
                agent_id: Some(agent_id.clone()),
                idempotency_key: format!("{agent_id}:archived"),
                kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    // The Worker does not track idle-vs-running; claiming a
                    // previous state it never observed would be invention.
                    previous_state: AgentLifecycleState::Unspecified.into(),
                    state: AgentLifecycleState::Archived.into(),
                    reason: archive.reason.clone(),
                }),
            }],
            then: AfterCommit::Retire { agent_id },
        }
    }

    fn cancel_placement(&mut self, cancel: &CancelPlacementCommand) -> Applied {
        // A placement cancelled before its spawn command arrived names an
        // agent this Worker never started; one cancelled after is an archive
        // in different clothes. PlacementChanged events are Server-authored,
        // so there is nothing to journal either way.
        let Some(agent_id) = known_agent(&self.agents, cancel.agent_id.as_ref()) else {
            return Applied {
                effects: Vec::new(),
                then: AfterCommit::Nothing,
            };
        };
        Applied {
            effects: Vec::new(),
            then: AfterCommit::Retire { agent_id },
        }
    }

    fn deliver_permission_decision(
        &mut self,
        deliver: DeliverPermissionDecisionCommand,
    ) -> Result<Applied, WorkerError> {
        let Some(agent_id) = known_agent(&self.agents, deliver.agent_id.as_ref()) else {
            return Ok(Applied {
                effects: Vec::new(),
                then: AfterCommit::Nothing,
            });
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

        let can_rewrite = self
            .agents
            .get(&agent_id)
            .is_some_and(|handle| handle.can_rewrite);
        // Refuses a narrowing decision the adapter cannot apply
        // (RewriteUnsupported), which must never silently widen into a plain
        // allow — the whole point of the capability advertisement.
        let decision = permission_decision_from_wire(&decision, can_rewrite)?;

        // No journal effect: the decision is the Server's own event — it ran
        // the chain and journaled PermissionDecided before commanding us.
        Ok(Applied {
            effects: Vec::new(),
            then: AfterCommit::Control {
                agent_id,
                control: AgentControl::Answer {
                    tool_call_id: ToolCallId::new(tool_call_id),
                    decision,
                },
            },
        })
    }

    /// Runs a command's follow-up after its applied-mark committed.
    async fn after_commit(&mut self, then: AfterCommit) {
        match then {
            AfterCommit::Nothing => {}
            AfterCommit::Start {
                agent_id,
                session,
                context,
                can_rewrite,
                first_input,
            } => {
                let (control, control_receiver) = mpsc::channel(CONTROL_CAPACITY);
                let session_id = context.session_id.clone();
                let task = tokio::spawn(pump(
                    session,
                    self.journal.clone(),
                    context,
                    control_receiver,
                ));
                if let Some(input) = first_input {
                    let _ = control.send(AgentControl::Deliver(input)).await;
                }
                self.agents.insert(
                    agent_id,
                    AgentHandle {
                        control,
                        session_id,
                        can_rewrite,
                        task,
                    },
                );
            }
            AfterCommit::Control { agent_id, control } => {
                if let Some(handle) = self.agents.get(&agent_id)
                    && handle.control.send(control).await.is_err()
                {
                    tracing::warn!(agent_id, "control for an agent whose pump already ended");
                }
            }
            AfterCommit::Retire { agent_id } => {
                if let Some(handle) = self.agents.remove(&agent_id) {
                    let _ = handle.control.send(AgentControl::Archive).await;
                    self.retired.push(handle.task);
                }
            }
        }
    }
}

#[async_trait]
impl ApplyCommand for Runtime {
    async fn apply(&mut self, command: StreamCommandsResponse) -> Result<(), WorkerError> {
        // At-least-once: recognize a redelivery before anything executes,
        // because executing a spawn twice is two harness processes.
        if self.journal.is_command_applied(&command.command_id).await? {
            tracing::debug!(
                command_id = %command.command_id,
                "command redelivered; already applied"
            );
            return Ok(());
        }

        let kind = command.command.ok_or(WorkerError::Incomplete("command"))?;
        let applied = self.execute(kind).await?;

        self.journal
            .apply_command(
                &command.command_id,
                command.command_sequence,
                applied.effects,
            )
            .await?;
        self.after_commit(applied.then).await;
        Ok(())
    }
}

/// A lifecycle transition as a journal effect.
fn lifecycle_entry(
    context: &AgentContext,
    idempotency_key: String,
    previous: AgentLifecycleState,
    state: AgentLifecycleState,
    reason: String,
) -> JournalEntry {
    JournalEntry {
        session_id: context.session_id.clone(),
        agent_id: Some(context.agent_id.clone()),
        idempotency_key,
        kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: previous.into(),
            state: state.into(),
            reason,
        }),
    }
}

/// An id field the protocol requires, or the error that names it.
fn required_id<T: ToString>(id: Option<&T>, name: &'static str) -> Result<String, WorkerError> {
    id.map(ToString::to_string)
        .filter(|id| !id.is_empty())
        .ok_or(WorkerError::Incomplete(name))
}

/// The agent a command names, if this Runtime is actually running it.
///
/// `None` is a race, not a bug: the Server sent the command before it saw the
/// agent end. The command still applies — the causing event is already in the
/// log — there is just nobody left to forward it to.
fn known_agent(
    agents: &BTreeMap<String, AgentHandle>,
    agent_id: Option<&horde_proto::v1::AgentId>,
) -> Option<String> {
    let id = agent_id.map(ToString::to_string)?;
    if agents.contains_key(&id) {
        Some(id)
    } else {
        tracing::warn!(agent_id = %id, "command for an agent this worker is not running");
        None
    }
}

/// What the wire spawn command means to an adapter.
fn domain_spec(
    spawn: &SpawnAgentCommand,
    cwd: Option<std::path::PathBuf>,
) -> horde_adapter::AgentSpec {
    let binding = spawn.binding.as_ref();
    horde_adapter::AgentSpec {
        model: binding
            .map(|binding| binding.model.clone())
            .filter(|model| !model.is_empty()),
        append_system_prompt: (!spawn.prompt_overlay.is_empty())
            .then(|| spawn.prompt_overlay.clone()),
        // The checkout the spawn's workspace and policy called for; `None`
        // runs the agent where the Worker runs.
        cwd,
        account: binding
            .map(|binding| binding.provider_account.clone())
            .filter(|account| !account.is_empty())
            .map(ProviderAccount::new),
        // The MCP endpoint is the sibling increment; wired here when it lands.
        horde_endpoint: None,
        provider_options: BTreeMap::new(),
    }
}

/// What one pump iteration is about to handle.
enum Step {
    Control(Option<AgentControl>),
    Event(Result<Option<AgentEvent>, horde_adapter::AdapterError>),
}

/// Whether the pump should keep polling the session for events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PumpState {
    /// No turn in flight; only control can make something happen.
    Idle,
    /// A turn is producing events.
    Streaming,
    /// The harness is blocked on a permission decision. Polling `next_event`
    /// here would be answered with `AwaitingDecision` by an honest session and
    /// a hang by a piped one; only an answer can advance things.
    AwaitingDecision,
}

/// One agent's event loop: owns the session, obeys control, journals events.
async fn pump(
    mut session: Box<dyn AgentSession>,
    journal: Journal,
    context: AgentContext,
    mut control: mpsc::Receiver<AgentControl>,
) {
    let mut state = PumpState::Idle;
    loop {
        let step = if state == PumpState::Streaming {
            tokio::select! {
                // Biased toward control so a shutdown or interrupt is never
                // starved by a chatty harness.
                biased;
                message = control.recv() => Step::Control(message),
                event = session.next_event() => Step::Event(event),
            }
        } else {
            Step::Control(control.recv().await)
        };

        let flow = match step {
            Step::Control(message) => {
                apply_control(&mut session, &journal, &context, state, message).await
            }
            Step::Event(event) => apply_event(&mut session, &journal, &context, event).await,
        };
        match flow {
            Flow::Continue(next) => state = next,
            Flow::Stop => return,
        }
    }
}

/// What the pump does after handling one step.
enum Flow {
    Continue(PumpState),
    Stop,
}

/// Obeys one control message.
async fn apply_control(
    session: &mut Box<dyn AgentSession>,
    journal: &Journal,
    context: &AgentContext,
    state: PumpState,
    message: Option<AgentControl>,
) -> Flow {
    match message {
        // The Runtime dropped this agent (archive, cancel, shutdown).
        None | Some(AgentControl::Archive) => {
            let _ = session.shutdown().await;
            Flow::Stop
        }
        Some(AgentControl::Deliver(input)) => match session.send(input).await {
            Ok(()) => Flow::Continue(PumpState::Streaming),
            Err(error) => {
                record_failure(journal, context, &error.to_string()).await;
                let _ = session.shutdown().await;
                Flow::Stop
            }
        },
        Some(AgentControl::Answer {
            tool_call_id,
            decision,
        }) => match session.answer_permission(&tool_call_id, decision).await {
            Ok(()) => Flow::Continue(PumpState::Streaming),
            Err(error) => {
                // A decision for a call that is not waiting (raced with an
                // interrupt, or duplicated). The agent is not broken; saying
                // so in the log is the whole handling.
                tracing::warn!(agent_id = %context.agent_id, %error, "permission answer went nowhere");
                Flow::Continue(state)
            }
        },
        Some(AgentControl::Interrupt) => {
            if let Err(error) = session.interrupt().await {
                tracing::warn!(agent_id = %context.agent_id, %error, "interrupt failed");
            }
            // The interrupted turn still closes with TurnEnded, which arrives
            // as an ordinary event.
            Flow::Continue(state)
        }
    }
}

/// Journals one session event, or the session's end.
async fn apply_event(
    session: &mut Box<dyn AgentSession>,
    journal: &Journal,
    context: &AgentContext,
    event: Result<Option<AgentEvent>, horde_adapter::AdapterError>,
) -> Flow {
    match event {
        Ok(Some(event)) => {
            let next = state_after(&event);
            match journal_entries(context, &event) {
                Ok(entries) => {
                    for entry in entries {
                        if let Err(error) = journal.record(entry).await {
                            // A journal that cannot append has lost the
                            // partition-survival property; running an agent
                            // whose evidence is being dropped is worse than
                            // stopping it.
                            tracing::error!(agent_id = %context.agent_id, %error, "journal append failed; stopping the agent");
                            let _ = session.shutdown().await;
                            return Flow::Stop;
                        }
                    }
                    Flow::Continue(next)
                }
                Err(error) => {
                    tracing::error!(agent_id = %context.agent_id, %error, "an event could not be translated and was not journaled");
                    Flow::Continue(next)
                }
            }
        }
        Ok(None) => {
            // EOF mid-conversation: the harness is gone.
            record_failure(journal, context, "the harness ended its event stream").await;
            Flow::Stop
        }
        Err(error) => {
            record_failure(journal, context, &error.to_string()).await;
            let _ = session.shutdown().await;
            Flow::Stop
        }
    }
}

/// Where the pump stands after seeing `event`.
fn state_after(event: &AgentEvent) -> PumpState {
    match event {
        AgentEvent::PermissionRequested { .. } => PumpState::AwaitingDecision,
        AgentEvent::TurnEnded(_) => PumpState::Idle,
        _ => PumpState::Streaming,
    }
}

/// Journals an agent's failure, best-effort — the pump is already dying.
async fn record_failure(journal: &Journal, context: &AgentContext, reason: &str) {
    let entry = JournalEntry {
        session_id: context.session_id.clone(),
        agent_id: Some(context.agent_id.clone()),
        idempotency_key: String::new(),
        kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
            previous_state: AgentLifecycleState::Running.into(),
            state: AgentLifecycleState::Failed.into(),
            reason: reason.to_owned(),
        }),
    };
    if let Err(error) = journal.record(entry).await {
        tracing::error!(agent_id = %context.agent_id, %error, "could not journal an agent failure");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use horde_adapter::{Capabilities, Capability};
    use horde_adapter_fake::{FakeAdapter, Script, ScriptedTurn, Step};
    use horde_proto::prost_types::Struct;
    use horde_proto::v1::stream_commands_response::Command;
    use horde_proto::v1::{
        AgentId, ArchiveAgentCommand, Assignment, DeliverMessageCommand,
        DeliverPermissionDecisionCommand, DeliveryMode, MaterializeWorkspaceCommand,
        PermissionOutcome, PermissionRequestId, SessionId, SpawnAgentCommand,
        StreamCommandsResponse, deliver_message_command, event::Kind,
    };
    use serde_json::json;

    use super::Runtime;
    use crate::client::ApplyCommand;
    use crate::error::WorkerError;
    use crate::journal::Journal;

    fn runtime_with(script: Script) -> Runtime {
        Runtime::new(Journal::in_memory().expect("an in-memory journal"))
            .with_adapter(Arc::new(FakeAdapter::new(script)))
    }

    /// A runtime whose adapter advertises only pre-authorization — the shape
    /// of a provider that can neither rewrite inputs nor take an interrupt.
    fn preauthorize_only_runtime(script: Script) -> Runtime {
        let adapter = FakeAdapter::new(script)
            .with_capabilities(Capabilities::none().with(Capability::PreAuthorizeToolCalls));
        Runtime::new(Journal::in_memory().expect("an in-memory journal"))
            .with_adapter(Arc::new(adapter))
    }

    fn spawn_command(sequence: u64, agent: &str, first_message: &str) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("s1:{sequence}"),
            command: Some(Command::SpawnAgent(SpawnAgentCommand {
                session_id: Some(SessionId::new("s1")),
                agent_id: Some(AgentId::new(agent)),
                parent_agent_id: None,
                spec: Some(horde_proto::v1::AgentSpec {
                    initial_assignment: Some(Assignment {
                        assignment_id: None,
                        text: first_message.to_owned(),
                        payload: None,
                    }),
                    ..horde_proto::v1::AgentSpec::default()
                }),
                binding: None,
                tree_path: Vec::new(),
                prompt_overlay: String::new(),
            })),
        }
    }

    fn decision_command(
        sequence: u64,
        agent: &str,
        request_id: &str,
        updated_input: Option<Struct>,
    ) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("s1:{}", sequence + 100),
            command: Some(Command::DeliverPermissionDecision(
                DeliverPermissionDecisionCommand {
                    agent_id: Some(AgentId::new(agent)),
                    decision: Some(horde_proto::v1::PermissionDecision {
                        request_id: Some(PermissionRequestId::new(request_id)),
                        outcome: PermissionOutcome::Allow.into(),
                        updated_input,
                        ..horde_proto::v1::PermissionDecision::default()
                    }),
                },
            )),
        }
    }

    /// Waits until the journal holds at least `minimum` events.
    ///
    /// Bounded twice: by iteration count (a hot loop cannot spin past 64
    /// polls) and by a ceiling on each park (a journal that stops moving fails
    /// in seconds, not never).
    async fn journal_at_least(
        runtime: &Runtime,
        minimum: usize,
    ) -> Vec<horde_proto::v1::JournaledEvent> {
        let journal = runtime.journal.clone();
        let mut head = journal.watch_head();
        let mut polls = 0_u32;
        loop {
            let events = journal.read_from(1, 100).await.expect("a readable journal");
            if events.len() >= minimum {
                return events;
            }
            polls += 1;
            assert!(polls < 64, "the journal never reached {minimum} events");
            tokio::time::timeout(core::time::Duration::from_secs(5), head.changed())
                .await
                .expect("the journal stopped moving")
                .expect("the journal outlives the wait");
        }
    }

    fn kinds(events: &[horde_proto::v1::JournaledEvent]) -> Vec<&'static str> {
        events
            .iter()
            .map(|journaled| {
                match journaled
                    .event
                    .as_ref()
                    .expect("an event")
                    .kind
                    .as_ref()
                    .expect("a kind")
                {
                    Kind::AgentLifecycleChanged(_) => "lifecycle",
                    Kind::AgentOutputChunk(_) => "output",
                    Kind::UsageRecorded(_) => "usage",
                    Kind::PermissionRequested(_) => "permission-requested",
                    Kind::PermissionDecided(_) => "permission-decided",
                    Kind::TurnInterrupted(_) => "interrupted",
                    _ => "other",
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn a_spawned_agent_streams_its_turn_into_the_journal() {
        let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi there")]));
        runtime
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        assert_eq!(runtime.running_agents(), 1);

        // starting, established(running), output, usage.
        let events = journal_at_least(&runtime, 4).await;
        assert_eq!(
            kinds(&events),
            vec!["lifecycle", "lifecycle", "output", "usage"],
            "the whole turn reaches the journal in order"
        );
        assert_eq!(
            runtime
                .journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            1
        );
        runtime.shutdown().await;
    }

    /// The at-least-once contract: a redelivered spawn must not start a second
    /// harness or append a second effect.
    #[tokio::test]
    async fn a_redelivered_spawn_neither_respawns_nor_rejournals() {
        let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
        runtime
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&runtime, 4).await.len();

        runtime
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("a redelivery is not an error");

        assert_eq!(runtime.running_agents(), 1, "one agent, not two");
        let after = runtime
            .journal
            .read_from(1, 100)
            .await
            .expect("a readable journal");
        assert_eq!(after.len(), settled, "nothing was re-journaled");
        runtime.shutdown().await;
    }

    /// A turn that proposes `Bash ls` as tool call `call-1` and blocks on the
    /// decision — the shape both permission tests drive.
    fn asking_permission() -> Script {
        Script::of(vec![ScriptedTurn::saying("checking").with_step(
            Step::AskPermission {
                tool_call_id: "call-1".to_owned(),
                tool_name: "Bash".to_owned(),
                input: json!({ "command": "ls" }),
            },
        )])
    }

    #[tokio::test]
    async fn a_permission_decision_unblocks_the_waiting_agent() {
        let mut runtime = runtime_with(asking_permission());
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");

        // starting, established, output, permission request — then blocked.
        let events = journal_at_least(&runtime, 4).await;
        assert!(
            kinds(&events).contains(&"permission-requested"),
            "the request must reach the journal or nobody can ever answer it: {:?}",
            kinds(&events)
        );

        runtime
            .apply(decision_command(2, "agent-1", "call-1", None))
            .await
            .expect("the decision applies");

        // The turn resumes and closes: a usage record arrives.
        let events = journal_at_least(&runtime, 5).await;
        let all = kinds(&events);
        assert!(
            all.contains(&"usage"),
            "the turn must end after the allow: {all:?}"
        );
        assert!(
            !all.contains(&"permission-decided"),
            "the decision is the Server's event; journaling it here doubles it"
        );
        runtime.shutdown().await;
    }

    /// The reject-don't-drop rule, end to end: a narrowing decision toward an
    /// adapter that cannot rewrite is refused loudly, never silently widened.
    #[tokio::test]
    async fn a_narrowing_decision_is_refused_when_the_adapter_cannot_rewrite() {
        let mut runtime = preauthorize_only_runtime(asking_permission());
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");
        journal_at_least(&runtime, 4).await;

        let error = runtime
            .apply(decision_command(
                2,
                "agent-1",
                "call-1",
                Some(Struct::default()),
            ))
            .await
            .expect_err("a narrowing this adapter cannot apply must be refused");
        assert!(matches!(error, WorkerError::Translate(_)), "got {error}");
        runtime.shutdown().await;
    }

    /// A command for an agent this Worker is not running is a race with the
    /// agent ending, not a failure: it applies (advancing the ack) and
    /// forwards to nobody.
    #[tokio::test]
    async fn a_command_for_an_unknown_agent_still_applies() {
        let mut runtime = runtime_with(Script::echoing());
        runtime
            .apply(StreamCommandsResponse {
                command_sequence: 1,
                command_id: "s1:9".to_owned(),
                command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                    agent_id: Some(AgentId::new("nobody")),
                    reason: "cleanup".to_owned(),
                })),
            })
            .await
            .expect("applies with nobody to tell");

        assert_eq!(
            runtime
                .journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            1,
            "the ack must advance or the server redelivers forever"
        );
        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn archiving_ends_the_agent_and_journals_the_transition() {
        let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
        runtime
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        journal_at_least(&runtime, 4).await;

        runtime
            .apply(StreamCommandsResponse {
                command_sequence: 2,
                command_id: "s1:8".to_owned(),
                command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                    agent_id: Some(AgentId::new("agent-1")),
                    reason: "done with it".to_owned(),
                })),
            })
            .await
            .expect("the archive applies");

        assert_eq!(runtime.running_agents(), 0);
        let events = journal_at_least(&runtime, 5).await;
        let archived = events
            .iter()
            .filter_map(|journaled| journaled.event.as_ref()?.kind.as_ref())
            .any(|kind| {
                matches!(
                    kind,
                    Kind::AgentLifecycleChanged(changed)
                        if changed.state
                            == i32::from(horde_proto::v1::AgentLifecycleState::Archived)
                            && changed.reason == "done with it"
                )
            });
        assert!(archived, "the archive is an event like everything else");
        // Shutdown must complete even though the pump was already retired.
        runtime.shutdown().await;
    }

    fn deliver_command(
        sequence: u64,
        agent: &str,
        body: deliver_message_command::Body,
        delivery: DeliveryMode,
    ) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("s1:{}", sequence + 200),
            command: Some(Command::DeliverMessage(DeliverMessageCommand {
                agent_id: Some(AgentId::new(agent)),
                body: Some(body),
                delivery: delivery.into(),
            })),
        }
    }

    /// An `agent-1` mid-conversation: spawned with an echoing script, its
    /// opening turn settled. Returns the runtime and the settled event count.
    async fn spawned_echoing_agent() -> (Runtime, usize) {
        let mut runtime = runtime_with(Script::echoing());
        runtime
            .apply(spawn_command(1, "agent-1", "first"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&runtime, 4).await.len();
        (runtime, settled)
    }

    /// The delivered text must reach the agent — the fake echoes it, and the
    /// echo can only get back to the journal through the whole pump.
    #[tokio::test]
    async fn a_delivered_note_reaches_the_agent_and_its_echo_the_journal() {
        let (mut runtime, settled) = spawned_echoing_agent().await;

        runtime
            .apply(deliver_command(
                2,
                "agent-1",
                deliver_message_command::Body::Note(horde_proto::v1::Note {
                    text: "a mid-life note".to_owned(),
                    payload: None,
                }),
                DeliveryMode::TurnBoundary,
            ))
            .await
            .expect("the delivery applies");

        let events = journal_at_least(&runtime, settled + 2).await;
        let echoed = events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("a mid-life note")
            )
        });
        assert!(
            echoed,
            "the note must reach the agent, not just the journal"
        );
        runtime.shutdown().await;
    }

    /// Did any journaled event mark the agent Failed, with a reason this
    /// predicate accepts? The loud-failure tests differ only in the reason.
    fn failed_with_reason(
        events: &[horde_proto::v1::JournaledEvent],
        reason: impl Fn(&str) -> bool,
    ) -> bool {
        events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentLifecycleChanged(changed))
                    if changed.state == i32::from(horde_proto::v1::AgentLifecycleState::Failed)
                        && reason(&changed.reason)
            )
        })
    }

    /// The delivery mode is load-bearing: an interrupt-mode message toward a
    /// provider that cannot interrupt must fail the agent loudly (the adapter
    /// refuses it), never quietly downgrade to a queued delivery. This is the
    /// observable difference between the Interrupt arm existing and not.
    #[tokio::test]
    async fn an_interrupt_delivery_is_refused_by_an_uninterruptible_provider() {
        let mut runtime = preauthorize_only_runtime(Script::echoing());
        runtime
            .apply(spawn_command(1, "agent-1", ""))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&runtime, 1).await.len();

        runtime
            .apply(deliver_command(
                2,
                "agent-1",
                deliver_message_command::Body::Assignment(Assignment {
                    assignment_id: None,
                    text: "stop and do this".to_owned(),
                    payload: None,
                }),
                DeliveryMode::Interrupt,
            ))
            .await
            .expect("the command applies; the refusal is the agent's failure");

        let events = journal_at_least(&runtime, settled + 1).await;
        let refused = failed_with_reason(&events, |reason| reason.contains("does not support"));
        assert!(
            refused,
            "an interrupt the provider cannot honour must fail loudly, not \
             quietly queue: {events:?}"
        );
        runtime.shutdown().await;
    }

    /// An interrupt command journals the interruption as its effect, with the
    /// reason and the agent's session — the audit trail of who stopped what.
    #[tokio::test]
    async fn an_interrupt_command_journals_the_interruption() {
        let (mut runtime, settled) = spawned_echoing_agent().await;

        runtime
            .apply(StreamCommandsResponse {
                command_sequence: 2,
                command_id: "s1:300".to_owned(),
                command: Some(Command::InterruptAgent(
                    horde_proto::v1::InterruptAgentCommand {
                        agent_id: Some(AgentId::new("agent-1")),
                        reason: "the user pressed escape".to_owned(),
                    },
                )),
            })
            .await
            .expect("the interrupt applies");

        let events = journal_at_least(&runtime, settled + 1).await;
        let interrupted = events.iter().find_map(|journaled| {
            let event = journaled.event.as_ref()?;
            match event.kind.as_ref()? {
                Kind::TurnInterrupted(interrupted) => Some((
                    interrupted.reason.clone(),
                    journaled.session_id.as_ref().map(ToString::to_string),
                )),
                _ => None,
            }
        });
        let (reason, session) = interrupted.expect("the interruption is journaled");
        assert_eq!(reason, "the user pressed escape");
        assert_eq!(
            session.as_deref(),
            Some("s1"),
            "the effect lands in the agent's own session log"
        );
        runtime.shutdown().await;
    }

    /// A decision for a call nobody is waiting on is a race, not a crash: the
    /// pump logs it and the agent lives on.
    #[tokio::test]
    async fn a_decision_for_no_outstanding_call_leaves_the_agent_alive() {
        let mut runtime = runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]));
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");
        journal_at_least(&runtime, 4).await;

        runtime
            .apply(decision_command(2, "agent-1", "no-such-call", None))
            .await
            .expect("a stray decision is not an error");

        assert_eq!(runtime.running_agents(), 1, "the agent must survive");
        assert_eq!(
            runtime.shutdown().await,
            1,
            "and its pump is still reapable"
        );
    }

    /// A dead harness fails the agent with a journaled reason — silence here
    /// would leave a permanently hung agent that looks merely idle.
    #[tokio::test]
    async fn a_message_after_the_harness_died_fails_the_agent_loudly() {
        let mut runtime =
            runtime_with(Script::of(vec![ScriptedTurn::saying("hi")]).ending_after_the_script());
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&runtime, 4).await.len();

        // The script is spent and the fake behaves as a dead harness; the
        // next delivery must surface that, not hang.
        runtime
            .apply(deliver_command(
                2,
                "agent-1",
                deliver_message_command::Body::Note(horde_proto::v1::Note {
                    text: "anyone home?".to_owned(),
                    payload: None,
                }),
                DeliveryMode::TurnBoundary,
            ))
            .await
            .expect("the delivery applies; the death is the agent's failure");

        let events = journal_at_least(&runtime, settled + 1).await;
        let failed = failed_with_reason(&events, |_| true);
        assert!(
            failed,
            "a dead harness must be journaled, not hidden: {events:?}"
        );
        runtime.shutdown().await;
    }

    /// The registered adapters flow into the advertisement — an empty
    /// iterator here would advertise a worker no harness can run on.
    #[tokio::test]
    async fn the_registered_adapters_are_what_the_advertisement_sees() {
        let runtime = runtime_with(Script::echoing());
        let advertised = crate::capabilities::advertise(
            runtime.adapters(),
            runtime.workspaces().map(|_| Vec::new()),
        );
        assert_eq!(advertised.harnesses.len(), 1);
        assert_eq!(advertised.harnesses[0].provider, "fake");
    }

    /// Shutdown reports how many pumps it reaped — with two agents running,
    /// anything but two means an agent leaked past its shutdown.
    #[tokio::test]
    async fn shutdown_reaps_every_running_agent() {
        let mut runtime = runtime_with(Script::echoing());
        runtime
            .apply(spawn_command(1, "agent-1", "one"))
            .await
            .expect("the first spawn applies");
        runtime
            .apply(spawn_command(2, "agent-2", "two"))
            .await
            .expect("the second spawn applies");

        assert_eq!(runtime.running_agents(), 2);
        assert_eq!(
            runtime.shutdown().await,
            2,
            "both pumps must be waited for, or an agent outlives its worker"
        );
    }

    /// `Debug` on a runtime is what an operator reads when a worker looks
    /// wedged: which adapters it holds and which agents it is running are the
    /// two facts that diagnose it — same precedent as the server's views.
    #[tokio::test]
    async fn the_debug_view_names_the_adapters_and_the_agents() {
        let mut runtime = runtime_with(Script::echoing());
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");

        let view = format!("{runtime:?}");
        assert!(view.contains("Runtime"), "got {view}");
        assert!(
            view.contains("fake"),
            "the adapters are half the diagnosis: {view}"
        );
        assert!(
            view.contains("agent-1"),
            "the agents are the other half: {view}"
        );
        runtime.shutdown().await;
    }

    /// The wire spawn maps onto the adapter spec field by field; a defaulted
    /// spec would run every agent on the default model under the ambient
    /// account with no overlay, silently.
    #[test]
    fn the_domain_spec_carries_binding_model_account_and_overlay() {
        let spawn = SpawnAgentCommand {
            binding: Some(horde_proto::v1::ModelBinding {
                tier: String::new(),
                provider: "fake".to_owned(),
                model: "fake-deep".to_owned(),
                provider_account: "work".to_owned(),
            }),
            prompt_overlay: "be brief".to_owned(),
            ..SpawnAgentCommand::default()
        };

        let spec = super::domain_spec(&spawn, None);
        assert_eq!(spec.model.as_deref(), Some("fake-deep"));
        assert_eq!(
            spec.account,
            Some(horde_adapter::ProviderAccount::new("work"))
        );
        assert_eq!(spec.append_system_prompt.as_deref(), Some("be brief"));
    }

    /// The empty-string cases are decisions, not accidents: an empty model or
    /// account means "the harness's default", and protobuf cannot say `None`.
    #[test]
    fn empty_wire_strings_mean_absent_not_empty() {
        let spec = super::domain_spec(&SpawnAgentCommand::default(), None);
        assert_eq!(spec.model, None, "no binding is not an empty model name");
        assert_eq!(spec.account, None, "no account is the ambient default");
        assert_eq!(
            spec.append_system_prompt, None,
            "no overlay is no flag at all, not an empty one"
        );
    }

    /// This Worker never advertised workspace support, so the command means
    /// the handshake promised something it cannot honour — fatal on purpose.
    #[tokio::test]
    async fn materialize_workspace_is_refused_loudly() {
        let mut runtime = runtime_with(Script::echoing());
        let error = runtime
            .apply(StreamCommandsResponse {
                command_sequence: 1,
                command_id: "s1:7".to_owned(),
                command: Some(Command::MaterializeWorkspace(
                    MaterializeWorkspaceCommand::default(),
                )),
            })
            .await
            .expect_err("acking unappliable work tells the server it happened");
        assert!(matches!(error, WorkerError::UnsupportedCommand(_)));
        assert_eq!(
            runtime
                .journal
                .applied_command_sequence()
                .await
                .expect("a watermark"),
            0,
            "a refused command is not acked"
        );
    }

    /// A single-commit origin, built with git itself.
    fn tiny_origin(root: &std::path::Path) -> std::path::PathBuf {
        let origin = root.join("origin");
        std::fs::create_dir_all(&origin).expect("origin dir");
        for arguments in [
            ["init", "-q", "-b", "main"].as_slice(),
            [
                "-c",
                "user.email=t@t",
                "-c",
                "user.name=t",
                "commit",
                "-qm",
                "seed",
                "--allow-empty",
            ]
            .as_slice(),
        ] {
            let status = std::process::Command::new("git")
                .args(arguments)
                .current_dir(&origin)
                .status()
                .expect("git runs");
            assert!(status.success(), "git {arguments:?} failed");
        }
        origin
    }

    /// A materialize command for one definition cloning `origin`.
    fn materialize_command(sequence: u64, origin: &std::path::Path) -> StreamCommandsResponse {
        StreamCommandsResponse {
            command_sequence: sequence,
            command_id: format!("s1:{sequence}"),
            command: Some(Command::MaterializeWorkspace(MaterializeWorkspaceCommand {
                definition: Some(horde_proto::v1::WorkspaceDefinition {
                    workspace_definition_id: Some(horde_proto::v1::WorkspaceDefinitionId::new(
                        "ws-1",
                    )),
                    name: "atlas".to_owned(),
                    repos: vec![horde_proto::v1::WorkspaceRepo {
                        name: "atlas".to_owned(),
                        remote_url: origin.display().to_string(),
                        default_branch: "main".to_owned(),
                        layout_path: String::new(),
                    }],
                    unmanaged_paths: Vec::new(),
                    created_at: None,
                    etag: "1".to_owned(),
                }),
                session_id: Some(SessionId::new("s1")),
            })),
        }
    }

    /// The full workspace path through the runtime: materialize journals what
    /// landed on disk, and the spawn that follows runs in its checkout.
    #[tokio::test]
    async fn a_materialize_then_spawn_runs_the_agent_in_its_checkout() {
        let temp = tempfile::tempdir().expect("a tempdir");
        let origin = tiny_origin(temp.path());
        let workspaces_root = temp.path().join("workspaces");
        let mut runtime =
            runtime_with(Script::of(vec![ScriptedTurn::saying("hi")])).with_workspaces(
                crate::workspace::WorkspaceManager::new("worker-a", workspaces_root.clone()),
            );

        runtime
            .apply(materialize_command(1, &origin))
            .await
            .expect("the materialize applies");

        let events = journal_at_least(&runtime, 1).await;
        match events[0]
            .event
            .as_ref()
            .and_then(|event| event.kind.as_ref())
        {
            Some(Kind::WorkspaceInstanceChanged(changed)) => {
                let report = changed.report.as_ref().expect("a report");
                assert_eq!(
                    report.state,
                    horde_proto::v1::WorkspaceInstanceState::Ready as i32,
                    "detail: {}",
                    report.detail
                );
            }
            other => panic!("expected a workspace instance report, got {other:?}"),
        }

        let spawn = workspace_spawn(2, "agent-1", "ws-1");
        runtime.apply(spawn).await.expect("the spawn applies");

        assert_eq!(runtime.running_agents(), 1);
        assert!(
            workspaces_root
                .join("ws-1")
                .join(".horde-agents")
                .join("agent-1")
                .join("atlas")
                .join(".git")
                .is_file(),
            "the agent's worktree checkout was composed before the harness started"
        );
        runtime.shutdown().await;
    }

    /// The advertisement builder reads workspace support from this accessor,
    /// so it has to report what the builder actually installed — a `None`
    /// here would un-advertise a capability the runtime honours.
    #[test]
    fn workspace_support_is_what_the_builder_installed() {
        let bare = Runtime::new(Journal::in_memory().expect("a journal"));
        assert!(bare.workspaces().is_none());

        let temp = tempfile::tempdir().expect("a tempdir");
        let with = Runtime::new(Journal::in_memory().expect("a journal")).with_workspaces(
            crate::workspace::WorkspaceManager::new("worker-a", temp.path()),
        );
        assert!(with.workspaces().is_some());
    }

    /// A spawn command whose spec names `workspace`, under the worktree
    /// policy — the shape the server sends once placement chose this worker.
    fn workspace_spawn(sequence: u64, agent: &str, workspace: &str) -> StreamCommandsResponse {
        let mut spawn = spawn_command(sequence, agent, "hello");
        if let Some(Command::SpawnAgent(command)) = &mut spawn.command
            && let Some(spec) = &mut command.spec
        {
            spec.workspace_definition_id =
                Some(horde_proto::v1::WorkspaceDefinitionId::new(workspace));
            spec.checkout_policy = horde_proto::v1::CheckoutPolicy::Worktree as i32;
        }
        spawn
    }

    /// Applies a spawn that must fail the *agent* and not the command loop,
    /// returning the journaled failure's reason for the caller to inspect.
    async fn failed_spawn_reason(runtime: &mut Runtime, command: StreamCommandsResponse) -> String {
        runtime
            .apply(command)
            .await
            .expect("the command applies; the agent is what failed");

        assert_eq!(runtime.running_agents(), 0);
        let events = journal_at_least(runtime, 1).await;
        match events[0]
            .event
            .as_ref()
            .and_then(|event| event.kind.as_ref())
        {
            Some(Kind::AgentLifecycleChanged(changed)) => {
                assert_eq!(
                    changed.state,
                    i32::from(horde_proto::v1::AgentLifecycleState::Failed)
                );
                changed.reason.clone()
            }
            other => panic!("expected a failed lifecycle, got {other:?}"),
        }
    }

    /// A spawn naming a workspace on a Worker with no workspace root fails
    /// the *agent* honestly rather than the command loop.
    #[tokio::test]
    async fn a_workspace_spawn_without_support_fails_the_agent_honestly() {
        let mut runtime = runtime_with(Script::echoing());
        let reason = failed_spawn_reason(&mut runtime, workspace_spawn(1, "agent-1", "ws-1")).await;
        assert!(
            reason.contains("workspace"),
            "the reason names the missing half: {reason}"
        );
    }

    /// A spawn that cannot start fails the *agent*, not the command loop: the
    /// command applies with the failure as its journaled effect.
    #[tokio::test]
    async fn a_spawn_naming_an_unknown_provider_fails_the_agent_not_the_loop() {
        let mut runtime = runtime_with(Script::echoing());
        let mut command = spawn_command(1, "agent-1", "hello");
        if let Some(Command::SpawnAgent(spawn)) = &mut command.command {
            spawn.binding = Some(horde_proto::v1::ModelBinding {
                provider: "codex".to_owned(),
                ..horde_proto::v1::ModelBinding::default()
            });
        }

        let reason = failed_spawn_reason(&mut runtime, command).await;
        assert!(
            reason.contains("codex"),
            "the reason names the missing adapter: {reason}"
        );
    }
}
