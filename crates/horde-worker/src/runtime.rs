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
use crate::journal::{AgentBookkeeping, AgentProgress, Journal, JournalEntry, RevivableAgent};
use crate::share::{AgentCheckout, ShareBroker, StampingSink};
use crate::translate::{AgentContext, failure_reason, journal_entries};

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
    /// Tracks every running agent's checkout and executes payload shares —
    /// stamping on the way out, obtaining on the way in.
    broker: Arc<ShareBroker>,
    /// Stashes outgoing attachment content and materializes incoming
    /// attachments into receiving checkouts — the blob half of a Payload,
    /// beside the broker's commit half.
    attachments: Arc<crate::blob::AttachmentBroker>,
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
    /// Revival bookkeeping that must commit with the applied-mark.
    bookkeeping: AgentBookkeeping,
    then: AfterCommit,
}

impl Applied {
    /// A command that applied without effects or follow-up — the race case,
    /// where the agent a command names is already gone.
    fn nothing() -> Self {
        Self {
            effects: Vec::new(),
            bookkeeping: AgentBookkeeping::None,
            then: AfterCommit::Nothing,
        }
    }
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
            broker: Arc::new(ShareBroker::new(None)),
            attachments: Arc::new(crate::blob::AttachmentBroker::new(None)),
            agents: BTreeMap::new(),
            retired: Vec::new(),
        }
    }

    /// Points attachment transfer at the Server's blob store, over the
    /// Worker's own authenticated channel. Without one, inline attachments
    /// still work in both directions and anything needing the store refuses
    /// honestly.
    #[must_use]
    pub fn with_blob_service(mut self, channel: tonic::transport::Channel) -> Self {
        self.attachments = Arc::new(crate::blob::AttachmentBroker::new(Some(channel)));
        self
    }

    /// Points payload shares at a Relay: stamped commits are pushed to their
    /// share refs, and commits absent from this Worker are fetched through
    /// it. Without one, only the local routes exist. Configure before any
    /// spawn — the broker is rebuilt, not updated.
    #[must_use]
    pub fn with_relay(mut self, relay: horde_relay::RelayLocation) -> Self {
        self.broker = Arc::new(ShareBroker::new(Some(relay)));
        self
    }

    /// The journal sink the Worker hands its MCP endpoint: results reported
    /// without a payload are stamped with the reporting agent's observed
    /// HEAD on their way into the journal ("the Worker's to stamp",
    /// horde-mcp's `report_result`).
    #[must_use]
    pub fn stamping_sink(&self) -> StampingSink {
        StampingSink::new(self.journal.sink(), Arc::clone(&self.broker))
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
            Command::DeliverMessage(deliver) => self.deliver_message(deliver).await,
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
            bookkeeping: AgentBookkeeping::None,
            then: AfterCommit::Nothing,
        })
    }

    async fn spawn_agent(&mut self, spawn: SpawnAgentCommand) -> Result<Applied, WorkerError> {
        let agent_id = required_id(spawn.agent_id.as_ref(), "agent id")?;
        let session_id = required_id(spawn.session_id.as_ref(), "session id")?;
        let context = context_of(&spawn, &session_id, &agent_id);

        // A spawn that cannot start is the *agent* failing, not the command
        // loop: the command still applies, with the failure as its journaled
        // effect, and the Server hears an honest lifecycle instead of losing
        // its Worker.
        let checkout = match self.agent_checkout(&spawn).await {
            Ok(checkout) => checkout,
            Err(error) => return Ok(spawn_failed(&context, &agent_id, &error)),
        };
        let cwd = checkout.as_ref().map(|checkout| checkout.root.clone());
        let (session, can_rewrite) = match self.start_session(&spawn, cwd).await {
            Ok(started) => started,
            Err(error) => return Ok(spawn_failed(&context, &agent_id, &error)),
        };
        // Registered only once the harness actually started: a checkout with
        // no agent behind it must not become a peer-fetch source or a
        // stamping target.
        if let Some(checkout) = checkout {
            self.broker.register(&agent_id, checkout);
        }

        let first_input = spawn
            .spec
            .as_ref()
            .and_then(|spec| spec.initial_assignment.as_ref())
            .map(|assignment| assignment.text.clone())
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
            // Remembered in the same transaction as the applied-mark: a spawn
            // this Worker acked but cannot rebuild is an agent the Server
            // believes in and nobody can ever revive.
            bookkeeping: AgentBookkeeping::Remember {
                agent_id: agent_id.clone(),
                session_id,
                spawn: Box::new(spawn),
            },
            then: AfterCommit::Start {
                agent_id,
                session,
                context,
                can_rewrite,
                first_input,
            },
        })
    }

    /// Resolves the adapter and starts the harness session in `cwd`.
    async fn start_session(
        &self,
        spawn: &SpawnAgentCommand,
        cwd: Option<std::path::PathBuf>,
    ) -> Result<(Box<dyn AgentSession>, bool), WorkerError> {
        let adapter = self.adapter_for(spawn)?;
        let spec = domain_spec(spawn, cwd);
        let session = adapter.spawn(&spec).await?;
        let can_rewrite = adapter
            .capabilities()
            .supports(Capability::RewritesToolInput);
        Ok((session, can_rewrite))
    }

    /// The adapter a spawn command binds, or the Worker's default.
    fn adapter_for(&self, spawn: &SpawnAgentCommand) -> Result<&Arc<dyn Adapter>, WorkerError> {
        let provider = spawn
            .binding
            .as_ref()
            .map(|binding| binding.provider.clone())
            .filter(|provider| !provider.is_empty())
            .or_else(|| self.default_provider.clone())
            .ok_or_else(|| WorkerError::UnknownProvider(String::new()))?;
        self.adapters
            .get(&provider)
            .ok_or(WorkerError::UnknownProvider(provider))
    }

    /// The checkout a spawn's workspace and Checkout Policy call for — its
    /// root is the agent's working directory, its repos the payload seam's
    /// map — or `None` when the spawn names no workspace and the agent runs
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
    ) -> Result<Option<AgentCheckout>, WorkerError> {
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
        let root = workspaces
            .checkout(&definition_id, &agent_id, policy)
            .await?;
        let repos = workspaces.instance_repos(&definition_id).await?;
        Ok(Some(AgentCheckout { root, repos }))
    }

    async fn deliver_message(
        &mut self,
        deliver: DeliverMessageCommand,
    ) -> Result<Applied, WorkerError> {
        let agent_id = required_id(deliver.agent_id.as_ref(), "agent id")?;

        let (text, payload) = match deliver.body {
            Some(deliver_message_command::Body::Assignment(assignment)) => {
                (assignment.text, assignment.payload)
            }
            Some(deliver_message_command::Body::Note(note)) => (note.text, note.payload),
            None => {
                tracing::warn!(
                    agent_id,
                    "a deliver command carried no message; applied empty"
                );
                return Ok(Applied::nothing());
            }
        };

        let input = match DeliveryMode::try_from(deliver.delivery) {
            Ok(DeliveryMode::Interrupt) => TurnInput::interrupting(text),
            // Turn-boundary is every provider's safe default, and an
            // unrecognized mode from a newer Server must not become an
            // interrupt nobody asked for.
            _ => TurnInput::queued(text),
        };

        if has_live_pump(&self.agents, &agent_id) {
            // A Payload's commits and attachments are made present for the
            // receiving agent *before* the message text reaches it — the
            // message names work, and handing the agent a name it cannot
            // open would be the transport becoming the agent's concern
            // (DESIGN §5).
            if let Some(payload) = carried_payload(payload.as_ref())
                && let Err(error) = self.obtain_payload(&agent_id, payload).await
            {
                return Ok(self.payload_failed(&agent_id, &error));
            }
            // No journal effect: the Server wrote AssignmentSent/NoteSent when
            // the message happened; that event is why this command exists.
            return Ok(Applied {
                effects: Vec::new(),
                bookkeeping: AgentBookkeeping::None,
                then: AfterCommit::Control {
                    agent_id,
                    control: AgentControl::Deliver(input),
                },
            });
        }

        // Nobody is running the agent — a dead harness, a retired pump, or a
        // Worker that restarted. Sending an agent another message is what
        // revives it (DESIGN.md §5), and the journal remembers how. The
        // payload rides along: it can only be obtained once the revival has
        // re-registered the agent's checkout.
        self.revive_and_deliver(agent_id, input, payload).await
    }

    /// Revives a remembered agent to deliver `input`, or journals why not.
    async fn revive_and_deliver(
        &mut self,
        agent_id: String,
        input: TurnInput,
        payload: Option<horde_proto::v1::Payload>,
    ) -> Result<Applied, WorkerError> {
        let Some(record) = self.journal.revivable_agent(&agent_id).await? else {
            // Never spawned here (or long archived): the race case, applied
            // with nobody to tell — the causing event is already in the log.
            tracing::warn!(
                agent_id,
                "a message for an agent this worker neither runs nor remembers"
            );
            return Ok(Applied::nothing());
        };

        let context = context_of(&record.spawn, &record.session_id, &agent_id);
        if record.provider_session_id.is_none() {
            // The harness died before ever naming its conversation, so there
            // is nothing to resume — said loudly, or the sender reads an
            // unreachable agent as one that is merely slow.
            return Ok(Applied {
                effects: vec![lifecycle_entry(
                    &context,
                    String::new(),
                    AgentLifecycleState::Unspecified,
                    AgentLifecycleState::Failed,
                    "cannot revive: no provider session was ever established".to_owned(),
                )],
                bookkeeping: AgentBookkeeping::None,
                then: AfterCommit::Nothing,
            });
        }

        match self.resume_session(&record).await {
            Ok((session, can_rewrite, checkout)) => {
                // Re-registered before the payload is obtained: the broker
                // routes the obtain through the receiver's own checkout.
                if let Some(checkout) = checkout {
                    self.broker.register(&agent_id, checkout);
                }
                if let Some(payload) = carried_payload(payload.as_ref())
                    && let Err(error) = self.obtain_payload(&agent_id, payload).await
                {
                    // The revived checkout must not linger as a stamping
                    // target for an agent whose pump never started.
                    self.broker.forget(&agent_id);
                    return Ok(self.payload_failed(&agent_id, &error));
                }
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
                    then: AfterCommit::Start {
                        agent_id,
                        session,
                        context,
                        can_rewrite,
                        first_input: Some(input),
                    },
                })
            }
            Err(error) => Ok(Applied {
                // The revival failing is the *agent's* failure, journaled so
                // the sender learns the message has no one to reach — silence
                // here would read as an agent that is merely slow.
                effects: vec![lifecycle_entry(
                    &context,
                    String::new(),
                    AgentLifecycleState::Unspecified,
                    AgentLifecycleState::Failed,
                    error.to_string(),
                )],
                bookkeeping: AgentBookkeeping::None,
                then: AfterCommit::Nothing,
            }),
        }
    }

    /// Resumes the provider conversation a remembered agent names.
    ///
    /// The caller has already established that a provider session id exists;
    /// an empty one here would resume nothing and is refused by the adapter.
    async fn resume_session(
        &self,
        record: &RevivableAgent,
    ) -> Result<(Box<dyn AgentSession>, bool, Option<AgentCheckout>), WorkerError> {
        let provider_session_id = record.provider_session_id.as_deref().unwrap_or_default();
        let adapter = self.adapter_for(&record.spawn)?;
        let checkout = self.agent_checkout(&record.spawn).await?;
        let cwd = checkout.as_ref().map(|checkout| checkout.root.clone());
        // One past the recorded count: a turn that died half-journaled has
        // already spent its ordinal, and reusing it would hand the revived
        // turn journal keys the Server has partially seen. Turn ids need
        // uniqueness, not density, so the skip is free.
        let spec = domain_spec(&record.spawn, cwd).with_turns_taken(record.turns_taken + 1);
        let session = adapter
            .resume(
                &spec,
                &horde_adapter::ProviderSessionId::new(provider_session_id),
            )
            .await?;
        let can_rewrite = adapter
            .capabilities()
            .supports(Capability::RewritesToolInput);
        Ok((session, can_rewrite, checkout))
    }

    /// Makes one delivered Payload wholly present for its receiver: commits
    /// into the checkout through the share broker, attachments as files
    /// under the checkout root through the attachment broker.
    ///
    /// Attachments need somewhere to land, so a payload carrying them for
    /// an agent with no checkout is refused rather than half-delivered —
    /// the same honesty `payload_failed` gives an unobtainable commit.
    async fn obtain_payload(
        &self,
        agent_id: &str,
        payload: &horde_proto::v1::Payload,
    ) -> Result<(), WorkerError> {
        self.broker.obtain(agent_id, payload).await?;
        if payload.attachments.is_empty() {
            return Ok(());
        }
        let root = self
            .broker
            .checkout_of(agent_id)
            .map(|checkout| checkout.root)
            .ok_or_else(|| {
                WorkerError::Attachment(format!(
                    "agent {agent_id} has no checkout to receive {} attachment(s) into",
                    payload.attachments.len()
                ))
            })?;
        self.attachments
            .materialize(&payload.attachments, &root)
            .await?;
        Ok(())
    }

    /// A delivery whose Payload could not be obtained fails the *agent*,
    /// loudly: the message names work the agent cannot see, and delivering
    /// the text anyway would have it act on a payload it cannot check out —
    /// a silent drop with a delayed symptom. The command still applies, the
    /// failure is its journaled effect, and the pump is retired.
    fn payload_failed(&self, agent_id: &str, error: &WorkerError) -> Applied {
        let session_id = self
            .agents
            .get(agent_id)
            .map(|handle| handle.session_id.clone())
            .unwrap_or_default();
        Applied {
            effects: vec![JournalEntry {
                session_id,
                agent_id: Some(agent_id.to_owned()),
                idempotency_key: String::new(),
                kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                    previous_state: AgentLifecycleState::Running.into(),
                    state: AgentLifecycleState::Failed.into(),
                    reason: format!("a delivered payload could not be obtained: {error}"),
                }),
            }],
            // Failed, not archived: the context survives, so another message
            // (with an obtainable payload) may still revive the agent.
            bookkeeping: AgentBookkeeping::None,
            then: AfterCommit::Retire {
                agent_id: agent_id.to_owned(),
            },
        }
    }

    fn interrupt_agent(&mut self, interrupt: &InterruptAgentCommand) -> Applied {
        let Some(agent_id) = known_agent(&self.agents, interrupt.agent_id.as_ref()) else {
            return Applied::nothing();
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
            bookkeeping: AgentBookkeeping::None,
            then: AfterCommit::Control {
                agent_id,
                control: AgentControl::Interrupt,
            },
        }
    }

    fn archive_agent(&mut self, archive: &ArchiveAgentCommand) -> Applied {
        // Forgotten whether or not the agent is still running here: context
        // dies at archive, so a revivable-but-idle agent must stop being
        // revivable even though there is no pump left to tell.
        let agent_id = archive
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        if !self.agents.contains_key(&agent_id) {
            return Applied {
                effects: Vec::new(),
                bookkeeping: AgentBookkeeping::Forget {
                    agent_id: agent_id.clone(),
                },
                then: AfterCommit::Nothing,
            };
        }

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
            bookkeeping: AgentBookkeeping::Forget {
                agent_id: agent_id.clone(),
            },
            then: AfterCommit::Retire { agent_id },
        }
    }

    fn cancel_placement(&mut self, cancel: &CancelPlacementCommand) -> Applied {
        // A placement cancelled before its spawn command arrived names an
        // agent this Worker never started; one cancelled after is an archive
        // in different clothes. PlacementChanged events are Server-authored,
        // so there is nothing to journal either way.
        let agent_id = cancel
            .agent_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        let then = if self.agents.contains_key(&agent_id) {
            AfterCommit::Retire {
                agent_id: agent_id.clone(),
            }
        } else {
            AfterCommit::Nothing
        };
        Applied {
            effects: Vec::new(),
            // The agent no longer runs here, so this Worker must not keep the
            // power to revive it somewhere the scheduler moved it away from.
            bookkeeping: AgentBookkeeping::Forget { agent_id },
            then,
        }
    }

    fn deliver_permission_decision(
        &mut self,
        deliver: DeliverPermissionDecisionCommand,
    ) -> Result<Applied, WorkerError> {
        let Some(agent_id) = known_agent(&self.agents, deliver.agent_id.as_ref()) else {
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
            bookkeeping: AgentBookkeeping::None,
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
                let previous = self.agents.insert(
                    agent_id,
                    AgentHandle {
                        control,
                        session_id,
                        can_rewrite,
                        task,
                    },
                );
                if let Some(previous) = previous {
                    // A revival replacing a dead pump: the old task is done or
                    // about to be, but shutdown still owes it a join.
                    self.retired.push(previous.task);
                }
            }
            AfterCommit::Control { agent_id, control } => {
                if let Some(handle) = self.agents.get(&agent_id)
                    && handle.control.send(control).await.is_err()
                {
                    tracing::warn!(agent_id, "control for an agent whose pump already ended");
                }
            }
            AfterCommit::Retire { agent_id } => {
                // The checkout registry must not outlive the agent: a stale
                // entry would stamp an archived agent's HEAD, or offer its
                // checkout as a peer-fetch source, forever.
                self.broker.forget(&agent_id);
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
                applied.bookkeeping,
            )
            .await?;
        self.after_commit(applied.then).await;
        Ok(())
    }
}

/// The applied form of a spawn that could not start: the command applies,
/// with the agent's failure as its journaled effect — the Server hears an
/// honest lifecycle instead of losing its Worker.
fn spawn_failed(context: &AgentContext, agent_id: &str, error: &WorkerError) -> Applied {
    Applied {
        effects: vec![lifecycle_entry(
            context,
            format!("{agent_id}:spawn-failed"),
            AgentLifecycleState::Queued,
            AgentLifecycleState::Failed,
            error.to_string(),
        )],
        // An agent that never started has nothing to revive; remembering it
        // would promise a revival the journal cannot deliver.
        bookkeeping: AgentBookkeeping::None,
        then: AfterCommit::Nothing,
    }
}

/// The payload a message actually carries, if it names any commits.
fn carried_payload(
    payload: Option<&horde_proto::v1::Payload>,
) -> Option<&horde_proto::v1::Payload> {
    payload.filter(|payload| !payload.git_refs.is_empty() || !payload.attachments.is_empty())
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

/// The standing facts a spawn command carries, as the pump journals them.
///
/// Shared between the spawn and revival paths — a revived agent's events must
/// journal under exactly the facts its spawn established, or attribution
/// changes across an incarnation boundary.
fn context_of(spawn: &SpawnAgentCommand, session_id: &str, agent_id: &str) -> AgentContext {
    AgentContext {
        session_id: session_id.to_owned(),
        agent_id: agent_id.to_owned(),
        binding: spawn.binding.clone(),
        permission_mode: spawn
            .spec
            .as_ref()
            .map(|spec| spec.permission_mode.clone())
            .unwrap_or_default(),
        tree_path: spawn.tree_path.clone(),
    }
}

/// Whether a pump is actually alive to take a delivery.
///
/// Presence in the map is not enough: a pump whose harness died has ended,
/// but its handle lingers until something replaces it. Both facts matter —
/// a missing handle and a closed channel are each an agent nobody is running,
/// and each is a revival case, never a message to drop.
fn has_live_pump(agents: &BTreeMap<String, AgentHandle>, agent_id: &str) -> bool {
    agents
        .get(agent_id)
        .is_some_and(|handle| !handle.control.is_closed())
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
        // The named mode rides to the adapter because for a sandboxed-at-spawn
        // provider (Codex) it is what the sandbox tier is derived from; empty
        // means the spawn named none, which must read as absent, not as a mode
        // called "".
        permission_mode: spawn
            .spec
            .as_ref()
            .map(|spec| spec.permission_mode.clone())
            .filter(|mode| !mode.is_empty()),
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
    session: Box<dyn AgentSession>,
    journal: Journal,
    context: AgentContext,
    mut control: mpsc::Receiver<AgentControl>,
) {
    let mut pump = Pump {
        session,
        journal,
        context,
        state: PumpState::Idle,
        turns_in_flight: 0,
        queued: std::collections::VecDeque::new(),
    };
    loop {
        let step = if pump.state == PumpState::Streaming {
            tokio::select! {
                // Biased toward control so a shutdown or interrupt is never
                // starved by a chatty harness.
                biased;
                message = control.recv() => Step::Control(message),
                event = pump.session.next_event() => Step::Event(event),
            }
        } else {
            Step::Control(control.recv().await)
        };

        let flow = match step {
            Step::Control(message) => pump.apply_control(message).await,
            Step::Event(event) => pump.apply_event(event).await,
        };
        match flow {
            Flow::Continue => {}
            Flow::Stop => return,
        }
    }
}

/// What the pump does after handling one step.
enum Flow {
    Continue,
    Stop,
}

/// One agent's pump: the session it owns, and the steering it holds.
struct Pump {
    session: Box<dyn AgentSession>,
    journal: Journal,
    context: AgentContext,
    state: PumpState,
    /// Turns sent whose `TurnEnded` has not arrived yet. Usually zero or one,
    /// but an interrupting send makes it two for a moment: the aborted turn's
    /// ending and the new turn's events both still owe the stream something,
    /// and a pump that went idle on the first ending would strand the second
    /// turn unread.
    turns_in_flight: u32,
    /// Steering held for the next turn boundary (DESIGN.md §11): a
    /// turn-boundary message that arrives mid-turn waits *here*, in the
    /// orchestrator, because holding the message is the one mechanism that
    /// works against every provider. Interrupt-mode deliveries never queue.
    queued: std::collections::VecDeque<TurnInput>,
}

impl Pump {
    /// Obeys one control message.
    async fn apply_control(&mut self, message: Option<AgentControl>) -> Flow {
        match message {
            // The Runtime dropped this agent (archive, cancel, shutdown).
            None | Some(AgentControl::Archive) => {
                let _ = self.session.shutdown().await;
                Flow::Stop
            }
            Some(AgentControl::Deliver(input)) => {
                if should_hold_for_boundary(&input, self.state) {
                    self.queued.push_back(input);
                    return Flow::Continue;
                }
                self.deliver(input).await
            }
            Some(AgentControl::Answer {
                tool_call_id,
                decision,
            }) => match self
                .session
                .answer_permission(&tool_call_id, decision)
                .await
            {
                Ok(()) => {
                    self.state = PumpState::Streaming;
                    Flow::Continue
                }
                Err(error) => {
                    // A decision for a call that is not waiting (raced with an
                    // interrupt, or duplicated). The agent is not broken;
                    // saying so in the log is the whole handling.
                    tracing::warn!(agent_id = %self.context.agent_id, %error, "permission answer went nowhere");
                    Flow::Continue
                }
            },
            Some(AgentControl::Interrupt) => {
                if let Err(error) = self.session.interrupt().await {
                    tracing::warn!(agent_id = %self.context.agent_id, %error, "interrupt failed");
                }
                // The interrupted turn still closes with TurnEnded, which
                // arrives as an ordinary event — so the pump must be willing
                // to read one even if it was parked on a decision that will
                // now never be answered.
                self.state = state_after_interrupt(self.state);
                Flow::Continue
            }
        }
    }

    /// Sends one message, failing the agent loudly if the harness is gone.
    async fn deliver(&mut self, input: TurnInput) -> Flow {
        let interrupting = input.delivery == horde_adapter::Delivery::Interrupt;
        match self.session.send(input).await {
            Ok(()) => {
                self.turns_in_flight =
                    turns_owed_after_send(interrupting, self.state, self.turns_in_flight);
                self.state = PumpState::Streaming;
                Flow::Continue
            }
            Err(error) => {
                self.record_failure(&failure_reason(&error)).await;
                let _ = self.session.shutdown().await;
                Flow::Stop
            }
        }
    }

    /// Journals one session event, or the session's end.
    async fn apply_event(
        &mut self,
        event: Result<Option<AgentEvent>, horde_adapter::AdapterError>,
    ) -> Flow {
        match event {
            Ok(Some(event)) => {
                if matches!(event, AgentEvent::TurnEnded(_)) {
                    self.turns_in_flight = self.turns_in_flight.saturating_sub(1);
                }
                self.state = state_after(&event, self.turns_in_flight);
                let flow = self.journal_event(&event).await;
                if matches!(flow, Flow::Stop) {
                    return Flow::Stop;
                }
                // The turn boundary: whatever steering was held for it goes
                // out now, before the harness can start anything else.
                if self.state == PumpState::Idle
                    && let Some(next) = self.queued.pop_front()
                {
                    return self.deliver(next).await;
                }
                Flow::Continue
            }
            Ok(None) => {
                // EOF mid-conversation: the harness is gone.
                self.record_failure(&format!(
                    "{}: the harness ended its event stream",
                    crate::translate::PROCESS_DEATH
                ))
                .await;
                Flow::Stop
            }
            Err(error) => {
                self.record_failure(&failure_reason(&error)).await;
                let _ = self.session.shutdown().await;
                Flow::Stop
            }
        }
    }

    /// Journals one event's entries and progress atomically.
    async fn journal_event(&mut self, event: &AgentEvent) -> Flow {
        match journal_entries(&self.context, event) {
            Ok(entries) => {
                let progress = agent_progress(&self.context.agent_id, event);
                if entries.is_empty() && progress == AgentProgress::None {
                    return Flow::Continue;
                }
                if let Err(error) = self.journal.record_batch(entries, progress).await {
                    // A journal that cannot append has lost the
                    // partition-survival property; running an agent whose
                    // evidence is being dropped is worse than stopping it.
                    tracing::error!(agent_id = %self.context.agent_id, %error, "journal append failed; stopping the agent");
                    let _ = self.session.shutdown().await;
                    return Flow::Stop;
                }
                Flow::Continue
            }
            Err(error) => {
                tracing::error!(agent_id = %self.context.agent_id, %error, "an event could not be translated and was not journaled");
                Flow::Continue
            }
        }
    }

    /// Journals this agent's failure, best-effort — the pump is already dying.
    async fn record_failure(&mut self, reason: &str) {
        let entry = JournalEntry {
            session_id: self.context.session_id.clone(),
            agent_id: Some(self.context.agent_id.clone()),
            idempotency_key: String::new(),
            kind: Kind::AgentLifecycleChanged(AgentLifecycleChanged {
                previous_state: AgentLifecycleState::Running.into(),
                state: AgentLifecycleState::Failed.into(),
                reason: reason.to_owned(),
            }),
        };
        if let Err(error) = self.journal.record(entry).await {
            tracing::error!(agent_id = %self.context.agent_id, %error, "could not journal an agent failure");
        }
    }
}

/// Whether a delivery waits for the turn boundary instead of going out now.
///
/// Both halves carry weight, and inline they are not separately reachable in
/// most runs: a turn-boundary message *during* a turn is held (that is what
/// steering means by default), while an interrupt-mode message never waits and
/// an idle agent never makes anything wait.
fn should_hold_for_boundary(input: &TurnInput, state: PumpState) -> bool {
    let mid_turn = state != PumpState::Idle;
    let boundary_delivery = input.delivery == horde_adapter::Delivery::TurnBoundary;
    mid_turn && boundary_delivery
}

/// How many turn endings a send leaves outstanding.
///
/// An interrupting send into a live turn aborts it AND starts its own, so
/// both endings are owed; every other send owes exactly one. Both halves of
/// the condition matter and are not separately reachable through the pump —
/// it never sends a queued message mid-turn (it queues them itself), and an
/// idle pump has nothing in flight to add to — which is exactly the shape
/// that hides a flipped operator from an end-to-end suite.
fn turns_owed_after_send(interrupting: bool, state: PumpState, owed: u32) -> u32 {
    if interrupting && state != PumpState::Idle {
        owed.saturating_add(1)
    } else {
        1
    }
}

/// Where the pump stands after asking for an interrupt.
///
/// A pump parked on a permission decision must go back to reading events:
/// the interrupt ends the turn, that ending arrives as an ordinary event, and
/// a pump still waiting for a decision nobody will ever send would hold the
/// agent forever. A streaming pump keeps streaming, and an idle one stays
/// idle — there is no turn whose ending it should go looking for.
fn state_after_interrupt(state: PumpState) -> PumpState {
    match state {
        PumpState::AwaitingDecision => PumpState::Streaming,
        other => other,
    }
}

/// Where the pump stands after seeing `event`, given how many turns still
/// owe their ending.
///
/// A `TurnEnded` is the boundary only when nothing else is in flight: after
/// an interrupting send, the aborted turn's ending arrives while the new
/// turn is still running, and a pump that went idle there would never read
/// the new turn's events.
fn state_after(event: &AgentEvent, turns_in_flight: u32) -> PumpState {
    match event {
        AgentEvent::PermissionRequested { .. } => PumpState::AwaitingDecision,
        AgentEvent::TurnEnded(_) if turns_in_flight == 0 => PumpState::Idle,
        _ => PumpState::Streaming,
    }
}

/// The revival bookkeeping one event advances, keyed to commit with it.
fn agent_progress(agent_id: &str, event: &AgentEvent) -> AgentProgress {
    match event {
        AgentEvent::SessionEstablished {
            provider_session_id,
            ..
        } => AgentProgress::Established {
            agent_id: agent_id.to_owned(),
            provider_session_id: provider_session_id.to_string(),
        },
        AgentEvent::TurnEnded(_) => AgentProgress::TurnTaken {
            agent_id: agent_id.to_owned(),
        },
        _ => AgentProgress::None,
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

    use super::{Runtime, carried_payload};
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
    fn the_domain_spec_carries_binding_model_account_overlay_and_mode() {
        let spawn = SpawnAgentCommand {
            binding: Some(horde_proto::v1::ModelBinding {
                tier: String::new(),
                provider: "fake".to_owned(),
                model: "fake-deep".to_owned(),
                provider_account: "work".to_owned(),
            }),
            prompt_overlay: "be brief".to_owned(),
            spec: Some(horde_proto::v1::AgentSpec {
                permission_mode: "cautious".to_owned(),
                ..horde_proto::v1::AgentSpec::default()
            }),
            ..SpawnAgentCommand::default()
        };

        let spec = super::domain_spec(&spawn, None);
        assert_eq!(spec.model.as_deref(), Some("fake-deep"));
        assert_eq!(
            spec.account,
            Some(horde_adapter::ProviderAccount::new("work"))
        );
        assert_eq!(spec.append_system_prompt.as_deref(), Some("be brief"));
        assert_eq!(
            spec.permission_mode.as_deref(),
            Some("cautious"),
            "the mode is what a sandboxed-at-spawn provider derives its tier from"
        );
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
        assert_eq!(
            spec.permission_mode, None,
            "an empty mode name is a spawn that named none, not a mode called \"\""
        );
    }

    // -- Steering, interrupt, and revival --------------------------------------

    /// The four corners of the boundary-hold predicate. Inline as
    /// `state != Idle && delivery == TurnBoundary` the corners are not
    /// separately reachable in most runs, which is exactly how a flipped
    /// operator survives an end-to-end suite.
    #[test]
    fn steering_holds_only_boundary_messages_during_a_turn() {
        use super::{PumpState, should_hold_for_boundary};
        use horde_adapter::TurnInput;

        let queued = TurnInput::queued("later");
        let interrupting = TurnInput::interrupting("now");

        assert!(should_hold_for_boundary(&queued, PumpState::Streaming));
        assert!(should_hold_for_boundary(
            &queued,
            PumpState::AwaitingDecision
        ));
        assert!(
            !should_hold_for_boundary(&queued, PumpState::Idle),
            "an idle agent takes its message immediately"
        );
        assert!(
            !should_hold_for_boundary(&interrupting, PumpState::Streaming),
            "an interrupting message never waits — waiting is what it exists to not do"
        );
    }

    /// The interrupt state table. The AwaitingDecision arm is the load-bearing
    /// one: an interrupt aborts the turn a decision was blocking, so a pump
    /// still parked on that decision would wait forever for an answer nobody
    /// owes — while Idle must NOT start polling a harness that owes nothing.
    #[test]
    fn an_interrupt_unparks_only_a_decision_wait() {
        use super::{PumpState, state_after_interrupt};

        assert_eq!(
            state_after_interrupt(PumpState::AwaitingDecision),
            PumpState::Streaming
        );
        assert_eq!(
            state_after_interrupt(PumpState::Streaming),
            PumpState::Streaming
        );
        assert_eq!(state_after_interrupt(PumpState::Idle), PumpState::Idle);
    }

    /// The send-side ledger. The unreachable corners are pinned here because
    /// the pump's own discipline (queue mid-turn, idle means nothing owed)
    /// keeps every live run away from them.
    #[test]
    fn a_send_owes_one_ending_unless_it_also_aborted_a_turn() {
        use super::{PumpState, turns_owed_after_send};

        // The interrupting-send-into-a-live-turn case: two endings owed.
        assert_eq!(turns_owed_after_send(true, PumpState::Streaming, 1), 2);
        assert_eq!(
            turns_owed_after_send(true, PumpState::AwaitingDecision, 1),
            2
        );
        // A queued send owes one, whatever was in flight before it.
        assert_eq!(
            turns_owed_after_send(false, PumpState::Streaming, 1),
            1,
            "a queued send aborts nothing and must not inflate the ledger"
        );
        // An idle interrupting send has nothing to abort.
        assert_eq!(turns_owed_after_send(true, PumpState::Idle, 0), 1);
    }

    /// The turn-ending state table: an ending is the boundary only when no
    /// other turn still owes its ending — the interrupting-send case, where
    /// the aborted turn ends while the steered turn is still running.
    #[test]
    fn a_turn_ending_is_the_boundary_only_when_nothing_else_is_in_flight() {
        use core::time::Duration;

        use horde_adapter::{TurnId, TurnOutcome, TurnStatus, UsageDelta};

        use super::{PumpState, state_after};

        let ended = horde_adapter::AgentEvent::TurnEnded(TurnOutcome {
            turn_id: TurnId::new("t1"),
            status: TurnStatus::Interrupted,
            text: None,
            usage: UsageDelta::default(),
            denials: Vec::new(),
            duration: Duration::from_millis(1),
        });

        assert_eq!(state_after(&ended, 0), PumpState::Idle);
        assert_eq!(
            state_after(&ended, 1),
            PumpState::Streaming,
            "the steered turn is still running; going idle would strand it unread"
        );
    }

    /// An `agent-1` blocked mid-turn on permission call `call-1`, plus the
    /// settled journal length — the preamble every steering and interrupt
    /// test shares.
    async fn blocked_agent() -> (Runtime, usize) {
        let mut runtime = runtime_with(asking_permission());
        runtime
            .apply(spawn_command(1, "agent-1", "go"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&runtime, 4).await.len();
        (runtime, settled)
    }

    /// A turn-boundary note to `agent-1` — the routine delivery the revival
    /// and steering tests keep sending.
    fn note_command(sequence: u64, text: &str) -> StreamCommandsResponse {
        deliver_command(
            sequence,
            "agent-1",
            deliver_message_command::Body::Note(horde_proto::v1::Note {
                text: text.to_owned(),
                payload: None,
            }),
            DeliveryMode::TurnBoundary,
        )
    }

    /// A restarted Worker: a fresh runtime over `journal` with an echoing
    /// adapter, that has just been asked to deliver a note to `agent-1`.
    async fn restarted_runtime_delivering(journal: &Journal, sequence: u64) -> Runtime {
        let mut second = Runtime::new(journal.clone())
            .with_adapter(Arc::new(FakeAdapter::new(Script::echoing())));
        second
            .apply(note_command(sequence, "hello?"))
            .await
            .expect("the delivery applies");
        second
    }

    /// Steering, end to end: a turn-boundary message that arrives while the
    /// agent is blocked mid-turn is held by the pump and delivered at the turn
    /// boundary — after the blocked turn ends, never into the middle of it.
    #[tokio::test]
    async fn a_boundary_message_during_a_blocked_turn_waits_for_the_boundary() {
        let (mut runtime, _settled) = blocked_agent().await;

        // Mid-turn, blocked on the decision: steer.
        runtime
            .apply(note_command(2, "steered note"))
            .await
            .expect("the delivery applies");

        // Only the decision releases the turn; then the held note plays.
        runtime
            .apply(decision_command(3, "agent-1", "call-1", None))
            .await
            .expect("the decision applies");

        let events = journal_at_least(&runtime, 8).await;
        let all = kinds(&events);
        let first_usage = all
            .iter()
            .position(|kind| *kind == "usage")
            .expect("the blocked turn must end: {all:?}");
        let echo = events
            .iter()
            .position(|journaled| {
                matches!(
                    journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                    Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("steered note")
                )
            })
            .expect("the held note must reach the agent after the boundary");
        assert!(
            echo > first_usage,
            "the steered note must not enter the blocked turn: {all:?}"
        );
        runtime.shutdown().await;
    }

    /// The interrupt path out of a blocked turn: the turn ends (usage lands)
    /// and the agent takes the next message — a pump still parked on the
    /// now-dead decision would hang both.
    #[tokio::test]
    async fn an_interrupt_releases_an_agent_blocked_on_a_decision() {
        let (mut runtime, settled) = blocked_agent().await;

        runtime
            .apply(StreamCommandsResponse {
                command_sequence: 2,
                command_id: "s1:301".to_owned(),
                command: Some(Command::InterruptAgent(
                    horde_proto::v1::InterruptAgentCommand {
                        agent_id: Some(AgentId::new("agent-1")),
                        reason: "escape".to_owned(),
                    },
                )),
            })
            .await
            .expect("the interrupt applies");

        // interrupted + usage: the aborted turn still closes.
        let events = journal_at_least(&runtime, settled + 2).await;
        assert!(
            kinds(&events).contains(&"usage"),
            "the aborted turn must end and bill: {:?}",
            kinds(&events)
        );

        // And the agent is idle, not wedged: a follow-up message echoes.
        runtime
            .apply(note_command(3, "after the interrupt"))
            .await
            .expect("the delivery applies");
        let events = journal_at_least(&runtime, settled + 4).await;
        assert!(
            events.iter().any(|journaled| matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("after the interrupt")
            )),
            "the interrupted agent must remain steerable"
        );
        runtime.shutdown().await;
    }

    /// An interrupt-mode delivery cuts through a blocked turn immediately:
    /// the blocked turn ends, the message starts its own.
    #[tokio::test]
    async fn an_interrupting_message_cuts_through_a_blocked_turn() {
        let (mut runtime, settled) = blocked_agent().await;

        runtime
            .apply(deliver_command(
                2,
                "agent-1",
                deliver_message_command::Body::Assignment(Assignment {
                    assignment_id: None,
                    text: "drop that and do this".to_owned(),
                    payload: None,
                }),
                DeliveryMode::Interrupt,
            ))
            .await
            .expect("the delivery applies");

        let events = journal_at_least(&runtime, settled + 3).await;
        let all = kinds(&events);
        assert!(
            all.contains(&"usage"),
            "the aborted turn still closes: {all:?}"
        );
        assert!(
            events.iter().any(|journaled| matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("drop that and do this")
            )),
            "the interrupting message must start its own turn: {all:?}"
        );
        runtime.shutdown().await;
    }

    /// Revival, the headline path: the Worker restarts, its agents map is
    /// empty, and a message for a remembered agent revives the conversation —
    /// with the turn numbering continued, so nothing the revived turn journals
    /// collides with keys the first incarnation already used.
    #[tokio::test]
    async fn a_message_after_a_worker_restart_revives_the_agent() {
        let journal = Journal::in_memory().expect("an in-memory journal");
        let script = || {
            Script::of(vec![
                ScriptedTurn::saying("turn one"),
                ScriptedTurn::saying("turn two"),
                ScriptedTurn::saying("turn three"),
            ])
        };

        let mut first =
            Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(script())));
        first
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&first, 4).await.len();
        first.shutdown().await;

        // The restarted Worker: same journal, fresh runtime, no agents.
        let mut second =
            Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(script())));
        assert_eq!(second.running_agents(), 0);

        second
            .apply(note_command(2, "are you still there?"))
            .await
            .expect("the delivery applies by reviving");

        assert_eq!(second.running_agents(), 1, "the agent lives again");
        let events = journal_at_least(&second, settled + 4).await;

        let revived = journaled_lifecycle(
            &second,
            settled + 4,
            horde_proto::v1::AgentLifecycleState::Starting,
            "revived",
        )
        .await;
        assert!(
            revived,
            "the revival is journaled as a lifecycle transition"
        );

        let continued = events.iter().find_map(|journaled| {
            match journaled
                .event
                .as_ref()
                .and_then(|event| event.kind.as_ref())
            {
                Some(Kind::AgentOutputChunk(chunk)) if chunk.text.contains("turn three") => {
                    Some(chunk.turn_id.clone())
                }
                _ => None,
            }
        });
        assert_eq!(
            continued.as_deref(),
            Some("fake-session-1#3"),
            "one incarnation's turns continue the other's numbering, one \
             ordinal skipped for the turn that may have died half-journaled"
        );
        second.shutdown().await;
    }

    /// Context dies at archive: an archived agent must not be revivable, even
    /// though its spawn was once remembered.
    #[tokio::test]
    async fn an_archived_agent_cannot_be_revived() {
        let journal = Journal::in_memory().expect("an in-memory journal");
        let mut first = Runtime::new(journal.clone())
            .with_adapter(Arc::new(FakeAdapter::new(Script::echoing())));
        first
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        journal_at_least(&first, 4).await;
        first
            .apply(StreamCommandsResponse {
                command_sequence: 2,
                command_id: "s1:8".to_owned(),
                command: Some(Command::ArchiveAgent(ArchiveAgentCommand {
                    agent_id: Some(AgentId::new("agent-1")),
                    reason: "done".to_owned(),
                })),
            })
            .await
            .expect("the archive applies");
        let settled = journal_at_least(&first, 5).await.len();
        first.shutdown().await;

        let second = restarted_runtime_delivering(&journal, 3).await;

        assert_eq!(
            second.running_agents(),
            0,
            "archive discarded the context; nothing may rise from it"
        );
        assert_eq!(
            second
                .journal
                .read_from(1, 100)
                .await
                .expect("readable")
                .len(),
            settled,
            "and nothing new is journaled for a message with no recipient"
        );
        second.shutdown().await;
    }

    /// A revival that cannot resume — the harness never established a provider
    /// session — fails the agent loudly instead of leaving the sender waiting
    /// on an agent that looks merely slow.
    #[tokio::test]
    async fn reviving_an_agent_that_never_established_fails_loudly() {
        let journal = Journal::in_memory().expect("an in-memory journal");
        // A misbehaving script that never announces its session: nothing to
        // resume ever gets recorded.
        let mut first = Runtime::new(journal.clone()).with_adapter(Arc::new(FakeAdapter::new(
            Script::of(vec![ScriptedTurn::saying("hi")])
                .misbehaving(horde_adapter_fake::Misbehaviour::SkipSessionEstablished),
        )));
        first
            .apply(spawn_command(1, "agent-1", "hello"))
            .await
            .expect("the spawn applies");
        let settled = journal_at_least(&first, 3).await.len();
        first.shutdown().await;

        let second = restarted_runtime_delivering(&journal, 2).await;

        let events = journal_at_least(&second, settled + 1).await;
        assert!(
            failed_with_reason(&events, |reason| reason.contains("no provider session")),
            "an unrevivable agent must be journaled as failed: {events:?}"
        );
        assert_eq!(second.running_agents(), 0);
        second.shutdown().await;
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

    /// A workspace-supporting runtime whose `ws-1` instance is already
    /// materialized from a fresh tiny origin. The tempdir rides back so the
    /// checkouts live as long as the test.
    async fn materialized_runtime(
        script: Script,
    ) -> (tempfile::TempDir, std::path::PathBuf, Runtime) {
        let temp = tempfile::tempdir().expect("a tempdir");
        let origin = tiny_origin(temp.path());
        let workspaces_root = temp.path().join("workspaces");
        let mut runtime = runtime_with(script).with_workspaces(
            crate::workspace::WorkspaceManager::new("worker-a", workspaces_root.clone()),
        );
        runtime
            .apply(materialize_command(1, &origin))
            .await
            .expect("the materialize applies");
        (temp, workspaces_root, runtime)
    }

    /// The full workspace path through the runtime: materialize journals what
    /// landed on disk, and the spawn that follows runs in its checkout.
    #[tokio::test]
    async fn a_materialize_then_spawn_runs_the_agent_in_its_checkout() {
        let (_temp, workspaces_root, mut runtime) =
            materialized_runtime(Script::of(vec![ScriptedTurn::saying("hi")])).await;

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

    /// Only a payload carrying something is worth obtaining: git refs,
    /// attachments, or both. An attachments-only payload skipped here would
    /// silently deliver a message naming files that never arrive.
    #[test]
    fn a_payload_is_carried_when_it_holds_refs_or_attachments() {
        use horde_proto::v1::{Attachment, GitRef, Payload};
        let refs_only = Payload {
            git_refs: vec![GitRef::default()],
            attachments: Vec::new(),
        };
        let attachments_only = Payload {
            git_refs: Vec::new(),
            attachments: vec![Attachment::default()],
        };
        let empty = Payload::default();

        assert!(carried_payload(Some(&refs_only)).is_some());
        assert!(carried_payload(Some(&attachments_only)).is_some());
        assert!(
            carried_payload(Some(&empty)).is_none(),
            "an empty payload obtains nothing"
        );
        assert!(carried_payload(None).is_none());
    }

    /// The receiving half of the attachment seam, through the whole
    /// runtime: a delivered note's inline attachment becomes a verified
    /// file under the agent's checkout root before the text reaches it.
    #[tokio::test]
    async fn a_delivered_attachment_is_materialized_into_the_agents_checkout() {
        let (_temp, workspaces_root, mut runtime) = materialized_runtime(Script::echoing()).await;
        runtime
            .apply(workspace_spawn(2, "agent-1", "ws-1"))
            .await
            .expect("the spawn applies");
        assert_eq!(runtime.running_agents(), 1);

        let content = b"the failing assertion".to_vec();
        let attachment = horde_proto::v1::Attachment {
            name: "failing-test.log".to_owned(),
            blob_digest: horde_proto::blob::BlobDigest::of(&content).to_string(),
            size_bytes: content.len() as u64,
            media_type: "text/plain".to_owned(),
            inline: content.clone(),
        };
        let digest_prefix: String = horde_proto::blob::BlobDigest::of(&content)
            .hex()
            .chars()
            .take(12)
            .collect();
        runtime
            .apply(deliver_command(
                3,
                "agent-1",
                deliver_message_command::Body::Note(horde_proto::v1::Note {
                    text: "see the attached log".to_owned(),
                    payload: Some(horde_proto::v1::Payload {
                        git_refs: Vec::new(),
                        attachments: vec![attachment],
                    }),
                }),
                DeliveryMode::TurnBoundary,
            ))
            .await
            .expect("the delivery applies");

        let materialized = workspaces_root
            .join("ws-1")
            .join(".horde-agents")
            .join("agent-1")
            .join(crate::blob::ATTACHMENTS_DIR)
            .join(format!("{digest_prefix}-failing-test.log"));
        assert_eq!(
            std::fs::read(&materialized).expect("the attachment file exists"),
            content,
            "the file is the verified content, in place before the agent reads the note"
        );
        runtime.shutdown().await;
    }

    /// An attachment for an agent with nowhere to put it fails the agent
    /// loudly — delivering text that names files nobody materialized would
    /// be a silent drop with a delayed symptom.
    #[tokio::test]
    async fn an_attachment_for_a_checkoutless_agent_fails_the_delivery_honestly() {
        let (mut runtime, settled) = spawned_echoing_agent().await;

        runtime
            .apply(deliver_command(
                2,
                "agent-1",
                deliver_message_command::Body::Note(horde_proto::v1::Note {
                    text: "see the attachment".to_owned(),
                    payload: Some(horde_proto::v1::Payload {
                        git_refs: Vec::new(),
                        attachments: vec![horde_proto::v1::Attachment {
                            name: "shot.png".to_owned(),
                            blob_digest: horde_proto::blob::BlobDigest::of(b"px").to_string(),
                            size_bytes: 2,
                            media_type: String::new(),
                            inline: b"px".to_vec(),
                        }],
                    }),
                }),
                DeliveryMode::TurnBoundary,
            ))
            .await
            .expect("the command applies; the agent is what fails");

        let failed = journaled_lifecycle(
            &runtime,
            settled + 1,
            horde_proto::v1::AgentLifecycleState::Failed,
            "no checkout to receive",
        )
        .await;
        assert!(failed, "the failure must be journaled with its reason");
        runtime.shutdown().await;
    }

    /// Whether the journal (grown to `at_least` events) holds a lifecycle
    /// transition into `state` whose reason contains `needle` — the shape
    /// every "did the runtime say why" assertion shares.
    async fn journaled_lifecycle(
        runtime: &Runtime,
        at_least: usize,
        state: horde_proto::v1::AgentLifecycleState,
        needle: &str,
    ) -> bool {
        let events = journal_at_least(runtime, at_least).await;
        events.iter().any(|journaled| {
            matches!(
                journaled.event.as_ref().and_then(|event| event.kind.as_ref()),
                Some(Kind::AgentLifecycleChanged(changed))
                    if changed.state == i32::from(state) && changed.reason.contains(needle)
            )
        })
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
