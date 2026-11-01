//! What every lane shares: the journal, the adapters, and the brokers — and
//! the translations from wire commands to the domain they all lean on.
//!
//! A leaf of the runtime module: lanes and the dispatcher both use it, and it
//! uses neither, so no module cycle runs through the runtime.

use std::collections::BTreeMap;
use std::sync::Arc;

use horde_adapter::{Adapter, AgentSession, Capability, ProviderAccount, TurnInput};
use horde_proto::v1::{
    AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentOutputChunk,
    DeliverMessageCommand, DeliveryMode, SpawnAgentCommand, deliver_message_command, event::Kind,
};

use crate::error::WorkerError;
use crate::journal::{Journal, JournalEntry, RevivableAgent};
use crate::share::{AgentCheckout, ShareBroker, StampingSink};
use crate::translate::AgentContext;

/// Everything a lane needs that is not its own agent. Cheap to clone: every
/// field is a handle.
#[derive(Clone)]
pub(super) struct Shared {
    pub(super) journal: Journal,
    pub(super) adapters: BTreeMap<String, Arc<dyn Adapter>>,
    /// The provider used when a spawn command names none — the state of the
    /// world before the scheduler resolves bindings.
    pub(super) default_provider: Option<String>,
    /// Materializes instances and produces per-agent checkouts. `None` means
    /// this Worker does not advertise workspace support.
    pub(super) workspaces: Option<crate::workspace::WorkspaceManager>,
    /// Tracks every running agent's checkout and executes payload shares —
    /// stamping on the way out, obtaining on the way in.
    pub(super) broker: Arc<ShareBroker>,
    /// Stashes outgoing attachment content and materializes incoming
    /// attachments into receiving checkouts.
    pub(super) attachments: Arc<crate::blob::AttachmentBroker>,
    /// Serves each Session's MCP endpoint and mints per-agent tokens. `None`
    /// means spawned agents get no endpoint — the in-process test shape.
    pub(super) endpoints: Option<Arc<crate::endpoint::EndpointManager>>,
}

impl Shared {
    pub(super) fn new(journal: Journal) -> Self {
        Self {
            journal,
            adapters: BTreeMap::new(),
            default_provider: None,
            workspaces: None,
            broker: Arc::new(ShareBroker::new(None)),
            attachments: Arc::new(crate::blob::AttachmentBroker::new(None)),
            endpoints: None,
        }
    }

    /// The journal sink results are stamped through (see
    /// [`Runtime::stamping_sink`](super::Runtime::stamping_sink)).
    pub(super) fn stamping_sink(&self) -> StampingSink {
        StampingSink::new(self.journal.sink(), Arc::clone(&self.broker))
    }

    /// Revokes an agent's endpoint identity and drops its MCP surface (and
    /// its Session's endpoint, when it was the last agent there).
    pub(super) async fn forget_endpoint(&self, agent_id: &str) {
        if let Some(endpoints) = &self.endpoints {
            endpoints.forget_agent(agent_id).await;
        }
    }

    /// Gives back what a spawn minted for an agent whose harness never
    /// started: the endpoint token (a credential nobody holds must not stay
    /// valid), its `local` MCP children, and the checkout — the agent is not
    /// remembered, so nothing would ever reuse or release it.
    pub(super) async fn abandon_spawn(&self, agent_id: &str, checkout: Option<&AgentCheckout>) {
        self.forget_endpoint(agent_id).await;
        if let Some(checkout) = checkout {
            self.release_checkout(agent_id, &checkout.root).await;
        }
    }

    /// Gives one agent's checkout back (ADR-0010), logging rather than
    /// failing: a checkout that will not release is disk, not correctness.
    pub(super) async fn release_checkout(&self, agent_id: &str, root: &std::path::Path) {
        if let Some(workspaces) = &self.workspaces
            && let Err(error) = workspaces.release_checkout(root).await
        {
            tracing::warn!(agent_id, %error, "could not release an agent's checkout");
        }
    }

    /// Where a remembered agent's private checkout lives, when it has one —
    /// what an archive gives back for an agent no pump is running.
    pub(super) fn remembered_checkout(
        &self,
        record: &RevivableAgent,
    ) -> Option<std::path::PathBuf> {
        let workspaces = self.workspaces.as_ref()?;
        let definition_id = record
            .spawn
            .spec
            .as_ref()?
            .workspace_definition_id
            .as_ref()?
            .to_string();
        workspaces.agent_root(&definition_id, &record.agent_id)
    }

    /// Resolves the adapter and starts the harness session in `cwd`, with
    /// `endpoint` as the agent's one Horde-controlled MCP endpoint.
    pub(super) async fn start_session(
        &self,
        spawn: &SpawnAgentCommand,
        cwd: Option<std::path::PathBuf>,
        endpoint: Option<horde_adapter::McpEndpoint>,
    ) -> Result<(Box<dyn AgentSession>, bool), WorkerError> {
        let adapter = self.adapter_for(spawn)?;
        let spec = domain_spec(spawn, cwd, endpoint);
        let session = adapter.spawn(&spec).await?;
        let can_rewrite = adapter
            .capabilities()
            .supports(Capability::RewritesToolInput);
        Ok((session, can_rewrite))
    }

    /// The endpoint one agent's harness is pointed at, or `None` when this
    /// Runtime serves no endpoints. The spawn command's resolved MCP surface
    /// — `local` configs in full, visible hub servers by name — is consumed
    /// here, per ADR-0004.
    pub(super) async fn agent_endpoint(
        &self,
        session_id: &str,
        agent_id: &str,
        spawn: &SpawnAgentCommand,
    ) -> Result<Option<horde_adapter::McpEndpoint>, WorkerError> {
        let Some(endpoints) = &self.endpoints else {
            return Ok(None);
        };
        let surface = crate::endpoint::AgentMcpSurface {
            local: spawn.local_mcp_servers.clone(),
            visible_hub: spawn.visible_hub_mcp_servers.clone(),
        };
        endpoints
            .endpoint_for(session_id, agent_id, self.stamping_sink(), &surface)
            .await
            .map(Some)
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
    pub(super) async fn agent_checkout(
        &self,
        spawn: &SpawnAgentCommand,
    ) -> Result<Option<AgentCheckout>, WorkerError> {
        let Some(definition_id) = workspace_of(spawn) else {
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
        let policy = spawn
            .spec
            .as_ref()
            .and_then(|spec| horde_proto::v1::CheckoutPolicy::try_from(spec.checkout_policy).ok())
            .unwrap_or(horde_proto::v1::CheckoutPolicy::Unspecified);
        let start_points = self.parent_start_points(&spawn.parent_agent_id).await;
        let root = workspaces
            .checkout(&definition_id, &agent_id, policy, &start_points)
            .await?;
        let repos = workspaces.instance_repos(&definition_id).await?;
        Ok(Some(AgentCheckout { root, repos }))
    }

    /// The parent's committed HEAD per repo — what a child's checkout starts
    /// from (DESIGN §5, fix a8). Empty when the spawn has no parent, or the
    /// parent's checkout is not on this Worker (a cross-worker parent's
    /// commits travel as payloads, not as checkout ancestry): those children
    /// honestly start from the instance's HEAD.
    async fn parent_start_points(
        &self,
        parent_agent_id: &Option<horde_proto::v1::AgentId>,
    ) -> BTreeMap<String, String> {
        let mut start_points = BTreeMap::new();
        let Some(parent) = parent_agent_id
            .as_ref()
            .map(ToString::to_string)
            .filter(|id| !id.is_empty())
        else {
            return start_points;
        };
        let Some(checkout) = self.broker.checkout_of(&parent) else {
            return start_points;
        };
        for (repo_name, relative) in &checkout.repos {
            match horde_relay::observe_head(&checkout.root.join(relative)).await {
                Ok(observed) => {
                    start_points.insert(repo_name.clone(), observed.commit_sha);
                }
                // A parent repo whose HEAD cannot be observed contributes no
                // start point rather than failing the child's spawn: the
                // instance HEAD is a working default, a dead spawn is not.
                Err(error) => {
                    tracing::warn!(
                        parent,
                        repo = %repo_name,
                        %error,
                        "could not observe the parent's HEAD; the child starts at the instance's"
                    );
                }
            }
        }
        start_points
    }

    /// Resumes the provider conversation a remembered agent names.
    ///
    /// The caller has already established that a provider session id exists;
    /// an empty one here would resume nothing and is refused by the adapter.
    pub(super) async fn resume_session(
        &self,
        record: &RevivableAgent,
    ) -> Result<(Box<dyn AgentSession>, bool, Option<AgentCheckout>), WorkerError> {
        let provider_session_id = record.provider_session_id.as_deref().unwrap_or_default();
        let adapter = self.adapter_for(&record.spawn)?;
        let checkout = self.agent_checkout(&record.spawn).await?;
        let cwd = checkout.as_ref().map(|checkout| checkout.root.clone());
        // A fresh endpoint identity per incarnation, and a re-assembled MCP
        // surface: the token the previous process held is replaced, never
        // reused, and its `local` children are new.
        let endpoint = self
            .agent_endpoint(&record.session_id, &record.agent_id, &record.spawn)
            .await?;
        // One past the recorded count: a turn that died half-journaled has
        // already spent its ordinal, and reusing it would hand the revived
        // turn journal keys the Server has partially seen. Turn ids need
        // uniqueness, not density, so the skip is free.
        let spec =
            domain_spec(&record.spawn, cwd, endpoint).with_turns_taken(record.turns_taken + 1);
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
    /// an agent with no checkout is refused rather than half-delivered.
    pub(super) async fn obtain_payload(
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
}

/// The workspace definition a spawn names, if any.
pub(super) fn workspace_of(spawn: &SpawnAgentCommand) -> Option<String> {
    spawn
        .spec
        .as_ref()?
        .workspace_definition_id
        .as_ref()
        .map(ToString::to_string)
        .filter(|id| !id.is_empty())
}

/// What a deliver command asks the agent to read, and the payload it rides
/// with; `None` when it carries no message at all.
pub(super) fn message_input(
    deliver: DeliverMessageCommand,
) -> Option<(TurnInput, Option<horde_proto::v1::Payload>)> {
    let (text, payload) = match deliver.body? {
        deliver_message_command::Body::Assignment(assignment) => {
            (assignment.text, assignment.payload)
        }
        deliver_message_command::Body::Note(note) => (note.text, note.payload),
    };
    let input = match DeliveryMode::try_from(deliver.delivery) {
        Ok(DeliveryMode::Interrupt) => TurnInput::interrupting(text),
        // Turn-boundary is every provider's safe default, and an
        // unrecognized mode from a newer Server must not become an
        // interrupt nobody asked for.
        _ => TurnInput::queued(text),
    };
    Some((input, payload))
}

/// The payload a message actually carries, if it names any commits or
/// attachments.
pub(super) fn carried_payload(
    payload: Option<&horde_proto::v1::Payload>,
) -> Option<&horde_proto::v1::Payload> {
    payload.filter(|payload| !payload.git_refs.is_empty() || !payload.attachments.is_empty())
}

/// A lifecycle transition as a journal effect.
pub(super) fn lifecycle_entry(
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

/// A Worker-side diagnostic against one agent, on the harness-diagnostic
/// channel: how a command that could not be applied is journaled without
/// claiming the agent itself failed.
pub(super) fn diagnostic_entry(session_id: &str, agent_id: &str, text: String) -> JournalEntry {
    JournalEntry {
        session_id: session_id.to_owned(),
        agent_id: Some(agent_id.to_owned()),
        idempotency_key: String::new(),
        kind: Kind::AgentOutputChunk(AgentOutputChunk {
            channel: AgentOutputChannel::HarnessDiagnostic.into(),
            text,
            turn_id: String::new(),
            chunk_index: 0,
        }),
    }
}

/// An id field the protocol requires, or the error that names it.
pub(super) fn required_id<T: ToString>(
    id: Option<&T>,
    name: &'static str,
) -> Result<String, WorkerError> {
    id.map(ToString::to_string)
        .filter(|id| !id.is_empty())
        .ok_or(WorkerError::Incomplete(name))
}

/// The standing facts a spawn command carries, as the pump journals them.
///
/// Shared between the spawn and revival paths — a revived agent's events must
/// journal under exactly the facts its spawn established, or attribution
/// changes across an incarnation boundary.
pub(super) fn context_of(
    spawn: &SpawnAgentCommand,
    session_id: &str,
    agent_id: &str,
) -> AgentContext {
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

/// What the wire spawn command means to an adapter.
pub(super) fn domain_spec(
    spawn: &SpawnAgentCommand,
    cwd: Option<std::path::PathBuf>,
    horde_endpoint: Option<horde_adapter::McpEndpoint>,
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
        // The Session's served endpoint with this agent's own identity pair
        // (`None` only when this Runtime serves no endpoints).
        horde_endpoint,
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
