//! What every lane shares: the journal, the adapters, and the brokers — and
//! the translations from wire commands to the domain they all lean on.
//!
//! A leaf of the runtime module: lanes and the dispatcher both use it, and it
//! uses neither, so no module cycle runs through the runtime.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use horde_adapter::{Adapter, AgentSession, Capability, ProviderAccount, TurnInput};
use horde_proto::v1::{
    AgentLifecycleChanged, AgentLifecycleState, AgentOutputChannel, AgentOutputChunk,
    DeliverMessageCommand, DeliveryMode, SpawnAgentCommand, deliver_message_command, event::Kind,
};

use super::housekeeping::{Activity, CheckoutLocks, Housekeeper, InstanceReporter};
use crate::error::WorkerError;
use crate::journal::{Journal, JournalEntry, RevivableAgent};
use crate::share::{AgentCheckout, ShareBroker, StampingSink};
use crate::translate::AgentContext;
use crate::workspace::policy::WorkspacePolicy;

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
    /// Which agents have a turn in flight, and since when they are idle.
    pub(super) activity: Arc<Activity>,
    /// One lock per agent checkout: spawn, `workspace_checkout`, archive and
    /// every cleanup take it (ADR-0014).
    pub(super) checkout_locks: Arc<CheckoutLocks>,
    /// Where the Instance set is reported after changes.
    pub(super) reporter: Option<Arc<dyn InstanceReporter>>,
    /// The last workspace policy each Instance's commands carried — what
    /// the scan, which no command carries, cleans up by.
    pub(super) policies: Arc<Mutex<BTreeMap<String, WorkspacePolicy>>>,
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
            activity: Arc::new(Activity::default()),
            checkout_locks: Arc::new(CheckoutLocks::default()),
            reporter: None,
            policies: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The journal sink results are stamped through (see
    /// [`Runtime::stamping_sink`](super::Runtime::stamping_sink)).
    pub(super) fn stamping_sink(&self) -> StampingSink {
        StampingSink::new(self.journal.sink(), Arc::clone(&self.broker))
    }

    /// The housekeeper, when this Worker has workspaces.
    pub(super) fn housekeeper(&self) -> Option<Housekeeper> {
        self.workspaces
            .clone()
            .map(|workspaces| Housekeeper::new(self.clone(), workspaces))
    }

    /// Remembers the policy a command carried for an Instance.
    pub(super) fn remember_policy(
        &self,
        definition_id: &str,
        wire: Option<&horde_proto::v1::ResolvedWorkspacePolicy>,
    ) {
        if wire.is_none() {
            return;
        }
        self.policies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(definition_id.to_owned(), WorkspacePolicy::from_wire(wire));
    }

    /// The last policy seen for an Instance, or the defaults.
    pub(super) fn policy_for(&self, definition_id: &str) -> WorkspacePolicy {
        self.policies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(definition_id)
            .cloned()
            .unwrap_or_default()
    }

    /// The Relay URL of one repo, when a Relay is configured.
    pub(super) fn relay_repo_url(&self) -> impl Fn(&str) -> Option<String> + Sync + use<> {
        let relay = self.broker.relay().cloned();
        move |repo_name: &str| relay.as_ref().map(|relay| relay.repo_url(repo_name))
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
    /// valid), its `local` MCP children, and the checkout — nothing ran in
    /// it, so it goes at once when lossless (ADR-0014).
    pub(super) async fn abandon_spawn(&self, agent_id: &str, checkout: Option<&AgentCheckout>) {
        self.forget_endpoint(agent_id).await;
        if let Some(checkout) = checkout
            && let Some(housekeeper) = self.housekeeper()
        {
            housekeeper.archived(agent_id, &checkout.root, true).await;
        }
    }

    /// An archived agent's checkout: soft reclaim, then hard removal when
    /// provably lossless and the grace allows; otherwise retained. Never the
    /// old unconditional pin-and-force-remove (ADR-0014).
    pub(super) async fn release_checkout(&self, agent_id: &str, root: &std::path::Path) {
        if let Some(housekeeper) = self.housekeeper() {
            housekeeper.archived(agent_id, root, false).await;
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

    /// The checkout a spawn's workspace, Checkout Policy, mode, bases and
    /// branch call for — its root is the agent's working directory, its
    /// repos the payload seam's map — or `None` when the spawn names no
    /// workspace and the agent runs where the Worker runs.
    ///
    /// A checkout with a ledger record is recomposed from it, so a revival
    /// reuses the same repos, branch and bases (`REUSED`).
    ///
    /// # Errors
    ///
    /// [`WorkerError::Workspace`] when a workspace is named but this Worker
    /// has no manager, no materialized instance, or the checkout fails — the
    /// spawn's failure, journaled as a failed lifecycle by the caller.
    pub(super) async fn agent_checkout(
        &self,
        spawn: &SpawnAgentCommand,
    ) -> Result<Option<SpawnCheckout>, WorkerError> {
        let Some(definition_id) = workspace_of(spawn) else {
            return Ok(None);
        };
        let workspaces = self.workspaces.as_ref().ok_or_else(|| {
            WorkerError::Workspace(format!(
                "the spawn names workspace {definition_id}, but this worker has no workspace root"
            ))
        })?;
        self.remember_policy(&definition_id, spawn.workspace_policy.as_ref());
        let policy = self.policy_for(&definition_id);
        let spec = spawn.spec.clone().unwrap_or_default();
        let request = crate::workspace::CheckoutRequest {
            definition_id: definition_id.clone(),
            agent_id: spawn
                .agent_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            session_id: spawn
                .session_id
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            policy: horde_proto::v1::CheckoutPolicy::try_from(spec.checkout_policy)
                .unwrap_or(horde_proto::v1::CheckoutPolicy::Unspecified),
            mode: horde_proto::v1::CheckoutMode::try_from(spec.checkout_mode)
                .unwrap_or(horde_proto::v1::CheckoutMode::Unspecified),
            checkout_repos: spec.checkout_repos,
            bases: spec.repo_bases,
            branch: spec.branch,
            fetch: policy.fetch,
        };
        let prepared = workspaces
            .checkout_agent(&request, self.broker.as_ref())
            .await?;
        Ok(Some(SpawnCheckout {
            checkout: AgentCheckout {
                root: prepared.root,
                repos: prepared.repos,
            },
            prepared: prepared.prepared,
            overlay: prepared.overlay,
        }))
    }

    /// Resumes the provider conversation a remembered agent names.
    ///
    /// The caller has already established that a provider session id exists;
    /// an empty one here would resume nothing and is refused by the adapter.
    pub(super) async fn resume_session(
        &self,
        record: &RevivableAgent,
    ) -> Result<(Box<dyn AgentSession>, bool, Option<SpawnCheckout>), WorkerError> {
        let provider_session_id = record.provider_session_id.as_deref().unwrap_or_default();
        let adapter = self.adapter_for(&record.spawn)?;
        let checkout = self.agent_checkout(&record.spawn).await?;
        let cwd = checkout
            .as_ref()
            .map(|checkout| checkout.checkout.root.clone());
        let spawn = with_overlay(&record.spawn, checkout.as_ref());
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
        let spec = domain_spec(&spawn, cwd, endpoint).with_turns_taken(record.turns_taken + 1);
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
        let payload = self.obtain_pending(agent_id, payload).await?;
        let payload = &payload;
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

/// A spawn's composed checkout: the broker's view, what each repo started
/// from, and the lazy prompt paragraph.
pub(super) struct SpawnCheckout {
    pub(super) checkout: AgentCheckout,
    pub(super) prepared: horde_proto::v1::CheckoutPrepared,
    pub(super) overlay: Option<String>,
}

impl Shared {
    /// Handles the commits of a payload naming repos the receiver has not
    /// checked out (a lazy agent): each is made present in the Instance repo
    /// and listed as pending in the manifest — no worktree, since the agent
    /// did not ask for one (ADR-0014). Returns the rest of the payload, for
    /// the broker's ordinary delivery into checked-out repos.
    async fn obtain_pending(
        &self,
        agent_id: &str,
        payload: &horde_proto::v1::Payload,
    ) -> Result<horde_proto::v1::Payload, WorkerError> {
        let Some(checkout) = self.broker.checkout_of(agent_id) else {
            return Ok(payload.clone());
        };
        let (Some(workspaces), Some((definition_id, _))) = (
            self.workspaces.as_ref(),
            self.workspaces
                .as_ref()
                .and_then(|workspaces| workspaces.agent_private(&checkout.root)),
        ) else {
            return Ok(payload.clone());
        };
        let known: Vec<String> = workspaces
            .instance_repos(&definition_id)
            .await?
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        let mut rest = payload.clone();
        rest.git_refs.clear();
        for git_ref in &payload.git_refs {
            let checked_out = checkout
                .repos
                .iter()
                .any(|(name, _)| *name == git_ref.repo_name);
            if checked_out || !known.contains(&git_ref.repo_name) {
                rest.git_refs.push(git_ref.clone());
                continue;
            }
            let instance_repo = workspaces
                .instance_repo_path(&definition_id, &git_ref.repo_name)
                .await?;
            self.broker
                .obtain_into(&instance_repo, &git_ref.repo_name, &git_ref.commit_sha)
                .await?;
            workspaces
                .note_pending(&checkout.root, &git_ref.repo_name, &git_ref.commit_sha)
                .await?;
        }
        Ok(rest)
    }
}

/// A spawn with the lazy checkout's paragraph appended to its overlay.
pub(super) fn with_overlay(
    spawn: &SpawnAgentCommand,
    checkout: Option<&SpawnCheckout>,
) -> SpawnAgentCommand {
    let mut spawn = spawn.clone();
    if let Some(overlay) = checkout.and_then(|checkout| checkout.overlay.as_ref()) {
        if !spawn.prompt_overlay.is_empty() {
            spawn.prompt_overlay.push_str("\n\n");
        }
        spawn.prompt_overlay.push_str(overlay);
    }
    spawn
}

/// The `CheckoutPrepared` event a spawn or revival journals in the agent's
/// Session.
pub(super) fn checkout_prepared_entry(
    session_id: &str,
    agent_id: &str,
    idempotency_key: String,
    prepared: horde_proto::v1::CheckoutPrepared,
) -> JournalEntry {
    JournalEntry {
        session_id: session_id.to_owned(),
        agent_id: Some(agent_id.to_owned()),
        idempotency_key,
        kind: Kind::CheckoutPrepared(prepared),
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
