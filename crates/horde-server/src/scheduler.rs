//! Placement: which worker runs an agent.
//!
//! The scheduler matches a spawn's `PlacementConstraints` against what each
//! connected worker advertises (DESIGN §6): tags, provider accounts, reachable
//! remotes, workspace support, and declared capacity. Among the workers that
//! qualify it *prefers* — warmth first (an instance of the agent's workspace
//! already on disk drives colocation), then the parent's worker, then the
//! least loaded — but preference never overrides a constraint.
//!
//! Two decisions are deliberately not made here:
//!
//! * **An unplaceable spawn queues with a reason instead of failing** — spawn
//!   never blocks the caller, and the reason names which worker fell short of
//!   what, in words the spawner can act on.
//! * **A placed agent is never migrated.** Placement happens at spawn and at
//!   re-placement of a *queued* agent (`placement` module); a live agent's
//!   binding is held for its life, and revival resumes on the worker its
//!   placement event names — stickiness is a property of the log, not a
//!   scheduler special case.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use horde_proto::v1::{PlacementConstraints, WorkerCapabilities};

use crate::workspace::InstanceCache;

/// A worker the server currently believes is connected.
#[derive(Debug, Clone)]
pub struct RegisteredWorker {
    pub worker_id: String,
    pub capabilities: WorkerCapabilities,
}

/// Where an agent should run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Placement {
    Placed {
        worker_id: String,
    },
    /// No worker matched. Carries the reason in words the spawner can act on,
    /// because DESIGN.md §6 requires spawn never to block and never to fail
    /// silently.
    Queued {
        reason: String,
    },
}

/// Everything one placement decision needs beyond the registry's own facts.
#[derive(Debug, Clone, Copy)]
pub struct PlacementRequest<'a> {
    pub constraints: &'a PlacementConstraints,
    /// The spawning parent's worker, preferred when nothing forces otherwise.
    pub parent_worker_id: Option<&'a str>,
    /// The workspace the agent will occupy. Requires workspace support on the
    /// chosen worker, and makes warmth the strongest preference.
    pub workspace_definition_id: Option<&'a str>,
    /// Active agents per worker, for capacity. Computed from the log by the
    /// caller (`placement::worker_load`) so the scheduler stays a pure
    /// decision over facts it is handed.
    pub load: &'a BTreeMap<String, u64>,
}

impl<'a> PlacementRequest<'a> {
    /// A request with no preferences, no workspace, and no load anywhere.
    #[must_use]
    pub fn new(constraints: &'a PlacementConstraints, load: &'a BTreeMap<String, u64>) -> Self {
        Self {
            constraints,
            parent_worker_id: None,
            workspace_definition_id: None,
            load,
        }
    }
}

/// Chooses a worker for an agent.
pub trait Scheduler: Send + Sync {
    fn place(&self, request: &PlacementRequest<'_>) -> Placement;
}

/// The registry of connected workers, shared by the worker service and the
/// scheduler.
#[derive(Debug, Clone, Default)]
pub struct WorkerRegistry {
    workers: Arc<Mutex<Registrations>>,
}

/// The registry's guarded state: who is connected, and which registration of
/// each worker is current.
#[derive(Debug, Default)]
struct Registrations {
    connected: BTreeMap<String, RegisteredWorker>,
    /// Bumped on every registration and never forgotten, so a stream opened
    /// against an old registration can prove it is stale. A worker's
    /// disconnect is detected lazily — the command stream only notices its
    /// peer is gone when it next tries to send — and by then the worker may
    /// have re-registered; letting the stale stream deregister the fresh
    /// registration would knock a live worker out of placement.
    epochs: BTreeMap<String, u64>,
}

impl WorkerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a worker, replacing any previous registration.
    ///
    /// Capabilities are replaced wholesale rather than merged: a partial update
    /// is how a scheduler ends up matching on facts that stopped being true.
    pub fn register(&self, worker_id: &str, capabilities: WorkerCapabilities) {
        if let Ok(mut registrations) = self.workers.lock() {
            *registrations
                .epochs
                .entry(worker_id.to_owned())
                .or_insert(0) += 1;
            registrations.connected.insert(
                worker_id.to_owned(),
                RegisteredWorker {
                    worker_id: worker_id.to_owned(),
                    capabilities,
                },
            );
        }
    }

    /// The current registration epoch for a worker; zero if never registered.
    #[must_use]
    pub fn epoch_of(&self, worker_id: &str) -> u64 {
        self.workers
            .lock()
            .map(|registrations| registrations.epochs.get(worker_id).copied().unwrap_or(0))
            .unwrap_or(0)
    }

    /// Removes a worker, but only if `epoch` is still its current
    /// registration. Returns whether anything was removed, so the caller
    /// knows whether the disconnect consequences (re-queueing) are its to
    /// run. A stale epoch means the worker re-registered since this stream
    /// was opened, and the fresh registration must not be disturbed.
    pub fn deregister_if_current(&self, worker_id: &str, epoch: u64) -> bool {
        self.workers
            .lock()
            .map(|mut registrations| {
                let current = registrations.epochs.get(worker_id).copied().unwrap_or(0);
                if current == epoch {
                    registrations.connected.remove(worker_id).is_some()
                } else {
                    false
                }
            })
            .unwrap_or(false)
    }

    pub fn deregister(&self, worker_id: &str) {
        if let Ok(mut registrations) = self.workers.lock() {
            registrations.connected.remove(worker_id);
        }
    }

    #[must_use]
    pub fn connected(&self) -> Vec<RegisteredWorker> {
        self.workers
            .lock()
            .map(|registrations| registrations.connected.values().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn contains(&self, worker_id: &str) -> bool {
        self.workers
            .lock()
            .is_ok_and(|registrations| registrations.connected.contains_key(worker_id))
    }
}

/// The real scheduler: constraints filter, preferences order.
#[derive(Debug, Clone)]
pub struct CapabilityScheduler {
    registry: WorkerRegistry,
    instances: InstanceCache,
}

impl CapabilityScheduler {
    #[must_use]
    pub fn new(registry: WorkerRegistry, instances: InstanceCache) -> Self {
        Self {
            registry,
            instances,
        }
    }
}

impl Scheduler for CapabilityScheduler {
    fn place(&self, request: &PlacementRequest<'_>) -> Placement {
        let workers = self.registry.connected();
        if workers.is_empty() {
            return Placement::Queued {
                reason: "no worker is connected".to_owned(),
            };
        }

        // Pinning skips matching entirely and demands that worker (the
        // proto's words) — connected or not is the one thing it cannot skip.
        if let Some(pinned) = pinned_worker(request.constraints) {
            return match workers.iter().find(|worker| worker.worker_id == pinned) {
                Some(worker) => match workspace_refusal(request, &worker.capabilities) {
                    // A pinned worker that would refuse the materialize
                    // command is not a placement, it is a queued failure with
                    // the pin's name on it.
                    Some(refusal) => Placement::Queued {
                        reason: format!("pinned worker `{pinned}` {refusal}"),
                    },
                    None => Placement::Placed {
                        worker_id: worker.worker_id.clone(),
                    },
                },
                None => Placement::Queued {
                    reason: format!("pinned worker `{pinned}` is not connected"),
                },
            };
        }

        let mut refusals_by_worker = Vec::new();
        let mut candidates = Vec::new();
        for worker in &workers {
            let load = worker_load(request.load, &worker.worker_id);
            let refusals = refusals(request, &worker.capabilities, load);
            if refusals.is_empty() {
                candidates.push(Candidate {
                    worker_id: worker.worker_id.clone(),
                    warm: is_warm_candidate(
                        &self.instances,
                        &worker.worker_id,
                        request.workspace_definition_id,
                    ),
                    on_parent: Some(worker.worker_id.as_str()) == request.parent_worker_id,
                    load,
                });
            } else {
                refusals_by_worker.push(format!("{} {}", worker.worker_id, refusals.join(", ")));
            }
        }

        match preferred(candidates) {
            Some(worker_id) => Placement::Placed { worker_id },
            None => Placement::Queued {
                reason: format!(
                    "no connected worker matches: {}",
                    refusals_by_worker.join("; ")
                ),
            },
        }
    }
}

/// One eligible worker, with the facts preference orders on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Candidate {
    worker_id: String,
    /// Holds (or is building) an instance of the requested workspace.
    warm: bool,
    /// Is the spawning parent's worker.
    on_parent: bool,
    load: u64,
}

/// The preference order among eligible workers: warmth, then the parent's
/// worker, then the least loaded, then the lexicographically first — the last
/// tiebreak so two identical registries always place identically, because a
/// scheduler that flips coins cannot be replayed or tested.
fn preferred(candidates: Vec<Candidate>) -> Option<String> {
    candidates
        .into_iter()
        .min_by_key(|candidate| {
            (
                !candidate.warm,
                !candidate.on_parent,
                candidate.load,
                candidate.worker_id.clone(),
            )
        })
        .map(|candidate| candidate.worker_id)
}

/// Whether this worker counts as warm for this spawn: only a spawn that names
/// a workspace has warmth at all.
fn is_warm_candidate(
    instances: &InstanceCache,
    worker_id: &str,
    workspace_definition_id: Option<&str>,
) -> bool {
    workspace_definition_id.is_some_and(|definition| instances.warm_on(worker_id, definition))
}

/// The pinned worker, when the constraints demand one.
fn pinned_worker(constraints: &PlacementConstraints) -> Option<String> {
    constraints
        .pinned_worker_id
        .as_ref()
        .map(ToString::to_string)
        .filter(|id| !id.is_empty())
}

/// This worker's active-agent count, absent meaning zero.
fn worker_load(load: &BTreeMap<String, u64>, worker_id: &str) -> u64 {
    load.get(worker_id).copied().unwrap_or(0)
}

/// Everything this worker falls short of, in words. Empty means eligible.
///
/// Every check contributes independently so the queued reason can name all of
/// them at once — a spawner told only about the first missing tag relaxes it
/// and discovers the next refusal one round trip at a time.
fn refusals(
    request: &PlacementRequest<'_>,
    capabilities: &WorkerCapabilities,
    load: u64,
) -> Vec<String> {
    let mut refusals = Vec::new();

    let missing_tags = missing(&request.constraints.required_tags, &capabilities.tags);
    if !missing_tags.is_empty() {
        refusals.push(format!("lacks tags [{}]", missing_tags.join(", ")));
    }

    let held_accounts: Vec<String> = capabilities
        .provider_accounts
        .iter()
        .map(|account| account.account.clone())
        .collect();
    let missing_accounts = missing(
        &request.constraints.required_provider_accounts,
        &held_accounts,
    );
    if !missing_accounts.is_empty() {
        refusals.push(format!(
            "lacks provider accounts [{}]",
            missing_accounts.join(", ")
        ));
    }

    let missing_remotes = missing(
        &request.constraints.required_remotes,
        &capabilities.reachable_remotes,
    );
    if !missing_remotes.is_empty() {
        refusals.push(format!(
            "cannot reach remotes [{}]",
            missing_remotes.join(", ")
        ));
    }

    if let Some(refusal) = workspace_refusal(request, capabilities) {
        refusals.push(refusal);
    }

    if at_capacity(capabilities.max_concurrent_agents, load) {
        refusals.push(format!(
            "is at capacity ({load}/{})",
            capabilities.max_concurrent_agents
        ));
    }

    refusals
}

/// Why this worker cannot host the requested workspace, if it cannot.
///
/// Split out of [`refusals`] because the pinned path needs exactly this check
/// and none of the others: a pin skips matching, but a worker that would
/// refuse the materialize command can never satisfy the spawn.
fn workspace_refusal(
    request: &PlacementRequest<'_>,
    capabilities: &WorkerCapabilities,
) -> Option<String> {
    let needs = request.workspace_definition_id.is_some();
    let supports = capabilities.supports_workspaces;
    if needs && !supports {
        Some("does not support workspaces".to_owned())
    } else {
        None
    }
}

/// Required entries the worker does not advertise, in the order required.
fn missing(required: &[String], held: &[String]) -> Vec<String> {
    required
        .iter()
        .filter(|needed| !held.contains(needed))
        .map(|needed| format!("`{needed}`"))
        .collect()
}

/// Whether a worker's declared limit is used up. Zero declares no limit.
///
/// A named predicate because `max > 0 && load >= max` inline is exactly the
/// compound a live run never separates: workers rarely declare limits, so the
/// left half decides every staged test and the comparison never gets
/// exercised.
fn at_capacity(max_concurrent_agents: u32, load: u64) -> bool {
    max_concurrent_agents > 0 && load >= u64::from(max_concurrent_agents)
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{
        PlacementConstraints, ProviderAccountCapability, WorkerId, WorkspaceDefinitionId,
        WorkspaceInstanceReport, WorkspaceInstanceState,
    };

    use super::*;

    use std::sync::LazyLock;

    static NO_CONSTRAINTS: LazyLock<PlacementConstraints> =
        LazyLock::new(PlacementConstraints::default);
    static NO_LOAD: LazyLock<BTreeMap<String, u64>> = LazyLock::new(BTreeMap::new);

    /// The unconstrained request, for struct-update spreads in these tests.
    fn request_defaults() -> PlacementRequest<'static> {
        PlacementRequest::new(&NO_CONSTRAINTS, &NO_LOAD)
    }

    struct Harness {
        registry: WorkerRegistry,
        instances: InstanceCache,
        scheduler: CapabilityScheduler,
    }

    fn harness() -> Harness {
        let registry = WorkerRegistry::new();
        let instances = InstanceCache::new();
        let scheduler = CapabilityScheduler::new(registry.clone(), instances.clone());
        Harness {
            registry,
            instances,
            scheduler,
        }
    }

    fn capabilities(tags: &[&str]) -> WorkerCapabilities {
        WorkerCapabilities {
            tags: tags.iter().map(ToString::to_string).collect(),
            supports_workspaces: true,
            ..WorkerCapabilities::default()
        }
    }

    fn place_default(harness: &Harness) -> Placement {
        let constraints = PlacementConstraints::default();
        harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            ..request_defaults()
        })
    }

    fn queued_reason(placement: Placement) -> String {
        match placement {
            Placement::Queued { reason } => reason,
            Placement::Placed { worker_id } => {
                panic!("expected Queued, got Placed on {worker_id}")
            }
        }
    }

    fn placed_on(placement: Placement) -> String {
        match placement {
            Placement::Placed { worker_id } => worker_id,
            Placement::Queued { reason } => panic!("expected Placed, got Queued: {reason}"),
        }
    }

    #[test]
    fn with_no_worker_a_spawn_queues_with_a_reason_rather_than_failing() {
        let harness = harness();
        assert_eq!(
            queued_reason(place_default(&harness)),
            "no worker is connected"
        );
    }

    #[test]
    fn with_one_unconstrained_worker_everything_lands_there() {
        let harness = harness();
        harness.registry.register("worker-a", capabilities(&[]));
        assert_eq!(placed_on(place_default(&harness)), "worker-a");
    }

    /// The epoch is what keeps a stale command stream from knocking a
    /// re-registered worker out of placement: every registration is a new
    /// epoch, and only the current one may deregister.
    #[test]
    fn registration_epochs_grow_and_gate_deregistration() {
        let registry = WorkerRegistry::new();
        assert_eq!(registry.epoch_of("w"), 0, "never registered is epoch zero");

        registry.register("w", capabilities(&[]));
        assert_eq!(registry.epoch_of("w"), 1);
        registry.register("w", capabilities(&[]));
        assert_eq!(
            registry.epoch_of("w"),
            2,
            "each registration is a new epoch"
        );

        assert!(
            !registry.deregister_if_current("w", 1),
            "a stream from the old registration must not disturb the new one"
        );
        assert!(registry.contains("w"));

        assert!(registry.deregister_if_current("w", 2));
        assert!(!registry.contains("w"));
        assert!(
            !registry.deregister_if_current("w", 2),
            "nothing was removed the second time, so the caller must not \
             run the disconnect consequences again"
        );
    }

    #[test]
    fn deregistering_removes_a_worker_from_placement() {
        let harness = harness();
        harness.registry.register("worker-a", capabilities(&[]));
        assert!(harness.registry.contains("worker-a"));

        harness.registry.deregister("worker-a");
        assert!(!harness.registry.contains("worker-a"));
        assert!(matches!(place_default(&harness), Placement::Queued { .. }));
    }

    #[test]
    fn required_tags_filter_workers_and_the_refusal_names_what_is_missing() {
        let harness = harness();
        harness
            .registry
            .register("cpu-box", capabilities(&["linux"]));
        harness
            .registry
            .register("gpu-box", capabilities(&["linux", "gpu"]));

        let constraints = PlacementConstraints {
            required_tags: vec!["gpu".to_owned()],
            ..PlacementConstraints::default()
        };
        let request = PlacementRequest {
            constraints: &constraints,
            ..request_defaults()
        };
        assert_eq!(placed_on(harness.scheduler.place(&request)), "gpu-box");

        harness.registry.deregister("gpu-box");
        let reason = queued_reason(harness.scheduler.place(&request));
        assert_eq!(
            reason,
            "no connected worker matches: cpu-box lacks tags [`gpu`]"
        );
    }

    #[test]
    fn provider_accounts_and_remotes_are_matched_by_name() {
        let harness = harness();
        let mut able = capabilities(&[]);
        able.provider_accounts = vec![ProviderAccountCapability {
            provider: "claude".to_owned(),
            account: "personal".to_owned(),
            models: Vec::new(),
            headroom: None,
        }];
        able.reachable_remotes = vec!["github.com/acme/atlas".to_owned()];
        harness.registry.register("able", able);
        harness.registry.register("bare", capabilities(&[]));

        let constraints = PlacementConstraints {
            required_provider_accounts: vec!["personal".to_owned()],
            required_remotes: vec!["github.com/acme/atlas".to_owned()],
            ..PlacementConstraints::default()
        };
        let request = PlacementRequest {
            constraints: &constraints,
            ..request_defaults()
        };
        assert_eq!(placed_on(harness.scheduler.place(&request)), "able");

        harness.registry.deregister("able");
        let reason = queued_reason(harness.scheduler.place(&request));
        assert_eq!(
            reason,
            "no connected worker matches: bare lacks provider accounts \
             [`personal`], cannot reach remotes [`github.com/acme/atlas`]"
        );
    }

    /// Zero declares no limit; a declared limit refuses at the limit, not
    /// before it, and the refusal shows the arithmetic.
    #[test]
    fn a_worker_at_its_declared_capacity_is_refused_with_the_numbers() {
        assert!(!at_capacity(0, 1_000), "zero declares no limit");
        assert!(!at_capacity(2, 1), "below the limit is room");
        assert!(at_capacity(2, 2), "the limit itself is full");
        assert!(at_capacity(2, 3));

        let harness = harness();
        let mut limited = capabilities(&[]);
        limited.max_concurrent_agents = 2;
        harness.registry.register("small", limited);

        let mut load = BTreeMap::new();
        load.insert("small".to_owned(), 2_u64);
        let constraints = PlacementConstraints::default();
        let reason = queued_reason(harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            load: &load,
            ..request_defaults()
        }));
        assert_eq!(
            reason,
            "no connected worker matches: small is at capacity (2/2)"
        );
    }

    #[test]
    fn a_workspace_spawn_requires_a_worker_that_supports_workspaces() {
        let harness = harness();
        let mut bare = capabilities(&[]);
        bare.supports_workspaces = false;
        harness.registry.register("bare", bare);

        let constraints = PlacementConstraints::default();
        let reason = queued_reason(harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            workspace_definition_id: Some("ws-1"),
            ..request_defaults()
        }));
        assert_eq!(
            reason,
            "no connected worker matches: bare does not support workspaces"
        );

        // The same worker takes workspace-less spawns: absence of support is a
        // fact about workspaces, not a general refusal.
        assert_eq!(placed_on(place_default(&harness)), "bare");
    }

    #[test]
    fn a_pinned_worker_is_demanded_not_matched() {
        let harness = harness();
        let mut tagged = capabilities(&["gpu"]);
        tagged.max_concurrent_agents = 1;
        harness.registry.register("pinned-box", tagged);
        harness.registry.register("idle-box", capabilities(&[]));

        // Constraints the pinned worker fails (a tag it lacks, a full slot)
        // are skipped: pinning is "demand this worker".
        let mut load = BTreeMap::new();
        load.insert("pinned-box".to_owned(), 5_u64);
        let constraints = PlacementConstraints {
            pinned_worker_id: Some(WorkerId::new("pinned-box")),
            required_tags: vec!["macos".to_owned()],
            ..PlacementConstraints::default()
        };
        let placed = harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            load: &load,
            ..request_defaults()
        });
        assert_eq!(placed_on(placed), "pinned-box");
    }

    #[test]
    fn a_pin_on_a_disconnected_worker_queues_with_its_name() {
        let harness = harness();
        harness.registry.register("other", capabilities(&[]));

        let constraints = PlacementConstraints {
            pinned_worker_id: Some(WorkerId::new("gpu-box")),
            ..PlacementConstraints::default()
        };
        let reason = queued_reason(harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            ..request_defaults()
        }));
        assert_eq!(reason, "pinned worker `gpu-box` is not connected");
    }

    /// The one constraint a pin cannot skip: a worker that would refuse the
    /// materialize command can never satisfy a workspace spawn.
    #[test]
    fn a_pin_cannot_force_a_workspace_onto_a_worker_that_refuses_them() {
        let harness = harness();
        let mut bare = capabilities(&[]);
        bare.supports_workspaces = false;
        harness.registry.register("bare", bare);

        let constraints = PlacementConstraints {
            pinned_worker_id: Some(WorkerId::new("bare")),
            ..PlacementConstraints::default()
        };
        let reason = queued_reason(harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            workspace_definition_id: Some("ws-1"),
            ..request_defaults()
        }));
        assert_eq!(reason, "pinned worker `bare` does not support workspaces");
    }

    #[test]
    fn an_empty_pin_id_is_no_pin() {
        let constraints = PlacementConstraints {
            pinned_worker_id: Some(WorkerId::new("")),
            ..PlacementConstraints::default()
        };
        assert_eq!(pinned_worker(&constraints), None);
        assert_eq!(pinned_worker(&PlacementConstraints::default()), None);
    }

    // -- Preference --------------------------------------------------------

    fn candidate(worker_id: &str, warm: bool, on_parent: bool, load: u64) -> Candidate {
        Candidate {
            worker_id: worker_id.to_owned(),
            warm,
            on_parent,
            load,
        }
    }

    /// The whole preference order in one table: warmth beats the parent's
    /// worker beats load beats name, and each rung only decides when every
    /// rung above it ties.
    #[test]
    fn preference_orders_warmth_then_parent_then_load_then_name() {
        assert_eq!(preferred(Vec::new()), None);

        assert_eq!(
            preferred(vec![
                candidate("parent", false, true, 0),
                candidate("warm", true, false, 9),
            ]),
            Some("warm".to_owned()),
            "warmth beats the parent's worker even fully loaded"
        );
        assert_eq!(
            preferred(vec![
                candidate("idle", false, false, 0),
                candidate("parent", false, true, 9),
            ]),
            Some("parent".to_owned()),
            "the parent's worker beats a merely idle one"
        );
        assert_eq!(
            preferred(vec![
                candidate("busy", false, false, 3),
                candidate("idle", false, false, 1),
            ]),
            Some("idle".to_owned()),
            "load decides when nothing else does"
        );
        assert_eq!(
            preferred(vec![
                candidate("b", false, false, 1),
                candidate("a", false, false, 1),
            ]),
            Some("a".to_owned()),
            "the final tiebreak is deterministic, not arbitrary"
        );
    }

    #[test]
    fn warmth_drives_colocation_through_the_scheduler() {
        let harness = harness();
        harness.registry.register("cold", capabilities(&[]));
        harness.registry.register("warm", capabilities(&[]));
        harness.instances.record(&WorkspaceInstanceReport {
            workspace_definition_id: Some(WorkspaceDefinitionId::new("ws-1")),
            worker_id: Some(WorkerId::new("warm")),
            state: WorkspaceInstanceState::Ready as i32,
            ..WorkspaceInstanceReport::default()
        });

        let constraints = PlacementConstraints::default();
        let placed = harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            workspace_definition_id: Some("ws-1"),
            ..request_defaults()
        });
        assert_eq!(placed_on(placed), "warm");
    }

    #[test]
    fn the_parents_worker_is_preferred_when_nothing_forces_otherwise() {
        let harness = harness();
        harness.registry.register("a", capabilities(&[]));
        harness.registry.register("b", capabilities(&[]));

        let constraints = PlacementConstraints::default();
        let placed = harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            parent_worker_id: Some("b"),
            ..request_defaults()
        });
        assert_eq!(placed_on(placed), "b");
    }

    #[test]
    fn load_spreads_agents_across_otherwise_equal_workers() {
        let harness = harness();
        harness.registry.register("a", capabilities(&[]));
        harness.registry.register("b", capabilities(&[]));

        let mut load = BTreeMap::new();
        load.insert("a".to_owned(), 4_u64);
        load.insert("b".to_owned(), 1_u64);
        let constraints = PlacementConstraints::default();
        let placed = harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            load: &load,
            ..request_defaults()
        });
        assert_eq!(placed_on(placed), "b");
    }

    #[test]
    fn a_multi_worker_refusal_names_every_worker_and_every_shortfall() {
        let harness = harness();
        harness.registry.register("a", capabilities(&[]));
        let mut limited = capabilities(&["gpu"]);
        limited.max_concurrent_agents = 1;
        harness.registry.register("b", limited);

        let mut load = BTreeMap::new();
        load.insert("b".to_owned(), 1_u64);
        let constraints = PlacementConstraints {
            required_tags: vec!["gpu".to_owned()],
            ..PlacementConstraints::default()
        };
        let reason = queued_reason(harness.scheduler.place(&PlacementRequest {
            constraints: &constraints,
            load: &load,
            ..request_defaults()
        }));
        assert_eq!(
            reason,
            "no connected worker matches: a lacks tags [`gpu`]; \
             b is at capacity (1/1)"
        );
    }
}
