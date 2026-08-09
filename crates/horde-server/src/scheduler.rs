//! Placement: which worker runs an agent.
//!
//! Wave 4 owns the real scheduler (capability matching, headroom, queueing with
//! reasons). What exists here is the seam it will slot into, plus the only
//! policy that is honest today: with one worker registered, everything goes
//! there, and with none, a spawn is queued with a reason rather than failing.
//!
//! The seam is worth having now because the alternative — calling a registry
//! directly from the service layer — is what makes a scheduler hard to
//! introduce later.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use horde_proto::v1::{PlacementConstraints, WorkerCapabilities};

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

/// Chooses a worker for an agent.
pub trait Scheduler: Send + Sync {
    fn place(&self, constraints: &PlacementConstraints) -> Placement;
}

/// The registry of connected workers, shared by the worker service and the
/// scheduler.
#[derive(Debug, Clone, Default)]
pub struct WorkerRegistry {
    workers: Arc<Mutex<BTreeMap<String, RegisteredWorker>>>,
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
        if let Ok(mut workers) = self.workers.lock() {
            workers.insert(
                worker_id.to_owned(),
                RegisteredWorker {
                    worker_id: worker_id.to_owned(),
                    capabilities,
                },
            );
        }
    }

    pub fn deregister(&self, worker_id: &str) {
        if let Ok(mut workers) = self.workers.lock() {
            workers.remove(worker_id);
        }
    }

    #[must_use]
    pub fn connected(&self) -> Vec<RegisteredWorker> {
        self.workers
            .lock()
            .map(|workers| workers.values().cloned().collect())
            .unwrap_or_default()
    }

    #[must_use]
    pub fn contains(&self, worker_id: &str) -> bool {
        self.workers
            .lock()
            .is_ok_and(|workers| workers.contains_key(worker_id))
    }
}

/// The bootstrap policy: place on the single registered worker.
///
/// Deliberately refuses to guess when there is more than one. Picking
/// arbitrarily would produce a system that appears to schedule and does not,
/// and the failure would only surface as agents landing in surprising places.
#[derive(Debug, Clone)]
pub struct SingleWorkerScheduler {
    registry: WorkerRegistry,
}

impl SingleWorkerScheduler {
    #[must_use]
    pub fn new(registry: WorkerRegistry) -> Self {
        Self { registry }
    }
}

impl Scheduler for SingleWorkerScheduler {
    fn place(&self, constraints: &PlacementConstraints) -> Placement {
        let workers = self.registry.connected();

        match workers.as_slice() {
            [] => Placement::Queued {
                reason: "no worker is connected".to_owned(),
            },
            [only] => {
                // Constraint matching is wave-4 work. Saying so is better than
                // silently honouring constraints we do not evaluate.
                if constraints.required_tags.is_empty()
                    && constraints.required_provider_accounts.is_empty()
                {
                    Placement::Placed {
                        worker_id: only.worker_id.clone(),
                    }
                } else {
                    Placement::Queued {
                        reason: format!(
                            "placement constraints are not evaluated yet; \
                             `{}` is the only connected worker",
                            only.worker_id
                        ),
                    }
                }
            }
            many => Placement::Queued {
                reason: format!(
                    "{} workers are connected but capability matching lands in wave 4; \
                     refusing to choose arbitrarily",
                    many.len()
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{PlacementConstraints, WorkerCapabilities};

    use super::{Placement, Scheduler, SingleWorkerScheduler, WorkerRegistry};

    fn scheduler() -> (WorkerRegistry, SingleWorkerScheduler) {
        let registry = WorkerRegistry::new();
        let scheduler = SingleWorkerScheduler::new(registry.clone());
        (registry, scheduler)
    }

    #[test]
    fn with_no_worker_a_spawn_queues_with_a_reason_rather_than_failing() {
        let (_registry, scheduler) = scheduler();
        match scheduler.place(&PlacementConstraints::default()) {
            Placement::Queued { reason } => assert!(reason.contains("no worker")),
            other => panic!("expected Queued, got {other:?}"),
        }
    }

    #[test]
    fn with_one_worker_everything_lands_there() {
        let (registry, scheduler) = scheduler();
        registry.register("worker-a", WorkerCapabilities::default());

        assert_eq!(
            scheduler.place(&PlacementConstraints::default()),
            Placement::Placed {
                worker_id: "worker-a".to_owned()
            }
        );
    }

    /// Honouring a constraint we do not evaluate would be worse than queueing:
    /// the caller would believe `gpu` was matched.
    #[test]
    fn constraints_queue_rather_than_being_silently_ignored() {
        let (registry, scheduler) = scheduler();
        registry.register("worker-a", WorkerCapabilities::default());

        let constrained = PlacementConstraints {
            required_tags: vec!["gpu".to_owned()],
            ..PlacementConstraints::default()
        };
        match scheduler.place(&constrained) {
            Placement::Queued { reason } => assert!(reason.contains("not evaluated yet")),
            other => panic!("expected Queued, got {other:?}"),
        }
    }

    #[test]
    fn more_than_one_worker_queues_rather_than_choosing_arbitrarily() {
        let (registry, scheduler) = scheduler();
        registry.register("worker-a", WorkerCapabilities::default());
        registry.register("worker-b", WorkerCapabilities::default());

        match scheduler.place(&PlacementConstraints::default()) {
            Placement::Queued { reason } => assert!(reason.contains("wave 4")),
            other => panic!("expected Queued, got {other:?}"),
        }
    }

    #[test]
    fn deregistering_removes_a_worker_from_placement() {
        let (registry, scheduler) = scheduler();
        registry.register("worker-a", WorkerCapabilities::default());
        assert!(registry.contains("worker-a"));

        registry.deregister("worker-a");
        assert!(!registry.contains("worker-a"));
        assert!(matches!(
            scheduler.place(&PlacementConstraints::default()),
            Placement::Queued { .. }
        ));
    }
}
