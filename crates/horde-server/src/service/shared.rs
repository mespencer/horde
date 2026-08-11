//! What both service spines share.
//!
//! Lives beside the spines rather than in their parent module: the parent
//! re-exports both spines, so anything the spines import from it would close a
//! module cycle.

use std::sync::Arc;

use crate::log::EventLog;
use crate::scheduler::{Scheduler, SingleWorkerScheduler, WorkerRegistry};

/// Everything the two services share.
///
/// One value, cloned into both, so there is no question of them disagreeing
/// about which log or which registry they are talking to.
///
/// There is no command queue here any more. Commands are folded out of `log`
/// on demand by `service::commands`, so the only thing to share is the log
/// they are derived from — which is also the only thing that survives a
/// restart.
#[derive(Clone)]
pub struct ServerCore {
    pub log: EventLog,
    pub workers: WorkerRegistry,
    pub scheduler: Arc<dyn Scheduler>,
}

impl ServerCore {
    /// Builds a core around a log, with the bootstrap single-worker scheduler.
    #[must_use]
    pub fn new(log: EventLog) -> Self {
        let workers = WorkerRegistry::new();
        let scheduler = Arc::new(SingleWorkerScheduler::new(workers.clone()));
        Self {
            log,
            workers,
            scheduler,
        }
    }

    /// Replaces the scheduler. Wave 4 swaps in the real one here.
    #[must_use]
    pub fn with_scheduler(mut self, scheduler: Arc<dyn Scheduler>) -> Self {
        self.scheduler = scheduler;
        self
    }
}

impl std::fmt::Debug for ServerCore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerCore")
            .field("workers", &self.workers.connected().len())
            .finish_non_exhaustive()
    }
}

/// This build's semver, echoed in every handshake.
pub const SERVER_SEMVER: &str = env!("CARGO_PKG_VERSION");

/// Below this, a peer's role is refused outright (DESIGN.md §2).
pub const MIN_SUPPORTED_SEMVER: &str = "0.1.0";

#[cfg(test)]
mod tests {
    use super::ServerCore;
    use crate::log::EventLog;

    #[test]
    fn a_core_debugs_as_how_many_workers_it_can_see() {
        let core = ServerCore::new(EventLog::in_memory().expect("in-memory log"));
        let rendered = format!("{core:?}");
        assert!(rendered.contains("ServerCore"), "{rendered}");
        assert!(
            rendered.contains('0'),
            "a fresh core has no connected workers: {rendered}"
        );
    }
}
