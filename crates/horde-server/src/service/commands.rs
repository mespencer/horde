//! The per-worker command queue.
//!
//! Commands are at-least-once and carry a dense per-worker sequence, so a
//! worker that reconnects resumes from the last one it durably applied rather
//! than replaying blindly (`worker_service.proto`).
//!
//! **Known gap, stated rather than hidden:** this queue is in memory, so a
//! server restart loses undelivered commands. That is wrong by ADR-0001's
//! standard — everything that matters should be an event — and the fix is not a
//! commands table but deriving the stream from the log, since every command
//! already corresponds to an event that caused it (a spawn command to an
//! `AgentSpawned`, a delivery to an `AssignmentSent`). Doing that properly
//! needs the worker to exist to reconcile against, so it lands with the worker
//! in wave 3. Until then a restart requires workers to re-register, and this
//! comment is the reason a `commands` table should not be added in the
//! meantime.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use horde_proto::v1::StreamCommandsResponse;
use tokio::sync::Notify;

/// Queued commands for one worker.
#[derive(Debug, Default)]
struct WorkerQueue {
    /// Dense, 1-based, monotonic across reconnects.
    next_sequence: u64,
    pending: VecDeque<StreamCommandsResponse>,
    /// Highest command the worker says it durably applied.
    acked: u64,
}

/// Command queues for every worker, with wakeups for waiting streams.
#[derive(Debug, Clone, Default)]
pub struct CommandQueues {
    queues: Arc<Mutex<BTreeMap<String, WorkerQueue>>>,
    /// Woken on every enqueue. One notifier for all workers is enough: a
    /// stream re-checks its own queue and sleeps again if the wakeup was for
    /// someone else, which costs a comparison and avoids a map of notifiers.
    notify: Arc<Notify>,
}

impl CommandQueues {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queues a command, assigning its sequence and id.
    ///
    /// Returns the assigned sequence, or `None` if the lock was poisoned.
    pub fn enqueue(
        &self,
        worker_id: &str,
        command: horde_proto::v1::stream_commands_response::Command,
    ) -> Option<u64> {
        let sequence = {
            let mut queues = self.queues.lock().ok()?;
            let queue = queues.entry(worker_id.to_owned()).or_default();
            queue.next_sequence = queue.next_sequence.saturating_add(1);
            let sequence = queue.next_sequence;
            queue.pending.push_back(StreamCommandsResponse {
                command_sequence: sequence,
                // Stable across redelivery: the worker dedupes on this, so it
                // must not be regenerated when a command is replayed.
                command_id: format!("{worker_id}:{sequence}"),
                command: Some(command),
            });
            sequence
        };
        self.notify.notify_waiters();
        Some(sequence)
    }

    /// Commands at or after `from_sequence`, without removing them.
    ///
    /// Nothing is dropped on read: the worker's ack is what releases a command,
    /// because a worker that crashes between receiving and applying must see it
    /// again.
    #[must_use]
    pub fn since(&self, worker_id: &str, from_sequence: u64) -> Vec<StreamCommandsResponse> {
        self.queues
            .lock()
            .map(|queues| {
                queues.get(worker_id).map_or_else(Vec::new, |queue| {
                    queue
                        .pending
                        .iter()
                        .filter(|command| command.command_sequence >= from_sequence.max(1))
                        .cloned()
                        .collect()
                })
            })
            .unwrap_or_default()
    }

    /// Records the worker's high-water mark and releases everything below it.
    pub fn ack(&self, worker_id: &str, acked_sequence: u64) {
        if let Ok(mut queues) = self.queues.lock()
            && let Some(queue) = queues.get_mut(worker_id)
        {
            queue.acked = queue.acked.max(acked_sequence);
            let acked = queue.acked;
            queue
                .pending
                .retain(|command| command.command_sequence > acked);
        }
    }

    /// The highest command this worker has acked.
    #[must_use]
    pub fn acked(&self, worker_id: &str) -> u64 {
        self.queues
            .lock()
            .map(|queues| queues.get(worker_id).map_or(0, |queue| queue.acked))
            .unwrap_or_default()
    }

    /// Waits for the next enqueue.
    pub async fn changed(&self) {
        self.notify.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{AgentId, InterruptAgentCommand, stream_commands_response::Command};

    use super::CommandQueues;

    fn interrupt(agent: &str) -> Command {
        Command::InterruptAgent(InterruptAgentCommand {
            agent_id: Some(AgentId::new(agent)),
            reason: String::new(),
        })
    }

    #[test]
    fn sequences_are_dense_and_one_based_per_worker() {
        let queues = CommandQueues::new();
        assert_eq!(queues.enqueue("w1", interrupt("a")), Some(1));
        assert_eq!(queues.enqueue("w1", interrupt("b")), Some(2));
        assert_eq!(
            queues.enqueue("w2", interrupt("c")),
            Some(1),
            "each worker numbers independently"
        );
    }

    #[test]
    fn command_ids_are_stable_across_redelivery() {
        let queues = CommandQueues::new();
        queues.enqueue("w1", interrupt("a"));

        let first = queues.since("w1", 1);
        let again = queues.since("w1", 1);
        assert_eq!(first[0].command_id, again[0].command_id);
    }

    /// A read must not consume: a worker that crashes between receiving a
    /// command and applying it has to see it again.
    #[test]
    fn reading_does_not_remove_commands() {
        let queues = CommandQueues::new();
        queues.enqueue("w1", interrupt("a"));

        assert_eq!(queues.since("w1", 1).len(), 1);
        assert_eq!(queues.since("w1", 1).len(), 1);
    }

    #[test]
    fn acking_releases_everything_at_or_below_the_watermark() {
        let queues = CommandQueues::new();
        queues.enqueue("w1", interrupt("a"));
        queues.enqueue("w1", interrupt("b"));
        queues.enqueue("w1", interrupt("c"));

        queues.ack("w1", 2);
        let remaining: Vec<u64> = queues
            .since("w1", 1)
            .iter()
            .map(|command| command.command_sequence)
            .collect();
        assert_eq!(remaining, vec![3]);
        assert_eq!(queues.acked("w1"), 2);
    }

    #[test]
    fn an_ack_never_moves_backwards() {
        let queues = CommandQueues::new();
        queues.enqueue("w1", interrupt("a"));
        queues.enqueue("w1", interrupt("b"));

        queues.ack("w1", 2);
        queues.ack("w1", 1);
        assert_eq!(queues.acked("w1"), 2, "a stale ack must not undo progress");
    }

    #[test]
    fn resuming_from_a_later_sequence_skips_what_the_worker_already_applied() {
        let queues = CommandQueues::new();
        queues.enqueue("w1", interrupt("a"));
        queues.enqueue("w1", interrupt("b"));

        let from_second: Vec<u64> = queues
            .since("w1", 2)
            .iter()
            .map(|command| command.command_sequence)
            .collect();
        assert_eq!(from_second, vec![2]);
    }

    #[test]
    fn an_unknown_worker_has_an_empty_queue_rather_than_an_error() {
        let queues = CommandQueues::new();
        assert!(queues.since("never-registered", 1).is_empty());
        assert_eq!(queues.acked("never-registered"), 0);
    }
}
