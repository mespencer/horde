//! The in-process broadcast of every record a process writes (ADR-0015).
//!
//! Follow mode reads it: a reader subscribes, reads the files, then keeps
//! taking records off the tap. The tap is bounded; a receiver that falls
//! behind is told how many records it missed instead of slowing the writer.

use std::sync::Arc;

use tokio::sync::broadcast;

/// How many records the tap holds for its slowest receiver.
pub const TAP_CAPACITY: usize = 4096;

/// Records under this target go to the file but never to the tap: the code
/// that serves logs logs under it, so a follow never follows itself.
pub const QUIET_TARGET: &str = "horde_logs";

/// The tap: cheap to clone, every clone publishing into the same channel.
#[derive(Debug, Clone)]
pub struct LogTap {
    sender: broadcast::Sender<Arc<str>>,
}

impl Default for LogTap {
    fn default() -> Self {
        Self::new(TAP_CAPACITY)
    }
}

impl LogTap {
    /// A tap holding `capacity` records for its slowest receiver.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Publishes one serialized record. Never blocks; with no receivers the
    /// record is simply not kept.
    pub fn publish(&self, line: Arc<str>) {
        let _ = self.sender.send(line);
    }

    /// A receiver that sees every record published from now on.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        self.sender.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::broadcast::error::TryRecvError;

    use super::LogTap;

    #[test]
    fn a_lagging_receiver_learns_how_much_it_missed() {
        let tap = LogTap::new(2);
        let mut receiver = tap.subscribe();
        for n in 0..5 {
            tap.publish(Arc::from(format!("r{n}")));
        }
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Lagged(3)));
        assert_eq!(receiver.try_recv().as_deref(), Ok("r3"));
        assert_eq!(receiver.try_recv().as_deref(), Ok("r4"));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn publishing_with_nobody_listening_is_harmless() {
        let tap = LogTap::default();
        tap.publish(Arc::from("nobody"));
        let mut late = tap.subscribe();
        assert_eq!(late.try_recv(), Err(TryRecvError::Empty));
    }
}
