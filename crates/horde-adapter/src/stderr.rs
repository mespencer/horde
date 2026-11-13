//! Where a harness's stderr goes besides the death tail (ADR-0015).
//!
//! The Worker scopes a sink around each `spawn`/`resume` call; whatever
//! process the adapter starts inside that scope captures the sink with
//! [`current`] and hands every stderr line to it. A task-local rather than
//! an [`AgentSpec`](crate::AgentSpec) field: the spec is plain data that
//! derives equality, and a sink is a live handle that belongs to whoever is
//! supervising the process, not to the agent's identity.

use std::future::Future;
use std::sync::Arc;

/// Receives a harness's stderr, one line at a time.
pub trait StderrSink: Send + Sync {
    /// One line, without its newline. Must not block for long and must not
    /// fail: stderr is diagnostics, never protocol.
    fn line(&self, text: &str);
}

tokio::task_local! {
    static SINK: Arc<dyn StderrSink>;
}

/// Runs `future` with `sink` as the current stderr sink (none when `None`).
pub async fn scoped<F: Future>(sink: Option<Arc<dyn StderrSink>>, future: F) -> F::Output {
    match sink {
        Some(sink) => SINK.scope(sink, future).await,
        None => future.await,
    }
}

/// The sink the current spawn was scoped with, if any — captured by the
/// code that starts a harness, before it spawns the task draining stderr.
#[must_use]
pub fn current() -> Option<Arc<dyn StderrSink>> {
    SINK.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::{StderrSink, current, scoped};

    #[derive(Default)]
    struct Collect(Mutex<Vec<String>>);

    impl StderrSink for Collect {
        fn line(&self, text: &str) {
            if let Ok(mut lines) = self.0.lock() {
                lines.push(text.to_owned());
            }
        }
    }

    #[tokio::test]
    async fn the_sink_is_current_only_inside_its_scope() {
        assert!(current().is_none());
        let collect = Arc::new(Collect::default());
        let sink: Arc<dyn StderrSink> = collect.clone();
        scoped(Some(sink), async {
            if let Some(sink) = current() {
                sink.line("inside");
            }
        })
        .await;
        assert!(current().is_none());
        scoped(None, async {
            assert!(current().is_none(), "no sink means none");
        })
        .await;
        assert_eq!(
            collect
                .0
                .lock()
                .map(|lines| lines.clone())
                .unwrap_or_default(),
            ["inside"]
        );
    }
}
