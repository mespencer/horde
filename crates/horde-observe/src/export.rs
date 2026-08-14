//! The wrapper that makes loss visible, and the filter that keeps the
//! pipeline from watching itself.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SpanData, SpanExporter};

/// Targets whose telemetry *is* the export machinery: the OTLP exporter's own
/// internals, and the gRPC stack it ships batches through. Exporting these
/// would make every export produce telemetry that the next export exports —
/// the pipeline watching itself, forever. The price is that the product's own
/// use of the same crates (the server is a tonic server) is console-only,
/// which is where that noise belongs anyway.
const SUPPRESSED_TARGETS: [&str; 7] = [
    "h2",
    "horde_observe",
    "hyper",
    "hyper_util",
    "opentelemetry",
    "tonic",
    "tower",
];

/// Whether a span or event with this target may leave the process.
///
/// A target is suppressed when it is a suppressed crate's root (`hyper`) or a
/// module under one (`hyper::client`) — and only then: `hyperx` is somebody
/// else's crate.
pub(crate) fn is_exportable(target: &str) -> bool {
    !SUPPRESSED_TARGETS.iter().any(|prefix| {
        target == *prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with("::"))
    })
}

/// A [`SpanExporter`] that counts what its inner exporter fails to deliver.
///
/// The batch processor already gives up on a batch when its exporter errors —
/// that is the "never block the product" half of graceful degradation. This
/// wrapper adds the honesty half: the loss is counted where
/// [`Telemetry::dropped_spans`](crate::Telemetry::dropped_spans) can report
/// it, and said out loud on the console log (under a target the filter above
/// keeps out of the pipeline, so the complaint cannot loop back in).
#[derive(Debug)]
pub(crate) struct CountingExporter<E> {
    inner: E,
    dropped: Arc<AtomicU64>,
}

impl<E> CountingExporter<E> {
    /// Wraps `inner`, returning the shared counter alongside.
    pub(crate) fn new(inner: E) -> (Self, Arc<AtomicU64>) {
        let dropped = Arc::new(AtomicU64::new(0));
        (
            Self {
                inner,
                dropped: Arc::clone(&dropped),
            },
            dropped,
        )
    }
}

impl<E: SpanExporter> SpanExporter for CountingExporter<E> {
    fn export(
        &self,
        batch: Vec<SpanData>,
    ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
        let spans = batch.len() as u64;
        let export = self.inner.export(batch);
        let dropped = Arc::clone(&self.dropped);
        async move {
            let result = export.await;
            if let Err(error) = &result {
                dropped.fetch_add(spans, Ordering::Relaxed);
                tracing::warn!(
                    target: "horde_observe",
                    %error,
                    spans,
                    "OTLP export failed; the batch is dropped"
                );
            }
            result
        }
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
    use opentelemetry_sdk::trace::{SpanData, SpanExporter};

    use super::{CountingExporter, is_exportable};

    #[test]
    fn product_targets_export_and_pipeline_targets_do_not() {
        for exportable in [
            "horde_server::service",
            "horde_worker",
            "horde_cli",
            "hyperx",       // prefix of a suppressed crate, different crate
            "tonic_health", // underscore, not a module separator
            "h2c::stream",
        ] {
            assert!(is_exportable(exportable), "{exportable:?} must export");
        }
        for suppressed in [
            "h2",
            "h2::codec",
            "horde_observe",
            "hyper",
            "hyper::proto",
            "hyper_util::client",
            "opentelemetry",
            "opentelemetry_sdk", // no: this is a distinct crate...
            "tonic",
            "tonic::transport",
            "tower::buffer",
        ] {
            if suppressed == "opentelemetry_sdk" {
                // ...and it stays exportable only if listed so; it is not, and
                // the `opentelemetry` entry must not swallow it by prefix.
                assert!(
                    is_exportable(suppressed),
                    "{suppressed:?} is a crate of its own"
                );
                continue;
            }
            assert!(!is_exportable(suppressed), "{suppressed:?} must not export");
        }
    }

    /// A scripted inner exporter: the wrapper under test is the production
    /// one; this only records what reached it. Interior mutability because
    /// the trait hands shutdown and flush a shared receiver.
    #[derive(Debug, Default)]
    struct Recording {
        export_error: Option<String>,
        resources_seen: u32,
        flushes: std::sync::atomic::AtomicU32,
        shutdown_timeouts: std::sync::Mutex<Vec<Duration>>,
    }

    impl SpanExporter for Recording {
        fn export(
            &self,
            _batch: Vec<SpanData>,
        ) -> impl std::future::Future<Output = OTelSdkResult> + Send {
            let scripted = self.export_error.clone().map(OTelSdkError::InternalFailure);
            async move {
                match scripted {
                    Some(error) => Err(error),
                    None => Ok(()),
                }
            }
        }

        fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
            self.shutdown_timeouts
                .lock()
                .expect("no test panics while recording")
                .push(timeout);
            Ok(())
        }

        fn force_flush(&self) -> OTelSdkResult {
            self.flushes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn set_resource(&mut self, _resource: &Resource) {
            self.resources_seen += 1;
        }
    }

    #[test]
    fn lifecycle_calls_reach_the_inner_exporter() {
        // The exporter learns its resource through `set_resource`; a wrapper
        // that swallowed it would ship every span as `unknown_service`. Same
        // for flush and shutdown: dropped on the floor here, spans queued at
        // exit would silently never leave.
        let (mut counting, _dropped) = CountingExporter::new(Recording::default());
        counting.set_resource(&Resource::builder_empty().build());
        counting.set_resource(&Resource::builder_empty().build());
        assert_eq!(counting.inner.resources_seen, 2);

        assert!(counting.force_flush().is_ok());
        assert_eq!(
            counting
                .inner
                .flushes
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        assert!(
            counting
                .shutdown_with_timeout(Duration::from_secs(7))
                .is_ok()
        );
        assert_eq!(
            *counting
                .inner
                .shutdown_timeouts
                .lock()
                .expect("no test panics while recording"),
            [Duration::from_secs(7)]
        );
    }

    #[test]
    fn a_successful_export_counts_nothing_and_a_failure_passes_through() {
        let (counting, dropped) = CountingExporter::new(Recording::default());
        let result = futures_executor(counting.export(Vec::new()));
        assert!(result.is_ok());
        assert_eq!(dropped.load(std::sync::atomic::Ordering::Relaxed), 0);

        let (counting, _dropped) = CountingExporter::new(Recording {
            export_error: Some("scripted".to_owned()),
            ..Recording::default()
        });
        let result = futures_executor(counting.export(Vec::new()));
        assert!(
            matches!(result, Err(OTelSdkError::InternalFailure(ref why)) if why == "scripted"),
            "the inner verdict must pass through unchanged: {result:?}"
        );
    }

    /// Polls a ready-on-first-poll future to completion without a runtime;
    /// both scripted exports above resolve immediately.
    fn futures_executor<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => output,
            std::task::Poll::Pending => panic!("a scripted export must resolve immediately"),
        }
    }
}
