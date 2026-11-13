//! Building the export pipeline, and what owning one means.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::Layer as _;

use crate::config::OtlpConfig;
use crate::export::{CountingExporter, is_exportable};

/// How long one export attempt may take before its batch is given up on.
/// Paid by the batch thread, never by the product.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long shutdown may spend flushing what is still queued. This bounds a
/// binary's exit latency when the collector is gone.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);

/// The resource attribute carrying the enrolled mTLS principal.
pub const PRINCIPAL_ATTRIBUTE: &str = "horde.principal";

/// Who the exported spans speak for.
#[derive(Debug, Clone)]
pub struct ServiceIdentity {
    service: &'static str,
    principal: Option<String>,
}

impl ServiceIdentity {
    /// Identity for `service` — the binary's own name (`horde-server`,
    /// `horde`, `horde-worker`), which becomes the OTLP `service.name`.
    #[must_use]
    pub fn new(service: &'static str) -> Self {
        Self {
            service,
            principal: None,
        }
    }

    /// Stamps the enrolled principal (`worker:name`, `client:name`) onto
    /// every exported span, as [`PRINCIPAL_ATTRIBUTE`].
    ///
    /// Takes an `Option` because that is what a binary has: enrollment is
    /// optional, and `None` leaves the identity as the bare binary.
    #[must_use]
    pub fn with_principal(mut self, principal: Option<String>) -> Self {
        self.principal = principal;
        self
    }

    /// The binary's name.
    #[must_use]
    pub fn service(&self) -> &'static str {
        self.service
    }

    /// The enrolled principal, when there is one.
    #[must_use]
    pub fn principal(&self) -> Option<String> {
        self.principal.clone()
    }
}

/// A live export pipeline: the tracer provider, and the count of what it has
/// had to drop.
///
/// Dropping it flushes and dismantles the pipeline (bounded by
/// [`SHUTDOWN_TIMEOUT`]); [`Telemetry::shutdown`] does the same with the
/// verdict visible. Both block the calling thread — call them from `main`
/// after the runtime work is done, not from an async task (use
/// `spawn_blocking` there), and while the runtime the pipeline was built on
/// is still alive, because the flush travels its gRPC channel.
#[derive(Debug)]
pub struct Telemetry {
    provider: SdkTracerProvider,
    dropped: Arc<AtomicU64>,
}

/// Why a pipeline could not be built. Always a refusal: the operator asked
/// for export, so starting without it would be an ack with nothing behind it.
#[derive(Debug, thiserror::Error)]
#[error("could not build the OTLP exporter for {endpoint}: {source}")]
pub struct PipelineError {
    endpoint: String,
    source: opentelemetry_otlp::ExporterBuildError,
}

/// Why startup telemetry could not be arranged: the environment was unusable,
/// or the exporter it asked for could not be built.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// The environment variables were present but unusable.
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    /// The exporter could not be built from a valid-looking configuration.
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

/// One-call startup for a binary: read the environment, and build the
/// pipeline when it opts in.
///
/// `Ok(None)` is the default-off path: nothing was built, nothing changed.
/// Must be called within a tokio runtime when export is configured — the
/// exporter's gRPC channel lives on it (in a sync `main`, wrap the call in
/// `runtime.enter()`).
///
/// # Errors
///
/// [`InitError`], to be printed as a startup refusal.
pub fn init_from_env(
    identity: ServiceIdentity,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<Telemetry>, InitError> {
    match crate::config::from_env(lookup)? {
        None => Ok(None),
        Some(config) => Ok(Some(pipeline(&config, &identity)?)),
    }
}

/// Builds the export pipeline: OTLP/gRPC exporter, drop-counting wrapper,
/// bounded batching on a dedicated thread, parent-respecting ratio sampling,
/// and the service's resource identity.
///
/// Must be called within a tokio runtime — the exporter's gRPC channel lives
/// on it. The channel connects lazily, so an unreachable collector does not
/// fail here; it fails per-batch, counted, later.
///
/// # Errors
///
/// [`PipelineError`] when the exporter cannot be built — an unparseable
/// endpoint, in practice.
pub fn pipeline(
    config: &OtlpConfig,
    identity: &ServiceIdentity,
) -> Result<Telemetry, PipelineError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.endpoint.clone())
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|source| PipelineError {
            endpoint: config.endpoint.clone(),
            source,
        })?;
    let (counting, dropped) = CountingExporter::new(exporter);
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(counting)
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            config.sample_ratio,
        ))))
        .with_resource(resource(identity))
        .build();
    Ok(Telemetry { provider, dropped })
}

/// The OTLP resource: `service.name` from the binary, the principal when
/// enrolled.
fn resource(identity: &ServiceIdentity) -> Resource {
    let builder = Resource::builder().with_service_name(identity.service);
    match &identity.principal {
        Some(principal) => builder
            .with_attribute(KeyValue::new(PRINCIPAL_ATTRIBUTE, principal.clone()))
            .build(),
        None => builder.build(),
    }
}

impl Telemetry {
    /// The tracing layer that feeds this pipeline, for composing into a
    /// subscriber (or take a whole one from
    /// [`stderr_subscriber`](crate::stderr_subscriber) /
    /// [`silent_subscriber`](crate::silent_subscriber)).
    ///
    /// Filtered so the export machinery's own telemetry is never exported —
    /// see `export::is_exportable` for why that would otherwise loop.
    pub fn layer<S>(&self) -> impl tracing_subscriber::Layer<S> + Send + Sync + 'static + use<S>
    where
        S: tracing::Subscriber
            + for<'span> tracing_subscriber::registry::LookupSpan<'span>
            + Send
            + Sync,
    {
        tracing_opentelemetry::layer()
            .with_tracer(self.provider.tracer("horde-observe"))
            .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
                is_exportable(metadata.target())
            }))
    }

    /// How many spans reached the exporter and were dropped because the
    /// collector could not take them. Monotonic over the pipeline's life;
    /// zero is health. (Spans dropped by a full queue before reaching the
    /// exporter are additional, and complained about by the SDK itself.)
    #[must_use]
    pub fn dropped_spans(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Pushes everything queued at the collector now, and says whether it
    /// arrived. The product never needs this — the batch thread ships on its
    /// own cadence — but a test asserting on arrival does, and so would a
    /// short-lived process handing off before exit.
    ///
    /// # Errors
    ///
    /// The SDK's verdict when the flush could not complete — an unreachable
    /// collector, in practice.
    pub fn flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.provider.force_flush()
    }

    /// Flushes what is queued (bounded by [`SHUTDOWN_TIMEOUT`]) and
    /// dismantles the pipeline. See the type docs for where to call this.
    /// Takes `&self` so what the shutdown flush could not deliver is still
    /// readable off [`dropped_spans`](Self::dropped_spans) afterwards; a
    /// second call finds nothing left to do and says so.
    ///
    /// # Errors
    ///
    /// The SDK's verdict; a binary exiting anyway may reasonably discard it,
    /// because a collector that is down must not change an exit code.
    pub fn shutdown(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT)
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        // The layer's tracer keeps the provider alive from inside the global
        // subscriber, so the SDK's own last-reference shutdown would never
        // fire; this one does. Second calls (after an explicit `shutdown`)
        // report AlreadyShutdown, which is exactly nothing left to do.
        let _ = self.provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
    }
}

#[cfg(test)]
mod tests {
    use super::{InitError, ServiceIdentity, init_from_env};
    use crate::config::{ENDPOINT_VAR, SAMPLE_VAR};

    /// The whole opt-in truth table through the one call the binaries make.
    /// (What a built pipeline *does* is the smoke suite's business; this
    /// pins that one is or is not built, and that refusals pass through.)
    #[tokio::test(flavor = "multi_thread")]
    async fn init_from_env_builds_exactly_when_opted_in() {
        let off = init_from_env(ServiceIdentity::new("probe"), |_| None);
        assert!(
            matches!(off, Ok(None)),
            "no environment, no pipeline: {off:?}"
        );

        let on = init_from_env(ServiceIdentity::new("probe"), |name| {
            (name == ENDPOINT_VAR).then(|| "http://127.0.0.1:1".to_owned())
        });
        assert!(
            matches!(on, Ok(Some(_))),
            "an endpoint must build a (lazy) pipeline: {:?}",
            on.map(|telemetry| telemetry.is_some())
        );

        let misconfigured = init_from_env(ServiceIdentity::new("probe"), |name| {
            (name == SAMPLE_VAR).then(|| "0.5".to_owned())
        });
        assert!(
            matches!(misconfigured, Err(InitError::Config(_))),
            "config refusals must pass through: {misconfigured:?}"
        );

        let unbuildable = init_from_env(ServiceIdentity::new("probe"), |name| {
            (name == ENDPOINT_VAR).then(|| "not a uri".to_owned())
        });
        assert!(
            matches!(unbuildable, Err(InitError::Pipeline(_))),
            "an endpoint the exporter cannot parse must refuse: {unbuildable:?}"
        );
    }
}
