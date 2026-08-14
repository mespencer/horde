//! The pipeline against a real collector.
//!
//! The collector here is not a mock of this crate's exporter: it is the
//! OTLP/gRPC `TraceService` — the same generated contract a Jaeger or an
//! otel-collector serves — bound on an ephemeral port in this process. What
//! these tests assert is therefore the wire truth: what a real collector
//! would receive from a Horde binary, identity, fields, sampling and all.

use horde_observe::{OtlpConfig, ServiceIdentity, pipeline, silent_subscriber, stderr_subscriber};
use tracing_subscriber::EnvFilter;

#[cfg(test)]
mod harness {
    use std::time::Duration;

    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
        TraceService, TraceServiceServer,
    };
    use opentelemetry_proto::tonic::collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
    };
    use opentelemetry_proto::tonic::common::v1::any_value;
    use tokio::sync::mpsc;

    /// Every wait in this suite is ceilinged; nothing observes a child
    /// process, so one generous bound fits all.
    const CEILING: Duration = Duration::from_secs(30);

    pub(crate) async fn within<F: Future>(what: &str, future: F) -> F::Output {
        match tokio::time::timeout(CEILING, future).await {
            Ok(value) => value,
            Err(_) => panic!("{what} must happen rather than hang"),
        }
    }

    /// The receiving half of the OTLP contract: every export lands on the
    /// channel for the test to read back.
    struct Collector {
        received: mpsc::UnboundedSender<ExportTraceServiceRequest>,
    }

    #[tonic::async_trait]
    impl TraceService for Collector {
        async fn export(
            &self,
            request: tonic::Request<ExportTraceServiceRequest>,
        ) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
            self.received
                .send(request.into_inner())
                .map_err(|_| tonic::Status::unavailable("the test has hung up"))?;
            Ok(tonic::Response::new(ExportTraceServiceResponse::default()))
        }
    }

    /// A live OTLP/gRPC collector on an ephemeral port: its endpoint, and
    /// everything it receives.
    pub(crate) async fn collector() -> (String, mpsc::UnboundedReceiver<ExportTraceServiceRequest>)
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port must bind");
        let address = listener
            .local_addr()
            .expect("a bound socket has an address");
        let (sender, receiver) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(Collector { received: sender }))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        (format!("http://{address}"), receiver)
    }

    /// An endpoint nothing listens on: bound to learn a free port, then
    /// closed again, so a connection there is refused rather than queued.
    pub(crate) async fn dead_endpoint() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("an ephemeral port must bind");
        let address = listener
            .local_addr()
            .expect("a bound socket has an address");
        drop(listener);
        format!("http://{address}")
    }

    /// Every span name in one export request, in wire order.
    pub(crate) fn span_names(request: &ExportTraceServiceRequest) -> Vec<String> {
        request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
            .map(|span| span.name.clone())
            .collect()
    }

    /// Receives export requests until one carries `sentinel`, returning every
    /// span name seen on the way (sentinel included). Bounded by count, not
    /// only by clock: a collector spammed with batches that never contain the
    /// sentinel is a finding, not a hang.
    pub(crate) async fn span_names_until(
        received: &mut mpsc::UnboundedReceiver<ExportTraceServiceRequest>,
        sentinel: &str,
    ) -> Vec<String> {
        let mut names = Vec::new();
        for _ in 0..64 {
            let request = within("an export batch arrives", received.recv())
                .await
                .expect("the collector must outlive the test");
            names.extend(span_names(&request));
            if names.iter().any(|name| name == sentinel) {
                return names;
            }
        }
        panic!("{sentinel:?} must arrive within 64 batches; saw {names:?}");
    }

    /// The string value of an attribute list's `key`, wherever it appears.
    pub(crate) fn string_attribute(
        attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
        key: &str,
    ) -> Option<String> {
        attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.value.as_ref())
            .and_then(|value| match value {
                any_value::Value::StringValue(string) => Some(string.clone()),
                _ => None,
            })
    }

    /// The integer value of an attribute list's `key`, wherever it appears.
    pub(crate) fn int_attribute(
        attributes: &[opentelemetry_proto::tonic::common::v1::KeyValue],
        key: &str,
    ) -> Option<i64> {
        attributes
            .iter()
            .find(|attribute| attribute.key == key)
            .and_then(|attribute| attribute.value.as_ref())
            .and_then(|value| value.value.as_ref())
            .and_then(|value| match value {
                any_value::Value::IntValue(int) => Some(*int),
                _ => None,
            })
    }
}

/// The whole contract in one pass: a span emitted through the composed
/// server-shaped subscriber arrives at the collector named, stamped with the
/// binary's service identity and enrolled principal, carrying its fields and
/// its events — and nothing was dropped to get it there.
#[tokio::test(flavor = "multi_thread")]
async fn an_exported_span_carries_the_service_identity_fields_and_events() {
    let (endpoint, mut received) = harness::collector().await;
    let config = OtlpConfig {
        endpoint,
        sample_ratio: 1.0,
    };
    let identity =
        ServiceIdentity::new("horde-observe-smoke").with_principal(Some("worker:smoke".to_owned()));
    let telemetry = pipeline(&config, &identity).expect("a lazy pipeline must build");

    let dispatch = stderr_subscriber(EnvFilter::new("info"), Some(&telemetry));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!(target: "horde_smoke", "smoke_turn", agent = "a-1");
        let _entered = span.entered();
        tracing::info!(target: "horde_smoke", answer = 42_i64, "smoke_event");
    });

    assert!(
        telemetry.flush().is_ok(),
        "flushing at a live collector must succeed"
    );

    let request = harness::within("the export batch arrives", received.recv())
        .await
        .expect("the collector must receive one batch");

    let resource = request
        .resource_spans
        .first()
        .and_then(|spans| spans.resource.as_ref())
        .expect("an export carries its resource");
    assert_eq!(
        harness::string_attribute(&resource.attributes, "service.name").as_deref(),
        Some("horde-observe-smoke"),
        "the binary's name is the service identity"
    );
    assert_eq!(
        harness::string_attribute(&resource.attributes, horde_observe::PRINCIPAL_ATTRIBUTE)
            .as_deref(),
        Some("worker:smoke"),
        "the enrolled principal rides every export"
    );

    let span = request
        .resource_spans
        .iter()
        .flat_map(|resource| &resource.scope_spans)
        .flat_map(|scope| &scope.spans)
        .find(|span| span.name == "smoke_turn")
        .expect("the emitted span arrives under its own name");
    assert_eq!(
        harness::string_attribute(&span.attributes, "agent").as_deref(),
        Some("a-1"),
        "span fields become span attributes"
    );
    let event = span
        .events
        .iter()
        .find(|event| harness::int_attribute(&event.attributes, "answer") == Some(42))
        .expect("the event inside the span rides along with its fields");
    assert!(
        !event.name.is_empty(),
        "an exported event keeps a name to grep for"
    );

    assert_eq!(
        telemetry.dropped_spans(),
        0,
        "a healthy pipeline reports zero drops"
    );
}

/// Sampling is a real spigot, not a stored number: at ratio 0.0 nothing
/// leaves the process, at 1.0 everything does. The sentinel through the same
/// collector is what makes the absence assertable — by the time it arrives,
/// an unsampled span that was going to leak would already have.
#[tokio::test(flavor = "multi_thread")]
async fn a_zero_ratio_exports_nothing_where_one_exports_everything() {
    let (endpoint, mut received) = harness::collector().await;
    let identity = ServiceIdentity::new("horde-observe-smoke");

    let none = pipeline(
        &OtlpConfig {
            endpoint: endpoint.clone(),
            sample_ratio: 0.0,
        },
        &identity,
    )
    .expect("a lazy pipeline must build");
    let dispatch = silent_subscriber(&none);
    tracing::dispatcher::with_default(&dispatch, || {
        let _entered = tracing::info_span!(target: "horde_smoke", "unsampled_turn").entered();
    });
    assert!(
        none.flush().is_ok(),
        "flushing an unsampled pipeline is a no-op, not a failure"
    );

    let all = pipeline(
        &OtlpConfig {
            endpoint,
            sample_ratio: 1.0,
        },
        &identity,
    )
    .expect("a lazy pipeline must build");
    let dispatch = silent_subscriber(&all);
    tracing::dispatcher::with_default(&dispatch, || {
        let _entered = tracing::info_span!(target: "horde_smoke", "sentinel_turn").entered();
    });
    assert!(all.flush().is_ok(), "the sentinel flush must succeed");

    let seen = harness::span_names_until(&mut received, "sentinel_turn").await;
    assert!(
        !seen.iter().any(|name| name == "unsampled_turn"),
        "ratio 0.0 must sample nothing, but the collector saw {seen:?}"
    );
}

/// Graceful degradation, measured: with nobody listening, the product's
/// emitting threads were never blocked, the flush comes back bounded with a
/// verdict instead of hanging, and every span that reached the exporter is
/// counted as dropped rather than silently gone.
#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_collector_costs_spans_and_never_blocks() {
    let endpoint = harness::dead_endpoint().await;
    let telemetry = std::sync::Arc::new(
        pipeline(
            &OtlpConfig {
                endpoint,
                sample_ratio: 1.0,
            },
            &ServiceIdentity::new("horde-observe-smoke"),
        )
        .expect("the pipeline builds without connecting"),
    );

    let dispatch = silent_subscriber(&telemetry);
    tracing::dispatcher::with_default(&dispatch, || {
        for turn in 0..3 {
            let _entered =
                tracing::info_span!(target: "horde_smoke", "doomed_turn", turn).entered();
        }
    });

    let flusher = std::sync::Arc::clone(&telemetry);
    let verdict = harness::within(
        "a flush at a dead collector returns",
        tokio::task::spawn_blocking(move || flusher.flush()),
    )
    .await
    .expect("the flushing task must not panic");
    assert!(
        verdict.is_err(),
        "a flush that delivered nothing must not claim success"
    );
    assert_eq!(
        telemetry.dropped_spans(),
        3,
        "every span handed to the exporter is accounted for"
    );
}
