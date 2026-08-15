//! The composed subscribers the binaries install.
//!
//! Composition lives here, as ordinary returned values, because a binary's
//! `main` is reachable only by running the binary (horde-server's bin says
//! why): a subscriber built here can be held to its behavior by a test using
//! [`tracing::dispatcher::with_default`], and the binary's own line shrinks
//! to "install this".

use tracing::Dispatch;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

use crate::pipeline::Telemetry;

/// The subscriber for a binary whose console is stderr (`horde-server`):
/// human-readable lines under `filter`, plus export when `telemetry` is
/// configured.
///
/// With `None` this is the fmt-to-stderr subscriber the server has always
/// installed, in layered clothing: same format, same filter, stdout left to
/// the operator interface.
#[must_use]
pub fn stderr_subscriber(filter: EnvFilter, telemetry: Option<&Telemetry>) -> Dispatch {
    let fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(filter);
    match telemetry {
        None => Dispatch::new(tracing_subscriber::registry().with(fmt)),
        Some(telemetry) => Dispatch::new(
            tracing_subscriber::registry()
                .with(fmt)
                .with(telemetry.layer()),
        ),
    }
}

/// The subscriber for a binary that owns its terminal (`horde`, whose stdout
/// is the TUI and whose stderr is the refusal channel): export only, nothing
/// written anywhere locally.
///
/// There is deliberately no `Option` here — with telemetry off, such a binary
/// installs no subscriber at all, exactly as it always has.
#[must_use]
pub fn silent_subscriber(telemetry: &Telemetry) -> Dispatch {
    Dispatch::new(tracing_subscriber::registry().with(telemetry.layer()))
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::EnvFilter;

    use super::{silent_subscriber, stderr_subscriber};
    use crate::config::OtlpConfig;
    use crate::pipeline::{ServiceIdentity, pipeline};

    /// A pipeline at a closed port: builds fine (the channel is lazy), and
    /// these tests never flush, so nothing is ever sent anywhere.
    fn idle_telemetry() -> crate::pipeline::Telemetry {
        let config = OtlpConfig {
            endpoint: "http://127.0.0.1:1".to_owned(),
            sample_ratio: 1.0,
        };
        pipeline(&config, &ServiceIdentity::new("horde-observe-test"))
            .expect("a lazy pipeline must build without a collector")
    }

    /// Whether a span born under `dispatch` is live. A span is disabled only
    /// when no layer of the subscriber wants it, so this distinguishes the
    /// composed subscriber both from the no-op dispatch (which disables
    /// everything) and from one missing a layer — without capturing output.
    fn span_lives(dispatch: &tracing::Dispatch, span: impl Fn() -> tracing::Span) -> bool {
        tracing::dispatcher::with_default(dispatch, || !span().is_disabled())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_stderr_subscriber_honors_its_filter() {
        let dispatch = stderr_subscriber(EnvFilter::new("info"), None);
        assert!(
            span_lives(&dispatch, || tracing::info_span!("inside")),
            "info is inside the filter and must be enabled"
        );
        assert!(
            !span_lives(&dispatch, || tracing::trace_span!("outside")),
            "trace is outside the filter and must not be"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn telemetry_widens_the_stderr_subscriber_beyond_its_console_filter() {
        let telemetry = idle_telemetry();
        let dispatch = stderr_subscriber(EnvFilter::new("info"), Some(&telemetry));
        // The console still shows info+, but export wants everything the
        // target filter admits — so a trace-level product span is now live
        // (for the export layer alone)...
        assert!(span_lives(&dispatch, || tracing::trace_span!(
            target: "horde_worker",
            "turn"
        )));
        // ...while the export machinery's own targets stay dark below the
        // console filter: neither layer wants them.
        assert!(!span_lives(&dispatch, || tracing::trace_span!(
            target: "tonic::transport",
            "noise"
        )));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_silent_subscriber_exports_product_targets_and_nothing_of_its_own() {
        let telemetry = idle_telemetry();
        let dispatch = silent_subscriber(&telemetry);
        assert!(span_lives(&dispatch, || tracing::trace_span!(
            target: "horde_cli",
            "attach"
        )));
        for own in [
            || tracing::trace_span!(target: "horde_observe", "own"),
            || tracing::trace_span!(target: "opentelemetry::export", "own"),
        ] {
            assert!(
                !span_lives(&dispatch, own),
                "the pipeline must not export its own machinery"
            );
        }
    }
}
