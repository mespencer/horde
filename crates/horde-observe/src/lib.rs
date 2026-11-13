//! Local diagnostic logs (ADR-0015) and OTLP export for the tracing Horde's
//! binaries already emit (DESIGN §12).
//!
//! # Local logs
//!
//! Each daemon writes bounded, rotating JSON lines under `<state>/logs`
//! ([`layer::FileLogging`], [`files`]); a worker also keeps each harness's
//! stderr in `logs/agents/` ([`agents`]). [`reader`] reads them back for
//! `horde logs`, `ControlPlane/ReadLogs` and the `logs` MCP verb, and every
//! path that serves a record off the machine redacts it through
//! [`reader::egress`] ([`redact`]). Logs are never session facts: nothing
//! decides from them, and losing every file loses no state.
//!
//! # OTLP export
//!
//! This crate is the export pipeline, not the instrumentation: the spans and
//! events in the tree stay where they are, and a binary that opts in gets them
//! shipped to an OTLP/gRPC collector, stamped with who it is. A binary that
//! does not opt in gets exactly what it always had — no exporter is built, no
//! background thread starts, no subscriber changes shape.
//!
//! # Turning it on
//!
//! Two environment variables, read once at startup through [`from_env`]:
//!
//! - **`HORDE_OTLP_ENDPOINT`** — the collector, e.g. `http://127.0.0.1:4317`.
//!   Absent or empty means off.
//! - **`HORDE_OTLP_SAMPLE`** — the fraction of traces exported, `0.0..=1.0`,
//!   default `1.0`. Head sampling, parent-respecting: a sampled trace keeps
//!   all of its spans, an unsampled one ships none of them.
//!
//! Misconfiguration is refused loudly at startup rather than degraded around:
//! a ratio that does not parse, or a ratio without an endpoint, is exactly the
//! shape a mistyped variable takes, and silently exporting nothing would be a
//! write acked and discarded.
//!
//! # What never happens
//!
//! The product never blocks on the collector and never dies of it. Emitting a
//! span enqueues it on a bounded queue (the batch processor's, 2048 spans);
//! export happens on a dedicated background thread; a full queue drops the
//! newest span, and a failed export drops its batch, counts it
//! ([`Telemetry::dropped_spans`]) and says so on the console log. An
//! unreachable collector costs spans, never requests.
//!
//! # Who the spans say they are
//!
//! Every exported span carries the OTLP resource built from
//! [`ServiceIdentity`]: `service.name` is the binary (`horde-server`,
//! `horde`, `horde-worker`), and `horde.principal` is the enrolled mTLS
//! principal (`worker:name`, `client:name`) when the binary has one.

pub mod agents;
mod config;
mod export;
pub mod files;
pub mod layer;
mod pipeline;
pub mod reader;
pub mod record;
pub mod redact;
pub mod settings;
mod subscriber;
pub mod tap;

pub use config::{ConfigError, ENDPOINT_VAR, OtlpConfig, SAMPLE_VAR, from_env};
pub use layer::FileLogging;
pub use pipeline::{
    InitError, PRINCIPAL_ATTRIBUTE, PipelineError, ServiceIdentity, Telemetry, init_from_env,
    pipeline,
};
pub use subscriber::{silent_subscriber, stderr_subscriber};
