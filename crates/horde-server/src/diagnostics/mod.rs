//! Diagnostic logs, server half (ADR-0015).
//!
//! `ControlPlane/ReadLogs` serves three targets: the server's own files,
//! a worker's (through the worker log stream pair), and an agent's (its
//! placement's worker, filtered to it and merged with its harness stderr).
//! Nothing here is journaled and nothing here decides anything: a log read
//! is ephemeral intent, living exactly as long as its streams, and logs are
//! read only by people and agents.
//!
//! # Who may read what (ADR-0009)
//!
//! A client may read every target: Horde is single-user and a client is
//! the operator. A worker caller is the agent conduit: it must name, in
//! `on_behalf_of`, an agent placed on it; it may read the server, any
//! worker, and agents of that agent's own session; and every record naming
//! another session is dropped before it leaves the machine that wrote it.
//! A worker caller's reads are clamped and rate-limited per agent.
//!
//! This module is the plane's state and its pure halves (the registry, the
//! relay, the refusals); the decisions that read the session projection —
//! authorizing and opening a read — live in `crate::service::diagnostics`,
//! beside the handlers, so this module never reaches back into the service
//! layer that holds it.

pub mod registry;

use std::sync::Arc;
use std::time::Duration;

use horde_observe::limits::{RateBuckets, retry_after_secs};
use horde_observe::reader::{LocalLogs, egress_lines, scope_admits};
use horde_observe::record::Record;
use horde_observe::settings::AgentReadLimits;
use horde_proto::v1::LogEndReason;
use horde_wire::logs::{Frame, end};
use tokio::sync::mpsc;
use tonic::Status;

use registry::LogRegistry;

/// The prefix of the server's own log files.
pub const BINARY: &str = "horde-server";

/// How long a dispatched read waits for its worker's tunnel.
pub const TUNNEL_DEADLINE: Duration = Duration::from_secs(10);

/// The frames buffered between a read's source and its client.
pub const FRAME_DEPTH: usize = 16;

/// The server's log plane: its own files, the workers' lanes, and the
/// agent read limits.
#[derive(Debug, Clone)]
pub struct LogPlane {
    /// The server's own log directory, when it keeps one (the deployed
    /// binary does; in-process harnesses may not).
    pub local: Option<LocalLogs>,
    pub registry: LogRegistry,
    pub buckets: Arc<RateBuckets>,
}

impl Default for LogPlane {
    fn default() -> Self {
        Self {
            local: None,
            registry: LogRegistry::new(),
            buckets: Arc::new(RateBuckets::new(AgentReadLimits::default())),
        }
    }
}

/// Where one read resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    Server,
    Worker {
        worker_id: String,
        agent: Option<String>,
    },
}

/// A read the caller may make, and what it is held to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorized {
    pub target: Resolved,
    /// Records naming another session are dropped (agent callers).
    pub session_scope: Option<String>,
    /// Zero is unbounded (clients only).
    pub max_bytes: u32,
}

/// The refusal past the rate limit: a code the endpoint turns into a tool
/// error the model can read, with the wait in both the text and the
/// `retry-after` metadata.
#[must_use]
pub fn rate_limited(agent: &str, wait: Duration) -> Status {
    let secs = retry_after_secs(wait);
    let mut status = Status::resource_exhausted(format!(
        "agent {agent} is reading logs too often; retry after {secs}s"
    ));
    if let Ok(value) = secs.to_string().parse() {
        status.metadata_mut().insert("retry-after", value);
    }
    status
}

/// Relays a worker's frames to the client: the session scope and the byte
/// cap re-applied, every line redacted again for the server's own secrets,
/// and a worker that vanishes mid-read reported as such.
pub async fn relay(
    mut from: mpsc::Receiver<Frame>,
    to: mpsc::Sender<Frame>,
    scope: Option<String>,
    cap: Option<usize>,
) {
    let mut used = 0;
    loop {
        let frame = tokio::select! {
            frame = from.recv() => frame,
            () = to.closed() => return,
        };
        let frame = match frame {
            None => end(
                LogEndReason::WorkerDisconnected,
                "the worker disconnected mid-read",
            ),
            Some(Frame::Records(mut records)) => {
                if let Some(scope) = scope.as_deref() {
                    records.lines.retain(|line| {
                        Record::parse(line).is_some_and(|record| scope_admits(&record, Some(scope)))
                    });
                }
                let again = egress_lines(records.lines.iter().map(String::as_str));
                records.redacted += again.redacted;
                records.lines = again.lines;
                used += records.lines.iter().map(String::len).sum::<usize>();
                if cap.is_some_and(|cap| used > cap) {
                    end(LogEndReason::ByteCap, "the byte cap was reached")
                } else {
                    Frame::Records(records)
                }
            }
            Some(other) => other,
        };
        let ended = matches!(frame, Frame::End(_));
        if to.send(frame).await.is_err() || ended {
            return;
        }
    }
}

#[cfg(test)]
pub(crate) mod testfix;
