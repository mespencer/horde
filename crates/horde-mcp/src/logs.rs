//! The `logs` verb (ADR-0015): an agent reading the system's diagnostic
//! logs when it suspects the system is the problem.
//!
//! Targets: `self` (the default — the agent's own records on its worker,
//! merged with its harness's stderr), `server`, `worker:<name>`, and
//! `agent:<id>`. `self` and the agent's own worker are read here, from
//! local files, so an agent can still read its own stderr while the server
//! is unreachable; every other target goes through `ReadLogs` on the
//! worker's certificate. MCP is request/response, so there is no follow: an
//! agent pages forward with `next_cursor`.
//!
//! Limits, per agent: a byte cap per call, a token-bucket rate, and one call
//! in flight. A refusal is a tool error the model can read, never a
//! protocol error. Records from another session never reach the caller, on
//! either path.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use horde_observe::limits::{RateBuckets, retry_after_secs};
use horde_observe::reader::{Filter, LocalLogs, Target, Window, egress};
use horde_observe::record::Cursor;
use horde_observe::settings::AgentReadLimits;
use horde_proto::v1::{
    AgentId, LogEndReason, LogFilter, ReadLogsRequest, ServerTarget, read_logs_request,
    read_logs_response,
};
use serde::{Deserialize, Serialize};

/// The `logs` verb's parameters.
#[derive(Debug, Default, Deserialize, schemars::JsonSchema)]
pub struct LogsParams {
    /// `self` (default), `server`, `worker:<name>`, or `agent:<id>`.
    #[serde(default)]
    pub target: Option<String>,
    /// A duration back from now (`10m`) or an RFC 3339 timestamp.
    #[serde(default)]
    pub since: Option<String>,
    /// Minimum level: trace, debug, info, warn, error.
    #[serde(default)]
    pub level: Option<String>,
    /// A regular expression over each record's message and fields.
    #[serde(default)]
    pub grep: Option<String>,
    /// Bytes of records to return; clamped to the per-call limit.
    #[serde(default)]
    pub max_bytes: Option<u32>,
    /// A previous call's `next_cursor`: page forward from it.
    #[serde(default)]
    pub cursor: Option<String>,
}

/// What a `logs` call returns.
#[derive(Debug, Default, Serialize, schemars::JsonSchema, PartialEq, Eq)]
pub struct LogsView {
    /// Redacted JSON-lines records, oldest first.
    pub records: Vec<String>,
    /// Matching records were left out to respect the byte cap: older ones
    /// on a first call, newer ones when paging (call again with the cursor).
    pub truncated: bool,
    /// Pass back as `cursor` to read what comes after these records.
    pub next_cursor: Option<String>,
    /// How many of these records redaction touched.
    pub redacted: u32,
}

/// Where one call reads.
#[derive(Debug, Clone)]
pub enum LogsTarget<'a> {
    /// Read here, from the worker's own files.
    Local(&'a LocalLogs, Target),
    /// Read through the server.
    Remote(read_logs_request::Target),
}

/// Parses a `target` for an agent on `own_worker`; `local` is the worker's
/// files, when this endpoint has them to read.
///
/// # Errors
///
/// A target that is none of the four shapes.
pub fn parse_target<'a>(
    target: Option<&str>,
    agent: &str,
    own_worker: &str,
    local: Option<&'a LocalLogs>,
) -> Result<LogsTarget<'a>, String> {
    let target = target.unwrap_or_default().trim();
    if target.is_empty() || target == "self" {
        return Ok(match local {
            Some(local) => LogsTarget::Local(local, Target::Agent(agent.to_owned())),
            None => LogsTarget::Remote(read_logs_request::Target::Agent(AgentId::new(agent))),
        });
    }
    if target == "server" {
        return Ok(LogsTarget::Remote(read_logs_request::Target::Server(
            ServerTarget {},
        )));
    }
    if let Some(worker) = target.strip_prefix("worker:").filter(|w| !w.is_empty()) {
        return Ok(match local.filter(|_| worker == own_worker) {
            Some(local) => LogsTarget::Local(local, Target::Process),
            None => LogsTarget::Remote(read_logs_request::Target::Worker(worker.to_owned())),
        });
    }
    if let Some(other) = target.strip_prefix("agent:").filter(|a| !a.is_empty()) {
        return Ok(match local.filter(|_| other == agent) {
            Some(local) => LogsTarget::Local(local, Target::Agent(agent.to_owned())),
            None => LogsTarget::Remote(read_logs_request::Target::Agent(AgentId::new(other))),
        });
    }
    Err(format!(
        "{target:?} is not a log target; use self, server, worker:<name>, or agent:<id>"
    ))
}

/// What one endpoint needs to serve `logs`: the worker's files, its name,
/// and the per-agent limits.
#[derive(Debug)]
pub struct LogAccess {
    /// The worker's own files, and the worker's name: the reads they serve
    /// without asking the server.
    local: Option<(LocalLogs, String)>,
    buckets: RateBuckets,
    in_flight: Mutex<BTreeSet<String>>,
}

/// Holds an agent's one in-flight slot until dropped.
struct InFlight<'a> {
    access: &'a LogAccess,
    agent: String,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.access.locked().remove(&self.agent);
    }
}

impl LogAccess {
    /// Log access for agents on `worker_id`, reading its `local` files for
    /// the targets they hold.
    #[must_use]
    pub fn serving(local: LocalLogs, worker_id: &str, limits: AgentReadLimits) -> Self {
        Self::with(Some((local, worker_id.to_owned())), limits)
    }

    /// Log access with no files of its own: every target is read through
    /// the server.
    #[must_use]
    pub fn remote_only(limits: AgentReadLimits) -> Self {
        Self::with(None, limits)
    }

    fn with(local: Option<(LocalLogs, String)>, limits: AgentReadLimits) -> Self {
        Self {
            local,
            buckets: RateBuckets::new(limits),
            in_flight: Mutex::new(BTreeSet::new()),
        }
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, BTreeSet<String>> {
        match self.in_flight.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Admits one call for `agent`: one in flight, within the rate.
    fn admit(&self, agent: &str, now: Instant) -> Result<InFlight<'_>, String> {
        if !self.locked().insert(agent.to_owned()) {
            return Err("another logs call of yours is still running; wait for it".to_owned());
        }
        let slot = InFlight {
            access: self,
            agent: agent.to_owned(),
        };
        if let Err(wait) = self.buckets.take(agent, now) {
            return Err(format!(
                "logs is rate-limited to {} calls a minute; retry after {}s",
                self.buckets.limits().calls_per_minute,
                retry_after_secs(wait)
            ));
        }
        Ok(slot)
    }

    /// Serves one call for `agent` in `session`, through `client` for
    /// remote targets. `Err` is the tool error the model reads.
    ///
    /// # Errors
    ///
    /// A refusal in words: the limits, a bad parameter, or the server's
    /// answer.
    pub async fn call(
        &self,
        client: &mut horde_cli::client::Client,
        agent: &str,
        session: &str,
        params: &LogsParams,
    ) -> Result<LogsView, String> {
        let _slot = self.admit(agent, Instant::now())?;
        let (local, own_worker) = match &self.local {
            Some((local, worker_id)) => (Some(local), worker_id.as_str()),
            None => (None, ""),
        };
        let target = parse_target(params.target.as_deref(), agent, own_worker, local)?;
        let max_bytes = self
            .buckets
            .limits()
            .clamp(params.max_bytes.unwrap_or_default());
        let cursor = params
            .cursor
            .as_deref()
            .filter(|cursor| !cursor.is_empty())
            .map(|text| Cursor::decode(text).ok_or_else(|| format!("{text:?} is not a cursor")))
            .transpose()?;
        let filter = wire_filter(params);
        match target {
            LogsTarget::Local(local, target) => {
                read_local(local, &target, &filter, session, cursor, max_bytes)
            }
            LogsTarget::Remote(target) => {
                let request = remote_request(target, filter, agent, cursor.as_ref(), max_bytes);
                read_remote(client, request, cursor).await
            }
        }
    }
}

fn wire_filter(params: &LogsParams) -> LogFilter {
    LogFilter {
        since: params.since.clone().unwrap_or_default(),
        level: params.level.clone().unwrap_or_default(),
        grep: params.grep.clone().unwrap_or_default(),
        ..LogFilter::default()
    }
}

/// A local read, scoped to the caller's session and redacted at egress.
fn read_local(
    local: &LocalLogs,
    target: &Target,
    wire: &LogFilter,
    session: &str,
    cursor: Option<Cursor>,
    max_bytes: u32,
) -> Result<LogsView, String> {
    let filter: Filter =
        horde_wire::logs::filter_from_wire(wire, Some(session.to_owned()), SystemTime::now())?;
    let window = Window {
        after: cursor.clone(),
        tail: None,
        max_bytes: usize::try_from(max_bytes).ok(),
    };
    let selection = local.read(target, &filter, &window);
    let out = egress(&selection.records);
    Ok(LogsView {
        records: out.lines,
        truncated: selection.truncated,
        next_cursor: selection.last().or(cursor.as_ref()).map(Cursor::encode),
        redacted: out.redacted,
    })
}

/// The `ReadLogs` request a remote read sends: on the agent's behalf, never
/// following (MCP is request/response).
fn remote_request(
    target: read_logs_request::Target,
    filter: LogFilter,
    agent: &str,
    cursor: Option<&Cursor>,
    max_bytes: u32,
) -> ReadLogsRequest {
    ReadLogsRequest {
        target: Some(target),
        filter: Some(filter),
        follow: false,
        after: cursor.map(horde_wire::logs::cursor_to_wire),
        on_behalf_of: Some(AgentId::new(agent)),
        max_bytes,
    }
}

/// A remote read through `ReadLogs`, read until the server closes it.
async fn read_remote(
    client: &mut horde_cli::client::Client,
    request: ReadLogsRequest,
    cursor: Option<Cursor>,
) -> Result<LogsView, String> {
    let mut stream = client
        .read_logs(request)
        .await
        .map_err(|error| error.to_string())?;
    let mut view = LogsView {
        next_cursor: cursor.as_ref().map(Cursor::encode),
        ..LogsView::default()
    };
    while let Some(response) = stream
        .message()
        .await
        .map_err(|status| status.message().to_owned())?
    {
        absorb(&mut view, response.body)?;
    }
    Ok(view)
}

/// Folds one response into the view. A worker that vanished mid-read is an
/// error: what came before it is not the whole answer.
fn absorb(view: &mut LogsView, body: Option<read_logs_response::Body>) -> Result<(), String> {
    match body {
        Some(read_logs_response::Body::Records(records)) => {
            view.truncated |= records.truncated;
            view.redacted += records.redacted;
            view.records.extend(records.lines);
            if let Some(next) = records
                .cursor
                .as_ref()
                .and_then(|cursor| horde_wire::logs::cursor_from_wire(cursor).ok())
            {
                view.next_cursor = Some(next.encode());
            }
        }
        Some(read_logs_response::Body::End(end)) => {
            view.truncated |= end.reason == LogEndReason::ByteCap as i32;
            if end.reason == LogEndReason::WorkerDisconnected as i32 {
                return Err(format!("the read ended early: {}", end.detail));
            }
        }
        Some(read_logs_response::Body::Gap(_)) | None => {}
    }
    Ok(())
}

/// The shared handle an endpoint serves `logs` through.
pub type SharedLogAccess = Arc<LogAccess>;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use horde_observe::reader::{LocalLogs, Target};
    use horde_observe::settings::AgentReadLimits;
    use horde_proto::v1::read_logs_request;

    use horde_proto::v1::{LogEndReason, LogFilter, ReadLogsRequest};

    use super::{LogAccess, LogsParams, LogsTarget, LogsView, parse_target};

    #[test]
    fn targets_parse_to_local_or_remote() {
        let files = LocalLogs::new("/nowhere", "horde-worker", None);
        let local = |target: Option<&str>| parse_target(target, "a1", "w1", Some(&files));
        let reads_here = |parsed: Result<LogsTarget<'_>, String>, expected: &Target| {
            matches!(parsed, Ok(LogsTarget::Local(logs, target))
                if std::ptr::eq(logs, &files) && &target == expected)
        };
        let own = Target::Agent("a1".to_owned());
        assert!(reads_here(local(None), &own));
        assert!(reads_here(local(Some("self")), &own));
        assert!(reads_here(local(Some(" self ")), &own));
        assert!(reads_here(local(Some("")), &own));
        assert!(reads_here(local(Some("agent:a1")), &own));
        assert!(reads_here(local(Some("worker:w1")), &Target::Process));
        assert!(matches!(
            local(Some("worker:w2")),
            Ok(LogsTarget::Remote(read_logs_request::Target::Worker(name))) if name == "w2"
        ));
        assert!(matches!(
            local(Some("server")),
            Ok(LogsTarget::Remote(read_logs_request::Target::Server(_)))
        ));
        assert!(matches!(
            local(Some("agent:a2")),
            Ok(LogsTarget::Remote(read_logs_request::Target::Agent(id))) if id.value == "a2"
        ));
        assert!(matches!(
            parse_target(None, "a1", "w1", None),
            Ok(LogsTarget::Remote(read_logs_request::Target::Agent(id))) if id.value == "a1"
        ));
        assert!(matches!(
            parse_target(Some("worker:w1"), "a1", "w1", None),
            Ok(LogsTarget::Remote(_))
        ));
        assert!(matches!(
            parse_target(Some("agent:a1"), "a1", "w1", None),
            Ok(LogsTarget::Remote(read_logs_request::Target::Agent(id))) if id.value == "a1"
        ));
        for bad in ["worker:", "agent:", "disk", "worker"] {
            assert!(local(Some(bad)).is_err(), "{bad}");
        }
    }

    /// A remote read asks on the agent's behalf, for its filter, cursor and
    /// cap, and never follows.
    #[test]
    fn a_remote_read_asks_on_the_agents_behalf_without_following() {
        let cursor = horde_observe::record::Cursor::decode("2026-01-01T00:00:00.000000Z~horde~b~3")
            .expect("a cursor");
        let filter = LogFilter {
            grep: "boom".to_owned(),
            ..LogFilter::default()
        };
        let request = super::remote_request(
            read_logs_request::Target::Server(horde_proto::v1::ServerTarget {}),
            filter.clone(),
            "a1",
            Some(&cursor),
            123,
        );
        assert_eq!(
            request,
            ReadLogsRequest {
                target: Some(read_logs_request::Target::Server(
                    horde_proto::v1::ServerTarget {}
                )),
                filter: Some(filter),
                follow: false,
                after: Some(horde_wire::logs::cursor_to_wire(&cursor)),
                on_behalf_of: Some(horde_proto::v1::AgentId::new("a1")),
                max_bytes: 123,
            }
        );
    }

    /// What each response does to the view: records accumulate, a cap or a
    /// truncated batch marks it, a gap changes nothing, and a vanished
    /// worker fails the read in words.
    #[test]
    fn responses_fold_into_the_view() {
        use horde_proto::v1::{LogEnd, LogGap, LogRecords, read_logs_response::Body};
        let mut view = LogsView::default();
        assert_eq!(
            super::absorb(
                &mut view,
                Some(Body::Records(LogRecords {
                    lines: vec!["r".to_owned()],
                    redacted: 2,
                    ..LogRecords::default()
                }))
            ),
            Ok(())
        );
        assert_eq!(
            super::absorb(&mut view, Some(Body::Gap(LogGap::default()))),
            Ok(())
        );
        assert_eq!(super::absorb(&mut view, None), Ok(()));
        assert_eq!(view.records, ["r"]);
        assert_eq!(view.redacted, 2);
        assert!(!view.truncated);
        let done = Some(Body::End(LogEnd {
            reason: LogEndReason::Done as i32,
            detail: String::new(),
        }));
        assert_eq!(super::absorb(&mut view, done), Ok(()));
        assert!(!view.truncated, "a finished read left nothing out");
        let capped = Some(Body::End(LogEnd {
            reason: LogEndReason::ByteCap as i32,
            detail: String::new(),
        }));
        assert_eq!(super::absorb(&mut view, capped), Ok(()));
        assert!(view.truncated, "the cap left records out");
        let vanished = Some(Body::End(LogEnd {
            reason: LogEndReason::WorkerDisconnected as i32,
            detail: "gone".to_owned(),
        }));
        assert_eq!(
            super::absorb(&mut view, vanished),
            Err("the read ended early: gone".to_owned())
        );
    }

    fn client() -> horde_cli::client::Client {
        horde_cli::client::Client::with_channel(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        )
    }

    /// One agent's stderr with a secret, another session's record, and
    /// enough records to page.
    fn files(dir: &std::path::Path) -> LocalLogs {
        let serving = horde_observe::agents::AgentLogs::new(
            dir.join("agents"),
            horde_observe::settings::AgentLogSettings::default(),
            "boot",
            None,
        );
        let mine = serving.writer("a1", "s1");
        for n in 0..5 {
            mine.line(&format!("line {n}"));
        }
        mine.line("token horde://join/h:1/a.b");
        serving.writer("a1", "s-other").line("from another session");
        LocalLogs::new(dir, "horde-worker", None)
    }

    /// Access over [`files`] with limits no test here reaches.
    fn generous(dir: &std::path::Path) -> LogAccess {
        LogAccess::serving(
            files(dir),
            "w1",
            AgentReadLimits {
                calls_per_minute: 600,
                burst: 100,
                ..AgentReadLimits::default()
            },
        )
    }

    #[tokio::test]
    async fn a_local_read_pages_redacts_and_stays_in_its_session() {
        let dir = tempfile::tempdir().expect("tempdir");
        let access = generous(dir.path());
        let mut client = client();
        let all = access
            .call(&mut client, "a1", "s1", &LogsParams::default())
            .await
            .expect("read");
        assert_eq!(all.records.len(), 6, "{:?}", all.records);
        assert!(!all.records.iter().any(|r| r.contains("another session")));
        assert_eq!(all.redacted, 1);
        assert!(all.records[5].contains("[REDACTED:join-token]"));
        assert!(!all.truncated);

        let one = all.records[0].len() as u32;
        let first = access
            .call(
                &mut client,
                "a1",
                "s1",
                &LogsParams {
                    max_bytes: Some(one * 2),
                    ..LogsParams::default()
                },
            )
            .await
            .expect("tail");
        assert_eq!(
            first.records.len(),
            1,
            "the newest record only, {:?}",
            first.records
        );
        assert!(first.truncated);

        let from_start = access
            .call(
                &mut client,
                "a1",
                "s1",
                &LogsParams {
                    cursor: Some("1970-01-01T00:00:00.000000Z~horde~~0".to_owned()),
                    max_bytes: Some(one * 2 + 1),
                    ..LogsParams::default()
                },
            )
            .await
            .expect("page");
        assert_eq!(from_start.records.len(), 2);
        assert!(from_start.truncated);
        let next = access
            .call(
                &mut client,
                "a1",
                "s1",
                &LogsParams {
                    cursor: from_start.next_cursor.clone(),
                    ..LogsParams::default()
                },
            )
            .await
            .expect("next page");
        assert_eq!(next.records.len(), 4);
        assert!(next.records[0].contains("line 2"));
        let done = access
            .call(
                &mut client,
                "a1",
                "s1",
                &LogsParams {
                    cursor: next.next_cursor.clone(),
                    ..LogsParams::default()
                },
            )
            .await
            .expect("nothing more");
        assert!(done.records.is_empty());
        assert_eq!(done.next_cursor, next.next_cursor, "the cursor stays put");

        let bad = access
            .call(
                &mut client,
                "a1",
                "s1",
                &LogsParams {
                    cursor: Some("garbage".to_owned()),
                    ..LogsParams::default()
                },
            )
            .await;
        assert!(bad.is_err());
    }

    /// The cap is the caller's, exactly: two records' bytes page two
    /// records, one byte less pages one.
    #[tokio::test]
    async fn a_local_page_holds_exactly_its_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let access = generous(dir.path());
        let mut client = client();
        let all = access
            .call(&mut client, "a1", "s1", &LogsParams::default())
            .await
            .expect("read");
        let two = u32::try_from(all.records[0].len() + all.records[1].len()).expect("small");
        let page = |max_bytes| LogsParams {
            cursor: Some("1970-01-01T00:00:00.000000Z~horde~~0".to_owned()),
            max_bytes: Some(max_bytes),
            ..LogsParams::default()
        };
        for (cap, expected) in [(two, 2), (two - 1, 1)] {
            let read = access
                .call(&mut client, "a1", "s1", &page(cap))
                .await
                .expect("page");
            assert_eq!(read.records.len(), expected, "{cap} bytes");
        }
    }

    /// Each filter parameter reaches the read.
    #[tokio::test]
    async fn since_level_and_grep_each_narrow_the_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let access = generous(dir.path());
        let client = client();
        let count = |params: LogsParams| {
            let (access, mut client) = (&access, client.clone());
            async move {
                access
                    .call(&mut client, "a1", "s1", &params)
                    .await
                    .expect("read")
                    .records
                    .len()
            }
        };
        assert_eq!(
            count(LogsParams {
                grep: Some("line 3".to_owned()),
                ..LogsParams::default()
            })
            .await,
            1
        );
        assert_eq!(
            count(LogsParams {
                level: Some("warn".to_owned()),
                ..LogsParams::default()
            })
            .await,
            0,
            "stderr reads as info"
        );
        assert_eq!(
            count(LogsParams {
                since: Some("2999-01-01T00:00:00Z".to_owned()),
                ..LogsParams::default()
            })
            .await,
            0
        );
        assert_eq!(count(LogsParams::default()).await, 6);
    }

    #[tokio::test]
    async fn the_rate_and_concurrency_limits_refuse_in_words() {
        let dir = tempfile::tempdir().expect("tempdir");
        let access = LogAccess::serving(files(dir.path()), "w1", AgentReadLimits::default());
        let mut client = client();
        for _ in 0..3 {
            assert!(
                access
                    .call(&mut client, "a1", "s1", &LogsParams::default())
                    .await
                    .is_ok()
            );
        }
        let limited = access
            .call(&mut client, "a1", "s1", &LogsParams::default())
            .await;
        assert!(
            limited
                .as_ref()
                .is_err_and(|refusal| refusal.contains("retry after 10s")),
            "{limited:?}"
        );
        assert!(
            access
                .call(&mut client, "a2", "s1", &LogsParams::default())
                .await
                .is_ok(),
            "another agent has its own bucket"
        );

        let held = access.admit("a3", Instant::now()).expect("first slot");
        let busy = access.admit("a3", Instant::now() + Duration::from_secs(60));
        assert!(busy.is_err_and(|refusal| refusal.contains("still running")));
        drop(held);
        assert!(
            access
                .admit("a3", Instant::now() + Duration::from_secs(60))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn without_local_files_a_local_target_is_refused() {
        let access = LogAccess::remote_only(AgentReadLimits::default());
        let refused = access
            .call(
                &mut client(),
                "a1",
                "s1",
                &LogsParams {
                    target: Some("nowhere".to_owned()),
                    ..LogsParams::default()
                },
            )
            .await;
        assert!(refused.is_err());
    }
}
