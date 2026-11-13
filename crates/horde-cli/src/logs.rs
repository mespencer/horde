//! `horde logs`: read any process's diagnostic logs (ADR-0015).
//!
//! ```text
//! horde logs server               [-f] [--since D|T] [--level L] [--grep RE] [-n N] [--json]
//! horde logs worker <NAME>        (same flags)
//! horde logs agent  <AGENT>       (same flags) [--source all|horde|stderr]
//! horde logs --dir <PATH>         (same flags; local files, no RPC)
//! ```
//!
//! Remote reads go through `ControlPlane/ReadLogs`, redacted on the machine
//! that wrote them. `--dir` reads files the operator already owns, as they
//! are, with the same reader and filters — the escape hatch for a machine
//! that cannot reach the server, and how a CLI reads its own `--log-file`.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use horde_observe::reader::{
    DEFAULT_TAIL, Filter, Sources, Window, compile_grep, read_dir, resolve_since, select,
};
use horde_observe::record::{Cursor, Record};
use horde_proto::v1::{
    AgentId, LogEndReason, LogFilter, ReadLogsRequest, ServerTarget, read_logs_request,
    read_logs_response,
};
use serde_json::Value;

use crate::{Client, ClientError, Format};

/// `horde logs`.
#[derive(clap::Args, Debug, Clone, Default)]
pub struct LogsArgs {
    /// `server`, `worker`, or `agent`.
    pub target: Option<String>,
    /// The worker's name, or the agent's id.
    pub name: Option<String>,
    /// Read a local log directory instead of asking the server.
    #[arg(long)]
    pub dir: Option<PathBuf>,
    /// Keep reading as new records are written.
    #[arg(short = 'f', long)]
    pub follow: bool,
    /// Records since a duration ago (`10m`) or an RFC 3339 timestamp.
    #[arg(long)]
    pub since: Option<String>,
    /// Minimum level: trace, debug, info, warn, error.
    #[arg(long)]
    pub level: Option<String>,
    /// A regular expression over each record's message and fields.
    #[arg(long)]
    pub grep: Option<String>,
    /// How many of the newest matching records to show first.
    #[arg(short = 'n', default_value_t = DEFAULT_TAIL)]
    pub n: usize,
    /// For an agent: `all` (default), `horde`, or `stderr`.
    #[arg(long)]
    pub source: Option<String>,
}

/// Where this machine's CLI writes its own opt-in log: `~/.horde/client/logs`.
#[must_use]
pub fn client_log_dir(home: Option<PathBuf>) -> PathBuf {
    home.map_or_else(
        || PathBuf::from(".horde-client-logs"),
        |home| home.join(".horde").join("client").join("logs"),
    )
}

/// Whether this invocation writes its own log: `--log-file`, or
/// `HORDE_LOG_FILE` set to anything but empty or `0`.
#[must_use]
pub fn wants_log_file(flag: bool, ambient: Option<&str>) -> bool {
    flag || ambient.is_some_and(|value| !value.is_empty() && value != "0")
}

/// The CLI's own file logging, when this invocation asked for it.
///
/// # Errors
///
/// An unparseable `HORDE_LOG`, or a writer thread that cannot start.
pub fn client_file_logging(
    wanted: bool,
    home: Option<PathBuf>,
    principal: Option<String>,
    lookup: impl Fn(&str) -> Option<String>,
) -> Result<Option<horde_observe::FileLogging>, String> {
    if !wanted {
        return Ok(None);
    }
    let settings = horde_observe::settings::LogSettings::client().with_env(lookup)?;
    horde_observe::FileLogging::start(
        &client_log_dir(home),
        "horde",
        &settings,
        &horde_observe::ServiceIdentity::new("horde").with_principal(principal),
    )
    .map(Some)
}

/// Everything the `horde` binary installs for its own diagnostics: OTLP
/// export when the environment asks for it, and the opt-in log file.
///
/// # Errors
///
/// A refusal to print before any verb runs: unusable OTLP settings or an
/// unparseable `HORDE_LOG`.
pub fn observability(
    log_file: bool,
    principal: Option<String>,
    lookup: impl Fn(&str) -> Option<String>,
    home: Option<PathBuf>,
) -> Result<
    (
        Option<horde_observe::Telemetry>,
        Option<horde_observe::FileLogging>,
    ),
    String,
> {
    let identity = horde_observe::ServiceIdentity::new("horde").with_principal(principal.clone());
    let telemetry = horde_observe::init_from_env(identity, &lookup).map_err(|e| e.to_string())?;
    let wanted = wants_log_file(
        log_file,
        lookup(horde_observe::settings::CLIENT_FILE_VAR).as_deref(),
    );
    let file = client_file_logging(wanted, home, principal, lookup)?;
    Ok((telemetry, file))
}

/// The binary's last act: what the file still holds queued is written, and
/// export is flushed. Blocks; run it off the async runtime. Verdicts are
/// discarded on purpose — a dead collector must not change an exit code.
pub fn finish_observability(
    telemetry: Option<horde_observe::Telemetry>,
    file: Option<horde_observe::FileLogging>,
) {
    if let Some(file) = file {
        file.flush(Duration::from_secs(2));
    }
    if let Some(telemetry) = telemetry {
        let _ = telemetry.shutdown();
    }
}

/// What a remote read targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteTarget {
    Server,
    Worker(String),
    Agent(String),
}

/// The target `server` / `worker NAME` / `agent ID` names.
///
/// # Errors
///
/// No target, an unknown one, or a missing or surplus name.
pub fn remote_target(target: Option<&str>, name: Option<&str>) -> Result<RemoteTarget, String> {
    match (target, name) {
        (Some("server"), None) => Ok(RemoteTarget::Server),
        (Some("worker"), Some(name)) => Ok(RemoteTarget::Worker(name.to_owned())),
        (Some("agent"), Some(name)) => Ok(RemoteTarget::Agent(name.to_owned())),
        (Some("server"), Some(extra)) => Err(format!("`logs server` takes no name, got {extra:?}")),
        (Some(kind @ ("worker" | "agent")), None) => {
            Err(format!("`logs {kind}` needs the {kind}'s name"))
        }
        (Some(other), _) => Err(format!(
            "{other:?} is not a log target; use server, worker <NAME>, agent <AGENT>, or --dir"
        )),
        (None, _) => {
            Err("name what to read: server, worker <NAME>, agent <AGENT>, or --dir".to_owned())
        }
    }
}

/// The wire filter the flags describe.
#[must_use]
pub fn wire_filter(args: &LogsArgs) -> LogFilter {
    LogFilter {
        since: args.since.clone().unwrap_or_default(),
        level: args.level.clone().unwrap_or_default(),
        grep: args.grep.clone().unwrap_or_default(),
        agent: None,
        source: args.source.clone().unwrap_or_default(),
        tail_records: u32::try_from(args.n).unwrap_or(u32::MAX),
    }
}

/// The read request for a target.
#[must_use]
pub fn request(target: &RemoteTarget, args: &LogsArgs, after: Option<&Cursor>) -> ReadLogsRequest {
    let target = match target {
        RemoteTarget::Server => read_logs_request::Target::Server(ServerTarget {}),
        RemoteTarget::Worker(name) => read_logs_request::Target::Worker(name.clone()),
        RemoteTarget::Agent(id) => read_logs_request::Target::Agent(AgentId::new(id.clone())),
    };
    ReadLogsRequest {
        target: Some(target),
        filter: Some(wire_filter(args)),
        follow: args.follow,
        after: after.map(horde_wire::logs::cursor_to_wire),
        on_behalf_of: None,
        max_bytes: 0,
    }
}

/// One record as a terminal line: time, level, target, agent, message, and
/// the fields. A harness-stderr record reads as `STDERR`.
#[must_use]
pub fn human_line(record: &Record) -> String {
    let level = if record.key.source == horde_observe::record::Source::Stderr {
        "STDERR"
    } else {
        record.level()
    };
    let mut line = format!("{} {level:<5}", record.key.ts);
    if let Some(target) = record.text("target") {
        line.push(' ');
        line.push_str(target);
    }
    if let Some(agent) = record.text("agent") {
        line.push_str(&format!(" [{agent}]"));
    }
    line.push(' ');
    line.push_str(record.message());
    if let Some(Value::Object(fields)) = record.value.get("fields") {
        for (key, value) in fields {
            match value {
                Value::String(text) => line.push_str(&format!(" {key}={text}")),
                other => line.push_str(&format!(" {key}={other}")),
            }
        }
    }
    if record.value.get("truncated") == Some(&Value::Bool(true)) {
        line.push_str(" (truncated)");
    }
    line
}

/// One record as the format asks: its JSON line passed through, or a human
/// line.
#[must_use]
pub fn render(record: &Record, format: Format) -> String {
    match format {
        Format::Json => record.line.clone(),
        Format::Human => human_line(record),
    }
}

/// Prints records, marking a restart when the writing process's boot
/// changes between two of them.
struct Printer<'a> {
    out: &'a mut dyn Write,
    format: Format,
    boot: Option<String>,
}

impl Printer<'_> {
    fn record(&mut self, record: &Record) -> Result<(), ClientError> {
        let restarted = record.key.source == horde_observe::record::Source::Horde
            && !record.key.boot.is_empty()
            && self
                .boot
                .as_ref()
                .is_some_and(|boot| *boot != record.key.boot);
        if restarted && self.format == Format::Human {
            self.line(&format!("--- restarted (boot {}) ---", record.key.boot))?;
        }
        if record.key.source == horde_observe::record::Source::Horde && !record.key.boot.is_empty()
        {
            self.boot = Some(record.key.boot.clone());
        }
        let rendered = render(record, self.format);
        self.line(&rendered)
    }

    fn notice(&mut self, text: &str) -> Result<(), ClientError> {
        if self.format == Format::Human {
            self.line(&format!("--- {text} ---"))?;
        }
        Ok(())
    }

    fn line(&mut self, text: &str) -> Result<(), ClientError> {
        writeln!(self.out, "{text}")
            .and_then(|()| self.out.flush())
            .map_err(|source| ClientError::File {
                path: PathBuf::from("<stdout>"),
                source,
            })
    }
}

/// The local filter the flags describe, resolved against `now`.
///
/// # Errors
///
/// An unparseable `--since`, `--level`, `--grep` or `--source`.
pub fn local_filter(args: &LogsArgs, now: SystemTime) -> Result<Filter, String> {
    Ok(Filter {
        since: args
            .since
            .as_deref()
            .map(|since| resolve_since(since, now))
            .transpose()?,
        min_level: args
            .level
            .as_deref()
            .map(horde_observe::reader::Level::parse)
            .transpose()?,
        grep: args.grep.as_deref().map(compile_grep).transpose()?,
        agent: None,
        session_scope: None,
        sources: Sources::parse(args.source.as_deref().unwrap_or_default())?,
    })
}

/// How often `--dir --follow` looks for new records.
const DIR_POLL: Duration = Duration::from_millis(500);

/// `--dir`: the same reader and filters over local files, unredacted.
///
/// # Errors
///
/// A bad flag, or stdout going away.
pub async fn run_dir(
    dir: &Path,
    args: &LogsArgs,
    format: Format,
    out: &mut dyn Write,
) -> Result<(), ClientError> {
    let filter = local_filter(args, SystemTime::now()).map_err(ClientError::Logs)?;
    let mut printer = Printer {
        out,
        format,
        boot: None,
    };
    let mut window = Window {
        after: None,
        tail: Some(args.n),
        max_bytes: None,
    };
    loop {
        let selection = select(read_dir(dir, None), &filter, &window);
        for record in &selection.records {
            printer.record(record)?;
        }
        if !args.follow {
            return Ok(());
        }
        if let Some(last) = selection.last() {
            window.after = Some(last.clone());
        } else if window.after.is_none() {
            // Nothing matched yet: follow from now on, not from the start.
            window.after = Some(Cursor {
                ts: horde_observe::record::format_ts(SystemTime::now()),
                source: horde_observe::record::Source::Horde,
                boot: String::new(),
                seq: 0,
            });
        }
        window.tail = None;
        tokio::time::sleep(DIR_POLL).await;
    }
}

/// How a remote stream ended, as far as following it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ended {
    /// Nothing more will come.
    Finished,
    /// The transport or the worker went away; a follow redials.
    Lost(String),
}

/// Reads one stream to its end, printing as it goes; returns how it ended
/// and the last cursor seen.
async fn pump(
    stream: &mut tonic::Streaming<horde_proto::v1::ReadLogsResponse>,
    printer: &mut Printer<'_>,
    last: &mut Option<Cursor>,
) -> Result<Ended, ClientError> {
    loop {
        let message = match stream.message().await {
            Ok(Some(message)) => message,
            Ok(None) => return Ok(Ended::Lost("the stream closed".to_owned())),
            Err(status) => return Ok(Ended::Lost(status.message().to_owned())),
        };
        if let Some(ended) = show(message.body, printer, last)? {
            return Ok(ended);
        }
    }
}

/// Shows one response; `Some` when it was the stream's end.
fn show(
    body: Option<read_logs_response::Body>,
    printer: &mut Printer<'_>,
    last: &mut Option<Cursor>,
) -> Result<Option<Ended>, ClientError> {
    match body {
        Some(read_logs_response::Body::Records(batch)) => {
            for record in batch.lines.iter().filter_map(|line| Record::parse(line)) {
                printer.record(&record)?;
                *last = Some(record.key);
            }
            Ok(None)
        }
        Some(read_logs_response::Body::Gap(gap)) => {
            printer.notice(&format!("{} records missed", gap.lagged + gap.dropped))?;
            Ok(None)
        }
        Some(read_logs_response::Body::End(end)) => Ok(Some(ended_by(end.reason, &end.detail))),
        None => Ok(None),
    }
}

/// What an end frame means for a follow.
fn ended_by(reason: i32, detail: &str) -> Ended {
    if reason == LogEndReason::WorkerDisconnected as i32 {
        Ended::Lost(detail.to_owned())
    } else {
        Ended::Finished
    }
}

/// How long a follow waits before redialing, by attempt.
#[must_use]
pub fn redial_delay(attempt: u32) -> Duration {
    Duration::from_secs(u64::from(attempt.min(5)) * 2).max(Duration::from_millis(500))
}

/// A remote read; with `--follow`, redials on loss from the last cursor.
///
/// # Errors
///
/// A refusal on the first read (a follow keeps redialing after that), or
/// stdout going away.
pub async fn run_remote(
    client: &mut Client,
    target: &RemoteTarget,
    args: &LogsArgs,
    format: Format,
    out: &mut dyn Write,
) -> Result<(), ClientError> {
    let mut printer = Printer {
        out,
        format,
        boot: None,
    };
    let mut last: Option<Cursor> = None;
    let mut stream = client.read_logs(request(target, args, None)).await?;
    loop {
        match pump(&mut stream, &mut printer, &mut last).await? {
            Ended::Finished => return Ok(()),
            Ended::Lost(reason) if !args.follow => {
                return Err(ClientError::Rpc(tonic::Status::unavailable(reason)));
            }
            Ended::Lost(reason) => {
                printer.notice(&format!("lost the stream ({reason}); redialing"))?;
            }
        }
        stream = redial(client, target, args, last.as_ref()).await;
    }
}

/// Redials a lost follow from `last`, backing off until a read opens.
async fn redial(
    client: &mut Client,
    target: &RemoteTarget,
    args: &LogsArgs,
    last: Option<&Cursor>,
) -> tonic::Streaming<horde_proto::v1::ReadLogsResponse> {
    let mut attempt = 0;
    loop {
        tokio::time::sleep(redial_delay(attempt)).await;
        attempt = attempt.saturating_add(1);
        if let Ok(next) = client.read_logs(request(target, args, last)).await {
            return next;
        }
    }
}

/// Resolves an agent reference (an id, or a session name for its root) to
/// the agent id the read names.
async fn resolved(client: &mut Client, target: RemoteTarget) -> Result<RemoteTarget, ClientError> {
    match target {
        RemoteTarget::Agent(reference) => Ok(RemoteTarget::Agent(
            crate::resolve::agent_id(client, &reference).await?,
        )),
        other => Ok(other),
    }
}

/// `horde logs` against the server.
///
/// # Errors
///
/// A bad target, the server's refusal, or stdout going away.
pub async fn run(client: &mut Client, args: &LogsArgs, format: Format) -> Result<(), ClientError> {
    let target =
        remote_target(args.target.as_deref(), args.name.as_deref()).map_err(ClientError::Logs)?;
    let target = resolved(client, target).await?;
    run_remote(client, &target, args, format, &mut std::io::stdout()).await
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use horde_observe::record::Record;
    use horde_proto::v1::{LogEndReason, read_logs_request};

    use super::{
        Ended, LogsArgs, Printer, RemoteTarget, ended_by, human_line, local_filter, redial_delay,
        remote_target, render, request, run_dir,
    };
    use crate::Format;

    #[test]
    fn the_cli_writes_its_own_file_only_when_asked() {
        use super::{client_file_logging, client_log_dir, wants_log_file};
        assert!(!wants_log_file(false, None));
        assert!(!wants_log_file(false, Some("")));
        assert!(!wants_log_file(false, Some("0")));
        assert!(wants_log_file(false, Some("1")));
        assert!(wants_log_file(true, None));
        let home = tempfile::tempdir().expect("tempdir");
        assert_eq!(
            client_log_dir(Some(home.path().to_path_buf())),
            home.path().join(".horde/client/logs")
        );
        assert!(
            client_file_logging(false, Some(home.path().to_path_buf()), None, |_| None)
                .expect("off")
                .is_none()
        );
        let on = client_file_logging(true, Some(home.path().to_path_buf()), None, |_| None)
            .expect("on")
            .expect("a file layer");
        assert_eq!(on.dir(), home.path().join(".horde/client/logs"));
        assert_eq!(on.binary(), "horde");
        assert!(client_file_logging(true, None, None, |_| Some("=[".to_owned())).is_err());
    }

    #[test]
    fn the_binarys_observability_is_assembled_from_flags_and_environment() {
        use super::{finish_observability, observability};
        let home = tempfile::tempdir().expect("home");
        let (telemetry, file) =
            observability(false, None, |_| None, Some(home.path().to_path_buf())).expect("off");
        assert!(telemetry.is_none() && file.is_none());
        finish_observability(None, None);
        let (_, file) = observability(
            false,
            None,
            |name| (name == "HORDE_LOG_FILE").then(|| "1".to_owned()),
            Some(home.path().to_path_buf()),
        )
        .expect("on");
        let file = file.expect("the environment asked for a file");
        file.with_default(|| tracing::info!(target: "horde_cli", "noted"));
        finish_observability(None, Some(file));
        assert!(home.path().join(".horde/client/logs").is_dir());
        let refused = observability(
            true,
            None,
            |name| (name == "HORDE_LOG").then(|| "=[".to_owned()),
            Some(home.path().to_path_buf()),
        );
        assert!(refused.is_err());
        let otlp = observability(
            false,
            None,
            |name| (name == "HORDE_OTLP_SAMPLE").then(|| "0.5".to_owned()),
            None,
        );
        assert!(otlp.is_err(), "an OTLP refusal still refuses");
    }

    #[test]
    fn targets_parse_and_mistakes_are_named() {
        assert_eq!(
            remote_target(Some("server"), None),
            Ok(RemoteTarget::Server)
        );
        assert_eq!(
            remote_target(Some("worker"), Some("w1")),
            Ok(RemoteTarget::Worker("w1".to_owned()))
        );
        assert_eq!(
            remote_target(Some("agent"), Some("a1")),
            Ok(RemoteTarget::Agent("a1".to_owned()))
        );
        for (target, name, says) in [
            (Some("server"), Some("x"), "takes no name"),
            (Some("worker"), None, "needs the worker's name"),
            (Some("agent"), None, "needs the agent's name"),
            (Some("disk"), None, "not a log target"),
            (None, None, "name what to read"),
        ] {
            let refusal = remote_target(target, name).expect_err("refused");
            assert!(refusal.contains(says), "{refusal}");
        }
    }

    #[test]
    fn the_flags_become_the_request() {
        let args = LogsArgs {
            follow: true,
            since: Some("10m".to_owned()),
            level: Some("warn".to_owned()),
            grep: Some("boom".to_owned()),
            n: 5,
            source: Some("stderr".to_owned()),
            ..LogsArgs::default()
        };
        let read = request(&RemoteTarget::Agent("a1".to_owned()), &args, None);
        assert!(read.follow);
        assert!(read.after.is_none());
        assert_eq!(read.max_bytes, 0, "a client is unbounded");
        assert!(read.on_behalf_of.is_none());
        let filter = read.filter.expect("filter");
        assert_eq!(
            (
                filter.since.as_str(),
                filter.level.as_str(),
                filter.grep.as_str()
            ),
            ("10m", "warn", "boom")
        );
        assert_eq!((filter.source.as_str(), filter.tail_records), ("stderr", 5));
        assert!(
            matches!(read.target, Some(read_logs_request::Target::Agent(id)) if id.value == "a1")
        );
        let cursor = horde_observe::record::Cursor {
            ts: "t".to_owned(),
            source: horde_observe::record::Source::Horde,
            boot: "b".to_owned(),
            seq: 3,
        };
        let resumed = request(&RemoteTarget::Server, &LogsArgs::default(), Some(&cursor));
        assert_eq!(resumed.after.map(|after| after.seq), Some(3));
        assert!(matches!(
            resumed.target,
            Some(read_logs_request::Target::Server(_))
        ));
        let worker = request(
            &RemoteTarget::Worker("w".to_owned()),
            &LogsArgs::default(),
            None,
        );
        assert!(
            matches!(worker.target, Some(read_logs_request::Target::Worker(name)) if name == "w")
        );
    }

    fn record(line: &str) -> Record {
        Record::parse(line).expect("record")
    }

    #[test]
    fn a_record_renders_as_a_human_line_or_passes_through_as_json() {
        let horde = record(
            r#"{"ts":"2026-01-01T00:00:00.000000Z","level":"WARN","target":"horde_worker","msg":"slow","agent":"a1","fields":{"ms":12,"why":"disk"},"truncated":true}"#,
        );
        assert_eq!(
            human_line(&horde),
            "2026-01-01T00:00:00.000000Z WARN  horde_worker [a1] slow ms=12 why=disk (truncated)"
        );
        assert_eq!(render(&horde, Format::Json), horde.line);
        let stderr = record(
            r#"{"ts":"2026-01-01T00:00:01.000000Z","stream":"stderr","line":"boom","agent":"a1"}"#,
        );
        assert_eq!(
            human_line(&stderr),
            "2026-01-01T00:00:01.000000Z STDERR [a1] boom"
        );
    }

    #[test]
    fn a_boot_change_prints_a_restart_marker_in_human_output_only() {
        let first = record(r#"{"ts":"t1","msg":"a","boot":"b1","seq":1}"#);
        let second = record(r#"{"ts":"t2","msg":"b","boot":"b2","seq":1}"#);
        let mut human = Vec::new();
        {
            let mut printer = Printer {
                out: &mut human,
                format: Format::Human,
                boot: None,
            };
            printer.record(&first).expect("print");
            printer.record(&second).expect("print");
        }
        let text = String::from_utf8(human).expect("utf8");
        assert_eq!(
            text.matches("--- restarted (boot b2) ---").count(),
            1,
            "{text}"
        );
        let mut json = Vec::new();
        {
            let mut printer = Printer {
                out: &mut json,
                format: Format::Json,
                boot: None,
            };
            printer.record(&first).expect("print");
            printer.record(&second).expect("print");
            printer.notice("ignored").expect("print");
        }
        let text = String::from_utf8(json).expect("utf8");
        assert_eq!(text.lines().count(), 2, "only records in JSON: {text}");
    }

    #[test]
    fn only_a_vanished_worker_is_worth_redialing() {
        assert_eq!(ended_by(LogEndReason::Done as i32, ""), Ended::Finished);
        assert_eq!(ended_by(LogEndReason::ByteCap as i32, ""), Ended::Finished);
        assert_eq!(
            ended_by(LogEndReason::WorkerDisconnected as i32, "gone"),
            Ended::Lost("gone".to_owned())
        );
        assert_eq!(redial_delay(0), Duration::from_millis(500));
        assert_eq!(redial_delay(1), Duration::from_secs(2));
        assert_eq!(redial_delay(50), Duration::from_secs(10));
    }

    #[test]
    fn local_flags_resolve_or_refuse() {
        let now = SystemTime::now();
        assert!(local_filter(&LogsArgs::default(), now).is_ok());
        for bad in [
            LogsArgs {
                since: Some("later".to_owned()),
                ..LogsArgs::default()
            },
            LogsArgs {
                level: Some("loud".to_owned()),
                ..LogsArgs::default()
            },
            LogsArgs {
                grep: Some("(".to_owned()),
                ..LogsArgs::default()
            },
            LogsArgs {
                source: Some("disk".to_owned()),
                ..LogsArgs::default()
            },
        ] {
            assert!(local_filter(&bad, now).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn dir_reads_local_files_unredacted_with_the_same_filters() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("horde.b.1.jsonl"),
            [
                r#"{"ts":"2026-01-01T00:00:01.000000Z","level":"DEBUG","msg":"quiet"}"#,
                r#"{"ts":"2026-01-01T00:00:02.000000Z","level":"INFO","msg":"join horde://join/h:1/a.b"}"#,
                r#"{"ts":"2026-01-01T00:00:03.000000Z","level":"WARN","msg":"last"}"#,
            ]
            .join("\n"),
        )
        .expect("write");
        let args = LogsArgs {
            level: Some("info".to_owned()),
            n: 5,
            ..LogsArgs::default()
        };
        let mut out = Vec::new();
        run_dir(dir.path(), &args, Format::Json, &mut out)
            .await
            .expect("read");
        let text = String::from_utf8(out).expect("utf8");
        assert_eq!(text.lines().count(), 2, "{text}");
        assert!(
            text.contains("horde://join/h:1/a.b"),
            "the operator's own files are unredacted"
        );
        let mut tail = Vec::new();
        run_dir(
            dir.path(),
            &LogsArgs {
                n: 1,
                ..LogsArgs::default()
            },
            Format::Human,
            &mut tail,
        )
        .await
        .expect("read");
        assert!(String::from_utf8(tail).expect("utf8").contains("last"));
        let bad = run_dir(
            dir.path(),
            &LogsArgs {
                level: Some("loud".to_owned()),
                ..LogsArgs::default()
            },
            Format::Human,
            &mut Vec::new(),
        )
        .await;
        assert!(bad.is_err());
    }
}
