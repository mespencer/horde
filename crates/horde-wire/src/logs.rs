//! Diagnostic logs on the wire (ADR-0015): the filter, the cursor, and the
//! one serving loop both the server (its own log) and a worker (its log,
//! and its agents' stderr) run.
//!
//! Serving lives here, between `horde-observe` (which reads files) and
//! `horde-proto` (which carries frames), so the server and the worker share
//! one copy: same windowing, same byte cap, same follow, and — the part
//! that matters — the same [`horde_observe::reader::egress`] call on every
//! record that leaves the machine.

use std::time::SystemTime;

use horde_observe::reader::{
    DEFAULT_TAIL, Filter, Followed, Level, LocalLogs, Sources, Target, Window, compile_grep,
    egress, next_followed, resolve_since,
};
use horde_observe::record::{Cursor, Record, Source};
use horde_proto::v1::{LogCursor, LogEnd, LogEndReason, LogFilter, LogGap, LogRecords};
use tokio::sync::mpsc;

/// The capability flag a worker advertises in its Hello when it serves log
/// reads. Without it the server refuses a read with `FAILED_PRECONDITION`
/// instead of waiting on a stream the worker will never open.
pub const LOGS_CAPABILITY: &str = "logs.v1";

/// The most record bytes one frame carries; a larger initial read is split.
pub const FRAME_BYTES: usize = 1024 * 1024;

/// A cursor on the wire.
#[must_use]
pub fn cursor_to_wire(cursor: &Cursor) -> LogCursor {
    LogCursor {
        ts: cursor.ts.clone(),
        source: cursor.source.as_str().to_owned(),
        boot: cursor.boot.clone(),
        seq: cursor.seq,
    }
}

/// A wire cursor read back.
///
/// # Errors
///
/// An empty timestamp or an unknown source.
pub fn cursor_from_wire(cursor: &LogCursor) -> Result<Cursor, String> {
    if cursor.ts.is_empty() {
        return Err("a log cursor needs its timestamp".to_owned());
    }
    let source = Source::parse(&cursor.source)
        .ok_or_else(|| format!("{:?} is not a log source", cursor.source))?;
    Ok(Cursor {
        ts: cursor.ts.clone(),
        source,
        boot: cursor.boot.clone(),
        seq: cursor.seq,
    })
}

/// A wire filter, resolved on this machine's clock at `now`. The session
/// scope is the server's to set, never the caller's, so it arrives
/// separately.
///
/// # Errors
///
/// An unparseable `since`, `level`, `grep` or `source`.
pub fn filter_from_wire(
    wire: &LogFilter,
    session_scope: Option<String>,
    now: SystemTime,
) -> Result<Filter, String> {
    Ok(Filter {
        since: non_empty(&wire.since)
            .map(|since| resolve_since(since, now))
            .transpose()?,
        min_level: non_empty(&wire.level).map(Level::parse).transpose()?,
        grep: non_empty(&wire.grep).map(compile_grep).transpose()?,
        agent: wire
            .agent
            .as_ref()
            .map(|agent| agent.value.clone())
            .filter(|agent| !agent.is_empty()),
        session_scope,
        sources: Sources::parse(&wire.source)?,
    })
}

fn non_empty(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}

/// One read to serve from a machine's own files.
#[derive(Debug, Clone)]
pub struct LocalRead {
    pub target: Target,
    pub filter: Filter,
    pub after: Option<Cursor>,
    /// Newest records before any follow; `None` takes [`DEFAULT_TAIL`].
    pub tail: Option<usize>,
    pub follow: bool,
    /// Bytes of record lines, at most; `None` is unbounded.
    pub max_bytes: Option<usize>,
}

impl LocalRead {
    /// The read a wire request describes. `tail_records` of zero is the
    /// default tail; `max_bytes` of zero is unbounded.
    ///
    /// # Errors
    ///
    /// As [`filter_from_wire`] and [`cursor_from_wire`].
    pub fn from_wire(
        target: Target,
        filter: &LogFilter,
        session_scope: Option<String>,
        follow: bool,
        after: Option<&LogCursor>,
        max_bytes: u32,
    ) -> Result<Self, String> {
        Ok(Self {
            target,
            filter: filter_from_wire(filter, session_scope, SystemTime::now())?,
            after: after.map(cursor_from_wire).transpose()?,
            tail: usize::try_from(filter.tail_records)
                .ok()
                .filter(|tail| *tail != 0),
            follow,
            max_bytes: usize::try_from(max_bytes).ok().filter(|cap| *cap != 0),
        })
    }
}

/// One frame of a log stream, before it is wrapped in either spine's
/// response type.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Records(LogRecords),
    Gap(LogGap),
    End(LogEnd),
}

/// The end frame for `reason`.
#[must_use]
pub fn end(reason: LogEndReason, detail: impl Into<String>) -> Frame {
    Frame::End(LogEnd {
        reason: reason.into(),
        detail: detail.into(),
    })
}

/// Splits records into frames of at most [`FRAME_BYTES`] each, redacted.
fn record_frames(records: &[Record], truncated: bool) -> Vec<LogRecords> {
    if records.is_empty() && truncated {
        // Everything matched was over the cap: say so, with nothing in hand.
        return vec![LogRecords {
            truncated: true,
            ..LogRecords::default()
        }];
    }
    let mut frames = Vec::new();
    let mut start = 0;
    while start < records.len() {
        let mut bytes = 0;
        let mut end = start;
        while end < records.len()
            && (end == start || bytes + records[end].line.len() <= FRAME_BYTES)
        {
            bytes += records[end].line.len();
            end += 1;
        }
        let chunk = &records[start..end];
        let out = egress(chunk);
        frames.push(LogRecords {
            lines: out.lines,
            cursor: chunk.last().map(|record| cursor_to_wire(&record.key)),
            redacted: out.redacted,
            truncated: truncated && end == records.len(),
        });
        start = end;
    }
    frames
}

/// Serves one read into `out` until it is done, the byte cap is reached, or
/// the receiver goes away. Always ends with an end frame unless the
/// receiver is gone.
pub async fn serve_local(logs: &LocalLogs, read: LocalRead, out: &mpsc::Sender<Frame>) {
    // Subscribed before the files are read, so nothing written between the
    // read and the follow is missed; the cursor removes the overlap.
    let mut tap = read
        .follow
        .then(|| logs.tap().map(horde_observe::tap::LogTap::subscribe))
        .flatten();
    let window = Window {
        after: read.after.clone(),
        tail: Some(read.tail.unwrap_or(DEFAULT_TAIL)),
        max_bytes: read.max_bytes,
    };
    let initial = logs.read(&read.target, &read.filter, &window);
    let mut used: usize = initial.records.iter().map(|record| record.line.len()).sum();
    let mut last = initial.last().cloned().or(read.after.clone());
    for frame in record_frames(&initial.records, initial.truncated) {
        if out.send(Frame::Records(frame)).await.is_err() {
            return;
        }
    }
    if initial.truncated && read.after.is_some() {
        let _ = out
            .send(end(LogEndReason::ByteCap, "the byte cap was reached"))
            .await;
        return;
    }
    if !read.follow {
        let _ = out.send(end(LogEndReason::Done, "")).await;
        return;
    }
    let Some(receiver) = tap.as_mut() else {
        let _ = out
            .send(end(LogEndReason::Done, "this process cannot be followed"))
            .await;
        return;
    };
    loop {
        let next = tokio::select! {
            next = next_followed(receiver, &read.target, &read.filter, &mut last) => next,
            () = out.closed() => return,
        };
        let frame = match next {
            None => end(LogEndReason::Done, "the log closed"),
            Some(Followed::Lagged(missed)) => Frame::Gap(LogGap {
                lagged: missed,
                dropped: 0,
            }),
            Some(Followed::Record(record)) => {
                used += record.line.len();
                if read.max_bytes.is_some_and(|cap| used > cap) {
                    let _ = out
                        .send(end(LogEndReason::ByteCap, "the byte cap was reached"))
                        .await;
                    return;
                }
                let mut frames = record_frames(std::slice::from_ref(&record), false);
                match frames.pop() {
                    Some(records) => Frame::Records(records),
                    None => continue,
                }
            }
        };
        let ended = matches!(frame, Frame::End(_));
        if out.send(frame).await.is_err() || ended {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use horde_observe::layer::FileLogging;
    use horde_observe::reader::{LocalLogs, Target};
    use horde_observe::record::{Cursor, Source};
    use horde_observe::settings::LogSettings;
    use horde_proto::v1::{AgentId, LogCursor, LogEndReason, LogFilter};
    use tokio::sync::mpsc;
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{
        Frame, LocalRead, cursor_from_wire, cursor_to_wire, filter_from_wire, serve_local,
    };

    #[test]
    fn cursors_round_trip_and_bad_ones_are_refused() {
        let cursor = Cursor {
            ts: "2026-01-01T00:00:00.000000Z".to_owned(),
            source: Source::Stderr,
            boot: "b".to_owned(),
            seq: 9,
        };
        assert_eq!(cursor_from_wire(&cursor_to_wire(&cursor)), Ok(cursor));
        assert!(cursor_from_wire(&LogCursor::default()).is_err());
        assert!(
            cursor_from_wire(&LogCursor {
                ts: "t".to_owned(),
                source: "disk".to_owned(),
                ..LogCursor::default()
            })
            .is_err()
        );
    }

    #[test]
    fn a_wire_filter_resolves_or_refuses() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(600);
        let filter = filter_from_wire(
            &LogFilter {
                since: "5m".to_owned(),
                level: "warn".to_owned(),
                grep: "boom".to_owned(),
                agent: Some(AgentId::new("a1")),
                source: "stderr".to_owned(),
                tail_records: 0,
            },
            Some("s1".to_owned()),
            now,
        )
        .expect("valid");
        assert_eq!(filter.since.as_deref(), Some("1970-01-01T00:05:00.000000Z"));
        assert_eq!(filter.agent.as_deref(), Some("a1"));
        assert_eq!(filter.session_scope.as_deref(), Some("s1"));
        assert!(!filter.sources.horde && filter.sources.stderr);
        let empty = filter_from_wire(&LogFilter::default(), None, now).expect("empty is all");
        assert!(empty.since.is_none() && empty.grep.is_none() && empty.agent.is_none());
        for bad in [
            LogFilter {
                level: "loud".to_owned(),
                ..LogFilter::default()
            },
            LogFilter {
                grep: "(".to_owned(),
                ..LogFilter::default()
            },
            LogFilter {
                since: "later".to_owned(),
                ..LogFilter::default()
            },
            LogFilter {
                source: "disk".to_owned(),
                ..LogFilter::default()
            },
        ] {
            assert!(filter_from_wire(&bad, None, now).is_err(), "{bad:?}");
        }
    }

    fn written(dir: &std::path::Path, messages: &[&str]) -> FileLogging {
        let logging = FileLogging::start(
            dir,
            "horde-worker",
            &LogSettings::daemon(),
            &horde_observe::ServiceIdentity::new("horde-worker"),
        )
        .expect("start");
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(logging.layer()));
        tracing::dispatcher::with_default(&dispatch, || {
            for message in messages {
                tracing::info!(target: "horde_worker", "{message}");
            }
        });
        assert!(logging.flush(Duration::from_secs(5)));
        logging
    }

    async fn drain(mut receiver: mpsc::Receiver<Frame>) -> Vec<Frame> {
        let mut frames = Vec::new();
        while let Some(frame) = receiver.recv().await {
            frames.push(frame);
        }
        frames
    }

    fn read(after: Option<&LogCursor>, max_bytes: u32, follow: bool) -> LocalRead {
        LocalRead::from_wire(
            Target::Process,
            &LogFilter {
                grep: "^m".to_owned(),
                ..LogFilter::default()
            },
            None,
            follow,
            after,
            max_bytes,
        )
        .expect("read")
    }

    #[tokio::test]
    async fn an_initial_read_redacts_and_ends_done() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _logging = written(dir.path(), &["m1", "m2 horde://join/h:1/a.b"]);
        let logs = LocalLogs::new(dir.path(), "horde-worker", None);
        let (sender, receiver) = mpsc::channel(8);
        serve_local(&logs, read(None, 0, false), &sender).await;
        drop(sender);
        let frames = drain(receiver).await;
        let [Frame::Records(records), Frame::End(end)] = frames.as_slice() else {
            panic!("records then end: {frames:?}");
        };
        assert_eq!(records.lines.len(), 2);
        assert_eq!(records.redacted, 1);
        assert!(records.lines[1].contains("[REDACTED:join-token]"));
        assert!(records.cursor.is_some());
        assert_eq!(end.reason, LogEndReason::Done as i32);
    }

    #[tokio::test]
    async fn a_forward_read_that_hits_the_cap_ends_byte_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _logging = written(dir.path(), &["m1", "m2", "m3"]);
        let logs = LocalLogs::new(dir.path(), "horde-worker", None);
        let first = LogCursor {
            ts: "1970-01-01T00:00:00.000000Z".to_owned(),
            source: "horde".to_owned(),
            ..LogCursor::default()
        };
        let (sender, receiver) = mpsc::channel(8);
        serve_local(&logs, read(Some(&first), 1, false), &sender).await;
        drop(sender);
        let frames = drain(receiver).await;
        assert!(
            matches!(
                frames.as_slice(),
                [Frame::Records(none), Frame::End(end)]
                    if none.lines.is_empty() && none.truncated
                        && end.reason == LogEndReason::ByteCap as i32
            ),
            "{frames:?}"
        );
    }

    /// A tail read whose newest record is already over the cap still says
    /// it left records out.
    #[tokio::test]
    async fn a_tail_read_with_nothing_under_the_cap_reports_truncation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _logging = written(dir.path(), &["m1"]);
        let logs = LocalLogs::new(dir.path(), "horde-worker", None);
        let (sender, receiver) = mpsc::channel(8);
        serve_local(&logs, read(None, 1, false), &sender).await;
        drop(sender);
        let frames = drain(receiver).await;
        assert!(
            matches!(
                frames.as_slice(),
                [Frame::Records(none), Frame::End(end)]
                    if none.lines.is_empty() && none.truncated
                        && end.reason == LogEndReason::Done as i32
            ),
            "{frames:?}"
        );
    }

    #[tokio::test]
    async fn a_follow_sees_new_records_and_stops_at_the_cap() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = written(dir.path(), &["m-before"]);
        let logs = LocalLogs::new(dir.path(), "horde-worker", Some(logging.tap().clone()));
        let (sender, mut receiver) = mpsc::channel(8);
        let serving = tokio::spawn(async move {
            serve_local(&logs, read(None, 600, true), &sender).await;
        });
        let Some(Frame::Records(initial)) = receiver.recv().await else {
            panic!("the initial records come first");
        };
        assert_eq!(initial.lines.len(), 1);
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(logging.layer()));
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "horde_worker", "not matched");
            tracing::info!(target: "horde_worker", "m-after");
        });
        let Some(Frame::Records(followed)) = receiver.recv().await else {
            panic!("the followed record");
        };
        assert!(followed.lines[0].contains("m-after"));
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!(target: "horde_worker", "m-{}", "x".repeat(600));
        });
        let Some(Frame::End(end)) = receiver.recv().await else {
            panic!("the cap ends the follow");
        };
        assert_eq!(end.reason, LogEndReason::ByteCap as i32);
        serving.await.expect("served");
    }

    #[tokio::test]
    async fn following_a_process_without_a_tap_says_so() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logs = LocalLogs::new(dir.path(), "horde-worker", None);
        let (sender, receiver) = mpsc::channel(8);
        serve_local(&logs, read(None, 0, true), &sender).await;
        drop(sender);
        let frames = drain(receiver).await;
        assert!(
            matches!(frames.as_slice(), [Frame::End(end)] if end.detail.contains("cannot be followed")),
            "{frames:?}"
        );
    }
}
