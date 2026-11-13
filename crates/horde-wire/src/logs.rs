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
    let tap = read
        .follow
        .then(|| logs.tap().map(horde_observe::tap::LogTap::subscribe))
        .flatten();
    let Some((used, last)) = send_initial(logs, &read, out).await else {
        return;
    };
    let Some(receiver) = tap else {
        let _ = out
            .send(end(LogEndReason::Done, "this process cannot be followed"))
            .await;
        return;
    };
    follow(receiver, &read, used, last, out).await;
}

/// The byte-cap end frame.
fn byte_cap() -> Frame {
    end(LogEndReason::ByteCap, "the byte cap was reached")
}

/// Sends the initial records. `None` when the read is over (finished,
/// capped, or its receiver gone); otherwise the bytes used and the cursor a
/// follow continues from.
async fn send_initial(
    logs: &LocalLogs,
    read: &LocalRead,
    out: &mpsc::Sender<Frame>,
) -> Option<(usize, Option<Cursor>)> {
    let window = Window {
        after: read.after.clone(),
        tail: Some(read.tail.unwrap_or(DEFAULT_TAIL)),
        max_bytes: read.max_bytes,
    };
    let initial = logs.read(&read.target, &read.filter, &window);
    for frame in record_frames(&initial.records, initial.truncated) {
        out.send(Frame::Records(frame)).await.ok()?;
    }
    let closing = if initial.truncated && read.after.is_some() {
        Some(byte_cap())
    } else if read.follow {
        None
    } else {
        Some(end(LogEndReason::Done, ""))
    };
    if let Some(closing) = closing {
        let _ = out.send(closing).await;
        return None;
    }
    let used = initial.records.iter().map(|record| record.line.len()).sum();
    Some((used, initial.last().cloned().or_else(|| read.after.clone())))
}

/// Follows the tap past `last` until the cap, the tap's end, or the
/// receiver going away.
async fn follow(
    mut receiver: tokio::sync::broadcast::Receiver<std::sync::Arc<str>>,
    read: &LocalRead,
    mut used: usize,
    mut last: Option<Cursor>,
    out: &mpsc::Sender<Frame>,
) {
    loop {
        let next = tokio::select! {
            next = next_followed(&mut receiver, &read.target, &read.filter, &mut last) => next,
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
                followed_frame(&record, used, read.max_bytes)
            }
        };
        let ended = matches!(frame, Frame::End(_));
        if out.send(frame).await.is_err() || ended {
            return;
        }
    }
}

/// One followed record's frame, or the byte-cap end once `used` passes it.
fn followed_frame(record: &Record, used: usize, cap: Option<usize>) -> Frame {
    if cap.is_some_and(|cap| used > cap) {
        return byte_cap();
    }
    let out = egress(std::slice::from_ref(record));
    Frame::Records(LogRecords {
        lines: out.lines,
        cursor: Some(cursor_to_wire(&record.key)),
        redacted: out.redacted,
        truncated: false,
    })
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

    use horde_observe::record::Record;

    use super::{
        FRAME_BYTES, Frame, LocalRead, cursor_from_wire, cursor_to_wire, filter_from_wire,
        followed_frame, record_frames, serve_local,
    };

    /// A record whose line is about `bytes` long, the `n`th second's.
    fn sized(n: u32, bytes: usize) -> Record {
        let line = format!(
            r#"{{"ts":"2026-01-01T00:00:{n:02}.000000Z","msg":"{}"}}"#,
            "x".repeat(bytes)
        );
        Record::parse(&line).expect("a record")
    }

    /// `record_frames` on its own thread, failing in seconds rather than
    /// spinning forever if its windowing stopped advancing.
    fn framed(records: Vec<Record>, truncated: bool) -> Vec<horde_proto::v1::LogRecords> {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(record_frames(&records, truncated));
        });
        finished
            .recv_timeout(Duration::from_secs(10))
            .expect("the records are framed")
    }

    /// A frame holds as many records as fit in 1 MiB, the next record
    /// starts the next frame, and each frame's cursor is its last record.
    #[test]
    fn records_are_packed_into_frames_of_at_most_a_mebibyte() {
        assert_eq!(FRAME_BYTES, 1024 * 1024);
        let records: Vec<Record> = (0..7).map(|n| sized(n, 300 * 1024)).collect();
        let frames = framed(records.clone(), false);
        assert_eq!(
            frames.iter().map(|f| f.lines.len()).collect::<Vec<_>>(),
            [3, 3, 1]
        );
        assert!(frames.iter().all(|f| !f.truncated), "nothing was left out");
        assert_eq!(
            frames[1].cursor,
            Some(cursor_to_wire(&records[5].key)),
            "the second frame ends at the sixth record"
        );
        let truncated = framed(records, true);
        assert_eq!(
            truncated.iter().map(|f| f.truncated).collect::<Vec<_>>(),
            [false, false, true],
            "only the last frame says records were left out"
        );
    }

    /// A record bigger than a frame still travels, alone.
    #[test]
    fn an_oversized_record_gets_a_frame_of_its_own() {
        let frames = framed(
            vec![sized(0, 10), sized(1, FRAME_BYTES + 10), sized(2, 10)],
            false,
        );
        assert_eq!(
            frames.iter().map(|f| f.lines.len()).collect::<Vec<_>>(),
            [1, 1, 1]
        );
    }

    /// The cap admits a followed record that reaches it exactly, and ends
    /// the follow on the first byte past it.
    #[test]
    fn a_followed_record_exactly_at_the_cap_is_sent() {
        let record = sized(0, 10);
        assert!(matches!(
            followed_frame(&record, 100, Some(100)),
            Frame::Records(batch) if batch.lines.len() == 1 && batch.cursor.is_some()
        ));
        assert!(matches!(
            followed_frame(&record, 101, Some(100)),
            Frame::End(end) if end.reason == LogEndReason::ByteCap as i32
        ));
        assert!(matches!(
            followed_frame(&record, usize::MAX, None),
            Frame::Records(_)
        ));
    }

    /// Runs an async test body on its own thread and runtime, failing in
    /// seconds. Framing is synchronous, so a read whose windowing stopped
    /// advancing would spin on the runtime's only thread, starve its timers,
    /// and leave every `tokio::time::timeout` in the body unfired; the wall
    /// clock here does not depend on the runtime making progress.
    fn bounded<F, Fut>(body: F)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()>,
    {
        let (done, finished) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime")
                .block_on(body());
            let _ = done.send(());
        });
        match finished.recv_timeout(Duration::from_secs(20)) {
            Ok(()) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                if let Err(panic) = worker.join() {
                    std::panic::resume_unwind(panic);
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                panic!("the test body did not finish in 20s")
            }
        }
    }

    /// The next frame of a followed read, bounded: a read that never
    /// produces it fails in seconds instead of hanging.
    async fn next_frame(receiver: &mut mpsc::Receiver<Frame>) -> Option<Frame> {
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("a frame arrives")
    }

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
        logging.with_default(|| {
            for message in messages {
                tracing::info!(target: "horde_worker", "{message}");
            }
        });
        assert!(logging.flush(Duration::from_secs(5)));
        logging
    }

    /// Serves a read that does not follow, to its end, bounded: a read that
    /// never finishes, or sends more than its channel holds without being
    /// read, fails in seconds instead of hanging.
    async fn served(logs: &LocalLogs, read: LocalRead) -> Vec<Frame> {
        let (sender, mut receiver) = mpsc::channel(64);
        tokio::time::timeout(Duration::from_secs(10), serve_local(logs, read, &sender))
            .await
            .expect("a read that does not follow returns");
        drop(sender);
        let mut frames = Vec::new();
        while let Some(frame) = receiver.recv().await {
            frames.push(frame);
        }
        frames
    }

    /// Serves `read` and asserts it said "truncated" with nothing in hand,
    /// then ended for `reason`.
    async fn assert_empty_truncated(logs: &LocalLogs, read: LocalRead, reason: LogEndReason) {
        let frames = served(logs, read).await;
        assert!(
            matches!(
                frames.as_slice(),
                [Frame::Records(none), Frame::End(end)]
                    if none.lines.is_empty() && none.truncated && end.reason == reason as i32
            ),
            "{frames:?}"
        );
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

    #[test]
    fn an_initial_read_redacts_and_ends_done() {
        bounded(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let _logging = written(dir.path(), &["m1", "m2 horde://join/h:1/a.b"]);
            let logs = LocalLogs::new(dir.path(), "horde-worker", None);
            let frames = served(&logs, read(None, 0, false)).await;
            let [Frame::Records(records), Frame::End(end)] = frames.as_slice() else {
                panic!("records then end: {frames:?}");
            };
            assert_eq!(records.lines.len(), 2);
            assert_eq!(records.redacted, 1);
            assert!(records.lines[1].contains("[REDACTED:join-token]"));
            assert!(records.cursor.is_some());
            assert_eq!(end.reason, LogEndReason::Done as i32);
        });
    }

    #[test]
    fn a_forward_read_that_hits_the_cap_ends_byte_cap() {
        bounded(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let _logging = written(dir.path(), &["m1", "m2", "m3"]);
            let logs = LocalLogs::new(dir.path(), "horde-worker", None);
            let first = LogCursor {
                ts: "1970-01-01T00:00:00.000000Z".to_owned(),
                source: "horde".to_owned(),
                ..LogCursor::default()
            };
            assert_empty_truncated(&logs, read(Some(&first), 1, false), LogEndReason::ByteCap)
                .await;
        });
    }

    /// A tail read whose newest record is already over the cap still says
    /// it left records out.
    #[test]
    fn a_tail_read_with_nothing_under_the_cap_reports_truncation() {
        bounded(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let _logging = written(dir.path(), &["m1"]);
            let logs = LocalLogs::new(dir.path(), "horde-worker", None);
            assert_empty_truncated(&logs, read(None, 1, false), LogEndReason::Done).await;
        });
    }

    #[test]
    fn a_follow_sees_new_records_and_stops_at_the_cap() {
        bounded(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let logging = written(dir.path(), &["m-before"]);
            let logs = LocalLogs::new(dir.path(), "horde-worker", Some(logging.tap().clone()));
            let (sender, mut receiver) = mpsc::channel(8);
            let serving = tokio::spawn(async move {
                serve_local(&logs, read(None, 600, true), &sender).await;
            });
            let Some(Frame::Records(initial)) = next_frame(&mut receiver).await else {
                panic!("the initial records come first");
            };
            assert_eq!(initial.lines.len(), 1);
            logging.with_default(|| {
                tracing::info!(target: "horde_worker", "not matched");
                tracing::info!(target: "horde_worker", "m-after");
            });
            let Some(Frame::Records(followed)) = next_frame(&mut receiver).await else {
                panic!("the followed record");
            };
            assert!(followed.lines[0].contains("m-after"));
            logging.with_default(|| {
                tracing::info!(target: "horde_worker", "m-{}", "x".repeat(600));
            });
            let Some(Frame::End(end)) = next_frame(&mut receiver).await else {
                panic!("the cap ends the follow");
            };
            assert_eq!(end.reason, LogEndReason::ByteCap as i32);
            tokio::time::timeout(Duration::from_secs(10), serving)
                .await
                .expect("the follow returns after its end")
                .expect("served");
        });
    }

    #[test]
    fn following_a_process_without_a_tap_says_so() {
        bounded(|| async {
            let dir = tempfile::tempdir().expect("tempdir");
            let logs = LocalLogs::new(dir.path(), "horde-worker", None);
            let frames = served(&logs, read(None, 0, true)).await;
            assert!(
                matches!(frames.as_slice(), [Frame::End(end)] if end.detail.contains("cannot be followed")),
                "{frames:?}"
            );
        });
    }
}
