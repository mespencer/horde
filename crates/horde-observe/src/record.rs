//! One log record: its timestamp spelling, its place in the order, and the
//! bound on its size (ADR-0015).
//!
//! Every file the log layer and the harness-stderr writer produce holds one
//! JSON object per line. The reader never needs a schema beyond the handful
//! of keys named here — `ts`, `level`, `msg` (or `line`, for harness
//! stderr), `agent`, `session`, `stream`, `boot`, `seq` — which is why the
//! wire carries records as opaque strings.

use std::time::SystemTime;

use serde_json::{Map, Value};

/// The longest a record may be once serialized. Longer records are cut on a
/// char boundary and marked `"truncated": true`.
pub const MAX_RECORD_BYTES: usize = 16 * 1024;

/// The `stream` value that marks a harness-stderr record.
pub const STDERR_STREAM: &str = "stderr";

/// Which file family a record came from. Part of the order key, so the two
/// families merged for one agent never tie.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Source {
    /// A daemon's own tracing records.
    Horde,
    /// A harness's stderr, one line per record.
    Stderr,
}

impl Source {
    /// The spelling a cursor carries.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Horde => "horde",
            Self::Stderr => "stderr",
        }
    }

    /// Reads [`Self::as_str`] back.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "horde" => Some(Self::Horde),
            "stderr" => Some(Self::Stderr),
            _ => None,
        }
    }
}

/// A record's position in the merged order: timestamp first, then source,
/// boot and sequence to break ties. Readers sort by it, followers resume
/// strictly after it, and the file/tap boundary deduplicates on it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Cursor {
    /// The record's `ts`, in [`format_ts`]'s fixed-width spelling, so string
    /// order is time order.
    pub ts: String,
    pub source: Source,
    pub boot: String,
    pub seq: u64,
}

impl Cursor {
    /// The opaque one-string form the MCP tool hands an agent to page with.
    #[must_use]
    pub fn encode(&self) -> String {
        format!(
            "{}~{}~{}~{}",
            self.ts,
            self.source.as_str(),
            self.boot,
            self.seq
        )
    }

    /// Reads [`Self::encode`] back; `None` for anything else.
    #[must_use]
    pub fn decode(text: &str) -> Option<Self> {
        let mut parts = text.split('~');
        let ts = parts.next()?.to_owned();
        let source = Source::parse(parts.next()?)?;
        let boot = parts.next()?.to_owned();
        let seq = parts.next()?.parse().ok()?;
        if parts.next().is_some() || ts.is_empty() {
            return None;
        }
        Some(Self {
            ts,
            source,
            boot,
            seq,
        })
    }
}

/// `t` as RFC 3339 UTC with exactly six fractional digits:
/// `2026-11-13T10:00:00.123456Z`. Fixed width on purpose — the reader
/// compares timestamps as strings.
#[must_use]
pub fn format_ts(t: SystemTime) -> String {
    let at = time::OffsetDateTime::from(t);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}Z",
        at.year(),
        u8::from(at.month()),
        at.day(),
        at.hour(),
        at.minute(),
        at.second(),
        at.microsecond()
    )
}

/// Parses an RFC 3339 timestamp (any offset, any precision) into the
/// instant it names.
///
/// # Errors
///
/// A message naming what did not parse.
pub fn parse_ts(text: &str) -> Result<SystemTime, String> {
    time::OffsetDateTime::parse(text, &time::format_description::well_known::Rfc3339)
        .map(SystemTime::from)
        .map_err(|error| format!("{text:?} is not an RFC 3339 timestamp: {error}"))
}

/// Serializes `record`, cutting it to [`MAX_RECORD_BYTES`] when it is longer.
///
/// The cut keeps the line valid JSON: the longest text field (`msg` or
/// `line`) is shortened on a char boundary first, then `fields` and `spans`
/// are emptied if that was not enough, and the record is marked
/// `"truncated": true`.
#[must_use]
pub fn serialize_bounded(mut record: Map<String, Value>) -> String {
    let whole = Value::Object(record.clone()).to_string();
    if whole.len() <= MAX_RECORD_BYTES {
        return whole;
    }
    record.insert("truncated".to_owned(), Value::Bool(true));
    // Room for the marker and for escapes that the cut text may still carry.
    let excess = whole.len() - MAX_RECORD_BYTES + 64;
    let longest = ["msg", "line"].into_iter().find(|key| {
        record
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|text| text.len() >= excess)
    });
    if let Some(key) = longest {
        // The message is the oversized part: cut it, keep everything else.
        if let Some(Value::String(text)) = record.get_mut(key) {
            let keep = text.len() - excess;
            cut_on_char_boundary(text, keep);
        }
    } else {
        // The message is not the problem: drop the structure, keep it.
        record.insert("fields".to_owned(), Value::Object(Map::new()));
        record.insert("spans".to_owned(), Value::Array(Vec::new()));
    }
    let shortened = Value::Object(record.clone()).to_string();
    if shortened.len() <= MAX_RECORD_BYTES {
        return shortened;
    }
    // Something other than the known keys is oversized: keep only them.
    let mut minimal = Map::new();
    for key in ["ts", "level", "target", "boot", "seq", "agent", "session"] {
        if let Some(value) = record.get(key) {
            minimal.insert(key.to_owned(), value.clone());
        }
    }
    minimal.insert("msg".to_owned(), Value::String(String::new()));
    minimal.insert("truncated".to_owned(), Value::Bool(true));
    Value::Object(minimal).to_string()
}

/// Shortens `text` to at most `keep` bytes, backing off to a char boundary.
fn cut_on_char_boundary(text: &mut String, keep: usize) {
    if text.len() <= keep {
        return;
    }
    let mut end = keep;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
}

/// One parsed record, with its raw line kept for egress.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub line: String,
    pub value: Map<String, Value>,
    pub key: Cursor,
}

impl Record {
    /// Parses one line. `None` for a line that is not a JSON object with a
    /// `ts` — a torn last line, or a file that is not one of ours.
    #[must_use]
    pub fn parse(line: &str) -> Option<Self> {
        let Value::Object(value) = serde_json::from_str::<Value>(line).ok()? else {
            return None;
        };
        let ts = value.get("ts")?.as_str()?.to_owned();
        let source = if value.get("stream").and_then(Value::as_str) == Some(STDERR_STREAM) {
            Source::Stderr
        } else {
            Source::Horde
        };
        let boot = value
            .get("boot")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let seq = value.get("seq").and_then(Value::as_u64).unwrap_or_default();
        Some(Self {
            line: line.to_owned(),
            key: Cursor {
                ts,
                source,
                boot,
                seq,
            },
            value,
        })
    }

    /// A top-level string field.
    #[must_use]
    pub fn text(&self, key: &str) -> Option<&str> {
        self.value.get(key).and_then(Value::as_str)
    }

    /// The record's message: `msg`, or a harness-stderr record's `line`.
    #[must_use]
    pub fn message(&self) -> &str {
        self.text("msg").or_else(|| self.text("line")).unwrap_or("")
    }

    /// The level, uppercase (`INFO`). Harness stderr has no level of its own
    /// and reads as `INFO`.
    #[must_use]
    pub fn level(&self) -> &str {
        self.text("level").unwrap_or("INFO")
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use serde_json::{Map, Value, json};

    use super::{Cursor, MAX_RECORD_BYTES, Record, Source, format_ts, parse_ts, serialize_bounded};

    #[test]
    fn timestamps_are_fixed_width_and_round_trip() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_micros(1_763_028_000_000_001);
        let text = format_ts(at);
        assert_eq!(text, "2025-11-13T10:00:00.000001Z");
        assert_eq!(parse_ts(&text), Ok(at));
        let earlier = format_ts(at - Duration::from_secs(1));
        assert!(earlier < text, "string order is time order");
        assert!(parse_ts("yesterday").is_err());
    }

    #[test]
    fn cursors_encode_and_decode_and_refuse_garbage() {
        let cursor = Cursor {
            ts: "2026-01-01T00:00:00.000000Z".to_owned(),
            source: Source::Stderr,
            boot: "b1".to_owned(),
            seq: 42,
        };
        assert_eq!(Cursor::decode(&cursor.encode()), Some(cursor));
        for garbage in [
            "",
            "a~b",
            "~horde~b~1",
            "t~nope~b~1",
            "t~horde~b~x",
            "t~horde~b~1~extra",
        ] {
            assert_eq!(Cursor::decode(garbage), None, "{garbage:?}");
        }
        assert_eq!(Source::parse("horde"), Some(Source::Horde));
        assert!(Source::Horde < Source::Stderr);
    }

    fn record(msg: &str) -> Map<String, Value> {
        json!({"ts": "t", "level": "INFO", "msg": msg, "fields": {}, "spans": []})
            .as_object()
            .cloned()
            .unwrap_or_default()
    }

    /// The bound is 16 KiB: a record just under it is kept whole.
    #[test]
    fn a_record_just_under_sixteen_kib_is_kept_whole() {
        let line = serialize_bounded(record(&"m".repeat(16 * 1024 - 100)));
        assert!(!line.contains("truncated"), "{} bytes", line.len());
        assert_eq!(MAX_RECORD_BYTES, 16 * 1024);
    }

    /// Runs `work` on its own thread and fails if it has not finished in
    /// seconds: a cut that stopped backing off would spin forever, and must
    /// fail rather than hang the suite.
    fn bounded<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> T {
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = done.send(work());
        });
        finished
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the cut finishes")
    }

    #[test]
    fn a_cut_backs_off_to_a_char_boundary() {
        let text = bounded(|| {
            let mut text = "éé".to_owned();
            super::cut_on_char_boundary(&mut text, 3);
            text
        });
        assert_eq!(text, "é");
        let short = bounded(|| {
            let mut short = "ab".to_owned();
            super::cut_on_char_boundary(&mut short, 5);
            short
        });
        assert_eq!(short, "ab");
    }

    #[test]
    fn a_short_record_is_serialized_whole() {
        let line = serialize_bounded(record("hello"));
        assert!(!line.contains("truncated"));
        assert_eq!(
            Record::parse(&line).map(|r| r.message().to_owned()),
            Some("hello".to_owned())
        );
    }

    #[test]
    fn an_oversized_record_is_cut_on_a_char_boundary_and_stays_json() {
        // Multi-byte characters straddle every possible cut point.
        let huge = "é".repeat(MAX_RECORD_BYTES);
        let line = bounded(move || serialize_bounded(record(&huge)));
        assert!(line.len() <= MAX_RECORD_BYTES, "{}", line.len());
        let parsed = Record::parse(&line).expect("still one JSON object");
        assert_eq!(parsed.value.get("truncated"), Some(&Value::Bool(true)));
        assert!(parsed.message().chars().all(|c| c == 'é'));
        assert!(
            parsed.message().len() > MAX_RECORD_BYTES / 2,
            "cut, not emptied"
        );
    }

    #[test]
    fn oversized_fields_are_emptied_when_the_message_is_not_the_problem() {
        let mut map = record("short");
        map.insert(
            "fields".to_owned(),
            json!({"blob": "x".repeat(MAX_RECORD_BYTES * 2)}),
        );
        let line = serialize_bounded(map);
        assert!(line.len() <= MAX_RECORD_BYTES);
        let parsed = Record::parse(&line).expect("json");
        assert_eq!(parsed.value.get("fields"), Some(&json!({})));
        assert_eq!(parsed.message(), "short");

        let mut odd = record("short");
        odd.insert("other".to_owned(), json!("y".repeat(MAX_RECORD_BYTES * 2)));
        let line = serialize_bounded(odd);
        assert!(line.len() <= MAX_RECORD_BYTES);
        let parsed = Record::parse(&line).expect("json");
        assert!(parsed.value.get("other").is_none());
        assert_eq!(parsed.value.get("truncated"), Some(&Value::Bool(true)));
    }

    #[test]
    fn a_record_reads_its_key_message_and_level() {
        let stderr = Record::parse(
            r#"{"ts":"t1","stream":"stderr","line":"boom","boot":"b","seq":3,"agent":"a"}"#,
        )
        .expect("record");
        assert_eq!(stderr.key.source, Source::Stderr);
        assert_eq!(stderr.key.seq, 3);
        assert_eq!(stderr.message(), "boom");
        assert_eq!(stderr.level(), "INFO");
        assert_eq!(stderr.text("agent"), Some("a"));

        let horde = Record::parse(r#"{"ts":"t2","level":"WARN","msg":"m"}"#).expect("record");
        assert_eq!(horde.key.source, Source::Horde);
        assert_eq!(horde.key.boot, "");
        assert_eq!(horde.level(), "WARN");

        for torn in ["", "{", "[]", r#"{"msg":"no ts"}"#, r#"{"ts":5}"#] {
            assert!(Record::parse(torn).is_none(), "{torn:?}");
        }
    }
}
