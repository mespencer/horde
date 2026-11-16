//! The file layer: tracing records as JSON lines, written off-thread
//! (ADR-0015).
//!
//! The layer serializes each event where it happens — `ts`, `level`,
//! `target`, `msg`, `fields`, `spans`, the hoisted `agent`/`session`, and
//! the process's `boot` and `seq` — publishes it to the [`LogTap`], and
//! hands it to a writer thread through a bounded channel. A full channel
//! drops the record and counts it; the writer later writes one
//! `log.dropped` record with the count. An I/O failure disables the file
//! for a minute, warns once on stderr, and then tries a fresh file.
//! **Logging is never fatal**, and a slow disk never stalls a request.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::time::{Duration, SystemTime};

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

use crate::files::{FileSet, RotatingFiles};
use crate::pipeline::{ServiceIdentity, Telemetry};
use crate::record::{format_ts, serialize_bounded};
use crate::settings::LogSettings;
use crate::tap::{LogTap, QUIET_TARGET};

/// How many records may wait for the writer before new ones are dropped.
pub const CHANNEL_CAPACITY: usize = 8192;

/// How long a failed file stays disabled before a fresh one is tried.
pub const RETRY_AFTER: Duration = Duration::from_secs(60);

/// A process's boot id and record counter.
#[derive(Debug)]
pub struct Stamp {
    boot: String,
    seq: AtomicU64,
}

impl Stamp {
    #[must_use]
    pub fn new(boot: String) -> Self {
        Self {
            boot,
            seq: AtomicU64::new(0),
        }
    }

    /// The boot id.
    #[must_use]
    pub fn boot(&self) -> &str {
        &self.boot
    }

    /// The next sequence number, starting at 1.
    pub fn next(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// A fresh random boot id: 16 hex digits.
#[must_use]
pub fn new_boot() -> String {
    boot_from(getrandom::u64(), SystemTime::now())
}

/// The boot id for `random`, or for `now` when there was no randomness: no
/// randomness is no reason to stop logging, and the clock is unique enough
/// to tell two boots of one process apart.
fn boot_from<E>(random: Result<u64, E>, now: SystemTime) -> String {
    let value = random.unwrap_or_else(|_| {
        now.duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default()
    });
    format!("{value:016x}")
}

/// A record the logging machinery writes about itself.
fn system_record(stamp: &Stamp, now: SystemTime, msg: &str, fields: Map<String, Value>) -> String {
    let mut record = Map::new();
    record.insert("ts".to_owned(), Value::String(format_ts(now)));
    record.insert("level".to_owned(), Value::String("INFO".to_owned()));
    record.insert(
        "target".to_owned(),
        Value::String("horde_observe".to_owned()),
    );
    record.insert("msg".to_owned(), Value::String(msg.to_owned()));
    record.insert("fields".to_owned(), Value::Object(fields));
    record.insert("spans".to_owned(), Value::Array(Vec::new()));
    record.insert("boot".to_owned(), Value::String(stamp.boot.clone()));
    record.insert("seq".to_owned(), Value::from(stamp.next()));
    serialize_bounded(record)
}

/// The `log.dropped` record: how many records were lost since the last one.
#[must_use]
pub fn dropped_record(stamp: &Stamp, now: SystemTime, count: u64) -> String {
    let mut fields = Map::new();
    fields.insert("count".to_owned(), Value::from(count));
    system_record(stamp, now, "log.dropped", fields)
}

/// What the writer thread is handed.
#[derive(Debug)]
enum Message {
    Line(String),
    Flush(std::sync::mpsc::Sender<()>),
}

/// The writer thread's state, separated from the thread so its failure
/// handling is testable without one.
#[derive(Debug)]
pub struct WriterState {
    files: RotatingFiles,
    stamp: Arc<Stamp>,
    tap: LogTap,
    dropped: Arc<AtomicU64>,
    disabled_until: Option<SystemTime>,
    warn: fn(&str),
}

impl WriterState {
    #[must_use]
    pub fn new(
        files: RotatingFiles,
        stamp: Arc<Stamp>,
        tap: LogTap,
        dropped: Arc<AtomicU64>,
    ) -> Self {
        Self {
            files,
            stamp,
            tap,
            dropped,
            disabled_until: None,
            warn: |text| eprintln!("{text}"),
        }
    }

    /// Replaces the stderr warning (tests capture it).
    #[must_use]
    pub fn with_warn(mut self, warn: fn(&str)) -> Self {
        self.warn = warn;
        self
    }

    /// Whether the file is disabled at `now`.
    #[must_use]
    pub fn is_disabled(&self, now: SystemTime) -> bool {
        self.disabled_until.is_some_and(|until| now < until)
    }

    /// Writes one record, then the `log.dropped` record when records were
    /// lost before it. A failure disables the file until [`RETRY_AFTER`]
    /// has passed and counts the record as dropped.
    pub fn accept(&mut self, line: &str, now: SystemTime) {
        if self.is_disabled(now) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // A failure already discarded the file it failed on, so the first
        // write after the pause opens a fresh one.
        let lost = self.dropped.swap(0, Ordering::Relaxed);
        let mut result = self.files.write(line, now).map(|_| ());
        if result.is_ok() && lost > 0 {
            let notice = dropped_record(&self.stamp, now, lost);
            self.tap.publish(Arc::from(notice.as_str()));
            result = self.files.write(&notice, now).map(|_| ());
        }
        if let Err(error) = result {
            self.dropped.fetch_add(lost + 1, Ordering::Relaxed);
            self.files.discard();
            self.disabled_until = Some(now + RETRY_AFTER);
            (self.warn)(&format!(
                "horde: the log file in {} is disabled for a minute: {error}",
                self.files.set().dir.display()
            ));
        }
    }
}

/// The bounded hand-off from the layer to the writer thread.
fn channel() -> (SyncSender<Message>, Receiver<Message>) {
    std::sync::mpsc::sync_channel(CHANNEL_CAPACITY)
}

/// Hands the writer a flush marker and waits (up to `timeout`) for it to come
/// back: `false` when the writer is gone or too slow.
fn flush_through(sender: &SyncSender<Message>, timeout: Duration) -> bool {
    let (done, wait) = std::sync::mpsc::channel();
    sender.send(Message::Flush(done)).is_ok() && wait.recv_timeout(timeout).is_ok()
}

/// A process's file logging: the writer thread, the tap, and what the layer
/// needs to stamp records.
#[derive(Debug, Clone)]
pub struct FileLogging {
    stamp: Arc<Stamp>,
    tap: LogTap,
    sender: SyncSender<Message>,
    dropped: Arc<AtomicU64>,
    level: String,
    dir: PathBuf,
    binary: String,
}

impl FileLogging {
    /// Starts file logging for `binary` into `dir`: validates the filter,
    /// and spawns the writer thread. Nothing is created on disk until the
    /// first record.
    ///
    /// # Errors
    ///
    /// The filter does not parse, or the writer thread cannot start.
    pub fn start(
        dir: &Path,
        binary: &str,
        settings: &LogSettings,
        identity: &ServiceIdentity,
    ) -> Result<Self, String> {
        tracing_subscriber::EnvFilter::try_new(&settings.level)
            .map_err(|error| format!("{:?} is not a log filter: {error}", settings.level))?;
        let stamp = Arc::new(Stamp::new(new_boot()));
        let tap = LogTap::default();
        let dropped = Arc::new(AtomicU64::new(0));
        let header_stamp = Arc::clone(&stamp);
        let (service, principal) = (identity.service().to_owned(), identity.principal());
        let files = RotatingFiles::new(FileSet {
            dir: dir.to_path_buf(),
            prefix: binary.to_owned(),
            boot: Some(stamp.boot.clone()),
            max_file_bytes: settings.max_file_bytes,
            max_files: settings.max_files,
            max_age: settings.max_age,
        })
        .with_header(move |now| {
            let mut fields = Map::new();
            fields.insert("service".to_owned(), Value::String(service.clone()));
            if let Some(principal) = &principal {
                fields.insert("principal".to_owned(), Value::String(principal.clone()));
            }
            system_record(&header_stamp, now, "log.opened", fields)
        });
        let (sender, receiver) = channel();
        let state = WriterState::new(files, Arc::clone(&stamp), tap.clone(), Arc::clone(&dropped));
        std::thread::Builder::new()
            .name("horde-log-writer".to_owned())
            .spawn(move || writer_loop(&receiver, state))
            .map_err(|error| format!("could not start the log writer: {error}"))?;
        Ok(Self {
            stamp,
            tap,
            sender,
            dropped,
            level: settings.level.clone(),
            dir: dir.to_path_buf(),
            binary: binary.to_owned(),
        })
    }

    /// The tracing layer that feeds the file and the tap, filtered by the
    /// settings' level.
    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + 'static + use<S>
    where
        S: Subscriber + for<'span> LookupSpan<'span> + Send + Sync,
    {
        // Validated in `start`, so the lossy parse loses nothing.
        let filter = tracing_subscriber::EnvFilter::builder().parse_lossy(&self.level);
        JsonLayer {
            stamp: Arc::clone(&self.stamp),
            tap: self.tap.clone(),
            sender: self.sender.clone(),
            dropped: Arc::clone(&self.dropped),
        }
        .with_filter(filter)
    }

    /// The tap every record is published to.
    #[must_use]
    pub fn tap(&self) -> &LogTap {
        &self.tap
    }

    /// This process's boot id.
    #[must_use]
    pub fn boot(&self) -> &str {
        self.stamp.boot()
    }

    /// The directory the files go in.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The file prefix — the binary's name.
    #[must_use]
    pub fn binary(&self) -> &str {
        &self.binary
    }

    /// A subscriber of this file layer alone, for a caller that installs
    /// it somewhere other than the global slot.
    #[must_use]
    pub fn dispatch(&self) -> tracing::Dispatch {
        use tracing_subscriber::layer::SubscriberExt as _;
        tracing::Dispatch::new(tracing_subscriber::registry().with(self.layer()))
    }

    /// Runs `f` with this file layer as the thread's subscriber, then waits
    /// for what it wrote to reach the file: how a library or test writes
    /// real records without claiming the process's global subscriber.
    pub fn with_default<R>(&self, f: impl FnOnce() -> R) -> R {
        let result = tracing::dispatcher::with_default(&self.dispatch(), f);
        // #[gamma::skip(literal.int_decrement, literal.int_increment, literal.int_to_one, reason = "a deadline for a writer that answers in milliseconds: no test can tell one, four, five or six seconds apart")]
        self.flush(Duration::from_secs(5));
        result
    }

    /// Records this as the running process's file logging — the binaries
    /// do, once, when they install it — so the process's own serving paths
    /// can follow its tap. A second registration is ignored.
    pub fn register_process(&self) {
        let _ = PROCESS.set(self.clone());
    }

    /// Waits (up to `timeout`) until every record handed over so far is
    /// written. Binaries call it on the way out; tests call it before
    /// reading the files back.
    pub fn flush(&self, timeout: Duration) -> bool {
        flush_through(&self.sender, timeout)
    }
}

/// How long a binary's last act waits for its file to take what is queued.
pub const EXIT_FLUSH: Duration = Duration::from_secs(2);

/// A binary's last act, handing `outcome` back: what the file still holds
/// queued is written (waiting at most [`EXIT_FLUSH`]), then export is
/// flushed. Blocks; run it off an async runtime's worker threads. The export
/// verdict is discarded on purpose — a dead collector must not change an
/// exit code.
pub fn conclude<T>(outcome: T, file: Option<&FileLogging>, telemetry: Option<Telemetry>) -> T {
    if let Some(file) = file {
        file.flush(EXIT_FLUSH);
    }
    if let Some(telemetry) = telemetry {
        let _ = telemetry.shutdown();
    }
    outcome
}

/// The running process's file logging, once its binary registered it.
static PROCESS: std::sync::OnceLock<FileLogging> = std::sync::OnceLock::new();

/// The file logging the running binary installed, if it installed one.
/// `None` in libraries and in tests, whose processes are not daemons.
#[must_use]
pub fn process_logging() -> Option<&'static FileLogging> {
    PROCESS.get()
}

fn writer_loop(receiver: &Receiver<Message>, mut state: WriterState) {
    while let Ok(message) = receiver.recv() {
        match message {
            Message::Line(line) => state.accept(&line, SystemTime::now()),
            Message::Flush(done) => {
                let _ = done.send(());
            }
        }
    }
}

/// A span's fields, kept in its extensions for the records inside it.
#[derive(Debug, Default)]
struct SpanFields(Map<String, Value>);

/// Collects a record's fields as JSON values.
struct Visitor<'a>(&'a mut Map<String, Value>);

impl Visit for Visitor<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_owned(), Value::String(format!("{value:?}")));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0
            .insert(field.name().to_owned(), Value::String(value.to_owned()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().to_owned(), Value::from(value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().to_owned(), Value::from(value));
    }

    fn record_f64(&mut self, field: &Field, value: f64) {
        self.0.insert(field.name().to_owned(), Value::from(value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().to_owned(), Value::Bool(value));
    }
}

/// The layer itself; [`FileLogging::layer`] hands it out filtered.
struct JsonLayer {
    stamp: Arc<Stamp>,
    tap: LogTap,
    sender: SyncSender<Message>,
    dropped: Arc<AtomicU64>,
}

/// The two span-field conventions the reader relies on, hoisted to the top
/// of a record.
const HOISTED: [&str; 2] = ["agent", "session"];

impl<S> Layer<S> for JsonLayer
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let mut fields = Map::new();
        attrs.record(&mut Visitor(&mut fields));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields(fields));
        }
    }

    fn on_record(&self, id: &Id, values: &SpanRecord<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<SpanFields>()
        {
            values.record(&mut Visitor(&mut fields.0));
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let line = self.serialize(event, &ctx);
        if event.metadata().target() != QUIET_TARGET {
            self.tap.publish(Arc::from(line.as_str()));
        }
        match self.sender.try_send(Message::Line(line)) {
            Ok(()) => {}
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl JsonLayer {
    fn serialize<S>(&self, event: &Event<'_>, ctx: &Context<'_, S>) -> String
    where
        S: Subscriber + for<'span> LookupSpan<'span>,
    {
        let metadata = event.metadata();
        let mut fields = Map::new();
        event.record(&mut Visitor(&mut fields));
        let msg = match fields.remove("message") {
            Some(Value::String(text)) => text,
            Some(other) => other.to_string(),
            None => String::new(),
        };
        let mut hoisted = Map::new();
        let mut spans = Vec::new();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                let mut entry = Map::new();
                entry.insert("name".to_owned(), Value::String(span.name().to_owned()));
                if let Some(own) = span.extensions().get::<SpanFields>() {
                    for (key, value) in &own.0 {
                        if HOISTED.contains(&key.as_str()) {
                            hoisted.insert(key.clone(), value.clone());
                        }
                        entry.insert(key.clone(), value.clone());
                    }
                }
                spans.push(Value::Object(entry));
            }
        }
        // The event's own fields outrank its spans'.
        for key in HOISTED {
            if let Some(value) = fields.get(key) {
                hoisted.insert(key.to_owned(), value.clone());
            }
        }
        let mut record = Map::new();
        record.insert("ts".to_owned(), Value::String(format_ts(SystemTime::now())));
        record.insert(
            "level".to_owned(),
            Value::String(metadata.level().to_string()),
        );
        record.insert(
            "target".to_owned(),
            Value::String(metadata.target().to_owned()),
        );
        record.insert("msg".to_owned(), Value::String(msg));
        record.insert("fields".to_owned(), Value::Object(fields));
        record.insert("spans".to_owned(), Value::Array(spans));
        for (key, value) in hoisted {
            let text = match value {
                Value::String(text) => text,
                other => other.to_string(),
            };
            record.insert(key, Value::String(text));
        }
        record.insert("boot".to_owned(), Value::String(self.stamp.boot.clone()));
        record.insert("seq".to_owned(), Value::from(self.stamp.next()));
        serialize_bounded(record)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime};

    use serde_json::Value;

    use super::{
        CHANNEL_CAPACITY, FileLogging, JsonLayer, Message, RETRY_AFTER, Stamp, WriterState,
        boot_from, channel, flush_through, new_boot,
    };
    use crate::files::{FileSet, RotatingFiles, list};
    use crate::pipeline::ServiceIdentity;
    use crate::record::Record;
    use crate::settings::LogSettings;
    use crate::tap::{LogTap, QUIET_TARGET};

    fn records_in(dir: &std::path::Path) -> Vec<Record> {
        list(dir, None)
            .iter()
            .flat_map(|file| {
                std::fs::read_to_string(&file.path)
                    .unwrap_or_default()
                    .lines()
                    .filter_map(Record::parse)
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn logging(dir: &std::path::Path, level: &str) -> FileLogging {
        let settings = LogSettings {
            level: level.to_owned(),
            ..LogSettings::daemon()
        };
        FileLogging::start(
            dir,
            "horde-test",
            &settings,
            &ServiceIdentity::new("horde-test").with_principal(Some("worker:w1".to_owned())),
        )
        .expect("start")
    }

    #[test]
    fn each_record_is_one_json_object_with_boot_seq_and_hoisted_conventions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = logging(dir.path(), "info");
        logging.with_default(|| {
            let span = tracing::info_span!("pump", session = "s1", agent = "a1", pump = 7_u64);
            let _entered = span.enter();
            tracing::info!(target: "horde_worker", turns = 3_u64, "turn ended");
            tracing::debug!("below the filter");
        });
        assert!(logging.flush(Duration::from_secs(5)));

        let records = records_in(dir.path());
        assert_eq!(
            records.len(),
            2,
            "the header and the info record: {records:?}"
        );
        let header = &records[0];
        assert_eq!(header.message(), "log.opened");
        assert_eq!(header.value["fields"]["service"], "horde-test");
        assert_eq!(header.value["fields"]["principal"], "worker:w1");
        // The header is a whole record of its own, under this crate's target.
        assert_eq!(header.value["level"], "INFO");
        assert_eq!(header.value["target"], "horde_observe");
        assert_eq!(header.value["spans"], serde_json::json!([]));
        assert_eq!(header.value["boot"], logging.boot());
        // The header is stamped when the first record opens the file, so it
        // follows that record in the count.
        assert_eq!(header.value["seq"], 2);

        let turn = &records[1];
        assert_eq!(turn.message(), "turn ended");
        assert_eq!(turn.level(), "INFO");
        assert_eq!(turn.text("target"), Some("horde_worker"));
        assert_eq!(turn.value["fields"]["turns"], 3);
        assert_eq!(turn.text("agent"), Some("a1"));
        assert_eq!(turn.text("session"), Some("s1"));
        assert_eq!(turn.value["level"], "INFO");
        assert_eq!(turn.value["spans"][0]["name"], "pump");
        assert_eq!(turn.value["spans"][0]["session"], "s1");
        assert_eq!(turn.value["spans"][0]["pump"], 7);
        assert!(
            turn.value.get("pump").is_none(),
            "only the conventions are hoisted: {:?}",
            turn.value
        );
        assert_eq!(turn.key.boot, logging.boot());
        assert_eq!(turn.value["seq"], 1, "one counter numbers both");

        let files = list(dir.path(), None);
        assert_eq!(files.len(), 1, "{files:?}");
        assert_eq!(
            files[0].boot.as_deref(),
            Some(logging.boot()),
            "the file is named for its boot"
        );
    }

    #[test]
    fn an_events_own_fields_outrank_its_spans_and_late_records_update_spans() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = logging(dir.path(), "info");
        logging.with_default(|| {
            let span = tracing::info_span!("rpc", session = tracing::field::Empty);
            let _entered = span.enter();
            span.record("session", "s-late");
            tracing::info!(agent = "a-own", "named");
        });
        assert!(logging.flush(Duration::from_secs(5)));
        let records = records_in(dir.path());
        let named = records
            .iter()
            .find(|r| r.message() == "named")
            .expect("named");
        assert_eq!(named.text("session"), Some("s-late"));
        assert_eq!(named.text("agent"), Some("a-own"));
    }

    #[tokio::test]
    async fn the_tap_sees_records_except_the_quiet_target() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = logging(dir.path(), "info");
        let mut tap = logging.tap().subscribe();
        logging.with_default(|| {
            tracing::info!(target: QUIET_TARGET, "serving a query");
            tracing::info!("visible");
        });
        let seen = tokio::time::timeout(Duration::from_secs(5), tap.recv())
            .await
            .expect("the record reaches the tap")
            .expect("one record");
        assert!(seen.contains("visible"), "{seen}");
        assert!(
            tap.try_recv().is_err(),
            "the quiet record never reached the tap"
        );
        assert!(logging.flush(Duration::from_secs(5)));
        assert!(
            records_in(dir.path())
                .iter()
                .any(|r| r.message() == "serving a query"),
            "but it is in the file"
        );
    }

    #[test]
    fn sequences_start_at_one_and_count_up() {
        let stamp = Stamp::new("b".to_owned());
        assert_eq!((stamp.next(), stamp.next(), stamp.next()), (1, 2, 3));
        assert_eq!(stamp.boot(), "b");
    }

    /// Every field type keeps its JSON type.
    #[test]
    fn numbers_floats_and_booleans_keep_their_types() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = logging(dir.path(), "info");
        logging.with_default(|| {
            tracing::info!(signed = -3_i64, ratio = 0.5_f64, ok = true, "typed");
        });
        let records = records_in(dir.path());
        let typed = records
            .iter()
            .find(|r| r.message() == "typed")
            .expect("typed");
        assert_eq!(typed.value["fields"]["signed"], -3);
        assert_eq!(typed.value["fields"]["ratio"], 0.5);
        assert_eq!(typed.value["fields"]["ok"], true);
        assert_eq!(logging.binary(), "horde-test");
        assert_eq!(logging.dir(), dir.path());
    }

    /// The binary's registration is what its serving paths follow; only the
    /// first registration counts.
    #[test]
    fn the_process_logging_is_the_first_registered() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = logging(dir.path(), "info");
        let second = logging(dir.path(), "info");
        first.register_process();
        second.register_process();
        let registered = super::process_logging().expect("registered");
        assert_eq!(registered.boot(), first.boot());
        assert_ne!(registered.boot(), second.boot());
    }

    /// A binary's last act writes out everything still queued before it
    /// hands the outcome back.
    #[test]
    fn concluding_writes_out_the_backlog() {
        let dir = tempfile::tempdir().expect("tempdir");
        let logging = logging(dir.path(), "info");
        let queued = 4000;
        tracing::dispatcher::with_default(&logging.dispatch(), || {
            for n in 0..queued {
                tracing::info!(padding = %"p".repeat(512), "burst {n}");
            }
        });
        assert_eq!(super::conclude(7, Some(&logging), None), 7);
        let written = records_in(dir.path())
            .iter()
            .filter(|record| record.message().starts_with("burst "))
            .count();
        assert_eq!(written, queued);
        assert_eq!(super::conclude("no file", None, None), "no file");
    }

    #[test]
    fn an_unparseable_level_is_refused_at_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        let settings = LogSettings {
            level: "info,=[".to_owned(),
            ..LogSettings::daemon()
        };
        assert!(
            FileLogging::start(dir.path(), "x", &settings, &ServiceIdentity::new("x")).is_err()
        );
    }

    #[test]
    fn boots_are_sixteen_hex_digits_and_differ() {
        let (a, b) = (new_boot(), new_boot());
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, b);
    }

    /// The random value names the boot; without one, the clock does.
    #[test]
    fn a_boot_is_the_random_value_or_else_the_clock() {
        let at = SystemTime::UNIX_EPOCH + Duration::from_nanos(0x1234_5678);
        assert_eq!(boot_from::<()>(Ok(0xabc), at), "0000000000000abc");
        assert_eq!(boot_from(Err(()), at), "0000000012345678");
        assert_eq!(
            boot_from(Err(()), SystemTime::UNIX_EPOCH - Duration::from_secs(1)),
            "0000000000000000"
        );
    }

    /// The writer thread carries its name, for an operator reading `top`.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_writer_thread_is_named() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _logging = logging(dir.path(), "info");
        let names = || -> Vec<String> {
            std::fs::read_dir("/proc/self/task")
                .expect("tasks")
                .filter_map(Result::ok)
                .filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok())
                .collect()
        };
        // The thread names itself once it runs; the kernel keeps fifteen
        // bytes of the name.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !names()
            .iter()
            .any(|name| name.trim_end() == "horde-log-write")
        {
            assert!(
                std::time::Instant::now() < deadline,
                "no thread is named for the writer: {:?}",
                names()
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The hand-off holds exactly its capacity; the next record is refused,
    /// not queued.
    #[test]
    fn the_hand_off_holds_its_capacity_and_no_more() {
        let (sender, _receiver) = channel();
        for n in 0..CHANNEL_CAPACITY {
            assert!(
                sender.try_send(Message::Line(String::new())).is_ok(),
                "record {n} fits"
            );
        }
        assert!(matches!(
            sender.try_send(Message::Line(String::new())),
            Err(std::sync::mpsc::TrySendError::Full(_))
        ));
    }

    /// A flush is answered by the writer: a gone writer is `false` at
    /// once, a silent one `false` at the deadline, an answering one `true`.
    #[test]
    fn a_flush_needs_the_writer_to_answer() {
        let (gone, receiver) = channel();
        drop(receiver);
        assert!(!flush_through(&gone, Duration::from_secs(5)));

        let (silent, held) = channel();
        assert!(!flush_through(&silent, Duration::from_millis(20)));
        drop(held);

        let (answering, receiver) = channel();
        let writer = std::thread::spawn(move || {
            if let Ok(Message::Flush(done)) = receiver.recv() {
                let _ = done.send(());
            }
        });
        assert!(flush_through(&answering, Duration::from_secs(5)));
        writer.join().expect("the writer answered");
    }

    /// A record the writer cannot take is counted, not lost silently.
    #[test]
    fn a_record_the_writer_cannot_take_is_counted() {
        use tracing_subscriber::layer::SubscriberExt as _;
        let (sender, receiver) = channel();
        drop(receiver);
        let dropped = Arc::new(AtomicU64::new(0));
        let layer = JsonLayer {
            stamp: Arc::new(Stamp::new("b".to_owned())),
            tap: LogTap::default(),
            sender,
            dropped: Arc::clone(&dropped),
        };
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
        tracing::dispatcher::with_default(&dispatch, || {
            tracing::info!("one");
            tracing::info!("two");
        });
        assert_eq!(dropped.load(Ordering::Relaxed), 2);
    }

    fn state(dir: &std::path::Path) -> (WriterState, Arc<AtomicU64>) {
        let dropped = Arc::new(AtomicU64::new(0));
        let files = RotatingFiles::new(FileSet {
            dir: dir.to_path_buf(),
            prefix: "w".to_owned(),
            boot: Some("b".to_owned()),
            max_file_bytes: 1 << 20,
            max_files: 4,
            max_age: None,
        });
        let state = WriterState::new(
            files,
            Arc::new(Stamp::new("b".to_owned())),
            LogTap::default(),
            Arc::clone(&dropped),
        )
        .with_warn(|_| {});
        (state, dropped)
    }

    #[test]
    fn dropped_records_are_counted_and_announced_once_the_writer_catches_up() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (state, dropped) = state(dir.path());
        let tap = LogTap::default();
        let mut announced = tap.subscribe();
        let mut state = WriterState {
            tap: tap.clone(),
            ..state
        };
        dropped.store(5, Ordering::Relaxed);
        state.accept(r#"{"ts":"t","msg":"next"}"#, SystemTime::now());
        assert!(
            announced
                .try_recv()
                .is_ok_and(|line| line.contains("log.dropped")),
            "the notice is followable too"
        );
        let records = records_in(dir.path());
        let notice = records
            .iter()
            .find(|r| r.message() == "log.dropped")
            .expect("notice");
        assert_eq!(notice.value["fields"]["count"], 5);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        state.accept(r#"{"ts":"t","msg":"again"}"#, SystemTime::now());
        assert_eq!(
            records_in(dir.path())
                .iter()
                .filter(|r| r.message() == "log.dropped")
                .count(),
            1,
            "announced once"
        );

        // A single lost record is announced too.
        dropped.store(1, Ordering::Relaxed);
        state.accept(r#"{"ts":"t","msg":"third"}"#, SystemTime::now());
        let counts: Vec<Value> = records_in(dir.path())
            .iter()
            .filter(|r| r.message() == "log.dropped")
            .map(|r| r.value["fields"]["count"].clone())
            .collect();
        assert_eq!(counts, [Value::from(5), Value::from(1)]);
    }

    /// The warnings [`WriterState::with_warn`] routed here, from every test.
    static WARNED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

    fn remember_warning(text: &str) {
        if let Ok(mut warned) = WARNED.lock() {
            warned.push(text.to_owned());
        }
    }

    /// A file whose writes fail is let go: after the pause, the next
    /// record opens a fresh file instead of failing on the same one.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_file_that_fails_is_let_go_for_a_fresh_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        // The first file the family opens is a device that refuses writes.
        std::os::unix::fs::symlink("/dev/full", dir.path().join("w.b.1.jsonl")).expect("link");
        let (mut state, _dropped) = state(dir.path());
        let now = SystemTime::now();
        state.accept(r#"{"ts":"t","msg":"lost"}"#, now);
        assert!(state.is_disabled(now));
        state.accept(r#"{"ts":"t","msg":"back"}"#, now + RETRY_AFTER);
        assert!(
            records_in(dir.path()).iter().any(|r| r.message() == "back"),
            "the retry wrote to a fresh file"
        );
    }

    #[test]
    fn an_io_failure_disables_the_file_then_retries_a_fresh_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let blocker = dir.path().join("logs");
        std::fs::write(&blocker, "a file where the directory should be").expect("write");
        let (state, dropped) = state(&blocker);
        let mut state = state.with_warn(remember_warning);
        let now = SystemTime::now();
        state.accept(r#"{"ts":"t","msg":"lost"}"#, now);
        assert!(state.is_disabled(now));
        let warned = WARNED.lock().map(|w| w.clone()).unwrap_or_default();
        let expected = format!(
            "horde: the log file in {} is disabled for a minute: ",
            blocker.display()
        );
        assert!(
            warned.iter().any(|w| w.starts_with(&expected)),
            "{warned:?}"
        );
        assert_eq!(dropped.load(Ordering::Relaxed), 1);
        state.accept(
            r#"{"ts":"t","msg":"also lost"}"#,
            now + Duration::from_secs(1),
        );
        assert_eq!(
            dropped.load(Ordering::Relaxed),
            2,
            "disabled records are counted"
        );

        std::fs::remove_file(&blocker).expect("unblock");
        let later = now + RETRY_AFTER;
        assert!(!state.is_disabled(later));
        state.accept(r#"{"ts":"t","msg":"back"}"#, later);
        let records = records_in(&blocker);
        assert!(records.iter().any(|r| r.message() == "back"));
        let notice = records
            .iter()
            .find(|r| r.message() == "log.dropped")
            .expect("notice");
        assert_eq!(notice.value["fields"]["count"], 2);
    }
}
