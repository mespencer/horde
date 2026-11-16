//! A trace-level capture of everything this thread logs, for the rule that
//! relayed bytes — login PTY traffic, forward tunnel payloads — never reach
//! a log at any level (ADR-0015).

use std::time::Duration;

use horde_observe::reader::read_dir;
use horde_observe::settings::LogSettings;

/// Every record this thread emits while held, written by the real file
/// layer at `trace`. `#[tokio::test]`'s current-thread runtime polls every
/// spawned task on this thread, so the relays' records land here too.
pub(crate) struct Capture {
    dir: tempfile::TempDir,
    logging: horde_observe::FileLogging,
    _guard: tracing::dispatcher::DefaultGuard,
}

impl Capture {
    pub(crate) fn everything() -> Self {
        let dir = tempfile::tempdir().expect("a tempdir");
        let settings = LogSettings {
            level: "trace".to_owned(),
            ..LogSettings::daemon()
        };
        let logging = horde_observe::FileLogging::start(
            dir.path(),
            "capture",
            &settings,
            &horde_observe::ServiceIdentity::new("capture"),
        )
        .expect("the capture starts");
        let guard = tracing::dispatcher::set_default(&logging.dispatch());
        Self {
            dir,
            logging,
            _guard: guard,
        }
    }

    /// Fails if any record holds any payload, as text or as the `Debug` of
    /// its bytes — after proving, with a probe logged both ways, that the
    /// capture would have caught it.
    pub(crate) fn assert_never_logged(&self, payloads: &[&[u8]]) {
        let probe = b"capture-probe-bytes".to_vec();
        tracing::trace!(bytes = ?probe, text = "capture-probe-text", "probe");
        assert!(self.logging.flush(Duration::from_secs(5)));
        let lines: Vec<String> = read_dir(self.dir.path(), None)
            .into_iter()
            .map(|record| record.line)
            .collect();
        let found = |needle: &str| lines.iter().any(|line| line.contains(needle));
        assert!(
            found("capture-probe-text") && found(&format!("{probe:?}")),
            "the capture must be able to see both spellings: {lines:?}"
        );
        for payload in payloads {
            let text = String::from_utf8_lossy(payload);
            assert!(
                !found(&text),
                "relayed bytes {text:?} were logged: {lines:?}"
            );
            let debug = format!("{:?}", payload.to_vec());
            assert!(
                !found(&debug),
                "relayed bytes {debug} were logged: {lines:?}"
            );
        }
    }
}
