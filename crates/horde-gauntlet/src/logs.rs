//! Per-gate logs.
//!
//! A gate's verdict is only trustworthy if the evidence behind it survives, so
//! every command a gate runs — and everything it printed — lands in
//! `target/quality/<gate>.log`.

use std::cell::RefCell;
use std::fmt::Write as _;
use std::io;
use std::path::{Path, PathBuf};

use crate::command::{CommandOutput, CommandRunner, CommandSpec};

/// Wraps another runner and transcribes every command it is asked to run.
pub struct RecordingRunner<'a> {
    inner: &'a dyn CommandRunner,
    transcript: RefCell<String>,
}

impl std::fmt::Debug for RecordingRunner<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordingRunner")
            .field("transcript", &self.transcript)
            .finish_non_exhaustive()
    }
}

impl<'a> RecordingRunner<'a> {
    /// Wraps `inner`.
    pub fn new(inner: &'a dyn CommandRunner) -> Self {
        Self {
            inner,
            transcript: RefCell::new(String::new()),
        }
    }

    /// The transcript recorded so far.
    pub fn transcript(&self) -> String {
        self.transcript.borrow().clone()
    }
}

impl CommandRunner for RecordingRunner<'_> {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, String> {
        let result = self.inner.run(spec);
        let mut transcript = self.transcript.borrow_mut();
        transcript.push_str(&transcript_entry(spec, &result));
        result
    }
}

/// Renders one command and its result as a log entry.
pub fn transcript_entry(spec: &CommandSpec, result: &Result<CommandOutput, String>) -> String {
    let mut entry = String::new();
    let _ = writeln!(entry, "$ {}", spec.display());
    let _ = writeln!(entry, "# cwd: {}", spec.cwd.display());
    match result {
        Err(message) => {
            let _ = writeln!(entry, "# could not launch: {message}");
        }
        Ok(output) => {
            let status = match output.status {
                Some(code) => code.to_string(),
                None => "signal".to_owned(),
            };
            let _ = writeln!(entry, "# exit: {status}");
            push_stream(&mut entry, "stdout", &output.stdout);
            push_stream(&mut entry, "stderr", &output.stderr);
        }
    }
    entry.push('\n');
    entry
}

fn push_stream(entry: &mut String, name: &str, body: &str) {
    if body.is_empty() {
        return;
    }
    let _ = writeln!(entry, "--- {name} ---");
    entry.push_str(body);
    if !body.ends_with('\n') {
        entry.push('\n');
    }
}

/// Writes `body` to `<dir>/<name>.log`, creating `dir` if needed.
///
/// # Errors
///
/// Returns the underlying I/O error if the directory or file cannot be written.
pub fn write_log(dir: &Path, name: &str, body: &str) -> io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{name}.log"));
    std::fs::write(&path, body)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::ScriptedRunner;

    fn spec() -> CommandSpec {
        CommandSpec::new("cargo", &["fmt", "--check"], Path::new("/repo"))
    }

    #[test]
    fn entry_records_command_cwd_exit_and_both_streams() {
        let entry = transcript_entry(&spec(), &Ok(CommandOutput::new(Some(1), "out\n", "err\n")));
        assert_eq!(
            entry,
            "$ cargo fmt --check\n\
             # cwd: /repo\n\
             # exit: 1\n\
             --- stdout ---\n\
             out\n\
             --- stderr ---\n\
             err\n\
             \n"
        );
    }

    #[test]
    fn entry_omits_empty_streams() {
        let entry = transcript_entry(&spec(), &Ok(CommandOutput::new(Some(0), "", "")));
        assert!(!entry.contains("stdout"), "{entry}");
        assert!(!entry.contains("stderr"), "{entry}");
    }

    #[test]
    fn entry_terminates_a_stream_that_lacks_a_final_newline() {
        let entry = transcript_entry(&spec(), &Ok(CommandOutput::new(Some(0), "no newline", "")));
        assert!(entry.contains("no newline\n\n"), "{entry}");
    }

    #[test]
    fn entry_reports_death_by_signal_distinctly_from_an_exit_code() {
        let entry = transcript_entry(&spec(), &Ok(CommandOutput::new(None, "", "")));
        assert!(entry.contains("# exit: signal"), "{entry}");
    }

    #[test]
    fn entry_records_a_launch_failure() {
        let entry = transcript_entry(&spec(), &Err("no such binary".to_owned()));
        assert!(
            entry.contains("# could not launch: no such binary"),
            "{entry}"
        );
        assert!(!entry.contains("# exit:"), "{entry}");
    }

    #[test]
    fn recording_runner_passes_results_through_and_transcribes_every_call() {
        let inner = ScriptedRunner::new(vec![
            CommandOutput::new(Some(0), "first", ""),
            CommandOutput::new(Some(2), "second", ""),
        ]);
        let recorder = RecordingRunner::new(&inner);

        let first = recorder.run(&spec()).expect("scripted");
        let second = recorder
            .run(&CommandSpec::new("cargo", &["clippy"], Path::new("/repo")))
            .expect("scripted");

        assert_eq!(first.stdout, "first");
        assert_eq!(second.status, Some(2));
        let transcript = recorder.transcript();
        assert!(transcript.contains("$ cargo fmt --check"), "{transcript}");
        assert!(transcript.contains("$ cargo clippy"), "{transcript}");
        assert!(transcript.contains("first"), "{transcript}");
        assert!(transcript.contains("second"), "{transcript}");
    }

    #[test]
    fn recording_runner_transcribes_launch_failures_too() {
        let inner = ScriptedRunner::failing("nope");
        let recorder = RecordingRunner::new(&inner);
        let _ = recorder.run(&spec());
        assert!(recorder.transcript().contains("could not launch: nope"));
    }

    #[test]
    fn write_log_creates_the_directory_and_returns_the_path() {
        let dir = tempfile::tempdir().expect("temp dir");
        let target = dir.path().join("quality");
        let path = write_log(&target, "fmt", "body").expect("write");
        assert_eq!(path, target.join("fmt.log"));
        assert_eq!(std::fs::read_to_string(&path).expect("read"), "body");
    }
}
