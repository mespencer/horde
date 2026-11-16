//! The process-execution seam.
//!
//! Every gate reaches the outside world through [`CommandRunner`]. Production
//! uses [`ProcessRunner`]; tests use a scripted runner that replays captured
//! tool output, so gate logic is exercised for real without a toolchain.

use std::path::{Path, PathBuf};
use std::process::Command;

/// A fully-resolved external command: what to run, with which arguments, where.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CommandSpec {
    /// Program name or path, resolved through `PATH`.
    pub program: String,
    /// Arguments, already split (never re-parsed by a shell).
    pub args: Vec<String>,
    /// Working directory the command runs in.
    pub cwd: PathBuf,
    /// Variables set for this command on top of the inherited environment.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<(String, String)>,
}

impl CommandSpec {
    /// Builds a spec from borrowed parts.
    pub fn new(program: &str, args: &[&str], cwd: &Path) -> Self {
        Self {
            program: program.to_owned(),
            args: args.iter().map(|a| (*a).to_owned()).collect(),
            cwd: cwd.to_path_buf(),
            env: Vec::new(),
        }
    }

    /// Builds a spec from arguments that were assembled at runtime.
    pub fn owned(program: &str, args: Vec<String>, cwd: &Path) -> Self {
        Self {
            program: program.to_owned(),
            args,
            cwd: cwd.to_path_buf(),
            env: Vec::new(),
        }
    }

    /// The same command with `key` set to `value` in its environment.
    #[must_use]
    pub fn with_env(mut self, key: &str, value: impl Into<String>) -> Self {
        self.env.push((key.to_owned(), value.into()));
        self
    }

    /// Renders the command the way a human would retype it.
    ///
    /// This string goes into logs and failure messages, so "local = CI"
    /// (docs/TESTING.md §5) is a copy-paste away.
    pub fn display(&self) -> String {
        let mut out = String::new();
        for (key, value) in &self.env {
            out.push_str(key);
            out.push('=');
            out.push_str(value);
            out.push(' ');
        }
        out.push_str(&self.program);
        for arg in &self.args {
            out.push(' ');
            out.push_str(arg);
        }
        out
    }
}

/// What an external command produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    /// Exit status, or `None` when the process died from a signal.
    pub status: Option<i32>,
    /// Captured standard output.
    pub stdout: String,
    /// Captured standard error.
    pub stderr: String,
}

impl CommandOutput {
    /// Builds an output value; mainly useful for tests and fixtures.
    pub fn new(status: Option<i32>, stdout: &str, stderr: &str) -> Self {
        Self {
            status,
            stdout: stdout.to_owned(),
            stderr: stderr.to_owned(),
        }
    }

    /// True only for a clean exit code of zero.
    pub fn success(&self) -> bool {
        self.status == Some(0)
    }

    /// Both streams in the order a terminal would have interleaved them.
    pub fn combined(&self) -> String {
        let mut out = String::from(&self.stdout);
        out.push_str(&self.stderr);
        out
    }
}

/// Runs external commands.
///
/// The `Err` variant means the process could not be launched at all (missing
/// binary, permission denied) — never "the command reported a problem". That
/// distinction is what keeps `Error` separable from `Fail` in a gate outcome.
pub trait CommandRunner {
    /// Runs `spec` to completion, capturing both streams.
    ///
    /// # Errors
    ///
    /// Returns a human-readable message when the process could not be spawned.
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, String>;
}

/// The real runner, backed by [`std::process::Command`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ProcessRunner;

impl CommandRunner for ProcessRunner {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, String> {
        let output = Command::new(&spec.program)
            .args(&spec.args)
            .envs(spec.env.iter().map(|(key, value)| (key, value)))
            .current_dir(&spec.cwd)
            .output()
            .map_err(|err| format!("could not launch `{}`: {err}", spec.display()))?;
        Ok(CommandOutput {
            status: output.status.code(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_joins_program_and_arguments() {
        let spec = CommandSpec::new("cargo", &["fmt", "--check"], Path::new("/tmp"));
        assert_eq!(spec.display(), "cargo fmt --check");
    }

    #[test]
    fn display_of_bare_program_has_no_trailing_space() {
        let spec = CommandSpec::new("cargo", &[], Path::new("/tmp"));
        assert_eq!(spec.display(), "cargo");
    }

    #[test]
    fn display_puts_the_environment_first_the_way_a_shell_would_take_it() {
        let spec = CommandSpec::new("cargo", &["gamma"], Path::new("/tmp"))
            .with_env("A", "1")
            .with_env("B", "two");
        assert_eq!(spec.display(), "A=1 B=two cargo gamma");
    }

    #[test]
    fn process_runner_sets_the_command_s_environment() {
        let spec = CommandSpec::new(
            "/bin/sh",
            &["-c", "printf %s \"$HORDE_SPEC_ENV\""],
            Path::new("."),
        )
        .with_env("HORDE_SPEC_ENV", "seen");
        let output = ProcessRunner.run(&spec).expect("/bin/sh is present");
        assert_eq!(output.stdout, "seen");
    }

    #[test]
    fn success_is_true_only_for_exit_code_zero() {
        assert!(CommandOutput::new(Some(0), "", "").success());
        assert!(!CommandOutput::new(Some(1), "", "").success());
        assert!(!CommandOutput::new(None, "", "").success());
    }

    #[test]
    fn combined_concatenates_stdout_then_stderr() {
        let output = CommandOutput::new(Some(0), "out", "err");
        assert_eq!(output.combined(), "outerr");
    }

    #[test]
    fn process_runner_captures_stdout_and_exit_code() {
        let spec = CommandSpec::new("/bin/echo", &["hello"], Path::new("."));
        let output = ProcessRunner
            .run(&spec)
            .expect("/bin/echo is present on every supported platform");
        assert_eq!(output.status, Some(0));
        assert_eq!(output.stdout, "hello\n");
        assert_eq!(output.stderr, "");
    }

    #[test]
    fn process_runner_reports_nonzero_exit_without_erroring() {
        let spec = CommandSpec::new("/bin/sh", &["-c", "echo bad >&2; exit 3"], Path::new("."));
        let output = ProcessRunner.run(&spec).expect("/bin/sh is present");
        assert_eq!(output.status, Some(3));
        assert_eq!(output.stderr, "bad\n");
    }

    #[test]
    fn process_runner_errors_when_the_program_is_missing() {
        let spec = CommandSpec::new("horde-no-such-binary", &[], Path::new("."));
        let message = ProcessRunner
            .run(&spec)
            .expect_err("a missing binary cannot be launched");
        assert!(
            message.contains("horde-no-such-binary"),
            "message should name the program: {message}"
        );
    }

    #[test]
    fn process_runner_honours_the_working_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        let spec = CommandSpec::new("/bin/sh", &["-c", "pwd"], dir.path());
        let output = ProcessRunner.run(&spec).expect("/bin/sh is present");
        let printed = std::fs::canonicalize(output.stdout.trim()).expect("printed path exists");
        let expected = std::fs::canonicalize(dir.path()).expect("temp dir exists");
        assert_eq!(printed, expected);
    }
}
