//! A scripted [`CommandRunner`] used to drive real gate code from captured
//! tool output.
//!
//! This is not a mock: it is a second implementation of the same trait, and the
//! output it replays comes from `fixtures/`, recorded from the pinned tools.
//! Gate logic under test is the production logic.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::command::{CommandOutput, CommandRunner, CommandSpec};
use crate::gate::{Gate, GateContext, GateOutcome};

/// A whole-workspace context rooted at a fixed, obviously-fake path.
///
/// Gate tests that only build commands never touch the filesystem, so the root
/// exists to be matched against, not to be read.
pub(crate) fn repo_ctx() -> GateContext {
    GateContext::new(Path::new("/repo"), Vec::new())
}

/// A whole-workspace context rooted at a real directory.
pub(crate) fn ctx_at(root: &Path) -> GateContext {
    GateContext::new(root, Vec::new())
}

/// A command that exited with `code`, printing `stdout` and nothing on stderr.
pub(crate) fn exited(code: i32, stdout: &str) -> CommandOutput {
    CommandOutput::new(Some(code), stdout, "")
}

/// A command that exited with `code`, printing `stderr` and nothing on stdout.
pub(crate) fn failed(code: i32, stderr: &str) -> CommandOutput {
    CommandOutput::new(Some(code), "", stderr)
}

/// Runs a gate, so a test module need not import the [`Gate`] trait itself.
pub(crate) fn run_gate(
    gate: &impl Gate,
    ctx: &GateContext,
    runner: &ScriptedRunner,
) -> GateOutcome {
    gate.run(ctx, runner)
}

/// Replays a fixed script of command results and records what was asked of it.
#[derive(Debug)]
pub(crate) struct ScriptedRunner {
    responses: RefCell<VecDeque<Result<CommandOutput, String>>>,
    repeat_last: bool,
    calls: RefCell<Vec<CommandSpec>>,
    writes: Vec<(usize, PathBuf, String)>,
}

impl ScriptedRunner {
    /// Replays `responses` in order; every command is treated as launchable.
    pub(crate) fn new(responses: Vec<CommandOutput>) -> Self {
        Self::scripted(responses.into_iter().map(Ok).collect())
    }

    /// Replays `responses` in order, including launch failures.
    pub(crate) fn scripted(responses: Vec<Result<CommandOutput, String>>) -> Self {
        Self {
            responses: RefCell::new(responses.into()),
            repeat_last: false,
            calls: RefCell::new(Vec::new()),
            writes: Vec::new(),
        }
    }

    /// Writes `contents` to `path` when the `call`th command (from 0) runs,
    /// the way a tool leaves an artifact behind for its gate to read.
    pub(crate) fn writing(mut self, call: usize, path: &Path, contents: &str) -> Self {
        self.writes
            .push((call, path.to_path_buf(), contents.to_owned()));
        self
    }

    /// Fails every launch with `message`, however many times it is asked.
    pub(crate) fn failing(message: &str) -> Self {
        Self {
            responses: RefCell::new(VecDeque::from(vec![Err(message.to_owned())])),
            repeat_last: true,
            calls: RefCell::new(Vec::new()),
            writes: Vec::new(),
        }
    }

    /// Returns `output` for every command, however many times it is asked.
    pub(crate) fn always(output: CommandOutput) -> Self {
        Self {
            responses: RefCell::new(VecDeque::from(vec![Ok(output)])),
            repeat_last: true,
            calls: RefCell::new(Vec::new()),
            writes: Vec::new(),
        }
    }

    /// Every command that was run, in order.
    pub(crate) fn calls(&self) -> Vec<CommandSpec> {
        self.calls.borrow().clone()
    }

    /// Every command that was run, rendered as a shell-like line.
    pub(crate) fn command_lines(&self) -> Vec<String> {
        self.calls
            .borrow()
            .iter()
            .map(CommandSpec::display)
            .collect()
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(&self, spec: &CommandSpec) -> Result<CommandOutput, String> {
        let call = self.calls.borrow().len();
        self.calls.borrow_mut().push(spec.clone());
        for (at, path, contents) in &self.writes {
            if *at == call {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).expect("scripted artifact directory");
                }
                std::fs::write(path, contents).expect("scripted artifact");
            }
        }
        let mut responses = self.responses.borrow_mut();
        if self.repeat_last && responses.len() == 1 {
            let response = responses.front().expect("length checked").clone();
            return response;
        }
        responses.pop_front().unwrap_or_else(|| {
            panic!(
                "ScriptedRunner ran out of responses at `{}`",
                spec.display()
            )
        })
    }
}
