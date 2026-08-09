//! A scripted [`CommandRunner`] used to drive real gate code from captured
//! tool output.
//!
//! This is not a mock: it is a second implementation of the same trait, and the
//! output it replays comes from `fixtures/`, recorded from the pinned tools.
//! Gate logic under test is the production logic.

use std::cell::RefCell;
use std::collections::VecDeque;

use crate::command::{CommandOutput, CommandRunner, CommandSpec};

/// Replays a fixed script of command results and records what was asked of it.
#[derive(Debug)]
pub(crate) struct ScriptedRunner {
    responses: RefCell<VecDeque<Result<CommandOutput, String>>>,
    repeat_last: bool,
    calls: RefCell<Vec<CommandSpec>>,
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
        }
    }

    /// Fails every launch with `message`, however many times it is asked.
    pub(crate) fn failing(message: &str) -> Self {
        Self {
            responses: RefCell::new(VecDeque::from(vec![Err(message.to_owned())])),
            repeat_last: true,
            calls: RefCell::new(Vec::new()),
        }
    }

    /// Returns `output` for every command, however many times it is asked.
    pub(crate) fn always(output: CommandOutput) -> Self {
        Self {
            responses: RefCell::new(VecDeque::from(vec![Ok(output)])),
            repeat_last: true,
            calls: RefCell::new(Vec::new()),
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
        self.calls.borrow_mut().push(spec.clone());
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
