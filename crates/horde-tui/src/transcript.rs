//! Each agent's transcript: the conversation the main view shows, folded
//! from the same events as everything else.
//!
//! Where the feed is one line per event, a transcript is one *block* per
//! thing a human reads: a message to the agent, the agent's reply (streamed
//! chunks of one turn merge into one block), a tool call with its result
//! attached, a prompt and how it was settled, a child's spawn and result,
//! and the notices worth interrupting the reading for — a failure, a lost
//! worker, an interrupt. Bounded like the feed: the log is the record.

use std::collections::{BTreeMap, VecDeque};

use horde_proto::v1::{
    Actor, AgentLifecycleState, AgentOutputChannel, Event, PermissionOutcome, PlacementState,
    actor, event::Kind,
};

use crate::tree::{AgentNode, agent_name};

/// How many blocks one agent's transcript keeps.
pub const TRANSCRIPT_CAPACITY: usize = 1_000;

/// Who sent a message to an agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Speaker {
    /// A human, through a client (the id is the client's).
    Human,
    /// Another agent — usually the parent — by display name.
    Agent(String),
    /// The server on its own behalf.
    Server,
}

/// How loudly a notice speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Info,
    Warn,
    Error,
}

/// How a prompt was settled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    Allowed,
    Denied,
    /// Question answered: `question → answer` pairs, rendered.
    Answered(String),
}

/// One readable unit of a transcript.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Block {
    /// An assignment or note delivered to the agent.
    Message {
        from: Speaker,
        text: String,
        note: bool,
    },
    Assistant {
        turn: String,
        text: String,
    },
    Thinking {
        turn: String,
        text: String,
    },
    Tool {
        turn: String,
        name: String,
        /// The arguments as the harness rendered them (usually JSON).
        input: String,
        result: Option<String>,
    },
    Prompt {
        request_id: String,
        summary: String,
        settled: Option<Settled>,
    },
    /// A child agent was spawned or reported back.
    Child {
        text: String,
    },
    /// The agent reported its result.
    Reported {
        status: &'static str,
        summary: String,
    },
    Notice {
        tone: Tone,
        text: String,
    },
}

/// One agent's transcript.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transcript {
    pub blocks: VecDeque<Block>,
}

impl Transcript {
    fn push(&mut self, block: Block) {
        if self.blocks.len() >= TRANSCRIPT_CAPACITY {
            self.blocks.pop_front();
        }
        self.blocks.push_back(block);
    }

    /// Streamed text: appended to the previous block when it is the same
    /// kind of text from the same turn, a new block otherwise.
    fn stream(&mut self, turn: &str, text: &str, thinking: bool) {
        let merged = match self.blocks.back_mut() {
            Some(Block::Assistant { turn: last, text: body }) if !thinking && last == turn => {
                body.push_str(text);
                true
            }
            Some(Block::Thinking { turn: last, text: body }) if thinking && last == turn => {
                body.push_str(text);
                true
            }
            _ => false,
        };
        if merged {
            return;
        }
        let (turn, text) = (turn.to_owned(), text.to_owned());
        self.push(if thinking {
            Block::Thinking { turn, text }
        } else {
            Block::Assistant { turn, text }
        });
    }

    fn tool_call(&mut self, turn: &str, rendered: &str) {
        let (name, input) = rendered.split_once(' ').unwrap_or((rendered, ""));
        self.push(Block::Tool {
            turn: turn.to_owned(),
            name: name.to_owned(),
            input: input.to_owned(),
            result: None,
        });
    }

    /// Attaches a result to the oldest unanswered call of the same turn, or
    /// stands alone when no call is waiting for it.
    fn tool_result(&mut self, turn: &str, text: &str) {
        let waiting = self.blocks.iter_mut().rev().filter_map(|block| match block {
            Block::Tool {
                turn: call_turn,
                result: result @ None,
                ..
            } if call_turn == turn => Some(result),
            _ => None,
        });
        if let Some(slot) = waiting.last() {
            *slot = Some(text.to_owned());
            return;
        }
        self.push(Block::Tool {
            turn: turn.to_owned(),
            name: String::new(),
            input: String::new(),
            result: Some(text.to_owned()),
        });
    }

    fn settle(&mut self, request_id: &str, how: Settled) {
        for block in self.blocks.iter_mut().rev() {
            if let Block::Prompt {
                request_id: id,
                settled,
                ..
            } = block
                && id == request_id
            {
                *settled = Some(how);
                return;
            }
        }
    }
}

/// The transcript-shaped half of the fold: routes one event to the
/// transcript(s) it belongs in. Called after the tree fold, so names and
/// parents are already known.
pub fn transcribe(
    transcripts: &mut BTreeMap<String, Transcript>,
    agents: &BTreeMap<String, AgentNode>,
    event: &Event,
) {
    Scribe {
        transcripts,
        agents,
    }
    .transcribe(event);
}

/// The fold's two inputs, borrowed for one event.
struct Scribe<'a> {
    transcripts: &'a mut BTreeMap<String, Transcript>,
    agents: &'a BTreeMap<String, AgentNode>,
}

impl Scribe<'_> {
    fn transcribe(&mut self, event: &Event) {
        let agent_id = event
            .agent_id
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        let Some(kind) = event.kind.as_ref() else {
            return;
        };
        if !self.transcribe_conversation(&agent_id, kind) {
            self.transcribe_machinery(&agent_id, kind);
        }
    }

    fn transcript_mut(&mut self, agent_id: &str) -> &mut Transcript {
        self.transcripts.entry(agent_id.to_owned()).or_default()
    }

    /// Output, messages, spawns and results. False for any other kind.
    fn transcribe_conversation(&mut self, agent_id: &str, kind: &Kind) -> bool {
        match kind {
            Kind::AgentOutputChunk(chunk) => self.transcribe_output(agent_id, chunk),
            Kind::AssignmentSent(sent) => {
                let text = sent
                    .assignment
                    .as_ref()
                    .map_or_else(String::new, |assignment| assignment.text.clone());
                self.transcribe_message(agent_id, sent.from.as_ref(), text, false);
            }
            Kind::NoteSent(sent) => {
                let text = sent
                    .note
                    .as_ref()
                    .map_or_else(String::new, |note| note.text.clone());
                self.transcribe_message(agent_id, sent.from.as_ref(), text, true);
            }
            Kind::AgentSpawned(spawned) => self.transcribe_spawn(agent_id, spawned),
            Kind::ResultReported(reported) => self.transcribe_result(agent_id, reported),
            _ => return false,
        }
        true
    }

    fn transcribe_output(&mut self, agent_id: &str, chunk: &horde_proto::v1::AgentOutputChunk) {
        let transcript = self.transcript_mut(agent_id);
        match AgentOutputChannel::try_from(chunk.channel) {
            Ok(AgentOutputChannel::Reasoning) => transcript.stream(&chunk.turn_id, &chunk.text, true),
            Ok(AgentOutputChannel::ToolCall) => transcript.tool_call(&chunk.turn_id, &chunk.text),
            Ok(AgentOutputChannel::ToolResult) => {
                transcript.tool_result(&chunk.turn_id, &chunk.text);
            }
            // The harness talking to itself stays out, as in the feed.
            Ok(AgentOutputChannel::HarnessDiagnostic) => {}
            _ => transcript.stream(&chunk.turn_id, &chunk.text, false),
        }
    }

    fn transcribe_message(&mut self, agent_id: &str, from: Option<&Actor>, text: String, note: bool) {
        let from = self.speaker(from);
        self.transcript_mut(agent_id)
            .push(Block::Message { from, text, note });
    }

    fn speaker(&self, from: Option<&Actor>) -> Speaker {
        match from.and_then(|actor| actor.actor.as_ref()) {
            Some(actor::Actor::AgentId(id)) => Speaker::Agent(agent_name(self.agents, &id.to_string())),
            Some(actor::Actor::Server(_)) => Speaker::Server,
            _ => Speaker::Human,
        }
    }

    /// A child's spawn shows in its parent's transcript; the child's own
    /// transcript opens with the assignment it was born with.
    fn transcribe_spawn(&mut self, agent_id: &str, spawned: &horde_proto::v1::AgentSpawned) {
        let name = agent_name(self.agents, agent_id);
        if let Some(parent) = spawned.parent_agent_id.as_ref() {
            self.transcript_mut(&parent.to_string()).push(Block::Child {
                text: format!("spawned {name}"),
            });
        }
        let first = spawned
            .spec
            .as_ref()
            .and_then(|spec| spec.initial_assignment.as_ref())
            .map(|assignment| assignment.text.clone())
            .filter(|text| !text.is_empty());
        if let Some(text) = first {
            let from = self.speaker(spawned.spawned_by.as_ref());
            self.transcript_mut(agent_id).push(Block::Message {
                from,
                text,
                note: false,
            });
        }
    }

    fn transcribe_result(&mut self, agent_id: &str, reported: &horde_proto::v1::ResultReported) {
        let (status, summary) = reported.result.as_ref().map_or(("unspecified", String::new()), |result| {
            (crate::words::result_status_name(result.status), result.summary.clone())
        });
        let name = agent_name(self.agents, agent_id);
        let parent = self.agents.get(agent_id).and_then(|node| node.parent.clone());
        if let Some(parent) = parent {
            self.transcript_mut(&parent).push(Block::Child {
                text: format!("{name} reported {status}: {}", crate::wrap::clip(&summary, 200)),
            });
        }
        self.transcript_mut(agent_id)
            .push(Block::Reported { status, summary });
    }

    /// Prompts, lifecycles, placement, interrupts and the rest.
    fn transcribe_machinery(&mut self, agent_id: &str, kind: &Kind) {
        match kind {
            Kind::PermissionRequested(requested) => {
                let Some(request) = requested.request.as_ref() else {
                    return;
                };
                let request_id = request
                    .request_id
                    .as_ref()
                    .map_or_else(String::new, ToString::to_string);
                let summary = request
                    .tool_call
                    .as_ref()
                    .map_or_else(String::new, |call| call.rendered.clone());
                self.transcript_mut(agent_id).push(Block::Prompt {
                    request_id,
                    summary,
                    settled: None,
                });
            }
            Kind::PermissionDecided(decided) => self.transcribe_decision(decided),
            _ => {
                if let Some((tone, text)) = notice(kind) {
                    self.transcript_mut(agent_id).push(Block::Notice { tone, text });
                }
            }
        }
    }

    fn transcribe_decision(&mut self, decided: &horde_proto::v1::PermissionDecided) {
        let Some(decision) = decided.decision.as_ref() else {
            return;
        };
        let Some(request_id) = decision.request_id.as_ref().map(ToString::to_string) else {
            return;
        };
        let how = settled(decision);
        for transcript in self.transcripts.values_mut() {
            transcript.settle(&request_id, how.clone());
        }
    }
}

fn settled(decision: &horde_proto::v1::PermissionDecision) -> Settled {
    if !decision.answers.is_empty() {
        let mut pairs: Vec<String> = decision
            .answers
            .iter()
            .map(|(question, answer)| format!("{question} → {}", answer.values.join(", ")))
            .collect();
        pairs.sort();
        return Settled::Answered(pairs.join("; "));
    }
    if decision.outcome == PermissionOutcome::Allow as i32 {
        Settled::Allowed
    } else {
        Settled::Denied
    }
}

/// The machinery events worth a line in the transcript, and how loudly.
fn notice(kind: &Kind) -> Option<(Tone, String)> {
    match kind {
        Kind::AgentLifecycleChanged(changed) => lifecycle_notice(changed),
        Kind::PlacementChanged(changed) => changed.placement.as_ref().and_then(placement_notice),
        Kind::TurnInterrupted(interrupted) => Some((
            Tone::Warn,
            with_reason("Interrupted", &interrupted.reason),
        )),
        Kind::WorkspaceInstanceChanged(changed) => changed
            .report
            .as_ref()
            .filter(|report| !report.detail.is_empty())
            .map(|report| (Tone::Info, format!("workspace: {}", report.detail))),
        _ => None,
    }
}

fn lifecycle_notice(changed: &horde_proto::v1::AgentLifecycleChanged) -> Option<(Tone, String)> {
    match AgentLifecycleState::try_from(changed.state) {
        Ok(AgentLifecycleState::Failed) => Some((Tone::Error, with_reason("Failed", &changed.reason))),
        Ok(AgentLifecycleState::Archived) => Some((Tone::Info, with_reason("Archived", &changed.reason))),
        _ => None,
    }
}

fn placement_notice(placement: &horde_proto::v1::Placement) -> Option<(Tone, String)> {
    let reason = &placement.reason;
    match PlacementState::try_from(placement.state) {
        Ok(PlacementState::Queued) => Some((Tone::Warn, with_reason("Queued", reason))),
        Ok(PlacementState::Lost) => Some((Tone::Error, with_reason("LOST with its worker", reason))),
        Ok(PlacementState::Expired) => Some((Tone::Error, with_reason("Placement expired", reason))),
        Ok(PlacementState::Cancelled) => Some((Tone::Warn, with_reason("Placement cancelled", reason))),
        _ => None,
    }
}

fn with_reason(what: &str, reason: &str) -> String {
    if reason.is_empty() {
        what.to_owned()
    } else {
        format!("{what} · {reason}")
    }
}

#[cfg(test)]
mod tests;
