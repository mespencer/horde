//! The adapter conformance suite.
//!
//! One set of checks, run against every adapter — the real ones and the
//! FakeAdapter alike. That symmetry is the point: the fake is only useful for
//! testing orchestration if it behaves like a real harness, and the only way to
//! keep it honest is to hold both to the same suite (docs/TESTING.md §3).
//!
//! The invariants checked here are the ones `horde_adapter`'s crate
//! documentation promises to callers. They are deliberately about *sequence and
//! shape*, not content: what a model says is not a conformance question, but
//! whether every message gets exactly one turn ending very much is.
//!
//! Failures are returned rather than asserted, because this crate is ordinary
//! library code under the workspace lint policy — `panic` is denied outside
//! tests, and a suite that could not state its own findings as values would be
//! unable to report them anywhere but a test binary.

use horde_adapter::{
    Adapter, AdapterError, AgentEvent, AgentSession, AgentSpec, Capability, OutputChannel,
    ProviderSessionId, TurnInput, TurnOutcome,
};

/// A conformance violation, or the adapter error that prevented checking.
#[derive(Debug, thiserror::Error)]
pub enum ConformanceFailure {
    #[error("adapter failed while {context}: {source}")]
    Adapter {
        context: String,
        #[source]
        source: AdapterError,
    },
    #[error("conformance violation: {0}")]
    Violation(String),
}

impl ConformanceFailure {
    fn violation(message: impl Into<String>) -> Self {
        Self::Violation(message.into())
    }

    fn during(context: impl Into<String>) -> impl FnOnce(AdapterError) -> Self {
        let context = context.into();
        move |source| Self::Adapter { context, source }
    }
}

/// Everything observed during one turn.
#[derive(Debug, Clone)]
pub struct TurnRecord {
    /// Every event of the turn, in order, ending with the `TurnEnded`.
    pub events: Vec<AgentEvent>,
    pub outcome: TurnOutcome,
}

impl TurnRecord {
    /// Text the agent addressed to its reader, concatenated.
    #[must_use]
    pub fn assistant_text(&self) -> String {
        self.events
            .iter()
            .filter_map(|event| match event {
                AgentEvent::Output {
                    channel: OutputChannel::Assistant,
                    text,
                    ..
                } => Some(text.as_str()),
                _ => None,
            })
            .collect()
    }
}

/// Sends one message and drains events until the turn ends.
///
/// Enforces the invariant that a turn is closed by exactly one
/// [`AgentEvent::TurnEnded`]: a stream that ends without one, or that keeps
/// going after one, fails here rather than somewhere downstream.
pub async fn run_turn(
    session: &mut dyn AgentSession,
    input: TurnInput,
) -> Result<TurnRecord, ConformanceFailure> {
    let sent = input.text.clone();
    session
        .send(input)
        .await
        .map_err(ConformanceFailure::during(format!("sending {sent:?}")))?;

    let mut events = Vec::new();
    loop {
        let next = session
            .next_event()
            .await
            .map_err(ConformanceFailure::during(format!(
                "reading the reply to {sent:?}"
            )))?;

        let Some(event) = next else {
            return Err(ConformanceFailure::violation(format!(
                "the event stream ended without a TurnEnded for {sent:?}; \
                 every message must be answered by exactly one turn ending"
            )));
        };

        let ended = matches!(event, AgentEvent::TurnEnded(_));
        events.push(event);

        if ended {
            break;
        }
    }

    let Some(AgentEvent::TurnEnded(outcome)) = events.last().cloned() else {
        return Err(ConformanceFailure::violation(
            "a turn's last event must be its TurnEnded".to_owned(),
        ));
    };

    Ok(TurnRecord { events, outcome })
}

/// Drives a session through `messages` and checks the seam invariants.
pub async fn check_session(
    session: &mut dyn AgentSession,
    messages: &[&str],
) -> Result<Vec<TurnRecord>, ConformanceFailure> {
    let mut records = Vec::new();
    for message in messages {
        records.push(run_turn(session, TurnInput::queued(*message)).await?);
    }

    check_session_announced_before_output(&records)?;
    check_turn_ids_are_consistent(&records)?;
    check_chunk_indices(&records)?;

    if session.provider_session_id().is_none() {
        return Err(ConformanceFailure::violation(
            "the session reported no provider_session_id after a completed turn; \
             without it the agent cannot be revived"
                .to_owned(),
        ));
    }

    Ok(records)
}

/// The identifier that makes an agent revivable must reach the journal before
/// any output could be lost.
fn check_session_announced_before_output(records: &[TurnRecord]) -> Result<(), ConformanceFailure> {
    let mut announced: Option<ProviderSessionId> = None;

    for (turn, record) in records.iter().enumerate() {
        for event in &record.events {
            match event {
                AgentEvent::SessionEstablished {
                    provider_session_id,
                    ..
                } => {
                    if let Some(existing) = &announced
                        && existing != provider_session_id
                    {
                        return Err(ConformanceFailure::violation(format!(
                            "the session id changed mid-conversation ({existing} -> \
                             {provider_session_id}); revival would target the wrong conversation"
                        )));
                    }
                    announced = Some(provider_session_id.clone());
                }
                AgentEvent::Output { .. } if announced.is_none() => {
                    return Err(ConformanceFailure::violation(format!(
                        "turn {turn} produced output before any SessionEstablished; \
                         output that cannot be attributed to a resumable session is unjournalable"
                    )));
                }
                _ => {}
            }
        }
    }

    if announced.is_none() {
        return Err(ConformanceFailure::violation(
            "no SessionEstablished was ever emitted".to_owned(),
        ));
    }
    Ok(())
}

/// Every event of a turn must carry that turn's id, or the journal cannot group
/// them.
fn check_turn_ids_are_consistent(records: &[TurnRecord]) -> Result<(), ConformanceFailure> {
    let mut seen = Vec::new();

    for record in records {
        let expected = &record.outcome.turn_id;
        for event in &record.events {
            let carried = match event {
                AgentEvent::Output { turn_id, .. }
                | AgentEvent::ToolCompleted { turn_id, .. }
                | AgentEvent::PermissionRequested { turn_id, .. } => Some(turn_id),
                _ => None,
            };
            if let Some(carried) = carried
                && carried != expected
            {
                return Err(ConformanceFailure::violation(format!(
                    "an event carried turn id {carried} inside the turn that ended as {expected}"
                )));
            }
        }

        if seen.contains(expected) {
            return Err(ConformanceFailure::violation(format!(
                "turn id {expected} was reused; turn ids must distinguish turns"
            )));
        }
        seen.push(expected.clone());
    }
    Ok(())
}

/// Chunk indices exist so a client can detect a gap. They only do that if they
/// start at zero and advance by one within each turn and channel.
fn check_chunk_indices(records: &[TurnRecord]) -> Result<(), ConformanceFailure> {
    for record in records {
        let mut next_expected: Vec<(OutputChannel, u64)> = Vec::new();

        for event in &record.events {
            let AgentEvent::Output {
                channel,
                chunk_index,
                ..
            } = event
            else {
                continue;
            };

            let expected = next_expected
                .iter_mut()
                .find(|(known, _)| known == channel)
                .map(|(_, next)| next);

            match expected {
                Some(next) => {
                    if *chunk_index != *next {
                        return Err(ConformanceFailure::violation(format!(
                            "{channel:?} chunk index jumped to {chunk_index}, expected {next}"
                        )));
                    }
                    *next = next.saturating_add(1);
                }
                None => {
                    if *chunk_index != 0 {
                        return Err(ConformanceFailure::violation(format!(
                            "the first {channel:?} chunk of a turn had index {chunk_index}, \
                             expected 0"
                        )));
                    }
                    next_expected.push((*channel, 1));
                }
            }
        }
    }
    Ok(())
}

/// Checks that an adapter refuses what it does not claim, rather than
/// pretending.
///
/// A silent no-op here is the dangerous case: an interrupt that did not
/// interrupt looks exactly like one that did, and a revival that quietly
/// started a fresh conversation looks like one that restored context.
pub async fn check_capability_honesty(adapter: &dyn Adapter) -> Result<(), ConformanceFailure> {
    let capabilities = adapter.capabilities();

    if !capabilities.supports(Capability::Resumable) {
        let attempt = adapter
            .resume(&AgentSpec::default(), &ProviderSessionId::new("whatever"))
            .await;
        match attempt {
            Err(AdapterError::Unsupported { .. }) => {}
            Err(other) => {
                return Err(ConformanceFailure::violation(format!(
                    "an adapter without Resumable rejected resume with {other}, \
                     but must reject it as Unsupported so the caller can tell the difference"
                )));
            }
            Ok(_) => {
                return Err(ConformanceFailure::violation(
                    "an adapter without Resumable returned a session from resume; \
                     a revival that silently lost context is worse than a refusal"
                        .to_owned(),
                ));
            }
        }
    }

    Ok(())
}
