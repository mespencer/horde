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
    PermissionDecision, ProposedToolCall, ProviderSessionId, TurnInput, TurnOutcome,
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

/// How the suite answers permission requests while driving a session.
///
/// A turn that asks for permission does not advance until it is answered, so
/// anything driving a session has to bring a policy. Tests supply one that
/// exercises the case they care about — denying, narrowing, or stalling.
pub type Policy<'a> = &'a (dyn Fn(&ProposedToolCall) -> PermissionDecision + Sync);

/// A policy that permits every call unchanged.
///
/// The right default for tests about orchestration rather than about
/// permissions: it keeps turns moving without silently making a permission
/// decision look like it was considered.
#[must_use]
pub fn allow_all() -> PermissionDecision {
    PermissionDecision::allow()
}

/// Sends one message and drains events until the turn ends, permitting every
/// tool call. See [`run_turn_with`] to supply a policy.
pub async fn run_turn(
    session: &mut dyn AgentSession,
    input: TurnInput,
) -> Result<TurnRecord, ConformanceFailure> {
    run_turn_with(session, input, &|_| allow_all()).await
}

/// Sends one message and drains events until the turn ends.
///
/// Enforces the invariant that a turn is closed by exactly one
/// [`AgentEvent::TurnEnded`]: a stream that ends without one, or that keeps
/// going after one, fails here rather than somewhere downstream.
pub async fn run_turn_with(
    session: &mut dyn AgentSession,
    input: TurnInput,
    policy: Policy<'_>,
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

        if let AgentEvent::PermissionRequested { request, .. } = &event {
            let decision = policy(request);
            let tool_call_id = request.tool_call_id.clone();
            session
                .answer_permission(&tool_call_id, decision)
                .await
                .map_err(ConformanceFailure::during(format!(
                    "answering the permission request for {tool_call_id}"
                )))?;
        }

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
    check_session_with(session, messages, &|_| allow_all()).await
}

/// Drives a replayed recording through its `turns` turns — one placeholder
/// message per recorded turn, since a recording's reply does not depend on
/// what was sent — under an explicit permission policy.
pub async fn check_recording(
    session: &mut dyn AgentSession,
    turns: usize,
    policy: Policy<'_>,
) -> Result<Vec<TurnRecord>, ConformanceFailure> {
    let messages = vec!["replayed"; turns];
    check_session_with(session, &messages, policy).await
}

/// Drives a session through `messages` under an explicit permission policy.
pub async fn check_session_with(
    session: &mut dyn AgentSession,
    messages: &[&str],
    policy: Policy<'_>,
) -> Result<Vec<TurnRecord>, ConformanceFailure> {
    let mut records = Vec::new();
    for message in messages {
        records.push(run_turn_with(session, TurnInput::queued(*message), policy).await?);
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

#[cfg(test)]
mod tests {
    use core::time::Duration;

    use horde_adapter::{
        AgentEvent, OutputChannel, ProviderSessionId, TurnId, TurnOutcome, TurnStatus, UsageDelta,
    };

    use super::{
        TurnRecord, check_chunk_indices, check_session_announced_before_output,
        check_turn_ids_are_consistent,
    };

    /// A conformant turn: the session is announced, then two assistant chunks
    /// numbered from zero, then the turn ends.
    fn conformant_turn(turn: &str, session: &str) -> TurnRecord {
        let turn_id = TurnId::new(turn);
        TurnRecord {
            events: vec![
                AgentEvent::SessionEstablished {
                    provider_session_id: ProviderSessionId::new(session),
                    model: "m".to_owned(),
                },
                output(&turn_id, OutputChannel::Assistant, 0),
                output(&turn_id, OutputChannel::Assistant, 1),
                AgentEvent::TurnEnded(outcome(&turn_id)),
            ],
            outcome: outcome(&turn_id),
        }
    }

    fn output(turn_id: &TurnId, channel: OutputChannel, chunk_index: u64) -> AgentEvent {
        AgentEvent::Output {
            turn_id: turn_id.clone(),
            channel,
            text: "x".to_owned(),
            chunk_index,
        }
    }

    fn outcome(turn_id: &TurnId) -> TurnOutcome {
        TurnOutcome {
            turn_id: turn_id.clone(),
            status: TurnStatus::Completed,
            text: None,
            usage: UsageDelta::default(),
            denials: Vec::new(),
            duration: Duration::from_millis(1),
        }
    }

    /// Asserts a checker rejected something, and that it said why.
    ///
    /// The message matters: a checker that fails with the wrong explanation
    /// sends the next person to the wrong place, and an assertion on the text
    /// is what stops a checker's arms from being deleted unnoticed.
    fn rejects(result: Result<(), super::ConformanceFailure>, expected: &str) {
        let failure = result.expect_err("the checker must reject this input");
        let message = failure.to_string();
        assert!(
            message.contains(expected),
            "expected {expected:?} in {message:?}"
        );
    }

    // -- check_session_announced_before_output --------------------------------

    #[test]
    fn a_conformant_session_passes_every_check() {
        let records = vec![conformant_turn("t1", "s1"), conformant_turn("t2", "s1")];

        check_session_announced_before_output(&records).expect("announcement is conformant");
        check_turn_ids_are_consistent(&records).expect("turn ids are conformant");
        check_chunk_indices(&records).expect("chunk indices are conformant");
    }

    #[test]
    fn output_before_any_session_announcement_is_rejected() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![
                output(&turn_id, OutputChannel::Assistant, 0),
                AgentEvent::SessionEstablished {
                    provider_session_id: ProviderSessionId::new("s1"),
                    model: "m".to_owned(),
                },
            ],
            outcome: outcome(&turn_id),
        }];

        rejects(
            check_session_announced_before_output(&records),
            "before any SessionEstablished",
        );
    }

    #[test]
    fn a_session_that_is_never_announced_is_rejected() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![AgentEvent::TurnEnded(outcome(&turn_id))],
            outcome: outcome(&turn_id),
        }];

        rejects(
            check_session_announced_before_output(&records),
            "no SessionEstablished was ever emitted",
        );
    }

    /// Revival targets a session id, so an id that changes mid-conversation
    /// would send a revival to the wrong conversation.
    #[test]
    fn a_session_id_that_changes_mid_conversation_is_rejected() {
        let records = vec![conformant_turn("t1", "s1"), conformant_turn("t2", "s2")];

        rejects(
            check_session_announced_before_output(&records),
            "changed mid-conversation",
        );
    }

    /// The same id announced twice is re-announcement, not a change, and must
    /// pass — this is what distinguishes `!=` from `==` in the comparison.
    #[test]
    fn re_announcing_the_same_session_id_is_not_a_change() {
        let records = vec![conformant_turn("t1", "s1"), conformant_turn("t2", "s1")];
        check_session_announced_before_output(&records)
            .expect("announcing the same id twice is not a violation");
    }

    /// Output *after* the announcement is the normal case; the guard must not
    /// fire for it.
    #[test]
    fn output_after_the_announcement_is_accepted() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![
                AgentEvent::SessionEstablished {
                    provider_session_id: ProviderSessionId::new("s1"),
                    model: "m".to_owned(),
                },
                output(&turn_id, OutputChannel::Assistant, 0),
            ],
            outcome: outcome(&turn_id),
        }];
        check_session_announced_before_output(&records).expect("this is the ordinary ordering");
    }

    // -- check_turn_ids_are_consistent ----------------------------------------

    #[test]
    fn an_event_carrying_another_turns_id_is_rejected() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![output(&TurnId::new("t-other"), OutputChannel::Assistant, 0)],
            outcome: outcome(&turn_id),
        }];

        rejects(
            check_turn_ids_are_consistent(&records),
            "carried turn id t-other",
        );
    }

    #[test]
    fn a_reused_turn_id_is_rejected() {
        let records = vec![conformant_turn("t1", "s1"), conformant_turn("t1", "s1")];

        rejects(check_turn_ids_are_consistent(&records), "was reused");
    }

    /// Events whose ids match must pass, which is the other half of the `!=`
    /// comparison the reject case pins.
    #[test]
    fn matching_turn_ids_are_accepted() {
        let records = vec![conformant_turn("t1", "s1")];
        check_turn_ids_are_consistent(&records).expect("ids that match are conformant");
    }

    // -- check_chunk_indices ---------------------------------------------------

    #[test]
    fn a_first_chunk_that_does_not_start_at_zero_is_rejected() {
        rejects(
            check_chunk_indices(&turn_with_chunk_indices(&[1])),
            "expected 0",
        );
    }

    /// One turn whose assistant chunks carry the given indices.
    fn turn_with_chunk_indices(indices: &[u64]) -> Vec<TurnRecord> {
        let turn_id = TurnId::new("t1");
        vec![TurnRecord {
            events: indices
                .iter()
                .map(|index| output(&turn_id, OutputChannel::Assistant, *index))
                .collect(),
            outcome: outcome(&turn_id),
        }]
    }

    #[test]
    fn a_gap_in_chunk_indices_is_rejected() {
        rejects(
            check_chunk_indices(&turn_with_chunk_indices(&[0, 2])),
            "jumped to 2",
        );
    }

    #[test]
    fn a_repeated_chunk_index_is_rejected() {
        rejects(
            check_chunk_indices(&turn_with_chunk_indices(&[0, 0])),
            "jumped to 0",
        );
    }

    /// Channels are numbered independently, so a second channel starting at
    /// zero is correct rather than a repeat.
    #[test]
    fn each_channel_is_numbered_independently() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![
                output(&turn_id, OutputChannel::Assistant, 0),
                output(&turn_id, OutputChannel::Reasoning, 0),
                output(&turn_id, OutputChannel::Assistant, 1),
                output(&turn_id, OutputChannel::Reasoning, 1),
            ],
            outcome: outcome(&turn_id),
        }];

        check_chunk_indices(&records).expect("channels do not share a counter");
    }

    /// Counters reset per turn, so the second turn starting at zero is correct.
    #[test]
    fn chunk_counters_reset_between_turns() {
        let records = vec![conformant_turn("t1", "s1"), conformant_turn("t2", "s1")];
        check_chunk_indices(&records).expect("a new turn starts its numbering again");
    }

    #[test]
    fn non_output_events_do_not_disturb_the_numbering() {
        let turn_id = TurnId::new("t1");
        let records = vec![TurnRecord {
            events: vec![
                output(&turn_id, OutputChannel::Assistant, 0),
                AgentEvent::TurnEnded(outcome(&turn_id)),
                output(&turn_id, OutputChannel::Assistant, 1),
            ],
            outcome: outcome(&turn_id),
        }];

        check_chunk_indices(&records).expect("only Output events are counted");
    }

    // -- TurnRecord::assistant_text -------------------------------------------

    #[test]
    fn assistant_text_concatenates_only_the_assistant_channel() {
        let turn_id = TurnId::new("t1");
        let record = TurnRecord {
            events: vec![
                AgentEvent::Output {
                    turn_id: turn_id.clone(),
                    channel: OutputChannel::Assistant,
                    text: "hello ".to_owned(),
                    chunk_index: 0,
                },
                AgentEvent::Output {
                    turn_id: turn_id.clone(),
                    channel: OutputChannel::Reasoning,
                    text: "thinking".to_owned(),
                    chunk_index: 0,
                },
                AgentEvent::Output {
                    turn_id: turn_id.clone(),
                    channel: OutputChannel::Assistant,
                    text: "world".to_owned(),
                    chunk_index: 1,
                },
            ],
            outcome: outcome(&turn_id),
        };

        assert_eq!(
            record.assistant_text(),
            "hello world",
            "reasoning must not leak into what the agent said"
        );
    }

    #[test]
    fn assistant_text_is_empty_when_the_agent_said_nothing() {
        let turn_id = TurnId::new("t1");
        let record = TurnRecord {
            events: vec![AgentEvent::TurnEnded(outcome(&turn_id))],
            outcome: outcome(&turn_id),
        };
        assert!(record.assistant_text().is_empty());
    }

    // -- Doubles ---------------------------------------------------------------
    //
    // Written here rather than reached for from horde-adapter-fake: that crate
    // dev-depends on this one, so using it would be circular, and a checker's
    // own tests should not need the thing it checks.

    /// Replays a fixed event stream, one turn per `send`.
    struct ScriptedSession {
        turns: std::collections::VecDeque<Vec<AgentEvent>>,
        pending: std::collections::VecDeque<AgentEvent>,
        session_id: Option<ProviderSessionId>,
    }

    impl ScriptedSession {
        fn new(turns: Vec<Vec<AgentEvent>>) -> Self {
            Self {
                turns: turns.into(),
                pending: std::collections::VecDeque::new(),
                session_id: Some(ProviderSessionId::new("s1")),
            }
        }

        /// A session that never reports an id, so revival would be impossible.
        fn anonymous(turns: Vec<Vec<AgentEvent>>) -> Self {
            Self {
                session_id: None,
                ..Self::new(turns)
            }
        }
    }

    #[async_trait::async_trait]
    impl horde_adapter::AgentSession for ScriptedSession {
        fn provider_session_id(&self) -> Option<&ProviderSessionId> {
            self.session_id.as_ref()
        }

        async fn send(&mut self, _input: horde_adapter::TurnInput) -> horde_adapter::Result<()> {
            match self.turns.pop_front() {
                Some(events) => {
                    self.pending.extend(events);
                    Ok(())
                }
                None => Err(horde_adapter::AdapterError::HarnessExited { exit_code: None }),
            }
        }

        async fn next_event(&mut self) -> horde_adapter::Result<Option<AgentEvent>> {
            Ok(self.pending.pop_front())
        }

        async fn answer_permission(
            &mut self,
            tool_call_id: &horde_adapter::ToolCallId,
            _decision: horde_adapter::PermissionDecision,
        ) -> horde_adapter::Result<()> {
            Err(horde_adapter::AdapterError::UnknownPermissionRequest {
                tool_call_id: tool_call_id.clone(),
            })
        }

        async fn interrupt(&mut self) -> horde_adapter::Result<()> {
            Ok(())
        }

        async fn shutdown(&mut self) -> horde_adapter::Result<()> {
            Ok(())
        }
    }

    /// An adapter whose `resume` behaviour and claimed capabilities are set by
    /// the test, so capability honesty can be checked in both directions.
    struct ScriptedAdapter {
        capabilities: horde_adapter::Capabilities,
        resume: ResumeBehaviour,
    }

    enum ResumeBehaviour {
        RefusesAsUnsupported,
        RefusesWithSomethingElse,
        Succeeds,
    }

    #[async_trait::async_trait]
    impl horde_adapter::Adapter for ScriptedAdapter {
        fn provider(&self) -> &str {
            "scripted"
        }

        fn capabilities(&self) -> horde_adapter::Capabilities {
            self.capabilities.clone()
        }

        async fn spawn(
            &self,
            _spec: &horde_adapter::AgentSpec,
        ) -> horde_adapter::Result<Box<dyn horde_adapter::AgentSession>> {
            Ok(Box::new(ScriptedSession::new(Vec::new())))
        }

        async fn resume(
            &self,
            _spec: &horde_adapter::AgentSpec,
            _provider_session_id: &ProviderSessionId,
        ) -> horde_adapter::Result<Box<dyn horde_adapter::AgentSession>> {
            match self.resume {
                ResumeBehaviour::RefusesAsUnsupported => {
                    Err(horde_adapter::AdapterError::Unsupported {
                        provider: "scripted".to_owned(),
                        capability: horde_adapter::Capability::Resumable,
                    })
                }
                ResumeBehaviour::RefusesWithSomethingElse => {
                    Err(horde_adapter::AdapterError::HarnessExited { exit_code: Some(1) })
                }
                ResumeBehaviour::Succeeds => Ok(Box::new(ScriptedSession::new(Vec::new()))),
            }
        }
    }

    fn conformant_events(turn: &str, session: &str) -> Vec<AgentEvent> {
        conformant_turn(turn, session).events
    }

    // -- check_session ---------------------------------------------------------

    #[tokio::test]
    async fn check_session_returns_one_record_per_message_and_accepts_a_good_session() {
        let mut session = ScriptedSession::new(vec![
            conformant_events("t1", "s1"),
            conformant_events("t2", "s1"),
        ]);

        let records = super::check_session(&mut session, &["one", "two"])
            .await
            .expect("a conformant session must pass");

        assert_eq!(
            records.len(),
            2,
            "one record per message, or a caller cannot tell which turn is which"
        );
        assert_eq!(records[0].outcome.turn_id.as_str(), "t1");
        assert_eq!(records[1].outcome.turn_id.as_str(), "t2");
    }

    /// A recording is driven one turn per recorded turn, and its checks run.
    #[tokio::test]
    async fn check_recording_drives_every_recorded_turn() {
        let mut session = ScriptedSession::new(vec![
            conformant_events("t1", "s1"),
            conformant_events("t2", "s1"),
        ]);
        let records = super::check_recording(&mut session, 2, &|_| super::allow_all())
            .await
            .expect("a conformant recording must pass");
        assert_eq!(records.len(), 2);

        let mut anonymous = ScriptedSession::anonymous(vec![conformant_events("t1", "s1")]);
        assert!(
            super::check_recording(&mut anonymous, 1, &|_| super::allow_all())
                .await
                .is_err(),
            "the session checks run on a recording too"
        );
    }

    #[tokio::test]
    async fn check_session_rejects_a_session_that_never_reports_an_id() {
        let mut session = ScriptedSession::anonymous(vec![conformant_events("t1", "s1")]);

        rejects(
            super::check_session(&mut session, &["one"])
                .await
                .map(|_| ()),
            "provider_session_id",
        );
    }

    #[tokio::test]
    async fn check_session_propagates_a_violation_from_the_checks_it_runs() {
        let turn_id = TurnId::new("t1");
        let bad = vec![
            output(&turn_id, OutputChannel::Assistant, 0),
            AgentEvent::TurnEnded(outcome(&turn_id)),
        ];
        let mut session = ScriptedSession::new(vec![bad]);

        rejects(
            super::check_session(&mut session, &["one"])
                .await
                .map(|_| ()),
            "before any SessionEstablished",
        );
    }

    /// A turn whose stream ends without a `TurnEnded` must be rejected, not
    /// silently returned as an empty result set.
    #[tokio::test]
    async fn check_session_rejects_a_turn_that_never_ends() {
        let turn_id = TurnId::new("t1");
        let mut session = ScriptedSession::new(vec![vec![
            AgentEvent::SessionEstablished {
                provider_session_id: ProviderSessionId::new("s1"),
                model: "m".to_owned(),
            },
            output(&turn_id, OutputChannel::Assistant, 0),
        ]]);

        rejects(
            super::check_session(&mut session, &["one"])
                .await
                .map(|_| ()),
            "without a TurnEnded",
        );
    }

    // -- check_capability_honesty ---------------------------------------------

    #[tokio::test]
    async fn an_adapter_that_claims_resumable_is_not_asked_to_prove_a_refusal() {
        let adapter = ScriptedAdapter {
            capabilities: horde_adapter::Capabilities::none()
                .with(horde_adapter::Capability::Resumable),
            // Succeeding is correct for an adapter that claims the capability;
            // the check must not treat it as a violation.
            resume: ResumeBehaviour::Succeeds,
        };

        super::check_capability_honesty(&adapter)
            .await
            .expect("an adapter that claims Resumable may resume");
    }

    #[tokio::test]
    async fn an_adapter_without_resumable_must_refuse_as_unsupported() {
        let adapter = ScriptedAdapter {
            capabilities: horde_adapter::Capabilities::none(),
            resume: ResumeBehaviour::RefusesAsUnsupported,
        };

        super::check_capability_honesty(&adapter)
            .await
            .expect("refusing as Unsupported is the honest behaviour");
    }

    /// Refusing for the wrong reason hides the difference between "cannot" and
    /// "went wrong", which is the distinction the caller needs.
    #[tokio::test]
    async fn refusing_with_the_wrong_error_is_a_violation() {
        let adapter = ScriptedAdapter {
            capabilities: horde_adapter::Capabilities::none(),
            resume: ResumeBehaviour::RefusesWithSomethingElse,
        };

        rejects(
            super::check_capability_honesty(&adapter).await,
            "must reject it as Unsupported",
        );
    }

    /// The dangerous case: a revival that silently started a fresh conversation
    /// looks exactly like one that restored context.
    #[tokio::test]
    async fn resuming_without_claiming_resumable_is_a_violation() {
        let adapter = ScriptedAdapter {
            capabilities: horde_adapter::Capabilities::none(),
            resume: ResumeBehaviour::Succeeds,
        };

        rejects(
            super::check_capability_honesty(&adapter).await,
            "silently lost context",
        );
    }
}
