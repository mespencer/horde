//! Output shaping between a harness and the journal: coalesce, then chunk.
//!
//! Codex streams a turn as many tiny deltas — a token or two each — and every
//! one journaled alone is its own transaction, its own row, its own pushed
//! event, and its own client render. The shaper merges *consecutive* output of
//! one turn and channel that the harness has already produced, and splits any
//! text larger than [`MAX_CHUNK_BYTES`] so no single event can outgrow a push
//! batch or the wire's decoding limit (a tool that printed a megabyte).
//!
//! Neither step loses anything: text is concatenated in arrival order and cut
//! only at character boundaries, and a change of turn or channel — or any
//! non-output event, which the pump flushes ahead of — closes the merge, so
//! the journal's interleaving is the harness's. What changes is the
//! numbering: the harness's chunk indices stop meaning anything once chunks
//! merge or split, so each emitted chunk is numbered densely per turn and
//! channel here, which keeps "a gap means a lost chunk" true for clients.

use std::collections::BTreeMap;

use horde_adapter::{AgentEvent, OutputChannel, TurnId};

/// The largest text one journaled output chunk carries. The blob store's
/// inline threshold: big enough that ordinary output is never split, small
/// enough that a push batch holds many.
pub(super) const MAX_CHUNK_BYTES: usize = 64 * 1024;

/// How many ready events one coalescing pass may absorb before it journals —
/// a bound so a firehose harness cannot postpone its own journaling (or the
/// pump's control channel) indefinitely.
pub(super) const COALESCE_EVENTS: usize = 256;

/// Output held for merging, not yet journaled.
#[derive(Debug)]
struct Held {
    turn_id: TurnId,
    channel: OutputChannel,
    text: String,
}

/// Merges and splits one agent's output; see the module docs.
#[derive(Debug)]
pub(super) struct OutputShaper {
    max_chunk_bytes: usize,
    held: Option<Held>,
    /// The next dense chunk index per (turn, channel).
    next_index: BTreeMap<(String, u8), u64>,
}

impl Default for OutputShaper {
    fn default() -> Self {
        Self::with_max_chunk(MAX_CHUNK_BYTES)
    }
}

impl OutputShaper {
    /// A shaper splitting at `max_chunk_bytes` (at least 4, so a boundary
    /// always fits one character).
    pub(super) fn with_max_chunk(max_chunk_bytes: usize) -> Self {
        Self {
            max_chunk_bytes: max_chunk_bytes.max(4),
            held: None,
            next_index: BTreeMap::new(),
        }
    }

    /// Takes one output event's text, returning whatever is now ready to
    /// journal: the previous merge when this one starts a new turn or
    /// channel, and every full chunk the merge has outgrown.
    pub(super) fn absorb(
        &mut self,
        turn_id: TurnId,
        channel: OutputChannel,
        text: &str,
    ) -> Vec<AgentEvent> {
        let mut ready = Vec::new();
        match &mut self.held {
            Some(held) if held.turn_id == turn_id && held.channel == channel => {
                held.text.push_str(text);
            }
            _ => {
                ready.extend(self.flush());
                self.held = Some(Held {
                    turn_id,
                    channel,
                    text: text.to_owned(),
                });
            }
        }
        self.split_full(&mut ready);
        ready
    }

    /// Releases the held merge, if any.
    pub(super) fn flush(&mut self) -> Vec<AgentEvent> {
        match self.held.take() {
            Some(held) => vec![self.emit(held.turn_id, held.channel, held.text)],
            None => Vec::new(),
        }
    }

    /// Forgets a finished turn's numbering, so the map holds only live turns.
    pub(super) fn turn_ended(&mut self, turn_id: &TurnId) {
        self.next_index
            .retain(|(turn, _), _| turn.as_str() != turn_id.as_str());
    }

    /// Emits full chunks off the front of the held text while it is over
    /// the limit, each cut at the last character boundary that fits.
    fn split_full(&mut self, ready: &mut Vec<AgentEvent>) {
        while let Some(held) = self.held.as_mut()
            && held.text.len() > self.max_chunk_bytes
        {
            // The last character start at or under the limit: the last cut
            // that fits. Read off a finite walk of the text rather than a
            // backwards scan from the limit, so no mutation of it can keep
            // it from terminating. Never 0 — the limit is at least 4 and no
            // character is longer — so every pass shortens the held text.
            let cut = held
                .text
                .char_indices()
                .map(|(at, _)| at)
                .take_while(|at| *at <= self.max_chunk_bytes)
                .last()
                .unwrap_or_default();
            let rest = held.text.split_off(cut);
            let chunk = std::mem::replace(&mut held.text, rest);
            let (turn_id, channel) = (held.turn_id.clone(), held.channel);
            ready.push(self.emit(turn_id, channel, chunk));
        }
    }

    fn emit(&mut self, turn_id: TurnId, channel: OutputChannel, text: String) -> AgentEvent {
        let slot = self
            .next_index
            .entry((turn_id.as_str().to_owned(), channel_slot(channel)))
            .or_insert(0);
        let chunk_index = *slot;
        *slot += 1;
        AgentEvent::Output {
            turn_id,
            channel,
            text,
            chunk_index,
        }
    }
}

/// A stable small key per channel for the numbering map.
fn channel_slot(channel: OutputChannel) -> u8 {
    match channel {
        OutputChannel::Assistant => 0,
        OutputChannel::Reasoning => 1,
        OutputChannel::ToolCall => 2,
        OutputChannel::ToolResult => 3,
        OutputChannel::HarnessDiagnostic => 4,
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::{AgentEvent, OutputChannel, TurnId};

    use super::{MAX_CHUNK_BYTES, OutputShaper};

    /// `(turn, channel, text, index)` of each emitted output.
    fn shape(events: &[AgentEvent]) -> Vec<(String, OutputChannel, String, u64)> {
        events
            .iter()
            .map(|event| match event {
                AgentEvent::Output {
                    turn_id,
                    channel,
                    text,
                    chunk_index,
                } => (
                    turn_id.as_str().to_owned(),
                    *channel,
                    text.clone(),
                    *chunk_index,
                ),
                other => panic!("the shaper emits only output, got {other:?}"),
            })
            .collect()
    }

    fn turn(id: &str) -> TurnId {
        TurnId::new(id)
    }

    /// Consecutive deltas of one turn and channel merge into one chunk,
    /// released only on flush — the codex shape, a token at a time.
    #[test]
    fn consecutive_deltas_merge_until_flushed() {
        let mut shaper = OutputShaper::default();
        assert!(
            shaper
                .absorb(turn("t1"), OutputChannel::Assistant, "Hel")
                .is_empty()
        );
        assert!(
            shaper
                .absorb(turn("t1"), OutputChannel::Assistant, "lo, ")
                .is_empty()
        );
        assert!(
            shaper
                .absorb(turn("t1"), OutputChannel::Assistant, "world")
                .is_empty()
        );
        assert_eq!(
            shape(&shaper.flush()),
            vec![(
                "t1".to_owned(),
                OutputChannel::Assistant,
                "Hello, world".to_owned(),
                0
            )]
        );
        assert!(shaper.flush().is_empty(), "nothing is held twice");
    }

    /// A change of channel or turn closes the merge in arrival order, and
    /// each (turn, channel) is numbered densely from zero.
    #[test]
    fn a_new_channel_or_turn_releases_the_merge_in_order() {
        let mut shaper = OutputShaper::default();
        let mut out = shaper.absorb(turn("t1"), OutputChannel::Reasoning, "think");
        out.extend(shaper.absorb(turn("t1"), OutputChannel::Assistant, "say"));
        out.extend(shaper.absorb(turn("t1"), OutputChannel::Reasoning, "more"));
        out.extend(shaper.absorb(turn("t2"), OutputChannel::Reasoning, "next"));
        out.extend(shaper.flush());
        assert_eq!(
            shape(&out),
            vec![
                (
                    "t1".to_owned(),
                    OutputChannel::Reasoning,
                    "think".to_owned(),
                    0
                ),
                (
                    "t1".to_owned(),
                    OutputChannel::Assistant,
                    "say".to_owned(),
                    0
                ),
                (
                    "t1".to_owned(),
                    OutputChannel::Reasoning,
                    "more".to_owned(),
                    1
                ),
                (
                    "t2".to_owned(),
                    OutputChannel::Reasoning,
                    "next".to_owned(),
                    0
                ),
            ]
        );
    }

    /// Oversized text is chunked at the limit — exactly at it, never past —
    /// with the remainder still held, and the pieces rejoin to the original.
    #[test]
    fn oversized_text_is_split_at_the_limit_and_rejoins_whole() {
        let mut shaper = OutputShaper::with_max_chunk(4);
        let ready = shaper.absorb(turn("t1"), OutputChannel::ToolResult, "abcdefghij");
        let mut all = shape(&ready);
        assert_eq!(
            all.iter()
                .map(|(_, _, text, _)| text.as_str())
                .collect::<Vec<_>>(),
            vec!["abcd", "efgh"],
            "full chunks leave at once; the tail waits"
        );
        all.extend(shape(&shaper.flush()));
        assert_eq!(
            all.iter()
                .map(|(_, _, _, index)| *index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        let rejoined: String = all.into_iter().map(|(_, _, text, _)| text).collect();
        assert_eq!(rejoined, "abcdefghij");

        let mut exact = OutputShaper::with_max_chunk(4);
        assert!(
            exact
                .absorb(turn("t1"), OutputChannel::ToolResult, "abcd")
                .is_empty(),
            "text exactly at the limit is not split"
        );
    }

    /// A cut never lands inside a character: multi-byte text is split at the
    /// last boundary that fits.
    #[test]
    fn a_cut_lands_on_a_character_boundary() {
        let mut shaper = OutputShaper::with_max_chunk(5);
        // "é" is two bytes: "aéé" is five, the next "é" would make seven.
        let ready = shaper.absorb(turn("t1"), OutputChannel::Assistant, "aééé");
        let pieces = shape(&ready);
        assert_eq!(pieces.len(), 1);
        assert_eq!(pieces[0].2, "aéé");
        assert_eq!(shape(&shaper.flush())[0].2, "é");
    }

    /// A limit landing inside a character backs the cut up to that
    /// character's start, so no chunk ever exceeds the limit.
    #[test]
    fn a_cut_inside_a_character_backs_up_to_its_start() {
        let mut shaper = OutputShaper::with_max_chunk(4);
        // "a" then two two-byte "é": byte 4 is the middle of the second.
        let ready = shaper.absorb(turn("t1"), OutputChannel::Assistant, "aéé");
        assert_eq!(shape(&ready)[0].2, "aé");
        assert_eq!(shape(&shaper.flush())[0].2, "é");
    }

    /// The floor keeps one character always fitting, and the default is the
    /// inline threshold.
    #[test]
    fn the_limit_has_a_floor_and_a_default() {
        let mut tiny = OutputShaper::with_max_chunk(0);
        let ready = tiny.absorb(turn("t1"), OutputChannel::Assistant, "abcde");
        assert_eq!(shape(&ready)[0].2, "abcd");
        assert_eq!(MAX_CHUNK_BYTES, 64 * 1024);
        assert_eq!(OutputShaper::default().max_chunk_bytes, MAX_CHUNK_BYTES);
    }

    /// A finished turn's numbering is forgotten; another turn's is kept.
    #[test]
    fn a_finished_turn_forgets_its_numbering() {
        let mut shaper = OutputShaper::default();
        shaper.absorb(turn("t1"), OutputChannel::Assistant, "a");
        shaper.absorb(turn("t2"), OutputChannel::Assistant, "b");
        shaper.flush();
        shaper.turn_ended(&turn("t1"));
        assert_eq!(shaper.next_index.len(), 1, "only t2 remains");
        let again = shaper.absorb(turn("t2"), OutputChannel::Assistant, "c");
        assert!(again.is_empty());
        assert_eq!(shape(&shaper.flush())[0].3, 1, "t2 keeps counting");
    }
}
