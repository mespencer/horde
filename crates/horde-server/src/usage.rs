//! Usage accounting: pure folds over the log (DESIGN §12).
//!
//! There is no usage table. Every number this module reports is folded out of
//! the session logs' `UsageRecorded` events on demand, per ADR-0001 — a side
//! table could disagree with the events it was built from, and a fold cannot.
//! The log has already deduplicated the adapters' at-least-once deliveries
//! (worker journal watermarks and per-turn idempotency keys), so summing what
//! is stored is summing what happened.
//!
//! # Measured versus derived
//!
//! Two kinds of number leave this module, and they are never blended:
//!
//! * **Measured** — token sums straight off journaled records, and the latest
//!   provider-reported [`RateWindow`] an adapter journaled for an account.
//! * **Derived** — headroom arithmetic *over* those measurements:
//!   [`remaining_fraction`], [`window_exhausted`], and the Horde-metered
//!   window built from a declared quota. Every derived value travels next to
//!   the measurement it came from, with a [`basis_of`] string naming it.
//!
//! # Window semantics (recorded decision)
//!
//! DESIGN is silent on rolling versus calendar-reset windows. Provider-reported
//! windows carry their own `resets_at` and are passed through untouched — the
//! provider's word is the truth about the provider's window. For Horde's own
//! metering against a [`DeclaredQuota`], the window is a **rolling window of
//! the declared length ending at the query instant** — the simplest honest
//! default: it needs no anchor configuration and invents no reset boundary the
//! provider never declared. Calendar resets remain representable on the wire
//! (`resets_at`); adopting them for metering is a flagged follow-up for when
//! the config layer can say where a calendar window starts.

use std::collections::{BTreeMap, BTreeSet};

use horde_proto::v1::{
    AccountHeadroom, Event, HeadroomSource, ModelBinding, RateWindow, RateWindowUnit, TokenUsage,
    UsageAggregate, UsageGroupKey, event::Kind,
};
use prost::Message;

use crate::error::ServerError;
use crate::log::StoredEvent;

/// How usage rows are grouped — the wire's `UsageGroupBy`, validated.
///
/// A domain enum rather than the prost type so `UNSPECIFIED` is refused once,
/// at the boundary, and every match in the fold is honestly exhaustive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Grouping {
    Agent,
    Session,
    TreePath,
    ProviderAccount,
    Model,
}

impl Grouping {
    /// The wire value as a grouping, or `None` for `UNSPECIFIED` and anything
    /// this build does not know — a caller must say what it wants counted.
    #[must_use]
    pub fn from_wire(value: i32) -> Option<Self> {
        match horde_proto::v1::UsageGroupBy::try_from(value) {
            Ok(horde_proto::v1::UsageGroupBy::Agent) => Some(Self::Agent),
            Ok(horde_proto::v1::UsageGroupBy::Session) => Some(Self::Session),
            Ok(horde_proto::v1::UsageGroupBy::TreePath) => Some(Self::TreePath),
            Ok(horde_proto::v1::UsageGroupBy::ProviderAccount) => Some(Self::ProviderAccount),
            Ok(horde_proto::v1::UsageGroupBy::Model) => Some(Self::Model),
            _ => None,
        }
    }
}

/// What one usage query asks for, beyond which sessions to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageQuery {
    pub grouping: Grouping,
    /// Restrict to one agent's subtree — tree-level attribution. Only the
    /// session holding this agent can contribute anything.
    pub subtree_root: Option<String>,
    /// Inclusive lower bound on a record's log-recorded time.
    pub since_unix_ms: Option<i64>,
    /// Inclusive upper bound on a record's log-recorded time.
    pub until_unix_ms: Option<i64>,
}

impl UsageQuery {
    #[must_use]
    pub fn grouped_by(grouping: Grouping) -> Self {
        Self {
            grouping,
            subtree_root: None,
            since_unix_ms: None,
            until_unix_ms: None,
        }
    }

    #[must_use]
    pub fn scoped_to_subtree(mut self, root: impl Into<String>) -> Self {
        self.subtree_root = Some(root.into());
        self
    }

    #[must_use]
    pub fn since(mut self, unix_ms: i64) -> Self {
        self.since_unix_ms = Some(unix_ms);
        self
    }

    #[must_use]
    pub fn until(mut self, unix_ms: i64) -> Self {
        self.until_unix_ms = Some(unix_ms);
        self
    }
}

/// A per-account declared quota: how much one Provider Account may spend per
/// window when the provider itself reports no rate-limit state.
///
/// The config layer that lets a user author these has not landed; until it
/// does, callers pass an empty quota map and metered windows simply do not
/// appear. The shape is here so the accounting never has to be retrofitted
/// (DESIGN §12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeclaredQuota {
    pub window_ms: i64,
    pub budget_tokens: u64,
}

/// One group's accumulating totals.
#[derive(Debug, Clone, Default, PartialEq)]
struct GroupTotals {
    tokens: TokenUsage,
    /// Distinct contributing agents, as `(session_id, agent_id)`.
    agents: BTreeSet<(String, String)>,
}

/// One account's accumulating state.
#[derive(Debug, Clone, Default, PartialEq)]
struct AccountState {
    tokens: TokenUsage,
    /// The latest journaled rate window, with when the log recorded it.
    latest_window: Option<(i64, RateWindow)>,
    /// Every covered token record, as `(recorded_at_unix_ms, consumed)` —
    /// what a rolling metered window sums over.
    spends: Vec<(i64, u64)>,
}

/// The dimensions one aggregate row groups under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GroupKey {
    Agent {
        session_id: String,
        agent_id: String,
    },
    Session {
        session_id: String,
    },
    TreePath {
        tree_path: Vec<String>,
    },
    ProviderAccount {
        provider: String,
        account: String,
    },
    Model {
        provider: String,
        model: String,
    },
}

impl GroupKey {
    /// The key a record files under, given the query's grouping.
    fn of(grouping: Grouping, session_id: &str, agent_id: &str, binding: &ModelBinding) -> Self {
        match grouping {
            Grouping::Agent => Self::Agent {
                session_id: session_id.to_owned(),
                agent_id: agent_id.to_owned(),
            },
            Grouping::Session => Self::Session {
                session_id: session_id.to_owned(),
            },
            Grouping::TreePath => Self::TreePath {
                tree_path: Vec::new(),
            },
            Grouping::ProviderAccount => Self::ProviderAccount {
                provider: binding.provider.clone(),
                account: binding.provider_account.clone(),
            },
            Grouping::Model => Self::Model {
                provider: binding.provider.clone(),
                model: binding.model.clone(),
            },
        }
    }

    /// The wire form: only the fields this grouping implies are populated.
    fn to_wire(&self) -> UsageGroupKey {
        let mut key = UsageGroupKey::default();
        match self {
            Self::Agent {
                session_id,
                agent_id,
            } => {
                key.session_id = Some(horde_proto::v1::SessionId::new(session_id.clone()));
                key.agent_id = Some(horde_proto::v1::AgentId::new(agent_id.clone()));
            }
            Self::Session { session_id } => {
                key.session_id = Some(horde_proto::v1::SessionId::new(session_id.clone()));
            }
            Self::TreePath { tree_path } => {
                key.tree_path = tree_path.clone();
            }
            Self::ProviderAccount { provider, account } => {
                key.provider = provider.clone();
                key.provider_account = account.clone();
            }
            Self::Model { provider, model } => {
                key.provider = provider.clone();
                key.model = model.clone();
            }
        }
        key
    }
}

/// The usage fold: absorb sessions, then read aggregates and account rows.
#[derive(Debug, Clone, PartialEq)]
pub struct UsageFold {
    query: UsageQuery,
    groups: BTreeMap<GroupKey, GroupTotals>,
    accounts: BTreeMap<(String, String), AccountState>,
}

impl UsageFold {
    #[must_use]
    pub fn new(query: UsageQuery) -> Self {
        Self {
            query,
            groups: BTreeMap::new(),
            accounts: BTreeMap::new(),
        }
    }

    /// Folds one session's events in.
    ///
    /// Two passes over the same slice: ancestry first, records second, so a
    /// record is never mis-scoped by a spawn the replay had not reached yet.
    ///
    /// # Errors
    ///
    /// Whatever [`Event`] decoding refuses — a corrupt row is a storage
    /// problem to report, not a record to skip silently.
    pub fn absorb_session(
        &mut self,
        session_id: &str,
        events: &[StoredEvent],
    ) -> Result<(), ServerError> {
        let mut ancestry = Ancestry::default();
        let mut records = Vec::new();
        for stored in events {
            let event = Event::decode(stored.payload.as_slice())?;
            let agent_id = event
                .agent_id
                .as_ref()
                .map_or_else(String::new, ToString::to_string);
            match event.kind {
                Some(Kind::AgentSpawned(spawned)) => {
                    ancestry.learn(&agent_id, spawned);
                }
                Some(Kind::UsageRecorded(recorded)) => {
                    if let Some(record) = recorded.record {
                        records.push((agent_id, stored.recorded_at_unix_ms, record));
                    }
                }
                _ => {}
            }
        }

        for (agent_id, recorded_at, record) in records {
            if !self.covers(&ancestry, &agent_id, recorded_at) {
                continue;
            }
            self.absorb_record(session_id, &ancestry, &agent_id, recorded_at, &record);
        }
        Ok(())
    }

    /// Whether the query covers this record at all: inside the time range,
    /// and — when the query scopes to a subtree — journaled for an agent that
    /// subtree can prove it holds.
    fn covers(&self, ancestry: &Ancestry, agent_id: &str, recorded_at_unix_ms: i64) -> bool {
        let in_range = within_range(
            recorded_at_unix_ms,
            self.query.since_unix_ms,
            self.query.until_unix_ms,
        );
        let in_scope = match self.query.subtree_root.as_deref() {
            None => true,
            Some(root) => ancestry.in_subtree(root, agent_id),
        };
        in_range && in_scope
    }

    fn absorb_record(
        &mut self,
        session_id: &str,
        ancestry: &Ancestry,
        agent_id: &str,
        recorded_at_unix_ms: i64,
        record: &horde_proto::v1::UsageRecord,
    ) {
        let binding = record.binding.clone().unwrap_or_default();
        let account_key = (binding.provider.clone(), binding.provider_account.clone());

        if let Some(window) = &record.rate_window {
            let account = self.accounts.entry(account_key.clone()).or_default();
            if newer_window(account.latest_window.as_ref(), recorded_at_unix_ms) {
                account.latest_window = Some((recorded_at_unix_ms, *window));
            }
        }

        let Some(tokens) = &record.tokens else {
            // A headroom-only record spends nothing and counts no agent.
            return;
        };

        let key = self.group_key(session_id, ancestry, agent_id, &binding);
        let group = self.groups.entry(key).or_default();
        accumulate_tokens(&mut group.tokens, tokens);
        group
            .agents
            .insert((session_id.to_owned(), agent_id.to_owned()));

        let account = self.accounts.entry(account_key).or_default();
        accumulate_tokens(&mut account.tokens, tokens);
        account
            .spends
            .push((recorded_at_unix_ms, consumed_tokens(tokens)));
    }

    /// The key one record files under — [`GroupKey::of`], plus the tree path
    /// lookup only the fold's ancestry can answer.
    fn group_key(
        &self,
        session_id: &str,
        ancestry: &Ancestry,
        agent_id: &str,
        binding: &ModelBinding,
    ) -> GroupKey {
        match GroupKey::of(self.query.grouping, session_id, agent_id, binding) {
            GroupKey::TreePath { .. } => GroupKey::TreePath {
                tree_path: ancestry.tree_path(agent_id),
            },
            key => key,
        }
    }

    /// The aggregate rows, in key order — same fold, same order, every time.
    #[must_use]
    pub fn aggregates(&self) -> Vec<UsageAggregate> {
        self.groups
            .iter()
            .map(|(key, totals)| UsageAggregate {
                key: Some(key.to_wire()),
                tokens: Some(totals.tokens),
                agent_count: totals.agents.len() as u64,
            })
            .collect()
    }

    /// The per-account rows: measured window and token sums beside the
    /// headroom derived from them, as of `now_unix_ms`.
    ///
    /// An account whose provider reported no window falls back to Horde's own
    /// metering when a [`DeclaredQuota`] names it; the provider's word wins
    /// whenever there is one (DESIGN §5).
    #[must_use]
    pub fn accounts(
        &self,
        now_unix_ms: i64,
        quotas: &BTreeMap<(String, String), DeclaredQuota>,
    ) -> Vec<AccountHeadroom> {
        self.accounts
            .iter()
            .map(|(key, state)| {
                let window = state
                    .latest_window
                    .as_ref()
                    .map(|(_, window)| *window)
                    .or_else(|| {
                        quotas.get(key).map(|quota| {
                            metered_window(
                                quota,
                                spent_within(&state.spends, quota.window_ms, now_unix_ms),
                            )
                        })
                    });
                AccountHeadroom {
                    provider: key.0.clone(),
                    account: key.1.clone(),
                    remaining_fraction: window.as_ref().and_then(remaining_fraction),
                    exhausted: window
                        .as_ref()
                        .is_some_and(|window| window_exhausted(window, now_unix_ms)),
                    basis: basis_of(window.as_ref()),
                    window,
                    tokens: Some(state.tokens),
                }
            })
            .collect()
    }
}

/// Which agents a session holds, and where each sits in the tree.
#[derive(Debug, Clone, Default)]
struct Ancestry {
    agents: BTreeMap<String, (Option<String>, Vec<String>)>,
}

impl Ancestry {
    fn learn(&mut self, agent_id: &str, spawned: horde_proto::v1::AgentSpawned) {
        let parent = spawned.parent_agent_id.as_ref().map(ToString::to_string);
        self.agents
            .insert(agent_id.to_owned(), (parent, spawned.tree_path));
    }

    fn tree_path(&self, agent_id: &str) -> Vec<String> {
        self.agents
            .get(agent_id)
            .map(|(_, tree_path)| tree_path.clone())
            .unwrap_or_default()
    }

    /// Whether `agent_id` sits in the subtree rooted at `root`.
    ///
    /// An agent the session has no spawn record for is outside every subtree:
    /// attribution must never be claimed by a scope that cannot prove it. The
    /// walk is bounded by count, not clock — a malformed parent chain could
    /// cycle, and each legitimate hop consumes one distinct known agent.
    fn in_subtree(&self, root: &str, agent_id: &str) -> bool {
        let mut current = agent_id.to_owned();
        for _ in 0..=self.agents.len() {
            if current == root {
                return true;
            }
            match self
                .agents
                .get(&current)
                .and_then(|(parent, _)| parent.clone())
            {
                Some(parent) => current = parent,
                None => return false,
            }
        }
        false
    }
}

/// Whether a record's time sits inside the query's inclusive range.
///
/// A named predicate: the two bounds are independently optional, and inline
/// `&&` in the fold would leave the mixed corners unreachable from any staged
/// query (docs/QUALITY_GATES.md on compound conditions).
#[must_use]
pub fn within_range(unix_ms: i64, since_unix_ms: Option<i64>, until_unix_ms: Option<i64>) -> bool {
    let after_since = since_unix_ms.is_none_or(|since| unix_ms >= since);
    let before_until = until_unix_ms.is_none_or(|until| unix_ms <= until);
    after_since && before_until
}

/// Whether a window recorded at `recorded_at_unix_ms` replaces the one held.
///
/// `>=` rather than `>`: two windows stamped the same millisecond are ordered
/// by log position, and this fold absorbs in log order, so the later append
/// must win.
fn newer_window(held: Option<&(i64, RateWindow)>, recorded_at_unix_ms: i64) -> bool {
    held.is_none_or(|(held_at, _)| recorded_at_unix_ms >= *held_at)
}

/// Adds one record's tokens into a running sum, field by field, saturating.
pub fn accumulate_tokens(into: &mut TokenUsage, tokens: &TokenUsage) {
    into.input_tokens = into.input_tokens.saturating_add(tokens.input_tokens);
    into.output_tokens = into.output_tokens.saturating_add(tokens.output_tokens);
    into.cache_read_input_tokens = into
        .cache_read_input_tokens
        .saturating_add(tokens.cache_read_input_tokens);
    into.cache_write_input_tokens = into
        .cache_write_input_tokens
        .saturating_add(tokens.cache_write_input_tokens);
    into.reasoning_output_tokens = into
        .reasoning_output_tokens
        .saturating_add(tokens.reasoning_output_tokens);
}

/// What one record spends against a declared quota: every consumption field,
/// excluding `reasoning_output_tokens` — the proto marks reasoning a breakdown
/// of output, never an addend, and adding it would double-count on providers
/// that fold reasoning into output.
#[must_use]
pub fn consumed_tokens(tokens: &TokenUsage) -> u64 {
    tokens
        .input_tokens
        .saturating_add(tokens.output_tokens)
        .saturating_add(tokens.cache_read_input_tokens)
        .saturating_add(tokens.cache_write_input_tokens)
}

/// Tokens spent inside the rolling window `(now - window_ms, now]`.
fn spent_within(spends: &[(i64, u64)], window_ms: i64, now_unix_ms: i64) -> u64 {
    let floor = now_unix_ms.saturating_sub(window_ms);
    spends
        .iter()
        .filter(|(at, _)| *at > floor && *at <= now_unix_ms)
        .fold(0_u64, |sum, (_, spent)| sum.saturating_add(*spent))
}

/// A Horde-metered rolling window over a declared quota — see the module doc's
/// recorded decision on window semantics. No `resets_at`: a rolling window has
/// no reset boundary, and inventing one would be a calendar the user never
/// declared.
#[must_use]
pub fn metered_window(quota: &DeclaredQuota, used_tokens: u64) -> RateWindow {
    RateWindow {
        source: HeadroomSource::HordeMetered as i32,
        unit: RateWindowUnit::Tokens as i32,
        used: Some(used_tokens),
        limit: Some(quota.budget_tokens),
        window_length: Some(horde_proto::prost_types::Duration {
            seconds: quota.window_ms / 1_000,
            nanos: i32::try_from((quota.window_ms % 1_000) * 1_000_000).unwrap_or(0),
        }),
        resets_at: None,
        used_fraction: None,
        allowed: None,
    }
}

/// Derived: the fraction of a window remaining, 0.0–1.0, when the window
/// carries anything it can be derived from.
///
/// Counts win over a fraction when a window carries both — they carry strictly
/// more information (the ceiling). A published ceiling of zero admits nothing,
/// so it is 0.0 remaining rather than a division by zero. `None` is "unknown",
/// never zero: a serving-state-only window (Claude's rate-limit event shape)
/// has no quantity to derive from.
#[must_use]
pub fn remaining_fraction(window: &RateWindow) -> Option<f64> {
    if let (Some(used), Some(limit)) = (window.used, window.limit) {
        if limit == 0 {
            return Some(0.0);
        }
        let used = used as f64;
        let limit = limit as f64;
        return Some((1.0 - used / limit).clamp(0.0, 1.0));
    }
    window
        .used_fraction
        .map(|fraction| (1.0 - fraction).clamp(0.0, 1.0))
}

/// Whether a window's own reset moment has arrived.
///
/// A window with no `resets_at` never self-expires — the provider said nothing
/// about when it ends, and inventing an expiry would un-exhaust an account on
/// Horde's schedule rather than the provider's.
#[must_use]
pub fn reset_has_passed(window: &RateWindow, now_unix_ms: i64) -> bool {
    window
        .resets_at
        .as_ref()
        .is_some_and(|resets_at| now_unix_ms >= timestamp_unix_ms(resets_at))
}

/// Derived, and a named predicate because the scheduler's queue reasons hang
/// off it: an account is exhausted when its window says nothing is left —
/// `allowed == false`, or a derivable remaining fraction of zero — and the
/// window's own reset has not yet arrived. A window past its reset is never
/// exhausted, however empty it reported itself: the provider's own clock says
/// the budget came back.
#[must_use]
pub fn window_exhausted(window: &RateWindow, now_unix_ms: i64) -> bool {
    if reset_has_passed(window, now_unix_ms) {
        return false;
    }
    if window.allowed == Some(false) {
        return true;
    }
    matches!(remaining_fraction(window), Some(remaining) if remaining <= 0.0)
}

/// Words for where an account row's derived numbers came from, so a reader
/// can always tell a provider's own number from Horde's arithmetic over it.
#[must_use]
pub fn basis_of(window: Option<&RateWindow>) -> String {
    let Some(window) = window else {
        return "no rate window journaled".to_owned();
    };
    let source = match HeadroomSource::try_from(window.source) {
        Ok(HeadroomSource::ProviderReported) => "provider-reported",
        Ok(HeadroomSource::HordeMetered) => "horde-metered",
        _ => "unattributed",
    };
    if window.used.is_some() && window.limit.is_some() {
        format!("derived from {source} counts")
    } else if window.used_fraction.is_some() {
        format!("derived from {source} fraction")
    } else if window.allowed.is_some() {
        format!("{source} serving state only")
    } else {
        format!("{source} window carries no quantity")
    }
}

/// A prost timestamp in unix milliseconds, saturating rather than wrapping.
#[must_use]
pub fn timestamp_unix_ms(timestamp: &horde_proto::prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .saturating_mul(1_000)
        .saturating_add(i64::from(timestamp.nanos) / 1_000_000)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use horde_proto::prost_types::Timestamp;
    use horde_proto::v1::{
        AgentId, AgentSpawned, Event, HeadroomSource, ModelBinding, RateWindow, TokenUsage,
        UsageRecord, UsageRecorded, event::Kind,
    };
    use prost::Message;

    use super::{
        DeclaredQuota, Grouping, UsageFold, UsageQuery, accumulate_tokens, basis_of,
        consumed_tokens, metered_window, remaining_fraction, reset_has_passed, spent_within,
        timestamp_unix_ms, window_exhausted, within_range,
    };
    use crate::log::StoredEvent;

    fn stored(sequence: u64, recorded_at_unix_ms: i64, agent: &str, kind: Kind) -> StoredEvent {
        StoredEvent {
            session_id: "s1".to_owned(),
            sequence,
            recorded_at_unix_ms,
            payload: Event {
                agent_id: (!agent.is_empty()).then(|| AgentId::new(agent)),
                occurred_at: None,
                idempotency_key: String::new(),
                kind: Some(kind),
            }
            .encode_to_vec(),
        }
    }

    fn spawned(parent: Option<&str>, tree_path: &[&str]) -> Kind {
        Kind::AgentSpawned(AgentSpawned {
            parent_agent_id: parent.map(AgentId::new),
            tree_path: tree_path.iter().map(ToString::to_string).collect(),
            ..AgentSpawned::default()
        })
    }

    fn binding(provider: &str, account: &str, model: &str) -> ModelBinding {
        ModelBinding {
            tier: String::new(),
            provider: provider.to_owned(),
            model: model.to_owned(),
            provider_account: account.to_owned(),
        }
    }

    fn tokens(input: u64, output: u64) -> TokenUsage {
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            ..TokenUsage::default()
        }
    }

    fn usage(binding: Option<ModelBinding>, tokens: Option<TokenUsage>) -> Kind {
        Kind::UsageRecorded(UsageRecorded {
            record: Some(UsageRecord {
                binding,
                tokens,
                rate_window: None,
                turn_id: String::new(),
            }),
        })
    }

    fn headroom_report(binding: ModelBinding, window: RateWindow) -> Kind {
        Kind::UsageRecorded(UsageRecorded {
            record: Some(UsageRecord {
                binding: Some(binding),
                tokens: None,
                rate_window: Some(window),
                turn_id: String::new(),
            }),
        })
    }

    fn counted_window(used: u64, limit: u64) -> RateWindow {
        RateWindow {
            source: HeadroomSource::ProviderReported as i32,
            used: Some(used),
            limit: Some(limit),
            ..RateWindow::default()
        }
    }

    fn fold_of(query: UsageQuery, events: &[StoredEvent]) -> UsageFold {
        let mut fold = UsageFold::new(query);
        fold.absorb_session("s1", events).expect("a clean fold");
        fold
    }

    fn id_of(id: Option<&horde_proto::v1::SessionId>) -> Option<String> {
        id.map(ToString::to_string)
    }

    fn agent_of(id: Option<&horde_proto::v1::AgentId>) -> Option<String> {
        id.map(ToString::to_string)
    }

    /// A small session: root spawns a child, both spend, the child reports
    /// headroom.
    fn spending_session() -> Vec<StoredEvent> {
        vec![
            stored(1, 10, "root", spawned(None, &["ci-fix"])),
            stored(
                2,
                20,
                "child",
                spawned(Some("root"), &["ci-fix", "reviewer"]),
            ),
            stored(
                3,
                30,
                "root",
                usage(
                    Some(binding("claude", "personal", "sonnet")),
                    Some(tokens(100, 40)),
                ),
            ),
            stored(
                4,
                40,
                "child",
                usage(
                    Some(binding("claude", "personal", "haiku")),
                    Some(tokens(10, 5)),
                ),
            ),
            stored(
                5,
                50,
                "child",
                usage(Some(binding("codex", "team", "")), Some(tokens(1_000, 200))),
            ),
            stored(
                6,
                60,
                "child",
                headroom_report(
                    binding("claude", "personal", "haiku"),
                    counted_window(75, 100),
                ),
            ),
        ]
    }

    #[test]
    fn grouping_by_account_sums_tokens_and_counts_distinct_agents() {
        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::ProviderAccount),
            &spending_session(),
        );
        let aggregates = fold.aggregates();
        assert_eq!(aggregates.len(), 2);

        let claude = &aggregates[0];
        let key = claude.key.as_ref().expect("a key");
        assert_eq!(key.provider, "claude");
        assert_eq!(key.provider_account, "personal");
        assert_eq!(
            key.session_id, None,
            "only the implied fields are populated"
        );
        assert_eq!(claude.tokens.as_ref().expect("tokens"), &tokens(110, 45));
        assert_eq!(claude.agent_count, 2, "root and child both spent here");

        let codex = &aggregates[1];
        assert_eq!(codex.key.as_ref().expect("a key").provider, "codex");
        assert_eq!(codex.tokens.as_ref().expect("tokens"), &tokens(1_000, 200));
        assert_eq!(codex.agent_count, 1);
    }

    #[test]
    fn grouping_by_agent_keys_on_session_and_agent() {
        let fold = fold_of(UsageQuery::grouped_by(Grouping::Agent), &spending_session());
        let aggregates = fold.aggregates();
        assert_eq!(aggregates.len(), 2);

        let child = &aggregates[0];
        let key = child.key.as_ref().expect("a key");
        assert_eq!(id_of(key.session_id.as_ref()).as_deref(), Some("s1"));
        assert_eq!(agent_of(key.agent_id.as_ref()).as_deref(), Some("child"));
        assert_eq!(
            child.tokens.as_ref().expect("tokens"),
            &tokens(1_010, 205),
            "the child's two turns sum across providers"
        );
        assert_eq!(child.agent_count, 1);

        let root_key = aggregates[1].key.as_ref().expect("a key");
        assert_eq!(
            agent_of(root_key.agent_id.as_ref()).as_deref(),
            Some("root")
        );
        assert_eq!(
            aggregates[1].tokens.as_ref().expect("tokens"),
            &tokens(100, 40)
        );
    }

    #[test]
    fn grouping_by_session_folds_everything_into_one_row() {
        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::Session),
            &spending_session(),
        );
        let aggregates = fold.aggregates();
        assert_eq!(aggregates.len(), 1);
        let row = &aggregates[0];
        let key = row.key.as_ref().expect("a key");
        assert_eq!(id_of(key.session_id.as_ref()).as_deref(), Some("s1"));
        assert_eq!(row.tokens.as_ref().expect("tokens"), &tokens(1_110, 245));
        assert_eq!(row.agent_count, 2);
    }

    #[test]
    fn grouping_by_model_keys_on_provider_and_model() {
        let fold = fold_of(UsageQuery::grouped_by(Grouping::Model), &spending_session());
        let described: Vec<(String, String, u64)> = fold
            .aggregates()
            .iter()
            .map(|aggregate| {
                let key = aggregate.key.clone().unwrap_or_default();
                (
                    key.provider,
                    key.model,
                    aggregate.tokens.unwrap_or_default().input_tokens,
                )
            })
            .collect();
        assert_eq!(
            described,
            vec![
                ("claude".to_owned(), "haiku".to_owned(), 10),
                ("claude".to_owned(), "sonnet".to_owned(), 100),
                ("codex".to_owned(), String::new(), 1_000),
            ]
        );
    }

    #[test]
    fn grouping_by_tree_path_uses_the_spawned_ancestry() {
        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::TreePath),
            &spending_session(),
        );
        let paths: Vec<Vec<String>> = fold
            .aggregates()
            .iter()
            .map(|aggregate| aggregate.key.clone().unwrap_or_default().tree_path)
            .collect();
        assert_eq!(
            paths,
            vec![
                vec!["ci-fix".to_owned()],
                vec!["ci-fix".to_owned(), "reviewer".to_owned()],
            ]
        );
    }

    #[test]
    fn a_record_with_no_binding_still_counts_under_empty_dimensions() {
        let events = vec![stored(1, 10, "root", usage(None, Some(tokens(7, 3))))];
        let fold = fold_of(UsageQuery::grouped_by(Grouping::ProviderAccount), &events);
        let aggregates = fold.aggregates();
        assert_eq!(aggregates.len(), 1);
        let key = aggregates[0].key.as_ref().expect("a key");
        assert_eq!(
            (key.provider.as_str(), key.provider_account.as_str()),
            ("", "")
        );
        assert_eq!(
            aggregates[0].tokens.as_ref().expect("tokens"),
            &tokens(7, 3)
        );
    }

    #[test]
    fn a_headroom_only_record_moves_the_window_but_spends_nothing() {
        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::ProviderAccount),
            &spending_session(),
        );
        let accounts = fold.accounts(1_000, &BTreeMap::new());
        assert_eq!(accounts.len(), 2);

        let claude = &accounts[0];
        assert_eq!(
            (claude.provider.as_str(), claude.account.as_str()),
            ("claude", "personal")
        );
        assert_eq!(
            claude.tokens.as_ref().expect("measured tokens"),
            &tokens(110, 45),
            "the headroom report added no tokens"
        );
        let window = claude.window.as_ref().expect("the journaled window");
        assert_eq!((window.used, window.limit), (Some(75), Some(100)));
        assert_eq!(claude.remaining_fraction, Some(0.25));
        assert!(!claude.exhausted);
        assert_eq!(claude.basis, "derived from provider-reported counts");

        let codex = &accounts[1];
        assert_eq!(codex.window, None);
        assert_eq!(codex.remaining_fraction, None, "unknown is not zero");
        assert!(!codex.exhausted);
        assert_eq!(codex.basis, "no rate window journaled");
    }

    /// Folds one headroom report per session — `(recorded_at, used)` — and
    /// answers which `used` the account's window ended up holding.
    fn surviving_window_used(reports: &[(i64, u64)]) -> Option<u64> {
        let mut fold = UsageFold::new(UsageQuery::grouped_by(Grouping::ProviderAccount));
        for (index, (recorded_at, used)) in reports.iter().enumerate() {
            let report = headroom_report(
                binding("claude", "personal", ""),
                counted_window(*used, 100),
            );
            fold.absorb_session(
                &format!("s{index}"),
                &[stored(1, *recorded_at, "a", report)],
            )
            .expect("a clean fold");
        }
        fold.accounts(0, &BTreeMap::new())
            .first()
            .and_then(|account| account.window.as_ref().and_then(|window| window.used))
    }

    #[test]
    fn the_latest_journaled_window_wins_and_ties_go_to_the_later_append() {
        assert_eq!(
            surviving_window_used(&[(100, 10), (100, 50)]),
            Some(50),
            "same stamp: the later append wins"
        );
        assert_eq!(
            surviving_window_used(&[(200, 90), (100, 10)]),
            Some(90),
            "an older-stamped window from another session must not replace a newer one"
        );
    }

    #[test]
    fn the_time_range_covers_inclusively_and_drops_the_rest() {
        let spend = |sequence, at| {
            stored(
                sequence,
                at,
                "root",
                usage(Some(binding("claude", "p", "")), Some(tokens(1, 0))),
            )
        };
        let events = vec![spend(1, 10), spend(2, 20), spend(3, 30), spend(4, 40)];

        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::Session)
                .since(20)
                .until(30),
            &events,
        );
        let aggregates = fold.aggregates();
        assert_eq!(
            aggregates[0].tokens.as_ref().expect("tokens").input_tokens,
            2,
            "both boundary records are inside the range"
        );
    }

    #[test]
    fn a_subtree_scope_covers_the_root_and_its_descendants_only() {
        let events = vec![
            stored(1, 10, "root", spawned(None, &["ci-fix"])),
            stored(
                2,
                11,
                "child",
                spawned(Some("root"), &["ci-fix", "reviewer"]),
            ),
            stored(3, 12, "grandchild", spawned(Some("child"), &[])),
            stored(4, 13, "sibling", spawned(Some("root"), &[])),
            stored(5, 20, "child", usage(None, Some(tokens(1, 0)))),
            stored(6, 21, "grandchild", usage(None, Some(tokens(2, 0)))),
            stored(7, 22, "sibling", usage(None, Some(tokens(4, 0)))),
            stored(8, 23, "root", usage(None, Some(tokens(8, 0)))),
            stored(9, 24, "unspawned", usage(None, Some(tokens(16, 0)))),
        ];

        let scoped = fold_of(
            UsageQuery::grouped_by(Grouping::Session).scoped_to_subtree("child"),
            &events,
        );
        assert_eq!(
            scoped.aggregates()[0]
                .tokens
                .as_ref()
                .expect("tokens")
                .input_tokens,
            3,
            "the subtree is the child and the grandchild; the sibling, the \
             root, and an agent nobody spawned are outside it"
        );

        let unscoped = fold_of(UsageQuery::grouped_by(Grouping::Session), &events);
        assert_eq!(
            unscoped.aggregates()[0]
                .tokens
                .as_ref()
                .expect("tokens")
                .input_tokens,
            31,
            "unscoped covers everything, including the unspawned agent"
        );
    }

    #[test]
    fn a_metered_quota_answers_only_when_no_provider_window_exists() {
        let mut quotas = BTreeMap::new();
        quotas.insert(
            ("claude".to_owned(), "personal".to_owned()),
            DeclaredQuota {
                window_ms: 1_000,
                budget_tokens: 300,
            },
        );
        quotas.insert(
            ("codex".to_owned(), "team".to_owned()),
            DeclaredQuota {
                window_ms: 1_000,
                budget_tokens: 2_000,
            },
        );

        let fold = fold_of(
            UsageQuery::grouped_by(Grouping::ProviderAccount),
            &spending_session(),
        );
        let accounts = fold.accounts(500, &quotas);

        let claude = &accounts[0];
        assert_eq!(
            claude.window.as_ref().and_then(|window| window.used),
            Some(75),
            "the provider's own window wins over the declared quota"
        );

        let codex = &accounts[1];
        let window = codex.window.as_ref().expect("a metered window");
        assert_eq!(window.source, HeadroomSource::HordeMetered as i32);
        assert_eq!(window.used, Some(1_200), "input + output within the window");
        assert_eq!(window.limit, Some(2_000));
        assert_eq!(codex.remaining_fraction, Some(0.4));
        assert!(!codex.exhausted);
        assert_eq!(codex.basis, "derived from horde-metered counts");
    }

    #[test]
    fn a_corrupt_row_is_a_decode_error_not_a_skipped_record() {
        let corrupt = StoredEvent {
            session_id: "s1".to_owned(),
            sequence: 1,
            recorded_at_unix_ms: 0,
            payload: vec![0xff, 0xff, 0xff],
        };
        let mut fold = UsageFold::new(UsageQuery::grouped_by(Grouping::Session));
        assert!(matches!(
            fold.absorb_session("s1", &[corrupt]),
            Err(crate::error::ServerError::Decode(_))
        ));
    }

    #[test]
    fn group_by_wire_values_map_and_unspecified_is_refused() {
        use horde_proto::v1::UsageGroupBy;
        assert_eq!(
            Grouping::from_wire(UsageGroupBy::Agent as i32),
            Some(Grouping::Agent)
        );
        assert_eq!(
            Grouping::from_wire(UsageGroupBy::Session as i32),
            Some(Grouping::Session)
        );
        assert_eq!(
            Grouping::from_wire(UsageGroupBy::TreePath as i32),
            Some(Grouping::TreePath)
        );
        assert_eq!(
            Grouping::from_wire(UsageGroupBy::ProviderAccount as i32),
            Some(Grouping::ProviderAccount)
        );
        assert_eq!(
            Grouping::from_wire(UsageGroupBy::Model as i32),
            Some(Grouping::Model)
        );
        assert_eq!(Grouping::from_wire(UsageGroupBy::Unspecified as i32), None);
        assert_eq!(Grouping::from_wire(887), None);
    }

    // -- The arithmetic, corner by corner ---------------------------------

    #[test]
    fn the_time_range_predicate_holds_at_all_four_corners() {
        assert!(within_range(5, None, None));
        assert!(within_range(5, Some(5), None), "since is inclusive");
        assert!(!within_range(4, Some(5), None));
        assert!(within_range(5, None, Some(5)), "until is inclusive");
        assert!(!within_range(6, None, Some(5)));
        assert!(within_range(5, Some(5), Some(5)));
        assert!(!within_range(4, Some(5), Some(6)));
        assert!(!within_range(7, Some(5), Some(6)));
    }

    #[test]
    fn accumulation_adds_every_field_and_saturates() {
        let mut sum = TokenUsage::default();
        accumulate_tokens(
            &mut sum,
            &TokenUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_read_input_tokens: 3,
                cache_write_input_tokens: 4,
                reasoning_output_tokens: 5,
            },
        );
        accumulate_tokens(
            &mut sum,
            &TokenUsage {
                input_tokens: 10,
                output_tokens: 20,
                cache_read_input_tokens: 30,
                cache_write_input_tokens: 40,
                reasoning_output_tokens: 50,
            },
        );
        assert_eq!(
            sum,
            TokenUsage {
                input_tokens: 11,
                output_tokens: 22,
                cache_read_input_tokens: 33,
                cache_write_input_tokens: 44,
                reasoning_output_tokens: 55,
            }
        );

        let mut nearly_full = TokenUsage {
            input_tokens: u64::MAX,
            ..TokenUsage::default()
        };
        accumulate_tokens(&mut nearly_full, &tokens(1, 0));
        assert_eq!(nearly_full.input_tokens, u64::MAX, "saturates, never wraps");
    }

    #[test]
    fn consumption_counts_every_input_and_output_but_never_reasoning() {
        assert_eq!(
            consumed_tokens(&TokenUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_read_input_tokens: 4,
                cache_write_input_tokens: 8,
                reasoning_output_tokens: 16,
            }),
            15,
            "reasoning is a breakdown of output, never an addend"
        );
        assert_eq!(consumed_tokens(&TokenUsage::default()), 0);
    }

    #[test]
    fn the_rolling_window_takes_spends_after_the_floor_up_to_now() {
        let spends = [(10, 1_u64), (11, 2), (50, 4), (60, 8), (61, 16)];
        assert_eq!(
            spent_within(&spends, 50, 60),
            14,
            "the floor itself (now - window = 10) is outside; now is inside; \
             later than now is outside"
        );
        assert_eq!(spent_within(&spends, i64::MAX, 61), 31, "saturating floor");
        assert_eq!(spent_within(&[], 100, 100), 0);
    }

    #[test]
    fn a_metered_window_says_what_it_is_and_carries_no_reset() {
        let window = metered_window(
            &DeclaredQuota {
                window_ms: 90_500,
                budget_tokens: 1_000,
            },
            250,
        );
        assert_eq!(window.source, HeadroomSource::HordeMetered as i32);
        assert_eq!(window.unit, horde_proto::v1::RateWindowUnit::Tokens as i32);
        assert_eq!((window.used, window.limit), (Some(250), Some(1_000)));
        let length = window.window_length.expect("the declared length");
        assert_eq!((length.seconds, length.nanos), (90, 500_000_000));
        assert_eq!(
            window.resets_at, None,
            "a rolling window has no reset boundary"
        );
        assert_eq!(window.used_fraction, None);
        assert_eq!(window.allowed, None);
    }

    #[test]
    fn remaining_fraction_prefers_counts_and_clamps_both_shapes() {
        assert_eq!(remaining_fraction(&counted_window(75, 100)), Some(0.25));
        assert_eq!(remaining_fraction(&counted_window(0, 100)), Some(1.0));
        assert_eq!(
            remaining_fraction(&counted_window(150, 100)),
            Some(0.0),
            "over the limit clamps rather than going negative"
        );
        assert_eq!(
            remaining_fraction(&counted_window(0, 0)),
            Some(0.0),
            "a published ceiling of zero admits nothing"
        );

        let fraction_only = RateWindow {
            used_fraction: Some(0.4),
            ..RateWindow::default()
        };
        assert_eq!(remaining_fraction(&fraction_only), Some(0.6));
        let overspent = RateWindow {
            used_fraction: Some(1.5),
            ..RateWindow::default()
        };
        assert_eq!(remaining_fraction(&overspent), Some(0.0));
        let negative = RateWindow {
            used_fraction: Some(-0.5),
            ..RateWindow::default()
        };
        assert_eq!(
            remaining_fraction(&negative),
            Some(1.0),
            "clamped above too"
        );

        let both = RateWindow {
            used_fraction: Some(0.9),
            ..counted_window(10, 100)
        };
        assert_eq!(
            remaining_fraction(&both),
            Some(0.9),
            "counts carry the ceiling, so they win over a disagreeing fraction"
        );

        assert_eq!(remaining_fraction(&RateWindow::default()), None);
        let serving_only = RateWindow {
            allowed: Some(true),
            ..RateWindow::default()
        };
        assert_eq!(
            remaining_fraction(&serving_only),
            None,
            "unknown, never zero"
        );
    }

    fn resetting(at_unix_ms: i64) -> Timestamp {
        Timestamp {
            seconds: at_unix_ms / 1_000,
            nanos: i32::try_from((at_unix_ms % 1_000) * 1_000_000).unwrap_or(0),
        }
    }

    #[test]
    fn a_reset_passes_at_its_own_moment_and_never_without_one() {
        let window = RateWindow {
            resets_at: Some(resetting(5_000)),
            ..RateWindow::default()
        };
        assert!(!reset_has_passed(&window, 4_999));
        assert!(reset_has_passed(&window, 5_000), "the moment itself counts");
        assert!(reset_has_passed(&window, 5_001));
        assert!(
            !reset_has_passed(&RateWindow::default(), i64::MAX),
            "no resets_at never self-expires"
        );
    }

    #[test]
    fn exhaustion_needs_an_empty_window_whose_reset_has_not_arrived() {
        let spent = counted_window(100, 100);
        assert!(window_exhausted(&spent, 0), "zero remaining is exhausted");
        assert!(
            !window_exhausted(&counted_window(99, 100), 0),
            "a sliver remaining is not"
        );

        let refusing = RateWindow {
            allowed: Some(false),
            ..RateWindow::default()
        };
        assert!(
            window_exhausted(&refusing, 0),
            "allowed=false is exhaustion even with no quantity"
        );
        let serving = RateWindow {
            allowed: Some(true),
            ..RateWindow::default()
        };
        assert!(!window_exhausted(&serving, 0));
        assert!(
            !window_exhausted(&RateWindow::default(), 0),
            "an unknown window is not exhausted"
        );

        let spent_but_reset = RateWindow {
            resets_at: Some(resetting(5_000)),
            ..counted_window(100, 100)
        };
        assert!(window_exhausted(&spent_but_reset, 4_999));
        assert!(
            !window_exhausted(&spent_but_reset, 5_000),
            "the provider's own clock says the budget came back"
        );

        let refusing_but_reset = RateWindow {
            resets_at: Some(resetting(5_000)),
            allowed: Some(false),
            ..RateWindow::default()
        };
        assert!(!window_exhausted(&refusing_but_reset, 6_000));
    }

    #[test]
    fn the_basis_names_the_source_and_the_shape() {
        assert_eq!(basis_of(None), "no rate window journaled");
        assert_eq!(
            basis_of(Some(&counted_window(1, 2))),
            "derived from provider-reported counts"
        );
        let metered = metered_window(
            &DeclaredQuota {
                window_ms: 1,
                budget_tokens: 1,
            },
            0,
        );
        assert_eq!(
            basis_of(Some(&metered)),
            "derived from horde-metered counts"
        );

        let fraction = RateWindow {
            source: HeadroomSource::ProviderReported as i32,
            used_fraction: Some(0.5),
            ..RateWindow::default()
        };
        assert_eq!(
            basis_of(Some(&fraction)),
            "derived from provider-reported fraction"
        );

        let serving = RateWindow {
            source: HeadroomSource::ProviderReported as i32,
            allowed: Some(true),
            ..RateWindow::default()
        };
        assert_eq!(
            basis_of(Some(&serving)),
            "provider-reported serving state only"
        );

        let empty = RateWindow::default();
        assert_eq!(
            basis_of(Some(&empty)),
            "unattributed window carries no quantity"
        );

        let counts_only_one_side = RateWindow {
            source: HeadroomSource::ProviderReported as i32,
            used: Some(5),
            used_fraction: Some(0.5),
            ..RateWindow::default()
        };
        assert_eq!(
            basis_of(Some(&counts_only_one_side)),
            "derived from provider-reported fraction",
            "used without a limit is not counts; the fraction is what derives"
        );
    }

    #[test]
    fn timestamps_convert_to_milliseconds_and_saturate() {
        assert_eq!(
            timestamp_unix_ms(&Timestamp {
                seconds: 5,
                nanos: 250_000_000,
            }),
            5_250
        );
        assert_eq!(
            timestamp_unix_ms(&Timestamp {
                seconds: i64::MAX,
                nanos: 999_999_999,
            }),
            i64::MAX,
            "saturates rather than wrapping"
        );
    }
}
