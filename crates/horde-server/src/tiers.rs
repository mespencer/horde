//! Model tiers and their selection strategies (DESIGN §5).
//!
//! Agents request a **tier**, not a brand. A tier is a user-defined config key
//! mapping to candidate `(provider, model)` pairs; multiplying the candidates
//! by the Provider Accounts a worker advertises yields the concrete options a
//! spawn can actually bind. Within a tier, a per-tier strategy picks one
//! option: `headroom` (default — most remaining), `ordered` (first listed
//! candidate at or above a floor), or `weighted` (preference weights combined
//! with headroom).
//!
//! Everything here is pure: options in, choice out, no clock and no log. The
//! precedence chain that decides *which* tier applies to a spawn (spawn args >
//! agent type > session defaults > user-global) belongs to the config layer,
//! which has not landed; this module is the seam it will resolve through, and
//! [`TierCatalog::shipped_defaults`] is the catalog it starts from.
//!
//! An exhausted option is never selected, under any strategy: a binding is
//! held for the agent's life (DESIGN §5), and binding an account with nothing
//! left would pin the agent to a wall. When every option is exhausted the
//! answer is honestly `None` — the caller queues or relaxes, it does not get a
//! doomed binding.

use std::cmp::Ordering;

use horde_proto::v1::{ModelBinding, ProviderAccountCapability};

use crate::usage::{remaining_fraction, window_exhausted};

/// One `(provider, model)` a tier may bind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TierCandidate {
    /// Adapter key, e.g. "claude", "codex".
    pub provider: String,
    /// Harness-side model name. Empty means the harness's own default model —
    /// a real state, not a wildcard convenience: some harnesses (codex) are
    /// driven by their configured default rather than a per-spawn name.
    pub model: String,
    /// Preference weight for the `weighted` strategy; ignored by the others.
    pub weight: u32,
}

impl TierCandidate {
    #[must_use]
    pub fn new(provider: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            weight: 1,
        }
    }

    #[must_use]
    pub fn weighted(mut self, weight: u32) -> Self {
        self.weight = weight;
        self
    }
}

/// How a tier picks among its concrete options.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SelectionStrategy {
    /// The option with the most remaining headroom. The default.
    Headroom,
    /// The first *listed* candidate whose option has headroom at or above
    /// `floor` (a fraction, 0.0–1.0).
    Ordered { floor: f64 },
    /// Highest `weight × remaining headroom`.
    Weighted,
}

/// One named tier: candidates plus the strategy that picks among them.
#[derive(Debug, Clone, PartialEq)]
pub struct Tier {
    pub name: String,
    pub candidates: Vec<TierCandidate>,
    pub strategy: SelectionStrategy,
}

impl Tier {
    #[must_use]
    pub fn new(name: impl Into<String>, candidates: Vec<TierCandidate>) -> Self {
        Self {
            name: name.into(),
            candidates,
            strategy: SelectionStrategy::Headroom,
        }
    }

    #[must_use]
    pub fn with_strategy(mut self, strategy: SelectionStrategy) -> Self {
        self.strategy = strategy;
        self
    }
}

/// The named tiers in force, keyed by tier name.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TierCatalog {
    tiers: std::collections::BTreeMap<String, Tier>,
}

impl TierCatalog {
    /// The tiers Horde ships: `fast` / `standard` / `deep` (DESIGN §5).
    ///
    /// Model names are harness-side names — claude's model aliases, and the
    /// empty string for codex meaning its configured default. User-authored
    /// config replaces these wholesale; they exist so a fresh server can
    /// resolve a tier before anyone has authored anything.
    #[must_use]
    pub fn shipped_defaults() -> Self {
        Self::default()
            .with_tier(Tier::new(
                "fast",
                vec![TierCandidate::new("claude", "haiku")],
            ))
            .with_tier(Tier::new(
                "standard",
                vec![
                    TierCandidate::new("claude", "sonnet"),
                    TierCandidate::new("codex", ""),
                ],
            ))
            .with_tier(Tier::new(
                "deep",
                vec![
                    TierCandidate::new("claude", "opus"),
                    TierCandidate::new("codex", ""),
                ],
            ))
    }

    /// Adds or replaces a tier by name — user config overrides shipped
    /// defaults wholesale rather than merging into them.
    #[must_use]
    pub fn with_tier(mut self, tier: Tier) -> Self {
        self.tiers.insert(tier.name.clone(), tier);
        self
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Tier> {
        self.tiers.get(name)
    }

    /// Every tier name, sorted — what an error naming valid tiers lists.
    #[must_use]
    pub fn names(&self) -> Vec<&str> {
        self.tiers.keys().map(String::as_str).collect()
    }
}

/// One concrete `(provider, model, account)` a tier could bind, with the
/// headroom facts selection orders on.
#[derive(Debug, Clone, PartialEq)]
pub struct ConcreteOption {
    pub provider: String,
    pub model: String,
    pub account: String,
    /// Derived remaining fraction from the account's advertised window;
    /// `None` is unknown, which ranks as full headroom — see
    /// [`effective_remaining`].
    pub remaining: Option<f64>,
    /// The account's window says nothing is left and has not reset.
    pub exhausted: bool,
    /// The candidate's preference weight, for `weighted`.
    pub weight: u32,
    /// The candidate's position in the tier's list, for `ordered` and for
    /// deterministic tie-breaks everywhere.
    pub candidate_index: usize,
}

/// Multiplies a tier's candidates by the accounts on offer, at `now`.
///
/// `accounts` is whatever the caller scopes to — one worker's advertisement
/// at placement time, or a whole pool's. Headroom facts are derived here,
/// once, so every strategy orders on the same arithmetic.
#[must_use]
pub fn options_for(
    tier: &Tier,
    accounts: &[ProviderAccountCapability],
    now_unix_ms: i64,
) -> Vec<ConcreteOption> {
    let mut options = Vec::new();
    for (candidate_index, candidate) in tier.candidates.iter().enumerate() {
        for account in accounts {
            if !account_serves(candidate, account) {
                continue;
            }
            let window = account.headroom.as_ref();
            options.push(ConcreteOption {
                provider: candidate.provider.clone(),
                model: candidate.model.clone(),
                account: account.account.clone(),
                remaining: window.and_then(remaining_fraction),
                exhausted: window.is_some_and(|window| window_exhausted(window, now_unix_ms)),
                weight: candidate.weight,
                candidate_index,
            });
        }
    }
    options
}

/// Whether one account can serve one candidate.
///
/// Three facts, named because the compound is not separately reachable at any
/// call site: the providers must match, and the account's model list must
/// cover the candidate's model — where an empty list advertises every model
/// the provider has, and an empty candidate model (the harness default) is
/// served by any account of that provider.
#[must_use]
pub fn account_serves(candidate: &TierCandidate, account: &ProviderAccountCapability) -> bool {
    let provider_matches = candidate.provider == account.provider;
    let model_served = account.models.is_empty()
        || candidate.model.is_empty()
        || account.models.contains(&candidate.model);
    provider_matches && model_served
}

/// The headroom an option is ordered by: an account with no derivable window
/// ranks as full headroom. Every token flows through Horde, so an account
/// nobody has metered and no provider has bounded is, as far as anyone can
/// say, unspent — ranking it *below* known headroom would starve fresh
/// accounts in favour of half-empty ones.
#[must_use]
pub fn effective_remaining(option: &ConcreteOption) -> f64 {
    option.remaining.unwrap_or(1.0)
}

/// Picks one option per the strategy, or `None` when nothing selectable is
/// left. Deterministic: ties fall to the earlier-listed candidate, then the
/// lexicographically first account — a selector that flips coins cannot be
/// replayed or tested.
#[must_use]
pub fn select(strategy: SelectionStrategy, options: &[ConcreteOption]) -> Option<&ConcreteOption> {
    let selectable: Vec<&ConcreteOption> =
        options.iter().filter(|option| !option.exhausted).collect();
    match strategy {
        SelectionStrategy::Headroom => selectable
            .into_iter()
            .min_by(|a, b| by_score(score_headroom(a), a, score_headroom(b), b)),
        SelectionStrategy::Ordered { floor } => selectable
            .into_iter()
            .filter(|option| at_or_above(effective_remaining(option), floor))
            .min_by(|a, b| listing_order(a, b)),
        SelectionStrategy::Weighted => selectable
            .into_iter()
            .min_by(|a, b| by_score(score_weighted(a), a, score_weighted(b), b)),
    }
}

/// Whether an option's headroom clears the `ordered` strategy's floor.
///
/// "At or above": a floor of 0.25 accepts exactly 0.25. Named because the
/// boundary is the whole point of a floor, and `>` versus `>=` inline would
/// survive every test whose fixtures avoid the boundary.
#[must_use]
pub fn at_or_above(remaining: f64, floor: f64) -> bool {
    remaining >= floor
}

fn score_headroom(option: &ConcreteOption) -> f64 {
    effective_remaining(option)
}

fn score_weighted(option: &ConcreteOption) -> f64 {
    f64::from(option.weight) * effective_remaining(option)
}

/// Higher score first, then listing order — the shared shape of both scored
/// strategies, written once so their tie-breaks cannot drift apart.
fn by_score(score_a: f64, a: &ConcreteOption, score_b: f64, b: &ConcreteOption) -> Ordering {
    score_b
        .total_cmp(&score_a)
        .then_with(|| listing_order(a, b))
}

/// Earlier-listed candidate first, then account name.
fn listing_order(a: &ConcreteOption, b: &ConcreteOption) -> Ordering {
    a.candidate_index
        .cmp(&b.candidate_index)
        .then_with(|| a.account.cmp(&b.account))
}

/// The binding a selection becomes: made once, at spawn, held for the agent's
/// life (DESIGN §5).
#[must_use]
pub fn bind(tier_name: &str, option: &ConcreteOption) -> ModelBinding {
    ModelBinding {
        tier: tier_name.to_owned(),
        provider: option.provider.clone(),
        model: option.model.clone(),
        provider_account: option.account.clone(),
    }
}

#[cfg(test)]
mod tests {
    use horde_proto::v1::{ProviderAccountCapability, RateWindow};

    use super::{
        ConcreteOption, SelectionStrategy, Tier, TierCandidate, TierCatalog, account_serves,
        at_or_above, bind, effective_remaining, options_for, select,
    };

    fn account(provider: &str, name: &str, models: &[&str]) -> ProviderAccountCapability {
        ProviderAccountCapability {
            provider: provider.to_owned(),
            account: name.to_owned(),
            models: models.iter().map(ToString::to_string).collect(),
            headroom: None,
        }
    }

    fn with_fraction_used(
        mut capability: ProviderAccountCapability,
        used_fraction: f64,
    ) -> ProviderAccountCapability {
        capability.headroom = Some(RateWindow {
            used_fraction: Some(used_fraction),
            ..RateWindow::default()
        });
        capability
    }

    fn option(candidate_index: usize, account: &str, remaining: Option<f64>) -> ConcreteOption {
        ConcreteOption {
            provider: "claude".to_owned(),
            model: "sonnet".to_owned(),
            account: account.to_owned(),
            remaining,
            exhausted: false,
            weight: 1,
            candidate_index,
        }
    }

    #[test]
    fn the_shipped_defaults_name_the_three_design_tiers() {
        let catalog = TierCatalog::shipped_defaults();
        assert_eq!(catalog.names(), vec!["deep", "fast", "standard"]);

        let fast = catalog.get("fast").expect("fast ships");
        assert_eq!(fast.strategy, SelectionStrategy::Headroom, "the default");
        assert_eq!(fast.candidates, vec![TierCandidate::new("claude", "haiku")]);

        let standard = catalog.get("standard").expect("standard ships");
        assert_eq!(
            standard.candidates,
            vec![
                TierCandidate::new("claude", "sonnet"),
                TierCandidate::new("codex", ""),
            ]
        );
        assert!(catalog.get("deep").is_some());
        assert!(catalog.get("bespoke").is_none());
    }

    #[test]
    fn a_user_tier_replaces_a_shipped_one_wholesale() {
        let catalog = TierCatalog::shipped_defaults().with_tier(
            Tier::new("fast", vec![TierCandidate::new("codex", "")])
                .with_strategy(SelectionStrategy::Ordered { floor: 0.5 }),
        );
        let fast = catalog.get("fast").expect("still present");
        assert_eq!(fast.candidates, vec![TierCandidate::new("codex", "")]);
        assert_eq!(fast.strategy, SelectionStrategy::Ordered { floor: 0.5 });
        assert_eq!(
            catalog.names().len(),
            3,
            "an override replaces, it does not add"
        );
    }

    /// All the corners of "can this account serve this candidate": provider
    /// mismatch refuses regardless of models, an empty model list serves
    /// everything, a listed model must be present, and an empty candidate
    /// model (harness default) is served by any account of the provider.
    #[test]
    fn an_account_serves_a_candidate_by_provider_and_model() {
        let sonnet = TierCandidate::new("claude", "sonnet");
        let harness_default = TierCandidate::new("codex", "");

        assert!(account_serves(&sonnet, &account("claude", "a", &[])));
        assert!(account_serves(
            &sonnet,
            &account("claude", "a", &["sonnet", "haiku"])
        ));
        assert!(!account_serves(
            &sonnet,
            &account("claude", "a", &["haiku"])
        ));
        assert!(
            !account_serves(&sonnet, &account("codex", "a", &[])),
            "an empty model list on the wrong provider is still the wrong provider"
        );
        assert!(account_serves(
            &harness_default,
            &account("codex", "a", &[])
        ));
        assert!(
            account_serves(&harness_default, &account("codex", "a", &["gpt-x"])),
            "the harness default is served by any account of the provider"
        );
    }

    #[test]
    fn options_multiply_candidates_by_the_accounts_that_serve_them() {
        let tier = Tier::new(
            "standard",
            vec![
                TierCandidate::new("claude", "sonnet"),
                TierCandidate::new("codex", ""),
            ],
        );
        let accounts = vec![
            with_fraction_used(account("claude", "personal", &[]), 0.25),
            account("claude", "work", &["haiku"]),
            account("codex", "team", &[]),
        ];

        let options = options_for(&tier, &accounts, 0);
        let described: Vec<(usize, &str, &str)> = options
            .iter()
            .map(|option| {
                (
                    option.candidate_index,
                    option.provider.as_str(),
                    option.account.as_str(),
                )
            })
            .collect();
        assert_eq!(
            described,
            vec![(0, "claude", "personal"), (1, "codex", "team")],
            "work only serves haiku, so the sonnet candidate skips it"
        );
        assert_eq!(
            options[0].remaining,
            Some(0.75),
            "the derived headroom rides the option"
        );
        assert_eq!(
            options[1].remaining, None,
            "no advertised window is unknown, not zero"
        );
    }

    #[test]
    fn an_exhausted_account_yields_an_option_marked_exhausted() {
        let tier = Tier::new("fast", vec![TierCandidate::new("claude", "haiku")]);
        let spent = with_fraction_used(account("claude", "personal", &[]), 1.0);
        let options = options_for(&tier, &[spent], 0);
        assert_eq!(options.len(), 1);
        assert!(options[0].exhausted);
        assert_eq!(options[0].remaining, Some(0.0));
    }

    #[test]
    fn unknown_headroom_ranks_as_full() {
        assert_eq!(effective_remaining(&option(0, "a", None)), 1.0);
        assert_eq!(effective_remaining(&option(0, "a", Some(0.3))), 0.3);
    }

    #[test]
    fn the_floor_is_inclusive() {
        assert!(at_or_above(0.25, 0.25), "at the floor clears it");
        assert!(at_or_above(0.26, 0.25));
        assert!(!at_or_above(0.24, 0.25));
    }

    #[test]
    fn headroom_takes_the_most_remaining_and_ties_fall_to_listing_order() {
        let options = vec![
            option(0, "half", Some(0.5)),
            option(1, "fresh", Some(0.9)),
            option(2, "low", Some(0.1)),
        ];
        let picked = select(SelectionStrategy::Headroom, &options).expect("something remains");
        assert_eq!(picked.account, "fresh");

        // A tie on remaining falls to the earlier candidate, then the account
        // name — never to iteration luck.
        let tied = vec![
            option(1, "beta", Some(0.5)),
            option(0, "delta", Some(0.5)),
            option(0, "alpha", Some(0.5)),
        ];
        let picked = select(SelectionStrategy::Headroom, &tied).expect("tied is not empty");
        assert_eq!(
            (picked.candidate_index, picked.account.as_str()),
            (0, "alpha")
        );
    }

    #[test]
    fn unknown_headroom_beats_a_half_spent_account() {
        let options = vec![option(0, "half", Some(0.5)), option(1, "unmetered", None)];
        let picked = select(SelectionStrategy::Headroom, &options).expect("selectable");
        assert_eq!(
            picked.account, "unmetered",
            "an account nobody has bounded ranks as full headroom"
        );
    }

    #[test]
    fn ordered_takes_the_first_listed_candidate_above_the_floor() {
        let options = vec![
            option(0, "preferred-but-low", Some(0.1)),
            option(1, "second", Some(0.6)),
            option(2, "third", Some(0.9)),
        ];
        let picked = select(SelectionStrategy::Ordered { floor: 0.25 }, &options)
            .expect("two clear the floor");
        assert_eq!(
            picked.account, "second",
            "ordered is listing order, not most headroom — third has more and must not win"
        );

        assert_eq!(
            select(SelectionStrategy::Ordered { floor: 0.95 }, &options),
            None,
            "nothing above the floor is honestly nothing"
        );
    }

    #[test]
    fn weighted_combines_weight_with_remaining() {
        let mut heavy = option(0, "heavy", Some(0.4));
        heavy.weight = 3; // score 1.2
        let light = option(1, "light", Some(0.9)); // score 0.9
        let options = [heavy, light];
        let picked = select(SelectionStrategy::Weighted, &options).expect("selectable options");
        assert_eq!(
            picked.account, "heavy",
            "3 × 0.4 outweighs 1 × 0.9; weight multiplies rather than decorates"
        );
    }

    #[test]
    fn no_strategy_ever_selects_an_exhausted_option() {
        let mut spent = option(0, "spent", Some(0.0));
        spent.exhausted = true;
        let alive = option(1, "alive", Some(0.2));

        let both = [spent.clone(), alive.clone()];
        let only_spent = [spent];
        for strategy in [
            SelectionStrategy::Headroom,
            SelectionStrategy::Ordered { floor: 0.0 },
            SelectionStrategy::Weighted,
        ] {
            let picked = select(strategy, &both).expect("a live option remains");
            assert_eq!(picked.account, "alive", "under {strategy:?}");
            assert_eq!(
                select(strategy, &only_spent),
                None,
                "all exhausted is honestly none, not a doomed binding"
            );
        }
    }

    #[test]
    fn selecting_from_nothing_is_none() {
        assert_eq!(select(SelectionStrategy::Headroom, &[]), None);
    }

    #[test]
    fn a_binding_carries_the_tier_and_the_whole_choice() {
        let picked = option(0, "personal", Some(0.5));
        let binding = bind("standard", &picked);
        assert_eq!(binding.tier, "standard");
        assert_eq!(binding.provider, "claude");
        assert_eq!(binding.model, "sonnet");
        assert_eq!(binding.provider_account, "personal");
    }
}

#[cfg(test)]
mod properties {
    use proptest::prelude::*;

    use super::{ConcreteOption, SelectionStrategy, at_or_above, effective_remaining, select};

    /// Distinct `(candidate_index, account)` identities, so no two generated
    /// options are tie-break twins — the tie-break is what makes selection
    /// deterministic, and it can only distinguish options with distinct
    /// identities.
    fn some_options() -> impl Strategy<Value = Vec<ConcreteOption>> {
        proptest::collection::btree_map(
            (0_usize..4, "[a-d]{1,3}"),
            (
                proptest::option::of(0.0_f64..=1.0),
                proptest::bool::ANY,
                0_u32..4,
            ),
            0..8,
        )
        .prop_map(|generated| {
            generated
                .into_iter()
                .map(
                    |((candidate_index, account), (remaining, exhausted, weight))| ConcreteOption {
                        provider: "claude".to_owned(),
                        model: "sonnet".to_owned(),
                        account,
                        remaining,
                        exhausted,
                        weight,
                        candidate_index,
                    },
                )
                .collect()
        })
    }

    fn any_strategy() -> impl Strategy<Value = SelectionStrategy> {
        prop_oneof![
            Just(SelectionStrategy::Headroom),
            (0.0_f64..=1.0).prop_map(|floor| SelectionStrategy::Ordered { floor }),
            Just(SelectionStrategy::Weighted),
        ]
    }

    proptest! {
        /// Whatever the strategy and whatever the options, a selection is one
        /// of the options handed in and is never exhausted.
        #[test]
        fn a_selection_is_a_non_exhausted_member_of_the_options(
            strategy in any_strategy(),
            options in some_options(),
        ) {
            if let Some(picked) = select(strategy, &options) {
                prop_assert!(options.contains(picked));
                prop_assert!(!picked.exhausted);
            }
        }

        /// Headroom's contract: nothing selectable had strictly more.
        #[test]
        fn headroom_never_leaves_more_remaining_on_the_table(
            options in some_options(),
        ) {
            if let Some(picked) = select(SelectionStrategy::Headroom, &options) {
                for other in options.iter().filter(|option| !option.exhausted) {
                    prop_assert!(
                        effective_remaining(other) <= effective_remaining(picked),
                        "{other:?} out-headrooms the pick {picked:?}"
                    );
                }
            }
        }

        /// Ordered's contract: the pick clears the floor, and no
        /// floor-clearing option was listed earlier.
        #[test]
        fn ordered_takes_the_earliest_listed_above_the_floor(
            floor in 0.0_f64..=1.0,
            options in some_options(),
        ) {
            let strategy = SelectionStrategy::Ordered { floor };
            match select(strategy, &options) {
                Some(picked) => {
                    prop_assert!(at_or_above(effective_remaining(picked), floor));
                    for other in options.iter().filter(|option| {
                        !option.exhausted && at_or_above(effective_remaining(option), floor)
                    }) {
                        prop_assert!(other.candidate_index >= picked.candidate_index);
                    }
                }
                None => {
                    for other in &options {
                        prop_assert!(
                            other.exhausted || !at_or_above(effective_remaining(other), floor),
                            "{other:?} was selectable yet nothing was selected"
                        );
                    }
                }
            }
        }

        /// Determinism: the same options in a different order select the same
        /// thing — a selector that depends on iteration order cannot be
        /// replayed.
        #[test]
        fn selection_ignores_the_order_options_arrive_in(
            strategy in any_strategy(),
            options in some_options(),
        ) {
            let mut reversed = options.clone();
            reversed.reverse();
            prop_assert_eq!(select(strategy, &options), select(strategy, &reversed));
        }
    }
}
