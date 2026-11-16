//! `[tiers.<name>]`: the model-tier catalog (DESIGN §5, ADR-0016).
//!
//! A configured `[tiers]` table replaces the shipped catalog wholesale:
//! which tiers exist is one fact, and a catalog that was partly the
//! operator's and partly the default would surprise someone.

use horde_config::{Origin, Provenance, Section};

use crate::tiers::{SelectionStrategy, Tier, TierCandidate, TierCatalog};

/// Reads `[tiers]` from the file's root: `None` when the file has none.
///
/// # Errors
///
/// A key of the wrong type, an unknown key or strategy, a tier with no
/// candidates or a candidate with no provider, or a `floor` outside 0–1 or
/// beside a strategy other than `ordered`.
pub fn read(root: &mut Section<'_>) -> Result<Option<TierCatalog>, String> {
    let Some(named) = root.named("tiers")? else {
        return Ok(None);
    };
    named
        .into_iter()
        .map(|(name, tier)| tier_from(name, tier))
        .collect::<Result<Vec<_>, String>>()
        .map(|tiers| {
            Some(
                tiers
                    .into_iter()
                    .fold(TierCatalog::default(), TierCatalog::with_tier),
            )
        })
}

/// One `[tiers.<name>]` table.
fn tier_from(name: String, mut section: Section<'_>) -> Result<Tier, String> {
    let strategy = section.string("strategy")?;
    let floor = section.number("floor")?;
    let strategy = match (strategy.as_deref(), floor) {
        (None | Some("headroom"), None) => SelectionStrategy::Headroom,
        (Some("weighted"), None) => SelectionStrategy::Weighted,
        (Some("ordered"), floor) => {
            let floor = floor.unwrap_or(0.0);
            if !(0.0..=1.0).contains(&floor) {
                return Err(section.refuse("floor", format!("{floor} is not between 0.0 and 1.0")));
            }
            SelectionStrategy::Ordered { floor }
        }
        (None | Some("headroom" | "weighted"), Some(_)) => {
            return Err(section.refuse("floor", "applies only with strategy = \"ordered\""));
        }
        (Some(other), _) => {
            return Err(section.refuse(
                "strategy",
                format!("{other:?} is not one of \"headroom\", \"ordered\", \"weighted\""),
            ));
        }
    };
    let candidates = section
        .sections("candidates")?
        .unwrap_or_default()
        .into_iter()
        .map(candidate_from)
        .collect::<Result<Vec<_>, String>>()?;
    if candidates.is_empty() {
        return Err(section.refuse(
            "candidates",
            "a tier needs at least one { provider, model } candidate",
        ));
    }
    section.finish()?;
    Ok(Tier::new(name, candidates).with_strategy(strategy))
}

/// One `{ provider, model, weight }` candidate.
fn candidate_from(mut section: Section<'_>) -> Result<TierCandidate, String> {
    let provider = section
        .string("provider")?
        .filter(|provider| !provider.is_empty())
        .ok_or_else(|| {
            section.refuse(
                "provider",
                "a candidate needs its provider, e.g. \"claude\"",
            )
        })?;
    let model = section.string("model")?.unwrap_or_default();
    let weight = section.count::<u32>("weight")?.unwrap_or(1);
    section.finish()?;
    Ok(TierCandidate::new(provider, model).weighted(weight))
}

/// Records one `tiers.<name>` entry per tier of `catalog`.
pub fn record(catalog: &TierCatalog, origin: &Origin, provenance: &mut Provenance) {
    for name in catalog.names() {
        if let Some(tier) = catalog.get(name) {
            provenance.record(format!("tiers.{name}"), summary(tier), origin.clone());
        }
    }
}

/// A tier in one line, as `config show` prints it.
#[must_use]
pub fn summary(tier: &Tier) -> String {
    let strategy = match tier.strategy {
        SelectionStrategy::Headroom => "\"headroom\"".to_owned(),
        SelectionStrategy::Weighted => "\"weighted\"".to_owned(),
        SelectionStrategy::Ordered { floor } => format!("\"ordered\", floor = {floor}"),
    };
    let candidates: Vec<String> = tier
        .candidates
        .iter()
        .map(|candidate| {
            let model = if candidate.model.is_empty() {
                String::new()
            } else {
                format!(":{}", candidate.model)
            };
            let weight = if candidate.weight == 1 {
                String::new()
            } else {
                format!("*{}", candidate.weight)
            };
            format!("\"{}{model}{weight}\"", candidate.provider)
        })
        .collect();
    format!(
        "{{ strategy = {strategy}, candidates = [{}] }}",
        candidates.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use horde_config::{Origin, Provenance, Section};

    use super::{read, record, summary};
    use crate::tiers::{SelectionStrategy, Tier, TierCandidate, TierCatalog};

    fn catalog_of(text: &str) -> Result<Option<TierCatalog>, String> {
        let table: toml::Table = text.parse().expect("test TOML parses");
        let mut root = Section::root(&table);
        read(&mut root)
    }

    #[test]
    fn a_configured_catalog_replaces_the_shipped_one_wholesale() {
        let catalog = catalog_of(
            r#"
            [tiers.cheap]
            candidates = [{ provider = "claude", model = "haiku" }]
            [tiers.big]
            strategy = "ordered"
            floor = 0.25
            candidates = [{ provider = "claude", model = "opus" }, { provider = "codex" }]
            [tiers.mix]
            strategy = "weighted"
            candidates = [{ provider = "claude", model = "sonnet", weight = 3 }]
            "#,
        )
        .expect("parses")
        .expect("present");
        assert_eq!(catalog.names(), ["big", "cheap", "mix"]);
        assert_eq!(
            catalog.get("big"),
            Some(
                &Tier::new(
                    "big",
                    vec![
                        TierCandidate::new("claude", "opus"),
                        TierCandidate::new("codex", "")
                    ]
                )
                .with_strategy(SelectionStrategy::Ordered { floor: 0.25 })
            )
        );
        assert_eq!(
            catalog
                .get("mix")
                .map(|tier| (tier.strategy, tier.candidates[0].weight)),
            Some((SelectionStrategy::Weighted, 3))
        );
        assert_eq!(
            catalog.get("cheap").map(|tier| tier.strategy),
            Some(SelectionStrategy::Headroom)
        );
        assert_eq!(catalog.get("fast"), None, "the shipped tiers are gone");
        assert_eq!(catalog_of(""), Ok(None));
        assert_eq!(
            catalog_of("[tiers.o]\nstrategy = \"ordered\"\ncandidates = [{ provider = \"c\" }]")
                .expect("parses")
                .and_then(|catalog| catalog.get("o").map(|tier| tier.strategy)),
            Some(SelectionStrategy::Ordered { floor: 0.0 })
        );
    }

    #[test]
    fn malformed_tiers_are_refused_by_path() {
        for (text, wanted) in [
            (
                "[tiers.t]\n",
                "tiers.t.candidates: a tier needs at least one",
            ),
            (
                "[tiers.t]\ncandidates = [{ model = \"opus\" }]",
                "tiers.t.candidates[0].provider: a candidate needs its provider",
            ),
            (
                "[tiers.t]\ncandidates = [{ provider = \"\" }]",
                "tiers.t.candidates[0].provider: a candidate needs its provider",
            ),
            (
                "[tiers.t]\nstrategy = \"fastest\"\ncandidates = [{ provider = \"c\" }]",
                "tiers.t.strategy: \"fastest\" is not one of",
            ),
            (
                "[tiers.t]\nfloor = 0.5\ncandidates = [{ provider = \"c\" }]",
                "tiers.t.floor: applies only with strategy = \"ordered\"",
            ),
            (
                "[tiers.t]\nstrategy = \"weighted\"\nfloor = 0.5\ncandidates = [{ provider = \"c\" }]",
                "tiers.t.floor: applies only with strategy = \"ordered\"",
            ),
            (
                "[tiers.t]\nstrategy = \"ordered\"\nfloor = 1.5\ncandidates = [{ provider = \"c\" }]",
                "tiers.t.floor: 1.5 is not between 0.0 and 1.0",
            ),
            (
                "[tiers.t]\nstrategy = \"ordered\"\nfloor = -0.1\ncandidates = [{ provider = \"c\" }]",
                "tiers.t.floor: -0.1 is not between 0.0 and 1.0",
            ),
            (
                "[tiers.t]\ncandidates = [{ provider = \"c\", modle = \"x\" }]",
                "unknown key tiers.t.candidates[0].modle (known here: model, provider, weight)",
            ),
            (
                "[tiers.t]\nmodels = []\ncandidates = [{ provider = \"c\" }]",
                "unknown key tiers.t.models",
            ),
            (
                "[tiers]\nt = 1",
                "tiers.t: expected a table, found integer 1",
            ),
        ] {
            let refused = catalog_of(text).expect_err(text);
            assert!(refused.starts_with(wanted), "{text}: {refused}");
        }
    }

    #[test]
    fn each_tier_is_recorded_in_one_line() {
        let mut provenance = Provenance::default();
        record(
            &TierCatalog::shipped_defaults(),
            &Origin::Default,
            &mut provenance,
        );
        assert_eq!(
            provenance.render(false),
            "tiers.deep = { strategy = \"headroom\", candidates = [\"claude:opus\", \"codex\"] }\n\
             tiers.fast = { strategy = \"headroom\", candidates = [\"claude:haiku\"] }\n\
             tiers.standard = { strategy = \"headroom\", candidates = [\"claude:sonnet\", \"codex\"] }\n"
        );
        let tier = Tier::new("o", vec![TierCandidate::new("claude", "opus").weighted(2)])
            .with_strategy(SelectionStrategy::Ordered { floor: 0.5 });
        assert_eq!(
            summary(&tier),
            "{ strategy = \"ordered\", floor = 0.5, candidates = [\"claude:opus*2\"] }"
        );
        let weighted = Tier::new("w", vec![TierCandidate::new("c", "")])
            .with_strategy(SelectionStrategy::Weighted);
        assert_eq!(
            summary(&weighted),
            "{ strategy = \"weighted\", candidates = [\"c\"] }"
        );
    }
}
