//! Thematic session names, and the slugs every title resolves by.
//!
//! ADR-0008: an omitted title draws an `adjective-noun` name from a fixed
//! in-repo word list, a collision with a live session redraws rather than
//! suffixing, and a user-given title slugifies into the handle the resolver
//! matches. The word list is an asset, not configuration — determinism under
//! a seed is what makes the draw testable.

/// The adjective half of the word list. Fixed, lowercase, dash-free: an
/// entry with a dash would make a drawn name un-splittable back into its
/// halves, and casing would leak into handles that are defined lowercase.
pub const ADJECTIVES: [&str; 40] = [
    "amber", "bold", "bright", "brisk", "calm", "clever", "crimson", "daring", "dusky", "eager",
    "emerald", "fierce", "frosted", "gallant", "gilded", "hale", "hardy", "iron", "ivory",
    "jaunty", "keen", "kindled", "limber", "lively", "merry", "nimble", "oaken", "patient",
    "quick", "quiet", "rugged", "sable", "steady", "swift", "tireless", "umber", "valiant", "wary",
    "wintry", "zealous",
];

/// The noun half: horde-flavored words for a group on the move.
pub const NOUNS: [&str; 40] = [
    "banner",
    "battalion",
    "brigade",
    "bulwark",
    "campaign",
    "caravan",
    "charge",
    "cohort",
    "column",
    "company",
    "convoy",
    "detachment",
    "encampment",
    "escort",
    "foray",
    "formation",
    "garrison",
    "host",
    "legion",
    "levy",
    "march",
    "muster",
    "outpost",
    "patrol",
    "pennant",
    "phalanx",
    "rally",
    "rearguard",
    "regiment",
    "retinue",
    "sortie",
    "squadron",
    "standard",
    "swarm",
    "throng",
    "troop",
    "vanguard",
    "vigil",
    "warband",
    "warhorn",
];

/// A title's handle: lowercased, with every non-alphanumeric run collapsed
/// to a single dash and no dash at either end. `"Fix Auth Timeout"` becomes
/// `fix-auth-timeout` — what the resolver matches and the row shows.
#[must_use]
pub fn slugify(title: &str) -> String {
    let mut slug = String::with_capacity(title.len());
    for character in title.chars() {
        if character.is_alphanumeric() {
            slug.extend(character.to_lowercase());
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_end_matches('-').to_owned()
}

/// Draws a name no live session already answers to.
///
/// One seed, one order: the walk visits every `adjective-noun` combination
/// exactly once in a seed-shuffled sequence, so a collision redraws rather
/// than suffixing (ADR-0008) and only a fully taken word list yields `None`.
#[must_use]
pub fn generate(seed: u64, taken: &[String]) -> Option<String> {
    let mut order: Vec<usize> = (0..ADJECTIVES.len() * NOUNS.len()).collect();
    shuffle(&mut order, seed);
    order
        .into_iter()
        .map(combination)
        .find(|name| !taken.contains(name))
}

/// A seed for interactive draws. Entropy, not cryptography: two humans
/// creating sessions in the same instant still land on different names via
/// the collision redraw, so nanoseconds mixed with the process id is plenty.
#[must_use]
pub fn entropy_seed() -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    seed_from(nanos as u64, std::process::id())
}

/// The mixing rule, split from the clock so it is a fact a test can state:
/// the pid occupies the high half (nanosecond ticks touch the low bits), and
/// the combination is xor, so neither input can mask the other.
fn seed_from(nanos: u64, pid: u32) -> u64 {
    nanos ^ u64::from(pid).rotate_left(32)
}

/// The combination one index names: adjectives major, nouns minor.
fn combination(index: usize) -> String {
    format!(
        "{}-{}",
        ADJECTIVES[index / NOUNS.len()],
        NOUNS[index % NOUNS.len()]
    )
}

/// Fisher–Yates under [`splitmix`] — deterministic for a seed, and no
/// dependency for what four lines of arithmetic cover.
fn shuffle(order: &mut [usize], seed: u64) {
    let mut state = seed;
    for index in (1..order.len()).rev() {
        let other = (splitmix(&mut state) % (index as u64 + 1)) as usize;
        order.swap(index, other);
    }
}

/// The splitmix64 step: the standard seed-expansion generator, chosen
/// because identical seeds must yield identical draws on every platform.
fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = *state;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

#[cfg(test)]
mod tests {
    use super::{ADJECTIVES, NOUNS, generate, slugify};

    /// The list is an asset with invariants the generator leans on: dashes
    /// would break the `adjective-noun` shape, uppercase would leak into
    /// lowercase handles, and a duplicate would silently shrink the space.
    #[test]
    fn the_word_list_is_lowercase_dash_free_and_duplicate_free() {
        for word in ADJECTIVES.iter().chain(NOUNS.iter()) {
            assert!(!word.is_empty(), "an empty word draws a malformed name");
            assert!(
                word.chars().all(|c| c.is_ascii_lowercase()),
                "{word:?} must be lowercase ascii letters only"
            );
        }
        let mut adjectives: Vec<_> = ADJECTIVES.to_vec();
        adjectives.sort_unstable();
        adjectives.dedup();
        assert_eq!(adjectives.len(), ADJECTIVES.len(), "duplicate adjective");
        let mut nouns: Vec<_> = NOUNS.to_vec();
        nouns.sort_unstable();
        nouns.dedup();
        assert_eq!(nouns.len(), NOUNS.len(), "duplicate noun");
    }

    #[test]
    fn titles_slugify_to_lowercase_dashed_handles() {
        assert_eq!(slugify("Fix Auth Timeout"), "fix-auth-timeout");
        assert_eq!(slugify("  spaced   out  "), "spaced-out");
        assert_eq!(slugify("already-a-slug"), "already-a-slug");
        assert_eq!(slugify("v2.0 (rollout)"), "v2-0-rollout");
        assert_eq!(slugify("Café Run"), "café-run");
        assert_eq!(slugify("***"), "", "no alphanumerics, no handle");
        assert_eq!(slugify(""), "");
    }

    /// Same seed, same draw — the determinism the ADR requires under test —
    /// and the draw is a real combination of the two lists.
    #[test]
    fn a_seeded_draw_is_deterministic_and_well_formed() {
        let first = generate(7, &[]).expect("an untaken list yields a name");
        assert_eq!(generate(7, &[]).as_deref(), Some(first.as_str()));

        let (adjective, noun) = first
            .split_once('-')
            .expect("a drawn name is adjective-noun");
        assert!(ADJECTIVES.contains(&adjective), "{first}");
        assert!(NOUNS.contains(&noun), "{first}");

        assert_ne!(
            generate(7, &[]),
            generate(8, &[]),
            "different seeds must be able to draw differently"
        );
    }

    /// The draws for fixed seeds, as golden values: a seedable generator's
    /// contract IS its exact output, so any change to the mixer, the
    /// shuffle, or the index-to-combination rule shows up here by name.
    #[test]
    fn fixed_seeds_draw_their_golden_names() {
        for (seed, expected) in [
            (0, "kindled-charge"),
            (1, "dusky-company"),
            (7, "kindled-patrol"),
            (42, "wary-foray"),
        ] {
            assert_eq!(
                generate(seed, &[]).as_deref(),
                Some(expected),
                "seed {seed}"
            );
        }
    }

    /// The entropy seed mixes the pid into the high half by xor: the clock
    /// cannot be masked by the pid, nor the pid by the clock — and two
    /// consecutive real draws differ, because the nanosecond clock moved.
    #[test]
    fn the_entropy_seed_mixes_pid_and_clock_without_masking() {
        assert_eq!(super::seed_from(0, 1), 1_u64.rotate_left(32));
        assert_eq!(
            super::seed_from(u64::MAX, 1),
            u64::MAX ^ 1_u64.rotate_left(32),
            "xor flips the pid bit; an or would saturate, an and would erase"
        );
        // The clock's tick may be coarser than one call, so the draw is
        // re-read until it moves — a frozen constant never would.
        let first = super::entropy_seed();
        assert!(
            std::iter::repeat_with(super::entropy_seed)
                .take(1_000_000)
                .any(|seed| seed != first),
            "the seed must follow the clock, not freeze on a constant"
        );
    }

    /// A collision redraws rather than suffixing: with the seed's first pick
    /// taken, the same seed yields a different full name from the list — no
    /// `-2`, no mutation of the taken one.
    #[test]
    fn a_collision_redraws_instead_of_suffixing() {
        let first = generate(7, &[]).expect("a first draw");
        let second = generate(7, std::slice::from_ref(&first)).expect("a redraw");
        assert_ne!(second, first);
        assert!(
            !second.starts_with(&format!("{first}-")),
            "{second:?} must be a fresh draw, not a suffixed {first:?}"
        );
    }

    /// Exhaustion is `None`, honestly, rather than a loop or a repeat — and
    /// with one name left, every seed finds it.
    #[test]
    fn a_fully_taken_list_yields_none_and_one_gap_is_always_found() {
        let all: Vec<String> = ADJECTIVES
            .iter()
            .flat_map(|adjective| NOUNS.iter().map(move |noun| format!("{adjective}-{noun}")))
            .collect();
        assert_eq!(generate(3, &all), None);

        let last = all.last().expect("a non-empty list").clone();
        let almost: Vec<String> = all.into_iter().filter(|name| *name != last).collect();
        for seed in [0, 1, 99] {
            assert_eq!(generate(seed, &almost).as_deref(), Some(last.as_str()));
        }
    }
}
