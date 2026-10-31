//! Named permission modes and the registry that resolves them.
//!
//! A mode is the unit a user authors (DESIGN §10): deterministic deny rules,
//! deterministic allow rules, and optionally the plain-text instructions a
//! Policy Judge applies to whatever the rules did not settle. A mode with none
//! of these escalates everything to a human — which is also what an *absent*
//! mode does, so the chain's default is exactly the behavior the system had
//! before the chain existed.
//!
//! Modes live in this registry rather than on the wire. DESIGN §13's config
//! layering (user-global, workspace, repo) is where they will be authored
//! from; until that lands, whoever builds the server builds the registry.

use std::collections::BTreeMap;

use super::rules::{DenyRule, Rule};

/// One named mode: what it refuses, what it waves through, and what it asks a
/// judge about.
///
/// Deliberately no `Default`: the empty mode has a name and a meaning —
/// [`PermissionMode::escalate_everything`] — and a derived default would also
/// hand cargo-mutants a provably equivalent rewrite of that constructor.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionMode {
    /// Evaluated first. A matching deny is final — nothing later in the chain,
    /// including a remembered allow, can override "never Y".
    pub deny: Vec<DenyRule>,
    /// Evaluated after deny. A matching allow decides without a human.
    pub allow: Vec<Rule>,
    /// Plain-text policy for the judge, e.g. "allow read-only git commands,
    /// never anything that talks to the network". `None` skips the judge
    /// stage entirely and unsettled calls go straight to a human.
    pub judge_instructions: Option<String>,
    /// Whether an agent's own orchestrator verbs — its spawns, its result,
    /// its messages and waits within its own subtree — are allowed without a
    /// human (ADR-0009). Evaluated after deny, before the allow rules.
    pub own_orchestrator_verbs: bool,
}

impl PermissionMode {
    /// The mode that decides nothing itself: every call goes to a human.
    #[must_use]
    pub fn escalate_everything() -> Self {
        Self {
            deny: Vec::new(),
            allow: Vec::new(),
            judge_instructions: None,
            own_orchestrator_verbs: false,
        }
    }

    /// Lets an agent drive its own subtree through the orchestrator verbs
    /// without a human in the loop.
    #[must_use]
    pub fn allowing_own_orchestrator_verbs(mut self) -> Self {
        self.own_orchestrator_verbs = true;
        self
    }

    /// Adds a deny rule with the reason the refused agent will read.
    #[must_use]
    pub fn deny(mut self, rule: Rule, reason: impl Into<String>) -> Self {
        self.deny.push(DenyRule::new(rule, reason));
        self
    }

    /// Adds an allow rule.
    #[must_use]
    pub fn allow(mut self, rule: Rule) -> Self {
        self.allow.push(rule);
        self
    }

    /// Sets the plain-text policy a judge should apply to unsettled calls.
    #[must_use]
    pub fn judged_by(mut self, instructions: impl Into<String>) -> Self {
        self.judge_instructions = Some(instructions.into());
        self
    }
}

/// Every mode this server knows, plus the one agents get when their spec
/// names none.
#[derive(Debug, Clone, PartialEq)]
pub struct PermissionModes {
    named: BTreeMap<String, PermissionMode>,
    /// What an empty mode name resolves to.
    unnamed: PermissionMode,
}

impl Default for PermissionModes {
    /// Escalate everything, always — the chain's do-no-harm configuration.
    fn default() -> Self {
        Self {
            named: BTreeMap::new(),
            unnamed: PermissionMode::escalate_everything(),
        }
    }
}

impl PermissionModes {
    /// What a production server starts with (ADR-0009): an agent's own
    /// orchestrator verbs are allowed, and everything else escalates to a
    /// human. [`Default`] stays escalate-everything — the do-no-harm
    /// configuration the chain's own tests start from.
    #[must_use]
    pub fn shipped() -> Self {
        Self::default()
            .with_default(PermissionMode::escalate_everything().allowing_own_orchestrator_verbs())
    }

    /// Registers a mode under its name.
    #[must_use]
    pub fn with_mode(mut self, name: impl Into<String>, mode: PermissionMode) -> Self {
        self.named.insert(name.into(), mode);
        self
    }

    /// Replaces what agents whose spec names no mode get.
    #[must_use]
    pub fn with_default(mut self, mode: PermissionMode) -> Self {
        self.unnamed = mode;
        self
    }

    /// The mode `name` resolves to.
    ///
    /// An empty name is the unnamed default. An unknown name is `None`, and
    /// the chain escalates rather than guessing: substituting the default for
    /// a name someone deliberately wrote would apply rules nobody chose for
    /// that agent.
    #[must_use]
    pub fn resolve(&self, name: &str) -> Option<&PermissionMode> {
        if name.is_empty() {
            Some(&self.unnamed)
        } else {
            self.named.get(name)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PermissionMode, PermissionModes};
    use crate::permission::rules::Rule;

    #[test]
    fn an_empty_name_resolves_to_the_default_mode() {
        let modes = PermissionModes::default();
        assert_eq!(
            modes.resolve(""),
            Some(&PermissionMode::escalate_everything())
        );
    }

    #[test]
    fn a_registered_mode_resolves_by_its_name() {
        let cautious = PermissionMode::escalate_everything().allow(Rule::tool("Read"));
        let modes = PermissionModes::default().with_mode("cautious", cautious.clone());

        assert_eq!(modes.resolve("cautious"), Some(&cautious));
    }

    /// An unknown name must resolve to nothing, not to the default: a name
    /// someone deliberately wrote, applied as rules nobody chose, is a silent
    /// misconfiguration.
    #[test]
    fn an_unknown_name_is_not_guessed_at() {
        let modes = PermissionModes::default();
        assert_eq!(modes.resolve("typo-mode"), None);
    }

    #[test]
    fn the_shipped_default_allows_own_orchestrator_verbs_and_nothing_else() {
        let shipped = PermissionModes::shipped();
        let unnamed = shipped.resolve("").expect("the unnamed mode resolves");
        assert!(unnamed.own_orchestrator_verbs);
        assert!(unnamed.allow.is_empty() && unnamed.deny.is_empty());
        assert_eq!(unnamed.judge_instructions, None);
        assert!(
            !PermissionModes::default()
                .resolve("")
                .expect("resolves")
                .own_orchestrator_verbs,
            "Default stays escalate-everything"
        );
    }

    #[test]
    fn the_default_mode_is_replaceable() {
        let allow_reads = PermissionMode::escalate_everything().allow(Rule::tool("Read"));
        let modes = PermissionModes::default().with_default(allow_reads.clone());

        assert_eq!(modes.resolve(""), Some(&allow_reads));
    }

    #[test]
    fn a_mode_accumulates_its_rules_in_authoring_order() {
        let mode = PermissionMode::escalate_everything()
            .deny(Rule::tool("Bash"), "first")
            .deny(Rule::any_tool(), "second")
            .allow(Rule::tool("Read"))
            .judged_by("allow read-only calls");

        assert_eq!(mode.deny.len(), 2);
        assert_eq!(mode.deny[0].reason, "first");
        assert_eq!(mode.deny[1].reason, "second");
        assert_eq!(mode.allow, vec![Rule::tool("Read")]);
        assert_eq!(
            mode.judge_instructions.as_deref(),
            Some("allow read-only calls")
        );
    }
}
