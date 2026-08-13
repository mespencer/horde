//! Deterministic rules: the two fast layers of DESIGN §10's chain.
//!
//! A rule is deliberately literal. It matches a tool name exactly, requires
//! string-prefix facts about named arguments, and can be scoped to a subtree
//! of the agent tree by ancestry prefix. Anything a rule cannot express is not
//! the deterministic layers' business — that is what the Policy Judge and the
//! human are for. A clever rule language would move judgment calls into the
//! layer whose whole value is that it never makes one.
//!
//! Every compound condition here is a named predicate with its truth table in
//! the tests (QUALITY_GATES.md): the corners that separate `&&` from `||` are
//! exactly the ones a live run never stages, because harnesses send
//! well-formed calls and rules are authored to match them.

use horde_proto::prost_types::{Struct, value::Kind};
use horde_proto::v1::ProposedToolCall;

/// A string-prefix requirement on one named argument.
///
/// `field: "command", prefix: "git "` matches `{"command": "git status"}` and
/// nothing else — not a missing field, not a non-string field. A constraint
/// that treated "argument absent" as "argument acceptable" would let an
/// allow rule pass a call it never looked at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldPrefix {
    pub field: String,
    pub prefix: String,
}

/// One deterministic rule: which calls it speaks for.
///
/// The same shape serves both layers; a deny rule wraps it with a reason
/// ([`DenyRule`]) because a denial without one teaches the agent nothing and
/// it retries the same call forever.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rule {
    /// Tool this rule speaks to. `None` means every tool.
    pub tool_name: Option<String>,
    /// Prefix constraints that must all hold on the call's arguments.
    pub where_input: Vec<FieldPrefix>,
    /// Ancestry prefix (root first) this rule is scoped under. Empty means the
    /// whole session — this is the tree-path routing from DESIGN §10.
    pub under: Vec<String>,
}

impl Rule {
    /// A rule matching every call. The building block of a deny-all or
    /// allow-all mode; useful mostly with [`Rule::under`] scoping.
    #[must_use]
    pub fn any_tool() -> Self {
        Self::default()
    }

    /// A rule matching one tool by exact name.
    #[must_use]
    pub fn tool(name: impl Into<String>) -> Self {
        Self {
            tool_name: Some(name.into()),
            ..Self::default()
        }
    }

    /// Adds a string-prefix requirement on one argument.
    #[must_use]
    pub fn where_input_starts_with(
        mut self,
        field: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        self.where_input.push(FieldPrefix {
            field: field.into(),
            prefix: prefix.into(),
        });
        self
    }

    /// Scopes the rule to agents whose ancestry starts with `path`.
    #[must_use]
    pub fn under(mut self, path: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.under = path.into_iter().map(Into::into).collect();
        self
    }

    /// Whether this rule speaks for `call` made by an agent at `ancestry`.
    ///
    /// Three independent predicates, each with its own truth table below.
    /// Inline, their corners are unreachable in a live run — a rule authored
    /// for `Bash` is only ever evaluated against well-formed `Bash` calls — so
    /// the wrong operator would behave correctly in every staged run.
    #[must_use]
    pub fn matches(&self, call: &ProposedToolCall, ancestry: &[String]) -> bool {
        tool_matches(self.tool_name.as_deref(), &call.tool_name)
            && input_satisfies(&self.where_input, call.input.as_ref())
            && scoped_under(&self.under, ancestry)
    }
}

/// A deny rule: what it refuses, and what the agent is told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenyRule {
    pub rule: Rule,
    /// Rides the decision as its reason. An agent denied without one retries
    /// the same call forever.
    pub reason: String,
}

impl DenyRule {
    #[must_use]
    pub fn new(rule: Rule, reason: impl Into<String>) -> Self {
        Self {
            rule,
            reason: reason.into(),
        }
    }
}

/// Whether a rule's tool selector accepts this tool name.
fn tool_matches(selector: Option<&str>, tool_name: &str) -> bool {
    selector.is_none_or(|wanted| wanted == tool_name)
}

/// Whether every argument constraint holds against the call's input.
///
/// A call with no input satisfies only a rule with no constraints: "the
/// argument I constrain is absent" is a call the rule never examined, and
/// passing it would be an allow nobody authored.
fn input_satisfies(constraints: &[FieldPrefix], input: Option<&Struct>) -> bool {
    constraints
        .iter()
        .all(|constraint| field_has_prefix(input, constraint))
}

/// Whether one named argument exists, is a string, and starts with the prefix.
fn field_has_prefix(input: Option<&Struct>, constraint: &FieldPrefix) -> bool {
    let value = input
        .and_then(|fields| fields.fields.get(&constraint.field))
        .and_then(|value| value.kind.as_ref());
    match value {
        Some(Kind::StringValue(text)) => text.starts_with(&constraint.prefix),
        _ => false,
    }
}

/// Whether an agent's ancestry sits under a rule's scope.
///
/// Prefix, root first: a rule under `["ci-fix", "reviewer"]` covers the
/// reviewer and everything it spawned, and an empty scope covers everyone.
fn scoped_under(under: &[String], ancestry: &[String]) -> bool {
    ancestry.starts_with(under)
}

#[cfg(test)]
mod tests {
    use super::{DenyRule, FieldPrefix, Rule, field_has_prefix, scoped_under, tool_matches};
    use horde_proto::prost_types::{Struct, Value, value::Kind};
    use horde_proto::v1::ProposedToolCall;

    fn call(tool: &str, fields: &[(&str, Kind)]) -> ProposedToolCall {
        let mut input = Struct::default();
        for (name, kind) in fields {
            input.fields.insert(
                (*name).to_owned(),
                Value {
                    kind: Some(kind.clone()),
                },
            );
        }
        ProposedToolCall {
            tool_name: tool.to_owned(),
            input: Some(input),
            rendered: String::new(),
        }
    }

    fn text(value: &str) -> Kind {
        Kind::StringValue(value.to_owned())
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn a_tool_selector_accepts_its_own_name_and_no_other() {
        assert!(tool_matches(Some("Bash"), "Bash"));
        assert!(!tool_matches(Some("Bash"), "Write"));
        assert!(
            tool_matches(None, "anything"),
            "no selector means every tool"
        );
    }

    /// The full table for one constraint: present-and-prefixed is the only
    /// true row. Absent, wrong-typed, and wrong-prefixed must all be false —
    /// each is a call the rule did not actually examine.
    #[test]
    fn a_field_prefix_holds_only_for_a_string_that_starts_with_it() {
        let constraint = FieldPrefix {
            field: "command".to_owned(),
            prefix: "git ".to_owned(),
        };
        let with = |kind: Kind| call("Bash", &[("command", kind)]);

        assert!(field_has_prefix(
            with(text("git status")).input.as_ref(),
            &constraint
        ));
        assert!(!field_has_prefix(
            with(text("rm -rf /")).input.as_ref(),
            &constraint
        ));
        assert!(
            !field_has_prefix(with(Kind::NumberValue(3.0)).input.as_ref(), &constraint),
            "a non-string argument is not examined text"
        );
        assert!(
            !field_has_prefix(call("Bash", &[]).input.as_ref(), &constraint),
            "an absent argument is a call the rule never looked at"
        );
        assert!(
            !field_has_prefix(None, &constraint),
            "no input at all cannot satisfy a constraint"
        );
    }

    #[test]
    fn every_constraint_must_hold_not_just_one() {
        let rule = Rule::tool("Bash")
            .where_input_starts_with("command", "git ")
            .where_input_starts_with("cwd", "/workspace");

        let both = call(
            "Bash",
            &[
                ("command", text("git status")),
                ("cwd", text("/workspace/a")),
            ],
        );
        let one = call("Bash", &[("command", text("git status"))]);

        assert!(rule.matches(&both, &[]));
        assert!(
            !rule.matches(&one, &[]),
            "with || a rule would pass calls half-examined"
        );
    }

    /// Scope is a root-first prefix. The corner that separates prefix from
    /// suffix or membership: the same names deeper in the tree must not match.
    #[test]
    fn scope_is_an_ancestry_prefix_root_first() {
        let under = owned(&["ci-fix", "reviewer"]);

        assert!(scoped_under(&under, &owned(&["ci-fix", "reviewer"])));
        assert!(
            scoped_under(&under, &owned(&["ci-fix", "reviewer", "test-runner"])),
            "a scope covers everything the scoped agent spawned"
        );
        assert!(!scoped_under(&under, &owned(&["ci-fix"])));
        assert!(
            !scoped_under(&under, &owned(&["other", "ci-fix", "reviewer"])),
            "the same names deeper in the tree are a different subtree"
        );
        assert!(
            scoped_under(&[], &owned(&["anyone"])),
            "an empty scope is the whole session"
        );
        assert!(
            scoped_under(&[], &[]),
            "and covers an agent with no known ancestry"
        );
        assert!(
            !scoped_under(&under, &[]),
            "a scoped rule must not match an agent whose ancestry is unknown"
        );
    }

    /// The conjunction's own corners: each predicate false alone must sink the
    /// match. This is the truth table the inline `&&` could never reach.
    #[test]
    fn a_rule_matches_only_when_tool_input_and_scope_all_agree() {
        let rule = Rule::tool("Bash")
            .where_input_starts_with("command", "git ")
            .under(["ci-fix"]);
        let good = call("Bash", &[("command", text("git status"))]);
        let ancestry = owned(&["ci-fix", "reviewer"]);

        assert!(rule.matches(&good, &ancestry));
        assert!(
            !rule.matches(&call("Write", &[("command", text("git status"))]), &ancestry),
            "wrong tool"
        );
        assert!(
            !rule.matches(&call("Bash", &[("command", text("rm -rf /"))]), &ancestry),
            "wrong argument"
        );
        assert!(
            !rule.matches(&good, &owned(&["other-session-root"])),
            "wrong subtree"
        );
    }

    #[test]
    fn any_tool_matches_everything_until_scoped() {
        let everything = Rule::any_tool();
        assert!(everything.matches(&call("Bash", &[]), &[]));

        let scoped = Rule::any_tool().under(["ci-fix"]);
        assert!(scoped.matches(&call("Bash", &[]), &owned(&["ci-fix"])));
        assert!(!scoped.matches(&call("Bash", &[]), &owned(&["elsewhere"])));
    }

    #[test]
    fn a_deny_rule_carries_the_reason_the_agent_will_read() {
        let deny = DenyRule::new(Rule::tool("Bash"), "no shell in this mode");
        assert_eq!(deny.reason, "no shell in this mode");
        assert_eq!(deny.rule, Rule::tool("Bash"));
    }
}
