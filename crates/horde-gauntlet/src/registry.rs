//! The gate list.
//!
//! Name, enforcement level and dependencies are registry *data*, not methods on
//! each gate, so the whole policy is one reviewable literal in one file and
//! promoting a gate is a one-line diff. The self-tests below assert the exact
//! list and the exact enforcement of every entry: if a gate is added, removed or
//! demoted, a test fails and someone has to mean it.

use crate::gate::{Enforcement, GateSpec, RegisteredGate};
use crate::gates;

/// Every gate, in the order they run.
pub fn registry() -> Vec<RegisteredGate> {
    vec![
        RegisteredGate {
            spec: GateSpec {
                name: "fmt",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo fmt --check: every file is rustfmt-clean",
            },
            gate: Box::new(gates::fmt::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "clippy",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo clippy over all targets and features, warnings denied",
            },
            gate: Box::new(gates::clippy::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "test",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo nextest (or cargo test) plus doctests, all features",
            },
            gate: Box::new(gates::test::TestGate),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "coverage",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo llvm-cov: records line coverage and writes the LCOV crap reads",
            },
            gate: Box::new(gates::coverage::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "crap",
                enforcement: Enforcement::Blocking,
                depends_on: &["coverage"],
                summary: "cargo-crap: no function scores above CRAP 15",
            },
            gate: Box::new(gates::crap::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "duplication",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "jscpd: no copy-pasted blocks at the detector's default size",
            },
            gate: Box::new(gates::duplication::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "machete",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo-machete: no unused dependencies, dev-dependencies included",
            },
            gate: Box::new(gates::machete::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "module-structure",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo-modules: no orphaned files, module cycles ratcheted both ways",
            },
            gate: Box::new(gates::module_structure::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "proto-contract",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "buf lint + buf breaking: the schema is idiomatic and stays compatible",
            },
            gate: Box::new(gates::proto_contract::gate()),
        },
        RegisteredGate {
            spec: GateSpec {
                name: "mutants",
                enforcement: Enforcement::Blocking,
                depends_on: &[],
                summary: "cargo-mutants: no mutant survives, diff-scoped against main",
            },
            gate: Box::new(gates::mutants::gate()),
        },
    ]
}

/// Looks a gate up by name.
pub fn find<'a>(gates: &'a [RegisteredGate], name: &str) -> Option<&'a RegisteredGate> {
    gates.iter().find(|gate| gate.spec.name == name)
}

/// Renders `gauntlet list`.
pub fn render_list(gates: &[RegisteredGate]) -> String {
    let width = gates
        .iter()
        .map(|gate| gate.spec.name.len())
        .max()
        .unwrap_or(0);
    let mut out = String::new();
    for gate in gates {
        let name = gate.spec.name;
        let padding = " ".repeat(width.saturating_sub(name.len()));
        out.push_str(&format!(
            "{name}{padding}  {}  {}\n",
            gate.spec.enforcement.label(),
            gate.spec.summary
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The gate list, verbatim. Changing this line is the point.
    const EXPECTED: [&str; 10] = [
        "fmt",
        "clippy",
        "test",
        "coverage",
        "crap",
        "duplication",
        "machete",
        "module-structure",
        "proto-contract",
        "mutants",
    ];

    #[test]
    fn the_registry_holds_exactly_these_gates_in_this_order() {
        let names: Vec<&str> = registry().iter().map(|gate| gate.spec.name).collect();
        assert_eq!(names, EXPECTED);
    }

    #[test]
    fn every_gate_is_blocking() {
        for gate in registry() {
            assert_eq!(
                gate.spec.enforcement,
                Enforcement::Blocking,
                "{} must be blocking: this workspace starts from zero, so there is no debt to \
                 grandfather and no gate has an advisory stage",
                gate.spec.name
            );
        }
    }

    #[test]
    fn crap_is_the_only_gate_with_a_dependency_and_it_needs_coverages_lcov() {
        let with_dependencies: Vec<(&str, &[&str])> = registry()
            .iter()
            .filter(|gate| !gate.spec.depends_on.is_empty())
            .map(|gate| (gate.spec.name, gate.spec.depends_on))
            .collect();
        assert_eq!(with_dependencies, vec![("crap", &["coverage"][..])]);
    }

    #[test]
    fn every_dependency_names_a_registered_gate() {
        let gates = registry();
        for gate in &gates {
            for dependency in gate.spec.depends_on {
                assert!(
                    find(&gates, dependency).is_some(),
                    "{} depends on unregistered gate {dependency}",
                    gate.spec.name
                );
            }
        }
    }

    #[test]
    fn registry_order_already_satisfies_every_dependency() {
        let gates = registry();
        for (position, gate) in gates.iter().enumerate() {
            for dependency in gate.spec.depends_on {
                let at = gates
                    .iter()
                    .position(|other| other.spec.name == *dependency)
                    .expect("dependencies are registered");
                assert!(
                    at < position,
                    "{} runs before its dependency {dependency}",
                    gate.spec.name
                );
            }
        }
    }

    #[test]
    fn gate_names_are_unique() {
        let gates = registry();
        let mut names: Vec<&str> = gates.iter().map(|gate| gate.spec.name).collect();
        names.sort_unstable();
        let count = names.len();
        names.dedup();
        assert_eq!(names.len(), count);
    }

    #[test]
    fn every_gate_carries_a_summary() {
        for gate in registry() {
            assert!(
                !gate.spec.summary.is_empty(),
                "{} has no summary for `gauntlet list`",
                gate.spec.name
            );
        }
    }

    #[test]
    fn a_gate_can_be_looked_up_by_name() {
        let gates = registry();
        assert_eq!(find(&gates, "crap").expect("registered").spec.name, "crap");
        assert!(find(&gates, "no-such-gate").is_none());
    }

    #[test]
    fn the_listing_names_every_gate_with_its_enforcement_and_summary() {
        let gates = registry();
        let listing = render_list(&gates);
        assert_eq!(listing.lines().count(), gates.len());
        for gate in &gates {
            assert!(listing.contains(gate.spec.name), "{listing}");
            assert!(listing.contains(gate.spec.summary), "{listing}");
        }
        assert_eq!(listing.matches("blocking").count(), gates.len());
    }

    #[test]
    fn the_listing_aligns_the_enforcement_column() {
        let gates = registry();
        let listing = render_list(&gates);
        let columns: Vec<Option<usize>> =
            listing.lines().map(|line| line.find("blocking")).collect();
        assert!(
            columns.windows(2).all(|pair| pair[0] == pair[1]),
            "{listing}"
        );
    }

    #[test]
    fn an_empty_registry_lists_nothing() {
        assert_eq!(render_list(&[]), "");
    }
}
