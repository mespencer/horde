//! Module-level dependency cycles, derived from a cargo-modules DOT graph and
//! ratcheted against a committed baseline.
//!
//! cargo-modules draws *items* — functions, types, modules — so a `uses` edge
//! is usually `some::module -> some::other::function`. Filtering the functions
//! out with `--no-fns` would delete the edge along with the node, so this module
//! keeps every node and lifts each endpoint to its enclosing module instead.
//!
//! The ratchet is bidirectional (docs/TESTING.md §4): a new cycle fails the
//! gate, and a baselined cycle that no longer exists *also* fails, so the file
//! can never quietly overstate the debt it licenses.

use std::collections::{BTreeMap, BTreeSet};

/// A group of modules that can all reach each other.
pub type Cycle = Vec<String>;

/// A DOT node declaration: `"probe::tests" [label="pub(crate) mod|probe::tests", …]`.
fn parse_node_line(line: &str) -> Option<(&str, &str)> {
    let trimmed = line.trim();
    if trimmed.contains("->") {
        return None;
    }
    let name = first_quoted(trimmed)?;
    let label = attribute(trimmed, "label")?;
    Some((name, label))
}

/// True for the node kinds that own a module namespace.
fn is_module_label(label: &str) -> bool {
    let kind = label.split('|').next().unwrap_or_default().trim();
    kind == "crate" || kind.ends_with("mod")
}

/// The module nodes of the graph, longest path first so prefix matching is greedy.
pub fn module_nodes(dot: &str) -> Vec<String> {
    let mut modules: Vec<String> = dot
        .lines()
        .filter_map(parse_node_line)
        .filter(|(_, label)| is_module_label(label))
        .map(|(name, _)| name.to_owned())
        .collect();
    modules.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    modules
}

/// The `uses` edges of the graph, as raw item paths.
pub fn use_edges(dot: &str) -> Vec<(String, String)> {
    dot.lines().filter_map(parse_use_edge).collect()
}

fn parse_use_edge(line: &str) -> Option<(String, String)> {
    let (before, after) = line.split_once("->")?;
    if attribute(line, "label")? != "uses" {
        return None;
    }
    let from = first_quoted(before)?;
    let to = first_quoted(after)?;
    Some((from.to_owned(), to.to_owned()))
}

/// The module an item belongs to: the longest module node that is a path prefix.
pub fn enclosing_module<'a>(item: &str, modules: &'a [String]) -> Option<&'a str> {
    modules
        .iter()
        .find(|module| is_path_prefix(module, item))
        .map(String::as_str)
}

fn is_path_prefix(module: &str, item: &str) -> bool {
    if item == module {
        return true;
    }
    match item.strip_prefix(module) {
        Some(rest) => rest.starts_with("::"),
        None => false,
    }
}

/// Lifts item-level `uses` edges to module-level edges, dropping self-edges.
pub fn module_edges(edges: &[(String, String)], modules: &[String]) -> Vec<(String, String)> {
    let mut lifted: BTreeSet<(String, String)> = BTreeSet::new();
    for (from, to) in edges {
        if let (Some(source), Some(target)) = (
            enclosing_module(from, modules),
            enclosing_module(to, modules),
        ) && source != target
        {
            lifted.insert((source.to_owned(), target.to_owned()));
        }
    }
    lifted.into_iter().collect()
}

/// Reads a cargo-modules DOT graph into module-level dependency edges.
pub fn parse_module_graph(dot: &str) -> Vec<(String, String)> {
    module_edges(&use_edges(dot), &module_nodes(dot))
}

fn first_quoted(text: &str) -> Option<&str> {
    let open = text.find('"')?;
    let rest = text.get(open + 1..)?;
    let close = rest.find('"')?;
    rest.get(..close)
}

/// The value of a `key="value"` DOT attribute.
fn attribute<'a>(text: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("{key}=\"");
    let start = text.find(&needle)? + needle.len();
    let rest = text.get(start..)?;
    let end = rest.find('"')?;
    rest.get(..end)
}

/// Every set of modules that are mutually reachable, sorted for stable output.
pub fn find_cycles(edges: &[(String, String)]) -> Vec<Cycle> {
    let names = node_names(edges);
    let mut reachable = adjacency(edges, &names);
    close_transitively(&mut reachable);
    group_mutually_reachable(&reachable, &names)
}

fn node_names(edges: &[(String, String)]) -> Vec<String> {
    let unique: BTreeSet<&str> = edges
        .iter()
        .flat_map(|(from, to)| [from.as_str(), to.as_str()])
        .collect();
    unique.into_iter().map(str::to_owned).collect()
}

fn adjacency(edges: &[(String, String)], names: &[String]) -> Vec<Vec<bool>> {
    let index: BTreeMap<&str, usize> = names
        .iter()
        .enumerate()
        .map(|(position, name)| (name.as_str(), position))
        .collect();
    let mut matrix = vec![vec![false; names.len()]; names.len()];
    for (from, to) in edges {
        if let (Some(row), Some(column)) = (index.get(from.as_str()), index.get(to.as_str())) {
            set_cell(&mut matrix, *row, *column);
        }
    }
    matrix
}

/// Warshall's algorithm: `matrix[i][j]` becomes "j is reachable from i".
fn close_transitively(matrix: &mut [Vec<bool>]) {
    let size = matrix.len();
    for via in 0..size {
        for from in 0..size {
            if !cell(matrix, from, via) {
                continue;
            }
            for to in 0..size {
                if cell(matrix, via, to) {
                    set_cell(matrix, from, to);
                }
            }
        }
    }
}

fn cell(matrix: &[Vec<bool>], row: usize, column: usize) -> bool {
    matrix
        .get(row)
        .and_then(|values| values.get(column))
        .copied()
        .unwrap_or(false)
}

fn set_cell(matrix: &mut [Vec<bool>], row: usize, column: usize) {
    if let Some(target) = matrix
        .get_mut(row)
        .and_then(|values| values.get_mut(column))
    {
        *target = true;
    }
}

fn group_mutually_reachable(reachable: &[Vec<bool>], names: &[String]) -> Vec<Cycle> {
    let mut claimed = vec![false; names.len()];
    let mut cycles = Vec::new();
    for position in 0..names.len() {
        if claimed.get(position).copied().unwrap_or(true) || !cell(reachable, position, position) {
            continue;
        }
        let mut group = Vec::new();
        for (other, name) in names.iter().enumerate() {
            if cell(reachable, position, other) && cell(reachable, other, position) {
                if let Some(flag) = claimed.get_mut(other) {
                    *flag = true;
                }
                group.push(name.clone());
            }
        }
        cycles.push(group);
    }
    cycles.sort();
    cycles
}

const BASELINE_HEADER: &str = "\
# Module cycles this workspace currently licenses, one cycle per line,
# members comma-separated. Bidirectional ratchet: a new cycle fails the gate,
# and so does a line here that no longer describes a real cycle.
# Update by hand-copying the gate's output; there is no --update flag.
";

/// Renders cycles as the committed baseline file's body.
pub fn render_baseline(cycles: &[Cycle]) -> String {
    let mut body = String::from(BASELINE_HEADER);
    for cycle in cycles {
        body.push_str(&cycle.join(", "));
        body.push('\n');
    }
    body
}

/// Reads a baseline file body, ignoring comments and blank lines.
pub fn parse_baseline(text: &str) -> Vec<Cycle> {
    let mut cycles: Vec<Cycle> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(parse_baseline_line)
        .collect();
    cycles.sort();
    cycles
}

fn parse_baseline_line(line: &str) -> Cycle {
    let mut members: Cycle = line
        .split(',')
        .map(str::trim)
        .filter(|member| !member.is_empty())
        .map(str::to_owned)
        .collect();
    members.sort();
    members
}

/// How the observed cycles differ from the committed baseline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleDiff {
    /// Cycles present in the code but not in the baseline.
    pub added: Vec<Cycle>,
    /// Baselined cycles that no longer exist.
    pub removed: Vec<Cycle>,
}

impl CycleDiff {
    /// True when the baseline describes the code exactly.
    pub fn is_clean(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty()
    }
}

/// Compares observed cycles against the baseline, in both directions.
pub fn diff_cycles(found: &[Cycle], baseline: &[Cycle]) -> CycleDiff {
    CycleDiff {
        added: missing_from(found, baseline),
        removed: missing_from(baseline, found),
    }
}

fn missing_from(candidates: &[Cycle], reference: &[Cycle]) -> Vec<Cycle> {
    candidates
        .iter()
        .filter(|candidate| !reference.contains(candidate))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DOT: &str = include_str!("../fixtures/cargo-modules-dependencies.dot");

    fn cycle(members: &[&str]) -> Cycle {
        members.iter().map(|member| (*member).to_owned()).collect()
    }

    fn edges(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(from, to)| ((*from).to_owned(), (*to).to_owned()))
            .collect()
    }

    #[test]
    fn modules_are_recognised_by_their_node_kind() {
        let modules = module_nodes(DOT);
        assert!(modules.contains(&"probe".to_owned()), "{modules:?}");
        assert!(modules.contains(&"probe::tests".to_owned()), "{modules:?}");
        assert!(
            !modules.contains(&"probe::classify".to_owned()),
            "a function is not a module: {modules:?}"
        );
    }

    #[test]
    fn modules_are_ordered_longest_first_so_prefix_matching_is_greedy() {
        let modules = module_nodes(DOT);
        let tests = modules
            .iter()
            .position(|module| module == "probe::tests")
            .expect("present");
        let root = modules
            .iter()
            .position(|module| module == "probe")
            .expect("present");
        assert!(tests < root, "{modules:?}");
    }

    #[test]
    fn only_uses_edges_are_dependencies() {
        let found = use_edges(DOT);
        assert!(
            found.contains(&("probe::tests".to_owned(), "probe::classify".to_owned())),
            "{found:?}"
        );
        assert!(
            !found
                .iter()
                .any(|(from, to)| from == "probe" && to == "probe::tests"),
            "`owns` is structure, not dependency: {found:?}"
        );
    }

    #[test]
    fn an_item_belongs_to_its_longest_enclosing_module() {
        let modules = vec!["a::b".to_owned(), "a".to_owned()];
        assert_eq!(enclosing_module("a::b::f", &modules), Some("a::b"));
        assert_eq!(enclosing_module("a::f", &modules), Some("a"));
        assert_eq!(enclosing_module("a::b", &modules), Some("a::b"));
        assert_eq!(enclosing_module("other::f", &modules), None);
    }

    #[test]
    fn a_shared_name_prefix_is_not_a_path_prefix() {
        let modules = vec!["a".to_owned()];
        assert_eq!(enclosing_module("abc::f", &modules), None);
    }

    #[test]
    fn item_edges_are_lifted_to_the_modules_that_own_them() {
        let modules = vec!["a".to_owned(), "b".to_owned()];
        let lifted = module_edges(&edges(&[("a::f", "b::g")]), &modules);
        assert_eq!(lifted, edges(&[("a", "b")]));
    }

    #[test]
    fn edges_inside_one_module_are_not_dependencies() {
        let modules = vec!["a".to_owned()];
        assert_eq!(
            module_edges(&edges(&[("a::f", "a::g")]), &modules),
            Vec::new()
        );
    }

    #[test]
    fn duplicate_item_edges_collapse_to_one_module_edge() {
        let modules = vec!["a".to_owned(), "b".to_owned()];
        let lifted = module_edges(&edges(&[("a::f", "b::g"), ("a::h", "b::i")]), &modules);
        assert_eq!(lifted, edges(&[("a", "b")]));
    }

    #[test]
    fn a_real_cargo_modules_graph_yields_module_level_edges() {
        let graph = parse_module_graph(DOT);
        assert_eq!(graph, edges(&[("probe::tests", "probe")]));
    }

    #[test]
    fn a_crate_whose_only_edge_is_a_test_module_using_its_parent_is_acyclic() {
        assert_eq!(find_cycles(&parse_module_graph(DOT)), Vec::<Cycle>::new());
    }

    #[test]
    fn an_acyclic_graph_has_no_cycles() {
        assert_eq!(
            find_cycles(&edges(&[("a", "b"), ("b", "c"), ("a", "c")])),
            Vec::<Cycle>::new()
        );
    }

    #[test]
    fn a_two_module_cycle_is_reported_once_with_both_members() {
        assert_eq!(
            find_cycles(&edges(&[("a", "b"), ("b", "a")])),
            vec![cycle(&["a", "b"])]
        );
    }

    #[test]
    fn an_indirect_cycle_is_found_through_the_transitive_closure() {
        assert_eq!(
            find_cycles(&edges(&[("a", "b"), ("b", "c"), ("c", "a")])),
            vec![cycle(&["a", "b", "c"])]
        );
    }

    #[test]
    fn modules_that_only_feed_a_cycle_are_not_part_of_it() {
        let found = find_cycles(&edges(&[
            ("entry", "a"),
            ("a", "b"),
            ("b", "a"),
            ("a", "exit"),
        ]));
        assert_eq!(found, vec![cycle(&["a", "b"])]);
    }

    #[test]
    fn separate_cycles_are_reported_separately_and_sorted() {
        let found = find_cycles(&edges(&[("y", "z"), ("z", "y"), ("a", "b"), ("b", "a")]));
        assert_eq!(found, vec![cycle(&["a", "b"]), cycle(&["y", "z"])]);
    }

    #[test]
    fn baseline_round_trips_through_render_and_parse() {
        let cycles = vec![cycle(&["a", "b"]), cycle(&["y", "z"])];
        assert_eq!(parse_baseline(&render_baseline(&cycles)), cycles);
    }

    #[test]
    fn an_empty_baseline_renders_only_its_explanatory_header() {
        let body = render_baseline(&[]);
        assert!(body.starts_with('#'), "{body}");
        assert_eq!(parse_baseline(&body), Vec::<Cycle>::new());
    }

    #[test]
    fn baseline_members_are_sorted_so_hand_edits_still_match() {
        assert_eq!(parse_baseline("b, a\n"), vec![cycle(&["a", "b"])]);
    }

    #[test]
    fn baseline_ignores_blank_lines_and_comments() {
        assert_eq!(
            parse_baseline("\n# note\n\na, b\n"),
            vec![cycle(&["a", "b"])]
        );
    }

    #[test]
    fn a_matching_baseline_is_clean() {
        let cycles = vec![cycle(&["a", "b"])];
        let diff = diff_cycles(&cycles, &cycles);
        assert!(diff.is_clean());
        assert_eq!(
            diff,
            CycleDiff {
                added: Vec::new(),
                removed: Vec::new()
            }
        );
    }

    #[test]
    fn a_new_cycle_is_reported_as_added() {
        let diff = diff_cycles(&[cycle(&["a", "b"])], &[]);
        assert!(!diff.is_clean());
        assert_eq!(diff.added, vec![cycle(&["a", "b"])]);
        assert!(diff.removed.is_empty());
    }

    #[test]
    fn a_baselined_cycle_that_disappeared_is_reported_as_removed() {
        let diff = diff_cycles(&[], &[cycle(&["a", "b"])]);
        assert!(!diff.is_clean());
        assert_eq!(diff.removed, vec![cycle(&["a", "b"])]);
        assert!(diff.added.is_empty());
    }

    #[test]
    fn a_swapped_cycle_is_both_an_addition_and_a_removal() {
        let diff = diff_cycles(&[cycle(&["a", "b"])], &[cycle(&["y", "z"])]);
        assert_eq!(diff.added, vec![cycle(&["a", "b"])]);
        assert_eq!(diff.removed, vec![cycle(&["y", "z"])]);
    }
}
