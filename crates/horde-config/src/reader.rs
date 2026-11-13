//! The strict reader: a TOML table walked against a role's key table.
//!
//! Every accessor names the key it reads, so the reader learns the schema
//! as it goes; [`Section::finish`] then refuses whatever the file holds that
//! nothing asked for, by full path, listing the keys valid at that level.
//! Type errors name the key, what it wanted, and what it found. Nothing is
//! deserialized leniently: a typo that silently configures nothing is the
//! failure ADR-0016 exists to prevent.

use std::time::Duration;

use toml::{Table, Value};

/// One table of the file, at a dotted path.
#[derive(Debug)]
pub struct Section<'a> {
    path: String,
    table: &'a Table,
    known: Vec<String>,
    foreign: Vec<(String, &'static str)>,
}

/// How a value reads in a refusal: its TOML type, and the value itself
/// when it is short enough to quote.
fn describe(value: &Value) -> String {
    match value {
        Value::String(text) => format!("string {text:?}"),
        Value::Integer(number) => format!("integer {number}"),
        Value::Float(number) => format!("float {number}"),
        Value::Boolean(flag) => format!("boolean {flag}"),
        Value::Datetime(when) => format!("datetime {when}"),
        Value::Array(_) => "an array".to_owned(),
        Value::Table(_) => "a table".to_owned(),
    }
}

impl<'a> Section<'a> {
    /// The whole file.
    #[must_use]
    pub fn root(table: &'a Table) -> Self {
        Self::at(String::new(), table)
    }

    fn at(path: String, table: &'a Table) -> Self {
        Self {
            path,
            table,
            known: Vec::new(),
            foreign: Vec::new(),
        }
    }

    /// This section's dotted path; empty for the file itself.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The dotted path of `key` in this section.
    #[must_use]
    pub fn key_path(&self, key: &str) -> String {
        if self.path.is_empty() {
            key.to_owned()
        } else {
            format!("{}.{key}", self.path)
        }
    }

    /// A refusal about `key` that no type check catches — a cross-key rule.
    #[must_use]
    pub fn refuse(&self, key: &str, message: impl std::fmt::Display) -> String {
        format!("{}: {message}", self.key_path(key))
    }

    fn take(&mut self, key: &str) -> Option<&'a Value> {
        self.known.push(key.to_owned());
        self.table.get(key)
    }

    fn mismatch(&self, key: &str, expected: &str, found: &Value) -> String {
        self.refuse(
            key,
            format!("expected {expected}, found {}", describe(found)),
        )
    }

    /// Reads `key` with `convert`, which answers `None` for the wrong type.
    fn typed<T>(
        &mut self,
        key: &str,
        expected: &str,
        convert: impl Fn(&'a Value) -> Option<T>,
    ) -> Result<Option<T>, String> {
        match self.take(key) {
            None => Ok(None),
            Some(value) => convert(value)
                .map(Some)
                .ok_or_else(|| self.mismatch(key, expected, value)),
        }
    }

    /// A string.
    ///
    /// # Errors
    ///
    /// The key holds another type.
    pub fn string(&mut self, key: &str) -> Result<Option<String>, String> {
        self.typed(key, "a string", |value| value.as_str().map(str::to_owned))
    }

    /// A boolean.
    ///
    /// # Errors
    ///
    /// The key holds another type.
    pub fn boolean(&mut self, key: &str) -> Result<Option<bool>, String> {
        self.typed(key, "true or false", Value::as_bool)
    }

    /// A non-negative integer that fits `T`.
    ///
    /// # Errors
    ///
    /// The key holds another type, or a number outside `T`.
    pub fn count<T: TryFrom<i64>>(&mut self, key: &str) -> Result<Option<T>, String> {
        let Some(number) = self.typed(key, "a whole number", Value::as_integer)? else {
            return Ok(None);
        };
        T::try_from(number)
            .map(Some)
            .map_err(|_| self.refuse(key, format!("{number} is out of range")))
    }

    /// A count where zero would disable what it bounds.
    ///
    /// # Errors
    ///
    /// As [`Section::count`], and zero.
    pub fn positive_count<T: TryFrom<i64>>(&mut self, key: &str) -> Result<Option<T>, String> {
        match self.count::<i64>(key)? {
            Some(0) => Err(self.refuse(key, "must be at least 1")),
            Some(number) => T::try_from(number)
                .map(Some)
                .map_err(|_| self.refuse(key, format!("{number} is out of range"))),
            None => Ok(None),
        }
    }

    /// A size where zero would disable what it bounds.
    ///
    /// # Errors
    ///
    /// As [`Section::size`], and zero.
    pub fn positive_size(&mut self, key: &str) -> Result<Option<u64>, String> {
        match self.size(key)? {
            Some(0) => Err(self.refuse(key, "must be at least 1")),
            bytes => Ok(bytes),
        }
    }

    /// A number; an integer is accepted as one.
    ///
    /// # Errors
    ///
    /// The key holds another type.
    pub fn number(&mut self, key: &str) -> Result<Option<f64>, String> {
        self.typed(key, "a number", |value| {
            value
                .as_float()
                .or_else(|| value.as_integer().map(|number| number as f64))
        })
    }

    /// A string parsed by `parse`, whose refusal is quoted after the key.
    fn parsed<T>(
        &mut self,
        key: &str,
        expected: &str,
        parse: impl Fn(&str) -> Result<T, String>,
    ) -> Result<Option<T>, String> {
        let Some(text) = self.typed(key, expected, Value::as_str)? else {
            return Ok(None);
        };
        parse(text)
            .map(Some)
            .map_err(|error| self.refuse(key, error))
    }

    /// A duration string (`"60s"`, `"4h"`, `"0"`).
    ///
    /// # Errors
    ///
    /// The key holds another type, or an unparseable duration.
    pub fn duration(&mut self, key: &str) -> Result<Option<Duration>, String> {
        self.parsed(
            key,
            r#"a duration such as "60s" or "4h""#,
            horde_proto::units::parse_duration,
        )
    }

    /// A size string (`"512MiB"`).
    ///
    /// # Errors
    ///
    /// The key holds another type, or an unparseable size.
    pub fn size(&mut self, key: &str) -> Result<Option<u64>, String> {
        self.parsed(
            key,
            r#"a size such as "512MiB" or "20GiB""#,
            horde_proto::units::parse_size,
        )
    }

    /// A free-space threshold string (`"10%"`, `"20GiB"`, `"10%,20GiB"`).
    ///
    /// # Errors
    ///
    /// The key holds another type, or an unparseable threshold.
    pub fn threshold(&mut self, key: &str) -> Result<Option<(u64, u64)>, String> {
        self.parsed(
            key,
            r#"a threshold such as "10%", "20GiB" or "10%,20GiB""#,
            horde_proto::units::parse_threshold,
        )
    }

    /// An array of strings.
    ///
    /// # Errors
    ///
    /// The key holds another type, or an element that is not a string.
    pub fn strings(&mut self, key: &str) -> Result<Option<Vec<String>>, String> {
        let Some(items) = self.typed(key, "an array of strings", Value::as_array)? else {
            return Ok(None);
        };
        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| self.mismatch(&format!("{key}[{index}]"), "a string", item))
            })
            .collect::<Result<_, _>>()
            .map(Some)
    }

    /// A table, as a section of its own (which its caller must finish).
    ///
    /// # Errors
    ///
    /// The key holds another type.
    pub fn section(&mut self, key: &str) -> Result<Option<Section<'a>>, String> {
        let path = self.key_path(key);
        Ok(self
            .typed(key, "a table", Value::as_table)?
            .map(|table| Self::at(path, table)))
    }

    /// An array of tables (`[[key]]` or `key = [{…}]`), each a section.
    ///
    /// # Errors
    ///
    /// The key holds another type, or an element that is not a table.
    pub fn sections(&mut self, key: &str) -> Result<Option<Vec<Section<'a>>>, String> {
        let Some(items) = self.typed(key, "an array of tables", Value::as_array)? else {
            return Ok(None);
        };
        items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let element = format!("{key}[{index}]");
                item.as_table()
                    .map(|table| Self::at(self.key_path(&element), table))
                    .ok_or_else(|| self.mismatch(&element, "a table", item))
            })
            .collect::<Result<_, _>>()
            .map(Some)
    }

    /// A table whose keys are names the operator chose (`[tiers.<name>]`),
    /// each value a section.
    ///
    /// # Errors
    ///
    /// The key holds another type, or a named entry that is not a table.
    pub fn named(&mut self, key: &str) -> Result<Option<Vec<(String, Section<'a>)>>, String> {
        let Some(mut outer) = self.section(key)? else {
            return Ok(None);
        };
        let names: Vec<String> = outer.table.keys().cloned().collect();
        let named = names
            .into_iter()
            .map(|name| {
                let inner = outer.section(&name)?;
                Ok((name, inner))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Some(
            named
                .into_iter()
                .filter_map(|(name, inner)| inner.map(|inner| (name, inner)))
                .collect(),
        ))
    }

    /// A table of operator-chosen names to strings (`[codex.sandbox]`).
    ///
    /// # Errors
    ///
    /// The key holds another type, or a value that is not a string.
    pub fn string_map(&mut self, key: &str) -> Result<Option<Vec<(String, String)>>, String> {
        let Some(mut outer) = self.section(key)? else {
            return Ok(None);
        };
        let names: Vec<String> = outer.table.keys().cloned().collect();
        names
            .into_iter()
            .map(|name| {
                let value = outer.string(&name)?.unwrap_or_default();
                Ok((name, value))
            })
            .collect::<Result<_, _>>()
            .map(Some)
    }

    /// Declares `key` as another role's: present here, it is refused with
    /// that role's name rather than as unknown.
    pub fn foreign(&mut self, key: &str, role: &'static str) {
        self.foreign.push((key.to_owned(), role));
    }

    /// Refuses every key nothing asked for.
    ///
    /// # Errors
    ///
    /// The first such key, by path: another role's with that role's name,
    /// otherwise with the keys valid at this level.
    pub fn finish(mut self) -> Result<(), String> {
        let Some(stray) = self
            .table
            .keys()
            .find(|key| !self.known.contains(key))
            .cloned()
        else {
            return Ok(());
        };
        if let Some((_, role)) = self.foreign.iter().find(|(key, _)| *key == stray) {
            return Err(format!(
                "{} belongs in the {role}'s config file, not this one",
                self.key_path(&stray)
            ));
        }
        self.known.sort();
        self.known.dedup();
        Err(format!(
            "unknown key {} (known here: {})",
            self.key_path(&stray),
            self.known.join(", ")
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use toml::Table;

    use super::Section;

    fn table(text: &str) -> Table {
        text.parse().expect("test TOML parses")
    }

    #[test]
    fn scalars_read_and_absent_keys_are_none() {
        let file = table(
            r#"
            name = "x"
            on = true
            n = 7
            f = 0.5
            whole = 2
            d = "90s"
            s = "2KiB"
            t = "10%,1GiB"
            list = ["a", "b"]
            "#,
        );
        let mut root = Section::root(&file);
        assert_eq!(root.string("name"), Ok(Some("x".to_owned())));
        assert_eq!(root.boolean("on"), Ok(Some(true)));
        assert_eq!(root.count::<u32>("n"), Ok(Some(7)));
        assert_eq!(root.number("f"), Ok(Some(0.5)));
        assert_eq!(root.number("whole"), Ok(Some(2.0)));
        assert_eq!(root.duration("d"), Ok(Some(Duration::from_secs(90))));
        assert_eq!(root.size("s"), Ok(Some(2048)));
        assert_eq!(root.threshold("t"), Ok(Some((1_000, 1 << 30))));
        assert_eq!(
            root.strings("list"),
            Ok(Some(vec!["a".to_owned(), "b".to_owned()]))
        );
        assert_eq!(root.string("absent"), Ok(None));
        assert_eq!(root.duration("absent_too"), Ok(None));
        assert_eq!(root.finish(), Ok(()));
    }

    #[test]
    fn a_type_error_names_the_key_what_it_wanted_and_what_it_found() {
        let file = table(
            r#"
            [workspace]
            fetch_throttle = 60
            on = "yes"
            n = -1
            list = ["a", 2]
            t = 1.5
            d = 2024-01-01
            "#,
        );
        let mut root = Section::root(&file);
        let mut workspace = root
            .section("workspace")
            .expect("a table")
            .expect("present");
        assert_eq!(
            workspace.duration("fetch_throttle"),
            Err(r#"workspace.fetch_throttle: expected a duration such as "60s" or "4h", found integer 60"#.to_owned())
        );
        assert_eq!(
            workspace.boolean("on"),
            Err(r#"workspace.on: expected true or false, found string "yes""#.to_owned())
        );
        assert_eq!(
            workspace.count::<u32>("n"),
            Err("workspace.n: -1 is out of range".to_owned())
        );
        assert_eq!(
            workspace.strings("list"),
            Err("workspace.list[1]: expected a string, found integer 2".to_owned())
        );
        assert_eq!(
            workspace.section("t").map(|section| section.is_some()),
            Err("workspace.t: expected a table, found float 1.5".to_owned())
        );
        assert_eq!(
            workspace.string("d"),
            Err("workspace.d: expected a string, found datetime 2024-01-01".to_owned())
        );
        let mut root = Section::root(&file);
        assert_eq!(
            root.strings("workspace"),
            Err("workspace: expected an array of strings, found a table".to_owned())
        );
        let file = table("x = [1]\ny = [[1]]");
        let mut root = Section::root(&file);
        assert_eq!(
            root.string("x"),
            Err("x: expected a string, found an array".to_owned())
        );
    }

    #[test]
    fn a_value_error_quotes_the_parser_after_the_key() {
        let file = table(r#"d = "4w""#);
        let mut root = Section::root(&file);
        let refused = root.duration("d").expect_err("not a duration");
        assert!(
            refused.starts_with("d: \"4w\" is not a duration"),
            "{refused}"
        );
        let file = table(r#"s = "1GB""#);
        assert!(
            Section::root(&file)
                .size("s")
                .expect_err("not a size")
                .starts_with("s: \"1GB\" is not a size")
        );
        let file = table(r#"t = "110%""#);
        assert!(
            Section::root(&file)
                .threshold("t")
                .expect_err("not a threshold")
                .contains("percentage")
        );
    }

    #[test]
    fn an_unknown_key_is_refused_by_path_with_the_keys_known_there() {
        let file = table("[workspace]\nfetch_throtle = \"1s\"\n");
        let mut root = Section::root(&file);
        let mut workspace = root
            .section("workspace")
            .expect("a table")
            .expect("present");
        workspace.duration("fetch_timeout").expect("absent");
        workspace.duration("fetch_throttle").expect("absent");
        workspace
            .duration("fetch_timeout")
            .expect("asked twice, listed once");
        assert_eq!(
            workspace.finish(),
            Err(
                "unknown key workspace.fetch_throtle (known here: fetch_throttle, fetch_timeout)"
                    .to_owned()
            )
        );
        assert_eq!(root.finish(), Ok(()));

        let file = table("stray = 1");
        assert_eq!(
            Section::root(&file).finish(),
            Err("unknown key stray (known here: )".to_owned())
        );
    }

    #[test]
    fn another_roles_key_is_refused_with_that_roles_name() {
        let file = table("[worker]\nx = 1\n");
        let mut root = Section::root(&file);
        root.foreign("codex", "worker");
        root.foreign("worker", "worker");
        assert_eq!(
            root.finish(),
            Err("worker belongs in the worker's config file, not this one".to_owned())
        );
    }

    #[test]
    fn arrays_of_tables_and_named_tables_are_sections_at_their_paths() {
        let file = table(
            r#"
            [[rules]]
            marker = "a"
            [[rules]]
            marker = "b"
            extra = 1
            [tiers.fast]
            x = 1
            [tiers.deep]
            [sandbox]
            builder = "workspace-write"
            "#,
        );
        let mut root = Section::root(&file);
        let rules = root.sections("rules").expect("tables").expect("present");
        let paths: Vec<&str> = rules.iter().map(Section::path).collect();
        assert_eq!(paths, ["rules[0]", "rules[1]"]);
        let mut second = rules.into_iter().nth(1).expect("two");
        assert_eq!(second.string("marker"), Ok(Some("b".to_owned())));
        assert_eq!(
            second.finish(),
            Err("unknown key rules[1].extra (known here: marker)".to_owned())
        );

        let tiers = root.named("tiers").expect("tables").expect("present");
        let names: Vec<(&str, &str)> = tiers
            .iter()
            .map(|(name, section)| (name.as_str(), section.path()))
            .collect();
        assert_eq!(names, [("deep", "tiers.deep"), ("fast", "tiers.fast")]);

        assert_eq!(
            root.string_map("sandbox"),
            Ok(Some(vec![(
                "builder".to_owned(),
                "workspace-write".to_owned()
            )]))
        );
        assert_eq!(root.sections("absent").map(|s| s.is_none()), Ok(true));
        assert_eq!(root.named("absent2").map(|s| s.is_none()), Ok(true));
        assert_eq!(root.string_map("absent3"), Ok(None));
        assert_eq!(root.finish(), Ok(()));
    }

    #[test]
    fn malformed_collections_name_the_offending_element() {
        let file = table("rules = [1]\ntiers = { fast = 1 }\nsandbox = { a = 1 }\nr2 = 3");
        let mut root = Section::root(&file);
        assert_eq!(
            root.sections("rules").map(|s| s.is_some()),
            Err("rules[0]: expected a table, found integer 1".to_owned())
        );
        assert_eq!(
            root.named("tiers").map(|s| s.is_some()),
            Err("tiers.fast: expected a table, found integer 1".to_owned())
        );
        assert_eq!(
            root.string_map("sandbox"),
            Err("sandbox.a: expected a string, found integer 1".to_owned())
        );
        assert_eq!(
            root.sections("r2").map(|s| s.is_some()),
            Err("r2: expected an array of tables, found integer 3".to_owned())
        );
        assert_eq!(root.refuse("k", "why"), "k: why");
    }
}
