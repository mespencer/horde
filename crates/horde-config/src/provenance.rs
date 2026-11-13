//! The effective settings of one role, each with the layer that decided it.

use crate::origin::Origin;

/// One effective key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The dotted key path, as the file spells it.
    pub key: String,
    /// The value, rendered the way the file would spell it.
    pub value: String,
    pub origin: Origin,
}

/// Every effective key of one role, in key order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Provenance {
    entries: Vec<Entry>,
}

impl Provenance {
    /// Records `key`, replacing an earlier record of the same key.
    pub fn record(&mut self, key: impl Into<String>, value: impl Into<String>, origin: Origin) {
        let key = key.into();
        let entry = Entry {
            key,
            value: value.into(),
            origin,
        };
        match self
            .entries
            .binary_search_by(|probe| probe.key.cmp(&entry.key))
        {
            Ok(at) => self.entries[at] = entry,
            Err(at) => self.entries.insert(at, entry),
        }
    }

    /// The most specific layer that set a value, else the default, recorded
    /// under `key` with its origin and returned.
    ///
    /// `layers` run least to most specific; a later `Some` wins.
    pub fn pick<T>(
        &mut self,
        key: &str,
        default: T,
        layers: Vec<(Option<T>, Origin)>,
        show: impl Fn(&T) -> String,
    ) -> T {
        let (value, origin) =
            layers
                .into_iter()
                .fold((default, Origin::Default), |chosen, layer| match layer {
                    (Some(value), origin) => (value, origin),
                    (None, _) => chosen,
                });
        self.record(key, show(&value), origin);
        value
    }

    /// The entries, in key order.
    #[must_use]
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The entry for `key`, if recorded.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.key == key)
    }

    /// Adds every entry of `other`, replacing same-named ones.
    pub fn extend(&mut self, other: Provenance) {
        for entry in other.entries {
            self.record(entry.key, entry.value, entry.origin);
        }
    }

    /// One `key = value` line per key, with `  # origin` appended when
    /// `origins` is set — what `config show [--origin]` prints.
    #[must_use]
    pub fn render(&self, origins: bool) -> String {
        self.entries
            .iter()
            .map(|entry| {
                if origins {
                    format!("{} = {}  # {}\n", entry.key, entry.value, entry.origin)
                } else {
                    format!("{} = {}\n", entry.key, entry.value)
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::Provenance;
    use crate::origin::Origin;

    fn show(value: &u32) -> String {
        value.to_string()
    }

    #[test]
    fn the_most_specific_layer_that_set_a_value_wins() {
        let mut provenance = Provenance::default();
        let file = Origin::File("/c.toml".into());
        let flag = Origin::Flag("--n".to_owned());

        let none = provenance.pick(
            "a",
            1,
            vec![(None, file.clone()), (None, flag.clone())],
            show,
        );
        let filed = provenance.pick(
            "b",
            1,
            vec![(Some(2), file.clone()), (None, flag.clone())],
            show,
        );
        let flagged = provenance.pick("c", 1, vec![(Some(2), file), (Some(3), flag)], show);

        assert_eq!((none, filed, flagged), (1, 2, 3));
        assert_eq!(
            provenance.render(true),
            "a = 1  # default\nb = 2  # file /c.toml\nc = 3  # flag --n\n"
        );
        assert_eq!(provenance.render(false), "a = 1\nb = 2\nc = 3\n");
    }

    #[test]
    fn records_stay_in_key_order_and_a_rerecord_replaces() {
        let mut provenance = Provenance::default();
        provenance.record("z", "1", Origin::Default);
        provenance.record("a", "1", Origin::Default);
        provenance.record("m", "1", Origin::Default);
        provenance.record("a", "2", Origin::Env("V".to_owned()));
        let keys: Vec<&str> = provenance
            .entries()
            .iter()
            .map(|entry| entry.key.as_str())
            .collect();
        assert_eq!(keys, ["a", "m", "z"]);
        let a = provenance.get("a").expect("recorded");
        assert_eq!(
            (a.value.as_str(), &a.origin),
            ("2", &Origin::Env("V".to_owned()))
        );
        assert_eq!(provenance.get("nope"), None);

        let mut other = Provenance::default();
        other.record("m", "9", Origin::Default);
        other.record("b", "1", Origin::Default);
        provenance.extend(other);
        assert_eq!(provenance.render(false), "a = 2\nb = 1\nm = 9\nz = 1\n");
    }
}
