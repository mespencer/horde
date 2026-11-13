//! Where a user's home is, on every platform Horde runs on (ADR-0017).
//!
//! Every default state directory (`~/.horde/…`) hangs off it. Unix has one
//! answer, `$HOME`. Windows usually sets no `HOME` at all; its home is
//! `%USERPROFILE%` — whose inherited ACL (the user, SYSTEM, Administrators)
//! is also what keeps a default state directory private there. A `HOME` set
//! on Windows (Git Bash, MSYS, an operator's choice) still wins, so one
//! machine never has two homes depending on the shell that started Horde.

use std::ffi::OsString;
use std::path::PathBuf;

/// This process's home directory: `$HOME`, or on Windows `%USERPROFILE%`
/// when `HOME` is unset or empty. `None` when neither says.
#[must_use]
pub fn home_dir() -> Option<PathBuf> {
    home_from(cfg!(windows), |name| std::env::var_os(name))
}

/// [`home_dir`] with the platform and the environment passed in, so both
/// platforms' rules are pinned by tests on either.
pub fn home_from(windows: bool, var: impl Fn(&str) -> Option<OsString>) -> Option<PathBuf> {
    if !windows {
        return var("HOME").map(PathBuf::from);
    }
    ["HOME", "USERPROFILE"]
        .into_iter()
        .filter_map(var)
        .find(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::path::PathBuf;

    use super::home_from;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<OsString> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| OsString::from(value))
        }
    }

    /// Unix reads `HOME` alone, exactly as before — even an empty one, and
    /// never a `USERPROFILE` that happens to be set.
    #[test]
    fn unix_reads_home_alone() {
        assert_eq!(
            home_from(false, env(&[("HOME", "/home/a"), ("USERPROFILE", "C:\\x")])),
            Some(PathBuf::from("/home/a"))
        );
        assert_eq!(home_from(false, env(&[("USERPROFILE", "C:\\x")])), None);
        assert_eq!(home_from(false, env(&[("HOME", "")])), Some(PathBuf::new()));
    }

    /// Windows prefers a set `HOME`, and otherwise takes `USERPROFILE`; an
    /// empty value is no answer.
    #[test]
    fn windows_falls_back_to_the_user_profile() {
        assert_eq!(
            home_from(
                true,
                env(&[("HOME", "D:\\h"), ("USERPROFILE", "C:\\Users\\a")])
            ),
            Some(PathBuf::from("D:\\h"))
        );
        assert_eq!(
            home_from(true, env(&[("USERPROFILE", "C:\\Users\\a")])),
            Some(PathBuf::from("C:\\Users\\a"))
        );
        assert_eq!(
            home_from(true, env(&[("HOME", ""), ("USERPROFILE", "C:\\Users\\a")])),
            Some(PathBuf::from("C:\\Users\\a"))
        );
        assert_eq!(home_from(true, env(&[])), None);
        assert_eq!(home_from(true, env(&[("USERPROFILE", "")])), None);
    }

    /// This process's home is a real directory: the suites run with `HOME`
    /// set (on Windows, at least `USERPROFILE`), and an empty path is no
    /// home.
    #[test]
    fn this_process_has_a_home_directory() {
        let home = super::home_dir().expect("a home");
        assert!(home.is_dir(), "{}", home.display());
    }
}
