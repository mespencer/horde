//! Executable stand-in scripts a test writes and then has a child exec.
//!
//! Writing one with `std::fs::write` and a chmod races under a
//! multi-threaded runner: while this process holds the script open for
//! writing, another test's spawn can fork, and its child keeps that write
//! handle until its own exec. Exec'ing the script meanwhile fails with
//! ETXTBSY ("Text file busy"). [`install`] never opens the script for
//! writing in this process: a draft beside it is written here (and never
//! executed), and `install` copies it into place in a child process, which
//! holds the only writable handle the script ever has.

use std::path::{Path, PathBuf};

/// Puts `content` at `path` as an executable (mode 0755) script.
///
/// # Errors
///
/// When the draft cannot be written or removed, `install` cannot run, or
/// it fails.
pub fn install(path: &Path, content: &str) -> std::io::Result<()> {
    let draft = draft_of(path);
    std::fs::write(&draft, content)?;
    let installed = std::process::Command::new("install")
        .args(["-m", "0755"])
        .arg(&draft)
        .arg(path)
        .status()?;
    std::fs::remove_file(&draft)?;
    if installed.success() {
        Ok(())
    } else {
        Err(std::io::Error::other(format!(
            "install could not place {}: {installed}",
            path.display()
        )))
    }
}

/// The draft beside `path`: `.<name>.draft`.
fn draft_of(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    path.with_file_name(format!(".{name}.draft"))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::install;

    /// The script lands with its content and mode 0755, runs, and leaves
    /// no draft behind.
    #[test]
    fn an_installed_script_runs_and_leaves_no_draft() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let script = dir.path().join("hello");
        install(&script, "#!/bin/sh\necho installed\n").expect("installs");
        let mode = std::fs::metadata(&script)
            .expect("present")
            .permissions()
            .mode();
        assert_eq!(mode & 0o7777, 0o755);
        let ran = std::process::Command::new(&script)
            .output()
            .expect("the script runs");
        assert_eq!(String::from_utf8_lossy(&ran.stdout), "installed\n");
        let left: Vec<_> = std::fs::read_dir(dir.path())
            .expect("readable")
            .map(|entry| entry.expect("an entry").file_name())
            .collect();
        assert_eq!(left, [std::ffi::OsString::from("hello")]);
    }

    /// Installing over an existing script replaces it.
    #[test]
    fn a_second_install_replaces_the_script() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let script = dir.path().join("twice");
        install(&script, "#!/bin/sh\necho first\n").expect("installs");
        install(&script, "#!/bin/sh\necho second\n").expect("reinstalls");
        let ran = std::process::Command::new(&script)
            .output()
            .expect("the script runs");
        assert_eq!(String::from_utf8_lossy(&ran.stdout), "second\n");
    }

    /// A place `install` cannot write is an error naming it, and the draft
    /// is still cleaned up. (`install` copies into a directory at `path`;
    /// a non-empty directory where that copy would land stops it.)
    #[test]
    fn a_failed_install_is_an_error_naming_the_path() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let blocked = dir.path().join("blocked");
        std::fs::create_dir_all(blocked.join(".blocked.draft/x")).expect("a directory in the way");
        let refused = install(&blocked, "#!/bin/sh\n").expect_err("a directory is in the way");
        assert!(refused.to_string().contains("blocked"), "{refused}");
        assert!(!dir.path().join(".blocked.draft").exists());
    }
}
