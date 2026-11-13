//! Windows process probes for the suites that watch a job's members die
//! (ADR-0017). By `tasklist`, never by signal: Windows has none to probe
//! with, and the probe must not be able to kill what it watches.
//!
//! Every item is `cfg(windows)`: the Linux gauntlet compiles none of it, so
//! `.cargo/mutants.toml` excludes this file. The module itself is always
//! declared, so the module-structure gate sees a file it can account for.

/// Whether `pid` is a running process. A `tasklist` that cannot run
/// answers "running": a probe that cannot see must never let a survivor
/// pass for gone.
#[cfg(windows)]
pub async fn running(pid: u32) -> bool {
    let listed = tokio::process::Command::new("tasklist.exe")
        .args(["/FI", &format!("PID eq {pid}"), "/NH", "/FO", "CSV"])
        .output()
        .await;
    listed.map_or(true, |output| {
        String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
    })
}

/// Whether `pid` is gone within ten seconds.
#[cfg(windows)]
pub async fn gone_soon(pid: u32) -> bool {
    for _ in 0..100 {
        if !running(pid).await {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    false
}
