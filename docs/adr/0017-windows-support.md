# Windows support: a Worker on Windows, with Job Objects for process trees and the profile ACL for privacy

Horde ran only on Unix. Every place that touches the OS assumed it: a
harness was the leader of a process group signalled with `killpg`
(ADR-0010); stop requests were SIGTERM, re-mints were SIGHUP; secrets and
state directories were private because of `0600`/`0700` modes; free disk
came from `statvfs` or `df`; homes came from `$HOME`; unmanaged workspace
paths were symlinks; the test tiers sent signals to process groups and read
`ps`. None of that exists on Windows, so nothing compiled for it.

The operator decided on 2026-11-13 (docs/WAVE12.md, W12g) that a Windows
machine must be able to be a Worker running `claude.exe` and `codex.exe`,
with the CLI and the server building there too. This ADR records how.

## Decision

### Scope

* **Supported on Windows:** `horde-worker` (the point), `horde` (the CLI),
  and `horde-server`. Target: `x86_64-pc-windows-msvc`.
* **Unix behaviour is unchanged.** Every Windows path sits behind
  `cfg(windows)` / `cfg(not(unix))` next to the Unix one, which keeps its
  code and its tests. The one Unix change is an unobservable tightening: a
  harness's group signal now goes through the same guard a reclaim
  command's always did (never this process's own group or pid), which a
  child spawned with `process_group(0)` can never trip.

### Process trees: Job Objects, reached by handle

`horde_worker::process_tree::ProcessTree` is the seam. A harness
(`process.rs`) and a soft-reclaim command (`workspace/reclaim.rs`) spawn
through it and stop it with a `Stop`:

| | Unix | Windows |
|---|---|---|
| tree | a new process group (`process_group(0)`) | a new Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` |
| joins how | at `exec` | created `CREATE_SUSPENDED`, assigned, then resumed — nothing it starts is ever outside the job |
| reached by | group id, `killpg` | handle, `TerminateJobObject` |
| `Stop::Terminate` | SIGTERM to the group | nothing (see below) |
| `Stop::Kill` | SIGKILL to the group | `TerminateJobObject` |
| handle dropped | `kill_on_drop` takes the leader; `Drop` SIGKILLs the group | the job's last handle closes, which kills every member |
| Worker crash | group ids in the journal ledger; the next start sweeps them | the kernel closes the Worker's handles, so every job dies with it |

The Win32 calls come from `process-wrap` (`job-object`, `kill-on-drop`,
`tokio1`): the workspace forbids `unsafe`, and the crate was already in the
lock file through `rmcp`. The Worker needs 10.0 or later: 9.x moved its
wrapper registry out of the command while the hooks ran, so the job wrapper
never saw `KillOnDrop` and made the job *without* kill-on-close — a dropped
tree lost its root but left every grandchild running (found by
`dropping_the_tree_kills_the_job` on the first Windows run). `rmcp` keeps
its own 9.x.

**Graceful stop on Windows is stdin EOF.** A console-less child has no
SIGTERM, and `GenerateConsoleCtrlEvent` needs a shared console (which a
daemon does not have) and `unsafe`. Both harness protocols end on stdin
EOF — Claude's stream-json and Codex's app-server — so
`SupervisedHarness::stop` closes the harness's stdin, waits the same stop
grace, and terminates the job if the leader has not exited. A write after
that is `SupervisorError::HungUp`, as it is for any closed stdin. A
timed-out reclaim command has no stdin to close, so its job is terminated
at once (Unix still offers SIGTERM and a 5 s grace).

**No ledger on Windows.** `remember_group` writes nothing there and
`reap_leftover_groups` only forgets rows, returning 0: a Windows Worker
that crashed has nothing left running, so there is nothing to sweep, and a
pid on Windows is no handle to a job.

### Privacy of state: the profile's inherited ACL

Windows has no mode bits. Setting an explicit DACL needs Win32 security
calls (or a crate that makes them) and per-file work whose failure modes
nobody here can test. The default state directories live under the user's
profile (`%USERPROFILE%\.horde\…`, see *Homes*), and a profile's inherited
ACL already grants only the user, SYSTEM and Administrators. So on
Windows:

* `create_private_dir`, `write_secret`, the harness profile's owner-only
  directories and secret files are created plainly and inherit that ACL.
* `horde doctor` reports a state directory as "inherits its ACL" with the
  `icacls` command that shows it, never as a mode.
* **An operator who points `--state-dir` outside the profile owns that
  directory's ACL.** That is the documented equivalent of `chmod 700`, and
  the limitation this ADR accepts.

### Homes

`horde_identity::home_dir()` is `$HOME` on Unix (exactly as before) and,
on Windows, `HOME` when set and non-empty, else `%USERPROFILE%`. Every
default that read `$HOME` — the worker's, server's and client's state and
config directories, `horde up`'s root, the client identity — reads it.

### Signals

* **Worker stop:** ctrl-c, ctrl-break, console close and system shutdown
  (`tokio::signal::windows`) stand in for SIGTERM.
* **`horde up`:** ctrl-c, ctrl-break or console close stop it. Its daemons
  share its console, so the same ctrl-c reaches them; the teardown keeps its
  bounded wait (there is no `kill -TERM` to send) and kills what remains.
  `sibling_binary` looks for `horde-server.exe` / `horde-worker.exe`.
* **`horde-server --remint`:** unsupported. There is no SIGHUP, so the
  server's hang-up channel is born closed (no re-mints, the server stays
  up), and `--remint` refuses in words: restart the server, and starting
  mints a fresh bootstrap token — the only re-mint before `--remint`
  existed. A file-based request is possible later; nothing needs it yet.
  (Windows file locks are mandatory, so another process cannot read the pid
  in a held lock file either; refusals that would name the holder's pid
  name none.)

### The interactive login relay: ConPTY

`portable-pty` already opens a ConPTY on Windows, so `horde worker login`
works there with one fix. A ConPTY's output pipe reports EOF only when the
pseudoconsole closes, never when its child exits, and the relay ends a
login on output EOF. On Windows the master side is shared with the reaping
wait, which closes the pseudoconsole as soon as the child is reaped; the
reader then sees EOF and the relay's existing exit path runs. Resizes after
that are no-ops. The client side (`horde worker login` in a Windows
terminal) has no SIGWINCH, so it polls the console size twice a second and
sends each change.

### Smaller differences

* **Disk space:** `fs4::statvfs` (`GetDiskFreeSpaceExW`) for the workspace
  pressure check and `horde doctor`, where Unix uses `statvfs` / `df`.
* **Unmanaged workspace paths:** a directory symlink where the account may
  make one (Developer Mode, or an elevated Worker), otherwise an NTFS
  junction (the `junction` crate), which needs no privilege.
* **Reclaim environment:** a reclaim command's cleared environment keeps
  `SystemRoot`, `SystemDrive`, `windir`, `ComSpec`, `PATHEXT`,
  `USERPROFILE`, `LOCALAPPDATA`, `APPDATA`, `TEMP` and `TMP` besides the
  Unix set — programs fail to start, or to open sockets, without
  `SystemRoot`.
* **Harness binaries:** the Worker launches `claude` and `codex` by bare
  name, which Windows resolves to `claude.exe` / `codex.exe` on `PATH` and
  nothing else. A `.cmd` shim (npm's) is not a program to `CreateProcess`,
  and `horde doctor` does not count one. Claude Code's native installer
  provides `claude.exe`; for Codex, the native `codex.exe` that npm's
  package vendors must be on `PATH`. Claude Code on Windows also needs Git
  for Windows for its Bash tool.

### What is not supported on Windows

* `horde-server --remint` (restart instead).
* Owner-only enforcement outside the user profile (the operator's ACL).
* A graceful SIGTERM-like request to a reclaim command (its job is
  terminated at the timeout).
* The test-side orphan sweep (`horde-server-testkit`'s `reap`, which stays
  Unix-only and unmodified) and the multi-process test tiers built on
  signals: the end-to-end and fault suites, the worker integration suite
  apart from its Windows module, the CLI's pty suites, and `horde up`'s
  stand-up tests. They are `cfg(unix)`.

## Consequences

* **A blocking gate compiles every target for Windows.** `clippy-windows`
  runs `cargo xwin clippy --target x86_64-pc-windows-msvc --all-targets
  --all-features -- -D warnings` (docs/QUALITY_GATES.md). cargo-xwin is
  pinned at 0.23.1; it downloads the MSVC CRT and SDK headers once and uses
  the host's clang. `rust-toolchain.toml` lists the target, so rustup
  installs its standard library. The gate builds and lints; it runs
  nothing.
* **Windows tests exist but run only on Windows.** Nothing in the
  gauntlet runs them; a Windows machine does (below). A pass of
  `clippy-windows` means "builds for Windows", never "works on Windows".
* **New Windows-only dependencies:** `process-wrap` 10 (9.x was already
  locked through `rmcp`), `fs4`, `junction`. `nix` moved to `cfg(unix)` dependencies.
* **The real-machine run is the acceptance test.** Supervision, ConPTY
  login and the job semantics are pinned by Windows tests written here and
  first run on a Windows box.

## Tests

Platform-neutral, run everywhere: `horde_identity::home` (both platforms'
rules), `doctor::tests::a_windows_program_is_its_exe`,
`remint::tests::a_remint_without_sighup_says_to_restart`.

Windows-only (`cfg(windows)`), to run on a Windows machine:

| test | what it pins |
|---|---|
| `horde-worker` `process_tree::windows_tests::*` | `TerminateJobObject` and a dropped tree each kill a grandchild; `Terminate` sends nothing |
| `horde-worker` `worker_suite windows_supervision::*` | the real stub harness: stop ends it by EOF and sweeps the job; kill and a dropped handle kill the job; a write after stop is a hang-up |
| `horde-worker` `login::tests::a_conpty_session_reaches_eof_when_its_command_exits`, `killing_a_conpty_login_reaps_it_and_ends_its_output` | the ConPTY close-on-reap fix |
| `horde-worker` `workspace::reclaim::tests::a_timed_out_command_is_killed_with_its_job`, `a_windows_command_keeps_what_windows_needs` | reclaim's job and its kept environment |
| `horde-cli` `doctor::tests::windows_harnesses_are_exes_on_the_given_path`, `windows_state_dirs_report_their_acl_and_disk` | harness lookup and state-dir checks |
| `horde-server` `startup::tests::without_sighup_the_hangup_channel_is_born_closed`, `remint::tests::send_hangup_refuses_without_sighup` | the SIGHUP-less server |

Run them on Windows (MSVC toolchain, Git for Windows on `PATH`):

```
cargo test -p horde-identity -p horde-worker -p horde-cli -p horde-server --lib
cargo test -p horde-worker --test worker_suite
```

The rest of each crate's unit tests run there too; any that fails on
Windows for a Unix assumption the gate could not see is a finding.
