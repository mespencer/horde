//! What this Worker tells the Server about itself.
//!
//! The handshake ([`hello`]) and the capability advertisement ([`advertise`])
//! are built from facts, not configuration: the version comes from the crate,
//! the platform from the compiler's own constants, and the harness list from
//! the adapters actually registered — an adapter that is not registered cannot
//! be advertised, so the Server can never place work on a harness the Worker
//! would then fail to drive.

use horde_adapter::Adapter;
use horde_proto::v1::{
    HarnessCapability, Hello, Platform, Role, WorkerCapabilities, WorkspaceInstanceReport,
};
use horde_wire::capability_to_wire;

/// The version/role handshake sent at registration (DESIGN.md §2).
///
/// Capability flags are empty until this Worker implements something worth
/// gating on — an empty list is honest, a guessed one is a feature the Server
/// believes in and nobody built.
#[must_use]
pub fn hello(display_name: &str) -> Hello {
    Hello {
        semver: env!("CARGO_PKG_VERSION").to_owned(),
        capability_flags: Vec::new(),
        role: Role::Worker.into(),
        display_name: display_name.to_owned(),
    }
}

/// [`hello`], advertising `logs.v1` when this Worker serves log reads
/// (ADR-0015) — without the flag the Server refuses reads aimed here
/// instead of waiting on a stream this Worker never opens.
#[must_use]
pub fn hello_serving_logs(display_name: &str, serves_logs: bool) -> Hello {
    let mut hello = hello(display_name);
    if serves_logs {
        hello
            .capability_flags
            .push(horde_wire::logs::LOGS_CAPABILITY.to_owned());
    }
    hello
}

/// Operator-declared placement facts (fix a15): the config source for the
/// advertisement's tags and reachable remotes. The scheduler has matched on
/// both all along; before this, nothing could ever pass those filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredFacts {
    /// Freeform placement tags, e.g. "gpu" — `--tag`, repeatable.
    pub tags: Vec<String>,
    /// Remote URLs this machine's ambient credentials can reach —
    /// `--advertise-remote`, repeatable. Declared, not probed: reachability
    /// is a property of the operator's credential setup, which only the
    /// operator knows.
    pub reachable_remotes: Vec<String>,
}

/// The full advertisement placement matches on, from the registered adapters,
/// the Instances actually on disk, and the operator's declared facts.
///
/// Provider accounts are not the adapters' fact but the login plane's —
/// [`crate::login::LoginPlane::held_accounts`], read from the account
/// directories logins sign in — so the daemon fills them from there; left
/// empty here, nothing is claimed that nothing maintains. Warm instances and
/// workspace support come from
/// the workspace half: `None` honestly advertises a Worker that would refuse
/// a materialize command.
#[must_use]
pub fn advertise<'a>(
    adapters: impl IntoIterator<Item = &'a dyn Adapter>,
    warm_instances: Option<Vec<WorkspaceInstanceReport>>,
    declared: &DeclaredFacts,
) -> WorkerCapabilities {
    WorkerCapabilities {
        tags: declared.tags.clone(),
        platform: Some(platform()),
        provider_accounts: Vec::new(),
        reachable_remotes: declared.reachable_remotes.clone(),
        supports_workspaces: warm_instances.is_some(),
        warm_instances: warm_instances.unwrap_or_default(),
        harnesses: adapters.into_iter().map(harness).collect(),
        max_concurrent_agents: 0,
    }
}

/// One registered adapter, as the wire advertises it.
fn harness(adapter: &dyn Adapter) -> HarnessCapability {
    HarnessCapability {
        provider: adapter.provider().to_owned(),
        // Registered means runnable: the Runtime only holds adapters it can
        // drive. "Installed but unregistered" is not a state this Worker has.
        installed: true,
        version: String::new(),
        installable: false,
        capabilities: adapter
            .capabilities()
            .iter()
            .filter_map(capability_to_wire)
            .map(i32::from)
            .collect(),
    }
}

fn platform() -> Platform {
    Platform {
        os: goos(std::env::consts::OS).to_owned(),
        arch: goarch(std::env::consts::ARCH).to_owned(),
        // The one fact std does not expose. Empty is honest; wave-5 enrollment
        // names workers properly, which is the name that actually matters.
        hostname: std::env::var("HOSTNAME").unwrap_or_default(),
    }
}

/// Rust's `target_os` vocabulary → the GOOS-style names the proto documents.
fn goos(os: &str) -> &str {
    match os {
        "macos" => "darwin",
        other => other,
    }
}

/// Rust's `target_arch` vocabulary → the GOARCH-style names the proto
/// documents.
fn goarch(arch: &str) -> &str {
    match arch {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use horde_adapter::Adapter;
    use horde_proto::v1::{AdapterCapability, Role};

    use super::{DeclaredFacts, advertise, goarch, goos, hello};
    use crate::claude::ClaudeAdapter;

    #[test]
    fn the_hello_names_this_workers_role_and_version() {
        let hello = hello("test-worker");
        assert_eq!(hello.role, i32::from(Role::Worker));
        assert_eq!(hello.display_name, "test-worker");
        assert!(
            !hello.semver.is_empty(),
            "the server enforces a minimum version; an empty one is unversioned"
        );
    }

    /// The Claude adapter's presence is the advertisement that matters first:
    /// it is what lets the scheduler place gated, high-trust work here.
    #[test]
    fn a_registered_claude_adapter_is_advertised_with_its_gating() {
        let claude = ClaudeAdapter::new(crate::harness::HarnessProfile::new(
            "/nonexistent/horde-test-profile",
        ));
        let capabilities = advertise([&claude as &dyn Adapter], None, &DeclaredFacts::default());

        let harnesses = &capabilities.harnesses;
        assert_eq!(harnesses.len(), 1);
        assert_eq!(harnesses[0].provider, "claude");
        assert!(harnesses[0].installed, "registered means runnable");
        assert!(
            harnesses[0]
                .capabilities
                .contains(&i32::from(AdapterCapability::ToolCallGating)),
            "without the gating advertisement, high-trust work cannot land here"
        );
        assert!(
            harnesses[0]
                .capabilities
                .contains(&i32::from(AdapterCapability::Resumable))
        );
    }

    /// The codex advertisement must NOT carry blanket tool-call gating: even
    /// a probed 0.147+ binary leaves file-change approvals unroutable (they
    /// park — transcripts 28/31), and TOOL_CALL_GATING is what placement
    /// reads as "high-trust work can land here". The per-kind MCP and exec
    /// gating stay off the wire until placement can match per call kind.
    #[test]
    fn a_registered_codex_adapter_advertises_without_blanket_gating() {
        let mut codex = crate::codex::CodexAdapter::new(crate::harness::HarnessProfile::new(
            "/nonexistent/horde-test-profile",
        ));
        for interception in [
            crate::codex::ExecInterception::SandboxOnly {
                reason: "unprobed".to_owned(),
            },
            crate::codex::ExecInterception::Routed {
                version: "codex-cli 0.148.0".to_owned(),
            },
        ] {
            codex.exec_interception = interception;
            let capabilities = advertise([&codex as &dyn Adapter], None, &DeclaredFacts::default());
            assert!(
                !capabilities.harnesses[0]
                    .capabilities
                    .contains(&i32::from(AdapterCapability::ToolCallGating)),
                "no probe verdict may advertise blanket gating: file changes stay ungated"
            );
        }

        let codex = crate::codex::CodexAdapter::new(crate::harness::HarnessProfile::new(
            "/nonexistent/horde-test-profile",
        ));
        let capabilities = advertise([&codex as &dyn Adapter], None, &DeclaredFacts::default());

        let harnesses = &capabilities.harnesses;
        assert_eq!(harnesses.len(), 1);
        assert_eq!(harnesses[0].provider, "codex");
        assert!(
            !harnesses[0]
                .capabilities
                .contains(&i32::from(AdapterCapability::ToolCallGating)),
            "advertising gating would let placement put high-trust exec here"
        );
        assert!(
            harnesses[0]
                .capabilities
                .contains(&i32::from(AdapterCapability::Steerable)),
            "turn/steer is real (transcript 15) and placement may rely on it"
        );
        assert!(
            harnesses[0]
                .capabilities
                .contains(&i32::from(AdapterCapability::Resumable))
        );
    }

    #[test]
    fn the_platform_speaks_the_protos_vocabulary_not_rusts() {
        // The proto documents GOOS/GOARCH-style names; Rust's constants
        // disagree for exactly these values, and a scheduler matching
        // tags like "darwin" would silently never match "macos".
        assert_eq!(goos("macos"), "darwin");
        assert_eq!(goos("linux"), "linux");
        assert_eq!(goarch("aarch64"), "arm64");
        assert_eq!(goarch("x86_64"), "amd64");
        assert_eq!(goarch("riscv64"), "riscv64");
    }

    #[test]
    fn nothing_unmaintained_is_advertised() {
        let capabilities = advertise([], None, &DeclaredFacts::default());
        assert!(capabilities.provider_accounts.is_empty());
        assert!(capabilities.warm_instances.is_empty());
        assert!(capabilities.reachable_remotes.is_empty());
        assert!(
            capabilities.tags.is_empty(),
            "nothing declared, nothing advertised"
        );
        assert!(
            capabilities.platform.is_some(),
            "the platform is auto-detected, so it is always real"
        );
    }

    /// Declared facts ride the advertisement verbatim (fix a15): the tag and
    /// remote filters the scheduler has matched on all along finally have a
    /// config source that can pass them.
    #[test]
    fn declared_tags_and_remotes_are_advertised_verbatim() {
        let declared = DeclaredFacts {
            tags: vec!["gpu".to_owned(), "fast-disk".to_owned()],
            reachable_remotes: vec!["https://git.example/atlas.git".to_owned()],
        };
        let capabilities = advertise([], None, &declared);
        assert_eq!(capabilities.tags, declared.tags);
        assert_eq!(capabilities.reachable_remotes, declared.reachable_remotes);
    }

    /// The advertised platform is this machine's, through the vocabulary
    /// mapping — a defaulted (empty) platform would have the scheduler
    /// matching placement constraints against a machine that runs nothing.
    #[test]
    fn the_platform_reports_this_machines_facts() {
        let platform = advertise([], None, &DeclaredFacts::default())
            .platform
            .expect("a platform");
        assert_eq!(platform.os, goos(std::env::consts::OS));
        assert_eq!(platform.arch, goarch(std::env::consts::ARCH));
        assert!(
            !platform.os.is_empty() && !platform.arch.is_empty(),
            "an empty fact is not auto-detection"
        );
    }
}
