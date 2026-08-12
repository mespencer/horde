//! What this Worker tells the Server about itself.
//!
//! The handshake ([`hello`]) and the capability advertisement ([`advertise`])
//! are built from facts, not configuration: the version comes from the crate,
//! the platform from the compiler's own constants, and the harness list from
//! the adapters actually registered — an adapter that is not registered cannot
//! be advertised, so the Server can never place work on a harness the Worker
//! would then fail to drive.

use horde_adapter::Adapter;
use horde_proto::v1::{HarnessCapability, Hello, Platform, Role, WorkerCapabilities};
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

/// The full advertisement placement matches on, from the registered adapters.
///
/// Tags, provider accounts, reachable remotes and warm instances stay empty
/// until the subsystems that own those facts exist (accounts are wave-5
/// enrollment, instances wave-4 workspaces); advertising them earlier would
/// have the scheduler matching on facts nothing maintains.
#[must_use]
pub fn advertise<'a>(adapters: impl IntoIterator<Item = &'a dyn Adapter>) -> WorkerCapabilities {
    WorkerCapabilities {
        tags: Vec::new(),
        platform: Some(platform()),
        provider_accounts: Vec::new(),
        reachable_remotes: Vec::new(),
        warm_instances: Vec::new(),
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

    use super::{advertise, goarch, goos, hello};
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
        let claude = ClaudeAdapter::new();
        let capabilities = advertise([&claude as &dyn Adapter]);

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
        let capabilities = advertise([]);
        assert!(capabilities.provider_accounts.is_empty());
        assert!(capabilities.warm_instances.is_empty());
        assert!(capabilities.reachable_remotes.is_empty());
        assert!(
            capabilities.platform.is_some(),
            "the platform is auto-detected, so it is always real"
        );
    }
}
