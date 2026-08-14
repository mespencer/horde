//! Generates the `horde.v1` Rust types and gRPC stubs from `proto/`.
//!
//! Generation runs through `tonic-prost-build` rather than `buf generate` so
//! the `prost`/`tonic` runtime versions and the generator are pinned by one
//! `Cargo.lock` instead of by a separately released Buf plugin. `buf` still
//! owns lint and breaking-change policy over the same files (`buf.yaml`).
//!
//! `protoc` comes from `protoc-bin-vendored` so the compiler version is pinned
//! too; `PROTOC` in the environment overrides it for anyone who needs a
//! specific build.

use std::error::Error;
use std::path::{Path, PathBuf};

/// Proto files compiled into the crate, relative to the proto root.
const PROTO_FILES: &[&str] = &[
    "horde/v1/agent.proto",
    "horde/v1/blob.proto",
    "horde/v1/control_plane.proto",
    "horde/v1/enrollment.proto",
    "horde/v1/event.proto",
    "horde/v1/handshake.proto",
    "horde/v1/id.proto",
    "horde/v1/payload.proto",
    "horde/v1/permission.proto",
    "horde/v1/usage.proto",
    "horde/v1/worker.proto",
    "horde/v1/worker_service.proto",
    "horde/v1/workspace.proto",
];

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);

    let proto_root = manifest_dir
        .join("../../proto")
        .canonicalize()
        .map_err(|e| format!("proto root not found next to crates/horde-proto: {e}"))?;

    let files: Vec<PathBuf> = PROTO_FILES.iter().map(|f| proto_root.join(f)).collect();
    emit_rerun_directives(&proto_root, &files);

    let mut config = prost_build::Config::new();
    if std::env::var_os("PROTOC").is_none() {
        config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Exposed as `horde_proto::FILE_DESCRIPTOR_SET`: the contract tests
        // assert against it, and gRPC reflection will need it later.
        .file_descriptor_set_path(out_dir.join("horde_v1_descriptor.bin"))
        .compile_with_config(config, &files, &[proto_root])?;

    Ok(())
}

fn emit_rerun_directives(proto_root: &Path, files: &[PathBuf]) {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=PROTOC");
    println!("cargo:rerun-if-changed={}", proto_root.display());
    for file in files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}
