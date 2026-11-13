#!/usr/bin/env bash
# Installs cargo-gamma 0.2.1 with the census-kill-final patch applied, or, with
# --check, confirms that the cargo-gamma on this machine is that build.
#
# Taken from atlas (.github/workflow-resources/cargo-gamma/), where the patch
# was written; the patch file is byte-identical, so one patched binary serves
# both repositories and the stamp beside it keeps atlas's name.
#
# Stock 0.2.1 re-runs a test binary whole whenever a census-filtered run of it
# does not pass (cargo-gamma-lib `src/exec/sweep.rs`, the `CensusSelection::
# Selected` branch of the per-mutant loop), so every kill pays for its binary
# twice. The patch treats a filtered *failure* as final; kill confirmation,
# which re-runs the failing test with no mutant active, is unchanged. See
# docs/QUALITY_GATES.md (mutants) for why.
#
# Both crates come from crates.io and are checked against the checksums the
# crates.io index publishes for them. The install records the patch's and the
# installed binary's digests next to the binary; a later run reuses that
# binary only when both still match, so a stock cargo-gamma — from a cache or
# a plain `cargo install` — is always replaced.
#
# Usage:
#   tools/cargo-gamma/install.sh          install (or confirm and reuse)
#   tools/cargo-gamma/install.sh --check  exit 0 only if the cargo-gamma that
#                                         `cargo gamma` runs is the patched
#                                         build; never installs anything
#
# Environment:
#   GAMMA_INSTALL_ROOT  install root passed to `cargo install --root`
#                       (default: cargo's own, normally ~/.cargo)
set -euo pipefail

version=0.2.1
gamma_sha256=ba1066fbf464622731740551c1d1899a55b909568e9c962dd8693e639b29b338
lib_sha256=9e0f3bc0fe8ea62fda3020a8aa1104bc21d0e0654165fceb6520294c6312a70c

here=$(cd "$(dirname "$0")" && pwd)
patch_file="$here/${version}-census-kill-final.patch"

sha256() {
    if command -v sha256sum > /dev/null; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

root="${GAMMA_INSTALL_ROOT:-${CARGO_INSTALL_ROOT:-${CARGO_HOME:-$HOME/.cargo}}}"
patch_sha256=$(sha256 "$patch_file")

# The stamp written beside a binary, and whether it vouches for that binary.
stamped() {
    local binary="$1"
    local stamp
    stamp="$(dirname "$binary")/cargo-gamma.atlas-patch"
    [ -x "$binary" ] && [ -f "$stamp" ] \
        && [ "$(cat "$stamp")" = "patch=$patch_sha256 binary=$(sha256 "$binary")" ]
}

if [ "${1:-}" = "--check" ]; then
    # Cargo runs an external subcommand from its own bin directory first and
    # then from PATH, so that is the order the binary is looked for in.
    binary="${CARGO_HOME:-$HOME/.cargo}/bin/cargo-gamma"
    if [ ! -x "$binary" ]; then
        binary=$(command -v cargo-gamma || true)
    fi
    if [ -z "$binary" ]; then
        echo "cargo-gamma is not installed" >&2
        exit 1
    fi
    if stamped "$binary"; then
        echo "patched cargo-gamma $version at $binary"
        exit 0
    fi
    echo "$binary is not the patched cargo-gamma $version (no stamp, or a stamp for another patch or binary beside it)" >&2
    exit 1
fi

binary="$root/bin/cargo-gamma"
stamp="$root/bin/cargo-gamma.atlas-patch"

if stamped "$binary"; then
    echo "patched cargo-gamma $version already installed at $binary"
    exit 0
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

fetch() {
    local name="$1" sum="$2"
    local crate="$work/$name-$version.crate"
    curl -sSfL --retry 3 -o "$crate" "https://static.crates.io/crates/$name/$name-$version.crate"
    local got
    got=$(sha256 "$crate")
    if [ "$got" != "$sum" ]; then
        echo "error: $name $version checksum $got, expected $sum" >&2
        exit 1
    fi
    tar -xzf "$crate" -C "$work"
}

fetch cargo-gamma "$gamma_sha256"
fetch cargo-gamma-lib "$lib_sha256"
patch -p1 --batch --forward -d "$work" -i "$patch_file"

# --force replaces a stock binary of the same version, which cargo would
# otherwise call already installed.
cargo install --locked --force --root "$root" --path "$work/cargo-gamma-$version"
echo "patch=$patch_sha256 binary=$(sha256 "$binary")" > "$stamp"
"$binary" gamma --version
