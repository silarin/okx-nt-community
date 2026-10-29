#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:---write}"
if [[ "$#" -gt 1 || ("$MODE" != "--write" && "$MODE" != "--check") ]]; then
  echo "Usage: $0 [--write|--check]" >&2
  exit 2
fi

EXPECTED_CAPNP="$(bash "$ROOT/scripts/tool-version.sh" capnp)"
if [[ "$(capnp --version)" != "Cap'n Proto version $EXPECTED_CAPNP" ]]; then
  echo "Expected Cap'n Proto $EXPECTED_CAPNP from the repository tool catalog" >&2
  exit 1
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
mkdir -p "$WORK_DIR/output"

# Keep code generation on the same API family as the patched transport crates
cargo install capnpc --version 0.24.1 --locked --root "$WORK_DIR/tools"
SCHEMA_DIR="$ROOT/patches/hypersync-net-types"
capnp compile --src-prefix="$SCHEMA_DIR" \
  -o "$WORK_DIR/tools/bin/capnpc-rust:$WORK_DIR/output" \
  "$SCHEMA_DIR/hypersync_net_types.capnp"
GENERATED="$WORK_DIR/output/hypersync_net_types_capnp.rs"
rustfmt +nightly --config-path "$ROOT/rustfmt.toml" "$GENERATED"
TARGET="$SCHEMA_DIR/src/__generated__/hypersync_net_types_capnp.rs"

if [[ "$MODE" == "--check" ]]; then
  diff -u "$TARGET" "$GENERATED"
else
  cp "$GENERATED" "$TARGET"
fi
