#!/bin/bash
set -euo pipefail

# Build script for the search engine
# Builds Rust workspace (release) and Go binary

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

export SDKROOT="${SDKROOT:-/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk}"
export DEVELOPER_DIR="${DEVELOPER_DIR:-/Library/Developer/CommandLineTools}"

echo "=== Building Rust workspace (release) ==="
cd "$ROOT_DIR/rust"
cargo build --release --workspace

echo ""
echo "=== Building Go server ==="
cd "$ROOT_DIR/go"
CGO_ENABLED=1 \
  CGO_LDFLAGS="-L$ROOT_DIR/rust/target/release -lsearch_ffi" \
  go build -buildvcs=false -o "$ROOT_DIR/bin/search-server" .

echo ""
echo "=== Build complete ==="
echo "Binaries:"
echo "  Indexer:     $ROOT_DIR/rust/target/release/indexer"
echo "  Search CLI:  $ROOT_DIR/rust/target/release/search-cli"
echo "  FFI Library: $ROOT_DIR/rust/target/release/libsearch_ffi.dylib"
echo "  Web Server:  $ROOT_DIR/bin/search-server"
