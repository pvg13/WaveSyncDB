#!/usr/bin/env bash
# Build the two Docker images the E2E harness depends on.
#
# Run from the repo root (or anywhere — the script cds itself).
# Re-running with cached layers takes ~10 s on a warm cache.

set -euo pipefail

# Find repo root regardless of cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Building wavesync-relay:e2e"
docker build \
    -t wavesync-relay:e2e \
    -f wavesync_relay/Dockerfile \
    .

echo "==> Building wavesync-test-peer:e2e"
docker build \
    -t wavesync-test-peer:e2e \
    -f tests-e2e/Dockerfile.test-peer \
    .

echo "==> Done."
echo "    Run: cargo test -p wavesyncdb-e2e --tests"
