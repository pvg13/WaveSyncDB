#!/usr/bin/env bash
#
# Build WaveSyncDB FFI for iOS and generate Swift bindings.
#
# Requirements:
#   - macOS with Xcode installed
#   - Rust toolchain with iOS targets:
#     rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
#
# Usage:
#   ./scripts/build-ios.sh          # Release build (default)
#   ./scripts/build-ios.sh debug    # Debug build (faster, larger)
#
# Output:
#   packages/wavesync-rn/ios/Frameworks/wavesyncdb_ffi.xcframework
#   packages/wavesync-rn/ios/Generated/wavesyncdb_ffi.swift
#   packages/wavesync-rn/ios/Generated/wavesyncdb_ffiFFI.h
#   packages/wavesync-rn/ios/Generated/wavesyncdb_ffiFFI.modulemap

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
FFI_CRATE="$ROOT_DIR/wavesyncdb_ffi"
OUT_DIR="$ROOT_DIR/packages/wavesync-rn/ios"

PROFILE="${1:-release}"
if [ "$PROFILE" = "release" ]; then
    CARGO_FLAGS="--release"
    TARGET_DIR="release"
else
    CARGO_FLAGS=""
    TARGET_DIR="debug"
fi

echo "=== Building wavesyncdb_ffi for iOS ($PROFILE) ==="

# Ensure targets are installed
for target in aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios; do
    if ! rustup target list --installed | grep -q "$target"; then
        echo "Installing Rust target: $target"
        rustup target add "$target"
    fi
done

# Build for all three targets
echo "--- Building aarch64-apple-ios (device) ---"
cargo build -p wavesyncdb_ffi --target aarch64-apple-ios $CARGO_FLAGS

echo "--- Building aarch64-apple-ios-sim (Apple Silicon simulator) ---"
cargo build -p wavesyncdb_ffi --target aarch64-apple-ios-sim $CARGO_FLAGS

echo "--- Building x86_64-apple-ios (Intel simulator) ---"
cargo build -p wavesyncdb_ffi --target x86_64-apple-ios $CARGO_FLAGS

# Create fat library for simulator (combine arm64 + x86_64)
echo "--- Creating fat simulator library ---"
SIMULATOR_DIR="$ROOT_DIR/target/ios-sim-fat/$TARGET_DIR"
mkdir -p "$SIMULATOR_DIR"
lipo -create \
    "$ROOT_DIR/target/aarch64-apple-ios-sim/$TARGET_DIR/libwavesyncdb_ffi.a" \
    "$ROOT_DIR/target/x86_64-apple-ios/$TARGET_DIR/libwavesyncdb_ffi.a" \
    -output "$SIMULATOR_DIR/libwavesyncdb_ffi.a"

# Create xcframework
echo "--- Creating xcframework ---"
FRAMEWORK_DIR="$OUT_DIR/Frameworks"
rm -rf "$FRAMEWORK_DIR/wavesyncdb_ffi.xcframework"
mkdir -p "$FRAMEWORK_DIR"

xcodebuild -create-xcframework \
    -library "$ROOT_DIR/target/aarch64-apple-ios/$TARGET_DIR/libwavesyncdb_ffi.a" \
    -headers "$OUT_DIR/Generated" \
    -library "$SIMULATOR_DIR/libwavesyncdb_ffi.a" \
    -headers "$OUT_DIR/Generated" \
    -output "$FRAMEWORK_DIR/wavesyncdb_ffi.xcframework"

echo "--- xcframework created at $FRAMEWORK_DIR/wavesyncdb_ffi.xcframework ---"

# Generate Swift bindings (use the device build for metadata extraction)
echo "--- Generating Swift bindings ---"
mkdir -p "$OUT_DIR/Generated"
cargo run --manifest-path "$FFI_CRATE/Cargo.toml" --bin uniffi-bindgen -- \
    generate --library --crate wavesyncdb_ffi \
    -l swift \
    -o "$OUT_DIR/Generated" \
    --no-format \
    "$ROOT_DIR/target/aarch64-apple-ios/$TARGET_DIR/libwavesyncdb_ffi.a"

echo ""
echo "=== iOS build complete ==="
echo "  xcframework: $FRAMEWORK_DIR/wavesyncdb_ffi.xcframework"
echo "  Swift bindings: $OUT_DIR/Generated/wavesyncdb_ffi.swift"
echo ""
echo "Next steps:"
echo "  cd examples/rn-sync-demo/ios && pod install"
echo "  open WaveSyncDemo.xcworkspace"
