// swift-tools-version: 5.9

import PackageDescription

// WaveSyncPush — iOS APNs integration for WaveSyncDB.
//
// The package produces a single static library (`WaveSyncPush`) with two
// compilation targets:
//
//   • WaveSyncPushObjC  — contains an ObjC class with a `+load` entry point
//     that runs at image load, before `UIApplicationMain`. It installs the
//     APNs AppDelegate selectors via `class_addMethod` once the app delegate
//     exists (observed via `UIApplicationDidFinishLaunchingNotification`).
//
//   • WaveSyncPush      — Swift helpers the ObjC proxy calls into:
//     token storage, payload handling, database discovery.
//
// The Swift target depends on the ObjC target so consumers link both through
// a single product. ObjC is needed specifically for `+load`, which has no
// Swift equivalent.

let package = Package(
    name: "WaveSyncPush",
    platforms: [
        // iOS 16 is the effective floor for Swift's auto-linked stdlib bits
        // (notably SwiftUICore) on current Xcode/iOS SDKs. Older deployment
        // targets trigger spurious `-Wincompatible-sysroot` warnings and
        // "not an allowed client of SwiftUICore" link errors when the Swift
        // toolchain auto-links against host frameworks.
        .iOS(.v16)
    ],
    products: [
        // `dx build` consumes this product as a dynamic framework embedded
        // in `.app/Frameworks/`; the Rust-side `@_silgen_name` symbols are
        // resolved by dyld against the main executable at runtime.
        .library(
            name: "WaveSyncPush",
            type: .dynamic,
            targets: ["WaveSyncPush", "WaveSyncPushObjC"]
        )
    ],
    targets: [
        .target(
            name: "WaveSyncPushObjC",
            path: "Sources/WaveSyncPushObjC",
            publicHeadersPath: "include"
        ),
        .target(
            name: "WaveSyncPush",
            dependencies: ["WaveSyncPushObjC"],
            path: "Sources/WaveSyncPush",
            // Rust's C-ABI exports (e.g. `wavesync_background_sync_with_peers`)
            // live in the main executable, not in any library this target
            // links against. Tell ld to accept them as undefined and let
            // dyld resolve them against the main image at load time. The
            // Darwin linker retains support for `dynamic_lookup` on dynamic
            // libraries; on iOS it is restricted from executable targets.
            linkerSettings: [
                .unsafeFlags([
                    "-Xlinker", "-undefined",
                    "-Xlinker", "dynamic_lookup",
                ])
            ]
        )
    ]
)
