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
        .iOS(.v14)
    ],
    products: [
        .library(
            name: "WaveSyncPush",
            type: .static,
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
            path: "Sources/WaveSyncPush"
        )
    ]
)
