// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "WaveSyncPush",
    platforms: [
        .iOS(.v14)
    ],
    products: [
        .library(
            name: "WaveSyncPush",
            type: .static,
            targets: ["WaveSyncPush"]
        )
    ],
    targets: [
        .target(
            name: "WaveSyncPush",
            path: "Sources/WaveSyncPush"
        )
    ]
)
