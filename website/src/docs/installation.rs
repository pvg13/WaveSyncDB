use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Installation" }

        p {
            "WaveSyncDB supports multiple platforms and frameworks. "
            "Choose the section that matches your setup."
        }

        // ── Rust / Dioxus ──

        H2 { id: "rust-dioxus", text: "Rust / Dioxus" }

        p { "Add WaveSyncDB to your Cargo.toml with the features you need:" }

        CodeBlock { html: CODE_CARGO }

        H3 { id: "feature-flags", text: "Feature Flags" }

        ul {
            li {
                code { "dioxus" }
                " — reactive hooks ("
                code { "use_synced_table" }
                ", "
                code { "use_network_status" }
                ") and automatic mobile lifecycle detection (backgrounding, resume, cold sync)."
            }
            li {
                code { "derive" }
                " — enables "
                code { "#[derive(SyncEntity)]" }
                " for automatic entity registration. Without this, you register tables manually."
            }
            li {
                code { "android-fcm" }
                " — bundles the Kotlin service for Android Firebase Cloud Messaging push wakeup. "
                "Requires a valid "
                code { "google-services.json" }
                " in your Android project."
            }
            li {
                code { "ios-push" }
                " — bundles the Swift package for iOS APNs push wakeup. "
                "Requires push notification entitlements in your Xcode project."
            }
        }

        H3 { id: "rust-requirements", text: "Requirements" }

        ul {
            li { "Minimum Rust edition: " strong { "2021" } }
            li { "MSRV: " strong { "latest stable" } " (tested on each release)" }
            li {
                "SeaORM is required for the connection wrapper. "
                "WaveSyncDB intercepts writes at the "
                code { "ConnectionTrait" }
                " level, so your SeaORM entities and queries work unchanged."
            }
        }

        CodeBlock { html: CODE_SEAORM_DEP }

        // ── React Native ──

        H2 { id: "react-native", text: "React Native" }

        p { "Install the core package from npm:" }

        CodeBlock { html: CODE_NPM }

        H3 { id: "rn-android", text: "Android" }

        p {
            "Android works out of the box. The package ships prebuilt "
            code { ".so" }
            " libraries and uses React Native autolinking \
             — no manual configuration needed."
        }

        H3 { id: "rn-ios", text: "iOS" }

        p {
            "iOS requires building the native Rust library before linking. "
            "Run the build script from the repository root, then install pods:"
        }

        CodeBlock { html: CODE_IOS_BUILD }

        p {
            "The build script compiles for both "
            code { "aarch64-apple-ios" }
            " and "
            code { "aarch64-apple-ios-sim" }
            " targets and produces an XCFramework "
            "that CocoaPods picks up automatically."
        }

        H3 { id: "rn-optional", text: "Optional Packages" }

        ul {
            li {
                code { "@wavesync/watermelondb" }
                " — adapter for WatermelonDB ORM. "
                "Provides reactive collections and lazy loading on top of WaveSyncDB."
            }
            li {
                code { "@react-native-firebase/app" }
                " + "
                code { "@react-native-firebase/messaging" }
                " — required for push-based background sync. "
                "Without these, sync only happens while the app is in the foreground or on manual triggers."
            }
        }

        CodeBlock { html: CODE_RN_OPTIONAL }

        // ── Relay Server ──

        H2 { id: "relay-server", text: "Relay Server" }

        p {
            "The relay server enables sync across NAT boundaries and the public internet. "
            "On local networks, WaveSyncDB uses mDNS for peer discovery and syncs directly \
             — no relay needed. "
            "For WAN connectivity or mobile-to-mobile sync across networks, deploy a relay."
        }

        H3 { id: "relay-install", text: "Install" }

        p { "Install via Cargo:" }

        CodeBlock { html: CODE_RELAY_CARGO }

        p { "Or run with Docker:" }

        CodeBlock { html: CODE_RELAY_DOCKER }

        H3 { id: "relay-flags", text: "Configuration" }

        ul {
            li {
                code { "-p 4001:4001" }
                " — the libp2p listener port. Peers connect to this address for relay circuit reservations."
            }
            li {
                code { "RUST_LOG=info" }
                " — controls log verbosity. The relay emits its events through "
                code { "tracing" }
                " (configured with "
                code { "tracing-subscriber" }
                "'s "
                code { "EnvFilter" }
                "), and "
                code { "EnvFilter" }
                " parses the same "
                code { "RUST_LOG" }
                " directive syntax "
                code { "env_logger" }
                " does, so this setting works exactly as before. Use "
                code { "debug" }
                " or "
                code { "trace" }
                " for troubleshooting connection issues."
            }
        }

        p { class: "note",
            "Already tailing relay output with an "
            code { "env_logger" }
            "-based setup? No changes needed — every "
            code { "tracing" }
            " event is also emitted as a "
            code { "log" }
            " record, so existing log-based tooling keeps receiving relay events unmodified."
        }

        p {
            "Point your clients at the relay by passing the multiaddr to the builder:"
        }

        CodeBlock { html: CODE_RELAY_CLIENT }

        p { class: "note",
            "The relay is optional for LAN sync. "
            "mDNS peer discovery works without any server. "
            "The relay is required only for WAN/NAT traversal \
             and for coordinating push notification delivery to sleeping mobile peers."
        }
    }
}

const CODE_CARGO: &str = r##"<span class="cmt"># Cargo.toml</span>
[dependencies]
wavesyncdb = { version = <span class="str">"0.4"</span>, features = [<span class="str">"dioxus"</span>, <span class="str">"derive"</span>] }

<span class="cmt"># For Android push wakeup:</span>
<span class="cmt"># wavesyncdb = { version = "0.4", features = ["dioxus", "derive", "android-fcm"] }</span>

<span class="cmt"># For iOS push wakeup:</span>
<span class="cmt"># wavesyncdb = { version = "0.4", features = ["dioxus", "derive", "ios-push"] }</span>"##;

const CODE_SEAORM_DEP: &str = r##"<span class="cmt"># SeaORM is required</span>
[dependencies]
sea-orm = { version = <span class="str">"1"</span>, features = [<span class="str">"sqlx-sqlite"</span>, <span class="str">"runtime-tokio-rustls"</span>] }"##;

const CODE_NPM: &str = r##"npm install @wavesync/react-native"##;

const CODE_IOS_BUILD: &str = r##"<span class="cmt"># Build the native iOS library</span>
./scripts/build-ios.sh release

<span class="cmt"># Install CocoaPods dependencies</span>
<span class="kw">cd</span> ios <span class="kw">&amp;&amp;</span> pod install"##;

const CODE_RN_OPTIONAL: &str = r##"<span class="cmt"># WatermelonDB adapter (optional)</span>
npm install @wavesync/watermelondb

<span class="cmt"># Firebase push notifications (optional)</span>
npm install @react-native-firebase/app @react-native-firebase/messaging"##;

const CODE_RELAY_CARGO: &str = r##"cargo install wavesync-relay"##;

const CODE_RELAY_DOCKER: &str = r##"docker run -d \
  --name wavesync-relay \
  -p <span class="num">4001</span>:<span class="num">4001</span> \
  -e RUST_LOG=info \
  ghcr.io/pvg13/wavesync-relay:latest"##;

const CODE_RELAY_CLIENT: &str = r##"<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
    .<span class="fn">with_relay</span>(<span class="str">"/ip4/YOUR_SERVER_IP/tcp/4001/p2p/RELAY_PEER_ID"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;"##;
