use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Push Notifications" }

        p {
            "Mobile devices suspend background processes aggressively. When your app is "
            "backgrounded or killed, the P2P sync engine stops. Push notifications solve "
            "this by waking the device when remote changes are available."
        }

        H2 { id: "architecture", text: "Architecture" }

        p {
            "Push notifications in WaveSyncDB are wake signals only --- they carry no "
            "data payload. The flow is:"
        }

        ul {
            li { "A peer writes data locally" }
            li { "The engine calls notify_relay_topic() to signal the relay server" }
            li { "The relay server sends a silent FCM or APNs push to registered devices" }
            li { "The device wakes and runs background_sync() or resumes the app" }
            li { "Normal catch-up sync pulls the actual data from peers" }
        }

        p {
            "This design means the relay server never sees your data. It only knows that "
            "\"something changed\" for a given topic."
        }

        H2 { id: "android-fcm", text: "Android: FCM Setup" }

        H3 { id: "android-config", text: "Configuration" }

        p {
            "Place your google-services.json in the project. For Dioxus apps, pass it "
            "to the builder. For React Native, follow the standard Firebase setup."
        }

        CodeBlock { html: CODE_ANDROID_CONFIG }

        H3 { id: "android-service", text: "WaveSyncService" }

        p {
            "WaveSyncDB includes an auto-registered Android service (WaveSyncService) that "
            "handles FCM messages. When a silent push arrives while the app is killed, "
            "the service:"
        }

        ul {
            li { "Reads .wavesync_config.json from the database directory" }
            li { "Rebuilds the WaveSyncDb with the saved configuration" }
            li { "Calls background_sync() to pull changes from peers" }
            li { "Shuts down cleanly after sync completes or times out" }
        }

        H3 { id: "android-rn", text: "React Native Android" }

        p {
            "In your React Native app, initialize FCM and register the background handler:"
        }

        CodeBlock { html: CODE_RN_ANDROID }

        H2 { id: "ios-apns", text: "iOS: APNs Setup" }

        H3 { id: "ios-xcode", text: "Xcode Configuration" }

        p {
            "Enable push notifications in your Xcode project:"
        }

        ul {
            li { "Add the Push Notifications capability in Signing and Capabilities" }
            li { "Ensure your provisioning profile includes push notification entitlements" }
            li { "For Dioxus: add the Background Modes capability with \"Remote notifications\" checked" }
        }

        H3 { id: "ios-dioxus", text: "Dioxus iOS" }

        p {
            "Use the use_auto_push() hook which handles APNs token registration and "
            "forwarding to the engine automatically:"
        }

        CodeBlock { html: CODE_IOS_DIOXUS }

        H3 { id: "ios-rn", text: "React Native iOS" }

        p {
            "React Native uses initWaveSyncFCM() which transparently handles APNs on iOS. "
            "Firebase Cloud Messaging bridges to APNs automatically."
        }

        CodeBlock { html: CODE_IOS_RN }

        H2 { id: "relay-setup", text: "Relay Server Push Setup" }

        p {
            "The relay server must be configured with push credentials to send notifications. "
            "At least one of FCM or APNs must be configured for push to work."
        }

        CodeBlock { html: CODE_RELAY_PUSH }

        H2 { id: "cold-sync", text: "Cold Sync Flow" }

        p {
            "Cold sync handles the case where the app has been fully killed by the OS. "
            "The flow differs by platform:"
        }

        H3 { id: "cold-android", text: "Android Cold Sync" }

        ul {
            li { "FCM delivers a silent push to WaveSyncService" }
            li { "The service reads .wavesync_config.json (saved by the last WaveSyncDbBuilder::build())" }
            li { "Config contains: database URL, topic, passphrase hash, relay address, registered tables" }
            li { "background_sync() is called with the saved config" }
            li { "The engine starts, discovers peers, pulls changes, then shuts down" }
            li { "Timeout defaults to 30 seconds to avoid ANR" }
        }

        H3 { id: "cold-ios", text: "iOS Cold Sync" }

        ul {
            li { "APNs delivers a silent notification (content-available: 1)" }
            li { "iOS grants approximately 30 seconds of background execution time" }
            li { "The app delegate's didReceiveRemoteNotification handler runs background_sync()" }
            li { "Same config-based reconstruction as Android" }
        }

        H2 { id: "token-persistence", text: "Token Persistence" }

        p {
            "Push tokens are persisted in the SyncConfig file alongside the database. "
            "This is necessary because cold sync runs without the app's normal initialization "
            "--- the token must be available from disk."
        }

        CodeBlock { html: CODE_TOKEN }

        H2 { id: "background-sync-api", text: "Background Sync API" }

        p {
            "The background_sync function is the entry point for cold sync. It returns "
            "a result indicating what happened:"
        }

        CodeBlock { html: CODE_BG_SYNC }

        H2 { id: "debounce", text: "Push Debouncing" }

        p {
            "The relay server debounces push notifications to avoid flooding devices "
            "during burst writes. The first notification fires immediately; subsequent "
            "notifications within the debounce window (default 2 seconds) are batched "
            "into a single push. Configure with --push-debounce-secs on the relay."
        }
    }
}

const CODE_ANDROID_CONFIG: &str = r##"<span class="cmt">// Dioxus: pass google-services.json to the builder</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"secret"</span>)
    .<span class="fn">with_relay_server</span>(<span class="str">"/ip4/relay.example.com/tcp/4001"</span>)
    .<span class="fn">with_google_services</span>(<span class="kw">include_str!</span>(<span class="str">"../google-services.json"</span>))
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// Or configure FCM manually without google-services.json</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"secret"</span>)
    .<span class="fn">with_relay_server</span>(<span class="str">"/ip4/relay.example.com/tcp/4001"</span>)
    .<span class="fn">with_fcm</span>(<span class="str">"project-id"</span>, <span class="str">"app-id"</span>, <span class="str">"api-key"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;"##;

const CODE_RN_ANDROID: &str = r##"<span class="cmt">// In your app's initialization (e.g., App.tsx)</span>
<span class="kw">import</span> { <span class="fn">initWaveSyncFCM</span> } <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;

<span class="cmt">// Initialize FCM — registers token with the sync engine</span>
<span class="kw">await</span> <span class="fn">initWaveSyncFCM</span>();

<span class="cmt">// In index.js — register the background handler</span>
<span class="kw">import</span> { <span class="fn">registerWaveSyncBackgroundHandler</span> } <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;

<span class="cmt">// Must be called at the top level (not inside a component)</span>
<span class="fn">registerWaveSyncBackgroundHandler</span>();"##;

const CODE_IOS_DIOXUS: &str = r##"<span class="kw">use</span> wavesyncdb::dioxus::hooks::*;

<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

    <span class="cmt">// Handles APNs token registration automatically:</span>
    <span class="cmt">// 1. Requests push notification permission</span>
    <span class="cmt">// 2. Receives the APNs device token</span>
    <span class="cmt">// 3. Calls register_push_token("Apns", token) on the engine</span>
    <span class="cmt">// 4. Engine registers the token with the relay server</span>
    <span class="fn">use_auto_push</span>(db);

    <span class="cmt">// ... rest of your app</span>
}"##;

const CODE_IOS_RN: &str = r##"<span class="cmt">// initWaveSyncFCM handles both platforms:</span>
<span class="cmt">// - Android: registers FCM token directly</span>
<span class="cmt">// - iOS: registers with APNs via Firebase (transparent bridging)</span>
<span class="kw">import</span> { <span class="fn">initWaveSyncFCM</span> } <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;

<span class="kw">await</span> <span class="fn">initWaveSyncFCM</span>();

<span class="cmt">// Background handler works on both platforms</span>
<span class="kw">import</span> { <span class="fn">registerWaveSyncBackgroundHandler</span> } <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;
<span class="fn">registerWaveSyncBackgroundHandler</span>();"##;

const CODE_RELAY_PUSH: &str = r##"<span class="cmt"># Relay server with FCM + APNs push credentials</span>
wavesync-relay \
  --listen-addr /ip4/0.0.0.0/tcp/4001 \
  --external-address /ip4/YOUR_PUBLIC_IP/tcp/4001 \
  --push-db /data/push_tokens.db \
  --fcm-credentials /path/to/service-account.json \
  --apns-key-pem /path/to/AuthKey_XXXXXX.p8 \
  --apns-key-id XXXXXX \
  --apns-team-id YYYYYY \
  --apns-bundle-id com.example.myapp \
  --push-debounce-secs <span class="num">2</span>"##;

const CODE_TOKEN: &str = r##"<span class="cmt">// Register a push token manually (usually handled by hooks)</span>
db.<span class="fn">register_push_token</span>(<span class="str">"Fcm"</span>, <span class="str">"firebase-token-string"</span>);
<span class="cmt">// or</span>
db.<span class="fn">register_push_token</span>(<span class="str">"Apns"</span>, <span class="str">"apns-device-token-hex"</span>);

<span class="cmt">// Token is saved to .wavesync_config.json in the database directory.</span>
<span class="cmt">// During cold sync, background_sync() reads this token to register</span>
<span class="cmt">// with the relay server before pulling changes.</span>"##;

const CODE_BG_SYNC: &str = r##"<span class="kw">use</span> wavesyncdb::background_sync::{background_sync, BackgroundSyncResult, BackgroundSyncError};
<span class="kw">use</span> std::time::Duration;

<span class="kw">let</span> result = <span class="fn">background_sync</span>(
    <span class="str">"sqlite:///data/data/com.app/files/app.db?mode=rwc"</span>,
    Duration::from_secs(<span class="num">30</span>),
).<span class="kw">await</span>;

<span class="kw">match</span> result {
    Ok(BackgroundSyncResult::Synced { peers_synced }) =&gt; {
        log::<span class="fn">info!</span>(<span class="str">"Cold sync complete: {peers_synced} peers"</span>);
    }
    Ok(BackgroundSyncResult::TimedOut { peers_synced }) =&gt; {
        log::<span class="fn">warn!</span>(<span class="str">"Cold sync timed out, synced with {peers_synced}"</span>);
    }
    Ok(BackgroundSyncResult::NoPeers) =&gt; {
        log::<span class="fn">warn!</span>(<span class="str">"No peers found during cold sync"</span>);
    }
    Err(BackgroundSyncError::ConfigNotFound(msg)) =&gt; {
        <span class="cmt">// App must have run at least once before cold sync works</span>
        log::<span class="fn">error!</span>(<span class="str">"No config: {msg}"</span>);
    }
    Err(e) =&gt; {
        log::<span class="fn">error!</span>(<span class="str">"Cold sync failed: {e}"</span>);
    }
}"##;
