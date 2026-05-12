//! Native side of the QR-pairing example.
//!
//! Two phases, both observed by the same `App` component via a
//! `Signal<Option<WaveSyncDb>>`:
//!
//! 1. **Pairing** — when the signal is `None`, render the manual
//!    pairing form plus, on Android, a "Pair via QR" button that
//!    opens [`QrScannerOverlay`]. On save (manual or scan), the
//!    pairing persists to `pairing.json` and a `WaveSyncDb` is built
//!    asynchronously; once it's ready, the signal flips to `Some`.
//! 2. **Task list** — when the signal is `Some(db)`, render the task
//!    list inside a `Paired` child component that calls
//!    `use_wavesync_provider(db)` so children's `use_wavesync()`
//!    succeeds. Local edits flow through `submit_local_write`
//!    (transparently via SeaORM); remote edits arrive through the
//!    relay's circuit.
//!
//! No restart needed — the DB is constructed and torn down in-place
//! from the running event loop. The "Re-pair" button replaces the
//! signal with `None` after dropping the existing DB; the app
//! transitions back to the pairing screen.

use std::path::PathBuf;

use dioxus::prelude::*;
use sea_orm::{ActiveModelTrait, Set};
use uuid::Uuid;
use wavesyncdb::dioxus::{
    SyncHandle, use_network_status, use_synced_table, use_wavesync, use_wavesync_provider,
};
use wavesyncdb::{NatStatus, RelayStatus, WaveSyncDb, WaveSyncDbBuilder};

use crate::pairing::{PairingParams, parse_pairing_url};
// `QrScannerOverlay` only mounts on Android (the `render_scan_button`
// helper is empty everywhere else, so `show_scanner` never flips to
// true). The import + the `if show_scanner()` rsx line still need to
// compile on non-Android targets where the overlay would never run —
// that's fine, the type itself is `cfg`-agnostic in `qr_scanner.rs`.
#[allow(unused_imports)]
use crate::qr_scanner::QrScannerOverlay;
use crate::task_native as task;

/// Relay host — invisible plumbing, not part of the pairing payload.
/// Resolution order, picked at compile time:
///
/// 1. **`WAVESYNC_RELAY_HOST` from `.env`** (or shell env) — set by
///    `build.rs`. Use this on a physical Android device, where
///    neither default below is reachable. See `.env.example`.
/// 2. **`10.0.2.2`** on Android — the emulator-provided alias for
///    the dev machine's loopback. Doesn't resolve on real devices.
/// 3. **`127.0.0.1`** elsewhere (desktop running on the same machine
///    as the relay).
///
/// **Native libp2p in this crate uses QUIC only** (see
/// `wavesyncdb/src/engine/mod.rs::build_swarm` — `.with_quic()` and
/// no `.with_tcp()`, deliberate for cold-start latency + to avoid
/// double connections per peer when both transports race). So we
/// dial the relay's `/udp/4001/quic-v1` listener — *not* `/tcp/4001`
/// (which would error out as `Unsupported resolved address`) and
/// *not* `/tcp/4002/ws` (no WebSocket transport on native). The web
/// build hits the same relay via its `/ws` listener.
const RELAY_HOST: &str = match option_env!("WAVESYNC_RELAY_HOST") {
    Some(h) => h,
    None => DEFAULT_RELAY_HOST,
};

#[cfg(target_os = "android")]
const DEFAULT_RELAY_HOST: &str = "10.0.2.2";
#[cfg(not(target_os = "android"))]
const DEFAULT_RELAY_HOST: &str = "127.0.0.1";

const RELAY_PEER_ID: &str = "12D3KooWP6oyorVZmZvTRPHdxsEyE2V8mR1dTiAZJYoNsMNX19KW";

fn relay_multiaddr() -> String {
    format!("/ip4/{RELAY_HOST}/udp/4001/quic-v1/p2p/{RELAY_PEER_ID}")
}

const DEFAULT_TOPIC: &str = "qr-pairing-demo";
const DEFAULT_PASS: &str = "demo-shared-secret";

/// Path of the persisted pairing JSON. Sits next to the SQLite database
/// in `data_directory()`. Schema is `{peer, topic, pass}` — the relay
/// is hardcoded per-platform, not persisted. Absent file (or a file
/// missing `peer`/`topic`) means "not paired yet — show the pairing
/// form". Old `{relay, topic, pass}` files written by previous
/// versions of this example fall through that check, so the user
/// re-pairs once.
fn pairing_path() -> PathBuf {
    dioxus_sdk_storage::data_directory().join("qr-pairing.json")
}

fn read_pairing() -> Option<PairingParams> {
    let raw = std::fs::read_to_string(pairing_path()).ok()?;
    let v: serde_json::Value = serde_json::from_str(&raw).ok()?;
    Some(PairingParams {
        peer_id: v.get("peer")?.as_str()?.to_string(),
        topic: v.get("topic")?.as_str()?.to_string(),
        pass: v
            .get("pass")
            .and_then(|p| p.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty()),
    })
}

fn write_pairing(p: &PairingParams) -> std::io::Result<()> {
    let path = pairing_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let v = serde_json::json!({
        "peer": p.peer_id,
        "topic": p.topic,
        "pass": p.pass.clone().unwrap_or_default(),
    });
    std::fs::write(path, serde_json::to_string_pretty(&v)?)
}

pub fn run() {
    // ── logging ───────────────────────────────────────────────
    // Android `/dev/null`s app stdout, so `env_logger` writes
    // nowhere on a phone. `android_logger` routes `log::*!` calls
    // into `adb logcat` under the tag below. Desktop/iOS keep using
    // `env_logger`. Initialize this BEFORE anything else so panics
    // and early init errors reach the logger we're about to install.
    init_logger();

    // Panic hook that logs the panic + backtrace before the default
    // hook tears the process down. Without this, an early panic on
    // Android shows up as "process died" in logcat with no
    // diagnostic — exactly the white-screen-no-logs failure mode.
    std::panic::set_hook(Box::new(|info| {
        log::error!("PANIC: {info}");
        eprintln!("PANIC: {info}");
    }));

    // Quiet libp2p / sqlx with the recommended log filters so the
    // example output is readable.
    if std::env::var_os("RUST_LOG").is_none() {
        let directives: Vec<String> = std::iter::once("info".to_string())
            .chain(
                wavesyncdb::recommended_log_filters()
                    .into_iter()
                    .map(|(m, lvl)| format!("{m}={}", lvl.as_str().to_lowercase())),
            )
            .collect();
        // SAFETY: setting an env var before threads start is sound;
        // this runs on the main thread before tokio spawns anything.
        unsafe {
            std::env::set_var("RUST_LOG", directives.join(","));
        }
    }

    log::info!("qr-pairing: native run() entered");

    // Build the runtime up-front and `enter()` so the guard lives for
    // the whole `dioxus::launch` call. Inside Dioxus components,
    // `spawn(async move { … })` then resolves `.await`s against this
    // runtime — that's how the reactive DB build below works.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime");
    let _guard = rt.enter();
    log::info!("qr-pairing: tokio runtime ready");
    log::info!("qr-pairing: launching Dioxus");
    dioxus::launch(App);
}

/// Build a `WaveSyncDb` against the supplied pairing. Async so it
/// can be awaited from a `spawn` inside a Dioxus event handler — that's
/// what lets the UI flip from "pairing screen" to "task list" without
/// restarting the app.
async fn build_db(paired: PairingParams) -> Result<WaveSyncDb, String> {
    let data_directory = dioxus_sdk_storage::data_directory().join("qr-pairing.db");
    if let Some(parent) = data_directory.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            return Err(format!("create data dir: {e}"));
        }
    }

    // Relay is plumbing — not part of the pairing payload. Both web
    // and native dial the same logical relay; only the transport
    // differs (web = `/tcp/4002/ws`, native = `/tcp/4001`). The QR
    // carries `peer_id` (which web peer to display + verify against
    // the topic mesh) plus `topic` + `pass` (per-session sync mesh).
    let relay = relay_multiaddr();
    log::info!(
        "qr-pairing: relay={relay} pairing-with-peer={}",
        paired.peer_id
    );

    let mut builder = WaveSyncDbBuilder::new(
        &format!("sqlite:{}?mode=rwc", data_directory.display()),
        &paired.topic,
    )
    .with_relay_server(&relay);
    if let Some(pass) = paired.pass.as_deref() {
        builder = builder.with_passphrase(pass);
    }

    log::info!("qr-pairing: building WaveSyncDb…");
    let db = builder.build().await.map_err(|e| format!("build: {e}"))?;
    log::info!("qr-pairing: schema sync…");
    db.get_schema_registry(module_path!().split("::").next().unwrap())
        .sync()
        .await
        .map_err(|e| format!("schema sync: {e}"))?;
    log::info!("qr-pairing: WaveSyncDb ready");
    Ok(db)
}

/// Init the right log backend per target. On Android, route to
/// `adb logcat` via `android_logger`; everywhere else, use the
/// standard `env_logger` (writes to stdout). Both honor `RUST_LOG`,
/// so the same module-level filter recipe works on every platform.
fn init_logger() {
    #[cfg(target_os = "android")]
    {
        use android_logger::{Config, FilterBuilder};
        let mut filter = FilterBuilder::new();
        filter.filter_level(log::LevelFilter::Info);
        // Same noise reduction the env_logger path applies — keep
        // the libp2p / sqlx output below `info` so logcat is
        // readable while debugging the demo.
        for (module, level) in wavesyncdb::recommended_log_filters() {
            filter.filter_module(module, level);
        }
        android_logger::init_once(
            Config::default()
                .with_max_level(log::LevelFilter::Trace)
                .with_tag("WaveSyncDB")
                .with_filter(filter.build()),
        );
    }
    #[cfg(not(target_os = "android"))]
    {
        let mut log_builder =
            env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
        for (module, level) in wavesyncdb::recommended_log_filters() {
            log_builder.filter_module(module, level);
        }
        let _ = log_builder.try_init();
    }
}

#[allow(non_snake_case)]
fn App() -> Element {
    // Single source of truth for "is the DB ready?" Signal-driven so
    // both first-launch (we read pairing.json on mount and build) and
    // post-pairing (the form's `on_paired` callback builds) flow
    // through the same path.
    let mut db_signal = use_signal(|| None::<WaveSyncDb>);
    let mut build_error = use_signal(|| Option::<String>::None);
    let mut building = use_signal(|| false);

    // Closure that takes a `PairingParams`, persists it, and triggers
    // an async build of the WaveSyncDb. On success, db_signal flips
    // to Some and the App branches into the Paired view. Reused by
    // first-launch auto-connect and post-pairing save.
    let mut build_for = move |paired: PairingParams| {
        if *building.read() {
            return;
        }
        building.set(true);
        build_error.set(None);
        spawn(async move {
            match build_db(paired).await {
                Ok(db) => db_signal.set(Some(db)),
                Err(e) => {
                    log::warn!("qr-pairing: build_db failed: {e}");
                    build_error.set(Some(e));
                }
            }
            building.set(false);
        });
    };

    // First mount: if pairing.json exists, kick off an async build so
    // the UI flips to Paired without user interaction. `use_hook` runs
    // exactly once for the component's lifetime; using `use_effect`
    // here would re-trigger every time we set db_signal/build_error
    // and produce a build loop.
    use_hook(move || {
        if let Some(paired) = read_pairing() {
            log::info!("qr-pairing: pairing.json present, auto-building DB");
            build_for(paired);
        } else {
            log::info!("qr-pairing: no pairing.json, showing pairing screen");
        }
    });

    rsx! {
        style { {STYLE} }
        div { class: "app",
            h1 { "WaveSyncDB · QR pairing" }
            if let Some(db) = db_signal() {
                Paired { db, db_signal }
            } else {
                PairingScreen {
                    building: building(),
                    error: build_error(),
                    on_paired: move |p| build_for(p),
                }
            }
        }
    }
}

/// Wraps the task-list views in a child component so
/// `use_wavesync_provider` is called consistently — Dioxus hook rules
/// require a stable hook order per component, so we can't call the
/// provider conditionally inside `App`'s rsx.
#[component]
fn Paired(db: WaveSyncDb, db_signal: Signal<Option<WaveSyncDb>>) -> Element {
    use_wavesync_provider(db);
    rsx! {
        PairedHeader { db_signal }
        DebugPanel {}
        AddTaskForm {}
        TaskList {}
    }
}

/// Live network state from `use_network_status`. Gives an at-a-glance
/// answer to "is the relay reachable, do we see peers, are we behind
/// NAT, what's our db_version, what peer-id are we presenting on the
/// wire." All values come from the engine's `NetworkStatus` snapshot,
/// updated on every relay/peer state change.
#[component]
fn DebugPanel() -> Element {
    let db = use_wavesync();
    let status = use_network_status(db);

    let s = status();
    let relay_class = match s.relay_status {
        RelayStatus::Listening => "value online",
        RelayStatus::Connected => "value pending",
        RelayStatus::Connecting => "value pending",
        RelayStatus::Disabled => "value offline",
    };
    let nat_label = match s.nat_status {
        NatStatus::Public => "Public",
        NatStatus::Private => "Private (NAT'd)",
        NatStatus::Unknown => "Unknown",
    };
    let peer_count = s.connected_peers.len();
    let group_count = s.group_peer_count();

    rsx! {
        div { class: "panel",
            h2 { "Network" }
            p { class: "kv", strong { "Local peer-id " }
                code { class: "wrap", "{s.local_peer_id}" }
            }
            p { class: "kv", strong { "Relay " }
                span { class: "{relay_class}", "{s.relay_status:?}" }
            }
            p { class: "kv", strong { "NAT " } "{nat_label}" }
            p { class: "kv", strong { "Topic " } code { "{s.topic}" } }
            p { class: "kv", strong { "Local db_version " } "{s.local_db_version}" }
            p { class: "kv", strong { "Peers " }
                span { class: if peer_count > 0 { "value online" } else { "value offline" },
                    "{peer_count} connected · {group_count} in group"
                }
            }

            // Per-peer detail. Group peers (same topic + passphrase)
            // get a "group" badge; bootstrap/relay peers are listed
            // but tagged so it's obvious they're plumbing, not data
            // partners.
            if !s.connected_peers.is_empty() {
                ul { class: "peer-list",
                    for peer in s.connected_peers.iter() {
                        li {
                            key: "{peer.peer_id}",
                            class: "peer",
                            div { class: "peer-id",
                                code { "{peer.peer_id}" }
                                if peer.is_group_member {
                                    span { class: "peer-badge group", "group" }
                                }
                                if peer.is_bootstrap {
                                    span { class: "peer-badge relay", "relay" }
                                }
                            }
                            div { class: "peer-meta",
                                span { class: "peer-addr", "{peer.address}" }
                                if let Some(v) = peer.db_version {
                                    span { class: "peer-version", "db_version={v}" }
                                } else {
                                    span { class: "peer-version", "db_version=?" }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[component]
fn PairingScreen(
    /// `true` while the parent is asynchronously building the
    /// `WaveSyncDb`. The form disables Save while in flight to avoid
    /// double-submits.
    building: bool,
    /// Surfaced from the parent's `build_db` future when it errors —
    /// e.g., relay unreachable, sqlite path bad.
    error: Option<String>,
    /// Fired when the user completes the form (manual save or QR
    /// scan). Parent persists the pairing and triggers `build_db`.
    on_paired: EventHandler<PairingParams>,
) -> Element {
    let mut peer_id_in = use_signal(String::new);
    let mut topic = use_signal(|| DEFAULT_TOPIC.to_string());
    let mut pass = use_signal(|| DEFAULT_PASS.to_string());
    let mut show_scanner = use_signal(|| false);
    let mut local_err = use_signal(|| Option::<String>::None);

    let mut submit_pairing = move |p: PairingParams| {
        // Persist for next launch even though we don't strictly need
        // restart anymore — keeps the saved-pairing path working when
        // the user kills + reopens.
        if let Err(e) = write_pairing(&p) {
            local_err.set(Some(format!("Failed to write pairing.json: {e}")));
            return;
        }
        local_err.set(None);
        on_paired.call(p);
    };

    let on_manual_save = move |_| {
        let pid = peer_id_in().trim().to_string();
        let t = topic().trim().to_string();
        if pid.is_empty() || t.is_empty() {
            local_err.set(Some("Web peer-id and topic are required.".into()));
            return;
        }
        let p = pass();
        let p_opt = if p.is_empty() { None } else { Some(p) };
        submit_pairing(PairingParams {
            peer_id: pid,
            topic: t,
            pass: p_opt,
        });
    };

    let mut on_scan_detect = move |raw: String| {
        show_scanner.set(false);
        match parse_pairing_url(&raw) {
            Some(p) => submit_pairing(p),
            None => local_err.set(Some(format!(
                "Scanned QR isn't a wavesync:// pairing URL: {raw}"
            ))),
        }
    };

    rsx! {
        div { class: "panel",
            h2 { "Pair this device" }
            p { class: "blurb",
                "Scan the QR shown by the web build, or paste the web peer-id and "
                "topic by hand. The relay is hardcoded into this app — see "
                "src/native_app.rs RELAY_HOST."
            }
            p { class: "kv", strong { "Relay " } code { "{relay_multiaddr()}" } }
            // Real-device hint. `#[cfg]` inside rsx isn't legal, so
            // factor the Android-only banner into a helper that's
            // defined twice (Android = real banner, others = empty).
            { render_real_device_hint() }

            // `#[cfg]` attributes inside rsx aren't legal — hand off
            // to a helper that's defined twice (Android = real button,
            // others = empty).
            { render_scan_button(show_scanner) }

            label { "Web peer-id" }
            input {
                r#type: "text",
                placeholder: "12D3KooW…",
                value: "{peer_id_in}",
                oninput: move |e| peer_id_in.set(e.value()),
                disabled: building,
            }
            label { "Topic" }
            input {
                r#type: "text",
                value: "{topic}",
                oninput: move |e| topic.set(e.value()),
                disabled: building,
            }
            label { "Passphrase (optional)" }
            input {
                r#type: "password",
                value: "{pass}",
                oninput: move |e| pass.set(e.value()),
                disabled: building,
            }
            button {
                class: "primary",
                onclick: on_manual_save,
                disabled: building,
                if building { "Connecting…" } else { "Connect" }
            }

            if let Some(err) = local_err() {
                div { class: "error", "{err}" }
            }
            if let Some(err) = error.clone() {
                div { class: "error", "Build failed: {err}" }
            }
        }

        if show_scanner() {
            QrScannerOverlay {
                on_detect: move |raw| on_scan_detect(raw),
                on_close: move |_| show_scanner.set(false),
            }
        }
    }
}

#[component]
fn PairedHeader(mut db_signal: Signal<Option<WaveSyncDb>>) -> Element {
    let paired = read_pairing();
    let mut clear_msg = use_signal(|| Option::<String>::None);

    let do_clear = move |_| {
        // Drop the DB by flipping the signal to None — `Paired` stops
        // rendering, the engine task and SQLite pool drop, the App
        // re-renders the pairing form. No restart.
        db_signal.set(None);
        match std::fs::remove_file(pairing_path()) {
            Ok(()) => clear_msg.set(None),
            Err(e) => clear_msg.set(Some(format!("Failed to clear pairing.json: {e}"))),
        }
    };

    rsx! {
        div { class: "panel",
            h2 { "Connected" }
            if let Some(p) = paired {
                p { class: "kv", strong { "Topic " } "{p.topic}" }
                p { class: "kv", strong { "Paired with " } code { class: "wrap", "{p.peer_id}" } }
                p { class: "kv", strong { "Relay " } code { "{relay_multiaddr()}" } }
                p { class: "kv", strong { "Passphrase " }
                    if p.pass.is_some() { "(set)" } else { "(none)" }
                }
            }
            button { class: "secondary", onclick: do_clear, "Re-pair" }
            if let Some(msg) = clear_msg() {
                div { class: "notice", "{msg}" }
            }
        }
    }
}

#[component]
fn AddTaskForm() -> Element {
    let db = use_wavesync();
    let mut title = use_signal(String::new);

    // `Callback` is `Copy`, so the same handler can be captured by
    // both the click and keydown closures without dancing around the
    // borrow checker (a plain `move ||` closure that mutates `title`
    // can only be moved into one place).
    let do_add = use_callback(move |_: ()| {
        let t = title().trim().to_string();
        if t.is_empty() {
            return;
        }
        title.set(String::new());
        let db = db.clone();
        spawn(async move {
            let now = chrono_now_ms();
            let task = task::ActiveModel {
                id: Set(Uuid::new_v4().to_string()),
                title: Set(t),
                done: Set(false),
                created_at: Set(now),
            };
            // SeaORM insert flows through wavesyncdb's interception
            // layer — the row commits to local SQLite AND fans out to
            // peers via the snapshot push protocol. No extra wiring.
            let _ = task.insert(&db).await;
        });
    });

    rsx! {
        div { class: "panel",
            h2 { "Add a task" }
            div { class: "add-row",
                input {
                    placeholder: "What needs to be done?",
                    value: "{title}",
                    oninput: move |e| title.set(e.value()),
                    onkeydown: move |e| { if e.key() == Key::Enter { do_add(()); } },
                }
                button {
                    class: "primary",
                    onclick: move |_| do_add(()),
                    "Add"
                }
            }
        }
    }
}

#[component]
fn TaskList() -> Element {
    let db = use_wavesync();
    // `SyncHandle` consumes the db value; clone first so we can still
    // hand it to the per-task toggle handlers below (those use the
    // db directly via SeaORM ActiveModel — native-only escape hatch).
    let tasks = use_synced_table::<task::Model>(SyncHandle::new(db.clone()));

    let mut visible: Vec<task::Model> = tasks().clone();
    visible.sort_by_key(|t| std::cmp::Reverse(t.created_at));

    rsx! {
        div { class: "panel",
            h2 { "Tasks" }
            ul { class: "task-list",
                if visible.is_empty() {
                    li { class: "empty", "No tasks yet — add one or wait for a peer." }
                }
                for t in visible.into_iter() {
                    {
                        let db = db.clone();
                        let t_for_toggle = t.clone();
                        rsx! {
                            li {
                                key: "{t.id}",
                                class: if t.done { "task done" } else { "task" },
                                input {
                                    r#type: "checkbox",
                                    checked: t.done,
                                    onchange: move |_| {
                                        let db = db.clone();
                                        let t = t_for_toggle.clone();
                                        spawn(async move {
                                            let mut am: task::ActiveModel = t.into();
                                            am.done = Set(!*am.done.as_ref());
                                            let _ = am.update(&db).await;
                                        });
                                    },
                                }
                                span { class: "title", "{t.title}" }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Camera-based QR scanning is Android-only in this example: dx 0.7's
// `RustWebChromeClient` only handles the runtime CAMERA permission
// flow on Android, and desktop has no camera worth pointing at the
// browser's QR anyway. iOS would need extra Info.plist entries this
// example doesn't ship. Two definitions of `render_scan_button` —
// the Android one renders the real button, others render nothing —
// keep the call site in `PairingScreen` clean.
#[cfg(target_os = "android")]
fn render_scan_button(mut show_scanner: Signal<bool>) -> Element {
    rsx! {
        button {
            class: "primary",
            onclick: move |_| show_scanner.set(true),
            "📷 Scan pairing QR"
        }
    }
}

#[cfg(not(target_os = "android"))]
fn render_scan_button(_show_scanner: Signal<bool>) -> Element {
    rsx! {}
}

/// Android-only banner that warns about the `10.0.2.2` emulator
/// default not working on physical devices and points at the QR-scan
/// workflow as the fix. Two definitions for the same reason as
/// `render_scan_button`.
#[cfg(target_os = "android")]
fn render_real_device_hint() -> Element {
    rsx! {
        p { class: "hint",
            "Relay host baked in at compile time: "
            code { "{RELAY_HOST}" }
            ". On a physical phone, set "
            code { "WAVESYNC_RELAY_HOST=<your LAN IP>" }
            " in "
            code { ".env" }
            " (see .env.example) and rebuild."
        }
    }
}

#[cfg(not(target_os = "android"))]
fn render_real_device_hint() -> Element {
    rsx! {}
}

const STYLE: &str = r#"
body { background: #0a0d12; color: #e6ecf2; font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, sans-serif; margin: 0; }
.app { max-width: 640px; margin: 0 auto; padding: 24px 16px 48px; }
.app h1 { font-size: 1.5rem; margin: 0 0 16px; }
.panel { background: #11151c; border: 1px solid #232a36; border-radius: 12px; padding: 18px; margin-bottom: 16px; }
.panel h2 { margin: 0 0 12px; font-size: 1rem; text-transform: uppercase; letter-spacing: 0.5px; color: #9aa6b6; }
.blurb { color: #9aa6b6; line-height: 1.5; margin: 0 0 14px; font-size: 0.9rem; }
.panel label { display: block; color: #9aa6b6; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 12px; margin-bottom: 4px; }
.panel input[type="text"], .panel input[type="password"] { width: 100%; padding: 10px 12px; background: #0a0d12; border: 1px solid #303949; border-radius: 6px; color: #e6ecf2; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.9rem; box-sizing: border-box; }
button.primary { margin-top: 12px; padding: 10px 18px; background: #6ce0c9; color: #06231f; border: none; border-radius: 6px; font-weight: 600; cursor: pointer; font-size: 0.95rem; }
button.secondary { margin-top: 8px; padding: 8px 14px; background: transparent; color: #9aa6b6; border: 1px solid #303949; border-radius: 6px; cursor: pointer; font-size: 0.85rem; }
.error { margin-top: 12px; padding: 10px; background: rgba(247, 200, 124, 0.08); border: 1px solid #f7c87c; color: #f7c87c; border-radius: 6px; font-size: 0.85rem; }
.notice { margin-top: 12px; padding: 10px; background: rgba(108, 224, 201, 0.08); border: 1px solid #6ce0c9; color: #6ce0c9; border-radius: 6px; font-size: 0.85rem; }
.hint { font-size: 0.78rem; color: #f7c87c; background: rgba(247, 200, 124, 0.06); border: 1px solid rgba(247, 200, 124, 0.3); padding: 8px 10px; border-radius: 6px; margin: 0 0 12px; line-height: 1.4; }
.hint code { background: rgba(0, 0, 0, 0.3); padding: 1px 4px; border-radius: 3px; font-size: 0.85em; }
.add-row { display: flex; gap: 8px; }
.add-row input { flex: 1; padding: 9px 12px; background: #0a0d12; border: 1px solid #303949; border-radius: 6px; color: #e6ecf2; font-size: 0.95rem; }
.add-row button.primary { margin-top: 0; }
.task-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.task { display: flex; align-items: center; gap: 12px; padding: 10px 12px; background: #0a0d12; border: 1px solid #232a36; border-radius: 6px; }
.task.done .title { text-decoration: line-through; color: #6f7c8e; }
.task input[type="checkbox"] { accent-color: #6ce0c9; width: 17px; height: 17px; cursor: pointer; }
.empty { text-align: center; color: #6f7c8e; font-style: italic; padding: 16px; }
.kv { font-size: 0.85rem; color: #e6ecf2; margin: 4px 0; }
.kv strong { color: #9aa6b6; font-weight: 500; margin-right: 6px; }
.kv code { font-size: 0.8rem; word-break: break-all; }
.kv code.wrap { word-break: break-all; display: inline-block; max-width: 100%; }
.value.online { color: #6ce0c9; }
.value.offline { color: #e74c3c; }
.value.pending { color: #f39c12; }
.peer-list { list-style: none; margin: 12px 0 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }
.peer { padding: 10px 12px; background: #0a0d12; border: 1px solid #232a36; border-radius: 6px; }
.peer-id { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
.peer-id code { font-size: 0.75rem; word-break: break-all; flex: 1; min-width: 0; }
.peer-badge { font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.5px; padding: 2px 6px; border-radius: 4px; flex-shrink: 0; }
.peer-badge.group { background: rgba(108, 224, 201, 0.15); color: #6ce0c9; }
.peer-badge.relay { background: rgba(124, 159, 247, 0.15); color: #7c9ff7; }
.peer-meta { display: flex; gap: 8px; font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.7rem; color: #6f7c8e; margin-top: 4px; word-break: break-all; }
.peer-version { color: #9aa6b6; }
"#;
