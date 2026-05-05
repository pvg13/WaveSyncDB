//! WaveSyncDB QR-pairing example — single crate, two binaries:
//!
//! - **Web build** (`dx serve --platform web`): renders a pairing
//!   form, generates a `wavesync://?relay=…&topic=…&pass=…` QR, opens
//!   a `WebSyncClient::connect_via_relay` to the same relay, and
//!   shows a reactive task list.
//! - **Native build** (`dx serve --platform android` / `desktop`):
//!   on first launch shows a pairing screen with a "Pair via QR"
//!   camera button (Android) and a manual form (any platform). Once
//!   paired, builds a `WaveSyncDb` against the saved relay + topic +
//!   passphrase and shows a reactive task list. Edits sync between
//!   web and native through the relay.
//!
//! Run a `wavesync-relay` somewhere both peers can reach (LAN works
//! for testing — the relay's `--ws-listen-addr` lets browsers join).
//! Open the web build, fill in the relay address, generate the QR,
//! scan it from the native build. Tasks added on either side sync.
//!
//! See `README.md` for the full end-to-end runbook.

mod pairing;

#[cfg(target_arch = "wasm32")]
mod web_app;

#[cfg(not(target_arch = "wasm32"))]
mod native_app;
#[cfg(not(target_arch = "wasm32"))]
mod qr_scanner;
#[cfg(not(target_arch = "wasm32"))]
mod task_native;

#[cfg(target_arch = "wasm32")]
fn main() {
    dioxus::launch(web_app::App);
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    native_app::run();
}
