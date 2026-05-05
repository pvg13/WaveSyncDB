//! Build script — loads `.env` from the example's root and re-exports
//! its `KEY=VALUE` pairs as `cargo:rustc-env=KEY=VALUE`, so source can
//! read them via `env!()` / `option_env!()` at compile time.
//!
//! Currently used for `WAVESYNC_RELAY_HOST`: the host the native app
//! dials when reaching the relay. Defaults to `127.0.0.1` (desktop)
//! and `10.0.2.2` (Android emulator) when unset; on a physical phone
//! the user puts their dev machine's LAN IP in `.env`:
//!
//! ```
//! WAVESYNC_RELAY_HOST=192.168.1.150
//! ```
//!
//! Re-runs whenever `.env` changes or `WAVESYNC_RELAY_HOST` is set on
//! the command line.

use std::fs;
use std::path::Path;

fn main() {
    let env_path = Path::new(".env");
    println!("cargo:rerun-if-changed=.env");
    println!("cargo:rerun-if-env-changed=WAVESYNC_RELAY_HOST");

    let Ok(contents) = fs::read_to_string(env_path) else {
        return;
    };
    for raw_line in contents.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = value
            .trim()
            .trim_start_matches('"')
            .trim_end_matches('"')
            .trim_start_matches('\'')
            .trim_end_matches('\'');
        if key.is_empty() {
            continue;
        }
        println!("cargo:rustc-env={key}={value}");
    }
}
