fn main() {
    println!("cargo::rustc-check-cfg=cfg(has_google_services)");
    println!("cargo:rerun-if-changed=google-services.json");
    // Pick up `WAVESYNC_RELAY_OVERRIDE` in `option_env!()` at compile
    // time. The Maestro test harness (`test.sh`) sets this so the
    // installed APK dials a local relay with controlled FCM creds
    // instead of the bundled production relay address.
    println!("cargo:rerun-if-env-changed=WAVESYNC_RELAY_OVERRIDE");

    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os != "android" {
        return;
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    let path = std::path::Path::new(&manifest_dir).join("google-services.json");
    if path.exists() {
        println!("cargo:rustc-cfg=has_google_services");
    }
}
