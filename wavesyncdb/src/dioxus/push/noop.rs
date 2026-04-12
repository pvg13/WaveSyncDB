/// No-op on non-iOS platforms. Push token registration is handled
/// natively (e.g., via `WaveSyncInitProvider` on Android).
pub fn setup_push_token_writer(_token_dir: std::path::PathBuf) {}
