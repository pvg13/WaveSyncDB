-keep class dev.dioxus.main.WaveSyncService { *; }
-keep class dev.dioxus.main.WaveSyncNative { *; }
# Reached only via JNI by name (env.call_static_method from Rust) — R8 can't
# see those references, so without these keeps a minified release build strips
# or renames them and the notification call silently fails.
-keep class dev.dioxus.main.NotificationHelper { *; }
-keep class dev.dioxus.main.NotificationPermission { *; }
