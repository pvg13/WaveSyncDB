//! Camera-based QR-code scanner. Mounted as a full-screen overlay
//! when the user taps "Pair via QR" on Android. Mirrors the pattern
//! from MediterraneaDiente's barcode scanner — same `BarcodeDetector`
//! polling, same `dioxus.send` IPC for messages back to Rust, same
//! `use_drop` cleanup.
//!
//! Mobile-only by convention — desktop builds never instantiate it
//! (the trigger button is `cfg(target_os = "android")`-gated). The
//! component itself compiles on every native target so we don't need
//! parallel cfg branches in the parent rsx.

use dioxus::prelude::*;
use serde::Deserialize;

/// IMPORTANT — do **not** wrap the body in `(async () => {...})()`.
/// `dioxus_desktop::query::QueryEngine::new_query` wraps user scripts
/// inside `new AsyncFunction("dioxus", SCRIPT)` and treats the
/// returned Promise as the query lifetime: when that Promise
/// resolves, `dioxus.close()` is called and any `eval.recv` on the
/// Rust side returns `Err(channel closed)`. Keep the body flat and
/// `await` the long-running work directly so the outer Promise only
/// resolves when we genuinely want to close.
const SCANNER_JS: &str = r#"
const send = (kind, value) => {
    // Pass the object directly. `dioxus.send` already JSON-encodes
    // the IPC envelope; double-encoding lands on Rust as
    // `Value::String("{...}")` and fails to deserialize.
    try { dioxus.send({ kind, value: value || "" }); } catch (e) {}
};

if (!('BarcodeDetector' in window)) {
    send('error', 'BarcodeDetector unsupported on this device');
    return;
}

let stream = null;
let stopped = false;

window.__wavesyncQrScannerStop = () => {
    stopped = true;
    if (stream) {
        stream.getTracks().forEach(t => t.stop());
        stream = null;
    }
};

let video;
try {
    video = document.getElementById('wavesync-qr-scanner-video');
    if (!video) {
        send('error', 'Camera element not found');
        return;
    }
    stream = await navigator.mediaDevices.getUserMedia({
        video: { facingMode: 'environment' }
    });
    if (stopped) {
        stream.getTracks().forEach(t => t.stop());
        return;
    }
    video.srcObject = stream;
    await video.play();
    send('started', '');
} catch (e) {
    send('error', String((e && e.message) || e));
    return;
}

const detector = new BarcodeDetector({ formats: ['qr_code'] });

while (!stopped) {
    try {
        const codes = await detector.detect(video);
        if (codes && codes.length > 0) {
            const value = codes[0].rawValue;
            send('detected', value);
            if (stream) {
                stream.getTracks().forEach(t => t.stop());
                stream = null;
            }
            return;
        }
    } catch (e) {
        // Swallow per-frame failures — detector occasionally throws
        // transiently when the camera resizes / focuses.
    }
    await new Promise(r => setTimeout(r, 200));
}
"#;

#[derive(Debug, Deserialize)]
struct ScannerMsg {
    kind: String,
    #[serde(default)]
    value: String,
}

/// Full-screen QR scanner overlay. On detect, calls `on_detect(raw)`
/// where `raw` is the QR's text payload (the caller passes that to
/// `parse_pairing_url`). On user cancel or fatal error, calls
/// `on_close()` and the parent unmounts the overlay.
#[component]
pub fn QrScannerOverlay(on_close: EventHandler<()>, on_detect: EventHandler<String>) -> Element {
    let mut error = use_signal(|| Option::<String>::None);
    let mut started = use_signal(|| false);

    use_effect(move || {
        let mut eval = document::eval(SCANNER_JS);
        spawn(async move {
            while let Ok(msg) = eval.recv::<ScannerMsg>().await {
                match msg.kind.as_str() {
                    "started" => started.set(true),
                    "detected" if !msg.value.is_empty() => {
                        on_detect.call(msg.value);
                        return;
                    }
                    "error" => {
                        error.set(Some(if msg.value.is_empty() {
                            "Camera unavailable.".to_string()
                        } else {
                            msg.value
                        }));
                        return;
                    }
                    _ => {}
                }
            }
        });
    });

    use_drop(|| {
        document::eval("if (window.__wavesyncQrScannerStop) { window.__wavesyncQrScannerStop(); }");
    });

    let close = move |_| on_close.call(());

    rsx! {
        style { {SCANNER_CSS} }
        div { class: "qr-scanner-overlay",
            div { class: "qr-scanner-bar",
                span { class: "qr-scanner-title",
                    if error().is_some() {
                        "Scanner error"
                    } else if started() {
                        "Point at the pairing QR"
                    } else {
                        "Starting camera…"
                    }
                }
                button {
                    r#type: "button",
                    class: "qr-scanner-close",
                    onclick: close,
                    "Cancel"
                }
            }
            div { class: "qr-scanner-frame",
                video {
                    id: "wavesync-qr-scanner-video",
                    class: "qr-scanner-video",
                    autoplay: "true",
                    playsinline: "true",
                    muted: "true",
                }
                div { class: "qr-scanner-guide" }
            }
            if let Some(msg) = error() {
                div { class: "qr-scanner-error",
                    p { strong { "{msg}" } }
                    p { "Type the relay multiaddr by hand instead." }
                    button {
                        r#type: "button",
                        class: "qr-scanner-error-close",
                        onclick: close,
                        "Close"
                    }
                }
            }
        }
    }
}

const SCANNER_CSS: &str = r#"
.qr-scanner-overlay {
    position: fixed;
    inset: 0;
    z-index: 60;
    display: flex;
    flex-direction: column;
    background: #000;
    color: #fff;
    padding-top: env(safe-area-inset-top, 0);
    padding-bottom: env(safe-area-inset-bottom, 0);
}
.qr-scanner-bar {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 12px 16px;
    background: rgba(0, 0, 0, 0.6);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
}
.qr-scanner-title {
    font-size: 14px;
    font-weight: 500;
}
.qr-scanner-close {
    cursor: pointer;
    border: none;
    border-radius: 8px;
    background: rgba(255, 255, 255, 0.15);
    color: #fff;
    padding: 6px 12px;
    font-size: 14px;
    font-weight: 500;
}
.qr-scanner-frame {
    position: relative;
    flex: 1;
    overflow: hidden;
}
.qr-scanner-video {
    position: absolute;
    inset: 0;
    height: 100%;
    width: 100%;
    object-fit: cover;
}
.qr-scanner-guide {
    pointer-events: none;
    position: absolute;
    top: 50%;
    left: 50%;
    aspect-ratio: 1 / 1;
    width: 70%;
    transform: translate(-50%, -50%);
    border: 2px solid rgba(255, 255, 255, 0.85);
    border-radius: 12px;
    box-shadow: 0 0 0 9999px rgba(0, 0, 0, 0.35);
}
.qr-scanner-error {
    position: absolute;
    left: 16px;
    right: 16px;
    bottom: 24px;
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 16px;
    border-radius: 12px;
    background: rgba(0, 0, 0, 0.8);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    color: #fff;
    font-size: 14px;
}
.qr-scanner-error-close {
    cursor: pointer;
    border: none;
    border-radius: 6px;
    background: #4a90d9;
    color: #fff;
    padding: 8px 16px;
    font-size: 14px;
    align-self: flex-start;
}
"#;
