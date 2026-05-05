//! Shared pairing-URL helpers.
//!
//! The pairing protocol is a URL of the form
//! `wavesync://?peer=<urlenc>&topic=<urlenc>&pass=<urlenc>` — the
//! browser side encodes one and renders it as a QR; the phone side
//! scans the QR, gets the same URL string back from
//! `BarcodeDetector`, and parses it here to recover the peer + topic +
//! passphrase the web is announcing on. Compiles on every target.
//!
//! The relay multiaddr is **not** in the QR. Both apps hard-code it
//! per-platform (web uses `/ws`, native uses `/tcp/4001`), since
//! that's invisible plumbing — the user thinks of "pairing my phone"
//! as exchanging an identity, not a server address.

/// Decoded pairing parameters scanned from a QR (or pasted from a
/// URL). `pass = None` means "no passphrase configured", same
/// convention `WebSyncClient::connect_via_relay` and
/// `WaveSyncDbBuilder::with_passphrase` use.
///
/// `peer_id` is the libp2p PeerId of the *web* peer (string form,
/// e.g. `12D3KooW…`) — encoded so the phone can display "paired with
/// X" and verify a peer it sees on the relay's topic mesh is the one
/// it scanned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PairingParams {
    pub peer_id: String,
    pub topic: String,
    pub pass: Option<String>,
}

/// Build the `wavesync://?peer=…&topic=…&pass=…` URL the browser
/// renders into a QR. Used on the web side; the parse function below
/// is the inverse, used on the phone side.
///
/// `#[allow(dead_code)]` because only the wasm32 path constructs URLs
/// — native parses them. Stripping the `cfg(...)`-gating keeps the
/// roundtrip test in this module compilable on every target.
#[allow(dead_code)]
pub fn build_pairing_url(p: &PairingParams) -> String {
    match &p.pass {
        Some(pass) if !pass.is_empty() => format!(
            "wavesync://?peer={}&topic={}&pass={}",
            percent_encode(&p.peer_id),
            percent_encode(&p.topic),
            percent_encode(pass),
        ),
        _ => format!(
            "wavesync://?peer={}&topic={}",
            percent_encode(&p.peer_id),
            percent_encode(&p.topic),
        ),
    }
}

/// Parse a pairing URL.
///
/// Lenient about scheme — accepts both `wavesync://` and `wavesync:`
/// prefixes; also accepts a bare query string so a user can paste
/// just the params if QR scanning fails. Returns `None` if `peer` or
/// `topic` is missing — those two are required.
///
/// `#[allow(dead_code)]` because only the native path scans QRs and
/// parses URLs — the wasm path constructs them. Kept module-public
/// (and target-agnostic) so the test below can exercise it on every
/// target without cfg gymnastics.
#[allow(dead_code)]
pub fn parse_pairing_url(raw: &str) -> Option<PairingParams> {
    let after_scheme = raw
        .strip_prefix("wavesync://")
        .or_else(|| raw.strip_prefix("wavesync:"))
        .unwrap_or(raw);
    let query = after_scheme
        .split_once('?')
        .map(|(_, q)| q)
        .unwrap_or(after_scheme);

    let mut peer_id = None;
    let mut topic = None;
    let mut pass = None;
    for kv in query.split('&') {
        let Some((k, v)) = kv.split_once('=') else {
            continue;
        };
        let v = percent_decode(v);
        match k {
            "peer" => peer_id = Some(v),
            "topic" => topic = Some(v),
            "pass" => pass = Some(v),
            _ => {}
        }
    }
    Some(PairingParams {
        peer_id: peer_id?,
        topic: topic?,
        pass: pass.filter(|s| !s.is_empty()),
    })
}

/// Tiny percent-encoder for the pairing-URL query params. Encodes
/// anything that isn't an unreserved character per RFC 3986. Adding
/// a `urlencoding` crate just for this would be overkill — we
/// control both ends and inputs are short.
#[allow(dead_code)]
fn percent_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Decode a percent-encoded value. Tolerant — unrecognized escapes
/// are left as-is rather than failing the whole parse, so a
/// hand-typed URL with one bad escape still produces something
/// readable rather than `None`.
#[allow(dead_code)]
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = hex(bytes[i + 1]);
                let lo = hex(bytes[i + 2]);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h << 4) | l);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).unwrap_or_default()
}

#[allow(dead_code)]
fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_roundtrip() {
        let p = PairingParams {
            peer_id: "12D3KooWP6oyorVZmZvTRPHdxsEyE2V8mR1dTiAZJYoNsMNX19KW".into(),
            topic: "demo".into(),
            pass: Some("hunter 2".into()),
        };
        let url = build_pairing_url(&p);
        let parsed = parse_pairing_url(&url).unwrap();
        assert_eq!(parsed, p);
    }

    #[test]
    fn no_passphrase() {
        let p = parse_pairing_url("wavesync://?peer=abcDEF&topic=bar").unwrap();
        assert_eq!(p.peer_id, "abcDEF");
        assert_eq!(p.topic, "bar");
        assert_eq!(p.pass, None);
    }

    #[test]
    fn empty_passphrase_yields_none() {
        let p = parse_pairing_url("wavesync://?peer=abcDEF&topic=bar&pass=").unwrap();
        assert_eq!(p.pass, None);
    }

    #[test]
    fn missing_peer_returns_none() {
        assert!(parse_pairing_url("wavesync://?topic=bar").is_none());
    }

    #[test]
    fn missing_topic_returns_none() {
        assert!(parse_pairing_url("wavesync://?peer=abcDEF").is_none());
    }
}
