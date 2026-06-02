use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Security Model" }

        p {
            "WaveSyncDB authenticates peers and protects data in transit using "
            "passphrase-derived keys and HMAC verification. This page explains "
            "what is protected, what is not, and how the cryptographic primitives "
            "work together."
        }

        H2 { id: "key-derivation", text: "Key Derivation" }

        p {
            "Everything derives from a single user-supplied passphrase. The passphrase "
            "produces two outputs:"
        }

        ul {
            li {
                "A GroupKey --- 32-byte BLAKE3 key used for HMAC signing and verification"
            }
            li {
                "A derived topic --- BLAKE3 hash of (user_topic + group_key), used as "
                "the libp2p topic and rendezvous namespace"
            }
        }

        CodeBlock { html: CODE_DERIVATION }

        H2 { id: "topic-isolation", text: "Topic Isolation" }

        p {
            "The derived topic serves as the first layer of defense. Because the topic "
            "is derived from both the user-visible topic string and the passphrase, two "
            "important properties hold:"
        }

        ul {
            li {
                "Different passphrases create completely isolated groups, even if the "
                "user-visible topic string is the same. App A with passphrase \"foo\" and "
                "App B with passphrase \"bar\" using topic \"shared\" will never see each other."
            }
            li {
                "The derived topic does not leak the passphrase. The relay server sees "
                "the topic string (for rendezvous routing) but cannot reverse it to "
                "recover the passphrase."
            }
        }

        CodeBlock { html: CODE_TOPIC }

        H2 { id: "hmac", text: "HMAC Verification" }

        p {
            "The GroupKey is used to compute a BLAKE3 keyed MAC over every request-response "
            "message. HMAC verification is mandatory on ALL message paths when a passphrase "
            "is configured:"
        }

        ul {
            li {
                "VersionVector requests and ChangesetResponse replies (catch-up sync)"
            }
            li { "Push requests (real-time fan-out)" }
            li { "IdentityAnnounce messages" }
        }

        p { class: "callout",
            "Missing HMAC verification on even one path leaves a full unauthenticated "
            "sync vector open. Any peer on the network could inject arbitrary data "
            "through the unprotected path. This has happened before when the catch-up "
            "path was left unprotected while real-time push was protected."
        }

        H3 { id: "hmac-no-clock", text: "No Wall-Clock Time" }

        p {
            "The HMAC is computed over the message content and the shared key only. "
            "Wall-clock time is deliberately excluded. Including timestamps would cause "
            "valid messages to fail verification when devices have clock skew, which is "
            "common on mobile devices and across time zones."
        }

        CodeBlock { html: CODE_HMAC }

        H2 { id: "peer-rejection", text: "Peer Rejection" }

        p {
            "When a peer sends a message with a mismatched topic or invalid HMAC, it is "
            "added to the rejected_peers set permanently for the engine's lifetime. "
            "This is intentional:"
        }

        ul {
            li {
                "mDNS rediscovers peers every few seconds. A per-connection rejection "
                "would cause rejected peers to be retried on every mDNS cycle, creating "
                "a storm of failed authentication attempts."
            }
            li {
                "The permanent rejection prevents cross-application interference on "
                "shared networks (e.g., two apps running on the same WiFi with different "
                "passphrases)."
            }
        }

        p {
            "Unauthenticated messages are dropped silently --- no error response is "
            "sent. This prevents an attacker from probing to learn which topics exist "
            "on the network."
        }

        H2 { id: "what-is-protected", text: "What IS Protected" }

        ul {
            li {
                "Data in transit --- all sync messages are HMAC-authenticated. A peer "
                "without the passphrase cannot inject, modify, or forge messages."
            }
            li {
                "Group membership --- only peers with the correct passphrase can join "
                "the sync group and receive data."
            }
            li {
                "Topic privacy --- the relay server cannot determine the original topic "
                "name or passphrase from the derived topic hash."
            }
        }

        H2 { id: "what-is-not-protected", text: "What is NOT Protected" }

        ul {
            li {
                "Data at rest --- SQLite database files are stored unencrypted on disk. "
                "Use full-disk encryption (Android: enabled by default since Android 10, "
                "iOS: Data Protection) or SQLCipher for database-level encryption."
            }
            li {
                "Connection metadata --- the relay server can observe which PeerIds "
                "connect and when, but cannot read the sync data payload."
            }
            li {
                "Denial of service --- a peer can flood the network with invalid "
                "messages (they will be rejected but still consume bandwidth). The "
                "permanent rejection set limits the impact."
            }
            li {
                "Replay attacks --- the HMAC does not include a nonce or sequence "
                "number. However, replayed messages are harmless because the CRDT "
                "conflict resolution is idempotent --- applying the same change twice "
                "has no effect."
            }
        }

        H2 { id: "configuration", text: "Configuring Security" }

        CodeBlock { html: CODE_CONFIG }

        H2 { id: "no-passphrase", text: "Running Without a Passphrase" }

        p {
            "If no passphrase is configured, WaveSyncDB runs in open mode:"
        }

        ul {
            li { "No HMAC verification on messages" }
            li { "The raw user topic string is used directly (no derivation)" }
            li { "Any peer can join and sync data" }
        }

        p {
            "Open mode is suitable for development, local testing, and scenarios "
            "where all devices are on a trusted network. It is not recommended for "
            "production deployments with sensitive data."
        }

        H2 { id: "threat-model", text: "Threat Model Summary" }

        CodeBlock { html: CODE_THREATS }
    }
}

const CODE_DERIVATION: &str = r##"<span class="cmt">// Step 1: Passphrase → GroupKey (32 bytes)</span>
<span class="kw">let</span> key = blake3::<span class="fn">derive_key</span>(
    <span class="str">"wavesyncdb-group-key-v1"</span>,
    passphrase.<span class="fn">as_bytes</span>(),
);

<span class="cmt">// Step 2: (user_topic + GroupKey) → derived topic string</span>
<span class="kw">let</span> <span class="kw">mut</span> hasher = blake3::Hasher::<span class="fn">new_derive_key</span>(<span class="str">"wavesyncdb-topic-v1"</span>);
hasher.<span class="fn">update</span>(user_topic.<span class="fn">as_bytes</span>());
hasher.<span class="fn">update</span>(&amp;group_key);
<span class="kw">let</span> topic = <span class="fn">format!</span>(<span class="str">"wavesync-{}"</span>, hasher.<span class="fn">finalize</span>().<span class="fn">to_hex</span>());

<span class="cmt">// The derived topic is used as:</span>
<span class="cmt">// - The libp2p pub/sub topic</span>
<span class="cmt">// - The rendezvous namespace</span>
<span class="cmt">// - The topic field in all sync messages</span>"##;

const CODE_TOPIC: &str = r##"<span class="cmt">// Same user topic, different passphrases → different derived topics</span>
GroupKey(<span class="str">"password-A"</span>).<span class="fn">derive_topic</span>(<span class="str">"my-app"</span>)
  → <span class="str">"wavesync-a1b2c3d4e5f6..."</span>

GroupKey(<span class="str">"password-B"</span>).<span class="fn">derive_topic</span>(<span class="str">"my-app"</span>)
  → <span class="str">"wavesync-f7e8d9c0b1a2..."</span>

<span class="cmt">// These two groups are completely isolated:</span>
<span class="cmt">// - Different topics → peers never discover each other via rendezvous</span>
<span class="cmt">// - Different GroupKeys → HMAC verification fails if they somehow connect</span>
<span class="cmt">// - Permanent rejection → never retried after first failure</span>"##;

const CODE_HMAC: &str = r##"<span class="cmt">// HMAC computation (BLAKE3 keyed hash)</span>
<span class="kw">let</span> tag = blake3::<span class="fn">keyed_hash</span>(&amp;group_key, &amp;message_bytes);

<span class="cmt">// Verification</span>
<span class="kw">let</span> expected = blake3::<span class="fn">keyed_hash</span>(&amp;group_key, &amp;message_bytes);
<span class="kw">let</span> valid = (expected == received_tag);  <span class="cmt">// constant-time comparison</span>

<span class="cmt">// What is included in the HMAC:</span>
<span class="cmt">//   ✓ Full message content (serialized JSON)</span>
<span class="cmt">//   ✓ Shared group key</span>
<span class="cmt">//</span>
<span class="cmt">// What is NOT included:</span>
<span class="cmt">//   ✗ Wall-clock time (would break with clock skew)</span>
<span class="cmt">//   ✗ Nonce/counter (not needed — CRDTs are idempotent)</span>"##;

const CODE_CONFIG: &str = r##"<span class="cmt">// Rust: set passphrase on the builder</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"a-strong-shared-secret"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// React Native</span>
<span class="kw">await</span> <span class="fn">initialize</span>(<span class="str">'app.db'</span>, <span class="str">'my-topic'</span>, <span class="str">'a-strong-shared-secret'</span>);

<span class="cmt">// WatermelonDB</span>
<span class="kw">const</span> adapter = <span class="kw">new</span> <span class="fn">WaveSyncAdapter</span>({
  schema,
  topic: <span class="str">'my-topic'</span>,
  passphrase: <span class="str">'a-strong-shared-secret'</span>,
});

<span class="cmt">// All devices in the sync group must use the same passphrase.</span>
<span class="cmt">// There is no key exchange protocol — the passphrase must be</span>
<span class="cmt">// shared out-of-band (e.g., QR code, invite link, manual entry).</span>"##;

const CODE_THREATS: &str = r##"<span class="cmt">Threat                          │ Mitigated?  │ How</span>
────────────────────────────────┼─────────────┼──────────────────────────
Eavesdropping on sync data      │ Yes         │ HMAC-authenticated messages
Injecting forged changes        │ Yes         │ HMAC verification on all paths
Joining without passphrase      │ Yes         │ Topic isolation + HMAC
Cross-app interference          │ Yes         │ Derived topics + permanent rejection
Reading data at rest            │ No          │ Use SQLCipher / disk encryption
Relay sees data content         │ N/A         │ Relay only routes, cannot decrypt
Replay of old messages          │ Harmless    │ CRDTs are idempotent
Denial of service               │ Partial     │ Rejected peers are permanently blocked
Clock-skew HMAC failures        │ N/A         │ No timestamps in HMAC</span>"##;
