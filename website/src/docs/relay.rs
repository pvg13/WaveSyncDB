use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Relay Server" }

        p {
            "The WaveSyncDB relay server provides three services for devices that cannot "
            "connect directly: circuit relay for NAT traversal, rendezvous for peer "
            "discovery, and push notification forwarding for waking mobile devices."
        }

        H2 { id: "when-needed", text: "When Do You Need a Relay?" }

        p {
            "You do NOT need a relay for LAN-only sync. Devices on the same local network "
            "discover each other via mDNS and connect directly."
        }

        p {
            "You need a relay when:"
        }

        ul {
            li { "Devices are on different networks (WAN sync)" }
            li { "Mobile devices are behind carrier NAT" }
            li { "You want push notifications to wake sleeping devices" }
            li { "You need rendezvous-based peer discovery (no mDNS on WAN)" }
        }

        H2 { id: "installation", text: "Installation" }

        H3 { id: "install-cargo", text: "From Cargo" }

        CodeBlock { html: CODE_CARGO_INSTALL }

        H3 { id: "install-docker", text: "Docker" }

        CodeBlock { html: CODE_DOCKER_INSTALL }

        H2 { id: "configuration", text: "Configuration" }

        p {
            "All configuration is via CLI arguments or environment variables. "
            "Environment variables match the long flag names in SCREAMING_SNAKE_CASE."
        }

        H3 { id: "core-args", text: "Core Arguments" }

        CodeBlock { html: CODE_CORE_ARGS }

        H3 { id: "circuit-args", text: "Circuit Relay Arguments" }

        CodeBlock { html: CODE_CIRCUIT_ARGS }

        H3 { id: "push-args", text: "Push Notification Arguments" }

        CodeBlock { html: CODE_PUSH_ARGS }

        H3 { id: "connection-args", text: "Connection Arguments" }

        CodeBlock { html: CODE_CONN_ARGS }

        H2 { id: "external-address", text: "External Address" }

        p { class: "callout",
            "CRITICAL: When running behind NAT or in Docker, you MUST set --external-address "
            "to your public IP and port. Without it, circuit relay reservations fail with "
            "NoAddressesInReservation and WAN sync is completely broken."
        }

        p {
            "The external address tells peers how to reach the relay from outside the "
            "local network. It must be a valid multiaddr:"
        }

        CodeBlock { html: CODE_EXTERNAL }

        H2 { id: "identity", text: "Identity Persistence" }

        p {
            "The relay server has a libp2p PeerId derived from an identity keypair. "
            "Clients reference this PeerId in their relay and rendezvous configuration. "
            "If the PeerId changes on restart, all clients need to be reconfigured."
        }

        p {
            "There are three ways to persist the identity:"
        }

        CodeBlock { html: CODE_IDENTITY }

        H2 { id: "docker-deploy", text: "Docker Deployment" }

        p {
            "The recommended production setup uses Docker Compose. Here is a complete "
            "docker-compose.yml:"
        }

        CodeBlock { html: CODE_DOCKER_COMPOSE }

        p {
            "Steps to deploy:"
        }

        ul {
            li {
                "Generate an identity keypair: "
                "docker compose run --rm wavesync-relay --generate-identity"
            }
            li { "Set IDENTITY_KEYPAIR in the environment section" }
            li { "Set EXTERNAL_ADDRESS to your server's public IP" }
            li { "Optionally add FCM_CREDENTIALS and/or APNs variables for push" }
            li { "Run: docker compose up -d" }
        }

        H2 { id: "client-config", text: "Client Configuration" }

        p {
            "Clients connect to the relay using its multiaddr. The relay serves as "
            "both the circuit relay and the rendezvous server on the same address."
        }

        CodeBlock { html: CODE_CLIENT }

        H2 { id: "monitoring", text: "Monitoring" }

        p {
            "The relay logs connection events, circuit reservations, rendezvous "
            "registrations, and push notifications through "
            code { "tracing" }
            " with "
            code { "tracing-subscriber" }
            "'s "
            code { "EnvFilter" }
            ", so verbosity is controlled the same way it always has been — "
            "with RUST_LOG:"
        }

        CodeBlock { html: CODE_LOGGING }

        p { class: "note",
            "If you're scraping relay output with an "
            code { "env_logger" }
            "-based tool, nothing changes on your end: every "
            code { "tracing" }
            " event the relay emits is also published as a "
            code { "log" }
            " record, so existing log-based tailing/parsing keeps working unmodified."
        }

        p {
            "Key events to monitor:"
        }

        ul {
            li { "\"New circuit reservation\" --- a client successfully reserved a relay circuit" }
            li { "\"Rendezvous: peer registered\" --- a peer registered for discovery" }
            li { "\"Push notification sent\" --- an FCM/APNs notification was delivered" }
            li { "\"Connection established\" / \"Connection closed\" --- peer connectivity" }
        }
    }
}

const CODE_CARGO_INSTALL: &str = r##"<span class="cmt"># Install from crates.io</span>
cargo install wavesync-relay

<span class="cmt"># Run with default settings (LAN only, no push)</span>
wavesync-relay

<span class="cmt"># Run with WAN support</span>
wavesync-relay \
  --listen-addr /ip4/0.0.0.0/tcp/4001 \
  --external-address /ip4/YOUR_PUBLIC_IP/tcp/4001 \
  --identity-file /var/lib/wavesync/identity.key"##;

const CODE_DOCKER_INSTALL: &str = r##"<span class="cmt"># Pull the image</span>
docker pull ghcr.io/pvg13/wavesync-relay:latest

<span class="cmt"># Run directly</span>
docker run -p 4001:4001 \
  -e EXTERNAL_ADDRESS=/ip4/YOUR_PUBLIC_IP/tcp/4001 \
  -v relay-data:/data \
  ghcr.io/pvg13/wavesync-relay:latest"##;

const CODE_CORE_ARGS: &str = r##"--listen-addr &lt;MULTIADDR&gt;
    <span class="cmt">Listen address. Default: /ip4/0.0.0.0/tcp/4001</span>

--identity-file &lt;PATH&gt;
    <span class="cmt">Path to persistent identity keypair file.</span>
    <span class="cmt">Created automatically if the file does not exist.</span>

--identity-keypair &lt;BASE64&gt;   (env: IDENTITY_KEYPAIR)
    <span class="cmt">Base64-encoded protobuf identity keypair.</span>
    <span class="cmt">Overrides --identity-file if both are set.</span>

--generate-identity
    <span class="cmt">Print a new base64-encoded keypair and PeerId, then exit.</span>
    <span class="cmt">Use this to generate a value for IDENTITY_KEYPAIR.</span>

--external-address &lt;MULTIADDR&gt;   (env: EXTERNAL_ADDRESS)
    <span class="cmt">Public address to advertise. Required behind NAT/Docker.</span>
    <span class="cmt">Repeatable for multiple addresses.</span>"##;

const CODE_CIRCUIT_ARGS: &str = r##"--max-circuits &lt;N&gt;
    <span class="cmt">Maximum number of concurrent relay circuits. 0 = unlimited.</span>
    <span class="cmt">Default: 256</span>

--max-circuit-duration &lt;SECS&gt;
    <span class="cmt">Maximum lifetime of a single circuit in seconds.</span>
    <span class="cmt">Default: 3600 (1 hour)</span>

--max-circuit-bytes &lt;N&gt;
    <span class="cmt">Maximum bytes transferred per circuit. 0 = unlimited.</span>
    <span class="cmt">Default: 0 (unlimited)</span>"##;

const CODE_PUSH_ARGS: &str = r##"--push-db &lt;PATH&gt;   (env: PUSH_DB)
    <span class="cmt">Path to push token SQLite database. Enables push notifications.</span>
    <span class="cmt">Created automatically if it does not exist.</span>

--fcm-credentials &lt;PATH_OR_JSON&gt;   (env: FCM_CREDENTIALS)
    <span class="cmt">FCM service account JSON. Accepts a file path or raw JSON string.</span>
    <span class="cmt">If the value starts with '{', treated as inline JSON.</span>

--apns-key-pem &lt;PATH_OR_PEM&gt;   (env: APNS_KEY_PEM)
    <span class="cmt">APNs .p8 private key. Accepts a file path or raw PEM string.</span>
    <span class="cmt">If the value starts with '-----BEGIN', treated as inline PEM.</span>

--apns-key-id &lt;ID&gt;   (env: APNS_KEY_ID)
    <span class="cmt">APNs key identifier from Apple Developer portal.</span>

--apns-team-id &lt;ID&gt;   (env: APNS_TEAM_ID)
    <span class="cmt">Apple Developer team identifier.</span>

--apns-bundle-id &lt;ID&gt;   (env: APNS_BUNDLE_ID)
    <span class="cmt">App bundle identifier (e.g., com.example.myapp).</span>

--apns-sandbox
    <span class="cmt">Use APNs sandbox endpoint instead of production.</span>

--push-debounce-secs &lt;SECS&gt;
    <span class="cmt">Push notification cooldown window. First fires immediately;</span>
    <span class="cmt">subsequent within window are batched. Default: 2</span>"##;

const CODE_CONN_ARGS: &str = r##"--idle-connection-timeout &lt;SECS&gt;
    <span class="cmt">Idle connection timeout in seconds. Must be longer than the</span>
    <span class="cmt">client keep-alive interval (default 90s) to prevent premature</span>
    <span class="cmt">disconnects. Default: 300</span>"##;

const CODE_EXTERNAL: &str = r##"<span class="cmt"># Single external address</span>
--external-address /ip4/77.37.125.212/tcp/4001

<span class="cmt"># Multiple addresses (e.g., IPv4 + IPv6)</span>
--external-address /ip4/77.37.125.212/tcp/4001 \
--external-address /ip6/2001:db8::1/tcp/4001

<span class="cmt"># In Docker, use the host's public IP, not the container IP</span>
<span class="cmt"># Docker's 172.x.x.x addresses are NOT reachable from outside</span>"##;

const CODE_IDENTITY: &str = r##"<span class="cmt"># Option 1: Identity file (auto-generated on first run)</span>
wavesync-relay --identity-file /data/identity.key

<span class="cmt"># Option 2: Base64 keypair (for containers, generated once)</span>
wavesync-relay --generate-identity
<span class="cmt"># Output: PeerId: 12D3KooW...  Keypair: CAESQ...</span>

<span class="cmt"># Then set as environment variable:</span>
IDENTITY_KEYPAIR=CAESQ... wavesync-relay

<span class="cmt"># Option 3: Docker volume (maps to identity-file)</span>
<span class="cmt"># volume mount at /data persists the auto-generated key</span>"##;

const CODE_DOCKER_COMPOSE: &str = r##"services:
  wavesync-relay:
    image: ghcr.io/pvg13/wavesync-relay:latest
    container_name: wavesync-relay
    restart: unless-stopped
    ports:
      - "4001:4001/tcp"
      - "4001:4001/udp"
    volumes:
      - relay-data:/data
    environment:
      <span class="cmt"># IDENTITY_KEYPAIR: "CAESQ..."  # from --generate-identity</span>
      PUSH_DB: /data/push_tokens.db
      EXTERNAL_ADDRESS: /ip4/YOUR_PUBLIC_IP/tcp/4001
      <span class="cmt"># FCM_CREDENTIALS: '{"type":"service_account",...}'</span>
      <span class="cmt"># APNS_KEY_PEM: "-----BEGIN PRIVATE KEY-----\nMIGT..."</span>
      <span class="cmt"># APNS_KEY_ID: "XXXXXX"</span>
      <span class="cmt"># APNS_TEAM_ID: "YYYYYY"</span>
      <span class="cmt"># APNS_BUNDLE_ID: "com.example.myapp"</span>

volumes:
  relay-data:"##;

const CODE_CLIENT: &str = r##"<span class="cmt">// Rust / Dioxus</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"secret"</span>)
    .<span class="fn">with_relay_server</span>(<span class="str">"/ip4/relay.example.com/tcp/4001"</span>)
    .<span class="fn">with_rendezvous_server</span>(<span class="str">"/ip4/relay.example.com/tcp/4001"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// React Native</span>
<span class="kw">await</span> <span class="fn">initialize</span>(<span class="str">'app.db'</span>, <span class="str">'my-topic'</span>, <span class="str">'secret'</span>, {
  relayAddr: <span class="str">'/ip4/relay.example.com/tcp/4001'</span>,
  rendezvousAddr: <span class="str">'/ip4/relay.example.com/tcp/4001'</span>,
});"##;

const CODE_LOGGING: &str = r##"<span class="cmt"># Default: info-level logs</span>
RUST_LOG=info wavesync-relay

<span class="cmt"># Detailed: debug-level for relay internals</span>
RUST_LOG=debug wavesync-relay

<span class="cmt"># Targeted: only relay and push events</span>
RUST_LOG=wavesync_relay=debug,libp2p_relay=info wavesync-relay"##;
