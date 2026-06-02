use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Network Status and Events" }

        p {
            "WaveSyncDB exposes the P2P engine's internal state through a set of "
            "observable types. Use these to build connection status indicators, peer "
            "lists, and debug views in your application."
        }

        H2 { id: "network-status", text: "NetworkStatus" }

        p {
            "A snapshot of the full network state. Obtained via db.network_status() "
            "(Rust) or networkStatus() (React Native). This is a read-only copy --- "
            "the engine is the sole writer."
        }

        CodeBlock { html: CODE_STATUS_STRUCT }

        H3 { id: "helper-methods", text: "Helper Methods" }

        CodeBlock { html: CODE_STATUS_METHODS }

        H2 { id: "peer-info", text: "PeerInfo" }

        p {
            "Information about a single connected peer:"
        }

        CodeBlock { html: CODE_PEER_INFO }

        H2 { id: "relay-status", text: "RelayStatus" }

        p {
            "Tracks the relay connection lifecycle. The progression is linear:"
        }

        CodeBlock { html: CODE_RELAY_STATUS }

        p {
            "Note: circuit relay listening is deferred until AutoNAT confirms the "
            "node is behind NAT (NatStatus::Private). Starting the listen before "
            "confirmation causes relay reservation failures."
        }

        H2 { id: "nat-status", text: "NatStatus" }

        CodeBlock { html: CODE_NAT_STATUS }

        H2 { id: "network-event", text: "NetworkEvent" }

        p {
            "Events emitted when network state changes. Subscribe via "
            "db.network_event_rx() to receive these asynchronously."
        }

        CodeBlock { html: CODE_EVENTS }

        H2 { id: "rust-usage", text: "Rust Usage" }

        H3 { id: "rust-snapshot", text: "Snapshot" }

        p {
            "Get a point-in-time snapshot of network state:"
        }

        CodeBlock { html: CODE_RUST_SNAPSHOT }

        H3 { id: "rust-stream", text: "Reactive Stream" }

        p {
            "Subscribe to events for real-time updates:"
        }

        CodeBlock { html: CODE_RUST_STREAM }

        H2 { id: "dioxus-hooks", text: "Dioxus Hooks" }

        p {
            "Dioxus provides reactive hooks that re-render components when network "
            "state changes:"
        }

        CodeBlock { html: CODE_DIOXUS }

        H2 { id: "react-native", text: "React Native" }

        CodeBlock { html: CODE_RN }

        H2 { id: "peer-identity", text: "Peer Identity" }

        p {
            "Applications can announce a custom identity string to peers. This is "
            "useful for distinguishing different app versions or user-facing names "
            "in a peer list."
        }

        CodeBlock { html: CODE_IDENTITY }

        p {
            "Peer identities are ephemeral and session-scoped. They are not persisted "
            "and are lost on disconnect. A peer must re-announce its identity on each "
            "new connection."
        }

        H2 { id: "tips", text: "Practical Tips" }

        ul {
            li {
                "Use group_peers() to show only peers in the same sync group. "
                "connected_peers includes non-group peers that haven't been verified yet."
            }
            li {
                "Check registry_ready before showing sync status --- the engine may be "
                "connected but not yet able to sync if the schema hasn't been registered."
            }
            li {
                "Use the PeerSynced event to show \"last synced\" timestamps in the UI."
            }
            li {
                "EngineStarted fires after the first network status update. If you "
                "subscribe after this event, do an initial read of network_status() to "
                "get the current state."
            }
        }
    }
}

const CODE_STATUS_STRUCT: &str = r##"<span class="kw">pub struct</span> <span class="fn">NetworkStatus</span> {
    <span class="cmt">/// This node's libp2p PeerId</span>
    <span class="kw">pub</span> local_peer_id: PeerId,
    <span class="cmt">/// All currently connected peers</span>
    <span class="kw">pub</span> connected_peers: Vec&lt;PeerInfo&gt;,
    <span class="cmt">/// The effective sync topic (derived from passphrase)</span>
    <span class="kw">pub</span> topic: String,
    <span class="cmt">/// Relay connection status</span>
    <span class="kw">pub</span> relay_status: RelayStatus,
    <span class="cmt">/// Detected NAT status</span>
    <span class="kw">pub</span> nat_status: NatStatus,
    <span class="cmt">/// Whether registered with rendezvous server</span>
    <span class="kw">pub</span> rendezvous_registered: bool,
    <span class="cmt">/// Whether push token registered with relay</span>
    <span class="kw">pub</span> push_registered: bool,
    <span class="cmt">/// Current local db_version counter</span>
    <span class="kw">pub</span> local_db_version: u64,
    <span class="cmt">/// Whether the schema registry is ready</span>
    <span class="kw">pub</span> registry_ready: bool,
}"##;

const CODE_STATUS_METHODS: &str = r##"<span class="kw">let</span> status = db.<span class="fn">network_status</span>();

<span class="cmt">// Only peers verified as group members (same topic/passphrase)</span>
<span class="kw">let</span> group = status.<span class="fn">group_peers</span>();        <span class="cmt">// Vec&lt;&amp;PeerInfo&gt;</span>
<span class="kw">let</span> count = status.<span class="fn">group_peer_count</span>();   <span class="cmt">// usize</span>

<span class="cmt">// All connected peers (including unverified)</span>
<span class="kw">let</span> total = status.<span class="fn">connected_peer_count</span>(); <span class="cmt">// usize</span>"##;

const CODE_PEER_INFO: &str = r##"<span class="kw">pub struct</span> <span class="fn">PeerInfo</span> {
    <span class="cmt">/// Opaque peer identifier</span>
    <span class="kw">pub</span> peer_id: PeerId,
    <span class="cmt">/// Multiaddr as string (e.g., "/ip4/192.168.1.5/tcp/45000")</span>
    <span class="kw">pub</span> address: String,
    <span class="cmt">/// Last-known db_version from this peer (None if no sync yet)</span>
    <span class="kw">pub</span> db_version: Option&lt;u64&gt;,
    <span class="cmt">/// Whether this peer was configured as a bootstrap peer</span>
    <span class="kw">pub</span> is_bootstrap: bool,
    <span class="cmt">/// Whether this peer is a verified group member</span>
    <span class="kw">pub</span> is_group_member: bool,
    <span class="cmt">/// Application-defined identity (ephemeral, session-scoped)</span>
    <span class="kw">pub</span> app_id: Option&lt;String&gt;,
}"##;

const CODE_RELAY_STATUS: &str = r##"<span class="kw">pub enum</span> <span class="fn">RelayStatus</span> {
    Disabled,    <span class="cmt">// No relay server configured</span>
    Connecting,  <span class="cmt">// Dialing the relay server</span>
    Connected,   <span class="cmt">// TCP/QUIC connection established</span>
    Listening,   <span class="cmt">// Circuit reservation accepted (after AutoNAT)</span>
}

<span class="cmt">// Progression: Disabled → Connecting → Connected → Listening</span>
<span class="cmt">// Listening is only reached after NatStatus::Private is confirmed</span>"##;

const CODE_NAT_STATUS: &str = r##"<span class="kw">pub enum</span> <span class="fn">NatStatus</span> {
    Unknown,  <span class="cmt">// NAT detection not yet completed</span>
    Public,   <span class="cmt">// Publicly reachable (no relay needed for inbound)</span>
    Private,  <span class="cmt">// Behind NAT (relay circuit used for inbound)</span>
}"##;

const CODE_EVENTS: &str = r##"<span class="kw">pub enum</span> <span class="fn">NetworkEvent</span> {
    <span class="cmt">/// A new peer connected</span>
    PeerConnected(PeerInfo),

    <span class="cmt">/// A peer disconnected</span>
    PeerDisconnected(PeerId),

    <span class="cmt">/// A peer was rejected (topic/passphrase mismatch)</span>
    PeerRejected(PeerId),

    <span class="cmt">/// A peer was verified via successful HMAC exchange</span>
    PeerVerified(PeerId),

    <span class="cmt">/// A peer announced its application-level identity</span>
    PeerIdentityReceived { peer_id: PeerId, app_id: String },

    <span class="cmt">/// Relay status changed</span>
    RelayStatusChanged(RelayStatus),

    <span class="cmt">/// NAT detection status changed</span>
    NatStatusChanged(NatStatus),

    <span class="cmt">/// Rendezvous registration status changed</span>
    RendezvousStatusChanged { registered: bool },

    <span class="cmt">/// Version vector sync completed with a peer</span>
    PeerSynced { peer_id: PeerId, db_version: u64 },

    <span class="cmt">/// Engine started, initial status available</span>
    EngineStarted,

    <span class="cmt">/// Engine failed with error or panic</span>
    EngineFailed { reason: String },
}"##;

const CODE_RUST_SNAPSHOT: &str = r##"<span class="kw">let</span> status = db.<span class="fn">network_status</span>();

<span class="fn">println!</span>(<span class="str">"Local peer: {}"</span>, status.local_peer_id);
<span class="fn">println!</span>(<span class="str">"Connected: {} peers"</span>, status.<span class="fn">connected_peer_count</span>());
<span class="fn">println!</span>(<span class="str">"Group: {} peers"</span>, status.<span class="fn">group_peer_count</span>());
<span class="fn">println!</span>(<span class="str">"Relay: {:?}"</span>, status.relay_status);
<span class="fn">println!</span>(<span class="str">"NAT: {:?}"</span>, status.nat_status);
<span class="fn">println!</span>(<span class="str">"db_version: {}"</span>, status.local_db_version);"##;

const CODE_RUST_STREAM: &str = r##"<span class="kw">let mut</span> rx = db.<span class="fn">network_event_rx</span>();

tokio::<span class="fn">spawn</span>(<span class="kw">async move</span> {
    <span class="kw">while let</span> Ok(event) = rx.<span class="fn">recv</span>().<span class="kw">await</span> {
        <span class="kw">match</span> event {
            NetworkEvent::PeerConnected(info) =&gt; {
                <span class="fn">println!</span>(<span class="str">"Connected: {} at {}"</span>, info.peer_id, info.address);
            }
            NetworkEvent::PeerVerified(id) =&gt; {
                <span class="fn">println!</span>(<span class="str">"Verified group member: {id}"</span>);
            }
            NetworkEvent::PeerSynced { peer_id, db_version } =&gt; {
                <span class="fn">println!</span>(<span class="str">"Synced with {peer_id} at version {db_version}"</span>);
            }
            NetworkEvent::RelayStatusChanged(status) =&gt; {
                <span class="fn">println!</span>(<span class="str">"Relay: {status:?}"</span>);
            }
            _ =&gt; {}
        }
    }
});"##;

const CODE_DIOXUS: &str = r##"<span class="kw">use</span> wavesyncdb::dioxus::hooks::*;

<span class="kw">fn</span> <span class="fn">StatusBar</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

    <span class="cmt">// Reactive signal — re-renders on any network state change</span>
    <span class="kw">let</span> status = <span class="fn">use_network_status</span>(db.<span class="fn">clone</span>());

    <span class="cmt">// Reactive peer identity map</span>
    <span class="kw">let</span> identities = <span class="fn">use_peer_identities</span>(db);

    rsx! {
        div {
            p { <span class="str">"Peers: {status().group_peer_count()}"</span> }
            p { <span class="str">"Relay: {status().relay_status:?}"</span> }
            p { <span class="str">"db_version: {status().local_db_version}"</span> }

            <span class="kw">for</span> (app_id, peers) <span class="kw">in</span> identities() {
                p { <span class="str">"{app_id}: {peers.len()} peers"</span> }
            }
        }
    }
}"##;

const CODE_RN: &str = r##"<span class="kw">import</span> {
  <span class="fn">networkStatus</span>,
  <span class="fn">subscribeNetworkEvents</span>,
  <span class="fn">onNetworkEvent</span>,
} <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;

<span class="cmt">// Snapshot</span>
<span class="kw">const</span> status = <span class="kw">await</span> <span class="fn">networkStatus</span>();
console.<span class="fn">log</span>(<span class="str">'Peers:'</span>, status.connected_peers.length);
console.<span class="fn">log</span>(<span class="str">'Relay:'</span>, status.relay_status);

<span class="cmt">// Subscribe to all events (returns unsubscribe function)</span>
<span class="kw">const</span> unsubscribe = <span class="fn">subscribeNetworkEvents</span>((event) =&gt; {
  console.<span class="fn">log</span>(<span class="str">'Event:'</span>, event.type, event.data);
});

<span class="cmt">// Subscribe to a specific event type</span>
<span class="kw">const</span> unsub = <span class="fn">onNetworkEvent</span>(<span class="str">'PeerSynced'</span>, (data) =&gt; {
  console.<span class="fn">log</span>(<span class="str">'Synced with'</span>, data.peer_id, <span class="str">'at version'</span>, data.db_version);
});"##;

const CODE_IDENTITY: &str = r##"<span class="cmt">// Set your app's identity (announced to all verified peers)</span>
db.<span class="fn">set_peer_identity</span>(<span class="str">"roommates-v2.1"</span>);

<span class="cmt">// Get peers grouped by their announced identity</span>
<span class="kw">let</span> groups = db.<span class="fn">peers_by_identity</span>();
<span class="cmt">// HashMap&lt;String, Vec&lt;PeerId&gt;&gt;</span>
<span class="cmt">// e.g., {"roommates-v2.1": [peer1, peer2], "roommates-v2.0": [peer3]}</span>

<span class="cmt">// Clear identity (stop announcing)</span>
db.<span class="fn">clear_peer_identity</span>();"##;
