use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Sync Protocol" }

        p {
            "WaveSyncDB uses a peer-to-peer sync protocol built on libp2p. Every local "
            "write is intercepted at the SQL layer, enriched with CRDT metadata in shadow "
            "tables, and replicated to all connected peers. This page explains the full "
            "data flow from write to convergence."
        }

        H2 { id: "architecture", text: "Architecture Overview" }

        p {
            "The sync system has four layers:"
        }

        ul {
            li {
                "Write interception --- the WaveSyncDb connection wrapper intercepts "
                "INSERT, UPDATE, and DELETE at the raw SQL level"
            }
            li {
                "Shadow tables --- per-column CRDT metadata stored alongside your data "
                "in the same SQLite database"
            }
            li {
                "Changeset production --- intercepted writes are transformed into "
                "SyncChangeset messages and sent to the engine"
            }
            li {
                "P2P engine --- a libp2p swarm that discovers peers and exchanges "
                "changesets via request-response protocols"
            }
        }

        H2 { id: "flow-diagram", text: "Full Sync Flow" }

        CodeBlock { html: FLOW_DIAGRAM }

        H2 { id: "real-time", text: "Real-Time Path (Fan-Out)" }

        p {
            "Every local write follows this path to reach all connected peers:"
        }

        H3 { id: "write-interception", text: "1. Write Interception" }

        p {
            "WaveSyncDb implements SeaORM's ConnectionTrait. All four methods that can "
            "produce writes are intercepted: execute_raw, execute_unprepared, query_one_raw, "
            "and query_all_raw. The last two matter because SeaORM generates "
            "INSERT ... RETURNING statements that route through query methods."
        }

        p {
            "The SQL is passed to classify_write() which determines if it is an INSERT, "
            "UPDATE, or DELETE on a registered table. Table names are normalized (double "
            "quotes stripped) before registry lookup. SELECTs and writes to unregistered "
            "tables pass through without interception."
        }

        H3 { id: "shadow-upsert", text: "2. Shadow Table Upsert" }

        p {
            "For each affected column, dispatch_sync performs an INSERT OR REPLACE into "
            "the shadow clock table. The shadow entry tracks:"
        }

        ul {
            li { "pk --- primary key of the affected row" }
            li { "cid --- column name (or __deleted for tombstones)" }
            li { "col_version --- Lamport clock for this specific column" }
            li { "db_version --- monotonically increasing local version counter" }
            li { "site_id --- this node's unique identifier" }
            li { "seq --- ordering within a single db_version batch" }
        }

        p {
            "The db_version is incremented and persisted to _wavesync_meta on every write. "
            "This is critical --- if db_version were only in memory, a restart would reset "
            "it and peers would skip sending needed changes."
        }

        H3 { id: "changeset-fanout", text: "3. Changeset Fan-Out" }

        p {
            "After the shadow upsert, a SyncChangeset is created containing all "
            "ColumnChange entries for the write. This is sent via mpsc channel to the "
            "engine task, which forwards it to every connected peer using the push "
            "request-response protocol."
        }

        CodeBlock { html: CODE_PUSH }

        p {
            "Each peer verifies the HMAC (if a passphrase is configured), checks the "
            "topic matches, then applies changes via apply_remote_changeset(). The "
            "conflict resolution algorithm (see Conflict Resolution page) determines "
            "whether each column change is applied or rejected."
        }

        H2 { id: "catch-up", text: "Catch-Up Path (Version Vector)" }

        p {
            "The catch-up path handles two scenarios: initial sync when a new peer joins, "
            "and recovering missed changes after a disconnect. It runs on peer discovery "
            "and every 30 seconds (configurable via with_sync_interval)."
        }

        H3 { id: "version-vector", text: "Version Vector Exchange" }

        p {
            "A single round trip synchronizes state between two peers:"
        }

        CodeBlock { html: CODE_CATCHUP }

        p {
            "The key fields in a VersionVector request:"
        }

        ul {
            li {
                "my_db_version --- the requester's current local version"
            }
            li {
                "your_last_db_version --- the last version the requester received from "
                "this peer (read from _wavesync_peer_versions). A value of 0 means "
                "\"I have never synced with you --- send me everything.\""
            }
            li { "site_id --- the requester's node identity" }
            li { "topic --- the derived topic string for group isolation" }
            li { "hmac --- BLAKE3 keyed MAC over the message content" }
        }

        p {
            "The responder queries its shadow tables for all changes where db_version is "
            "greater than your_last_db_version and returns them in a ChangesetResponse. "
            "Both peers update their _wavesync_peer_versions table with the latest known "
            "version for each other."
        }

        H2 { id: "wire-format", text: "Wire Format" }

        p {
            "Messages are serialized as JSON and length-prefixed on the wire:"
        }

        ul {
            li {
                "Snapshot protocol (version vector sync): 4-byte big-endian length prefix"
            }
            li {
                "Push protocol (real-time fan-out): 4-byte big-endian length prefix"
            }
            li {
                "Auth protocol (challenge-response): 4-byte little-endian length prefix"
            }
        }

        p {
            "The length prefix endianness must match between peers. Mismatched endianness "
            "causes silent deserialization failures."
        }

        H2 { id: "protocol-ids", text: "Protocol Identifiers" }

        p {
            "Each protocol has a versioned identifier string used during libp2p protocol "
            "negotiation. Peers running different protocol versions will fail to negotiate "
            "silently --- there is no version negotiation fallback."
        }

        CodeBlock { html: CODE_PROTOCOLS }

        H2 { id: "shadow-tables", text: "Shadow Table Structure" }

        p {
            "WaveSyncDB creates two types of internal tables, all prefixed with _wavesync "
            "to prevent sync loops:"
        }

        H3 { id: "meta-table", text: "_wavesync_meta" }

        p {
            "Stores the node's identity and version counter:"
        }

        CodeBlock { html: CODE_META }

        H3 { id: "clock-table", text: "_wavesync_{{table}}_clock" }

        p {
            "One clock table per synced user table. Stores per-column Lamport clocks:"
        }

        CodeBlock { html: CODE_CLOCK }

        H3 { id: "peer-versions", text: "_wavesync_peer_versions" }

        p {
            "Tracks the last known db_version for each peer, enabling efficient catch-up "
            "sync (only changes since the last known version are exchanged):"
        }

        CodeBlock { html: CODE_PEERS }

        H2 { id: "internal-guards", text: "Internal Guards" }

        p {
            "Two safety mechanisms prevent pathological behavior:"
        }

        ul {
            li {
                "The _wavesync prefix check in dispatch_sync is the first guard, before "
                "any other logic. Shadow table writes are themselves database operations --- "
                "without this check they would trigger more shadow writes in an infinite loop."
            }
            li {
                "ChangeNotification is only sent after the shadow table write transaction "
                "has committed. If sent before, subscribers re-query and read stale data."
            }
        }
    }
}

const FLOW_DIAGRAM: &str = r##"<span class="cmt">                     Local Write Flow
    ┌─────────────────────────────────────────────────┐
    │  App code: INSERT INTO tasks (id, title, done)  │
    │            VALUES ('t1', 'Buy milk', 0)         │
    └──────────────────────┬──────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │ classify_   │
                    │ write()     │
                    │ → Insert,   │
                    │   "tasks"   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────────┐
                    │ parse_write_    │
                    │ full()          │
                    │ → pk, columns,  │
                    │   values        │
                    └──────┬──────────┘
                           │
                    ┌──────▼──────────┐
                    │ dispatch_sync() │
                    │ 1. Check not    │
                    │    _wavesync    │
                    │ 2. Shadow       │
                    │    upsert       │
                    │ 3. Increment    │
                    │    db_version   │
                    └──────┬──────────┘
                           │
              ┌────────────▼────────────┐
              │     SyncChangeset       │
              │  site_id, db_version,   │
              │  changes: [ColumnChange]│
              └────────────┬────────────┘
                           │
                    mpsc channel
                           │
              ┌────────────▼────────────┐
              │      P2P Engine         │
              │  for each connected     │
              │  peer: send Push req    │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │     Remote Peer         │
              │  verify HMAC + topic    │
              │  apply_remote_          │
              │  changeset()            │
              │  → conflict resolution  │
              │  → apply or reject      │
              └─────────────────────────┘</span>"##;

const CODE_PUSH: &str = r##"<span class="cmt">// SyncRequest::Push — sent for every local write</span>
{
  <span class="str">"Push"</span>: {
    <span class="str">"changeset"</span>: {
      <span class="str">"site_id"</span>: [<span class="num">1</span>,<span class="num">2</span>,<span class="num">3</span>, ...],
      <span class="str">"db_version"</span>: <span class="num">42</span>,
      <span class="str">"changes"</span>: [
        {
          <span class="str">"table"</span>: <span class="str">"tasks"</span>,
          <span class="str">"pk"</span>: <span class="str">"t1"</span>,
          <span class="str">"cid"</span>: <span class="str">"title"</span>,
          <span class="str">"val"</span>: <span class="str">"Buy milk"</span>,
          <span class="str">"site_id"</span>: [<span class="num">1</span>,<span class="num">2</span>,<span class="num">3</span>, ...],
          <span class="str">"col_version"</span>: <span class="num">1</span>,
          <span class="str">"cl"</span>: <span class="num">1</span>,
          <span class="str">"seq"</span>: <span class="num">0</span>,
          <span class="str">"db_version"</span>: <span class="num">42</span>
        }
      ]
    },
    <span class="str">"topic"</span>: <span class="str">"wavesync-a1b2c3..."</span>,
    <span class="str">"hmac"</span>: [<span class="num">171</span>, <span class="num">205</span>, ...]
  }
}"##;

const CODE_CATCHUP: &str = r##"<span class="cmt">// Step 1: A sends VersionVector to B</span>
A → B: SyncRequest::VersionVector {
    my_db_version: <span class="num">42</span>,         <span class="cmt">// A's current version</span>
    your_last_db_version: <span class="num">37</span>,  <span class="cmt">// Last version A got from B</span>
    site_id: [<span class="num">1</span>,<span class="num">1</span>,<span class="num">1</span>,...],
    topic: <span class="str">"wavesync-a1b2c3..."</span>,
    hmac: Some([...])
}

<span class="cmt">// Step 2: B responds with changes since version 37</span>
B → A: SyncResponse::ChangesetResponse {
    changes: [<span class="cmt">/* all ColumnChanges where db_version &gt; 37 */</span>],
    my_db_version: <span class="num">50</span>,         <span class="cmt">// B's current version</span>
    your_last_db_version: <span class="num">42</span>,  <span class="cmt">// Echoed back so A can update</span>
    site_id: [<span class="num">2</span>,<span class="num">2</span>,<span class="num">2</span>,...],
    topic: <span class="str">"wavesync-a1b2c3..."</span>,
    hmac: Some([...])
}

<span class="cmt">// Step 3: A applies changes and updates peer tracking</span>
A: apply_remote_changeset(changes)
A: _wavesync_peer_versions[B] = <span class="num">50</span>"##;

const CODE_PROTOCOLS: &str = r##"<span class="cmt">// Protocol identifier strings (used in libp2p negotiation)</span>
<span class="str">"/wavesync/snapshot/3.0.0"</span>  <span class="cmt">// Version vector sync</span>
<span class="str">"/wavesync/push/1.0.0"</span>      <span class="cmt">// Real-time changeset fan-out</span>
<span class="str">"/wavesync/auth/challenge/1.0.0"</span>  <span class="cmt">// Auth challenge-response</span>

<span class="cmt">// Breaking changes to SyncRequest, SyncResponse, SyncChangeset,</span>
<span class="cmt">// or ColumnChange require updating the protocol version string.</span>
<span class="cmt">// Peers running different versions fail silently — there is no</span>
<span class="cmt">// version negotiation.</span>"##;

const CODE_META: &str = r##"<span class="cmt">-- _wavesync_meta: one row, always present</span>
<span class="kw">CREATE TABLE</span> _wavesync_meta (
    key   TEXT <span class="kw">PRIMARY KEY</span>,
    value TEXT <span class="kw">NOT NULL</span>
);

<span class="cmt">-- Contains two entries:</span>
<span class="cmt">-- ('db_version', '42')   — monotonically increasing write counter</span>
<span class="cmt">-- ('site_id', '0102...')  — 16-byte node ID as hex</span>"##;

const CODE_CLOCK: &str = r##"<span class="cmt">-- One clock table per synced table (e.g. _wavesync_tasks_clock)</span>
<span class="kw">CREATE TABLE</span> _wavesync_tasks_clock (
    pk          TEXT    <span class="kw">NOT NULL</span>,
    cid         TEXT    <span class="kw">NOT NULL</span>,
    col_version INTEGER <span class="kw">NOT NULL DEFAULT</span> <span class="num">0</span>,
    db_version  INTEGER <span class="kw">NOT NULL DEFAULT</span> <span class="num">0</span>,
    site_id     BLOB    <span class="kw">NOT NULL</span>,
    seq         INTEGER <span class="kw">NOT NULL DEFAULT</span> <span class="num">0</span>,
    <span class="kw">PRIMARY KEY</span> (pk, cid)
);

<span class="cmt">-- (pk, cid) is the primary key — INSERT OR REPLACE upserts in place.</span>
<span class="cmt">-- No history accumulation, no compaction needed.</span>
<span class="cmt">-- cid = "__deleted" is the tombstone sentinel for delete tracking.</span>"##;

const CODE_PEERS: &str = r##"<span class="cmt">-- Tracks last known version per peer</span>
<span class="kw">CREATE TABLE</span> _wavesync_peer_versions (
    peer_id     TEXT    <span class="kw">PRIMARY KEY</span>,
    db_version  INTEGER <span class="kw">NOT NULL DEFAULT</span> <span class="num">0</span>,
    last_seen   TEXT
);

<span class="cmt">-- db_version=0 means "never synced" — triggers full sync</span>"##;
