use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "WaveSyncDB Documentation" }

        p {
            "WaveSyncDB is a peer-to-peer SQLite sync library written in Rust. "
            "It acts as a drop-in replacement for your database connection \
             — every write replicates automatically to all peers on the network. "
            "Per-column CRDTs ensure concurrent edits to different fields both survive, "
            "HMAC authentication keeps sync groups private, and FCM/APNs push "
            "notifications wake sleeping mobile devices so they stay up to date."
        }

        p {
            "WaveSyncDB supports Rust (with first-class Dioxus integration), "
            "React Native (Android and iOS), and raw SQL via the connection wrapper. "
            "No server database, no sync API, no conflict resolution code required."
        }

        H2 { id: "quick-start", text: "Quick Start" }

        p {
            "Below are minimal examples for each supported platform. "
            "Each one sets up a synced database and performs a write that "
            "replicates to all connected peers."
        }

        H3 { id: "rust-dioxus", text: "Rust / Dioxus" }

        p {
            "Use the builder to create a synced connection, register your SeaORM entities, "
            "then write as normal."
        }

        CodeBlock { html: CODE_RUST }

        H3 { id: "react-native", text: "React Native" }

        p {
            "Initialize the native module, register your tables, and execute SQL. "
            "Changes sync automatically in the background."
        }

        CodeBlock { html: CODE_RN }

        H3 { id: "raw-sql", text: "Raw SQL" }

        p {
            "If you are not using an ORM, you can register tables manually "
            "and execute raw SQL statements through the synced connection."
        }

        CodeBlock { html: CODE_RAW }

        H2 { id: "next-steps", text: "Next Steps" }

        ul { class: "next-steps-list",
            li {
                a { href: "/docs/installation", "Installation" }
                " — add WaveSyncDB to your project"
            }
            li {
                a { href: "/docs/dioxus", "Dioxus Guide" }
                " — reactive hooks, lifecycle handling, and mobile integration"
            }
            li {
                a { href: "/docs/react-native", "React Native Guide" }
                " — setup, native linking, and bridging details"
            }
            li {
                a { href: "/docs/sync-protocol", "Sync Protocol" }
                " — how version vectors, changesets, and conflict resolution work under the hood"
            }
        }
    }
}

const CODE_RUST: &str = r##"<span class="cmt">// Build the synced database connection</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// Register SeaORM entities and start the sync engine</span>
db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;

<span class="cmt">// Write as normal — peers receive this automatically</span>
task::ActiveModel {
    id: <span class="fn">Set</span>(<span class="str">"task-1"</span>.<span class="fn">into</span>()),
    title: <span class="fn">Set</span>(<span class="str">"Buy milk"</span>.<span class="fn">into</span>()),
    done: <span class="fn">Set</span>(<span class="kw">false</span>),
    ..Default::default()
}.<span class="fn">insert</span>(&amp;db).<span class="kw">await</span>?;"##;

const CODE_RN: &str = r##"<span class="kw">import</span> { <span class="fn">initialize</span>, <span class="fn">registerSyncedTable</span>, <span class="fn">registryReady</span>, <span class="fn">execute</span> } <span class="kw">from</span> <span class="str">'@wavesync/react-native'</span>;

<span class="cmt">// Initialize with a database path, topic, and passphrase</span>
<span class="kw">await</span> <span class="fn">initialize</span>(<span class="str">'app.db'</span>, <span class="str">'my-topic'</span>, <span class="str">'shared-secret'</span>);

<span class="cmt">// Register tables you want to sync</span>
<span class="fn">registerSyncedTable</span>(<span class="str">'tasks'</span>, [<span class="str">'id'</span>, <span class="str">'title'</span>, <span class="str">'done'</span>]);
<span class="kw">await</span> <span class="fn">registryReady</span>();

<span class="cmt">// Write with standard SQL — syncs to all peers</span>
<span class="kw">await</span> <span class="fn">execute</span>(
  <span class="str">'INSERT INTO tasks (id, title, done) VALUES (?, ?, ?)'</span>,
  [<span class="str">'task-1'</span>, <span class="str">'Buy milk'</span>, <span class="num">0</span>]
);"##;

const CODE_RAW: &str = r##"<span class="cmt">// Build without an ORM — raw SQL mode</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// Register tables manually with column names</span>
db.<span class="fn">register_table</span>(<span class="str">"tasks"</span>, <span class="kw">vec!</span>[<span class="str">"id"</span>, <span class="str">"title"</span>, <span class="str">"done"</span>]).<span class="kw">await</span>?;

<span class="cmt">// Create the table if needed</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"CREATE TABLE IF NOT EXISTS tasks (id TEXT PRIMARY KEY, title TEXT, done INTEGER)"</span>
).<span class="kw">await</span>?;

<span class="cmt">// Insert with raw SQL — intercepted and synced</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"INSERT INTO tasks (id, title, done) VALUES ('task-1', 'Buy milk', 0)"</span>
).<span class="kw">await</span>?;"##;
