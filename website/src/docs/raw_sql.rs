use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Raw SQL (No ORM)" }

        p {
            "WaveSyncDB does not require an ORM. You can build a synced database connection "
            "directly and use raw SQL statements. Write interception happens at the connection "
            "level --- every INSERT, UPDATE, and DELETE is automatically captured and synced."
        }

        H2 { id: "setup", text: "Basic Setup" }

        p {
            "Build a WaveSyncDb with WaveSyncDbBuilder, create your tables with raw SQL, "
            "register them for sync, and call registry_ready() to start the engine."
        }

        CodeBlock { html: CODE_SETUP }

        H2 { id: "register-table", text: "Registering Tables" }

        p {
            "Each table you want to sync must be registered with its column names. "
            "Use register_table() which takes a TableMeta describing the table name, "
            "primary key column, and all column names."
        }

        CodeBlock { html: CODE_REGISTER }

        p {
            "After registering all tables, call registry_ready() to signal the engine "
            "that the schema is complete. Shadow tables are created automatically during "
            "sync() or you can create them manually with shadow::create_shadow_table."
        }

        H2 { id: "shadow-tables", text: "Shadow Tables" }

        p {
            "Shadow tables store CRDT metadata for each synced table. If you are not "
            "using the schema builder (which creates them automatically), you need to "
            "create them manually:"
        }

        CodeBlock { html: CODE_SHADOW }

        H2 { id: "writing", text: "Writing Data" }

        p {
            "Use execute_unprepared for raw SQL writes. WaveSyncDB intercepts these "
            "calls, parses the SQL, creates CRDT metadata in shadow tables, and pushes "
            "the changes to connected peers --- all transparently."
        }

        CodeBlock { html: CODE_WRITE }

        p {
            "The SQL parser handles standard INSERT, UPDATE, and DELETE statements. "
            "Table names may be double-quoted (as SeaORM generates them) and are "
            "normalized automatically."
        }

        H2 { id: "reading", text: "Reading Data" }

        p {
            "Read operations go through the same connection and return standard SeaORM "
            "query results. SELECT statements are never intercepted or modified."
        }

        CodeBlock { html: CODE_READ }

        H2 { id: "change-notifications", text: "Listening for Changes" }

        p {
            "Subscribe to the change_rx() broadcast channel to receive notifications "
            "when any synced table is modified, either by local writes or remote peers."
        }

        CodeBlock { html: CODE_CHANGES }

        H2 { id: "full-example", text: "Full Example: Synced Key-Value Store" }

        p {
            "A complete example showing a simple CLI app that syncs a key-value store "
            "between peers on the local network:"
        }

        CodeBlock { html: CODE_FULL }
    }
}

const CODE_SETUP: &str = r##"<span class="kw">use</span> wavesyncdb::{WaveSyncDbBuilder, registry::TableMeta, shadow};

<span class="cmt">// Build the synced database connection</span>
<span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:app.db"</span>, <span class="str">"my-topic"</span>)
    .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
    .<span class="fn">build</span>().<span class="kw">await</span>?;

<span class="cmt">// Create the table</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"CREATE TABLE IF NOT EXISTS kv_store (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )"</span>
).<span class="kw">await</span>?;

<span class="cmt">// Register for sync</span>
db.<span class="fn">register_table</span>(TableMeta {
    table_name: <span class="str">"kv_store"</span>.<span class="fn">into</span>(),
    pk_column: <span class="str">"key"</span>.<span class="fn">to_string</span>(),
    columns: <span class="kw">vec!</span>[<span class="str">"key"</span>.<span class="fn">into</span>(), <span class="str">"value"</span>.<span class="fn">into</span>()],
    delete_policy: Default::default(),
});

<span class="cmt">// Create shadow table for CRDT metadata</span>
shadow::<span class="fn">create_shadow_table</span>(db.<span class="fn">inner</span>(), <span class="str">"kv_store"</span>).<span class="kw">await</span>?;

<span class="cmt">// Signal that schema registration is complete</span>
db.<span class="fn">registry_ready</span>();"##;

const CODE_REGISTER: &str = r##"<span class="kw">use</span> wavesyncdb::registry::TableMeta;
<span class="kw">use</span> wavesyncdb::messages::DeletePolicy;

<span class="cmt">// Register a table with custom delete policy</span>
db.<span class="fn">register_table</span>(TableMeta {
    table_name: <span class="str">"tasks"</span>.<span class="fn">into</span>(),
    pk_column: <span class="str">"id"</span>.<span class="fn">to_string</span>(),
    columns: <span class="kw">vec!</span>[
        <span class="str">"id"</span>.<span class="fn">into</span>(),
        <span class="str">"title"</span>.<span class="fn">into</span>(),
        <span class="str">"done"</span>.<span class="fn">into</span>(),
    ],
    <span class="cmt">// DeleteWins (default): deletes override concurrent edits</span>
    <span class="cmt">// AddWins: concurrent edits resurrect deleted rows</span>
    delete_policy: DeletePolicy::DeleteWins,
});"##;

const CODE_SHADOW: &str = r##"<span class="kw">use</span> wavesyncdb::shadow;

<span class="cmt">// Creates _wavesync_kv_store_clock table with columns:</span>
<span class="cmt">//   pk TEXT, cid TEXT, col_version INTEGER, db_version INTEGER,</span>
<span class="cmt">//   site_id BLOB, seq INTEGER</span>
<span class="cmt">//   PRIMARY KEY (pk, cid)</span>
shadow::<span class="fn">create_shadow_table</span>(db.<span class="fn">inner</span>(), <span class="str">"kv_store"</span>).<span class="kw">await</span>?;

<span class="cmt">// The meta table (_wavesync_meta) is created automatically</span>
<span class="cmt">// by WaveSyncDbBuilder::build() and stores db_version + site_id</span>"##;

const CODE_WRITE: &str = r##"<span class="cmt">// INSERT — intercepted and synced to all peers</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"INSERT INTO kv_store (key, value) VALUES ('color', 'blue')"</span>
).<span class="kw">await</span>?;

<span class="cmt">// UPDATE — intercepted and synced</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"UPDATE kv_store SET value = 'red' WHERE key = 'color'"</span>
).<span class="kw">await</span>?;

<span class="cmt">// DELETE — intercepted, tombstone created, synced</span>
db.<span class="fn">execute_unprepared</span>(
    <span class="str">"DELETE FROM kv_store WHERE key = 'color'"</span>
).<span class="kw">await</span>?;"##;

const CODE_READ: &str = r##"<span class="kw">use</span> sea_orm::{ConnectionTrait, Statement, DatabaseBackend};

<span class="cmt">// Query all rows</span>
<span class="kw">let</span> results = db.<span class="fn">query_all_raw</span>(
    DatabaseBackend::Sqlite,
    Statement::from_string(
        DatabaseBackend::Sqlite,
        <span class="str">"SELECT key, value FROM kv_store"</span>.<span class="fn">to_string</span>(),
    ),
).<span class="kw">await</span>?;

<span class="kw">for</span> row <span class="kw">in</span> &amp;results {
    <span class="kw">let</span> key: String = row.<span class="fn">try_get_by_index</span>(<span class="num">0</span>)?;
    <span class="kw">let</span> value: String = row.<span class="fn">try_get_by_index</span>(<span class="num">1</span>)?;
    <span class="fn">println!</span>(<span class="str">"{key} = {value}"</span>);
}"##;

const CODE_CHANGES: &str = r##"<span class="kw">use</span> wavesyncdb::messages::ChangeNotification;

<span class="cmt">// Subscribe to changes (local and remote)</span>
<span class="kw">let mut</span> rx = db.<span class="fn">change_rx</span>();

tokio::<span class="fn">spawn</span>(<span class="kw">async move</span> {
    <span class="kw">loop</span> {
        <span class="kw">match</span> rx.<span class="fn">recv</span>().<span class="kw">await</span> {
            Ok(notification) =&gt; {
                <span class="fn">println!</span>(
                    <span class="str">"Change: {} {:?} pk={}"</span>,
                    notification.table,
                    notification.kind,
                    notification.primary_key,
                );
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) =&gt; {
                <span class="cmt">// Missed some notifications — re-query to catch up</span>
                <span class="fn">println!</span>(<span class="str">"Missed {n} notifications, re-querying..."</span>);
            }
            Err(_) =&gt; <span class="kw">break</span>,
        }
    }
});"##;

const CODE_FULL: &str = r##"<span class="kw">use</span> wavesyncdb::{WaveSyncDbBuilder, registry::TableMeta, shadow};
<span class="kw">use</span> sea_orm::{ConnectionTrait, Statement, DatabaseBackend};
<span class="kw">use</span> std::io::{self, BufRead};

#[tokio::main]
<span class="kw">async fn</span> <span class="fn">main</span>() -&gt; anyhow::Result&lt;()&gt; {
    <span class="cmt">// Build synced database</span>
    <span class="kw">let</span> db = <span class="fn">WaveSyncDbBuilder</span>::<span class="fn">new</span>(<span class="str">"sqlite:kvstore.db"</span>, <span class="str">"kv-demo"</span>)
        .<span class="fn">with_passphrase</span>(<span class="str">"demo-password"</span>)
        .<span class="fn">build</span>().<span class="kw">await</span>?;

    <span class="cmt">// Create table</span>
    db.<span class="fn">execute_unprepared</span>(
        <span class="str">"CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)"</span>
    ).<span class="kw">await</span>?;

    <span class="cmt">// Register and start sync</span>
    db.<span class="fn">register_table</span>(TableMeta {
        table_name: <span class="str">"kv"</span>.<span class="fn">into</span>(),
        pk_column: <span class="str">"key"</span>.<span class="fn">to_string</span>(),
        columns: <span class="kw">vec!</span>[<span class="str">"key"</span>.<span class="fn">into</span>(), <span class="str">"value"</span>.<span class="fn">into</span>()],
        delete_policy: Default::default(),
    });
    shadow::<span class="fn">create_shadow_table</span>(db.<span class="fn">inner</span>(), <span class="str">"kv"</span>).<span class="kw">await</span>?;
    db.<span class="fn">registry_ready</span>();

    <span class="cmt">// Listen for remote changes in background</span>
    <span class="kw">let mut</span> rx = db.<span class="fn">change_rx</span>();
    tokio::<span class="fn">spawn</span>(<span class="kw">async move</span> {
        <span class="kw">while let</span> Ok(n) = rx.<span class="fn">recv</span>().<span class="kw">await</span> {
            <span class="fn">println!</span>(<span class="str">"[sync] {} {:?} pk={}"</span>, n.table, n.kind, n.primary_key);
        }
    });

    <span class="cmt">// Simple REPL: "set key value" or "get key"</span>
    <span class="fn">println!</span>(<span class="str">"Commands: set &lt;key&gt; &lt;value&gt; | get &lt;key&gt; | list | quit"</span>);
    <span class="kw">let</span> stdin = io::stdin();
    <span class="kw">for</span> line <span class="kw">in</span> stdin.<span class="fn">lock</span>().<span class="fn">lines</span>() {
        <span class="kw">let</span> line = line?;
        <span class="kw">let</span> parts: Vec&lt;&amp;str&gt; = line.<span class="fn">splitn</span>(<span class="num">3</span>, <span class="str">' '</span>).<span class="fn">collect</span>();
        <span class="kw">match</span> parts.<span class="fn">as_slice</span>() {
            [<span class="str">"set"</span>, key, value] =&gt; {
                db.<span class="fn">execute_unprepared</span>(&amp;<span class="fn">format!</span>(
                    <span class="str">"INSERT OR REPLACE INTO kv (key, value) VALUES ('{key}', '{value}')"</span>
                )).<span class="kw">await</span>?;
            }
            [<span class="str">"get"</span>, key] =&gt; {
                <span class="kw">let</span> row = db.<span class="fn">query_one_raw</span>(
                    DatabaseBackend::Sqlite,
                    Statement::from_string(
                        DatabaseBackend::Sqlite,
                        <span class="fn">format!</span>(<span class="str">"SELECT value FROM kv WHERE key = '{key}'"</span>),
                    ),
                ).<span class="kw">await</span>?;
                <span class="kw">match</span> row {
                    Some(r) =&gt; <span class="fn">println!</span>(<span class="str">"{}"</span>, r.<span class="fn">try_get_by_index</span>::&lt;String&gt;(<span class="num">0</span>)?),
                    None =&gt; <span class="fn">println!</span>(<span class="str">"(not found)"</span>),
                }
            }
            [<span class="str">"quit"</span>] =&gt; <span class="kw">break</span>,
            _ =&gt; <span class="fn">println!</span>(<span class="str">"Unknown command"</span>),
        }
    }

    db.<span class="fn">shutdown</span>().<span class="kw">await</span>;
    Ok(())
}"##;
