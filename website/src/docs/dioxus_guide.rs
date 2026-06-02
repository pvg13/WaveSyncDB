use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Dioxus Integration Guide" }
        p { class: "doc-lead",
            "Complete guide to building offline-first Dioxus apps with WaveSyncDB. \
             Covers provider setup, initialization, reactive hooks, lifecycle management, \
             and push notifications."
        }

        // ── Provider Setup ──
        H2 { id: "provider-setup", text: "Provider Setup" }
        p {
            "WaveSyncDB uses Dioxus context to make the database available throughout \
             your component tree. There are two provider modes depending on when you \
             build the database."
        }

        H3 { id: "inject-provider", text: "Injecting a Pre-Built Database" }
        p {
            "If you build the database outside the component tree (e.g. in main), \
             inject it directly:"
        }
        CodeBlock { html: CODE_PROVIDER_INJECT }
        p {
            "This makes the database immediately available to all descendants."
        }

        H3 { id: "lazy-provider", text: "Lazy Provider" }
        p {
            "For most apps, the database is initialized asynchronously inside a \
             component. Use the lazy provider:"
        }
        CodeBlock { html: CODE_PROVIDER_LAZY }
        p {
            "The lazy provider starts as None and is populated after initialization \
             completes."
        }

        H3 { id: "consuming-provider", text: "Consuming the Provider" }
        p {
            "Descendant components access the database with one of two hooks:"
        }
        CodeBlock { html: CODE_CONSUME_PROVIDER }
        p {
            "use_wavesync() panics if the provider is not initialized yet. Prefer \
             use_wavesync_opt() in components that render before initialization completes."
        }

        // ── Initialization ──
        H2 { id: "initialization", text: "Initialization" }
        p {
            "The use_wavesync_init hook returns an InitDb handle that creates and \
             configures the database."
        }

        H3 { id: "basic-init", text: "Basic Initialization" }
        CodeBlock { html: CODE_INIT_BASIC }
        p {
            "init_db.call() uses the default builder. The setup function receives \
             the database and should register tables and perform any first-run logic."
        }

        H3 { id: "custom-init", text: "Custom Builder Configuration" }
        p {
            "Use call_with() to configure the builder before it builds. This is where \
             you set the passphrase, relay server, sync interval, and other options."
        }
        CodeBlock { html: CODE_INIT_CUSTOM }

        H3 { id: "reset", text: "Reset (Logout / Switch Account)" }
        p {
            "init_db.reset() shuts down the current database engine and clears the \
             provider. Use this when a user logs out or switches sync groups."
        }
        CodeBlock { html: CODE_INIT_RESET }
        p {
            "Internally, reset increments a generation counter. Any async tasks from \
             the previous generation are automatically ignored when they complete, \
             preventing stale data from being applied to the new session."
        }

        // ── Entity Registration ──
        H2 { id: "entity-registration", text: "Entity Registration" }
        p {
            "WaveSyncDB needs to know which SeaORM entities to sync. Entities must \
             derive SyncEntity in addition to the standard SeaORM derives."
        }

        H3 { id: "derive-sync", text: "Deriving SyncEntity" }
        CodeBlock { html: CODE_DERIVE_ENTITY }

        H3 { id: "register-manual", text: "Manual Registration" }
        p {
            "Register entities one at a time with the schema builder:"
        }
        CodeBlock { html: CODE_REGISTER_MANUAL }

        H3 { id: "register-auto", text: "Auto-Discovery Registration" }
        p {
            "If your entities are in a dedicated crate, auto-discover them all:"
        }
        CodeBlock { html: CODE_REGISTER_AUTO }

        H3 { id: "register-local", text: "Local-Only Tables" }
        p {
            "For tables that should not sync (caches, local preferences), use \
             register_local. These tables are created but excluded from the sync engine."
        }
        CodeBlock { html: CODE_REGISTER_LOCAL }

        // ── Reactive Hooks ──
        H2 { id: "reactive-hooks", text: "Reactive Hooks" }
        p {
            "WaveSyncDB provides reactive hooks that automatically re-query when \
             the underlying data changes -- from local writes or remote sync."
        }

        H3 { id: "use-synced-table", text: "use_synced_table" }
        p {
            "Returns a Signal containing all rows of the given entity. The signal \
             updates whenever a change notification arrives for that table."
        }
        CodeBlock { html: CODE_SYNCED_TABLE }
        p {
            "Under the hood, use_synced_table subscribes to the database's change_rx() \
             broadcast channel. When a ChangeNotification arrives, it checks if \
             notification.table matches the entity's table name. If it matches, \
             it re-queries the full table. If RecvError::Lagged is received (burst \
             writes overflowed the channel buffer), it performs a full re-query -- \
             data is never lost, only notifications."
        }

        H3 { id: "use-synced-row", text: "use_synced_row" }
        p {
            "Returns a Signal for a single row by primary key. Same reactivity \
             mechanism as use_synced_table but scoped to one row."
        }
        CodeBlock { html: CODE_SYNCED_ROW }

        // ── Network Hooks ──
        H2 { id: "network-hooks", text: "Network Hooks" }

        H3 { id: "use-network-status", text: "use_network_status" }
        p {
            "Returns a reactive Signal with the current network state: connected \
             peers, relay status, NAT type, and local db_version."
        }
        CodeBlock { html: CODE_NETWORK_STATUS }

        H3 { id: "use-peer-identities", text: "use_peer_identities" }
        p {
            "Returns peer identity information grouped by app_id. Useful for \
             displaying who is connected in a collaborative UI."
        }
        CodeBlock { html: CODE_PEER_IDENTITIES }

        // ── Lifecycle Hooks ──
        H2 { id: "lifecycle-hooks", text: "Lifecycle Hooks" }
        p {
            "Mobile apps are suspended by the OS. WaveSyncDB provides lifecycle \
             hooks to pause and resume the sync engine correctly."
        }

        H3 { id: "use-auto-lifecycle", text: "use_auto_lifecycle" }
        p {
            "Automatically detects foreground/background transitions on mobile \
             platforms. On Android, it uses JNI to monitor hasWindowFocus. On iOS, \
             it observes NSNotificationCenter for UIApplication lifecycle events. \
             When the app returns to the foreground, it calls db.resume() to trigger \
             an immediate catch-up sync."
        }
        CodeBlock { html: CODE_AUTO_LIFECYCLE }

        H3 { id: "use-app-resume", text: "use_app_resume" }
        p {
            "For manual control, provide your own foreground signal. The hook calls \
             db.resume() on false-to-true transitions."
        }
        CodeBlock { html: CODE_APP_RESUME }

        H3 { id: "use-auto-push", text: "use_auto_push (iOS)" }
        p {
            "iOS only. Injects an APNs delegate at runtime and automatically \
             registers the device token with the sync engine. When a push notification \
             arrives, the engine wakes and performs a catch-up sync."
        }
        CodeBlock { html: CODE_AUTO_PUSH }

        // ── Full Example ──
        H2 { id: "full-example", text: "Full Example" }
        p {
            "A complete Dioxus mobile app with initialization, schema registration, \
             a synced task list with add/toggle/delete, and a network status display."
        }
        CodeBlock { html: CODE_FULL_EXAMPLE }
    }
}

// ── Code block constants ──

const CODE_PROVIDER_INJECT: &str = r##"<span class="cmt">// In your root component, inject a database you built elsewhere</span>
<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_context</span>::&lt;WaveSyncDb&gt;();
    <span class="fn">use_wavesync_provider</span>(db);

    rsx! { Outlet::&lt;Route&gt; {} }
}"##;

const CODE_PROVIDER_LAZY: &str = r##"<span class="cmt">// Lazy provider -- starts as None, populated after async init</span>
<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="fn">use_wavesync_provider_lazy</span>();

    rsx! { Outlet::&lt;Route&gt; {} }
}"##;

const CODE_CONSUME_PROVIDER: &str = r##"<span class="cmt">// Panics if provider not initialized -- use in components that render after init</span>
<span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

<span class="cmt">// Safe version -- returns Signal&lt;Option&lt;WaveSyncDb&gt;&gt;</span>
<span class="kw">let</span> db_opt = <span class="fn">use_wavesync_opt</span>();
<span class="kw">if let</span> Some(db) = db_opt() {
    <span class="cmt">// database is ready</span>
}"##;

const CODE_INIT_BASIC: &str = r##"<span class="kw">fn</span> <span class="fn">InitScreen</span>() -&gt; Element {
    <span class="kw">let</span> init_db = <span class="fn">use_wavesync_init</span>();

    <span class="fn">use_effect</span>(<span class="kw">move</span> || {
        <span class="fn">spawn</span>(<span class="kw">async move</span> {
            init_db.<span class="fn">call</span>(
                <span class="str">"sqlite:app.db"</span>,     <span class="cmt">// database URL</span>
                <span class="str">"my-sync-topic"</span>,     <span class="cmt">// sync topic</span>
                |db| Box::pin(<span class="kw">async move</span> {
                    <span class="cmt">// Register tables here</span>
                    db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;
                    Ok(())
                }),
            ).<span class="kw">await</span>;
        });
    });

    rsx! { p { "Initializing..." } }
}"##;

const CODE_INIT_CUSTOM: &str = r##"init_db.<span class="fn">call_with</span>(
    <span class="str">"sqlite:app.db"</span>,
    <span class="str">"my-sync-topic"</span>,
    |builder| {
        builder
            .<span class="fn">with_passphrase</span>(<span class="str">"shared-secret"</span>)
            .<span class="fn">with_relay_server</span>(<span class="str">"relay.example.com:4001"</span>)
            .<span class="fn">with_rendezvous_server</span>(<span class="str">"rendezvous.example.com:62649"</span>)
            .<span class="fn">with_sync_interval</span>(Duration::from_secs(<span class="num">30</span>))
            .<span class="fn">with_ipv6</span>(<span class="kw">true</span>)
    },
    |db| Box::pin(<span class="kw">async move</span> {
        db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;
        db.<span class="fn">schema</span>().<span class="fn">register</span>(Project).<span class="fn">sync</span>().<span class="kw">await</span>?;
        Ok(())
    }),
).<span class="kw">await</span>;"##;

const CODE_INIT_RESET: &str = r##"<span class="cmt">// Shutdown the current database and clear the provider</span>
init_db.<span class="fn">reset</span>();

<span class="cmt">// Then re-initialize with a new topic/passphrase</span>
init_db.<span class="fn">call_with</span>(
    <span class="str">"sqlite:app.db"</span>,
    <span class="str">"new-group-topic"</span>,
    |builder| builder.<span class="fn">with_passphrase</span>(<span class="str">"new-passphrase"</span>),
    |db| Box::pin(<span class="kw">async move</span> {
        db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;
        Ok(())
    }),
).<span class="kw">await</span>;"##;

const CODE_DERIVE_ENTITY: &str = r##"<span class="kw">use</span> sea_orm::entity::prelude::*;
<span class="kw">use</span> wavesyncdb::SyncEntity;

<span class="cmt">// Add SyncEntity alongside your SeaORM derives</span>
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, SyncEntity)]
#[sea_orm(table_name = <span class="str">"tasks"</span>)]
<span class="kw">pub struct</span> Model {
    #[sea_orm(primary_key, auto_increment = <span class="kw">false</span>)]
    <span class="kw">pub</span> id: String,
    <span class="kw">pub</span> title: String,
    <span class="kw">pub</span> done: <span class="kw">bool</span>,
    <span class="kw">pub</span> created_at: String,
}"##;

const CODE_REGISTER_MANUAL: &str = r##"<span class="cmt">// Register one entity at a time</span>
db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;
db.<span class="fn">schema</span>().<span class="fn">register</span>(Project).<span class="fn">sync</span>().<span class="kw">await</span>?;
db.<span class="fn">schema</span>().<span class="fn">register</span>(Tag).<span class="fn">sync</span>().<span class="kw">await</span>?;"##;

const CODE_REGISTER_AUTO: &str = r##"<span class="cmt">// Auto-discover all SyncEntity types in a crate</span>
db.<span class="fn">get_schema_registry</span>(<span class="str">"my_entities_crate"</span>).<span class="fn">sync</span>().<span class="kw">await</span>?;"##;

const CODE_REGISTER_LOCAL: &str = r##"<span class="cmt">// Local-only table -- created but never synced</span>
db.<span class="fn">schema</span>().<span class="fn">register_local</span>(UserPreferences).<span class="fn">sync</span>().<span class="kw">await</span>?;"##;

const CODE_SYNCED_TABLE: &str = r##"<span class="kw">fn</span> <span class="fn">TaskList</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> tasks: Signal&lt;Vec&lt;task::Model&gt;&gt; = <span class="fn">use_synced_table</span>::&lt;Task&gt;(db);

    rsx! {
        <span class="kw">for</span> task <span class="kw">in</span> tasks() {
            div { class: <span class="str">"task-row"</span>,
                span { "{task.title}" }
            }
        }
    }
}"##;

const CODE_SYNCED_ROW: &str = r##"<span class="kw">fn</span> <span class="fn">TaskDetail</span>(id: String) -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> task: Signal&lt;Option&lt;task::Model&gt;&gt; = <span class="fn">use_synced_row</span>::&lt;Task&gt;(db, id);

    <span class="kw">match</span> task() {
        Some(t) =&gt; rsx! {
            h2 { "{t.title}" }
            p { "Done: {t.done}" }
        },
        None =&gt; rsx! { p { "Task not found" } },
    }
}"##;

const CODE_NETWORK_STATUS: &str = r##"<span class="kw">fn</span> <span class="fn">StatusBar</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> status: Signal&lt;NetworkStatus&gt; = <span class="fn">use_network_status</span>(db);

    <span class="kw">let</span> s = status();
    rsx! {
        div { class: <span class="str">"status-bar"</span>,
            span { "Peers: {s.connected_peers}" }
            span { "Relay: {s.relay_status:?}" }
            span { "NAT: {s.nat_status:?}" }
            span { "DB version: {s.db_version}" }
        }
    }
}"##;

const CODE_PEER_IDENTITIES: &str = r##"<span class="kw">fn</span> <span class="fn">PeerList</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> peers = <span class="fn">use_peer_identities</span>(db);

    rsx! {
        <span class="kw">for</span> (app_id, infos) <span class="kw">in</span> peers() {
            h4 { "{app_id}" }
            <span class="kw">for</span> info <span class="kw">in</span> infos {
                p { "Peer: {info.peer_id} -- {info.addr}" }
            }
        }
    }
}"##;

const CODE_AUTO_LIFECYCLE: &str = r##"<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

    <span class="cmt">// Automatically handles foreground/background on Android and iOS</span>
    <span class="fn">use_auto_lifecycle</span>(db);

    rsx! { Outlet::&lt;Route&gt; {} }
}"##;

const CODE_APP_RESUME: &str = r##"<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let mut</span> is_foreground = <span class="fn">use_signal</span>(|| <span class="kw">true</span>);

    <span class="cmt">// You control when foreground/background transitions happen</span>
    <span class="fn">use_app_resume</span>(db, is_foreground);

    <span class="cmt">// Toggle from your own platform code</span>
    <span class="cmt">// is_foreground.set(false); // app backgrounded</span>
    <span class="cmt">// is_foreground.set(true);  // app foregrounded -- triggers resume</span>

    rsx! { Outlet::&lt;Route&gt; {} }
}"##;

const CODE_AUTO_PUSH: &str = r##"<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

    <span class="cmt">// iOS only: injects APNs delegate, registers token with sync engine</span>
    <span class="fn">use_auto_push</span>(db);

    rsx! { Outlet::&lt;Route&gt; {} }
}"##;

const CODE_FULL_EXAMPLE: &str = r##"<span class="kw">use</span> dioxus::prelude::*;
<span class="kw">use</span> wavesyncdb::dioxus::*;
<span class="kw">use</span> uuid::Uuid;

<span class="kw">fn</span> <span class="fn">main</span>() {
    dioxus::launch(App);
}

<span class="kw">fn</span> <span class="fn">App</span>() -&gt; Element {
    <span class="fn">use_wavesync_provider_lazy</span>();

    rsx! {
        InitScreen {}
    }
}

<span class="kw">fn</span> <span class="fn">InitScreen</span>() -&gt; Element {
    <span class="kw">let</span> init_db = <span class="fn">use_wavesync_init</span>();
    <span class="kw">let</span> db_opt = <span class="fn">use_wavesync_opt</span>();

    <span class="fn">use_effect</span>(<span class="kw">move</span> || {
        <span class="fn">spawn</span>(<span class="kw">async move</span> {
            init_db.<span class="fn">call_with</span>(
                <span class="str">"sqlite:tasks.db"</span>,
                <span class="str">"tasks-sync"</span>,
                |builder| builder.<span class="fn">with_passphrase</span>(<span class="str">"my-secret"</span>),
                |db| Box::pin(<span class="kw">async move</span> {
                    db.<span class="fn">schema</span>().<span class="fn">register</span>(Task).<span class="fn">sync</span>().<span class="kw">await</span>?;
                    Ok(())
                }),
            ).<span class="kw">await</span>;
        });
    });

    <span class="kw">match</span> db_opt() {
        Some(_) =&gt; rsx! { MainApp {} },
        None =&gt; rsx! {
            div { class: <span class="str">"loading"</span>, "Initializing sync engine..." }
        },
    }
}

<span class="kw">fn</span> <span class="fn">MainApp</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();

    <span class="cmt">// Lifecycle hooks</span>
    <span class="fn">use_auto_lifecycle</span>(db);

    rsx! {
        h1 { "Synced Tasks" }
        TaskList {}
        NetworkPanel {}
    }
}

<span class="kw">fn</span> <span class="fn">TaskList</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> tasks = <span class="fn">use_synced_table</span>::&lt;Task&gt;(db);
    <span class="kw">let mut</span> new_title = <span class="fn">use_signal</span>(String::new);

    <span class="kw">let</span> on_add = <span class="kw">move</span> |_| {
        <span class="kw">let</span> title = new_title().<span class="fn">clone</span>();
        <span class="kw">let</span> db = db.<span class="fn">clone</span>();
        <span class="fn">spawn</span>(<span class="kw">async move</span> {
            task::ActiveModel {
                id: Set(Uuid::<span class="fn">new_v4</span>().<span class="fn">to_string</span>()),
                title: Set(title),
                done: Set(<span class="kw">false</span>),
                ..Default::default()
            }.<span class="fn">insert</span>(&amp;db).<span class="kw">await</span>.<span class="fn">ok</span>();
            new_title.<span class="fn">set</span>(String::new());
        });
    };

    rsx! {
        div { class: <span class="str">"add-task"</span>,
            input {
                value: "{new_title}",
                oninput: <span class="kw">move</span> |e| new_title.<span class="fn">set</span>(e.<span class="fn">value</span>()),
                placeholder: <span class="str">"New task..."</span>,
            }
            button { onclick: on_add, "Add" }
        }
        <span class="kw">for</span> task <span class="kw">in</span> tasks() {
            TaskRow { task: task }
        }
    }
}

#[component]
<span class="kw">fn</span> <span class="fn">TaskRow</span>(task: task::Model) -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> id = task.id.<span class="fn">clone</span>();

    <span class="kw">let</span> on_toggle = <span class="kw">move</span> |_| {
        <span class="kw">let</span> db = db.<span class="fn">clone</span>();
        <span class="kw">let</span> id = id.<span class="fn">clone</span>();
        <span class="kw">let</span> new_done = !task.done;
        <span class="fn">spawn</span>(<span class="kw">async move</span> {
            task::ActiveModel {
                id: Set(id),
                done: Set(new_done),
                ..Default::default()
            }.<span class="fn">update</span>(&amp;db).<span class="kw">await</span>.<span class="fn">ok</span>();
        });
    };

    <span class="kw">let</span> on_delete = <span class="kw">move</span> |_| {
        <span class="kw">let</span> db = db.<span class="fn">clone</span>();
        <span class="kw">let</span> id = task.id.<span class="fn">clone</span>();
        <span class="fn">spawn</span>(<span class="kw">async move</span> {
            task::Entity::<span class="fn">delete_by_id</span>(id)
                .<span class="fn">exec</span>(&amp;db).<span class="kw">await</span>.<span class="fn">ok</span>();
        });
    };

    rsx! {
        div { class: <span class="str">"task-row"</span>,
            input {
                r#type: <span class="str">"checkbox"</span>,
                checked: task.done,
                onchange: on_toggle,
            }
            span {
                class: <span class="kw">if</span> task.done { <span class="str">"done"</span> } <span class="kw">else</span> { <span class="str">""</span> },
                "{task.title}"
            }
            button { onclick: on_delete, "Delete" }
        }
    }
}

<span class="kw">fn</span> <span class="fn">NetworkPanel</span>() -&gt; Element {
    <span class="kw">let</span> db = <span class="fn">use_wavesync</span>();
    <span class="kw">let</span> status = <span class="fn">use_network_status</span>(db);

    <span class="kw">let</span> s = status();
    rsx! {
        div { class: <span class="str">"network-panel"</span>,
            h3 { "Network Status" }
            p { "Connected peers: {s.connected_peers}" }
            p { "Relay: {s.relay_status:?}" }
            p { "NAT: {s.nat_status:?}" }
            p { "Local DB version: {s.db_version}" }
        }
    }
}"##;
