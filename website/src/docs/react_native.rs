use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "React Native Integration Guide" }
        p { class: "doc-lead",
            "Complete guide to building offline-first React Native apps with WaveSyncDB. \
             Covers initialization, table registration, SQL operations, change events, \
             custom hooks, and platform setup for Android and iOS."
        }

        // ── Initialization ──
        H2 { id: "initialization", text: "Initialization" }
        p {
            "WaveSync.initialize() starts the sync engine and opens the SQLite database. \
             Call it once at app startup, before any table registration or queries."
        }
        CodeBlock { html: CODE_INIT }
        p {
            "The database path is set automatically: filesDir/wavesync.db on Android \
             and Documents/wavesync.db on iOS. You do not need to specify it."
        }

        H3 { id: "init-options", text: "Configuration Options" }
        CodeBlock { html: CODE_INIT_OPTIONS }

        // ── Table Registration ──
        H2 { id: "table-registration", text: "Table Registration" }
        p {
            "After initialization, register each table you want to sync. This creates \
             both the application table and the internal shadow CRDT table."
        }

        H3 { id: "registration-order", text: "Registration Order" }
        p { class: "doc-warning",
            "CRITICAL: Call subscribeChanges() BEFORE registryReady(). If you call \
             registryReady() first, the engine starts syncing immediately and you may \
             miss initial sync events that arrive before the JS event bridge is set up."
        }
        CodeBlock { html: CODE_REGISTER_TABLE }

        H3 { id: "table-rules", text: "Table Design Rules" }
        p {
            "WaveSyncDB's CRDT sync has specific requirements for table schemas:"
        }
        ul {
            li {
                strong { "UUID primary keys only. " }
                "Auto-increment integer PKs will collide across peers. Use UUIDs \
                 (v4 recommended) for all primary keys."
            }
            li {
                strong { "DEFAULT values on all columns. " }
                "Remote CRDT inserts may create a row before all column values arrive. \
                 Without defaults, the INSERT fails and data is lost."
            }
            li {
                strong { "No explicit transactions. " }
                "Do not wrap writes in BEGIN/COMMIT. Each write must auto-commit so \
                 the CRDT interceptor can process it individually."
            }
        }

        H3 { id: "registry-ready", text: "registryReady()" }
        p {
            "After all tables are registered, call registryReady() to signal the \
             engine to start syncing. This also persists table metadata so the \
             background sync service (Android) can re-register tables after a cold start."
        }
        CodeBlock { html: CODE_REGISTRY_READY }

        // ── SQL Operations ──
        H2 { id: "sql-operations", text: "SQL Operations" }
        p {
            "All writes go through the CRDT interceptor. Reads bypass it and query \
             SQLite directly."
        }

        H3 { id: "execute", text: "execute()" }
        p {
            "Executes a write statement (INSERT, UPDATE, DELETE). Returns the number \
             of rows affected. The write is automatically intercepted, CRDT metadata \
             is generated, and the change is broadcast to connected peers."
        }
        CodeBlock { html: CODE_EXECUTE }

        H3 { id: "query", text: "query()" }
        p {
            "Executes a read query and returns the result rows. Reads are not intercepted \
             -- they go directly to SQLite for maximum performance."
        }
        CodeBlock { html: CODE_QUERY }

        // ── Change Events ──
        H2 { id: "change-events", text: "Change Events" }
        p {
            "WaveSyncDB emits change events for both local writes and remote sync. \
             Use these to keep your UI reactive without polling."
        }

        H3 { id: "subscribe-changes", text: "subscribeChanges()" }
        p {
            "Starts the Rust-to-JS event bridge. Call this once during initialization, \
             before registryReady()."
        }
        CodeBlock { html: CODE_SUBSCRIBE }

        H3 { id: "on-change-event", text: "onChangeEvent()" }
        p {
            "Registers a callback for change notifications. The callback receives an \
             object with the table name, change kind, primary key, and which columns \
             changed."
        }
        CodeBlock { html: CODE_ON_CHANGE }
        p {
            "Call subscription.remove() to unsubscribe when the component unmounts."
        }

        // ── React Hooks Pattern ──
        H2 { id: "react-hooks", text: "React Hooks Pattern" }
        p {
            "Build a complete reactive data layer with a provider and custom hooks. \
             This pattern gives you automatic re-rendering when data changes -- from \
             local writes or remote sync."
        }

        H3 { id: "wavesync-provider", text: "WaveSyncProvider" }
        p {
            "The provider initializes the engine, subscribes to changes, registers \
             tables, and exposes readiness state to the rest of the app."
        }
        CodeBlock { html: CODE_PROVIDER }

        H3 { id: "use-query-hook", text: "useQuery Hook" }
        p {
            "A generic hook that fetches data and re-fetches automatically when a \
             change event arrives for any of the watched tables."
        }
        CodeBlock { html: CODE_USE_QUERY }

        H3 { id: "use-row-hook", text: "useRow Hook" }
        p {
            "Fetches a single row by primary key and re-fetches on changes."
        }
        CodeBlock { html: CODE_USE_ROW }

        H3 { id: "use-network-status-hook", text: "useNetworkStatus Hook" }
        p {
            "Returns the current network state and re-fetches reactively when \
             network events arrive. No polling required."
        }
        CodeBlock { html: CODE_USE_NETWORK }

        // ── Android Setup ──
        H2 { id: "android-setup", text: "Android Setup" }
        p {
            "The React Native module includes prebuilt .so files for all Android \
             architectures. Autolinking handles everything -- no manual linking required."
        }

        H3 { id: "android-basic", text: "Basic Setup" }
        CodeBlock { html: CODE_ANDROID_BASIC }

        H3 { id: "android-push", text: "Push Notifications (FCM)" }
        p {
            "For push-based background sync, add Firebase Cloud Messaging support:"
        }
        CodeBlock { html: CODE_ANDROID_PUSH }
        p {
            "WaveSyncService is auto-registered via manifest merging. It handles cold \
             sync -- when Android kills your app, an incoming FCM message starts the \
             service, which re-initializes the engine and syncs before the app opens."
        }

        // ── iOS Setup ──
        H2 { id: "ios-setup", text: "iOS Setup" }
        p {
            "iOS requires building the Rust XCFramework on a Mac before installing pods."
        }

        H3 { id: "ios-build", text: "Building the Framework" }
        CodeBlock { html: CODE_IOS_BUILD }

        H3 { id: "ios-install", text: "Installing" }
        CodeBlock { html: CODE_IOS_INSTALL }
        p {
            "The podspec is auto-detected by React Native autolinking. No manual \
             pod configuration is needed."
        }

        // ── Full Example ──
        H2 { id: "full-example", text: "Full Example" }
        p {
            "A complete React Native task list app with the WaveSync provider, \
             reactive query hook, add/toggle/delete operations, and a network \
             status panel."
        }
        CodeBlock { html: CODE_FULL_EXAMPLE }
    }
}

// ── Code block constants ──

const CODE_INIT: &str = r##"<span class="kw">import</span> { WaveSync } <span class="kw">from</span> <span class="str">'react-native-wavesync'</span>;

<span class="kw">await</span> WaveSync.<span class="fn">initialize</span>(<span class="str">'my-sync-topic'</span>, {
  passphrase: <span class="str">'shared-secret'</span>,
});"##;

const CODE_INIT_OPTIONS: &str = r##"<span class="kw">await</span> WaveSync.<span class="fn">initialize</span>(<span class="str">'my-sync-topic'</span>, {
  <span class="cmt">// Authentication -- peers must share the same passphrase to sync</span>
  passphrase: <span class="str">'shared-secret'</span>,

  <span class="cmt">// Relay server for WAN connectivity (peers behind NAT)</span>
  relayServer: <span class="str">'relay.example.com:4001'</span>,

  <span class="cmt">// Rendezvous server for peer discovery over the internet</span>
  rendezvousServer: <span class="str">'rendezvous.example.com:62649'</span>,

  <span class="cmt">// Direct peer address for bootstrap (optional)</span>
  bootstrapPeer: <span class="str">'/ip4/192.168.1.50/tcp/9000'</span>,

  <span class="cmt">// Enable IPv6 for local network discovery</span>
  ipv6: <span class="kw">true</span>,

  <span class="cmt">// Periodic catch-up sync interval (default: 30)</span>
  syncIntervalSeconds: <span class="num">30</span>,
});"##;

const CODE_REGISTER_TABLE: &str = r##"<span class="cmt">// 1. Subscribe to change events FIRST</span>
WaveSync.<span class="fn">subscribeChanges</span>();

<span class="cmt">// 2. Register each synced table</span>
<span class="kw">await</span> WaveSync.<span class="fn">registerSyncedTable</span>(
  <span class="str">'tasks'</span>,           <span class="cmt">// table name</span>
  <span class="str">'id'</span>,              <span class="cmt">// primary key column</span>
  [<span class="str">'id'</span>, <span class="str">'title'</span>, <span class="str">'done'</span>, <span class="str">'created_at'</span>],  <span class="cmt">// all columns</span>
  `CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    done INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT ''
  )`
);

<span class="cmt">// 3. Signal that all tables are registered</span>
<span class="kw">await</span> WaveSync.<span class="fn">registryReady</span>();"##;

const CODE_REGISTRY_READY: &str = r##"<span class="cmt">// After ALL tables are registered:</span>
<span class="kw">await</span> WaveSync.<span class="fn">registryReady</span>();

<span class="cmt">// The engine now:</span>
<span class="cmt">// 1. Starts accepting P2P connections</span>
<span class="cmt">// 2. Triggers an initial catch-up sync with discovered peers</span>
<span class="cmt">// 3. Persists table metadata for background sync (Android)</span>"##;

const CODE_EXECUTE: &str = r##"<span class="cmt">// INSERT -- use UUID primary keys</span>
<span class="kw">const</span> id = <span class="fn">uuid</span>();
<span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
  `INSERT INTO tasks (id, title, done, created_at)
   VALUES ('${id}', '${title}', 0, '${<span class="kw">new</span> <span class="fn">Date</span>().<span class="fn">toISOString</span>()}')`
);

<span class="cmt">// UPDATE</span>
<span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
  `UPDATE tasks SET done = ${done ? <span class="num">1</span> : <span class="num">0</span>} WHERE id = '${id}'`
);

<span class="cmt">// DELETE</span>
<span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
  `DELETE FROM tasks WHERE id = '${id}'`
);"##;

const CODE_QUERY: &str = r##"<span class="cmt">// Query all tasks</span>
<span class="kw">const</span> tasks = <span class="kw">await</span> WaveSync.<span class="fn">query</span>&lt;Task[]&gt;(
  <span class="str">'SELECT * FROM tasks ORDER BY created_at DESC'</span>
);

<span class="cmt">// Query single row</span>
<span class="kw">const</span> [task] = <span class="kw">await</span> WaveSync.<span class="fn">query</span>&lt;Task[]&gt;(
  `SELECT * FROM tasks WHERE id = '${id}'`
);

<span class="cmt">// Aggregates work too</span>
<span class="kw">const</span> [{ count }] = <span class="kw">await</span> WaveSync.<span class="fn">query</span>&lt;[{ count: number }]&gt;(
  <span class="str">'SELECT COUNT(*) as count FROM tasks WHERE done = 1'</span>
);"##;

const CODE_SUBSCRIBE: &str = r##"<span class="cmt">// Start the event bridge -- call once at startup</span>
WaveSync.<span class="fn">subscribeChanges</span>();"##;

const CODE_ON_CHANGE: &str = r##"<span class="kw">const</span> subscription = WaveSync.<span class="fn">onChangeEvent</span>((event) =&gt; {
  console.<span class="fn">log</span>(event.table);          <span class="cmt">// "tasks"</span>
  console.<span class="fn">log</span>(event.kind);           <span class="cmt">// "insert" | "update" | "delete"</span>
  console.<span class="fn">log</span>(event.primaryKey);     <span class="cmt">// "a1b2c3d4-..."</span>
  console.<span class="fn">log</span>(event.changedColumns); <span class="cmt">// ["title", "done"]</span>
});

<span class="cmt">// Cleanup on unmount</span>
subscription.<span class="fn">remove</span>();"##;

const CODE_PROVIDER: &str = r##"<span class="kw">import</span> React, { createContext, useContext, useEffect, useState } <span class="kw">from</span> <span class="str">'react'</span>;
<span class="kw">import</span> { WaveSync } <span class="kw">from</span> <span class="str">'react-native-wavesync'</span>;

<span class="kw">const</span> WaveSyncContext = <span class="fn">createContext</span>({ isReady: <span class="kw">false</span> });

<span class="kw">export function</span> <span class="fn">WaveSyncProvider</span>({ children, topic, passphrase, tables }) {
  <span class="kw">const</span> [isReady, setIsReady] = <span class="fn">useState</span>(<span class="kw">false</span>);

  <span class="fn">useEffect</span>(() =&gt; {
    (<span class="kw">async</span> () =&gt; {
      <span class="cmt">// 1. Initialize the engine</span>
      <span class="kw">await</span> WaveSync.<span class="fn">initialize</span>(topic, { passphrase });

      <span class="cmt">// 2. Subscribe to changes BEFORE registering tables</span>
      WaveSync.<span class="fn">subscribeChanges</span>();

      <span class="cmt">// 3. Register all tables</span>
      <span class="kw">for</span> (<span class="kw">const</span> t <span class="kw">of</span> tables) {
        <span class="kw">await</span> WaveSync.<span class="fn">registerSyncedTable</span>(
          t.name, t.pkColumn, t.columns, t.createSql
        );
      }

      <span class="cmt">// 4. Signal registry complete -- engine starts syncing</span>
      <span class="kw">await</span> WaveSync.<span class="fn">registryReady</span>();
      <span class="fn">setIsReady</span>(<span class="kw">true</span>);
    })();
  }, []);

  <span class="kw">return</span> (
    &lt;WaveSyncContext.Provider value={{ isReady }}&gt;
      {children}
    &lt;/WaveSyncContext.Provider&gt;
  );
}

<span class="kw">export const</span> <span class="fn">useWaveSync</span> = () =&gt; <span class="fn">useContext</span>(WaveSyncContext);"##;

const CODE_USE_QUERY: &str = r##"<span class="kw">export function</span> <span class="fn">useQuery</span>(sql, watchTables = []) {
  <span class="kw">const</span> { isReady } = <span class="fn">useWaveSync</span>();
  <span class="kw">const</span> [data, setData] = <span class="fn">useState</span>([]);
  <span class="kw">const</span> [generation, setGeneration] = <span class="fn">useState</span>(<span class="num">0</span>);

  <span class="cmt">// Re-fetch on change events for watched tables</span>
  <span class="fn">useEffect</span>(() =&gt; {
    <span class="kw">if</span> (!isReady) <span class="kw">return</span>;
    <span class="kw">const</span> sub = WaveSync.<span class="fn">onChangeEvent</span>((event) =&gt; {
      <span class="kw">if</span> (watchTables.<span class="fn">includes</span>(event.table)) {
        <span class="fn">setGeneration</span>(g =&gt; g + <span class="num">1</span>);
      }
    });
    <span class="kw">return</span> () =&gt; sub.<span class="fn">remove</span>();
  }, [isReady, watchTables.<span class="fn">join</span>(<span class="str">','</span>)]);

  <span class="cmt">// Fetch data on ready and on generation change</span>
  <span class="fn">useEffect</span>(() =&gt; {
    <span class="kw">if</span> (!isReady) <span class="kw">return</span>;
    WaveSync.<span class="fn">query</span>(sql).<span class="fn">then</span>(setData);
  }, [isReady, sql, generation]);

  <span class="kw">return</span> data;
}"##;

const CODE_USE_ROW: &str = r##"<span class="kw">export function</span> <span class="fn">useRow</span>(table, id) {
  <span class="kw">const</span> rows = <span class="fn">useQuery</span>(
    `SELECT * FROM ${table} WHERE id = '${id}' LIMIT 1`,
    [table]
  );
  <span class="kw">return</span> rows[<span class="num">0</span>] ?? <span class="kw">null</span>;
}"##;

const CODE_USE_NETWORK: &str = r##"<span class="kw">export function</span> <span class="fn">useNetworkStatus</span>() {
  <span class="kw">const</span> { isReady } = <span class="fn">useWaveSync</span>();
  <span class="kw">const</span> [status, setStatus] = <span class="fn">useState</span>({
    connectedPeers: <span class="num">0</span>,
    relayStatus: <span class="str">'disconnected'</span>,
    natStatus: <span class="str">'unknown'</span>,
    dbVersion: <span class="num">0</span>,
  });

  <span class="fn">useEffect</span>(() =&gt; {
    <span class="kw">if</span> (!isReady) <span class="kw">return</span>;

    <span class="cmt">// Fetch initial status</span>
    WaveSync.<span class="fn">getNetworkStatus</span>().<span class="fn">then</span>(setStatus);

    <span class="cmt">// Re-fetch on network events (reactive, no polling)</span>
    <span class="kw">const</span> sub = WaveSync.<span class="fn">onNetworkEvent</span>(() =&gt; {
      WaveSync.<span class="fn">getNetworkStatus</span>().<span class="fn">then</span>(setStatus);
    });
    <span class="kw">return</span> () =&gt; sub.<span class="fn">remove</span>();
  }, [isReady]);

  <span class="kw">return</span> status;
}"##;

const CODE_ANDROID_BASIC: &str = r##"<span class="cmt">// No manual setup needed! Autolinking handles everything.</span>
<span class="cmt">// Prebuilt .so files are included for:</span>
<span class="cmt">//   - armeabi-v7a</span>
<span class="cmt">//   - arm64-v8a</span>
<span class="cmt">//   - x86</span>
<span class="cmt">//   - x86_64</span>

<span class="cmt">// Just install the package:</span>
npm install react-native-wavesync
<span class="cmt">// or</span>
yarn add react-native-wavesync"##;

const CODE_ANDROID_PUSH: &str = r##"<span class="cmt">// 1. Add google-services.json to android/app/</span>
<span class="cmt">//    Download from Firebase Console &gt; Project Settings</span>

<span class="cmt">// 2. Add Google Services plugin to android/build.gradle:</span>
buildscript {
    dependencies {
        classpath <span class="str">'com.google.gms:google-services:4.4.0'</span>
    }
}

<span class="cmt">// 3. Apply plugin in android/app/build.gradle:</span>
apply plugin: <span class="str">'com.google.gms.google-services'</span>

<span class="cmt">// WaveSyncService is auto-registered via manifest merging.</span>
<span class="cmt">// It handles:</span>
<span class="cmt">//   - Receiving FCM data messages</span>
<span class="cmt">//   - Cold sync when the app has been killed</span>
<span class="cmt">//   - Re-initializing the engine with persisted table metadata</span>"##;

const CODE_IOS_BUILD: &str = r##"<span class="cmt"># On a Mac with Xcode installed:</span>
cd wavesyncdb_ffi
./scripts/build-ios.sh release

<span class="cmt"># This builds the XCFramework for:</span>
<span class="cmt">#   - aarch64-apple-ios (device)</span>
<span class="cmt">#   - aarch64-apple-ios-sim (Apple Silicon simulator)</span>
<span class="cmt">#   - x86_64-apple-ios (Intel simulator)</span>"##;

const CODE_IOS_INSTALL: &str = r##"<span class="cmt"># Install pods (picks up the auto-linked podspec)</span>
cd ios
pod install

<span class="cmt"># Open the workspace</span>
open MyApp.xcworkspace"##;

const CODE_FULL_EXAMPLE: &str = r##"<span class="kw">import</span> React, { useState } <span class="kw">from</span> <span class="str">'react'</span>;
<span class="kw">import</span> {
  View, Text, TextInput, TouchableOpacity,
  FlatList, StyleSheet,
} <span class="kw">from</span> <span class="str">'react-native'</span>;
<span class="kw">import</span> { WaveSync } <span class="kw">from</span> <span class="str">'react-native-wavesync'</span>;
<span class="kw">import</span> { v4 <span class="kw">as</span> uuid } <span class="kw">from</span> <span class="str">'uuid'</span>;

<span class="cmt">// -- Table definitions --</span>
<span class="kw">const</span> TABLES = [
  {
    name: <span class="str">'tasks'</span>,
    pkColumn: <span class="str">'id'</span>,
    columns: [<span class="str">'id'</span>, <span class="str">'title'</span>, <span class="str">'done'</span>, <span class="str">'created_at'</span>],
    createSql: `CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY NOT NULL,
      title TEXT NOT NULL DEFAULT '',
      done INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT ''
    )`,
  },
];

<span class="cmt">// -- Provider (see React Hooks Pattern section) --</span>
<span class="cmt">// WaveSyncProvider, useWaveSync, useQuery, useNetworkStatus</span>
<span class="cmt">// ... (defined as shown in the hooks section above)</span>

<span class="cmt">// -- Task List Screen --</span>
<span class="kw">function</span> <span class="fn">TaskListScreen</span>() {
  <span class="kw">const</span> [newTitle, setNewTitle] = <span class="fn">useState</span>(<span class="str">''</span>);
  <span class="kw">const</span> tasks = <span class="fn">useQuery</span>(
    <span class="str">'SELECT * FROM tasks ORDER BY created_at DESC'</span>,
    [<span class="str">'tasks'</span>]
  );
  <span class="kw">const</span> networkStatus = <span class="fn">useNetworkStatus</span>();

  <span class="kw">const</span> <span class="fn">addTask</span> = <span class="kw">async</span> () =&gt; {
    <span class="kw">if</span> (!newTitle.<span class="fn">trim</span>()) <span class="kw">return</span>;
    <span class="kw">const</span> id = <span class="fn">uuid</span>();
    <span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
      `INSERT INTO tasks (id, title, done, created_at)
       VALUES ('${id}', '${newTitle.<span class="fn">trim</span>()}', 0, '${<span class="kw">new</span> <span class="fn">Date</span>().<span class="fn">toISOString</span>()}')`
    );
    <span class="fn">setNewTitle</span>(<span class="str">''</span>);
  };

  <span class="kw">const</span> <span class="fn">toggleTask</span> = <span class="kw">async</span> (task) =&gt; {
    <span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
      `UPDATE tasks SET done = ${task.done ? <span class="num">0</span> : <span class="num">1</span>} WHERE id = '${task.id}'`
    );
  };

  <span class="kw">const</span> <span class="fn">deleteTask</span> = <span class="kw">async</span> (id) =&gt; {
    <span class="kw">await</span> WaveSync.<span class="fn">execute</span>(
      `DELETE FROM tasks WHERE id = '${id}'`
    );
  };

  <span class="kw">return</span> (
    &lt;View style={styles.container}&gt;
      &lt;Text style={styles.header}&gt;Synced Tasks&lt;/Text&gt;

      {<span class="cmt">/* Network status */</span>}
      &lt;View style={styles.statusBar}&gt;
        &lt;Text&gt;Peers: {networkStatus.connectedPeers}&lt;/Text&gt;
        &lt;Text&gt;Relay: {networkStatus.relayStatus}&lt;/Text&gt;
        &lt;Text&gt;DB v{networkStatus.dbVersion}&lt;/Text&gt;
      &lt;/View&gt;

      {<span class="cmt">/* Add task */</span>}
      &lt;View style={styles.addRow}&gt;
        &lt;TextInput
          style={styles.input}
          value={newTitle}
          onChangeText={setNewTitle}
          placeholder="New task..."
        /&gt;
        &lt;TouchableOpacity style={styles.addBtn} onPress={addTask}&gt;
          &lt;Text style={styles.addBtnText}&gt;Add&lt;/Text&gt;
        &lt;/TouchableOpacity&gt;
      &lt;/View&gt;

      {<span class="cmt">/* Task list */</span>}
      &lt;FlatList
        data={tasks}
        keyExtractor={(item) =&gt; item.id}
        renderItem={({ item }) =&gt; (
          &lt;View style={styles.taskRow}&gt;
            &lt;TouchableOpacity onPress={() =&gt; <span class="fn">toggleTask</span>(item)}&gt;
              &lt;Text style={styles.checkbox}&gt;
                {item.done ? '[x]' : '[ ]'}
              &lt;/Text&gt;
            &lt;/TouchableOpacity&gt;
            &lt;Text style={[
              styles.taskTitle,
              item.done &amp;&amp; styles.taskDone,
            ]}&gt;
              {item.title}
            &lt;/Text&gt;
            &lt;TouchableOpacity onPress={() =&gt; <span class="fn">deleteTask</span>(item.id)}&gt;
              &lt;Text style={styles.deleteBtn}&gt;Delete&lt;/Text&gt;
            &lt;/TouchableOpacity&gt;
          &lt;/View&gt;
        )}
      /&gt;
    &lt;/View&gt;
  );
}

<span class="cmt">// -- App entry point --</span>
<span class="kw">export default function</span> <span class="fn">App</span>() {
  <span class="kw">return</span> (
    &lt;WaveSyncProvider
      topic=<span class="str">"tasks-sync"</span>
      passphrase=<span class="str">"my-secret"</span>
      tables={TABLES}
    &gt;
      &lt;TaskListScreen /&gt;
    &lt;/WaveSyncProvider&gt;
  );
}"##;
