use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "WatermelonDB Adapter" }

        p {
            "WaveSyncDB provides a WatermelonDB-compatible database adapter that replaces "
            "the default SQLite adapter. Every write made through WatermelonDB is automatically "
            "intercepted, CRDT metadata is created, and changes are replicated to all peers."
        }

        H2 { id: "installation", text: "Installation" }

        p {
            "Install the WaveSyncDB WatermelonDB adapter alongside WatermelonDB itself:"
        }

        CodeBlock { html: CODE_INSTALL }

        H2 { id: "schema", text: "Schema Definition" }

        p {
            "Define your WatermelonDB schema as usual with AppSchema, tableSchema, and "
            "columnArray. WaveSyncDB reads this schema to know which tables and columns to sync."
        }

        CodeBlock { html: CODE_SCHEMA }

        H2 { id: "adapter-setup", text: "Adapter Setup" }

        p {
            "Create a WaveSyncAdapter instead of the default SQLiteAdapter. Pass your schema, "
            "a sync topic, a shared passphrase, and optionally relay and rendezvous server addresses "
            "for WAN connectivity."
        }

        CodeBlock { html: CODE_ADAPTER }

        p { class: "callout",
            "CRITICAL: You must call adapter.setDatabase(database) immediately after creating "
            "the Database instance. Without this call, remote changes cannot update the "
            "WatermelonDB in-memory cache and will be silently lost."
        }

        CodeBlock { html: CODE_SET_DB }

        H2 { id: "model", text: "Defining Models" }

        p {
            "Define WatermelonDB models as you normally would. WaveSyncDB is transparent "
            "at the model layer --- no special annotations or decorators are needed."
        }

        CodeBlock { html: CODE_MODEL }

        H2 { id: "writes", text: "How Writes Work" }

        p {
            "WatermelonDB calls batch() on the adapter for all write operations. "
            "WaveSyncDB executes each operation as an individual auto-committed write. "
            "There are no multi-statement transactions --- each INSERT, UPDATE, or DELETE "
            "is committed and synced independently."
        }

        p {
            "This means batch operations are not atomic across multiple records. If your "
            "app requires atomicity, structure your data so each logical unit is a single row."
        }

        H2 { id: "remote-changes", text: "Remote Change Propagation" }

        p {
            "When a remote peer sends changes, WaveSyncDB applies them to SQLite via the "
            "CRDT conflict resolution engine. The adapter then propagates these changes "
            "to WatermelonDB's in-memory cache using the following flow:"
        }

        ul {
            li { "The native change listener calls _enqueueChange(tableName) for each affected table" }
            li { "Changes are debounced for 50ms to batch rapid updates" }
            li { "_flushTableChanges() queries SQLite for updated rows" }
            li { "Updated records are injected into WatermelonDB's internal cache" }
            li { "Any active query().observe() subscriptions fire automatically" }
        }

        H2 { id: "reactive-ui", text: "Reactive UI with Observables" }

        p {
            "Use WatermelonDB's query().observe() and the withObservables HOC or "
            "useObservable hook to build reactive components that update automatically "
            "when local or remote writes occur."
        }

        CodeBlock { html: CODE_OBSERVE }

        H2 { id: "full-example", text: "Full Example" }

        p {
            "Putting it all together --- schema, model, adapter, database, and a reactive "
            "component:"
        }

        CodeBlock { html: CODE_FULL }
    }
}

const CODE_INSTALL: &str = r##"<span class="fn">npm install</span> @wavesync/watermelondb @nozbe/watermelondb"##;

const CODE_SCHEMA: &str = r##"<span class="kw">import</span> { appSchema, tableSchema } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb'</span>;
<span class="kw">import</span> { columnArray } <span class="kw">from</span> <span class="str">'@wavesync/watermelondb'</span>;

<span class="kw">const</span> schema = <span class="fn">appSchema</span>({
  version: <span class="num">1</span>,
  tables: [
    <span class="fn">tableSchema</span>({
      name: <span class="str">'tasks'</span>,
      columns: <span class="fn">columnArray</span>([
        <span class="cmt">// [name, type] pairs — type is 'string' | 'number' | 'boolean'</span>
        [<span class="str">'title'</span>, <span class="str">'string'</span>],
        [<span class="str">'done'</span>, <span class="str">'boolean'</span>],
        [<span class="str">'created_at'</span>, <span class="str">'number'</span>],
      ]),
    }),
  ],
});"##;

const CODE_ADAPTER: &str = r##"<span class="kw">import</span> { WaveSyncAdapter } <span class="kw">from</span> <span class="str">'@wavesync/watermelondb'</span>;
<span class="kw">import</span> { Database } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb'</span>;

<span class="kw">const</span> adapter = <span class="kw">new</span> <span class="fn">WaveSyncAdapter</span>({
  schema,
  topic: <span class="str">'my-app-tasks'</span>,
  passphrase: <span class="str">'shared-secret-phrase'</span>,
  <span class="cmt">// Optional: for WAN sync (not needed for LAN-only)</span>
  relayAddr: <span class="str">'/ip4/relay.example.com/tcp/4001'</span>,
  rendezvousAddr: <span class="str">'/ip4/relay.example.com/tcp/4001'</span>,
});

<span class="kw">const</span> database = <span class="kw">new</span> <span class="fn">Database</span>({
  adapter,
  modelClasses: [Task],
});"##;

const CODE_SET_DB: &str = r##"<span class="cmt">// CRITICAL: Must be called immediately after new Database()</span>
adapter.<span class="fn">setDatabase</span>(database);

<span class="cmt">// Without this call, remote changes are applied to SQLite but</span>
<span class="cmt">// WatermelonDB's in-memory cache is never updated. The UI</span>
<span class="cmt">// will not reflect changes from other devices.</span>"##;

const CODE_MODEL: &str = r##"<span class="kw">import</span> { Model } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb'</span>;
<span class="kw">import</span> { field, text, readonly, date } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb/decorators'</span>;

<span class="kw">class</span> <span class="fn">Task</span> <span class="kw">extends</span> Model {
  <span class="kw">static</span> table = <span class="str">'tasks'</span>;

  @<span class="fn">text</span>(<span class="str">'title'</span>) title;
  @<span class="fn">field</span>(<span class="str">'done'</span>) done;
  @<span class="fn">date</span>(<span class="str">'created_at'</span>) createdAt;
}"##;

const CODE_OBSERVE: &str = r##"<span class="kw">import</span> { useObservable } <span class="kw">from</span> <span class="str">'rxjs-hooks'</span>;

<span class="kw">function</span> <span class="fn">TaskList</span>() {
  <span class="kw">const</span> tasks = <span class="fn">useObservable</span>(
    () =&gt; database.collections
      .<span class="fn">get</span>(<span class="str">'tasks'</span>)
      .<span class="fn">query</span>()
      .<span class="fn">observe</span>(),
    []
  );

  <span class="kw">return</span> (
    &lt;FlatList
      data={tasks}
      renderItem={({ item }) =&gt; &lt;Text&gt;{item.title}&lt;/Text&gt;}
    /&gt;
  );
}

<span class="cmt">// Tasks update automatically when:</span>
<span class="cmt">// - Local writes via database.write()</span>
<span class="cmt">// - Remote changes arrive from peers</span>"##;

const CODE_FULL: &str = r##"<span class="kw">import</span> { appSchema, tableSchema, Database, Model } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb'</span>;
<span class="kw">import</span> { field, text } <span class="kw">from</span> <span class="str">'@nozbe/watermelondb/decorators'</span>;
<span class="kw">import</span> { WaveSyncAdapter, columnArray } <span class="kw">from</span> <span class="str">'@wavesync/watermelondb'</span>;

<span class="cmt">// 1. Schema</span>
<span class="kw">const</span> schema = <span class="fn">appSchema</span>({
  version: <span class="num">1</span>,
  tables: [
    <span class="fn">tableSchema</span>({
      name: <span class="str">'tasks'</span>,
      columns: <span class="fn">columnArray</span>([
        [<span class="str">'title'</span>, <span class="str">'string'</span>],
        [<span class="str">'done'</span>, <span class="str">'boolean'</span>],
      ]),
    }),
  ],
});

<span class="cmt">// 2. Model</span>
<span class="kw">class</span> <span class="fn">Task</span> <span class="kw">extends</span> Model {
  <span class="kw">static</span> table = <span class="str">'tasks'</span>;
  @<span class="fn">text</span>(<span class="str">'title'</span>) title;
  @<span class="fn">field</span>(<span class="str">'done'</span>) done;
}

<span class="cmt">// 3. Adapter + Database</span>
<span class="kw">const</span> adapter = <span class="kw">new</span> <span class="fn">WaveSyncAdapter</span>({
  schema,
  topic: <span class="str">'my-app'</span>,
  passphrase: <span class="str">'secret'</span>,
});

<span class="kw">const</span> database = <span class="kw">new</span> <span class="fn">Database</span>({
  adapter,
  modelClasses: [Task],
});

<span class="cmt">// 4. CRITICAL: connect adapter to database</span>
adapter.<span class="fn">setDatabase</span>(database);

<span class="cmt">// 5. Write — syncs to all peers automatically</span>
<span class="kw">await</span> database.<span class="fn">write</span>(<span class="kw">async</span> () =&gt; {
  <span class="kw">await</span> database.collections.<span class="fn">get</span>(<span class="str">'tasks'</span>).<span class="fn">create</span>((task) =&gt; {
    task.title = <span class="str">'Buy milk'</span>;
    task.done = <span class="kw">false</span>;
  });
});

<span class="cmt">// 6. Observe — updates on local and remote changes</span>
database.collections
  .<span class="fn">get</span>(<span class="str">'tasks'</span>)
  .<span class="fn">query</span>()
  .<span class="fn">observe</span>()
  .<span class="fn">subscribe</span>((tasks) =&gt; {
    console.<span class="fn">log</span>(<span class="str">'Tasks:'</span>, tasks.<span class="fn">map</span>((t) =&gt; t.title));
  });"##;
