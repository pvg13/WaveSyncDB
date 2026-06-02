use dioxus::prelude::*;
use super::{CodeBlock, H2, H3};

#[component]
pub fn Page() -> Element {
    rsx! {
        h1 { "Conflict Resolution" }

        p {
            "WaveSyncDB resolves conflicts automatically using per-column Lamport clocks "
            "and a fully deterministic tiebreaking algorithm. Every peer applies the same "
            "rules independently and converges to the same state, regardless of message "
            "arrival order."
        }

        H2 { id: "why-per-column", text: "Why Per-Column, Not Row-Level" }

        p {
            "Row-level last-write-wins (LWW) discards concurrent edits to different "
            "columns. If Peer A updates the title of a task while Peer B marks it as done, "
            "row-level LWW would keep only one of those changes. The other edit is silently "
            "lost."
        }

        p {
            "WaveSyncDB uses per-column conflict resolution instead. Each column in each "
            "row has its own independent Lamport clock (col_version). Concurrent edits to "
            "different columns on the same row both survive because they are tracked "
            "independently."
        }

        CodeBlock { html: CODE_SCENARIO_BOTH }

        H2 { id: "lamport-clocks", text: "Column-Level Lamport Clocks" }

        p {
            "Every (primary_key, column_name) pair has a col_version counter stored in "
            "the shadow table. When a column is written:"
        }

        ul {
            li { "The current col_version is read from the shadow table" }
            li { "The new col_version is set to max(local_col_version, remote_col_version) + 1" }
            li { "The shadow entry is upserted with INSERT OR REPLACE" }
        }

        p {
            "The col_version is always re-queried from the shadow table after the upsert "
            "--- never read from an in-memory cache. This prevents stale values from "
            "breaking the next increment."
        }

        H2 { id: "tiebreak", text: "Three-Level Tiebreak Algorithm" }

        p {
            "When a remote change arrives for a column that already has a local value, "
            "WaveSyncDB applies a deterministic tiebreak:"
        }

        H3 { id: "level-1", text: "Level 1: Higher col_version Wins" }

        p {
            "If the remote col_version is strictly greater than the local col_version, "
            "the remote value is applied. If strictly less, it is rejected."
        }

        CodeBlock { html: CODE_LEVEL_1 }

        H3 { id: "level-2", text: "Level 2: Compare Serialized Value Bytes" }

        p {
            "When col_version values are equal, the serialized JSON byte representation "
            "of each value is compared lexicographically. The greater byte sequence wins."
        }

        CodeBlock { html: CODE_LEVEL_2 }

        H3 { id: "level-3", text: "Level 3: Higher site_id Wins" }

        p {
            "If both col_version and value bytes are identical (extremely rare), the "
            "16-byte site_id arrays are compared. The greater site_id wins."
        }

        p {
            "This three-level tiebreak is a total order --- it always produces a winner. "
            "No randomness, no wall-clock time, no non-deterministic input. Every peer "
            "that sees the same pair of conflicting writes will independently choose the "
            "same winner."
        }

        CodeBlock { html: CODE_ALGORITHM }

        H2 { id: "delete-conflicts", text: "Delete Conflicts" }

        p {
            "Delete operations interact with concurrent edits differently from column "
            "updates. WaveSyncDB supports two policies:"
        }

        H3 { id: "delete-wins", text: "DeleteWins (Default)" }

        p {
            "A delete operation wins over concurrent non-delete edits when its causal "
            "length (cl) is greater than or equal to the local maximum col_version. This "
            "is the default and appropriate for most applications."
        }

        H3 { id: "add-wins", text: "AddWins" }

        p {
            "A concurrent non-delete edit resurrects a deleted row. This is useful for "
            "applications where data loss is worse than having deleted items reappear."
        }

        CodeBlock { html: CODE_DELETE_POLICY }

        H2 { id: "causal-length", text: "Causal Length" }

        p {
            "When a row is deleted, WaveSyncDB computes a causal_length equal to the "
            "maximum col_version across all columns of that row, plus 1. This establishes "
            "that the delete has seen all prior edits."
        }

        p {
            "A remote delete is only applied if its causal_length exceeds the local "
            "maximum col_version. This prevents out-of-order deletes from overriding "
            "edits that happened after the delete was issued."
        }

        H2 { id: "tombstones", text: "Tombstones" }

        p {
            "Deleted rows are tracked with a __deleted sentinel column in the shadow "
            "table. This tombstone serves two purposes:"
        }

        ul {
            li {
                "Prevents resurrection from out-of-order message delivery --- a peer "
                "receiving an old INSERT after a DELETE will check the tombstone and "
                "apply conflict resolution correctly"
            }
            li {
                "Propagates the delete to peers that have not yet seen it --- the "
                "tombstone is included in version vector catch-up responses"
            }
        }

        H2 { id: "examples", text: "Conflict Scenarios" }

        H3 { id: "different-columns", text: "Scenario: Different Columns" }

        p {
            "Peer A updates the title while Peer B marks the task as done. Both changes "
            "are to different columns, so both survive without any conflict."
        }

        CodeBlock { html: CODE_DIFFERENT_COLS }

        H3 { id: "same-column", text: "Scenario: Same Column" }

        p {
            "Peer A sets title to \"Buy milk\" while Peer B sets title to \"Buy eggs\". "
            "Both are at col_version=1. The tiebreak compares the serialized values: "
            "\"Buy milk\" vs \"Buy eggs\". Since \"m\" > \"e\" in byte order, "
            "Peer A's value wins on both peers."
        }

        CodeBlock { html: CODE_SAME_COL }

        H3 { id: "delete-edit", text: "Scenario: Delete vs Edit" }

        p {
            "Peer A deletes a task while Peer B edits its title. With DeleteWins policy, "
            "the delete takes precedence if its causal_length covers the edit's col_version."
        }

        CodeBlock { html: CODE_DELETE_EDIT }
    }
}

const CODE_SCENARIO_BOTH: &str = r##"<span class="cmt">// Row-level LWW: one edit lost</span>
Peer A: <span class="kw">UPDATE</span> tasks <span class="kw">SET</span> title = <span class="str">'Buy milk'</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
Peer B: <span class="kw">UPDATE</span> tasks <span class="kw">SET</span> done = <span class="num">1</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// Row LWW result: only one peer's entire row survives</span>

<span class="cmt">// Per-column (WaveSyncDB): both survive</span>
<span class="cmt">// title='Buy milk' (from A) + done=1 (from B)</span>
<span class="cmt">// No conflict — different columns have independent clocks</span>"##;

const CODE_LEVEL_1: &str = r##"<span class="cmt">// Remote col_version=5 vs local col_version=3</span>
<span class="cmt">// Remote wins — higher version always takes precedence</span>

<span class="cmt">// Remote col_version=2 vs local col_version=5</span>
<span class="cmt">// Remote rejected — local version is newer</span>"##;

const CODE_LEVEL_2: &str = r##"<span class="cmt">// Both at col_version=3</span>
<span class="cmt">// Remote value bytes: "Buy milk"  → [34,66,117,121,32,109,105,108,107,34]</span>
<span class="cmt">// Local value bytes:  "Buy eggs"  → [34,66,117,121,32,101,103,103,115,34]</span>
<span class="cmt">// "milk" &gt; "eggs" in byte order → remote wins</span>"##;

const CODE_ALGORITHM: &str = r##"<span class="kw">fn</span> <span class="fn">should_apply_column</span>(
    remote_col_version: u64,
    remote_val: &amp;[u8],
    remote_site_id: &amp;NodeId,
    local_col_version: u64,
    local_val: &amp;[u8],
    local_site_id: &amp;NodeId,
) -&gt; bool {
    <span class="kw">match</span> remote_col_version.<span class="fn">cmp</span>(&amp;local_col_version) {
        Greater =&gt; <span class="kw">true</span>,   <span class="cmt">// Level 1: higher version wins</span>
        Less =&gt; <span class="kw">false</span>,
        Equal =&gt; <span class="kw">match</span> remote_val.<span class="fn">cmp</span>(local_val) {
            Greater =&gt; <span class="kw">true</span>, <span class="cmt">// Level 2: greater value bytes</span>
            Less =&gt; <span class="kw">false</span>,
            Equal =&gt; remote_site_id.<span class="num">0</span> &gt; local_site_id.<span class="num">0</span>,
                               <span class="cmt">// Level 3: higher site_id</span>
        }
    }
}"##;

const CODE_DELETE_POLICY: &str = r##"<span class="kw">use</span> wavesyncdb::messages::DeletePolicy;

<span class="cmt">// Per-table delete policy configuration</span>
db.<span class="fn">register_table</span>(TableMeta {
    table_name: <span class="str">"tasks"</span>.<span class="fn">into</span>(),
    pk_column: <span class="str">"id"</span>.<span class="fn">to_string</span>(),
    columns: <span class="kw">vec!</span>[<span class="str">"id"</span>.<span class="fn">into</span>(), <span class="str">"title"</span>.<span class="fn">into</span>(), <span class="str">"done"</span>.<span class="fn">into</span>()],
    delete_policy: DeletePolicy::DeleteWins,  <span class="cmt">// default</span>
});

db.<span class="fn">register_table</span>(TableMeta {
    table_name: <span class="str">"notes"</span>.<span class="fn">into</span>(),
    pk_column: <span class="str">"id"</span>.<span class="fn">to_string</span>(),
    columns: <span class="kw">vec!</span>[<span class="str">"id"</span>.<span class="fn">into</span>(), <span class="str">"body"</span>.<span class="fn">into</span>()],
    delete_policy: DeletePolicy::AddWins,     <span class="cmt">// edits resurrect</span>
});"##;

const CODE_DIFFERENT_COLS: &str = r##"<span class="cmt">// Initial state: tasks row t1 = (title: "Task", done: 0)</span>
<span class="cmt">// Both columns at col_version=1</span>

<span class="cmt">// Peer A (offline):</span>
<span class="kw">UPDATE</span> tasks <span class="kw">SET</span> title = <span class="str">'Buy milk'</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → title col_version becomes 2</span>

<span class="cmt">// Peer B (offline):</span>
<span class="kw">UPDATE</span> tasks <span class="kw">SET</span> done = <span class="num">1</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → done col_version becomes 2</span>

<span class="cmt">// After sync — both peers converge to:</span>
<span class="cmt">//   title = 'Buy milk' (col_version=2, from A)</span>
<span class="cmt">//   done  = 1          (col_version=2, from B)</span>
<span class="cmt">// No conflict — independent columns, both changes preserved.</span>"##;

const CODE_SAME_COL: &str = r##"<span class="cmt">// Initial state: tasks row t1, title col_version=1</span>

<span class="cmt">// Peer A (offline):</span>
<span class="kw">UPDATE</span> tasks <span class="kw">SET</span> title = <span class="str">'Buy milk'</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → title col_version=2, val="Buy milk", site_id=A</span>

<span class="cmt">// Peer B (offline):</span>
<span class="kw">UPDATE</span> tasks <span class="kw">SET</span> title = <span class="str">'Buy eggs'</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → title col_version=2, val="Buy eggs", site_id=B</span>

<span class="cmt">// After sync — tiebreak applied:</span>
<span class="cmt">// 1. col_version=2 vs col_version=2 → tie</span>
<span class="cmt">// 2. "Buy milk" vs "Buy eggs" → "milk" &gt; "eggs" → A wins</span>
<span class="cmt">// Both peers converge to: title = 'Buy milk'</span>"##;

const CODE_DELETE_EDIT: &str = r##"<span class="cmt">// Initial state: tasks row t1, max col_version=3</span>

<span class="cmt">// Peer A (offline):</span>
<span class="kw">DELETE FROM</span> tasks <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → tombstone with causal_length = max(3) + 1 = 4</span>

<span class="cmt">// Peer B (offline):</span>
<span class="kw">UPDATE</span> tasks <span class="kw">SET</span> title = <span class="str">'Updated'</span> <span class="kw">WHERE</span> id = <span class="str">'t1'</span>
<span class="cmt">// → title col_version = 4</span>

<span class="cmt">// With DeleteWins (default):</span>
<span class="cmt">//   causal_length(4) &gt;= local_max_col_version(4) → delete wins</span>
<span class="cmt">//   Row stays deleted on both peers.</span>

<span class="cmt">// With AddWins:</span>
<span class="cmt">//   causal_length(4) &gt;= local_max_col_version(4) → tie → add wins</span>
<span class="cmt">//   Row is resurrected with title='Updated' on both peers.</span>"##;
