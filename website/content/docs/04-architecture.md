# Architecture

WaveSyncDB sits between your application code and SeaORM. From your code's perspective it's a `ConnectionTrait`. Underneath, every write is parsed, recorded in a shadow table, and dispatched to a libp2p engine that fans the change out to peers.

## The write path

```mermaid
flowchart TD
    App["App writes via SeaORM<br/>task.insert(&db).await?"]
    App --> Wrapper["WaveSyncDb<br/>(ConnectionTrait wrapper)"]
    Wrapper -->|"executes SQL"| SQLite[("Local SQLite")]
    Wrapper -->|"updates"| Shadow[("_wavesync_*_clock<br/>per-column Lamport clocks")]
    Wrapper -->|"dispatches SyncChangeset"| Engine["libp2p engine"]
    Wrapper -->|"broadcasts"| Notif["ChangeNotification"]
    Engine -->|"real-time fan-out"| RemotePeers["Remote peers"]
    Engine -.->|"catch-up via version vector"| RemotePeers
    Notif --> ReactiveUI["Reactive UI<br/>(Dioxus signals re-render)"]
```

The local write commits before any network I/O. If every peer is unreachable, your application keeps working — the change waits in the shadow table until a peer reconnects and asks for it.

## Two sync paths

### Real-time fan-out

```mermaid
sequenceDiagram
    participant A as Peer A
    participant B as Peer B
    A->>A: local write commits
    A->>B: SyncRequest::Push(changeset)
    B->>B: verify HMAC, check topic
    B->>B: apply_remote_changeset() in txn
    B-->>A: SyncResponse::PushAck
```

Each local write produces a `SyncChangeset` that is sent to every currently-connected peer via libp2p's request-response protocol. Receivers verify the HMAC, check the topic matches, then call `apply_remote_changeset()` and respond with `PushAck`. This is the fast path for live collaboration: typically <100 ms when peers are directly connected.

### Catch-up via version vector

```mermaid
sequenceDiagram
    participant A as Peer A
    participant B as Peer B
    Note over A,B: A reconnects after partition (or 30 s tick)
    A->>B: VersionVector { my=42, your_last=37, topic, hmac }
    B->>B: SELECT * FROM shadow WHERE db_version > 37
    B-->>A: ChangesetResponse { changes, my_db_version=50 }
    A->>A: verify HMAC, apply in txn
    A->>A: peer_versions[B] = 50
```

A new peer with no entry in `_wavesync_peer_versions` sends `your_last_db_version = 0`, which the receiver interprets as "give me everything". This is the only initial-state-transfer mechanism — there is no separate snapshot protocol.

## Shadow tables

For each synced table `tasks`, WaveSyncDB creates `_wavesync_tasks_clock` with primary key `(pk, cid)` (row id + column id). Each row records the column's current value, its Lamport clock (`col_version`), and the site id of the last writer.

The shadow table is what makes per-column conflict resolution work, what lets a peer answer "give me everything since version N" with a fast indexed query, and what lets WaveSyncDB resume sync correctly after a restart (because `db_version` is also persisted on every increment).

See [Schema & registration](/docs/schema) for the exact shadow-table schema.

## What you don't have to think about

- **Connection management** — the engine handles dial loops, reconnection, NAT traversal, and relay reservations.
- **Schema migration timing** — `get_schema_registry().sync()` creates entity tables and shadow tables atomically before the engine accepts inbound writes.
- **Ordering** — peers can apply changesets in any order; the per-column total ordering ensures everyone converges.
- **Idempotency** — duplicate deliveries are no-ops; the shadow-table comparison rejects equal-or-stale incoming changes.

## Further reading

- [Sync protocol](/docs/sync-protocol) — the wire format and message types.
- [Conflict resolution](/docs/conflict-resolution) — the deterministic ordering that makes convergence work.
- [Networking & discovery](/docs/networking) — how peers find each other.
- [API reference](/docs/api-reference) — the types you actually call.
