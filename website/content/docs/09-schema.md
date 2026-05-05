# Schema & registration

Before WaveSyncDB can sync a table, it needs to know:

1. The table name.
2. The primary key column.
3. Every other column (so it can produce per-column shadow rows on every write).
4. The delete policy (`DeleteWins` vs `AddWins`).

You can give it this metadata explicitly or, more commonly, derive it automatically from your SeaORM entity via `#[derive(SyncEntity)]`.

## The auto-discovery path

```rust
use sea_orm::entity::prelude::*;
use wavesyncdb_derive::SyncEntity;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
#[sea_orm(table_name = "tasks")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub title: String,
    pub completed: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
```

Then at startup:

```rust
db.get_schema_registry(module_path!().split("::").next().unwrap())
    .sync()
    .await?;
```

This:

1. Scans the `inventory` registry — every type that derived `SyncEntity` registered itself there at compile time, scoped by crate name.
2. Filters by the prefix you passed (so libraries don't accidentally pollute the registry).
3. Creates the entity tables if they don't exist.
4. Creates the matching shadow tables (`_wavesync_{table}_clock` and `_wavesync_meta`).
5. Marks the registry as ready, which unblocks the engine from accepting inbound writes.

The `module_path!().split("::").next().unwrap()` trick is just shorthand for "use my own crate's name". You can pass any string literal there.

## The explicit path

For dynamic-schema cases (when tables aren't known at compile time, e.g., user-defined sheets in a spreadsheet app), you can register manually:

```rust
db.get_schema_registry("my_crate")
    .register::<task::Entity>()
    .register::<note::Entity>()
    .register_local::<settings::Entity>() // exists in DB, NOT replicated
    .sync()
    .await?;
```

Both paths converge — `sync()` always issues the same `CREATE TABLE` + `CREATE TABLE _wavesync_*` statements.

## Local-only tables

`register_local::<E>()` creates the entity table the same way as `register::<E>()` does, but skips the shadow table and tells the connection wrapper to **not intercept writes** for this table. Use it for:

- App settings that are device-specific (theme, last-opened tab).
- Caches and ephemeral state.
- Tables that already have their own sync logic.

A write to a local-only table behaves exactly like raw SeaORM — no shadow rows, no broadcast, no fan-out.

## Primary keys

WaveSyncDB requires a primary key. It can be:

- A **string** (UUIDs are a great choice).
- An **integer** (but **not auto-increment** — see below).

```rust
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, SyncEntity)]
#[sea_orm(table_name = "items")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: String,
    pub label: String,
}
```

### Why not auto-increment?

Auto-increment integer PKs are a problem in any distributed system: two peers offline both insert "next available id" and end up colliding when they reconnect. WaveSyncDB doesn't try to solve this — instead, it asks you to pick a collision-free id scheme yourself. The standard answer is `Uuid::new_v4().to_string()`.

If you have an existing schema with auto-increment PKs and can't change them, you can still use WaveSyncDB, but every peer must own a disjoint id range (e.g., peer A uses 1–999_999, peer B uses 1_000_000–1_999_999). This is fragile; UUIDs are better.

## Composite primary keys

Composite PKs (multiple columns marked `primary_key`) are **not currently supported**. The shadow table assumes a single PK column. If you need them, derive a synthetic single-column PK by concatenating, e.g. `id = format!("{}-{}", customer_id, order_id)`, and store the original components as regular columns.

## Schema migration

When you add or remove columns from an entity:

1. **Adding a column** is safe. Old peers running pre-migration code keep writing with the old schema; the new column reads as NULL on their writes, which is fine.
2. **Removing a column** is mostly safe but old peers will keep emitting writes for it. The new code will receive those changes and discard them silently (the column doesn't exist locally any more). No data loss, but wasted bandwidth until every peer is upgraded.
3. **Renaming a column** is **NOT supported in place**. Add the new column, migrate data, eventually drop the old one in a later release.
4. **Changing a column type** must be handled at the application layer — WaveSyncDB stores values as raw bytes per `DataType` and doesn't enforce type compatibility on the receiver side.

There is no migration version negotiation in the protocol. Schema agreement is your responsibility. In practice, this means: ship a release that's backward-compatible with N-1, wait for everyone to upgrade, then ship N+1 that drops the back-compat shim.

## Shadow tables — what they look like

For each synced table `tasks`, WaveSyncDB creates `_wavesync_tasks_clock`:

```sql
CREATE TABLE _wavesync_tasks_clock (
    pk           TEXT NOT NULL,
    cid          INTEGER NOT NULL,        -- column id, stable per-table mapping
    col_version  INTEGER NOT NULL,        -- Lamport clock for this (row, column)
    site_id      BLOB NOT NULL,           -- node id of the last writer
    value_bytes  BLOB,                    -- raw bytes; NULL = column-level tombstone
    db_version   INTEGER NOT NULL,        -- monotonic per-DB write counter
    PRIMARY KEY (pk, cid)
);
CREATE INDEX _wavesync_tasks_clock_db_version ON _wavesync_tasks_clock (db_version);
```

The `(pk, cid)` primary key gives O(log N) per-column lookups for conflict resolution; the `db_version` index makes catch-up `WHERE db_version > N` queries cheap. There is also a single `_wavesync_meta` table that holds the global `db_version` counter and persisted peer-version map.

You don't read or write these tables directly. Treating them as opaque is the contract — schema-internal layout is allowed to change between minor releases.

## Why per-column instead of per-row?

If you used a single timestamp + winner per row (the "last write wins" pattern), concurrent edits to different columns would discard one of them. Per-column conflict resolution preserves both. The cost is roughly N times more shadow rows where N is the average column count — usually 5–15 columns, so 5–15× shadow size. SQLite handles this without breaking a sweat.

## Where to go from here

- [Conflict resolution](/docs/conflict-resolution) — what happens when two peers update the same column at the same time.
- [Sync protocol](/docs/sync-protocol) — how shadow-table state translates to wire format.
- [API reference](/docs/api-reference) — `SchemaBuilder`, `TableMeta`, `register_table`.
