# Changelog

## v0.3.0 — 25/02/2026

### Architecture rewrite: SeaORM connection wrapper

Replaced the custom `CrudModel`/`SyncedModel` trait system (sqlx-based) with a
transparent SeaORM connection wrapper. Applications now use standard SeaORM
entities and operations — sync happens automatically under the hood.

#### Added
- `WaveSyncDb` — implements SeaORM `ConnectionTrait`, intercepts writes and dispatches sync operations
- `WaveSyncDbBuilder` — one-call setup: `WaveSyncDbBuilder::new(url, topic).build().await`
- `SchemaBuilder` — fluent API for registering entities (`.register()`, `.register_local()`, `.sync()`)
- `#[derive(SyncEntity)]` — proc macro for auto-discovery via `db.get_schema_registry()`
- `TableRegistry` — tracks which tables participate in sync
- LWW conflict resolution with hybrid logical clocks (`uhlc` crate)
- Persistent `_wavesync_log` table for operation history and incremental sync
- Full sync protocol types (`SyncRequest`, `SyncResponse`, `TableSnapshot`) — WIP
- NAT traversal behaviours (relay, dcutr, autonat) integrated into the P2P engine
- Dioxus hooks: `use_synced_table`, `use_synced_row`, `launch()`, `use_db()`
- Comprehensive documentation: crate-level docs, module docs, doc comments on all public types

#### Changed
- Replaced `sqlx` with `sea-orm 2.0.0-rc` (features: `sqlx-sqlite`, `runtime-tokio`, `macros`)
- Replaced custom `CrudModel`/`SyncedModel` traits with standard `DeriveEntityModel`
- P2P engine now receives operations via mpsc channel instead of static TX
- Conflict resolution changed from single-writer-per-object (SWPO) to Last-Write-Wins (LWW)

#### Removed
- `wavesyncdb/src/crud.rs` — old `CrudModel` trait
- `wavesyncdb/src/sync.rs` — old `SyncedModel` trait
- `wavesyncdb/src/instrument.rs` — Diesel instrumentation layer
- `wavesyncdb/src/error.rs` — custom error types (now uses `sea_orm::DbErr`)
- `wavesyncdb_derive/tests/` — old derive macro tests (replaced by integration tests)
- `examples/p2p/src/model.rs` — old model with `CrudModel` derive

---

### 12/02/2026
- Added dioxus signals

### 10/02/2026
- Fixed some derive erros
- Tested p2p example: The data does not correctly sync :(

### 09/02/2026
- Changed the format from instrumenting an existing database connection to implementing basic Crud operations to simplify development and add custom features
- Added the `Crud` trait to implement basic CRUD operations
- Added the `Synced` traid to implement the syncronization features on top of crud
- Changed the SyncEngine to use an static TX channel and the new Crud trait to handle syncronization
- Replaced `diesel` for `sqlx` as the main sql database connection
- Added the `Operation` enum as the main synced message to sync and execute the network operations safetly (I hope to protect better against sql injections)
- Started to sketch the derive trait to easily implement the `Crud` and `Synced` traits
