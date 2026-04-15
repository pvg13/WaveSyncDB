/**
 * WaveSyncAdapter — A WatermelonDB DatabaseAdapter backed by WaveSyncDB.
 *
 * All writes go through WaveSyncDB's CRDT interceptor so they replicate P2P
 * automatically. The developer uses WatermelonDB's standard API (database.write,
 * withObservables, useQuery) and sync is invisible.
 *
 * Reactivity: For local writes, WatermelonDB's `Database.batch()` handles
 * notifications internally. For remote changes arriving via P2P sync (written
 * directly to SQLite by the Rust layer), this adapter bridges
 * `WaveSync.onChangeEvent()` into WatermelonDB's internal notification system
 * so that `query().observe()` and `withObservables` re-render automatically.
 * Call `setDatabase(db)` after construction to enable this.
 */

import { WaveSync, type ChangeNotification } from '@wavesync/react-native';
import { encodeQuery, encodeQueryIds } from './SqliteTranslator';
import type {
  AppSchema,
  TableSchema,
  ColumnSchema,
  RawRecord,
  BatchOperation,
  ResultCallback,
} from './types';

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface WaveSyncAdapterOptions {
  schema: AppSchema;
  topic: string;
  passphrase: string;
  relayAddr?: string;
  rendezvousAddr?: string;
  dbPath?: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function escapeValue(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'number') return String(value);
  if (typeof value === 'boolean') return value ? '1' : '0';
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sqlTypeForColumn(col: ColumnSchema): string {
  if (col.isOptional) {
    switch (col.type) {
      case 'string':
        return 'TEXT';
      case 'number':
        return 'REAL';
      case 'boolean':
        return 'INTEGER';
    }
  }
  switch (col.type) {
    case 'string':
      return "TEXT NOT NULL DEFAULT ''";
    case 'number':
      return 'REAL NOT NULL DEFAULT 0';
    case 'boolean':
      return 'INTEGER NOT NULL DEFAULT 0';
  }
}

function buildCreateTableSql(table: TableSchema): string {
  const columnDefs = [
    'id TEXT PRIMARY KEY NOT NULL',
    "_status TEXT NOT NULL DEFAULT ''",
    "_changed TEXT NOT NULL DEFAULT ''",
    ...table.columnArray.map(
      (col) => `"${col.name}" ${sqlTypeForColumn(col)}`,
    ),
  ];
  return `CREATE TABLE IF NOT EXISTS "${table.name}" (${columnDefs.join(', ')})`;
}

function buildInsertSql(table: string, raw: RawRecord, schema: TableSchema): string {
  const userCols = schema.columnArray.map((c) => c.name);
  const allCols = ['id', '_status', '_changed', ...userCols];
  const colsSql = allCols.map((c) => `"${c}"`).join(', ');
  const valsSql = allCols.map((c) => escapeValue(raw[c])).join(', ');
  return `INSERT INTO "${table}" (${colsSql}) VALUES (${valsSql})`;
}

function buildUpdateSql(table: string, raw: RawRecord, schema: TableSchema): string {
  const userCols = schema.columnArray.map((c) => c.name);
  const setClauses = ['_status', '_changed', ...userCols]
    .map((c) => `"${c}" = ${escapeValue(raw[c])}`)
    .join(', ');
  return `UPDATE "${table}" SET ${setClauses} WHERE "id" = ${escapeValue(raw.id)}`;
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

export class WaveSyncAdapter {
  schema: AppSchema;
  dbName: string;
  migrations: undefined;

  private _options: WaveSyncAdapterOptions;
  private _initPromise: Promise<void>;

  // Remote change propagation — bridges WaveSync CRDT events to WatermelonDB
  private _database: any = null;
  private _changeSubscription: { remove: () => void } | null = null;
  private _pendingChanges = new Map<string, Map<string, ChangeNotification>>();
  private _pendingTimers = new Map<string, ReturnType<typeof setTimeout>>();

  constructor(options: WaveSyncAdapterOptions) {
    this._options = options;
    this.schema = options.schema;
    this.dbName = options.dbPath ?? 'wavesync';
    this.migrations = undefined;

    // Kick off async initialization. WatermelonDB waits for this via
    // DatabaseAdapterCompat wrapping toPromise over setUpWithSchema, but
    // the modern path just needs the adapter to be ready before first use.
    this._initPromise = this._setUp();
  }

  /** Resolves once WaveSync is initialized and all tables are registered. */
  get initializingPromise(): Promise<void> {
    return this._initPromise;
  }

  /**
   * Connect this adapter to its owning WatermelonDB Database instance.
   * Must be called after `new Database({ adapter })` so that remote CRDT
   * changes can propagate into WatermelonDB's reactive system.
   *
   * Usage:
   *   const adapter = new WaveSyncAdapter({ ... });
   *   const database = new Database({ adapter: adapter as any, modelClasses });
   *   adapter.setDatabase(database);
   */
  setDatabase(database: any): void {
    this._database = database;
    this._startRemoteChangeListener();
  }

  /**
   * Tear down the remote change listener and clear pending notifications.
   * Call this before discarding the adapter to avoid leaking subscriptions.
   */
  tearDown(): void {
    this._changeSubscription?.remove();
    this._changeSubscription = null;
    for (const timer of this._pendingTimers.values()) {
      clearTimeout(timer);
    }
    this._pendingTimers.clear();
    this._pendingChanges.clear();
  }

  // -----------------------------------------------------------------------
  // Setup
  // -----------------------------------------------------------------------

  private async _setUp(): Promise<void> {
    const { topic, passphrase, relayAddr, rendezvousAddr } = this._options;

    await WaveSync.initialize(topic, {
      passphrase,
      relayServer: relayAddr,
      rendezvousServer: rendezvousAddr,
    });

    // Subscribe to change notifications BEFORE registering tables, so the
    // broadcast receiver is listening before registryReady() triggers sync.
    // Without this, catch-up sync events during setup are silently lost.
    await WaveSync.subscribeChanges();

    // Create and register each user table with WaveSyncDB.
    // registerSyncedTable creates both the user table and the CRDT shadow
    // clock table (_wavesync_<table>_clock) used for version vector sync.
    for (const table of Object.values(this.schema.tables)) {
      const createSql = buildCreateTableSql(table);
      const columns = table.columnArray.map((c) => c.name);
      await WaveSync.registerSyncedTable(table.name, 'id', columns, createSql);
    }

    // Create local KV table (not synced — internal WatermelonDB metadata)
    await WaveSync.execute(
      'CREATE TABLE IF NOT EXISTS "_wavesync_local_kv" ("key" TEXT PRIMARY KEY NOT NULL, "value" TEXT NOT NULL)',
    );

    await WaveSync.registryReady();
  }

  // -----------------------------------------------------------------------
  // DatabaseAdapter interface
  // -----------------------------------------------------------------------

  find(
    table: string,
    id: string,
    callback: ResultCallback<string | Record<string, unknown> | undefined>,
  ): void {
    this._wrap(callback, async () => {
      const rows = await WaveSync.query(
        `SELECT * FROM "${table}" WHERE "id" = ${escapeValue(id)} LIMIT 1`,
      );
      return rows.length > 0 ? (rows[0] as Record<string, unknown>) : undefined;
    });
  }

  query(query: any, callback: ResultCallback<any[]>): void {
    this._wrap(callback, async () => {
      const sql = encodeQuery(query);
      return WaveSync.query(sql);
    });
  }

  queryIds(query: any, callback: ResultCallback<string[]>): void {
    this._wrap(callback, async () => {
      const sql = encodeQueryIds(query);
      const rows = await WaveSync.query<{ id: string }>(sql);
      return rows.map((r) => r.id);
    });
  }

  unsafeQueryRaw(query: any, callback: ResultCallback<any[]>): void {
    this._wrap(callback, async () => {
      const sql = encodeQuery(query);
      return WaveSync.query(sql);
    });
  }

  count(query: any, callback: ResultCallback<number>): void {
    this._wrap(callback, async () => {
      const sql = encodeQuery(query, true);
      const rows = await WaveSync.query<{ count: number }>(sql);
      return rows[0]?.count ?? 0;
    });
  }

  batch(operations: BatchOperation[], callback: ResultCallback<void>): void {
    // Each operation is executed individually (no explicit BEGIN/COMMIT)
    // because dispatch_sync emits change notifications and sends changesets
    // to peers immediately.  Wrapping in a transaction would cause those
    // notifications to fire before COMMIT, and on ROLLBACK the changesets
    // would already have been sent to peers — creating phantom records.
    // Individual auto-committed writes avoid this; CRDT convergence handles
    // any partial-apply scenarios.
    this._wrap(callback, async () => {
      for (const op of operations) {
        const [type, table] = op;
        switch (type) {
          case 'create': {
            const raw = op[2] as RawRecord;
            const tableSchema = this.schema.tables[table];
            await WaveSync.execute(buildInsertSql(table, raw, tableSchema));
            break;
          }
          case 'update': {
            const raw = op[2] as RawRecord;
            const tableSchema = this.schema.tables[table];
            await WaveSync.execute(buildUpdateSql(table, raw, tableSchema));
            break;
          }
          case 'markAsDeleted': {
            const id = op[2] as string;
            await WaveSync.execute(
              `UPDATE "${table}" SET "_status" = 'deleted' WHERE "id" = ${escapeValue(id)}`,
            );
            break;
          }
          case 'destroyPermanently': {
            const id = op[2] as string;
            await WaveSync.execute(
              `DELETE FROM "${table}" WHERE "id" = ${escapeValue(id)}`,
            );
            break;
          }
        }
      }
    });
  }

  getDeletedRecords(table: string, callback: ResultCallback<string[]>): void {
    this._wrap(callback, async () => {
      const rows = await WaveSync.query<{ id: string }>(
        `SELECT "id" FROM "${table}" WHERE "_status" = 'deleted'`,
      );
      return rows.map((r) => r.id);
    });
  }

  destroyDeletedRecords(
    table: string,
    recordIds: string[],
    callback: ResultCallback<void>,
  ): void {
    this._wrap(callback, async () => {
      if (recordIds.length === 0) return;
      const ids = recordIds.map(escapeValue).join(', ');
      await WaveSync.execute(
        `DELETE FROM "${table}" WHERE "id" IN (${ids})`,
      );
    });
  }

  unsafeLoadFromSync(_jsonId: number, callback: ResultCallback<any>): void {
    callback({ error: new Error('unsafeLoadFromSync is not supported by WaveSyncAdapter') });
  }

  provideSyncJson(
    _id: number,
    _syncPullResultJson: string,
    callback: ResultCallback<void>,
  ): void {
    callback({ error: new Error('provideSyncJson is not supported by WaveSyncAdapter') });
  }

  unsafeResetDatabase(callback: ResultCallback<void>): void {
    this._wrap(callback, async () => {
      // Drop all user tables
      for (const table of Object.values(this.schema.tables)) {
        await WaveSync.execute(`DROP TABLE IF EXISTS "${table.name}"`);
      }
      // Drop local KV
      await WaveSync.execute('DROP TABLE IF EXISTS "_wavesync_local_kv"');
      // Recreate
      await this._setUp();
    });
  }

  unsafeExecute(
    work: { sqls?: [string, any[]][]; sqlString?: string },
    callback: ResultCallback<void>,
  ): void {
    this._wrap(callback, async () => {
      if (work.sqls) {
        for (const [sql] of work.sqls) {
          await WaveSync.execute(sql);
        }
      } else if (work.sqlString) {
        // Split on semicolons and execute each statement
        const statements = work.sqlString
          .split(';')
          .map((s) => s.trim())
          .filter(Boolean);
        for (const stmt of statements) {
          await WaveSync.execute(stmt);
        }
      }
    });
  }

  getLocal(key: string, callback: ResultCallback<string | null>): void {
    this._wrap(callback, async () => {
      const rows = await WaveSync.query<{ value: string }>(
        `SELECT "value" FROM "_wavesync_local_kv" WHERE "key" = ${escapeValue(key)}`,
      );
      return rows.length > 0 ? rows[0].value : null;
    });
  }

  setLocal(key: string, value: string, callback: ResultCallback<void>): void {
    this._wrap(callback, async () => {
      await WaveSync.execute(
        `INSERT OR REPLACE INTO "_wavesync_local_kv" ("key", "value") VALUES (${escapeValue(key)}, ${escapeValue(value)})`,
      );
    });
  }

  removeLocal(key: string, callback: ResultCallback<void>): void {
    this._wrap(callback, async () => {
      await WaveSync.execute(
        `DELETE FROM "_wavesync_local_kv" WHERE "key" = ${escapeValue(key)}`,
      );
    });
  }

  // -----------------------------------------------------------------------
  // Internal
  // -----------------------------------------------------------------------

  private _wrap<T>(callback: ResultCallback<T>, fn: () => Promise<T>): void {
    fn()
      .then((value) => callback({ value }))
      .catch((error) =>
        callback({ error: error instanceof Error ? error : new Error(String(error)) }),
      );
  }

  // -----------------------------------------------------------------------
  // Remote change propagation
  // -----------------------------------------------------------------------

  private _startRemoteChangeListener(): void {
    this._changeSubscription?.remove();
    this._changeSubscription = WaveSync.onChangeEvent((notification) => {
      this._enqueueChange(notification);
    });

    // After subscribing, do a one-time full refresh for each table.
    // This picks up any records that were synced to SQLite during
    // initialization (before the event listener or Database were ready).
    this._initPromise.then(() => {
      for (const table of Object.values(this.schema.tables)) {
        this._enqueueFullTableRefresh(table.name);
      }
    });
  }

  /**
   * Query all records in a table from SQLite and notify WatermelonDB about
   * any that exist in the DB but aren't in the in-memory cache. This handles
   * records that arrived via catch-up sync before the change listener was
   * ready, or before setDatabase() was called.
   */
  private async _enqueueFullTableRefresh(tableName: string): Promise<void> {
    const db = this._database;
    if (!db) return;

    let collection: any;
    try {
      collection = db.collections.get(tableName);
    } catch {
      return;
    }

    const rows = await WaveSync.query(
      `SELECT * FROM "${tableName}"`,
    );
    if (rows.length === 0) return;

    const changeSet: Array<{ record: any; type: string }> = [];

    for (const row of rows) {
      const id = (row as any).id;
      if (!id) continue;

      const cached = collection._cache.get(id);
      if (cached) {
        // Record in cache — update its raw data in case it's stale
        Object.assign(cached._raw, row);
        changeSet.push({ record: cached, type: 'updated' });
      } else {
        // Record in SQLite but not in cache — create Model
        const record = collection._cache._modelForRaw(row, false);
        changeSet.push({ record, type: 'created' });
      }
    }

    if (changeSet.length === 0) return;

    collection._applyChangesToCache(changeSet);
    db._notify([[tableName, changeSet]]);
  }

  /**
   * Buffer incoming change notifications and debounce per table.
   * A 50ms window coalesces burst syncs (e.g. a peer catching up after being
   * offline) into a single re-render per table.
   */
  private _enqueueChange(notification: ChangeNotification): void {
    const { table } = notification;

    if (!this._pendingChanges.has(table)) {
      this._pendingChanges.set(table, new Map());
    }
    // Last notification wins per primaryKey (e.g. INSERT then UPDATE → keep UPDATE)
    this._pendingChanges.get(table)!.set(notification.primaryKey, notification);

    const existing = this._pendingTimers.get(table);
    if (existing) clearTimeout(existing);

    this._pendingTimers.set(
      table,
      setTimeout(() => {
        this._pendingTimers.delete(table);
        const changes = this._pendingChanges.get(table);
        this._pendingChanges.delete(table);
        if (changes) {
          this._flushTableChanges(table, Array.from(changes.values()));
        }
      }, 50),
    );
  }

  /**
   * Apply a batch of remote changes to WatermelonDB's cache and notification
   * system. This is the core bridge: it translates WaveSyncDB CRDT events into
   * the exact same internal calls that Database.batch() makes after a local
   * write, so query().observe() and withObservables subscribers re-render.
   */
  private async _flushTableChanges(
    tableName: string,
    changes: ChangeNotification[],
  ): Promise<void> {
    const db = this._database;
    if (!db) return;

    let collection: any;
    try {
      collection = db.collections.get(tableName);
    } catch {
      return; // table has no registered WatermelonDB Model class
    }

    const changeSet: Array<{ record: any; type: string }> = [];

    for (const change of changes) {
      const { kind, primaryKey } = change;

      if (kind === 'DELETE') {
        const cachedRecord = collection._cache.get(primaryKey);
        if (cachedRecord) {
          changeSet.push({ record: cachedRecord, type: 'destroyed' });
        }
        // If not cached, no observer holds this record — nothing to notify.
      } else {
        // INSERT or UPDATE — WaveSyncDB emits INSERT for both new records
        // and catch-up/update syncs, so we must distinguish by checking
        // whether the record is already cached.
        const cachedRecord = collection._cache.get(primaryKey);
        const rows = await WaveSync.query(
          `SELECT * FROM "${tableName}" WHERE "id" = ${escapeValue(primaryKey)} LIMIT 1`,
        );
        if (rows.length === 0) continue;

        if (cachedRecord) {
          // Record exists in cache — update its raw data in-place so
          // observers see fresh values when _notifyChanged() fires.
          Object.assign(cachedRecord._raw, rows[0]);
          changeSet.push({ record: cachedRecord, type: 'updated' });
        } else {
          // New record — create a Model and add to cache.
          const record = collection._cache._modelForRaw(rows[0], false);
          changeSet.push({ record, type: 'created' });
        }
      }
    }

    if (changeSet.length === 0) return;

    // Mirror what Database.batch() does after the adapter call succeeds:
    // 1. Update caches (add created records, remove destroyed records)
    collection._applyChangesToCache(changeSet);
    // 2. Notify database-level subscribers (reloading queries re-fetch)
    //    and collection-level subscribers (simple queries update incrementally)
    db._notify([[tableName, changeSet]]);
  }
}
