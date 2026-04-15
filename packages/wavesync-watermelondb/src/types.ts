/**
 * Shared types for the WaveSyncAdapter.
 *
 * We avoid importing WatermelonDB's Flow-typed internals directly and instead
 * define the subset of types we actually depend on. This keeps the package
 * decoupled from WatermelonDB's internal type representation (which is Flow,
 * not TypeScript).
 */

/** Column type as declared in a WatermelonDB tableSchema */
export type ColumnType = 'string' | 'number' | 'boolean';

export interface ColumnSchema {
  name: string;
  type: ColumnType;
  isOptional?: boolean;
  isIndexed?: boolean;
}

export interface TableSchema {
  name: string;
  columns: Record<string, ColumnSchema>;
  columnArray: ColumnSchema[];
}

export interface AppSchema {
  version: number;
  tables: Record<string, TableSchema>;
}

/** The raw record object WatermelonDB passes through batch operations */
export interface RawRecord {
  id: string;
  _status: string;
  _changed: string;
  [column: string]: unknown;
}

export type BatchOperation =
  | ['create', string, RawRecord]
  | ['update', string, RawRecord]
  | ['markAsDeleted', string, string]
  | ['destroyPermanently', string, string];

/** Callback convention used by WatermelonDB adapters */
export type Result<T> =
  | { value: T; error?: undefined }
  | { error: Error; value?: undefined };

export type ResultCallback<T> = (result: Result<T>) => void;
