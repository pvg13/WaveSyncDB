/**
 * SqliteTranslator — WatermelonDB query → SQL translation
 *
 * Strategy: We import WatermelonDB's own `encodeQuery` from its SQLite adapter.
 * This function takes a SerializedQuery and returns [sql, args] where args is
 * always an empty array (values are embedded inline in the SQL string).
 *
 * This avoids reimplementing SQL generation for WatermelonDB's query DSL
 * (WHERE, AND, OR, JOIN, ORDER BY, LIMIT, etc.) which is non-trivial.
 *
 * For the `count` variant, pass `countMode = true` which wraps the query in
 * `SELECT COUNT(*) as "count"`.
 */

// WatermelonDB's encodeQuery is a JS module (Flow-typed), so we use require().
// It returns [sql: string, args: any[]] — args is always [] since values are
// encoded inline via sql-escape-string.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const wmdbEncodeQuery: (query: any, countMode?: boolean) => [string, any[]] =
  require('@nozbe/watermelondb/src/adapters/sqlite/encodeQuery').default;

/**
 * Encode a WatermelonDB SerializedQuery into a SQL string.
 * @returns The SQL string ready to pass to WaveSync.query()
 */
export function encodeQuery(query: any, countMode: boolean = false): string {
  const [sql] = wmdbEncodeQuery(query, countMode);
  return sql;
}

/**
 * Encode a WatermelonDB SerializedQuery into a SQL string that selects only ids.
 * Replaces `SELECT "table".* FROM` with `SELECT "table"."id" FROM`.
 */
export function encodeQueryIds(query: any): string {
  const sql = encodeQuery(query);
  // encodeQuery produces: select "table".* from "table" ...
  // or: select distinct "table".* from "table" ...
  // We replace the select target with just the id column
  return sql.replace(
    /select (distinct )?"([^"]+)"\.\*/,
    'select $1"$2"."id"',
  );
}
