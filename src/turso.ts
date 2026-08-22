// turso.ts
//
// Direct port of turso_tools.py's 9 tools onto @libsql/client/web, the
// fetch-based build of the official Turso client that works in
// Cloudflare Workers (no TCP sockets needed -- Turso speaks Hrana over
// HTTP). Same identifier-safety rules as the Python version: table/
// column names are validated against a strict regex before ever being
// string-interpolated into SQL; values always go through bound params.

import { createClient, type Client } from "@libsql/client/web";

const IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]*$/;

function requireIdentifier(name: string, what: string = "name"): string {
  if (!IDENTIFIER_RE.test(name)) {
    throw new Error(
      `Invalid ${what} '${name}': must be a plain identifier (letters, digits, underscore, not starting with a digit).`,
    );
  }
  return name;
}

export interface TursoEnv {
  TURSO_DATABASE_URL?: string;
  TURSO_AUTH_TOKEN?: string;
}

// Cached per-isolate client, mirrors the Python module's cached _conn.
// Cheap to recreate (it's just an HTTP client, not a live socket), but
// caching avoids rebuilding it on every tool call within one isolate.
let cachedClient: Client | null = null;
let cachedUrl: string | null = null;

function resolveTarget(
  env: TursoEnv,
  databaseUrl?: string,
  authToken?: string,
): { url: string; token: string } {
  const url = databaseUrl || env.TURSO_DATABASE_URL || "";
  const token = authToken || env.TURSO_AUTH_TOKEN || "";
  if (!url) {
    throw new Error(
      "No database configured. Set TURSO_DATABASE_URL (and TURSO_AUTH_TOKEN, for a remote database) as Worker secrets, or call turso_open_database() with an explicit database_url first.",
    );
  }
  return { url, token };
}

function getClient(env: TursoEnv, databaseUrl?: string, authToken?: string): Client {
  const { url, token } = resolveTarget(env, databaseUrl, authToken);
  if (cachedClient && cachedUrl === url && !databaseUrl) return cachedClient;

  const client = createClient(token ? { url, authToken: token } : { url });
  if (!databaseUrl) {
    cachedClient = client;
    cachedUrl = url;
  }
  return client;
}

export async function tursoOpenDatabase(
  env: TursoEnv,
  databaseUrl: string = "",
  authToken: string = "",
): Promise<string> {
  try {
    const { url, token } = resolveTarget(env, databaseUrl, authToken);
    const client = createClient(token ? { url, authToken: token } : { url });
    cachedClient = client;
    cachedUrl = url;
    return `Opened database: ${url}`;
  } catch (e) {
    return `Error opening database: ${e}`;
  }
}

export function tursoCurrentDatabase(env: TursoEnv): string {
  if (cachedUrl === null) {
    if (env.TURSO_DATABASE_URL) {
      return `Not yet connected this session. Would default to TURSO_DATABASE_URL: ${env.TURSO_DATABASE_URL}`;
    }
    return "No database configured yet. Call turso_open_database() or set TURSO_DATABASE_URL as a Worker secret.";
  }
  return `Currently connected to: ${cachedUrl}`;
}

export async function tursoListTables(env: TursoEnv): Promise<string> {
  try {
    const client = getClient(env);
    const rs = await client.execute(
      "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    );
    if (rs.rows.length === 0) return "No tables found.";
    return rs.rows.map((r) => String(r.name)).join("\n");
  } catch (e) {
    return `Error listing tables: ${e}`;
  }
}

export async function tursoDescribeTable(env: TursoEnv, tableName: string): Promise<string> {
  try {
    requireIdentifier(tableName, "table name");
    const client = getClient(env);
    const rs = await client.execute(`PRAGMA table_info("${tableName}")`);
    if (rs.rows.length === 0) return `Table '${tableName}' not found (or has no columns).`;
    return JSON.stringify(rs.rows, null, 2);
  } catch (e) {
    return `Error describing table: ${e}`;
  }
}

export async function tursoExecuteQuery(env: TursoEnv, sql: string): Promise<string> {
  try {
    const stripped = sql.trim().replace(/;$/, "").trim();
    const firstWord = (stripped.split(/\s+/, 1)[0] || "").toLowerCase();
    if (!["select", "pragma", "explain", "with"].includes(firstWord)) {
      return "Refused: turso_execute_query only runs read-only statements (SELECT/WITH/PRAGMA/EXPLAIN). Use turso_insert_data, turso_update_data, turso_delete_data, or turso_schema_change for writes.";
    }
    const client = getClient(env);
    const rs = await client.execute(stripped);
    return JSON.stringify(rs.rows, null, 2);
  } catch (e) {
    return `Error executing query: ${e}`;
  }
}

export async function tursoInsertData(
  env: TursoEnv,
  table: string,
  data: Record<string, unknown>,
): Promise<string> {
  try {
    requireIdentifier(table, "table name");
    const columns = Object.keys(data);
    if (columns.length === 0) return "Refused: data must be a non-empty object of column -> value.";
    columns.forEach((c) => requireIdentifier(c, "column name"));
    const placeholders = columns.map(() => "?").join(", ");
    const colList = columns.map((c) => `"${c}"`).join(", ");
    const sql = `INSERT INTO "${table}" (${colList}) VALUES (${placeholders})`;
    const client = getClient(env);
    await client.execute({ sql, args: Object.values(data) as any[] });
    return `Inserted 1 row into '${table}'.`;
  } catch (e) {
    return `Error inserting data: ${e}`;
  }
}

export async function tursoUpdateData(
  env: TursoEnv,
  table: string,
  data: Record<string, unknown>,
  where: string,
): Promise<string> {
  try {
    requireIdentifier(table, "table name");
    const columns = Object.keys(data);
    if (columns.length === 0) return "Refused: data must be a non-empty object of column -> value.";
    if (!where || !where.trim()) {
      return "Refused: 'where' is required (pass \"1=1\" explicitly to update every row on purpose).";
    }
    columns.forEach((c) => requireIdentifier(c, "column name"));
    const setClause = columns.map((c) => `"${c}" = ?`).join(", ");
    const sql = `UPDATE "${table}" SET ${setClause} WHERE ${where}`;
    const client = getClient(env);
    const rs = await client.execute({ sql, args: Object.values(data) as any[] });
    return `Updated ${rs.rowsAffected} row(s) in '${table}'.`;
  } catch (e) {
    return `Error updating data: ${e}`;
  }
}

export async function tursoDeleteData(env: TursoEnv, table: string, where: string): Promise<string> {
  try {
    requireIdentifier(table, "table name");
    if (!where || !where.trim()) {
      return "Refused: 'where' is required (pass \"1=1\" explicitly to delete every row on purpose).";
    }
    const sql = `DELETE FROM "${table}" WHERE ${where}`;
    const client = getClient(env);
    const rs = await client.execute(sql);
    return `Deleted ${rs.rowsAffected} row(s) from '${table}'.`;
  } catch (e) {
    return `Error deleting data: ${e}`;
  }
}

export async function tursoSchemaChange(env: TursoEnv, sql: string): Promise<string> {
  try {
    const stripped = sql.trim().replace(/;$/, "").trim();
    const firstWord = (stripped.split(/\s+/, 1)[0] || "").toLowerCase();
    if (!["create", "alter", "drop"].includes(firstWord)) {
      return "Refused: turso_schema_change only runs CREATE/ALTER/DROP statements. Use turso_execute_query for reads or turso_insert_data/turso_update_data/turso_delete_data for row-level writes.";
    }
    const client = getClient(env);
    await client.execute(stripped);
    return "Schema change applied.";
  } catch (e) {
    return `Error applying schema change: ${e}`;
  }
}
