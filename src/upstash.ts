// upstash.ts
//
// Direct port onto Upstash's Developer (Management) API
// (https://api.upstash.com) -- same shape as turso.ts/github.ts: stateless,
// one fetch() per call, no persistent client. This is NOT a proxy to a
// hosted remote MCP server the way cloudflare.ts is -- Upstash doesn't run
// one (mcp.upstash.com doesn't exist). Their official MCP server
// (@upstash/mcp-server) is an npm package that talks to this same REST API
// directly, so this module reimplements that.
//
// Auth: HTTP Basic, base64("<email>:<api_key>"). Both come from Worker
// secrets (UPSTASH_EMAIL / UPSTASH_API_KEY) -- never anything the MCP
// client (Claude) sees directly.
//
// IMPORTANT -- this key type is account-wide by design. Upstash's
// Developer API keys aren't scoped (role-based/read-only access is on
// their roadmap but not shipped as of writing) -- an UPSTASH_API_KEY can
// create, modify, and delete every Redis/Kafka/QStash/Vector resource on
// the account. There's no "limited" variant to opt into instead.
//
// Scope of this module: Redis database management (list/get/create/
// delete/rename, stats, password reset) -- the well-documented, stable
// subset of the API. Backup management (create/list/delete/restore
// backup) and Kafka/QStash/Vector endpoints are deliberately NOT included
// here since their exact paths weren't confirmed against current docs at
// write time; add them the same way once confirmed rather than guessing
// paths for an account-wide-privileged key.

export interface UpstashEnv {
  UPSTASH_EMAIL?: string;
  UPSTASH_API_KEY?: string;
}

const UPSTASH_API = "https://api.upstash.com";

function requireUpstashAuth(env: UpstashEnv): string {
  if (!env.UPSTASH_EMAIL || !env.UPSTASH_API_KEY) {
    throw new Error(
      "UPSTASH_EMAIL / UPSTASH_API_KEY are not configured on this Worker. Get an API key at " +
        "https://console.upstash.com/account/api (Account > Management API), then run: " +
        "wrangler secret put UPSTASH_EMAIL && wrangler secret put UPSTASH_API_KEY",
    );
  }
  // btoa is available in Workers; email:api_key must be ASCII (both are).
  return btoa(`${env.UPSTASH_EMAIL}:${env.UPSTASH_API_KEY}`);
}

async function upstashFetch(
  env: UpstashEnv,
  method: "GET" | "POST" | "DELETE",
  path: string,
  body?: unknown,
): Promise<string> {
  const basic = requireUpstashAuth(env);
  const resp = await fetch(`${UPSTASH_API}${path}`, {
    method,
    headers: {
      Authorization: `Basic ${basic}`,
      "Content-Type": "application/json",
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Upstash API ${method} ${path} returned ${resp.status}: ${text.slice(0, 500)}`);
  }
  return text;
}

function prettyJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text; // some endpoints (delete, reset-password) return plain text/empty
  }
}

export async function upstashListDatabases(env: UpstashEnv): Promise<string> {
  try {
    return prettyJson(await upstashFetch(env, "GET", "/v2/redis/databases"));
  } catch (e) {
    return `Error listing databases: ${e}`;
  }
}

export async function upstashGetDatabase(env: UpstashEnv, databaseId: string): Promise<string> {
  try {
    return prettyJson(await upstashFetch(env, "GET", `/v2/redis/database/${encodeURIComponent(databaseId)}`));
  } catch (e) {
    return `Error getting database: ${e}`;
  }
}

export async function upstashDatabaseStats(env: UpstashEnv, databaseId: string): Promise<string> {
  try {
    return prettyJson(await upstashFetch(env, "GET", `/v2/redis/stats/${encodeURIComponent(databaseId)}`));
  } catch (e) {
    return `Error getting database stats: ${e}`;
  }
}

export async function upstashCreateDatabase(
  env: UpstashEnv,
  name: string,
  region: string,
  opts?: { primaryRegion?: string; readRegions?: string[]; tls?: boolean },
): Promise<string> {
  try {
    const body: Record<string, unknown> = { name, region, tls: opts?.tls ?? true };
    if (opts?.primaryRegion) body.primary_region = opts.primaryRegion;
    if (opts?.readRegions?.length) body.read_regions = opts.readRegions;
    return prettyJson(await upstashFetch(env, "POST", "/v2/redis/database", body));
  } catch (e) {
    return `Error creating database: ${e}`;
  }
}

export async function upstashDeleteDatabase(env: UpstashEnv, databaseId: string): Promise<string> {
  try {
    await upstashFetch(env, "DELETE", `/v2/redis/database/${encodeURIComponent(databaseId)}`);
    return `Deleted database ${databaseId}.`;
  } catch (e) {
    return `Error deleting database: ${e}`;
  }
}

export async function upstashRenameDatabase(
  env: UpstashEnv,
  databaseId: string,
  newName: string,
): Promise<string> {
  try {
    return prettyJson(
      await upstashFetch(env, "POST", `/v2/redis/rename/${encodeURIComponent(databaseId)}`, { name: newName }),
    );
  } catch (e) {
    return `Error renaming database: ${e}`;
  }
}

export async function upstashResetPassword(env: UpstashEnv, databaseId: string): Promise<string> {
  try {
    return prettyJson(await upstashFetch(env, "POST", `/v2/redis/reset-password/${encodeURIComponent(databaseId)}`));
  } catch (e) {
    return `Error resetting password: ${e}`;
  }
}
