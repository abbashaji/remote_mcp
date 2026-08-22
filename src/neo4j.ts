// neo4j.ts
//
// Talks to Neo4j over its HTTP Query API (POST /db/{database}/query/v2),
// NOT the Bolt driver -- Bolt needs a raw TCP socket, which Workers can't
// open. The Query API is plain HTTPS/JSON and works fine from fetch(),
// same reasoning as turso.ts using @libsql/client/web's Hrana-over-HTTP
// transport instead of a TCP libSQL connection.
//
// Works against Aura (neo4j+s://xxxx.databases.neo4j.io) and self-hosted
// instances with the Query API enabled (Neo4j 5.x+). Auth is HTTP Basic
// (username/password), sent per-request -- there's no persistent
// connection to cache, just a resolved base URL + credentials.

export interface Neo4jEnv {
  NEO4J_URI?: string; // e.g. "neo4j+s://xxxx.databases.neo4j.io" or "https://host:7474"
  NEO4J_USERNAME?: string;
  NEO4J_PASSWORD?: string;
  NEO4J_DATABASE?: string; // default "neo4j"
}

interface Neo4jTarget {
  httpBase: string; // e.g. "https://xxxx.databases.neo4j.io"
  username: string;
  password: string;
  database: string;
}

// Query API results are typed JSON by default (application/json), where
// each row is an object keyed by the RETURN aliases -- no separate
// header/rows array to zip together like the old transactional endpoint.
interface QueryApiResponse {
  data?: { fields: string[]; values: unknown[][] };
  errors?: { message: string; code?: string }[];
}

// neo4j+s:// / neo4j:// / bolt+s:// / bolt:// all resolve to the same
// hostname as the instance's HTTPS/Query-API listener (this is how Aura
// is set up, and the documented convention for self-hosted too) -- swap
// the scheme and drop any bolt-specific port.
function deriveHttpBase(uri: string): string {
  const m = uri.match(/^([a-z0-9+]+):\/\/([^/]+)/i);
  if (!m) {
    throw new Error(
      `NEO4J_URI '${uri}' doesn't look like a Neo4j connection URI (expected e.g. "neo4j+s://xxxx.databases.neo4j.io").`,
    );
  }
  const [, scheme, hostport] = m;
  if (/^https?$/i.test(scheme)) return `${scheme.toLowerCase()}://${hostport}`;
  const host = hostport.replace(/:\d+$/, ""); // strip a bolt port like :7687
  return `https://${host}`;
}

function resolveTarget(env: Neo4jEnv): Neo4jTarget {
  const uri = env.NEO4J_URI || "";
  const username = env.NEO4J_USERNAME || "";
  const password = env.NEO4J_PASSWORD || "";
  if (!uri || !username || !password) {
    throw new Error(
      "Neo4j is not configured. Set NEO4J_URI, NEO4J_USERNAME, and NEO4J_PASSWORD as Worker secrets (and optionally NEO4J_DATABASE, default 'neo4j').",
    );
  }
  return { httpBase: deriveHttpBase(uri), username, password, database: env.NEO4J_DATABASE || "neo4j" };
}

async function runCypher(
  env: Neo4jEnv,
  statement: string,
  parameters: Record<string, unknown> = {},
): Promise<{ fields: string[]; rows: unknown[][] }> {
  const target = resolveTarget(env);
  const resp = await fetch(`${target.httpBase}/db/${encodeURIComponent(target.database)}/query/v2`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      Authorization: "Basic " + btoa(`${target.username}:${target.password}`),
    },
    body: JSON.stringify({ statement, parameters }),
  });

  const body = (await resp.json().catch(() => ({}))) as QueryApiResponse;
  if (!resp.ok || body.errors?.length) {
    const msg = body.errors?.map((e) => e.message).join("; ") || `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return { fields: body.data?.fields ?? [], rows: body.data?.values ?? [] };
}

function rowsToObjects(fields: string[], rows: unknown[][]): Record<string, unknown>[] {
  return rows.map((row) => Object.fromEntries(fields.map((f, i) => [f, row[i]])));
}

export function neo4jCurrentDatabase(env: Neo4jEnv): string {
  if (!env.NEO4J_URI) {
    return "No Neo4j instance configured. Set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD as Worker secrets.";
  }
  try {
    const target = resolveTarget(env);
    return `Connected to: ${target.httpBase} (database '${target.database}', user '${target.username}')`;
  } catch (e) {
    return `Error: ${e}`;
  }
}

export async function neo4jExecuteQuery(
  env: Neo4jEnv,
  cypher: string,
  parameters: Record<string, unknown> = {},
): Promise<string> {
  try {
    const { fields, rows } = await runCypher(env, cypher, parameters);
    if (rows.length === 0) return "Query returned no rows.";
    return JSON.stringify(rowsToObjects(fields, rows), null, 2);
  } catch (e) {
    return `Error executing query: ${e}`;
  }
}

// Schema discovery without requiring APOC: db.labels()/relationshipTypes()/
// propertyKeys() are built-in procedures on every Neo4j 5.x instance. If
// APOC *is* installed, apoc.meta.schema() gives a much richer picture
// (property types, relationship direction/counts) -- try it first and
// fall back gracefully.
export async function neo4jGetSchema(env: Neo4jEnv): Promise<string> {
  try {
    try {
      const { fields, rows } = await runCypher(env, "CALL apoc.meta.schema() YIELD value RETURN value");
      return JSON.stringify(rowsToObjects(fields, rows)[0]?.value ?? {}, null, 2);
    } catch {
      // APOC not installed (or meta.schema disabled) -- fall back to the
      // built-in procedures every instance has.
    }
    const [labels, relTypes, propKeys] = await Promise.all([
      runCypher(env, "CALL db.labels() YIELD label RETURN label"),
      runCypher(env, "CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType"),
      runCypher(env, "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey"),
    ]);
    return JSON.stringify(
      {
        labels: labels.rows.map((r) => r[0]),
        relationshipTypes: relTypes.rows.map((r) => r[0]),
        propertyKeys: propKeys.rows.map((r) => r[0]),
        note: "APOC not detected -- install it for richer schema (property types, relationship shape) via apoc.meta.schema().",
      },
      null,
      2,
    );
  } catch (e) {
    return `Error getting schema: ${e}`;
  }
}
