// codecells.ts
//
// Section 4a/5/5b of Zero-Cost-Stack-v11.md: the shared task table this
// server's CodeCellWorkflow (code_cell_workflow.ts) reads and writes.
// Built directly on turso.ts's existing client/helpers rather than a
// second HTTP client -- same identifier-safety and error-shape
// conventions as the rest of this project.

import { createClient, type Client } from "@libsql/client/web";
import type { TursoEnv } from "./turso";
import { b2PutObject, bytesToBase64, type B2Env } from "./b2";

// Re-resolves its own client rather than importing turso.ts's private
// cachedClient -- cheap (fetch-based, no socket), and keeps this module
// independent of turso_open_database()'s per-session override behavior,
// which shouldn't affect the workflow's own database target.
function getWorkflowClient(env: TursoEnv): Client {
  if (!env.TURSO_DATABASE_URL) {
    throw new Error("TURSO_DATABASE_URL is not configured on this Worker.");
  }
  return createClient(
    env.TURSO_AUTH_TOKEN
      ? { url: env.TURSO_DATABASE_URL, authToken: env.TURSO_AUTH_TOKEN }
      : { url: env.TURSO_DATABASE_URL },
  );
}

export async function ensureSchema(env: TursoEnv): Promise<void> {
  const client = getWorkflowClient(env);
  await client.batch(
    [
      `CREATE TABLE IF NOT EXISTS code_cells (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT NOT NULL DEFAULT 'Pending'
          CHECK (status IN (
            'Pending','Processing_Planned','Processing_Drafting','Processing_SelfTested',
            'Code_Ready','Testing','Failed','Completed','Dead_Letter'
          )),
        role TEXT NOT NULL DEFAULT 'Architect',
        spec TEXT NOT NULL,
        code TEXT,
        provider TEXT,
        tag TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        locked_by TEXT,
        locked_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      )`,
      `CREATE TABLE IF NOT EXISTS checkpoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cell_id INTEGER NOT NULL REFERENCES code_cells(id),
        phase TEXT NOT NULL,
        session_id TEXT NOT NULL,
        artifact TEXT,
        next_action TEXT,
        decision_notes TEXT,
        draft_committed INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
      )`,
    ],
    "write",
  );
  // Idempotent column additions for the optional B2 artifact-routing
  // integration below -- a plain CREATE TABLE IF NOT EXISTS above won't
  // add columns to a `checkpoints` table that already existed before
  // this feature shipped, so these run as their own best-effort
  // ALTER TABLEs, swallowing "column already exists" specifically.
  await ensureArtifactColumns(client);
}

async function ensureArtifactColumns(client: Client): Promise<void> {
  const alters = [
    `ALTER TABLE checkpoints ADD COLUMN artifact_provider TEXT NOT NULL DEFAULT 'inline'`,
    `ALTER TABLE checkpoints ADD COLUMN artifact_key TEXT`,
  ];
  for (const sql of alters) {
    try {
      await client.execute(sql);
    } catch (e) {
      // SQLite/libSQL's error text for a column that's already there --
      // anything else is a real problem and should surface.
      if (!String(e).toLowerCase().includes("duplicate column")) throw e;
    }
  }
}

export interface CellRow {
  id: number;
  status: string;
  role: string;
  spec: string;
  code: string | null;
  provider: string | null;
  tag: string | null;
  retry_count: number;
  last_error: string | null;
  updated_at: string;
}

export async function createCell(env: TursoEnv, spec: string, role: string): Promise<number> {
  const client = getWorkflowClient(env);
  const rs = await client.execute({
    sql: "INSERT INTO code_cells (spec, role) VALUES (?, ?) RETURNING id",
    args: [spec, role],
  });
  return Number(rs.rows[0].id);
}

export async function updateCell(
  env: TursoEnv,
  cellId: number,
  fields: Partial<{
    status: string;
    code: string;
    provider: string;
    tag: string;
    retry_count: number;
    last_error: string | null;
  }>,
): Promise<void> {
  const client = getWorkflowClient(env);
  const columns = Object.keys(fields);
  if (columns.length === 0) return;
  const setClause = columns.map((c) => `"${c}" = ?`).join(", ") + ", updated_at = datetime('now')";
  await client.execute({
    sql: `UPDATE code_cells SET ${setClause} WHERE id = ?`,
    args: [...(Object.values(fields) as any[]), cellId],
  });
}

export async function getCell(env: TursoEnv, cellId: number): Promise<CellRow | null> {
  const client = getWorkflowClient(env);
  const rs = await client.execute({ sql: "SELECT * FROM code_cells WHERE id = ?", args: [cellId] });
  return (rs.rows[0] as unknown as CellRow) ?? null;
}

// Section 5b/5c: generic resume query -- non-terminal cells first, stale
// locks (untouched >10 min) prioritized over fresh ones.
export async function resumeCandidate(env: TursoEnv): Promise<CellRow | null> {
  const client = getWorkflowClient(env);
  const rs = await client.execute(
    `SELECT * FROM code_cells
     WHERE status NOT IN ('Completed','Dead_Letter')
     ORDER BY (locked_by IS NOT NULL AND locked_at < datetime('now','-10 minutes')) DESC, updated_at ASC
     LIMIT 1`,
  );
  return (rs.rows[0] as unknown as CellRow) ?? null;
}

// Section 9's "if genuinely unsure, default to B2" rule, applied here:
// a checkpoint artifact bigger than this is routed to B2 rather than
// stored inline in the `checkpoints.artifact` TEXT column. 4KB is well
// under any Turso row-size concern -- this is about keeping checkpoint
// rows small and queryable, not working around a hard limit.
const ARTIFACT_B2_THRESHOLD_BYTES = 4096;

function b2Configured(env: Partial<B2Env>): boolean {
  return !!(env.B2_KEY_ID && env.B2_APPLICATION_KEY && env.B2_BUCKET_NAME && env.B2_ENDPOINT);
}

// Section 5c: a checkpoint without a real rationale is worse than no
// checkpoint -- it looks resumable but tells the next session nothing.
//
// Section 9 integration (optional scope from the cell prompt): a large
// `artifact` payload is routed to B2 by default rather than stored
// inline, per the "if genuinely unsure, default to B2" rule -- Turso
// keeps state ABOUT the artifact (artifact_provider/artifact_key),
// never a second copy of the bytes. B2 routing is attempted only when
// B2 is actually configured on this Worker, and any failure (missing
// config, a failed upload) falls back to inline storage rather than
// blocking the checkpoint write -- same "storage-routing failure never
// blocks the write" discipline Section 7d already applies to graph.ts's
// embedding calls.
export async function writeCheckpoint(
  env: TursoEnv & Partial<B2Env>,
  args: {
    cellId: number;
    phase: string;
    sessionId: string;
    artifact?: string;
    nextAction?: string;
    rationale: string;
  },
): Promise<void> {
  if (!args.rationale || args.rationale.trim().length < 10) {
    throw new Error("rationale is required (min 10 chars) -- Section 5c");
  }
  const client = getWorkflowClient(env);
  await ensureArtifactColumns(client);

  let artifactProvider = "inline";
  let artifactKey: string | null = null;
  let artifactColumnValue = args.artifact ?? "";

  if (args.artifact) {
    const artifactBytes = new TextEncoder().encode(args.artifact);
    if (artifactBytes.length > ARTIFACT_B2_THRESHOLD_BYTES && b2Configured(env)) {
      const key = `checkpoints/cell-${args.cellId}/${args.sessionId}-${Date.now()}.txt`;
      const result = await b2PutObject(env, key, bytesToBase64(artifactBytes), "text/plain");
      if (result.startsWith("Error")) {
        // Never let a B2 hiccup block the checkpoint write -- fall back
        // to inline storage, same as if B2 had never been configured.
        console.error(`B2 checkpoint-artifact routing failed for CodeCell #${args.cellId} (falling back to inline): ${result}`);
      } else {
        artifactProvider = "b2";
        artifactKey = key;
        artifactColumnValue = `[stored in B2: ${key}]`;
      }
    }
  }

  await client.execute({
    sql: `INSERT INTO checkpoints (cell_id, phase, session_id, artifact, next_action, decision_notes, draft_committed, artifact_provider, artifact_key)
          VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)`,
    args: [
      args.cellId,
      args.phase,
      args.sessionId,
      artifactColumnValue,
      args.nextAction ?? "",
      args.rationale,
      artifactProvider,
      artifactKey,
    ],
  });
}
