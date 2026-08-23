// graph.ts
//
// Section 7 of Zero-Cost-Stack-v11.md: Decision & Relationship Memory
// (Graph RAG). This is the application layer on top of neo4j.ts's raw
// Cypher access -- the same relationship codecells.ts has to turso.ts.
// Following that module's convention (not neo4j.ts's own): the exported
// functions here THROW plain Errors on failure rather than returning an
// "Error ..." string themselves; it's index.ts's tool handlers that catch
// and format the string, exactly like cell_create/checkpoint_write do.
//
// The one deliberate exception is embedding failure (Section 7d): an
// embedding-provider outage must never block a Decision/Error from being
// recorded, so embedWithFailover() below returns a null vector instead of
// throwing, and node writes proceed anyway with embedding_pending = true.
//
// Non-overlap discipline this module enforces by omission (Section 7b, 7g):
//   - Nothing here ever writes Turso's authoritative `status` column, and
//     nothing here is called from anywhere that would let it. A Task
//     node's own `status` property, if set, is denormalized
//     graph-traversal context only -- see writeTaskNode.
//   - Nothing here is wired to checkpoint_write's `rationale` field.
//     Promoting curated checkpoint rationale into a Decision node is a
//     judgment call an Architect/Reviewer session makes explicitly, by
//     calling writeDecisionNode after reading Turso during its own Orient
//     step -- never an automatic per-checkpoint write.

import type { Neo4jEnv } from "./neo4j";
import { runCypher, rowsToObjects } from "./neo4j";
import type { GeminiEnv } from "./gemini";
import { geminiEmbedContent } from "./gemini";

export interface GraphEnv extends Neo4jEnv, GeminiEnv {}

// ---- Schema constants ---------------------------------------------------

const NODE_LABELS = ["Decision", "Error", "Task", "CodeFile"] as const;
export type NodeLabel = (typeof NODE_LABELS)[number];

const REL_TYPES = ["DEPENDS_ON", "BLOCKS", "IMPLEMENTS", "AFFECTS"] as const;
export type RelType = (typeof REL_TYPES)[number];

function requireLabel(label: string): NodeLabel {
  if (!(NODE_LABELS as readonly string[]).includes(label)) {
    throw new Error(`Invalid node label '${label}'. Must be one of: ${NODE_LABELS.join(", ")}.`);
  }
  return label as NodeLabel;
}

function requireRelType(relType: string): RelType {
  if (!(REL_TYPES as readonly string[]).includes(relType)) {
    throw new Error(`Invalid relationship type '${relType}'. Must be one of: ${REL_TYPES.join(", ")}.`);
  }
  return relType as RelType;
}

export interface RelationshipSpec {
  type: RelType;
  direction: "out" | "in"; // "out": (this node)-[type]->(target); "in": (target)-[type]->(this node)
  toLabel: NodeLabel;
  toId: string;
}

// ---- Embedding: Gemini Embedding 1 <-> 2 failover pair (Section 7a, 7d) -

// "Embedding 1" / "Embedding 2" per Section 7a's component table map onto
// the current Gemini Embedding model family as: gemini-embedding-001 (the
// original, generally-available, text-only model) and
// gemini-embedding-2-flash-001 (the newer natively multimodal model --
// Section 7a-i). Both are reached through the existing gemini_embed_content
// wrapper (gemini.ts) -- this module never talks to the Gemini API
// directly, per the cell prompt's instruction not to duplicate that call
// logic. If Google renames/retires either model string, update these two
// constants; nothing else in this file assumes a specific model name.
export const EMBEDDING_MODEL_1 = "gemini-embedding-001";
export const EMBEDDING_MODEL_2 = "gemini-embedding-2-flash-001";
const EMBEDDING_PAIR = [EMBEDDING_MODEL_1, EMBEDDING_MODEL_2] as const;

export interface EmbedResult {
  vector: number[] | null;
  model: string | null;
  errors: string[]; // one entry per failed attempt (diagnostics only -- never stored on a node)
}

// Tries Embedding 1, then Embedding 2, on the SAME request (Section 7d:
// "the two share the same interface and node schema, so this is a
// same-request retry, not a queued one"). Never throws -- an embedding
// failure must never block the node write that calls this.
export async function embedWithFailover(
  env: GraphEnv,
  text: string,
  taskType: string = "RETRIEVAL_DOCUMENT",
): Promise<EmbedResult> {
  const errors: string[] = [];
  const trimmed = (text || "").trim();
  if (!trimmed) {
    return { vector: null, model: null, errors: ["No content to embed (empty text)."] };
  }
  for (const model of EMBEDDING_PAIR) {
    const raw = await geminiEmbedContent(env, model, trimmed, taskType);
    // geminiEmbedContent (gemini.ts) never throws -- on failure it returns
    // a string prefixed "Error calling Gemini embedContent: ...". On
    // success it returns a JSON-stringified array of floats.
    if (raw.startsWith("Error calling Gemini embedContent")) {
      errors.push(`${model}: ${raw}`);
      continue;
    }
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed) && parsed.length > 0 && parsed.every((v) => typeof v === "number")) {
        return { vector: parsed, model, errors };
      }
      errors.push(`${model}: response wasn't a numeric vector: ${raw.slice(0, 200)}`);
    } catch {
      errors.push(`${model}: response wasn't valid JSON: ${raw.slice(0, 200)}`);
    }
  }
  return { vector: null, model: null, errors };
}

// ---- Schema setup (constraints + vector indexes) -------------------------
//
// Idempotent (IF NOT EXISTS throughout) -- cheap to call before every
// write, same shape as codecells.ts's ensureSchema() before createCell().
// Cached per-isolate purely as a speed optimization; re-running is always
// safe since every statement is IF NOT EXISTS.
let schemaEnsured = false;

export async function ensureGraphSchema(env: GraphEnv): Promise<void> {
  if (schemaEnsured) return;
  for (const label of NODE_LABELS) {
    await runCypher(
      env,
      `CREATE CONSTRAINT ${label.toLowerCase()}_id_unique IF NOT EXISTS FOR (n:${label}) REQUIRE n.id IS UNIQUE`,
    );
  }
  // Vector indexes: Decision/Error/CodeFile are the node types Section 7a
  // expects to carry a semantic embedding. Task nodes are structural
  // (Section 7b) and are never embedded, so no vector index for them.
  // Dimension 3072 matches Gemini Embedding's default output size
  // (Section 7a: "single 3072-dimensional vector space").
  for (const label of ["Decision", "Error", "CodeFile"] as const) {
    await runCypher(
      env,
      `CREATE VECTOR INDEX ${label.toLowerCase()}_embedding_idx IF NOT EXISTS
       FOR (n:${label}) ON (n.embedding)
       OPTIONS { indexConfig: { \`vector.dimensions\`: 3072, \`vector.similarity_function\`: 'cosine' } }`,
    );
  }
  schemaEnsured = true;
}

// ---- Generic node + relationship writers ---------------------------------

export interface WriteNodeResult {
  id: string;
  embeddingPending: boolean;
  embeddingModel: string | null;
  embeddingErrors: string[];
}

async function writeNode(
  env: GraphEnv,
  label: NodeLabel,
  id: string,
  properties: Record<string, unknown>,
  embedText: string | null,
  relationships: RelationshipSpec[],
): Promise<WriteNodeResult> {
  requireLabel(label);
  await ensureGraphSchema(env);

  let vector: number[] | null = null;
  let model: string | null = null;
  let errors: string[] = [];
  if (embedText) {
    const embedded = await embedWithFailover(env, embedText);
    vector = embedded.vector;
    model = embedded.model;
    errors = embedded.errors;
  }

  const props: Record<string, unknown> = {
    ...properties,
    id,
    created_at: new Date().toISOString(),
    embedding_pending: embedText ? vector === null : false,
  };
  // Kept on the node (not just in-memory) so the backfill route can retry
  // later without needing to reconstruct the embeddable text from other
  // fields -- Section 7d's cron backstop reads this back.
  if (embedText) props.embed_text = embedText;
  if (vector) {
    props.embedding = vector;
    props.embedding_model = model;
  }

  await runCypher(env, `CREATE (n:${label} $props)`, { props });

  for (const rel of relationships) {
    await writeRelationship(env, label, id, rel);
  }

  return {
    id,
    embeddingPending: !!props.embedding_pending,
    embeddingModel: model,
    embeddingErrors: errors,
  };
}

export async function writeRelationship(
  env: GraphEnv,
  fromLabel: NodeLabel,
  fromId: string,
  rel: RelationshipSpec,
): Promise<void> {
  requireLabel(fromLabel);
  requireLabel(rel.toLabel);
  requireRelType(rel.type);
  const cypher =
    rel.direction === "out"
      ? `MATCH (a:${fromLabel} {id: $fromId}), (b:${rel.toLabel} {id: $toId}) MERGE (a)-[r:${rel.type}]->(b) RETURN type(r) AS t`
      : `MATCH (a:${fromLabel} {id: $fromId}), (b:${rel.toLabel} {id: $toId}) MERGE (b)-[r:${rel.type}]->(a) RETURN type(r) AS t`;
  const { rows } = await runCypher(env, cypher, { fromId, toId: rel.toId });
  if (rows.length === 0) {
    throw new Error(
      `Relationship not created: no ${fromLabel} with id '${fromId}' and/or ${rel.toLabel} with id '${rel.toId}' found.`,
    );
  }
}

// Stand-alone entry point for the graph_write_relationship tool -- same
// underlying writer as writeNode()'s inline `relationships` array, just
// for linking two nodes that already exist rather than one just created
// in this same call.
export async function writeRelationshipStandalone(
  env: GraphEnv,
  fromLabel: string,
  fromId: string,
  toLabel: string,
  toId: string,
  type: string,
  direction: "out" | "in" = "out",
): Promise<void> {
  await ensureGraphSchema(env);
  await writeRelationship(env, requireLabel(fromLabel), fromId, {
    type: requireRelType(type),
    direction,
    toLabel: requireLabel(toLabel),
    toId,
  });
}

// ---- Decision / Error / Task / CodeFile writers --------------------------

export interface WriteDecisionArgs {
  id?: string;
  title: string;
  rationale: string;
  role?: string; // e.g. "Architect", "Reviewer" -- who made the call
  cellId?: number; // Turso code_cells.id this decision originated from, if any (Section 7b reference link)
  relationships?: RelationshipSpec[];
}

export async function writeDecisionNode(env: GraphEnv, args: WriteDecisionArgs): Promise<WriteNodeResult> {
  if (!args.title?.trim()) throw new Error("title is required.");
  if (!args.rationale?.trim() || args.rationale.trim().length < 10) {
    throw new Error(
      "rationale is required (min 10 chars) -- a Decision node without a real 'why' defeats the point of Section 7.",
    );
  }
  const id = args.id || crypto.randomUUID();
  const embedText = `${args.title}\n\n${args.rationale}`;
  return writeNode(
    env,
    "Decision",
    id,
    { title: args.title, rationale: args.rationale, role: args.role ?? null, cell_id: args.cellId ?? null },
    embedText,
    args.relationships ?? [],
  );
}

export interface WriteErrorArgs {
  id?: string;
  message: string;
  context?: string;
  cellId?: number;
  relationships?: RelationshipSpec[];
}

export async function writeErrorNode(env: GraphEnv, args: WriteErrorArgs): Promise<WriteNodeResult> {
  if (!args.message?.trim()) throw new Error("message is required.");
  const id = args.id || crypto.randomUUID();
  const embedText = args.context ? `${args.message}\n\n${args.context}` : args.message;
  return writeNode(
    env,
    "Error",
    id,
    { message: args.message, context: args.context ?? null, cell_id: args.cellId ?? null },
    embedText,
    args.relationships ?? [],
  );
}

export interface WriteTaskArgs {
  id?: string;
  title: string;
  cellId?: number; // Turso code_cells.id -- Section 7b's reference-only link back to the authoritative row
  status?: string; // DENORMALIZED CONTEXT ONLY -- see module header. Orchestration logic must keep reading Turso's status column, never this.
  relationships?: RelationshipSpec[];
}

export async function writeTaskNode(env: GraphEnv, args: WriteTaskArgs): Promise<WriteNodeResult> {
  if (!args.title?.trim()) throw new Error("title is required.");
  const id = args.id || crypto.randomUUID();
  // Tasks are structural, not content (Section 7b) -- no embedding, and
  // no vector index exists for the Task label (ensureGraphSchema), so
  // there's no Gemini call to make here at all.
  return writeNode(
    env,
    "Task",
    id,
    { title: args.title, cell_id: args.cellId ?? null, status: args.status ?? null },
    null,
    args.relationships ?? [],
  );
}

export interface WriteCodeFileArgs {
  id?: string;
  path: string;
  description?: string; // text description/summary to embed (Section 7a: prose about the file, or a code excerpt)
  cellId?: number;
  // R2 object key for a screenshot/video/audio clip this file relates to
  // (Section 7a-i: "store the artifact in R2 first, embed a reference").
  // Stored as a plain reference property -- NOT itself embedded by this
  // function. Multimodal embedding needs gemini.ts's embedContent call
  // extended to send inline_data/file-reference parts instead of plain
  // text, which is out of scope for this cell (see summary at the end of
  // this module's PR). `description` is what actually gets embedded.
  mediaRef?: string;
  relationships?: RelationshipSpec[];
}

export async function writeCodeFileNode(env: GraphEnv, args: WriteCodeFileArgs): Promise<WriteNodeResult> {
  if (!args.path?.trim()) throw new Error("path is required.");
  const id = args.id || crypto.randomUUID();
  return writeNode(
    env,
    "CodeFile",
    id,
    {
      path: args.path,
      description: args.description ?? null,
      cell_id: args.cellId ?? null,
      media_ref: args.mediaRef ?? null,
    },
    args.description ?? null,
    args.relationships ?? [],
  );
}

// ---- Orient (retrieval) step (Section 7c) --------------------------------

export interface OrientHit {
  id: string;
  label: string;
  title?: string | null;
  message?: string | null;
  score: number;
}

export interface OrientBlockedTask {
  byDecisionId: string;
  id: string;
  title: string;
  status: string | null;
  cellId: number | null;
}

export interface OrientTouchedCodeFile {
  byId: string;
  id: string;
  path: string;
}

export interface OrientResult {
  query: string;
  embeddingModel: string | null;
  degraded: string | null; // set when query-embedding failed and this fell back to recency ordering instead of vector search
  hits: OrientHit[];
  blockedTasks: OrientBlockedTask[];
  touchedCodeFiles: OrientTouchedCodeFile[];
  narrative: string[];
}

// Two-part query: vector search for decisions/errors conceptually related
// to `queryText`, then graph traversal from those hits to find what Tasks
// they BLOCK and what CodeFiles they touch (AFFECTS/IMPLEMENTS) -- folded
// into a narrative line per hit, e.g. "Decision 'Use JWT' currently blocks
// Task 'Wire up auth middleware'."
export async function graphOrient(env: GraphEnv, queryText: string, limit: number = 5): Promise<OrientResult> {
  await ensureGraphSchema(env);
  const embedded = await embedWithFailover(env, queryText, "RETRIEVAL_QUERY");

  let hits: OrientHit[] = [];
  let degraded: string | null = null;

  if (embedded.vector) {
    for (const label of ["Decision", "Error"] as const) {
      const { fields, rows } = await runCypher(
        env,
        `CALL db.index.vector.queryNodes($indexName, $limit, $vector) YIELD node, score
         RETURN node.id AS id, labels(node)[0] AS label, node.title AS title, node.message AS message, score`,
        { indexName: `${label.toLowerCase()}_embedding_idx`, limit, vector: embedded.vector },
      );
      hits.push(...(rowsToObjects(fields, rows) as unknown as OrientHit[]));
    }
    hits.sort((a, b) => b.score - a.score);
    hits = hits.slice(0, limit);
  } else {
    degraded =
      `Query embedding failed on both models (${embedded.errors.join(" | ")}); ` +
      `falling back to the most recently created Decision/Error nodes instead of semantic search.`;
    const { fields, rows } = await runCypher(
      env,
      `MATCH (n) WHERE n:Decision OR n:Error
       RETURN n.id AS id, labels(n)[0] AS label, n.title AS title, n.message AS message, 0.0 AS score
       ORDER BY n.created_at DESC LIMIT $limit`,
      { limit },
    );
    hits = rowsToObjects(fields, rows) as unknown as OrientHit[];
  }

  const hitIds = hits.map((h) => h.id);
  let blockedTasks: OrientBlockedTask[] = [];
  let touchedCodeFiles: OrientTouchedCodeFile[] = [];

  if (hitIds.length > 0) {
    const { fields: taskFields, rows: taskRows } = await runCypher(
      env,
      `MATCH (src)-[:BLOCKS]->(t:Task) WHERE src.id IN $hitIds
       RETURN src.id AS byDecisionId, t.id AS id, t.title AS title, t.status AS status, t.cell_id AS cellId`,
      { hitIds },
    );
    blockedTasks = rowsToObjects(taskFields, taskRows) as unknown as OrientBlockedTask[];

    const { fields: fileFields, rows: fileRows } = await runCypher(
      env,
      `MATCH (src)-[:AFFECTS|IMPLEMENTS]-(f:CodeFile) WHERE src.id IN $hitIds
       RETURN src.id AS byId, f.id AS id, f.path AS path`,
      { hitIds },
    );
    touchedCodeFiles = rowsToObjects(fileFields, fileRows) as unknown as OrientTouchedCodeFile[];
  }

  const narrative: string[] = hits.map((h) => {
    const name = h.title ?? h.message ?? h.id;
    const blocks = blockedTasks.filter((t) => t.byDecisionId === h.id);
    if (blocks.length === 0) {
      return `${h.label} '${name}' (score ${h.score.toFixed(3)}) -- no blocked tasks found in the graph.`;
    }
    return blocks
      .map(
        (t) =>
          `${h.label} '${name}' currently blocks Task '${t.title}'` +
          `${t.status ? ` (status: ${t.status})` : ""}` +
          `${t.cellId != null ? `, Turso cell #${t.cellId}` : ""}.`,
      )
      .join(" ");
  });

  return { query: queryText, embeddingModel: embedded.model, degraded, hits, blockedTasks, touchedCodeFiles, narrative };
}

// ---- Embedding backfill (Section 7d) -------------------------------------

export interface BackfillResult {
  scanned: number;
  fixed: number;
  stillPending: number;
  details: string[];
}

// Scans for embedding_pending nodes and retries the Embedding 1<->2 pair,
// clearing the flag on success. Driven by a low-frequency QStash schedule
// hitting /webhook/graph-embedding-backfill (auth.ts) -- same pattern as
// Section 4a's stuck-Pending sweep.
export async function backfillPendingEmbeddings(env: GraphEnv, limit: number = 25): Promise<BackfillResult> {
  await ensureGraphSchema(env);
  const { fields, rows } = await runCypher(
    env,
    `MATCH (n) WHERE n.embedding_pending = true AND n.embed_text IS NOT NULL
     RETURN n.id AS id, labels(n)[0] AS label, n.embed_text AS text
     LIMIT $limit`,
    { limit },
  );
  const pending = rowsToObjects(fields, rows) as unknown as { id: string; label: string; text: string }[];

  let fixed = 0;
  const details: string[] = [];
  for (const row of pending) {
    const embedded = await embedWithFailover(env, row.text);
    if (embedded.vector) {
      await runCypher(
        env,
        `MATCH (n {id: $id}) SET n.embedding = $vector, n.embedding_model = $model, n.embedding_pending = false`,
        { id: row.id, vector: embedded.vector, model: embedded.model },
      );
      fixed++;
      details.push(`${row.label} ${row.id}: backfilled via ${embedded.model}.`);
    } else {
      details.push(`${row.label} ${row.id}: still failing (${embedded.errors.join(" | ")}).`);
    }
  }

  return { scanned: pending.length, fixed, stillPending: pending.length - fixed, details };
}

// ---- Keepalive (Section 7f) ----------------------------------------------

// Touches a dedicated singleton node so a quiet month of real project work
// doesn't let the free AuraDB instance cross its 30-day inactivity window
// and get deleted outright. Driven by a weekly QStash schedule hitting
// /webhook/graph-heartbeat (auth.ts).
export async function touchHeartbeat(env: GraphEnv): Promise<{ touchedAt: string; count: number }> {
  await ensureGraphSchema(env);
  const { fields, rows } = await runCypher(
    env,
    `MERGE (h:Heartbeat {key: '_heartbeat'})
     ON CREATE SET h.count = 1
     ON MATCH SET h.count = h.count + 1
     SET h.touched_at = datetime()
     RETURN toString(h.touched_at) AS touchedAt, h.count AS count`,
  );
  const obj = rowsToObjects(fields, rows)[0] as { touchedAt: string; count: number };
  return { touchedAt: obj.touchedAt, count: Number(obj.count) };
}
