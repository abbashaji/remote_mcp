// index.ts
//
// Entry point. Wraps the stateless MCP handler (13 github_*/turso_*
// tools, no Durable Objects, no D1) with @cloudflare/workers-oauth-provider,
// which implements a real, spec-compliant OAuth 2.1 authorization server
// -- including Dynamic Client Registration (DCR). That's what Claude's
// custom connector needs: it registers itself automatically against
// /register, sends the user to /authorize, and gets back a token it can
// use on /mcp. No manual Client ID/Secret entry required.
//
// The only thing you configure is one password (MCP_AUTH_TOKEN) that
// gates the /authorize consent screen -- see auth.ts. Section 12's
// operator dashboard reuses this same password rather than adding a
// second auth system -- see dashboard.ts.

import { OAuthProvider } from "@cloudflare/workers-oauth-provider";
import { McpServer } from "@modelcontextprotocol/server";
import { createMcpHandler } from "agents/mcp/server";
import { z } from "zod";

import * as gh from "./github";
import * as ghr from "./github_release";
import * as b2 from "./b2";
import * as turso from "./turso";
import * as neo4j from "./neo4j";
import * as graph from "./graph";
import * as cf from "./cloudflare";
import * as us from "./upstash";
import * as groq from "./groq";
import * as ph from "./posthog";
import * as dc from "./discord";
import * as gemini from "./gemini";
import * as qstash from "./qstash";
import * as ag from "./antigravity";
import { AuthHandler } from "./auth";
import { JobWorkflow } from "./workflows";
import { TaskRunner, type RunnerTask } from "./runner";
import { CodeCellWorkflow } from "./code_cell_workflow";
import { DashboardHub } from "./dashboard_do";
import { ensureSchema, createCell, resumeCandidate, writeCheckpoint, getCell } from "./codecells";

// Re-export the Workflow/Durable Object classes so wrangler can find them
// as binding targets (see [[workflows]] / [[durable_objects.bindings]] +
// [[migrations]] in wrangler.toml).
export { JobWorkflow, TaskRunner, CodeCellWorkflow, DashboardHub };

export interface Env {
  OAUTH_KV: KVNamespace;
  OAUTH_PROVIDER: any; // injected by OAuthProvider at runtime
  MCP_AUTH_TOKEN?: string;
  GITHUB_TOKEN?: string;
  TURSO_DATABASE_URL?: string;
  TURSO_AUTH_TOKEN?: string;
  NEO4J_URI?: string;
  NEO4J_USERNAME?: string;
  NEO4J_PASSWORD?: string;
  NEO4J_DATABASE?: string;
  CLOUDFLARE_API_TOKEN?: string;
  UPSTASH_EMAIL?: string;
  UPSTASH_API_KEY?: string;
  GROQ_API_KEY?: string;
  POSTHOG_API_KEY?: string;
  POSTHOG_MCP_URL?: string;
  POSTHOG_PROJECT_API_KEY?: string; // Section 10: Capture API (event ingestion), NOT the same credential as POSTHOG_API_KEY above -- see posthog_events.ts
  POSTHOG_INGEST_HOST?: string; // optional, e.g. "https://eu.i.posthog.com" for EU-region accounts -- defaults to the US Capture endpoint, see posthog_events.ts
  DISCORD_BOT_TOKEN?: string;
  GEMINI_API_KEY?: string;
  QSTASH_TOKEN?: string;
  QSTASH_URL?: string; // optional region override, e.g. "https://qstash-eu-central-1.upstash.io" -- see qstash.ts
  JOB_WORKFLOW: Workflow<import("./workflows").JobWorkflowParams>;
  RUNNER: DurableObjectNamespace<TaskRunner>;
  CODE_CELL_WORKFLOW: Workflow<import("./code_cell_workflow").CodeCellWorkflowParams>;
  DASHBOARD_HUB: DurableObjectNamespace<DashboardHub>; // Section 12c: operator-dashboard poll/broadcast DO -- see dashboard_do.ts
  VAPID_PUBLIC_KEY?: string; // Web Push migration Phase 1 -- browser-facing public key, also served via GET /vapid-public-key
  VAPID_PRIVATE_KEY?: string; // Web Push migration Phase 1 -- never leaves this Worker; signs push payloads via @block65/webcrypto-web-push
  VAPID_SUBJECT?: string; // Web Push migration Phase 1 -- "mailto:you@example.com", required by the VAPID spec
  HEAVY_WORKER_REPO?: string; // "owner/name" -- repo containing .github/workflows/test.yml
  HEAVY_WORKER_CALLBACK_TOKEN?: string; // machine-to-machine secret for /webhook/heavy-worker-result
  WORKER_URL?: string; // this Worker's own https://....workers.dev base URL (Section 4f QStash self-dispatch)
  FAST_WORKER_CALLBACK_TOKEN?: string; // machine-to-machine secret for /qstash/fast-worker-generate
  FAST_WORKER_RATE_PER_MINUTE?: string; // optional, defaults to "20" -- see code_cell_workflow.ts
  FAST_WORKER_PARALLELISM?: string; // optional -- see code_cell_workflow.ts
  OPENHANDS_CALLBACK_TOKEN?: string; // Section 4g: machine-to-machine secret for /webhook/openhands-result, also set as a repo secret on whichever repo runs openhands-run.yml
  OPENHANDS_REPO?: string; // Section 4g: optional, "owner/name" of the repo containing .github/workflows/openhands-run.yml -- defaults to HEAVY_WORKER_REPO if unset, see code_cell_workflow.ts
  GITHUB_RELEASE_ARTIFACTS_REPO?: string; // optional, defaults to "abbashaji/remote_mcp" -- see github_release.ts
  GRAPH_CRON_TOKEN?: string; // machine-to-machine secret for /webhook/graph-embedding-backfill and /webhook/graph-heartbeat (Section 7d, 7f)
  B2_KEY_ID?: string; // Backblaze B2 S3-compatible application key id -- see b2.ts
  B2_APPLICATION_KEY?: string; // Backblaze B2 S3-compatible application key secret -- see b2.ts
  B2_BUCKET_NAME?: string; // Backblaze B2 bucket name -- see b2.ts
  B2_ENDPOINT?: string; // Backblaze B2 bucket's S3-compatible endpoint, e.g. "s3.us-west-004.backblazeb2.com" -- see b2.ts
}

function text(s: string) {
  return { content: [{ type: "text" as const, text: s }] };
}

function requireGithubToken(env: Env): string {
  if (!env.GITHUB_TOKEN) {
    throw new Error(
      "GITHUB_TOKEN is not configured on this Worker. Run: wrangler secret put GITHUB_TOKEN",
    );
  }
  return env.GITHUB_TOKEN;
}

function requireDiscordToken(env: Env): string {
  if (!env.DISCORD_BOT_TOKEN) {
    throw new Error(
      "DISCORD_BOT_TOKEN is not configured on this Worker. Run: wrangler secret put DISCORD_BOT_TOKEN",
    );
  }
  return env.DISCORD_BOT_TOKEN;
}

// Default artifacts repo for github_release_* tools -- same repo as the
// Worker's own source (abbashaji/remote_mcp) unless overridden by the
// optional GITHUB_RELEASE_ARTIFACTS_REPO var, per Section 9's "default
// to remote_mcp unless there's a reason not to" guidance.
function defaultReleaseRepo(env: Env): string {
  return env.GITHUB_RELEASE_ARTIFACTS_REPO || "abbashaji/remote_mcp";
}

// Shared by every graph_* tool below that accepts inline relationships --
// Section 7a's DEPENDS_ON/BLOCKS/IMPLEMENTS/AFFECTS, validated again
// (defense in depth) inside graph.ts itself.
const relationshipInputSchema = z.object({
  type: z.enum(["DEPENDS_ON", "BLOCKS", "IMPLEMENTS", "AFFECTS"]),
  direction: z.enum(["out", "in"]).default("out").describe("'out': (this node)-[type]->(target); 'in': (target)-[type]->(this node)"),
  to_label: z.enum(["Decision", "Error", "Task", "CodeFile"]),
  to_id: z.string().describe("id of the already-existing target node"),
});

function toRelationshipSpecs(
  rels: { type: "DEPENDS_ON" | "BLOCKS" | "IMPLEMENTS" | "AFFECTS"; direction: "out" | "in"; to_label: graph.NodeLabel; to_id: string }[],
): graph.RelationshipSpec[] {
  return rels.map((r) => ({ type: r.type, direction: r.direction, toLabel: r.to_label, toId: r.to_id }));
}

function buildServer(env: Env): McpServer {
  const server = new McpServer({ name: "turso-github-mcp", version: "1.0.0" });

  // ---- github_* (6 tools) ----------------------------------------

  server.registerTool(
    "github_trigger_workflow",
    {
      description: "Trigger a GitHub Actions workflow_dispatch run.",
      inputSchema: {
        repo: z.string().describe('"owner/name", e.g. "abbashaji/geonodes-interop-testing"'),
        workflow_file: z.string().describe("filename inside .github/workflows/, e.g. test.yml"),
        ref: z.string().default("main").describe("branch to run against"),
        inputs: z
          .record(z.string(), z.union([z.string(), z.number(), z.boolean()]))
          .optional()
          .describe("workflow_dispatch input values; omit for workflows with no inputs"),
      },
    },
    async ({ repo, workflow_file, ref, inputs }) =>
      text(await gh.githubTriggerWorkflow(requireGithubToken(env), repo, workflow_file, ref, inputs)),
  );

  server.registerTool(
    "github_list_recent_runs",
    {
      description: "List the most recent runs of a workflow, with status/conclusion/id.",
      inputSchema: {
        repo: z.string(),
        workflow_file: z.string(),
        limit: z.number().int().default(5),
      },
    },
    async ({ repo, workflow_file, limit }) =>
      text(await gh.githubListRecentRuns(requireGithubToken(env), repo, workflow_file, limit)),
  );

  server.registerTool(
    "github_get_run_status",
    {
      description: "Get the status/conclusion of a specific workflow run.",
      inputSchema: { repo: z.string(), run_id: z.number().int() },
    },
    async ({ repo, run_id }) => text(await gh.githubGetRunStatus(requireGithubToken(env), repo, run_id)),
  );

  server.registerTool(
    "github_get_artifact_text",
    {
      description:
        "Download a run's artifact zip and return one text file's contents (text files only).",
      inputSchema: {
        repo: z.string(),
        run_id: z.number().int(),
        artifact_name: z.string().default("test-results"),
        file_in_zip: z.string().default("output.log"),
      },
    },
    async ({ repo, run_id, artifact_name, file_in_zip }) =>
      text(
        await gh.githubGetArtifactText(
          requireGithubToken(env),
          repo,
          run_id,
          artifact_name,
          file_in_zip,
        ),
      ),
  );

  server.registerTool(
    "github_push_file",
    {
      description: "Create or update a file in the repo (handles create vs. update automatically).",
      inputSchema: {
        repo: z.string(),
        path: z.string(),
        content: z.string(),
        message: z.string().describe("commit message"),
        branch: z.string().default("main"),
      },
    },
    async ({ repo, path, content, message, branch }) =>
      text(await gh.githubPushFile(requireGithubToken(env), repo, path, content, message, branch)),
  );

  server.registerTool(
    "github_read_file",
    {
      description:
        "Read a file's content from the repo, or list a directory's contents if path is a directory.",
      inputSchema: {
        repo: z.string(),
        path: z.string().describe('file or directory path, e.g. "01_constraints.md" or "" for root'),
        ref: z.string().default("main"),
      },
    },
    async ({ repo, path, ref }) => text(await gh.githubReadFile(requireGithubToken(env), repo, path, ref)),
  );

  server.registerTool(
    "github_models_chat_completion",
    {
      description:
        "Run a chat completion via GitHub Models (models.github.ai) -- Section 2's optional secondary " +
        "tagger, reusing the same GITHUB_TOKEN as the other github_* tools. Only worth using if Gemma 4's " +
        "pool (gemini_generate_content) is saturated; tighter per-request quotas than Gemma 4.",
      inputSchema: {
        model: z.string().describe('Publisher-prefixed model id, e.g. "openai/gpt-4o-mini"'),
        messages: z
          .array(z.object({ role: z.enum(["system", "user", "assistant"]), content: z.string() }))
          .describe('Chat messages, e.g. [{"role":"user","content":"..."}]'),
        temperature: z.number().optional(),
        max_tokens: z.number().int().optional(),
      },
    },
    async ({ model, messages, temperature, max_tokens }) =>
      text(await gh.githubModelsChatCompletion(requireGithubToken(env), model, messages, { temperature, maxTokens: max_tokens })),
  );

  // ---- github_release_* (4 tools) ------------------------------------
  // Section 9 (revised): GitHub Releases half of the object-storage
  // split -- versioned deliverables (a CodeCell's final build once it
  // reaches Completed, a packaged artifact a Reviewer might download),
  // NOT high-frequency or prefix-listed writes (that's b2_* below).
  // All assets accumulate under one rolling release (tag v0-artifacts)
  // on `repo` (defaults to this Worker's own source repo, overridable
  // via GITHUB_RELEASE_ARTIFACTS_REPO). Reuses GITHUB_TOKEN -- see
  // github_release.ts.

  server.registerTool(
    "github_release_put_asset",
    {
      description:
        "Upload (or overwrite, by name) an asset to the rolling 'v0-artifacts' release -- Section 9's " +
        "GitHub-Releases half of object storage, for versioned build deliverables rather than " +
        "high-frequency/prefix-listed blobs (use b2_put_object for those). `content_base64` is the " +
        "asset's bytes, base64-encoded at the tool boundary. Returns the asset id and public download URL.",
      inputSchema: {
        asset_name: z.string().describe("Unique file name for this asset within the release, e.g. 'cell-42-build.zip'"),
        content_base64: z.string().describe("Base64-encoded bytes of the asset."),
        content_type: z.string().default("application/octet-stream").describe('e.g. "application/zip", "text/plain"'),
        repo: z.string().optional().describe("\"owner/name\"; defaults to this Worker's own source repo"),
      },
    },
    async ({ asset_name, content_base64, content_type, repo }) =>
      text(
        await ghr.githubReleasePutAsset(
          requireGithubToken(env),
          repo || defaultReleaseRepo(env),
          asset_name,
          content_base64,
          content_type,
        ),
      ),
  );

  server.registerTool(
    "github_release_get_asset",
    {
      description:
        "Fetch one asset from the rolling 'v0-artifacts' release by id. Returns a JSON string with " +
        "name/size/content_type/download_url plus base64-encoded content_base64 bytes.",
      inputSchema: {
        asset_id: z.number().int(),
        repo: z.string().optional().describe("\"owner/name\"; defaults to this Worker's own source repo"),
      },
    },
    async ({ asset_id, repo }) =>
      text(await ghr.githubReleaseGetAsset(requireGithubToken(env), repo || defaultReleaseRepo(env), asset_id)),
  );

  server.registerTool(
    "github_release_list_assets",
    {
      description: "List all assets currently on the rolling 'v0-artifacts' release.",
      inputSchema: {
        repo: z.string().optional().describe("\"owner/name\"; defaults to this Worker's own source repo"),
      },
    },
    async ({ repo }) =>
      text(await ghr.githubReleaseListAssets(requireGithubToken(env), repo || defaultReleaseRepo(env))),
  );

  server.registerTool(
    "github_release_delete_asset",
    {
      description: "Delete one asset from the rolling 'v0-artifacts' release by id.",
      inputSchema: {
        asset_id: z.number().int(),
        repo: z.string().optional().describe("\"owner/name\"; defaults to this Worker's own source repo"),
      },
    },
    async ({ asset_id, repo }) =>
      text(await ghr.githubReleaseDeleteAsset(requireGithubToken(env), repo || defaultReleaseRepo(env), asset_id)),
  );

  // ---- b2_* (4 tools) --------------------------------------------------
  // Section 9 (revised): Backblaze B2 half of the object-storage split --
  // arbitrary keys, frequent/high-churn writes, prefix-based listing,
  // per-object deletes (the job GitHub Releases above isn't shaped for).
  // Per the cell prompt's "if genuinely unsure, default to B2" rule, this
  // is the general-purpose bucket for intermediate artifacts that aren't
  // yet "the deliverable." S3-compatible API, hand-signed with AWS SigV4
  // -- see b2.ts. Needs B2_KEY_ID / B2_APPLICATION_KEY / B2_BUCKET_NAME /
  // B2_ENDPOINT configured as Worker secrets.

  server.registerTool(
    "b2_put_object",
    {
      description:
        "Write a blob to Backblaze B2 under `key`, overwriting any existing object at that key -- " +
        "Section 9's B2 half of object storage, for arbitrary-key/high-frequency/prefix-listed blobs " +
        "(use github_release_put_asset instead for versioned build deliverables). `content_base64` is " +
        "the blob's bytes, base64-encoded at the tool boundary.",
      inputSchema: {
        key: z.string().describe('Object key, e.g. "screenshots/cell-42-ui.png" or "runs/2026-08-23/log.txt"'),
        content_base64: z.string().describe("Base64-encoded bytes of the blob."),
        content_type: z.string().default("application/octet-stream").describe('e.g. "image/png", "text/plain"'),
      },
    },
    async ({ key, content_base64, content_type }) =>
      text(await b2.b2PutObject(env, key, content_base64, content_type)),
  );

  server.registerTool(
    "b2_get_object",
    {
      description:
        "Read a blob back from Backblaze B2 by key. Returns a JSON string with key/size/content_type " +
        "plus base64-encoded content_base64 bytes.",
      inputSchema: { key: z.string() },
    },
    async ({ key }) => text(await b2.b2GetObject(env, key)),
  );

  server.registerTool(
    "b2_list_objects",
    {
      description: "List objects in the B2 bucket, optionally filtered by key prefix.",
      inputSchema: {
        prefix: z.string().optional().describe('e.g. "screenshots/" to list only objects under that prefix'),
      },
    },
    async ({ prefix }) => text(await b2.b2ListObjects(env, prefix)),
  );

  server.registerTool(
    "b2_delete_object",
    {
      description: "Delete an object from the B2 bucket by key.",
      inputSchema: { key: z.string() },
    },
    async ({ key }) => text(await b2.b2DeleteObject(env, key)),
  );

  // ---- turso_* (7 tools) ------------------------------------------

  server.registerTool(
    "turso_open_database",
    {
      description:
        "Open (and switch to) a Turso/libSQL database for the rest of this session. Leave blank to use the TURSO_DATABASE_URL secret.",
      inputSchema: { database_url: z.string().default(""), auth_token: z.string().default("") },
    },
    async ({ database_url, auth_token }) => text(await turso.tursoOpenDatabase(env, database_url, auth_token)),
  );

  server.registerTool(
    "turso_current_database",
    { description: "Describe the database this server is currently connected to.", inputSchema: {} },
    async () => text(turso.tursoCurrentDatabase(env)),
  );

  server.registerTool(
    "turso_list_tables",
    { description: "List all tables in the current database.", inputSchema: {} },
    async () => text(await turso.tursoListTables(env)),
  );

  server.registerTool(
    "turso_describe_table",
    {
      description: "Get the column structure (name, type, nullability, default, PK) of one table.",
      inputSchema: { table_name: z.string() },
    },
    async ({ table_name }) => text(await turso.tursoDescribeTable(env, table_name)),
  );

  server.registerTool(
    "turso_execute_query",
    {
      description:
        "Execute a read-only SELECT/WITH/PRAGMA/EXPLAIN query and return results as JSON.",
      inputSchema: { sql: z.string() },
    },
    async ({ sql }) => text(await turso.tursoExecuteQuery(env, sql)),
  );

  server.registerTool(
    "turso_insert_data",
    {
      description: "Insert one row into a table.",
      inputSchema: {
        table: z.string(),
        data: z
          .record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()]))
          .describe('column -> value, e.g. {"name": "Alice"}'),
      },
    },
    async ({ table, data }) => text(await turso.tursoInsertData(env, table, data)),
  );

  server.registerTool(
    "turso_update_data",
    {
      description: "Update existing rows in a table.",
      inputSchema: {
        table: z.string(),
        data: z.record(z.string(), z.union([z.string(), z.number(), z.boolean(), z.null()])),
        where: z
          .string()
          .describe('raw SQL WHERE clause without the word WHERE, e.g. "id = 42"; pass "1=1" to update every row'),
      },
    },
    async ({ table, data, where }) => text(await turso.tursoUpdateData(env, table, data, where)),
  );

  server.registerTool(
    "turso_delete_data",
    {
      description: "Delete rows from a table.",
      inputSchema: {
        table: z.string(),
        where: z.string().describe('raw SQL WHERE clause, e.g. "id = 42"; pass "1=1" to delete every row'),
      },
    },
    async ({ table, where }) => text(await turso.tursoDeleteData(env, table, where)),
  );

  server.registerTool(
    "turso_schema_change",
    {
      description: "Execute a CREATE/ALTER/DROP (DDL) statement.",
      inputSchema: { sql: z.string() },
    },
    async ({ sql }) => text(await turso.tursoSchemaChange(env, sql)),
  );

  // ---- neo4j_* (3 tools) --------------------------------------------
  // Talks to Neo4j's HTTP Query API directly (see neo4j.ts) rather than
  // proxying the official neo4j-mcp server -- that server is a separate
  // process you'd have to deploy/run yourself (STDIO or HTTP mode), and
  // the Query API gets you Cypher access over plain fetch() with no
  // extra infra, same tradeoff as turso_* above vs. a full Turso CLI.

  server.registerTool(
    "neo4j_current_database",
    { description: "Describe the Neo4j instance this server is currently configured to use.", inputSchema: {} },
    async () => text(neo4j.neo4jCurrentDatabase(env)),
  );

  server.registerTool(
    "neo4j_get_schema",
    {
      description:
        "Discover the graph schema: node labels, relationship types, and property keys. Uses " +
        "apoc.meta.schema() for a rich shape if APOC is installed, otherwise falls back to db.labels()/" +
        "db.relationshipTypes()/db.propertyKeys().",
      inputSchema: {},
    },
    async () => text(await neo4j.neo4jGetSchema(env)),
  );

  server.registerTool(
    "neo4j_execute_query",
    {
      description:
        "Run a Cypher statement (read or write) against the configured Neo4j database and return the " +
        "resulting rows as JSON. Use parameters for any user-supplied values instead of string-building " +
        'Cypher, e.g. statement "MATCH (p:Person {name: $name}) RETURN p" with parameters {"name": "Alice"}.',
      inputSchema: {
        cypher: z.string().describe("Cypher statement, e.g. \"MATCH (n) RETURN count(n) AS total\""),
        parameters: z
          .record(z.string(), z.any())
          .default({})
          .describe("Named parameters referenced in the statement as $paramName"),
      },
    },
    async ({ cypher, parameters }) => text(await neo4j.neo4jExecuteQuery(env, cypher, parameters)),
  );

  // ---- graph_* (8 tools) ----------------------------------------------
  // Section 7's application layer on top of neo4j_execute_query above
  // (see graph.ts) -- Decision/Error/Task/CodeFile node writes with
  // Gemini Embedding 1<->2 failover, the Orient retrieval step, the
  // embedding-pending backfill, and the AuraDB keepalive heartbeat. A
  // Context Slot loading the Architect/Reviewer role should reach for
  // these instead of hand-writing raw Cypher through neo4j_execute_query.

  server.registerTool(
    "graph_write_decision",
    {
      description:
        "Write a Decision node (Section 7a) -- a captured 'why', embedded via Gemini Embedding 1<->2 " +
        "(Section 7d: embedding failure never blocks the write; embedding_pending=true is set instead). " +
        "Optionally link it to existing nodes in the same call, e.g. BLOCKS a Task or DEPENDS_ON another " +
        "Decision. This is the curation point Section 7g describes -- promoting a Turso checkpoint's " +
        "rationale into the graph is a judgment call made here, never an automatic per-checkpoint write.",
      inputSchema: {
        title: z.string(),
        rationale: z.string().min(10).describe("the captured 'why' -- required, min 10 chars"),
        role: z.string().optional().describe('e.g. "Architect", "Reviewer"'),
        cell_id: z.number().int().optional().describe("Turso code_cells.id this decision relates to, if any (Section 7b reference link)"),
        id: z.string().optional().describe("explicit node id; omit to generate a UUID"),
        relationships: z.array(relationshipInputSchema).default([]),
      },
    },
    async ({ title, rationale, role, cell_id, id, relationships }) => {
      try {
        const result = await graph.writeDecisionNode(env, {
          id,
          title,
          rationale,
          role,
          cellId: cell_id,
          relationships: toRelationshipSpecs(relationships),
        });
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error writing Decision node: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_write_error",
    {
      description:
        "Write an Error node (Section 7a), embedded via Gemini Embedding 1<->2 with the same " +
        "never-block-the-write failure handling as graph_write_decision (Section 7d).",
      inputSchema: {
        message: z.string(),
        context: z.string().optional().describe("additional detail (traceback, surrounding circumstances) to embed alongside the message"),
        cell_id: z.number().int().optional().describe("Turso code_cells.id this error relates to, if any"),
        id: z.string().optional(),
        relationships: z.array(relationshipInputSchema).default([]),
      },
    },
    async ({ message, context, cell_id, id, relationships }) => {
      try {
        const result = await graph.writeErrorNode(env, {
          id,
          message,
          context,
          cellId: cell_id,
          relationships: toRelationshipSpecs(relationships),
        });
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error writing Error node: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_write_task",
    {
      description:
        "Write a Task node (Section 7b) -- structural, not content, so it's never embedded. `status`, if " +
        "set, is denormalized graph-traversal context ONLY; Turso's status column on code_cells remains " +
        "the only thing orchestration logic actually reads. `cell_id` is the reference-only link back to " +
        "the authoritative Turso row.",
      inputSchema: {
        title: z.string(),
        cell_id: z.number().int().optional().describe("Turso code_cells.id this Task corresponds to"),
        status: z.string().optional().describe("DENORMALIZED CONTEXT ONLY -- see description"),
        id: z.string().optional(),
        relationships: z.array(relationshipInputSchema).default([]),
      },
    },
    async ({ title, cell_id, status, id, relationships }) => {
      try {
        const result = await graph.writeTaskNode(env, {
          id,
          title,
          cellId: cell_id,
          status,
          relationships: toRelationshipSpecs(relationships),
        });
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error writing Task node: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_write_code_file",
    {
      description:
        "Write a CodeFile node (Section 7a), embedding `description` via Gemini Embedding 1<->2 if " +
        "provided. `media_ref` is an R2 object key for a related screenshot/video/audio clip (Section " +
        "7a-i) stored as a plain reference property -- it is NOT itself embedded by this tool (multimodal " +
        "embedding would need gemini_embed_content extended for inline media, which is out of scope here).",
      inputSchema: {
        path: z.string(),
        description: z.string().optional(),
        cell_id: z.number().int().optional(),
        media_ref: z.string().optional().describe("R2 object key, e.g. \"screenshots/cell-42-ui.png\""),
        id: z.string().optional(),
        relationships: z.array(relationshipInputSchema).default([]),
      },
    },
    async ({ path, description, cell_id, media_ref, id, relationships }) => {
      try {
        const result = await graph.writeCodeFileNode(env, {
          id,
          path,
          description,
          cellId: cell_id,
          mediaRef: media_ref,
          relationships: toRelationshipSpecs(relationships),
        });
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error writing CodeFile node: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_write_relationship",
    {
      description:
        "Link two already-existing nodes with a DEPENDS_ON/BLOCKS/IMPLEMENTS/AFFECTS relationship " +
        "(Section 7a). Use the `relationships` array on graph_write_* instead when the target already " +
        "exists and you're creating the source node in the same call.",
      inputSchema: {
        from_label: z.enum(["Decision", "Error", "Task", "CodeFile"]),
        from_id: z.string(),
        to_label: z.enum(["Decision", "Error", "Task", "CodeFile"]),
        to_id: z.string(),
        type: z.enum(["DEPENDS_ON", "BLOCKS", "IMPLEMENTS", "AFFECTS"]),
        direction: z.enum(["out", "in"]).default("out"),
      },
    },
    async ({ from_label, from_id, to_label, to_id, type, direction }) => {
      try {
        await graph.writeRelationshipStandalone(env, from_label, from_id, to_label, to_id, type, direction);
        return text(`Created ${type} relationship between ${from_label} '${from_id}' and ${to_label} '${to_id}'.`);
      } catch (e) {
        return text(`Error writing relationship: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_orient",
    {
      description:
        "Section 7c's Orient (retrieval) step: vector search across Decision/Error nodes conceptually " +
        "related to `query`, then graph traversal from those hits to find what Tasks they BLOCK and what " +
        "CodeFiles they touch (AFFECTS/IMPLEMENTS). Run this before an Architect/Reviewer session makes or " +
        "reviews a decision -- fold the returned `narrative` lines into the session's prompt as prior state.",
      inputSchema: {
        query: z.string().describe("natural-language description of the current task, for semantic similarity search"),
        limit: z.number().int().default(5).describe("max Decision/Error hits to return"),
      },
    },
    async ({ query, limit }) => {
      try {
        const result = await graph.graphOrient(env, query, limit);
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error running Orient query: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_backfill_embeddings",
    {
      description:
        "Section 7d's manual trigger for the embedding-pending backfill (the same logic the weekly-ish " +
        "QStash schedule at /webhook/graph-embedding-backfill runs automatically) -- retries Embedding " +
        "1<->2 for any node still missing a vector and clears embedding_pending on success.",
      inputSchema: { limit: z.number().int().default(25) },
    },
    async ({ limit }) => {
      try {
        const result = await graph.backfillPendingEmbeddings(env, limit);
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error backfilling embeddings: ${e}`);
      }
    },
  );

  server.registerTool(
    "graph_heartbeat",
    {
      description:
        "Section 7f's manual trigger for the AuraDB keepalive (the same logic the weekly QStash schedule " +
        "at /webhook/graph-heartbeat runs automatically) -- touches a dedicated _heartbeat node so a quiet " +
        "period doesn't let the free instance cross its 30-day inactivity window and get deleted.",
      inputSchema: {},
    },
    async () => {
      try {
        const result = await graph.touchHeartbeat(env);
        return text(JSON.stringify(result, null, 2));
      } catch (e) {
        return text(`Error touching heartbeat: ${e}`);
      }
    },
  );

  // ---- cloudflare_* (2 tools) --------------------------------------
  // Proxied through to Cloudflare's own remote MCP server. Between the
  // two of these, this connector gets read/write access to this Worker's
  // own deployed script plus every other Cloudflare product (Workflows,
  // Durable Objects, DNS, KV, R2, everything) -- see cloudflare.ts.

  server.registerTool(
    "cloudflare_search",
    {
      description:
        "Search the Cloudflare API's OpenAPI spec (proxied from Cloudflare's own mcp.cloudflare.com Code " +
        "Mode server). `code` MUST be a single bare, UNINVOKED async function literal -- the remote server " +
        "calls it itself, so do not self-invoke it (no trailing `()`) and do not wrap it in statements " +
        "outside a function. `spec` is available as an ambient global inside the function body -- there is " +
        "no `codemode.spec()` call. Example code: \"async () => { return Object.entries(spec.paths)" +
        ".filter(([p]) => p.includes('workers/scripts')).map(([p, o]) => ({path: p, methods: Object.keys(o)})); }\"",
      inputSchema: { code: z.string().describe("JavaScript to execute against the OpenAPI spec.") },
    },
    async ({ code }) => text(await cf.cloudflareSearch(env, code)),
  );

  server.registerTool(
    "cloudflare_execute",
    {
      description:
        "Run JavaScript against an authenticated `cloudflare.request()` client (proxied from Cloudflare's " +
        "own mcp.cloudflare.com Code Mode server) to actually call the Cloudflare API -- deploy/edit this " +
        "very Worker's script, manage Workflows and Durable Object namespaces, DNS, KV, R2, anything " +
        "covered by your CLOUDFLARE_API_TOKEN's scopes. Find the endpoint with cloudflare_search first. " +
        "`code` MUST be a single bare, UNINVOKED async function literal -- the remote server calls it " +
        "itself. `cloudflare` is an ambient global inside the function body (NOT a function parameter -- " +
        "an `async (cloudflare) => ...` signature receives undefined). Do not self-invoke the function " +
        "(no trailing `()`) and do not write top-level statements outside a function, both fail to parse. " +
        "`cloudflare.request()` takes a SINGLE options object -- { method, path, query?, body?, " +
        "contentType?, rawBody? } -- not (path, options). Example code: \"async () => { return await " +
        "cloudflare.request({ method: 'GET', path: '/accounts' }); }\"",
      inputSchema: { code: z.string().describe("JavaScript calling cloudflare.request(...) to execute.") },
    },
    async ({ code }) => text(await cf.cloudflareExecute(env, code)),
  );

  // ---- upstash_* (7 tools) ------------------------------------------
  // Direct REST calls to Upstash's Developer API (api.upstash.com), not a
  // proxy -- Upstash has no hosted remote MCP server to proxy to. Auth is
  // account-wide by design (Upstash's Developer API keys aren't scoped
  // yet) -- see upstash.ts for details and for why backup-management
  // endpoints aren't included here.

  server.registerTool(
    "upstash_list_databases",
    { description: "List all Upstash Redis databases on this account.", inputSchema: {} },
    async () => text(await us.upstashListDatabases(env)),
  );

  server.registerTool(
    "upstash_get_database",
    {
      description: "Get full details of one Upstash Redis database by id.",
      inputSchema: { database_id: z.string() },
    },
    async ({ database_id }) => text(await us.upstashGetDatabase(env, database_id)),
  );

  server.registerTool(
    "upstash_database_stats",
    {
      description:
        "Get usage statistics for one Upstash Redis database (command count, bandwidth, latency, keyspace, throughput, disk usage).",
      inputSchema: { database_id: z.string() },
    },
    async ({ database_id }) => text(await us.upstashDatabaseStats(env, database_id)),
  );

  server.registerTool(
    "upstash_create_database",
    {
      description: "Create a new Upstash Redis database.",
      inputSchema: {
        name: z.string().describe("Name of the database."),
        region: z.string().describe('Region, e.g. "us-east-1", or "global" for a Global Database.'),
        primary_region: z.string().optional().describe("Required if region is 'global': the primary write region."),
        read_regions: z.array(z.string()).optional().describe("Additional read regions for a Global Database."),
        tls: z.boolean().default(true).describe("Enable TLS (recommended, default true)."),
      },
    },
    async ({ name, region, primary_region, read_regions, tls }) =>
      text(await us.upstashCreateDatabase(env, name, region, { primaryRegion: primary_region, readRegions: read_regions, tls })),
  );

  server.registerTool(
    "upstash_delete_database",
    {
      description: "Permanently delete an Upstash Redis database. This cannot be undone.",
      inputSchema: { database_id: z.string() },
    },
    async ({ database_id }) => text(await us.upstashDeleteDatabase(env, database_id)),
  );

  server.registerTool(
    "upstash_rename_database",
    {
      description: "Rename an existing Upstash Redis database.",
      inputSchema: { database_id: z.string(), new_name: z.string() },
    },
    async ({ database_id, new_name }) => text(await us.upstashRenameDatabase(env, database_id, new_name)),
  );

  server.registerTool(
    "upstash_reset_password",
    {
      description: "Reset the REST/TCP password/token of an Upstash Redis database, invalidating the old one.",
      inputSchema: { database_id: z.string() },
    },
    async ({ database_id }) => text(await us.upstashResetPassword(env, database_id)),
  );

  // ---- groq_* (2 tools) ----------------------------------------------
  // Thin wrapper over Groq's OpenAI-compatible inference API
  // (api.groq.com/openai/v1) -- no hosted MCP server or account-mgmt
  // surface to proxy, just chat completions + model listing. See
  // groq.ts for why audio transcription isn't included here.

  server.registerTool(
    "groq_chat_completion",
    {
      description:
        "Run a chat completion on Groq's fast inference API. Returns the model's text response.",
      inputSchema: {
        model: z.string().describe('e.g. "openai/gpt-oss-20b", "groq/compound", "qwen/qwen3.6-27b". Use groq_list_models to see current options -- Groq\'s lineup changes over time.'),
        messages: z
          .array(z.object({ role: z.enum(["system", "user", "assistant"]), content: z.string() }))
          .describe('Chat messages, e.g. [{"role":"user","content":"..."}]'),
        temperature: z.number().optional(),
        max_tokens: z.number().int().optional(),
      },
    },
    async ({ model, messages, temperature, max_tokens }) =>
      text(await groq.groqChatCompletion(env, model, messages, { temperature, maxTokens: max_tokens })),
  );

  server.registerTool(
    "groq_list_models",
    { description: "List models currently available on Groq's inference API.", inputSchema: {} },
    async () => text(await groq.groqListModels(env)),
  );

  // ---- gemini_* (2 tools) ---------------------------------------------
  // Thin wrapper over the Gemini API's generateContent/embedContent REST
  // endpoints (generativelanguage.googleapis.com/v1beta). Covers several
  // stack-doc rows at once via the `model` param -- Gemini Flash/Flash-
  // Lite tiers AND Gemma 4 31B/26B are all the same endpoint, just a
  // different model string -- see gemini.ts.

  server.registerTool(
    "gemini_generate_content",
    {
      description:
        "Run a generateContent call against a Gemini or Gemma model. Covers the Fast Worker fallback " +
        "cascade (gemini-*-flash tiers), the Gemma 4 tagging/fallback tiers (gemma-4-*), the " +
        "response_schema discipline (set response_mime_type + response_schema for structured JSON out), " +
        "and google_search grounding (set grounding_with_google_search=true).",
      inputSchema: {
        model: z.string().describe('e.g. "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemma-4-31b", "gemma-4-26b"'),
        messages: z
          .array(z.object({ role: z.enum(["user", "model"]), content: z.string() }))
          .describe('Turn history, e.g. [{"role":"user","content":"..."}]'),
        system_instruction: z.string().optional(),
        temperature: z.number().optional(),
        max_output_tokens: z.number().int().optional(),
        response_mime_type: z.string().optional().describe('e.g. "application/json" for structured output'),
        response_schema: z.record(z.string(), z.any()).optional().describe("JSON Schema, paired with response_mime_type"),
        grounding_with_google_search: z.boolean().default(false).describe("Enable the google_search grounding tool (Section 11)"),
      },
    },
    async ({ model, messages, system_instruction, temperature, max_output_tokens, response_mime_type, response_schema, grounding_with_google_search }) =>
      text(
        await gemini.geminiGenerateContent(env, model, messages, {
          systemInstruction: system_instruction,
          temperature,
          maxOutputTokens: max_output_tokens,
          responseMimeType: response_mime_type,
          responseSchema: response_schema,
          groundingWithGoogleSearch: grounding_with_google_search,
        }),
      ),
  );

  server.registerTool(
    "gemini_embed_content",
    {
      description:
        "Generate an embedding vector for text using a Gemini Embedding model (Section 7a's Neo4j Graph RAG source).",
      inputSchema: {
        model: z.string().default("gemini-embedding-001").describe('e.g. "gemini-embedding-001"'),
        text: z.string(),
        task_type: z.string().optional().describe('e.g. "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "SEMANTIC_SIMILARITY"'),
      },
    },
    async ({ model, text: inputText, task_type }) =>
      text(await gemini.geminiEmbedContent(env, model, inputText, task_type)),
  );

  // ---- antigravity_* (2 tools) -----------------------------------------
  // The managed Antigravity agent, via the Gemini Interactions API --
  // PREVIEW as of writing (see antigravity.ts). This is Section 4a's
  // "autonomous first-pass fix" / Section 4c's triage controller: one
  // call provisions a sandbox, reads the traceback + code, and attempts
  // a single bounded fix or produces a triage tag.

  server.registerTool(
    "antigravity_run",
    {
      description:
        "Run a task through the managed Antigravity agent in a fresh or reused Linux sandbox (PREVIEW API). " +
        "Use for a bounded autonomous fix-attempt on a Failed cell's traceback, or as a pre-Heavy-Worker " +
        "triage pass -- scope the prompt tightly (Section 4a/4c: 'drafts a patch or a triage tag, never " +
        "marks a cell Completed'). Set background=true for tasks that may take minutes; poll with " +
        "antigravity_get_interaction using the returned id.",
      inputSchema: {
        input: z.string().describe("The task/prompt for the agent, e.g. traceback + current code + 'attempt a single bounded fix'"),
        environment: z.string().default("remote").describe('"remote" for a fresh sandbox, or an existing environment_id to resume one'),
        previous_interaction_id: z.string().optional().describe("Continue a prior interaction (multi-turn / after requires_action)"),
        background: z.boolean().default(false).describe("Run asynchronously; poll via antigravity_get_interaction"),
        max_total_tokens: z.number().int().optional().describe("Budget cap for this interaction (input+output+thinking)"),
        model: z.string().optional().describe('e.g. "gemini-3.5-flash-lite" for a cheaper/faster fix-attempt pass'),
      },
    },
    async ({ input, environment, previous_interaction_id, background, max_total_tokens, model }) =>
      text(
        await ag.antigravityRunInteraction(env, input, {
          environment,
          previousInteractionId: previous_interaction_id,
          background,
          maxTotalTokens: max_total_tokens,
          model,
        }),
      ),
  );

  server.registerTool(
    "antigravity_get_interaction",
    {
      description: "Poll or re-fetch a (possibly backgrounded) Antigravity interaction by id.",
      inputSchema: { interaction_id: z.string() },
    },
    async ({ interaction_id }) => text(await ag.antigravityGetInteraction(env, interaction_id)),
  );

  // ---- qstash_* (4 tools) -----------------------------------------------
  // Direct REST calls to QStash's own API (qstash.upstash.io), a
  // SEPARATE product and credential from upstash_* above (which manages
  // Redis databases via api.upstash.com) -- see qstash.ts. This is
  // Section 4 step 3's "Traffic Control" pacing layer and Section 4a's
  // dead-letter backstop cron, and (as of Section 4f) the cross-instance
  // pacing layer in front of CodeCellWorkflow's fast-worker-dispatch
  // step -- flow_control_key/rate/parallelism/period below let you drive
  // the same header-based mechanism manually, e.g. to test a key before
  // wiring it into code.

  server.registerTool(
    "qstash_publish",
    {
      description:
        "Publish a one-off message to a destination URL via QStash, with optional delay/retries/callback/" +
        "flow-control -- the pacing layer for bursts of outbound calls (Section 4 step 3 / 4f).",
      inputSchema: {
        destination_url: z.string().describe("Full https URL QStash will POST to"),
        body: z.any().describe("JSON body delivered to the destination"),
        delay_seconds: z.number().int().optional(),
        retries: z.number().int().optional(),
        callback_url: z.string().optional().describe("URL QStash calls back with the destination's response"),
        extra_headers: z
          .record(z.string(), z.string())
          .optional()
          .describe(
            'Extra headers sent with this publish, e.g. {"Upstash-Forward-Authorization": "Bearer <token>"} so QStash ' +
              "forwards a bearer token (stripped of the Upstash-Forward- prefix) to the destination route.",
          ),
        flow_control_key: z
          .string()
          .optional()
          .describe("Shared pacing key -- every publish using the same key competes for the same rate/parallelism budget, across callers and instances."),
        flow_control_rate: z.number().int().optional().describe("Max calls per flow_control_period for this key (default period: 1s)."),
        flow_control_parallelism: z.number().int().optional().describe("Max calls in flight at once for this key."),
        flow_control_period: z.string().optional().describe('e.g. "1m", "30s" -- paired with flow_control_rate.'),
      },
    },
    async ({ destination_url, body, delay_seconds, retries, callback_url, extra_headers, flow_control_key, flow_control_rate, flow_control_parallelism, flow_control_period }) =>
      text(
        await qstash.qstashPublish(env, destination_url, body, {
          delaySeconds: delay_seconds,
          retries,
          callbackUrl: callback_url,
          extraHeaders: extra_headers,
          flowControl: flow_control_key
            ? { key: flow_control_key, rate: flow_control_rate, parallelism: flow_control_parallelism, period: flow_control_period }
            : undefined,
        }),
      ),
  );

  server.registerTool(
    "qstash_schedule_create",
    {
      description:
        "Create a recurring cron schedule via QStash -- e.g. Section 4a's backstop that sweeps stuck " +
        "`Pending` cells every 10 minutes, or Section 7d/7f's Neo4j embedding-backfill/keepalive schedules.",
      inputSchema: {
        destination_url: z.string().describe("Full https URL QStash will POST to on each firing"),
        cron: z.string().describe('Cron expression, e.g. "*/10 * * * *" for every 10 minutes'),
        body: z.any().optional().describe("JSON body delivered on each firing"),
        retries: z.number().int().optional(),
        extra_headers: z
          .record(z.string(), z.string())
          .optional()
          .describe(
            'Extra headers sent on each firing, e.g. {"Upstash-Forward-Authorization": "Bearer <token>"} so QStash ' +
              "forwards a bearer token (stripped of the Upstash-Forward- prefix) to the destination route.",
          ),
      },
    },
    async ({ destination_url, cron, body, retries, extra_headers }) =>
      text(await qstash.qstashCreateSchedule(env, destination_url, cron, body, { retries, extraHeaders: extra_headers })),
  );

  server.registerTool(
    "qstash_schedule_list",
    { description: "List all QStash schedules on this account.", inputSchema: {} },
    async () => text(await qstash.qstashListSchedules(env)),
  );

  server.registerTool(
    "qstash_schedule_delete",
    {
      description: "Delete a QStash schedule by id.",
      inputSchema: { schedule_id: z.string() },
    },
    async ({ schedule_id }) => text(await qstash.qstashDeleteSchedule(env, schedule_id)),
  );

  // ---- posthog_* (2 tools) --------------------------------------------
  // Proxied through to PostHog's own hosted remote MCP server
  // (mcp.posthog.com/mcp). PostHog exposes a real, evolving catalog of
  // many named tools (projects, insights, feature flags, experiments,
  // HogQL queries, error tracking, CDP destinations, and more) rather
  // than Cloudflare's fixed 2-tool code-mode interface, so this proxies
  // generically -- see posthog.ts.

  server.registerTool(
    "posthog_list_tools",
    {
      description:
        "List every tool currently exposed by PostHog's remote MCP server, with descriptions. Call this " +
        "first to see what's available (analytics queries, feature flags, experiments, error tracking, " +
        "annotations, HogQL, CDP destinations, etc.) before calling posthog_call_tool.",
      inputSchema: {},
    },
    async () => text(await ph.posthogListTools(env)),
  );

  server.registerTool(
    "posthog_call_tool",
    {
      description:
        "Call any tool exposed by PostHog's remote MCP server by name. Use posthog_list_tools first to see " +
        "available tool names and what arguments each expects.",
      inputSchema: {
        tool_name: z.string().describe('e.g. "list-projects", "create-annotation", "query-insight"'),
        arguments: z.record(z.string(), z.any()).default({}).describe("Arguments object for the tool, matching its schema."),
      },
    },
    async ({ tool_name, arguments: toolArguments }) => text(await ph.posthogCallTool(env, tool_name, toolArguments)),
  );

  // ---- discord_* (6 tools) --------------------------------------------
  // Direct wrapper over Discord's Bot REST API (discord.com/api/v10).
  // Requires a Discord bot application, invited to the target server(s)
  // with the relevant permissions, and its token in DISCORD_BOT_TOKEN.
  // General-purpose Discord surface -- kept as-is by the Web Push
  // migration (web-push-migration-instructions.md), which only replaces
  // the out-of-band ALERT job (see notify() in code_cell_workflow.ts,
  // now backed by push.ts's sendWebPushToAll). Nothing here reads
  // DISCORD_ALERT_CHANNEL_ID anymore -- that var existed solely for the
  // alert path and has been removed.

  server.registerTool(
    "discord_send_message",
    {
      description: "Send a text message to a Discord channel.",
      inputSchema: {
        channel_id: z.string().describe("Discord channel ID (snowflake)"),
        content: z.string().describe("Message text to send"),
      },
    },
    async ({ channel_id, content }) =>
      text(await dc.discordSendMessage(requireDiscordToken(env), channel_id, content)),
  );

  server.registerTool(
    "discord_get_channel_messages",
    {
      description: "Fetch the most recent messages from a Discord channel.",
      inputSchema: {
        channel_id: z.string(),
        limit: z.number().int().default(20).describe("1-100, most recent first"),
      },
    },
    async ({ channel_id, limit }) =>
      text(await dc.discordGetChannelMessages(requireDiscordToken(env), channel_id, limit)),
  );

  server.registerTool(
    "discord_list_channels",
    {
      description: "List all channels in a Discord server (guild).",
      inputSchema: { guild_id: z.string().describe("Discord server/guild ID (snowflake)") },
    },
    async ({ guild_id }) => text(await dc.discordListChannels(requireDiscordToken(env), guild_id)),
  );

  server.registerTool(
    "discord_create_channel",
    {
      description: "Create a new channel in a Discord server (guild).",
      inputSchema: {
        guild_id: z.string(),
        name: z.string(),
        type: z
          .number()
          .int()
          .default(0)
          .describe("Discord channel type: 0=text, 2=voice, 4=category, 5=announcement, 13=stage"),
        parent_id: z.string().optional().describe("Category channel ID to nest this channel under"),
      },
    },
    async ({ guild_id, name, type, parent_id }) =>
      text(await dc.discordCreateChannel(requireDiscordToken(env), guild_id, name, type, parent_id)),
  );

  server.registerTool(
    "discord_add_reaction",
    {
      description: "Add an emoji reaction to a message.",
      inputSchema: {
        channel_id: z.string(),
        message_id: z.string(),
        emoji: z
          .string()
          .describe('Unicode emoji (e.g. "👍") or custom emoji as "name:id"'),
      },
    },
    async ({ channel_id, message_id, emoji }) =>
      text(await dc.discordAddReaction(requireDiscordToken(env), channel_id, message_id, emoji)),
  );

  server.registerTool(
    "discord_get_guild_info",
    {
      description: "Get basic info (name, owner, member count) about a Discord server (guild).",
      inputSchema: { guild_id: z.string() },
    },
    async ({ guild_id }) => text(await dc.discordGetGuildInfo(requireDiscordToken(env), guild_id)),
  );

  // ---- workflow_* (2 tools) -----------------------------------------
  // Drive the JOB_WORKFLOW binding (see workflows.ts) directly, without
  // going through the Cloudflare API proxy above.

  server.registerTool(
    "workflow_trigger",
    {
      description:
        "Start a new JobWorkflow instance: an ordered list of steps, each either an HTTP GET (set `url`) " +
        "or a delay (set `sleepSeconds`). Returns the instance id to check with workflow_status.",
      inputSchema: {
        label: z.string().default("job"),
        steps: z
          .array(
            z.object({
              name: z.string(),
              url: z.string().optional(),
              sleepSeconds: z.number().optional(),
            }),
          )
          .default([]),
      },
    },
    async ({ label, steps }) => {
      try {
        const instance = await env.JOB_WORKFLOW.create({ params: { label, steps } });
        return text(`Started workflow instance ${instance.id} (label='${label}', ${steps.length} step(s)).`);
      } catch (e) {
        return text(`Error starting workflow: ${e}`);
      }
    },
  );

  server.registerTool(
    "workflow_status",
    {
      description: "Get the status (and output, once complete) of a JobWorkflow instance by id.",
      inputSchema: { instance_id: z.string() },
    },
    async ({ instance_id }) => {
      try {
        const instance = await env.JOB_WORKFLOW.get(instance_id);
        const status = await instance.status();
        return text(JSON.stringify(status, null, 2));
      } catch (e) {
        return text(`Error getting workflow status: ${e}`);
      }
    },
  );

  // ---- cell_* (3 tools) -------------------------------------------------
  // Section 4a/4f/4g/5/5b/5c: the CodeCell pipeline. cell_create starts a
  // durable CodeCellWorkflow instance (fast-worker-generate ->
  // heavy-worker-dispatch -> wait for test.yml's callback -> tag -> notify,
  // or, per Section 4g, an OpenHands generation lane feeding the same
  // heavy-worker-dispatch/tag/notify tail -- see code_cell_workflow.ts).
  // cell_resume/checkpoint_write implement the generic cross-session
  // resumability pattern from Section 5b/5c.

  server.registerTool(
    "cell_create",
    {
      description:
        "Insert a new Pending CodeCell (Section 4a) and start its CodeCellWorkflow instance. " +
        "Requires HEAVY_WORKER_REPO to be set (the repo containing .github/workflows/test.yml). " +
        "execution_mode='openhands' (Section 4g) delegates initial code generation to an OpenHands " +
        "iterate-until-it-works loop instead of the Fast Worker cascade -- use for tasks where a full " +
        "autonomous agent loop is a better fit than this pipeline's own wiring; Heavy Worker's " +
        "deterministic test still has final say either way. Leave as 'pipeline' (default) for everything " +
        "else -- a pipeline-lane cell whose test fails still gets one automatic OpenHands escalation " +
        "attempt before falling to Failed/Debugger, so this is rarely worth setting explicitly at creation.",
      inputSchema: {
        spec: z.string().describe("The task/spec text the Fast Worker will generate code from."),
        role: z.string().default("Architect").describe('e.g. "Architect", "Coder", "Reviewer", "Debugger"'),
        execution_mode: z.enum(["pipeline", "openhands"]).default("pipeline"),
      },
    },
    async ({ spec, role, execution_mode }) => {
      try {
        await ensureSchema(env);
        const cellId = await createCell(env, spec, role, execution_mode);
        const instance = await env.CODE_CELL_WORKFLOW.create({ params: { cell_id: cellId, spec, execution_mode } });
        return text(`Created CodeCell #${cellId} (execution_mode=${execution_mode}), started workflow instance ${instance.id}.`);
      } catch (e) {
        return text(`Error creating CodeCell: ${e}`);
      }
    },
  );

  server.registerTool(
    "cell_resume",
    {
      description:
        "Section 5b/5c generic resume query: the highest-priority non-terminal CodeCell to pick up " +
        "next (stale locks -- untouched >10 min -- prioritized over fresh ones). Pass cell_id to look " +
        "up one specific cell instead.",
      inputSchema: { cell_id: z.number().int().optional() },
    },
    async ({ cell_id }) => {
      try {
        const row = cell_id !== undefined ? await getCell(env, cell_id) : await resumeCandidate(env);
        return text(row ? JSON.stringify(row, null, 2) : "No non-terminal CodeCells found.");
      } catch (e) {
        return text(`Error resuming: ${e}`);
      }
    },
  );

  server.registerTool(
    "checkpoint_write",
    {
      description:
        "Write a checkpoint row for a CodeCell (Section 5c). `rationale` is required (min 10 chars) -- " +
        "a checkpoint without a real 'why' looks resumable but tells the next session nothing. If " +
        "`artifact` is large (>4KB), it's routed to Backblaze B2 by default (Section 9's 'when unsure, " +
        "default to B2' rule) and the checkpoint row stores the provider + key instead of the raw text -- " +
        "falls back to inline storage if B2 isn't configured or the upload fails, so this never blocks " +
        "the checkpoint write.",
      inputSchema: {
        cell_id: z.number().int(),
        phase: z.string().describe("mirrors the CodeCell's status at write time"),
        session_id: z.string(),
        artifact: z.string().optional().describe("partial code/notes as they currently stand"),
        next_action: z.string().optional().describe("the exact next concrete step, not a vague summary"),
        rationale: z.string().min(10).describe("the 'why' that isn't recoverable from the artifact alone"),
      },
    },
    async ({ cell_id, phase, session_id, artifact, next_action, rationale }) => {
      try {
        await writeCheckpoint(env, {
          cellId: cell_id,
          phase,
          sessionId: session_id,
          artifact,
          nextAction: next_action,
          rationale,
        });
        return text(`Checkpoint written for CodeCell #${cell_id}.`);
      } catch (e) {
        return text(`Error writing checkpoint: ${e}`);
      }
    },
  );

  // ---- runner_* (3 tools) ---------------------------------------------
  // Drive the RUNNER Durable Object binding (see runner.ts): a persistent
  // per-name actor with its own task queue. Same runner_id across calls
  // == same actor/storage.

  server.registerTool(
    "runner_enqueue",
    {
      description:
        "Queue a task on a named runner (a persistent Durable Object actor -- reuse the same runner_id to " +
        "share one queue/state across calls). Built-in commands: 'ping' (echoes payload) and 'fetch' " +
        "(payload = url string or {url}). Add real commands in runner.ts's runTask(). Returns a task id.",
      inputSchema: {
        runner_id: z.string().default("default"),
        command: z.string(),
        payload: z.any().optional(),
      },
    },
    async ({ runner_id, command, payload }) => {
      try {
        const stub = env.RUNNER.get(env.RUNNER.idFromName(runner_id));
        const taskId = await stub.enqueue(command, payload);
        return text(`Queued task ${taskId} on runner '${runner_id}'.`);
      } catch (e) {
        return text(`Error enqueueing task: ${e}`);
      }
    },
  );

  server.registerTool(
    "runner_status",
    {
      description: "Get one task's status/result from a named runner.",
      inputSchema: { runner_id: z.string().default("default"), task_id: z.string() },
    },
    async ({ runner_id, task_id }) => {
      try {
        const stub = env.RUNNER.get(env.RUNNER.idFromName(runner_id));
        const task = await stub.status(task_id);
        return text(task ? JSON.stringify(task, null, 2) : `No task '${task_id}' on runner '${runner_id}'.`);
      } catch (e) {
        return text(`Error getting task status: ${e}`);
      }
    },
  );

  server.registerTool(
    "runner_list",
    {
      description: "List recent tasks (any status) on a named runner, most recent first.",
      inputSchema: { runner_id: z.string().default("default"), limit: z.number().int().default(20) },
    },
    async ({ runner_id, limit }) => {
      try {
        const stub = env.RUNNER.get(env.RUNNER.idFromName(runner_id));
        const tasks = (await stub.list(limit)) as RunnerTask[];
        return text(tasks.length ? JSON.stringify(tasks, null, 2) : `No tasks on runner '${runner_id}' yet.`);
      } catch (e) {
        return text(`Error listing tasks: ${e}`);
      }
    },
  );

  return server;
}

// The MCP endpoint itself. OAuthProvider only forwards requests here
// once it has already verified a valid access token, so no auth check
// is needed in this function -- that's the whole point of the library.
const mcpApiHandler = {
  fetch(request: Request, env: Env, ctx: ExecutionContext) {
    return createMcpHandler(() => buildServer(env))(request, env, ctx);
  },
};

export default new OAuthProvider({
  apiRoute: "/mcp",
  apiHandler: mcpApiHandler,
  defaultHandler: AuthHandler,
  authorizeEndpoint: "/authorize",
  tokenEndpoint: "/token",
  clientRegistrationEndpoint: "/register",
  scopesSupported: ["mcp"],
});
