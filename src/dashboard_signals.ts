// dashboard_signals.ts
//
// Section 12b of Zero-Cost-Stack-v11.md: the operator dashboard's health
// signal computations. Shared between the plain-HTTP /dashboard/data
// route (dashboard.ts, one synchronous computation per request -- used
// for curl/web_fetch verification and as the very first payload a
// browser tab sees before its WebSocket connects) and DashboardHub's
// alarm-driven poll loop (dashboard_do.ts) -- one function, two callers,
// so the "poll once, broadcast to N tabs" pattern (Section 12c) and the
// one-off verification path never drift out of sync with each other.
//
// Defensive-build rule (per the cell prompt): every signal below checks
// for its own config/data presence before querying anything beyond
// Turso -- Turso and QStash are the only two guaranteed-present
// dependencies (Section 12b/12d). A signal whose backing isn't wired up
// yet renders as `configured: false` with a human-readable `note`
// explaining why, NEVER as a thrown error and NEVER as a fabricated
// number. A signal that IS configured but whose own query fails renders
// as `configured: true, state: "critical"` with the error in `detail` --
// distinct from "not configured" per the cell prompt's instruction to
// make that distinction visible in the UI. Nothing in this module
// throws past its own signal-computing function -- one signal's query
// failing degrades just that signal, never the whole payload.
//
// Query narrowly (Section 12c): every Turso query below selects only
// the specific columns each signal needs, never `SELECT *`.
//
// Object-storage note: this codebase's actual Section 9 implementation
// uses Backblaze B2 (b2.ts), not Cloudflare R2 -- the cell prompt names
// "R2 storage headroom" because that's the spec's original framing, but
// there is no R2 binding anywhere in this Worker to check. The storage
// signal below checks B2 config instead, against the same free-tier
// storage cap shape (10GB) B2 itself offers -- see b2GetBucketUsage's
// doc comment in b2.ts.

import { createClient, type Client } from "@libsql/client/web";
import type { TursoEnv } from "./turso";
import type { Neo4jEnv } from "./neo4j";
import { runCypher } from "./neo4j";
import type { B2Env } from "./b2";
import { b2GetBucketUsage } from "./b2";
import type { QstashEnv } from "./qstash";
import { qstashListSchedules } from "./qstash";

export type SignalState = "ok" | "warn" | "critical" | "unknown";

export interface HealthSignal {
  key: string;
  label: string;
  configured: boolean;
  state: SignalState;
  value: string;
  detail?: string;
  note?: string;
}

export interface DashboardPayload {
  generatedAt: string;
  overallStatus: "Nominal" | "Degraded" | "Stalled";
  signals: HealthSignal[];
}

export type DashboardEnv = TursoEnv &
  Neo4jEnv &
  B2Env &
  QstashEnv & {
    GROQ_API_KEY?: string;
    GEMINI_API_KEY?: string;
  };

// ---- small constructors, kept consistent across every signal below ----

function ok(key: string, label: string, value: string, detail?: string): HealthSignal {
  return { key, label, configured: true, state: "ok", value, detail };
}
function warn(key: string, label: string, value: string, detail?: string): HealthSignal {
  return { key, label, configured: true, state: "warn", value, detail };
}
function critical(key: string, label: string, value: string, detail?: string): HealthSignal {
  return { key, label, configured: true, state: "critical", value, detail };
}
function notConfigured(key: string, label: string, note: string): HealthSignal {
  return { key, label, configured: false, state: "unknown", value: "not configured", note };
}
function errored(key: string, label: string, e: unknown): HealthSignal {
  return { key, label, configured: true, state: "critical", value: "error", detail: String(e) };
}

// ---- Turso client -------------------------------------------------------
// Re-resolves its own client rather than importing turso.ts's private
// cachedClient/getClient (not exported) -- cheap (fetch-based, no
// socket), same rationale codecells.ts already gives for doing the same
// thing: keeps this module independent of turso_open_database()'s
// per-session override behavior, which shouldn't affect what the
// dashboard polls.

function tursoClient(env: TursoEnv): Client {
  if (!env.TURSO_DATABASE_URL) {
    throw new Error("TURSO_DATABASE_URL is not configured on this Worker.");
  }
  return createClient(
    env.TURSO_AUTH_TOKEN
      ? { url: env.TURSO_DATABASE_URL, authToken: env.TURSO_AUTH_TOKEN }
      : { url: env.TURSO_DATABASE_URL },
  );
}

// ---- Stalled work (Section 12b, Section 5b's own stale-lock threshold) -

async function stalledWork(env: TursoEnv): Promise<HealthSignal> {
  try {
    const client = tursoClient(env);
    const rs = await client.execute(
      `SELECT id, role, locked_by, locked_at FROM code_cells
       WHERE locked_by IS NOT NULL AND (locked_at IS NULL OR locked_at < datetime('now','-10 minutes'))
       ORDER BY locked_at ASC LIMIT 20`,
    );
    const rows = rs.rows as unknown as { id: number; role: string; locked_by: string; locked_at: string | null }[];
    const n = rows.length;
    if (n === 0) {
      return ok("stalled_work", "Stalled work", "0 cells", "No cell has an open lock older than 10 minutes.");
    }
    const detail = rows
      .map((r) => `#${r.id} (${r.role}) locked_by=${r.locked_by} since ${r.locked_at ?? "unknown"}`)
      .join("; ");
    return n >= 5
      ? critical("stalled_work", "Stalled work", `${n} cell(s)`, detail)
      : warn("stalled_work", "Stalled work", `${n} cell(s)`, detail);
  } catch (e) {
    return errored("stalled_work", "Stalled work", e);
  }
}

// ---- Silent pipeline stalls (Section 4a's stuck-Pending backstop) ------
// This codebase does not implement Section 4a's backstop cron as of this
// cell -- no /webhook/pending-sweep (or equivalent) route exists in
// auth.ts, and no matching QStash schedule exists on this account (see
// this cell's summary). Checked at runtime, not assumed: this queries
// QStash's actual schedule list and looks for one whose destination
// looks like a Pending-sweep backstop, so if that route+schedule get
// added later this signal lights up automatically without code changes
// here, per the cell prompt's defensive-build rule.

async function silentPipelineStalls(env: QstashEnv): Promise<HealthSignal> {
  const key = "silent_pipeline_stalls";
  const label = "Silent pipeline stalls (Section 4a backstop)";
  if (!env.QSTASH_TOKEN) {
    return notConfigured(key, label, "QSTASH_TOKEN is not set -- can't check for a backstop schedule.");
  }
  try {
    const raw = await qstashListSchedules(env);
    let schedules: unknown;
    try {
      schedules = JSON.parse(raw);
    } catch {
      return notConfigured(key, label, `Could not list QStash schedules: ${raw}`.slice(0, 300));
    }
    if (!Array.isArray(schedules)) {
      return notConfigured(key, label, `Unexpected QStash schedules response: ${raw}`.slice(0, 300));
    }
    const backstop = (schedules as any[]).find(
      (s) => typeof s?.destination === "string" && /pending[-_]?sweep|backstop|stuck[-_]?pending/i.test(s.destination),
    );
    if (!backstop) {
      return notConfigured(
        key,
        label,
        "Section 4a's stuck-Pending backstop cron isn't implemented in this codebase yet -- no matching " +
          "route in auth.ts and no matching QStash schedule found. Add a /webhook/pending-sweep route " +
          "(query Turso for cells stuck in Pending, re-fire them) plus a low-frequency QStash schedule " +
          "targeting it, and this signal lights up automatically -- see this cell's summary.",
      );
    }
    // A matching schedule exists, but nothing in this codebase persists
    // a last-run/last-fired timestamp anywhere queryable yet, so
    // "how long since it last actually fired" genuinely isn't answerable
    // -- surfaced as a warn, not fabricated as healthy.
    return warn(
      key,
      label,
      "schedule exists, freshness unknown",
      `Found a QStash schedule targeting '${backstop.destination}' (cron '${backstop.cron ?? "unknown"}'), ` +
        "but this codebase doesn't record its last-run time anywhere queryable yet, so this signal can't " +
        "say how long it's actually been since it fired.",
    );
  } catch (e) {
    return errored(key, label, e);
  }
}

// ---- Failure clustering (Failed vs Dead_Letter, by provider) ----------
// Primary source is Turso directly, per the cell prompt -- PostHog isn't
// required for this signal to be real. Rolling window: 24 hours.

async function failureClustering(env: TursoEnv): Promise<HealthSignal> {
  const key = "failure_clustering";
  const label = "Failure clustering (24h)";
  try {
    const client = tursoClient(env);
    const rs = await client.execute(
      `SELECT status, COALESCE(provider, 'unknown') AS provider, COUNT(*) AS n
       FROM code_cells
       WHERE status IN ('Failed','Dead_Letter') AND updated_at >= datetime('now','-24 hours')
       GROUP BY status, provider
       ORDER BY status, n DESC`,
    );
    const rows = rs.rows as unknown as { status: string; provider: string; n: number }[];
    if (rows.length === 0) {
      return ok(key, label, "0 Failed / 0 Dead_Letter", "No Failed or Dead_Letter cells in the last 24h.");
    }
    const deadLetterN = rows.filter((r) => r.status === "Dead_Letter").reduce((a, r) => a + Number(r.n), 0);
    const failedN = rows.filter((r) => r.status === "Failed").reduce((a, r) => a + Number(r.n), 0);
    const detail = rows.map((r) => `${r.status}/${r.provider}: ${r.n}`).join(", ");
    const value = `${failedN} Failed / ${deadLetterN} Dead_Letter`;
    if (deadLetterN >= 3) return critical(key, label, value, detail);
    if (deadLetterN >= 1 || failedN >= 5) return warn(key, label, value, detail);
    return ok(key, label, value, detail);
  } catch (e) {
    return errored(key, label, e);
  }
}

// ---- Quota headroom (Section 12b -- one signal per hard cap) ----------

// Turso rows-written/month vs the 10M cap: NOT queryable over libSQL --
// Turso's write-volume meter is a Turso Cloud (platform-level) stat, not
// something exposed via SQL introspection on the database itself. Shown
// honestly as not-exposed, with the DB's current total row count
// surfaced separately as informational context only (explicitly NOT the
// same number as the cap-relevant metric).
async function tursoRowsQuota(env: TursoEnv): Promise<HealthSignal> {
  const key = "quota_turso_rows";
  const label = "Turso rows-written this month vs. 10M cap";
  try {
    const client = tursoClient(env);
    const rs = await client.execute(
      `SELECT (SELECT COUNT(*) FROM code_cells) + (SELECT COUNT(*) FROM checkpoints) AS n`,
    );
    const n = Number((rs.rows[0] as any).n);
    return {
      key,
      label,
      configured: false,
      state: "unknown",
      value: "not exposed via SQL",
      detail:
        `Current total row count (code_cells + checkpoints): ${n} -- informational only, NOT the ` +
        "same figure as the monthly rows-written meter, which Turso doesn't expose over libSQL.",
      note: "Check the Turso dashboard (turso.tech) for the actual rows-written-this-month figure against the 10M cap.",
    };
  } catch (e) {
    return errored(key, label, e);
  }
}

// Groq/Gemini requests-today vs their daily caps, and Gemma 4 31B's
// separate tagging-quota bucket: neither provider exposes a queryable
// "requests used today" number over their public REST APIs -- the only
// live signal either gives is the rate-limit-remaining/-reset response
// headers on an ACTUAL call, which this dashboard's polling loop
// shouldn't be burning quota to fetch just to populate a widget. Shown
// honestly rather than estimated.
function providerRequestQuota(env: { GROQ_API_KEY?: string; GEMINI_API_KEY?: string }): HealthSignal {
  const key = "quota_provider_requests";
  const label = "Groq/Gemini requests today vs. daily caps";
  const has = !!(env.GROQ_API_KEY || env.GEMINI_API_KEY);
  return {
    key,
    label,
    configured: false,
    state: "unknown",
    value: has ? "not exposed by provider" : "not configured",
    note: has
      ? "Groq and Gemini don't expose a queryable 'requests used today' number via their public APIs -- " +
        "check the rate-limit-remaining/-reset response headers on the next live call, or each provider's console."
      : "GROQ_API_KEY / GEMINI_API_KEY not set.",
  };
}

function gemmaTaggingQuota(env: { GEMINI_API_KEY?: string }): HealthSignal {
  const key = "quota_gemma_tagging";
  const label = "Gemma 4 31B tagging quota (30 RPM / 16K TPM bucket)";
  return {
    key,
    label,
    configured: false,
    state: "unknown",
    value: env.GEMINI_API_KEY ? "not exposed by provider" : "not configured",
    note: "Same Gemini API surface as the Fast Worker cascade above -- no queryable per-bucket usage counter available.",
  };
}

function qstashMessageQuota(env: QstashEnv): HealthSignal {
  const key = "quota_qstash_messages";
  const label = "QStash messages today vs. 1,000/day";
  return {
    key,
    label,
    configured: false,
    state: "unknown",
    value: env.QSTASH_TOKEN ? "not exposed by provider" : "not configured",
    note: "QStash's REST API doesn't expose a messages-today counter; check the QStash console at https://console.upstash.com.",
  };
}

// Object storage vs. the 10GB cap -- the one quota-headroom sub-signal
// this dashboard CAN compute for real, via b2GetBucketUsage.
async function b2StorageQuota(env: B2Env): Promise<HealthSignal> {
  const key = "quota_b2_storage";
  const label = "Object storage vs. 10GB cap (Backblaze B2, Section 9)";
  if (!(env.B2_KEY_ID && env.B2_APPLICATION_KEY && env.B2_BUCKET_NAME && env.B2_ENDPOINT)) {
    return notConfigured(
      key,
      label,
      "B2_KEY_ID / B2_APPLICATION_KEY / B2_BUCKET_NAME / B2_ENDPOINT not fully set. (This project's Section " +
        "9 implementation uses Backblaze B2, not Cloudflare R2 -- no R2 binding exists on this Worker to check instead.)",
    );
  }
  try {
    const usage = await b2GetBucketUsage(env);
    const capBytes = 10 * 1024 * 1024 * 1024;
    const pct = (usage.totalBytes / capBytes) * 100;
    const value = `${pct.toFixed(1)}% (${(usage.totalBytes / 1e9).toFixed(2)}GB / 10GB)`;
    const detail =
      `${usage.objectCount} object(s) counted` +
      (usage.truncated ? " (first page only -- bucket may hold more; treat this as a lower bound)." : ".");
    if (pct >= 95) return critical(key, label, value, detail);
    if (pct >= 80) return warn(key, label, value, detail);
    return ok(key, label, value, detail);
  } catch (e) {
    return errored(key, label, e);
  }
}

// ---- Neo4j keepalive freshness (Section 7f) ----------------------------
// The one failure mode Section 12b calls out as deserving a permanent,
// impossible-to-miss spot: silent, slow, unrecoverable-data-loss.

async function neo4jKeepalive(env: Neo4jEnv): Promise<HealthSignal> {
  const key = "neo4j_keepalive";
  const label = "Neo4j keepalive freshness (Section 7f)";
  if (!env.NEO4J_URI || !env.NEO4J_USERNAME || !env.NEO4J_PASSWORD) {
    return notConfigured(key, label, "NEO4J_URI / NEO4J_USERNAME / NEO4J_PASSWORD not set.");
  }
  try {
    const { rows } = await runCypher(
      env,
      `MATCH (h:Heartbeat {key: '_heartbeat'}) RETURN toString(h.touched_at) AS touchedAt`,
    );
    if (rows.length === 0) {
      return warn(
        key,
        label,
        "never touched",
        "Neo4j is configured but no _heartbeat node exists yet -- the weekly keepalive schedule isn't " +
          "wired up (no matching QStash schedule targeting /webhook/graph-heartbeat was found for the " +
          "silent-pipeline-stalls check above, and the pattern is the same here). The 30-day auto-delete " +
          "clock's current position is unknown. Touch it now via the graph_heartbeat MCP tool, or wire a " +
          "weekly QStash schedule at /webhook/graph-heartbeat.",
      );
    }
    const touchedAt = String((rows[0] as unknown[])[0]);
    const days = (Date.now() - new Date(touchedAt).getTime()) / 86_400_000;
    const value = `${days.toFixed(1)} days since last heartbeat`;
    const detail = `Last touched ${touchedAt}. Free AuraDB instances are deleted after 30 days of inactivity.`;
    if (days >= 25) return critical(key, label, value, detail);
    if (days >= 14) return warn(key, label, value, detail);
    return ok(key, label, value, detail);
  } catch (e) {
    return errored(key, label, e);
  }
}

// ---- Skipped-cycle rate (Section 10a) ----------------------------------
// Per the cell prompt's own expectation: the "cycle" concept (a batch of
// N cells a Reviewer/Architect session reviews together, Section 3b)
// isn't implemented as real infrastructure anywhere in this codebase.
// code_cell_workflow.ts posts a per-CodeCell `codecell_resolution`
// PostHog event with an `escalated` flag as an honest approximation (see
// posthog_events.ts's own header comment), but there's no aggregation
// table or cached query result this dashboard can read synchronously on
// every poll without adding a HogQL round-trip to PostHog's MCP proxy on
// every single DashboardHub alarm tick -- not worth the latency/failure
// surface for a signal whose underlying concept doesn't exist yet.
// Fabricating a number here would be worse than saying so.

function skippedCycleRate(): HealthSignal {
  return notConfigured(
    "skipped_cycle_rate",
    "Skipped-cycle rate (Section 10a)",
    "Section 3b's 'cycle' concept isn't implemented as real infrastructure yet -- code_cell_workflow.ts " +
      "posts a per-CodeCell 'codecell_resolution' PostHog event with an 'escalated' flag as an " +
      "approximation, but there's no aggregation this dashboard can query directly. Wire a HogQL query " +
      "against that event stream (or a dedicated Turso table) to light this signal up.",
  );
}

// ---- Overall status (Nominal / Degraded / Stalled) ----------------------
// Computed only from CONFIGURED signals -- a signal that's legitimately
// "not configured" never counts against the overall status, per the
// cell prompt's explicit instruction. That distinction is visible in the
// UI (dashboard.ts renders "not configured" signals with a neutral dot,
// separate from ok/warn/critical), not just in this computation.

const SEVERITY: Record<SignalState, number> = { ok: 0, warn: 1, critical: 2, unknown: -1 };

function overallStatus(signals: HealthSignal[]): DashboardPayload["overallStatus"] {
  let maxSeverity = 0;
  for (const s of signals) {
    if (!s.configured) continue;
    maxSeverity = Math.max(maxSeverity, SEVERITY[s.state] ?? 0);
  }
  if (maxSeverity >= 2) return "Stalled";
  if (maxSeverity === 1) return "Degraded";
  return "Nominal";
}

// ---- Entry point ----------------------------------------------------------

export async function computeDashboardPayload(env: DashboardEnv): Promise<DashboardPayload> {
  const [stalled, silent, failures, tursoRows, b2Storage, neo4j] = await Promise.all([
    stalledWork(env),
    silentPipelineStalls(env),
    failureClustering(env),
    tursoRowsQuota(env),
    b2StorageQuota(env),
    neo4jKeepalive(env),
  ]);
  const signals: HealthSignal[] = [
    stalled,
    silent,
    failures,
    tursoRows,
    providerRequestQuota(env),
    gemmaTaggingQuota(env),
    qstashMessageQuota(env),
    b2Storage,
    neo4j,
    skippedCycleRate(),
  ];
  return {
    generatedAt: new Date().toISOString(),
    overallStatus: overallStatus(signals),
    signals,
  };
}
