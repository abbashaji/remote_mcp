// review_cycles.ts
//
// Section 3b / 10a of Zero-Cost-Stack-v11.md: real infrastructure for the
// "cycle" concept, replacing the per-CodeCell `escalated`-flag
// approximation that code_cell_workflow.ts / posthog_events.ts previously
// shipped as a stopgap (see postHogCaptureCodeCellResolution's header
// comment, and dashboard_signals.ts's old skippedCycleRate() stub, both
// of which said plainly that this didn't exist yet).
//
// What a "cycle" actually is, per 3b: a batch of N cells' worth of
// results that Layer 0 (Section 4) and Section 4c's tagging layer
// accumulate before a Reviewer/Architect Context Slot picks anything up.
// "Skipped-cycle rate" (10a) is the fraction of those batches that closed
// needing zero Context Slot involvement. That's a property of a BATCH,
// not of any single CodeCell -- which is exactly what the old
// `escalated` flag couldn't express (it's per-cell by construction).
// This module gives cycles a real row, a real close event, and a real
// floor, so the rate above is a genuine aggregate query, not a proxy.
//
// Design, deliberately mechanical (Layer 0 / Section 4c territory, per
// 3b's own escalation test -- nothing here is a judgment call):
//
//   - Exactly one OPEN cycle exists at a time (review_cycle_state is a
//     singleton row pointing at it). Every CodeCell resolution
//     (classifyAndRecordResult in code_cell_workflow.ts) calls
//     recordCycleItem() once, which increments that cycle's item_count
//     and, if the resolution crossed Section 3b's escalation test
//     (the same "needs_human"/"dead_letter" vs. "passed"/
//     "known_flake_pattern" line the per-cell `escalated` flag already
//     drew), its escalation_count too.
//   - A cycle closes -- mechanically, no judgment involved -- once
//     EITHER it holds CYCLE_BATCH_SIZE items OR it's been open longer
//     than CYCLE_MAX_AGE_HOURS. The size check happens inline inside
//     recordCycleItem (the common case: enough traffic that an item
//     arrives to trigger it). The age check has no traffic to trigger
//     it if the pipeline goes quiet, so sweepStaleReviewCycle() below is
//     wired into the SAME low-frequency backstop cron Section 4a's
//     stuck-Pending sweep already uses (/webhook/pending-sweep,
//     auth.ts) -- one existing cron, two mechanical checks, not a new
//     Section-2 row.
//   - **The floor (3b's "this can't become silent drift" paragraph):**
//     review_cycle_state.consecutive_skipped counts closed cycles in a
//     row with zero escalations. If closing a cycle would make that
//     streak reach CYCLE_REVIEW_FLOOR, this cycle is forced to trigger
//     anyway (trigger_reason='floor') even though nothing in it crossed
//     the escalation bar on its own -- and, because a floor trip is
//     exactly the "genuinely stale Decision/gap sitting unexamined"
//     case 3b calls out, it also pushes a Web Push alert (push.ts,
//     already this project's alert channel for needs_human/dead_letter)
//     so it doesn't just sit as a dashboard number nobody opens.
//   - Every close is posted to PostHog (postHogCaptureReviewCycle,
//     posthog_events.ts) alongside the existing per-cell events, AND
//     written to the review_cycles table itself -- the table is the
//     thing dashboard_signals.ts's skippedCycleRate() can now query
//     directly and synchronously, no HogQL round-trip needed on every
//     DashboardHub poll tick.
//
// Non-overlap rule, same shape as Section 10's for PostHog generally:
// this module is a read/trend surface. Nothing about whether a cycle
// "triggered" a Context Slot gates or blocks anything in the CodeCell
// pipeline itself -- Turso's code_cells.status remains the only thing
// orchestration logic reads. A cycle closing "skipped" doesn't skip any
// actual review that Section 4a's push-alert path would otherwise have
// sent for an urgent cell; it's purely the aggregate signal 10a asks for.
//
// Every exported function here follows this project's existing
// tool convention: PostHog/push-notification side effects are always
// best-effort (caught, logged, never thrown) so a trend-data hiccup
// can never block a CodeCell resolution from completing. Turso writes
// themselves DO throw on failure -- same as every other codecells.ts
// function -- since a lost cycle-accounting row is a real bug worth
// surfacing, not something to silently swallow.

import { createClient, type Client } from "@libsql/client/web";
import type { TursoEnv } from "./turso";
import type { Env } from "./index";
import { sendWebPushToAll } from "./push";
import { postHogCaptureReviewCycle } from "./posthog_events";

// Read-only surface (schema/summary queries) only ever needs Turso plus
// the three cadence knobs below -- deliberately NOT the full `Env` type
// that recordCycleItem/closeCycle/sweepStaleReviewCycle need for their
// push-alert/PostHog side effects, so dashboard_signals.ts's narrower
// `DashboardEnv` (TursoEnv & ... , no VAPID/PostHog fields) can call
// getReviewCycleSummary() directly without widening its own env type
// just to satisfy this module.
export interface ReviewCycleEnv extends TursoEnv {
  CYCLE_BATCH_SIZE?: string;
  CYCLE_MAX_AGE_HOURS?: string;
  CYCLE_REVIEW_FLOOR?: string;
}

function getClient(env: TursoEnv): Client {
  if (!env.TURSO_DATABASE_URL) {
    throw new Error("TURSO_DATABASE_URL is not configured on this Worker.");
  }
  return createClient(
    env.TURSO_AUTH_TOKEN
      ? { url: env.TURSO_DATABASE_URL, authToken: env.TURSO_AUTH_TOKEN }
      : { url: env.TURSO_DATABASE_URL },
  );
}

// ---- tunables -------------------------------------------------------
// All three are cadence/floor knobs 3b explicitly leaves to "whatever
// cadence matches actual pace of work" -- exposed as env vars rather
// than hardcoded so that's actually adjustable without a redeploy of
// the constant itself, same pattern FAST_WORKER_RATE_PER_MINUTE already
// uses in code_cell_workflow.ts.

function batchSize(env: ReviewCycleEnv): number {
  return Number(env.CYCLE_BATCH_SIZE ?? "5");
}
function maxAgeHours(env: ReviewCycleEnv): number {
  return Number(env.CYCLE_MAX_AGE_HOURS ?? "24");
}
function reviewFloor(env: ReviewCycleEnv): number {
  return Number(env.CYCLE_REVIEW_FLOOR ?? "5");
}

export async function ensureReviewCycleSchema(env: ReviewCycleEnv): Promise<void> {
  const client = getClient(env);
  await client.batch(
    [
      `CREATE TABLE IF NOT EXISTS review_cycles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        opened_at TEXT NOT NULL DEFAULT (datetime('now')),
        closed_at TEXT,
        item_count INTEGER NOT NULL DEFAULT 0,
        escalation_count INTEGER NOT NULL DEFAULT 0,
        context_slot_triggered INTEGER,  -- NULL while open; 0/1 once closed
        trigger_reason TEXT,             -- 'escalation' | 'floor' | NULL (skipped)
        close_reason TEXT                -- 'batch_size' | 'max_age' | NULL while open
      )`,
      // Singleton current-cycle pointer + floor counter. id=1 enforced
      // by the CHECK, same "exactly one row" pattern project_state.ts's
      // key-value table achieves with a PRIMARY KEY instead.
      `CREATE TABLE IF NOT EXISTS review_cycle_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        current_cycle_id INTEGER NOT NULL,
        consecutive_skipped INTEGER NOT NULL DEFAULT 0
      )`,
    ],
    "write",
  );
}

async function openNewCycle(client: Client): Promise<number> {
  const rs = await client.execute(`INSERT INTO review_cycles DEFAULT VALUES RETURNING id`);
  return Number(rs.rows[0].id);
}

interface CycleRow {
  id: number;
  opened_at: string;
  closed_at: string | null;
  item_count: number;
  escalation_count: number;
}

// Reads the current open cycle, opening one (and the singleton state
// row alongside it) if this is the very first call this database has
// ever seen. Idempotent under concurrent callers in the sense that
// matters here: worst case, two near-simultaneous first-ever calls each
// open a cycle and only one wins the state-row INSERT OR IGNORE below --
// the loser's cycle row is simply never referenced again, a harmless
// orphan rather than a correctness problem, and normal operation from
// then on only ever has one current_cycle_id.
async function getOrOpenCurrentCycle(client: Client): Promise<{ stateExists: boolean; cycle: CycleRow }> {
  const stateRs = await client.execute(`SELECT current_cycle_id FROM review_cycle_state WHERE id = 1`);
  if (stateRs.rows.length > 0) {
    const cycleId = Number(stateRs.rows[0].current_cycle_id);
    const cycleRs = await client.execute({
      sql: `SELECT id, opened_at, closed_at, item_count, escalation_count FROM review_cycles WHERE id = ?`,
      args: [cycleId],
    });
    if (cycleRs.rows.length > 0) {
      return { stateExists: true, cycle: cycleRs.rows[0] as unknown as CycleRow };
    }
    // State pointed at a cycle row that's somehow gone -- fall through
    // and self-heal by opening a fresh one below.
  }
  const newId = await openNewCycle(client);
  await client.execute({
    sql: `INSERT INTO review_cycle_state (id, current_cycle_id, consecutive_skipped)
          VALUES (1, ?, 0)
          ON CONFLICT (id) DO UPDATE SET current_cycle_id = excluded.current_cycle_id`,
    args: [newId],
  });
  const cycleRs = await client.execute({
    sql: `SELECT id, opened_at, closed_at, item_count, escalation_count FROM review_cycles WHERE id = ?`,
    args: [newId],
  });
  return { stateExists: false, cycle: cycleRs.rows[0] as unknown as CycleRow };
}

export interface CycleCloseResult {
  cycleId: number;
  itemCount: number;
  escalationCount: number;
  contextSlotTriggered: boolean;
  triggerReason: "escalation" | "floor" | null;
  closeReason: "batch_size" | "max_age";
  nextCycleId: number;
}

// Closes exactly the cycle passed in (must currently be the open one),
// decides context_slot_triggered per the floor rule described in this
// file's header, opens the next cycle, and fires the two best-effort
// side effects (PostHog write, floor push-alert). Not exported directly
// -- callers go through recordCycleItem / sweepStaleReviewCycle so the
// "is this cycle actually still open and current" check always happens
// under the same read-then-write path.
async function closeCycle(
  env: Env,
  client: Client,
  cycle: CycleRow,
  closeReason: "batch_size" | "max_age",
): Promise<CycleCloseResult> {
  const stateRs = await client.execute(`SELECT consecutive_skipped FROM review_cycle_state WHERE id = 1`);
  const consecutiveSkipped = stateRs.rows.length > 0 ? Number(stateRs.rows[0].consecutive_skipped) : 0;
  const floor = reviewFloor(env);

  const escalated = cycle.escalation_count > 0;
  let triggered: boolean;
  let triggerReason: "escalation" | "floor" | null;
  let nextConsecutiveSkipped: number;
  let floorTripped = false;

  if (escalated) {
    triggered = true;
    triggerReason = "escalation";
    nextConsecutiveSkipped = 0;
  } else if (consecutiveSkipped + 1 >= floor) {
    // 3b: "wire in a floor ... so a genuinely stale Decision or
    // gap-shaped question doesn't sit unexamined." This cycle itself
    // crossed no escalation bar, but the skip streak it would extend
    // has now hit the floor -- force it to trigger instead of skip.
    triggered = true;
    triggerReason = "floor";
    nextConsecutiveSkipped = 0;
    floorTripped = true;
  } else {
    triggered = false;
    triggerReason = null;
    nextConsecutiveSkipped = consecutiveSkipped + 1;
  }

  const nextCycleId = await openNewCycle(client);
  await client.batch(
    [
      {
        sql: `UPDATE review_cycles
              SET closed_at = datetime('now'), context_slot_triggered = ?, trigger_reason = ?, close_reason = ?
              WHERE id = ?`,
        args: [triggered ? 1 : 0, triggerReason, closeReason, cycle.id],
      },
      {
        sql: `UPDATE review_cycle_state SET current_cycle_id = ?, consecutive_skipped = ? WHERE id = 1`,
        args: [nextCycleId, nextConsecutiveSkipped],
      },
    ],
    "write",
  );

  const result: CycleCloseResult = {
    cycleId: cycle.id,
    itemCount: cycle.item_count,
    escalationCount: cycle.escalation_count,
    contextSlotTriggered: triggered,
    triggerReason,
    closeReason,
    nextCycleId,
  };

  // ---- best-effort side effects, never allowed to throw past here ----
  try {
    const r = await postHogCaptureReviewCycle(env, {
      cycleId: cycle.id,
      itemCount: cycle.item_count,
      escalationCount: cycle.escalation_count,
      contextSlotTriggered: triggered,
      triggerReason,
      closeReason,
    });
    if (r.startsWith("Error ")) {
      console.error(`PostHog review-cycle capture failed for cycle #${cycle.id} (non-blocking): ${r}`);
    }
  } catch (e) {
    console.error(`PostHog review-cycle capture threw unexpectedly for cycle #${cycle.id} (non-blocking): ${e}`);
  }

  if (floorTripped) {
    try {
      await sendWebPushToAll(env, {
        title: "Skipped-cycle floor reached",
        body:
          `Cycle #${cycle.id} closed with no escalation, but that's ${floor} skipped cycles in a row -- ` +
          `Section 3b's minimum-cadence floor forced a review anyway. Worth a Reviewer/Architect look, ` +
          `not just a dashboard number.`,
        tag: "review-cycle-floor",
      });
    } catch (e) {
      // Same asymmetry code_cell_workflow.ts's notify() applies to
      // urgent-tag push alerts vs. PostHog capture: a genuine delivery
      // failure here is logged, not silently dropped, but it can't be
      // allowed to fail the cycle-close transaction that's already
      // committed to Turso above.
      console.error(`Floor-trip push alert failed for cycle #${cycle.id} (non-blocking): ${e}`);
    }
  }

  return result;
}

// Called once per CodeCell resolution (classifyAndRecordResult in
// code_cell_workflow.ts), with the same `escalated` boolean it already
// computes for the per-cell PostHog event -- Section 3b's escalation
// test applied at cell granularity is exactly right as the INPUT to a
// cycle's aggregate; what was missing was the batch/aggregate layer
// itself, not a new definition of what counts as an escalation.
export async function recordCycleItem(env: Env, escalated: boolean): Promise<CycleCloseResult | null> {
  await ensureReviewCycleSchema(env);
  const client = getClient(env);
  const { cycle } = await getOrOpenCurrentCycle(client);

  await client.execute({
    sql: `UPDATE review_cycles SET item_count = item_count + 1, escalation_count = escalation_count + ? WHERE id = ?`,
    args: [escalated ? 1 : 0, cycle.id],
  });

  const updated: CycleRow = {
    ...cycle,
    item_count: cycle.item_count + 1,
    escalation_count: cycle.escalation_count + (escalated ? 1 : 0),
  };

  if (updated.item_count >= batchSize(env)) {
    return closeCycle(env, client, updated, "batch_size");
  }
  return null;
}

// Wired into the existing /webhook/pending-sweep backstop cron
// (auth.ts / Section 4a) alongside sweepPendingCells -- the age-based
// close has nothing to trigger it if no new CodeCell resolves for a
// while, same "a mechanical check needs a mechanical heartbeat" reason
// Section 4a's own stuck-Pending sweep already exists. Best-effort by
// design: returns null (nothing to do) rather than throwing when the
// open cycle isn't stale yet, and the caller in auth.ts treats a real
// error the same way sweepPendingCells's errors are already surfaced.
export async function sweepStaleReviewCycle(env: Env): Promise<CycleCloseResult | null> {
  await ensureReviewCycleSchema(env);
  const client = getClient(env);
  const { cycle } = await getOrOpenCurrentCycle(client);
  if (cycle.item_count === 0) return null; // nothing accumulated yet -- an empty cycle closing tells no one anything
  // Turso's datetime('now') default column value is SQLite's own
  // "YYYY-MM-DD HH:MM:SS" (UTC, space-separated, no 'Z') -- not
  // directly Date-parseable across JS engines. Normalize to ISO-8601
  // before handing it to `new Date(...)`, same fix neo4jKeepalive
  // (dashboard_signals.ts) doesn't need only because Neo4j's
  // toString(datetime) already returns a real ISO string.
  const ageHours = (Date.now() - new Date(cycle.opened_at.replace(" ", "T") + "Z").getTime()) / 3_600_000;
  if (ageHours < maxAgeHours(env)) return null;
  return closeCycle(env, client, cycle, "max_age");
}

export interface ReviewCycleSummary {
  currentCycle: { id: number; openedAt: string; itemCount: number; escalationCount: number };
  consecutiveSkipped: number;
  reviewFloor: number;
  window: number;
  closedInWindow: number;
  skippedInWindow: number;
  skippedRate: number | null; // null when closedInWindow === 0 -- nothing to divide by yet
}

// Shared by the cycle_status MCP tool (index.ts) and dashboard_signals.ts's
// skippedCycleRate() -- one query shape, two callers, same "don't drift"
// rationale dashboard_signals.ts's own header comment already applies to
// the dashboard.ts/dashboard_do.ts split.
export async function getReviewCycleSummary(env: ReviewCycleEnv, window: number = 20): Promise<ReviewCycleSummary> {
  await ensureReviewCycleSchema(env);
  const client = getClient(env);
  const { cycle } = await getOrOpenCurrentCycle(client);
  const stateRs = await client.execute(`SELECT consecutive_skipped FROM review_cycle_state WHERE id = 1`);
  const consecutiveSkipped = stateRs.rows.length > 0 ? Number(stateRs.rows[0].consecutive_skipped) : 0;

  const closedRs = await client.execute({
    sql: `SELECT context_slot_triggered FROM review_cycles
          WHERE closed_at IS NOT NULL
          ORDER BY closed_at DESC
          LIMIT ?`,
    args: [window],
  });
  const closedInWindow = closedRs.rows.length;
  const skippedInWindow = closedRs.rows.filter((r) => Number((r as any).context_slot_triggered) === 0).length;

  return {
    currentCycle: {
      id: cycle.id,
      openedAt: cycle.opened_at,
      itemCount: cycle.item_count,
      escalationCount: cycle.escalation_count,
    },
    consecutiveSkipped,
    reviewFloor: reviewFloor(env),
    window,
    closedInWindow,
    skippedInWindow,
    skippedRate: closedInWindow > 0 ? skippedInWindow / closedInWindow : null,
  };
}
