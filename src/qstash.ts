// qstash.ts
//
// Direct port onto Upstash QSTASH's own REST API
// (https://qstash.upstash.io/v2) -- this is deliberately a SEPARATE
// module and a SEPARATE credential from upstash.ts. upstash.ts wraps
// api.upstash.com, the account-wide Developer/Management API (create,
// list, delete Redis databases -- and previously nothing else). QStash
// is a different product with its own dedicated bearer token, scoped
// only to publishing/scheduling messages -- it cannot touch Redis
// databases at all. Folding QStash into upstash.ts would have been
// wrong on both counts (wrong base URL, wrong auth), so it gets its own
// file and its own secret.
//
// This closes the gap the stack doc actually needs QStash for: Section
// 4 step 3's "Traffic Control" (pacing calls to Fast Workers), and
// Section 4a's dead-letter backstop cron (a QStash schedule that
// re-fires every N minutes to sweep stuck `Pending` cells).
//
// Section 4f addendum: flow-control support. Cloudflare Workflows
// durable-executes retry/persistence for a SINGLE CodeCell instance, but
// has no primitive for "don't send more than N requests/minute to Groq
// across every concurrently-running instance" -- that's a cross-instance
// concern QStash's flow-control headers exist for. A publish carrying
// Upstash-Flow-Control-Key/-Value shares its wait list with every other
// publish using the same key, regardless of which CodeCellWorkflow
// instance sent it (confirmed against Upstash's own docs: rate/period
// and/or parallelism, combinable, scoped per key not per destination
// URL). code_cell_workflow.ts's fast-worker-generate step is the
// concrete caller -- see the comment there for why the destination is
// this same Worker's own /qstash/fast-worker-generate route.
//
// Auth: single bearer token, QSTASH_TOKEN (Worker secret, from the
// QStash tab at https://console.upstash.com).

export interface QstashEnv {
  QSTASH_TOKEN?: string;
}

const QSTASH_API = "https://qstash.upstash.io/v2";

function requireQstashToken(env: QstashEnv): string {
  if (!env.QSTASH_TOKEN) {
    throw new Error(
      "QSTASH_TOKEN is not configured on this Worker. Get it from the QStash tab at " +
        "https://console.upstash.com, then run: wrangler secret put QSTASH_TOKEN",
    );
  }
  return env.QSTASH_TOKEN;
}

async function qstashFetch(
  env: QstashEnv,
  method: "GET" | "POST" | "DELETE",
  path: string,
  opts?: { body?: unknown; extraHeaders?: Record<string, string> },
): Promise<string> {
  const token = requireQstashToken(env);
  const headers: Record<string, string> = {
    Authorization: `Bearer ${token}`,
    ...(opts?.extraHeaders ?? {}),
  };
  if (opts?.body !== undefined) headers["Content-Type"] = "application/json";
  const resp = await fetch(`${QSTASH_API}${path}`, {
    method,
    headers,
    body: opts?.body !== undefined ? JSON.stringify(opts.body) : undefined,
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`QStash API ${method} ${path} returned ${resp.status}: ${text.slice(0, 500)}`);
  }
  return text;
}

function prettyJson(text: string): string {
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

// Flow-control config for a publish: shared across every publish that
// uses the same `key`, regardless of destination URL or which caller
// sent it -- this is what makes it a real cross-instance pacing
// primitive rather than a per-call setting. At least one of
// rate/parallelism/period-bearing-rate must be set or QStash has nothing
// to enforce; validated below rather than silently sending a key with
// no limit attached.
export interface QstashFlowControl {
  key: string;
  rate?: number;
  parallelism?: number;
  /** e.g. "1m", "30s". Only meaningful alongside `rate`; QStash defaults to 1s if omitted. */
  period?: string;
}

// Publish a one-off message to a destination URL, with optional
// delay/retry/callback -- this is the "hold the event, ensure API
// limits aren't breached, then push a webhook" step (Section 4 step 3).
export async function qstashPublish(
  env: QstashEnv,
  destinationUrl: string,
  body: unknown,
  opts?: {
    delaySeconds?: number;
    retries?: number;
    callbackUrl?: string;
    contentType?: string;
    flowControl?: QstashFlowControl;
    extraHeaders?: Record<string, string>;
  },
): Promise<string> {
  try {
    const extraHeaders: Record<string, string> = { ...(opts?.extraHeaders ?? {}) };
    if (opts?.delaySeconds !== undefined) extraHeaders["Upstash-Delay"] = `${opts.delaySeconds}s`;
    if (opts?.retries !== undefined) extraHeaders["Upstash-Retries"] = String(opts.retries);
    if (opts?.callbackUrl) extraHeaders["Upstash-Callback"] = opts.callbackUrl;
    if (opts?.contentType) extraHeaders["Content-Type"] = opts.contentType;
    if (opts?.flowControl) {
      const { key, rate, parallelism, period } = opts.flowControl;
      const parts: string[] = [];
      if (rate !== undefined) parts.push(`rate=${rate}`);
      if (parallelism !== undefined) parts.push(`parallelism=${parallelism}`);
      if (period !== undefined) parts.push(`period=${period}`);
      if (parts.length === 0) {
        throw new Error("flowControl.key was set but none of rate/parallelism/period were provided -- nothing to enforce.");
      }
      extraHeaders["Upstash-Flow-Control-Key"] = key;
      extraHeaders["Upstash-Flow-Control-Value"] = parts.join(",");
    }
    const raw = await qstashFetch(env, "POST", `/publish/${destinationUrl}`, {
      body,
      extraHeaders,
    });
    return prettyJson(raw);
  } catch (e) {
    return `Error publishing to QStash: ${e}`;
  }
}

// Create a recurring cron schedule -- this is Section 4a's low-frequency
// backstop ("every 10 minutes, query Turso for stuck-Pending cells and
// re-fire them").
export async function qstashCreateSchedule(
  env: QstashEnv,
  destinationUrl: string,
  cron: string,
  body?: unknown,
  opts?: { retries?: number },
): Promise<string> {
  try {
    const extraHeaders: Record<string, string> = { "Upstash-Cron": cron };
    if (opts?.retries !== undefined) extraHeaders["Upstash-Retries"] = String(opts.retries);
    const raw = await qstashFetch(env, "POST", `/schedules/${destinationUrl}`, {
      body: body ?? {},
      extraHeaders,
    });
    return prettyJson(raw);
  } catch (e) {
    return `Error creating QStash schedule: ${e}`;
  }
}

export async function qstashListSchedules(env: QstashEnv): Promise<string> {
  try {
    return prettyJson(await qstashFetch(env, "GET", "/schedules"));
  } catch (e) {
    return `Error listing QStash schedules: ${e}`;
  }
}

export async function qstashDeleteSchedule(env: QstashEnv, scheduleId: string): Promise<string> {
  try {
    await qstashFetch(env, "DELETE", `/schedules/${encodeURIComponent(scheduleId)}`);
    return `Deleted schedule ${scheduleId}.`;
  } catch (e) {
    return `Error deleting QStash schedule: ${e}`;
  }
}
