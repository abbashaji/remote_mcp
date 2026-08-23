// posthog_events.ts
//
// Section 10/10a: PostHog EVENT INGESTION (capture) -- distinct from
// posthog.ts, which proxies PostHog's own hosted remote MCP server for
// querying/managing PostHog (projects, insights, feature flags, HogQL,
// error-tracking issue queries, CDP destinations, etc.).
//
// Checked before writing this file, not assumed: posthog_list_tools
// (via posthog.ts, live against this project's actual PostHog MCP
// connection) was called first. Its catalog is large (error-tracking-*
// issue/alert/rule management, query-error-tracking-issue*, and dozens
// of analytics/insight/experiment tools) but contains NO tool that
// sends a *new* event or exception INTO PostHog -- ingestion isn't part
// of PostHog's management MCP surface. That surface is PostHog's
// separate Capture API (https://<region>.i.posthog.com/i/v0/e/),
// authenticated with a PROJECT API KEY, which is a different, write-
// only credential from the Personal API key (POSTHOG_API_KEY) that
// posthog.ts already uses for the MCP proxy -- see
// POSTHOG_PROJECT_API_KEY's doc comment in wrangler.toml for exactly
// how the two differ. This file therefore implements a direct HTTP
// call to the Capture endpoint rather than guessing a management-MCP
// tool name that doesn't exist.
//
// No posthog-js/posthog-node SDK dependency: neither targets the
// Workers runtime cleanly, and the ingestion API itself is a single
// stable JSON POST -- not worth a dependency for one endpoint.
//
// Two event shapes, both routed through the shared captureEvent() below:
//
//   - postHogCaptureException(): Section 10's per-Failed/Dead_Letter
//     write. Sent as a "$exception" event with PostHog's minimal Error
//     Tracking properties ($exception_list / $exception_type /
//     $exception_message) so PostHog's Error Tracking product
//     recognizes it as an exception rather than an ordinary analytics
//     event. This is a best-effort minimal shape -- the official SDKs
//     attach richer stack-frame data captured client-side; a Worker
//     calling this API directly has no client stack to walk, only the
//     CodeCell's own last_error text -- but the $exception_list/type/
//     message trio is what PostHog's Error Tracking UI keys off, so
//     these still land in Error Tracking, not just as a generic event.
//     Also carries cell_id/tag/status/provider as plain queryable
//     properties, for exactly the "Groq failures in the last hour"
//     style trend queries Section 10/12b describe.
//
//   - postHogCaptureCodeCellResolution(): Section 10a's per-resolution
//     write (see code_cell_workflow.ts's tag-result step). A plain
//     analytics event (counts against PostHog's separate, larger
//     general-events allowance, not the exceptions one), since it
//     fires on every resolution including passing ones, not just
//     failures.
//
// Convention match: every exported function here returns an error
// STRING on failure and never throws, same as every other provider-
// calling function in this project. Section 10's "optional, not
// load-bearing" framing depends on this -- callers in
// code_cell_workflow.ts treat a returned error string as "log it, move
// on," never as something to propagate into a Workflow step failure.

export interface PostHogEventsEnv {
  POSTHOG_PROJECT_API_KEY?: string;
  // Region host for the CAPTURE endpoint, e.g. "https://eu.i.posthog.com"
  // for EU-region PostHog accounts. Defaults to the US endpoint.
  // Deliberately a separate var from posthog.ts's POSTHOG_MCP_URL: same
  // region concept, but a different API surface (ingestion vs. MCP
  // proxy) with its own URL shape -- one override should not silently
  // move the other.
  POSTHOG_INGEST_HOST?: string;
}

const DEFAULT_INGEST_HOST = "https://us.i.posthog.com";

function requireProjectApiKey(env: PostHogEventsEnv): string {
  if (!env.POSTHOG_PROJECT_API_KEY) {
    throw new Error(
      "POSTHOG_PROJECT_API_KEY is not configured on this Worker. Create a PROJECT API key (Project " +
        "Settings -> Project API Key in PostHog -- NOT the Personal API key POSTHOG_API_KEY already uses " +
        "for the posthog_* MCP-proxy tools) then run: wrangler secret put POSTHOG_PROJECT_API_KEY",
    );
  }
  return env.POSTHOG_PROJECT_API_KEY;
}

function ingestUrl(env: PostHogEventsEnv): string {
  const host = (env.POSTHOG_INGEST_HOST || DEFAULT_INGEST_HOST).replace(/\/$/, "");
  return `${host}/i/v0/e/`;
}

async function captureEvent(
  env: PostHogEventsEnv,
  event: string,
  distinctId: string,
  properties: Record<string, unknown>,
): Promise<string> {
  try {
    const apiKey = requireProjectApiKey(env);
    const resp = await fetch(ingestUrl(env), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        api_key: apiKey,
        event,
        distinct_id: distinctId,
        properties,
        timestamp: new Date().toISOString(),
      }),
    });
    if (!resp.ok) {
      const text = await resp.text().catch(() => "");
      return `Error posting to PostHog capture endpoint: ${resp.status} ${text.slice(0, 300)}`;
    }
    return "ok";
  } catch (e) {
    return `Error posting to PostHog capture endpoint: ${e}`;
  }
}

export async function postHogCaptureException(
  env: PostHogEventsEnv,
  args: { cellId: number; tag: string; status: string; message: string; provider?: string },
): Promise<string> {
  const message = args.message.slice(0, 2000);
  return captureEvent(env, "$exception", `codecell-${args.cellId}`, {
    $exception_list: [
      {
        type: "CodeCellFailure",
        value: message,
        mechanism: { synthetic: true, handled: true },
      },
    ],
    $exception_type: "CodeCellFailure",
    $exception_message: message,
    cell_id: args.cellId,
    tag: args.tag,
    status: args.status,
    provider: args.provider ?? null,
    source: "code_cell_workflow",
  });
}

export async function postHogCaptureCodeCellResolution(
  env: PostHogEventsEnv,
  args: { cellId: number; tag: string; escalated: boolean; provider?: string },
): Promise<string> {
  // Section 10a: an approximation of "skipped-cycle rate," at CodeCell
  // granularity rather than Section 3b's actual "cycle" (a batch of N
  // cells a Reviewer/Architect session reviews together) -- that
  // batching concept doesn't exist as implemented infrastructure
  // anywhere in this codebase yet (the pipeline is CodeCell-centric,
  // not cycle-centric). `escalated` uses "resolved via automatic
  // tagging alone (passed / known_flake_pattern) vs. needing a human
  // (needs_human / dead_letter)" as a rough, honest proxy for "no
  // Context Slot involvement was needed" -- NOT a literal
  // implementation of Section 3b's cycle concept. See this cell's
  // summary for the same caveat spelled out for a human reader, and
  // code_cell_workflow.ts's tag-result step for where this is called.
  return captureEvent(env, "codecell_resolution", `codecell-${args.cellId}`, {
    cell_id: args.cellId,
    tag: args.tag,
    escalated: args.escalated,
    provider: args.provider ?? null,
    source: "code_cell_workflow",
  });
}
