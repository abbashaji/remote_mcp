// antigravity.ts
//
// Direct port onto the Gemini API's Interactions API, specifically the
// managed "antigravity-preview-05-2026" agent
// (https://ai.google.dev/gemini-api/docs/antigravity-agent). This is
// what Section 4a's "autonomous first-pass fix" and Section 4c's
// "triage controller" actually call: a single POST provisions a Linux
// sandbox, and the agent reads/writes files, runs code, and returns a
// result -- exactly the "reads the traceback and the cell's current
// code, attempts a single bounded fix" shape the stack doc describes.
//
// IMPORTANT: as of this writing the Antigravity agent and the
// Interactions API are explicitly in PREVIEW -- schemas may change.
// Re-check https://ai.google.dev/gemini-api/docs/antigravity-agent
// before depending on this in production.
//
// Auth: same GEMINI_API_KEY as gemini.ts (x-goog-api-key header) -- one
// credential, no separate OAuth/SDK setup needed for the REST surface.
//
// Scope of this module: starting an interaction (optionally
// backgrounded, for long-running fix attempts) and polling/fetching one
// by id. Triggers (cron-scheduled agent runs) and MCP-server-as-tool
// registration exist on this same API but aren't included here -- this
// doc's fix-attempt use case is a one-shot call per Failed cell, not a
// standing schedule.

export interface AntigravityEnv {
  GEMINI_API_KEY?: string;
}

const GEMINI_API = "https://generativelanguage.googleapis.com/v1beta";
const DEFAULT_AGENT = "antigravity-preview-05-2026";

function requireGeminiKey(env: AntigravityEnv): string {
  if (!env.GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY is not configured on this Worker. Get a key at https://aistudio.google.com/apikey, then run: " +
        "wrangler secret put GEMINI_API_KEY",
    );
  }
  return env.GEMINI_API_KEY;
}

async function interactionsFetch(
  env: AntigravityEnv,
  method: "GET" | "POST",
  path: string,
  body?: unknown,
): Promise<string> {
  const key = requireGeminiKey(env);
  const resp = await fetch(`${GEMINI_API}${path}`, {
    method,
    headers: {
      "x-goog-api-key": key,
      "Content-Type": "application/json",
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Gemini Interactions API ${method} ${path} returned ${resp.status}: ${text.slice(0, 500)}`);
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

// Run a bounded fix-attempt / triage task in a fresh (or reused) remote
// sandbox. Returns the interaction JSON, which includes `status`
// ("completed" | "in_progress" | "requires_action" | "incomplete" |
// "failed"), `id`, `environment_id`, and `output_text` once complete.
export async function antigravityRunInteraction(
  env: AntigravityEnv,
  input: string,
  opts?: {
    agent?: string;
    environment?: string; // "remote" for a fresh sandbox, or an existing environment_id to resume
    previousInteractionId?: string;
    background?: boolean;
    maxTotalTokens?: number; // budget control -- Section 4a says "bounded fix", not open-ended
    model?: string; // e.g. "gemini-3.5-flash-lite" for a cheaper/faster fix-attempt pass
  },
): Promise<string> {
  try {
    const body: Record<string, unknown> = {
      agent: opts?.agent ?? DEFAULT_AGENT,
      input,
      environment: opts?.environment ?? "remote",
    };
    if (opts?.previousInteractionId) body.previous_interaction_id = opts.previousInteractionId;
    if (opts?.background) body.background = true;
    if (opts?.maxTotalTokens !== undefined || opts?.model) {
      body.agent_config = {
        type: "antigravity",
        ...(opts?.model ? { model: opts.model } : {}),
        ...(opts?.maxTotalTokens !== undefined ? { max_total_tokens: opts.maxTotalTokens } : {}),
      };
    }
    const raw = await interactionsFetch(env, "POST", "/interactions", body);
    return prettyJson(raw);
  } catch (e) {
    return `Error running Antigravity interaction: ${e}`;
  }
}

// Poll a backgrounded interaction (or just re-fetch any interaction) by id.
export async function antigravityGetInteraction(env: AntigravityEnv, interactionId: string): Promise<string> {
  try {
    const raw = await interactionsFetch(env, "GET", `/interactions/${encodeURIComponent(interactionId)}`);
    return prettyJson(raw);
  } catch (e) {
    return `Error getting Antigravity interaction: ${e}`;
  }
}
