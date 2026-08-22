// groq.ts
//
// Direct port onto Groq's OpenAI-compatible inference API
// (https://api.groq.com/openai/v1). Same shape as turso.ts/upstash.ts:
// stateless, one fetch() per call. Groq has no hosted remote MCP server
// and no account-management API the way Upstash/Cloudflare do -- it's
// purely an LLM inference endpoint (chat completions, transcription,
// model listing), so there's nothing to proxy; this just wraps the REST
// calls directly.
//
// Auth: single bearer token, GROQ_API_KEY (Worker secret, from
// https://console.groq.com/keys). Unlike Upstash's key, this one is
// naturally scoped -- it can only run inference, there's no "account
// management" surface to worry about giving it access to.
//
// Scope of this module: chat completions and model listing -- the two
// stable, universally-used endpoints. Audio transcription/translation
// (/audio/transcriptions, /audio/translations) take multipart file
// uploads rather than JSON and aren't included here; add a dedicated
// tool for those later if needed, since they need a different calling
// convention (the MCP tool would need to accept a file/base64 blob).

export interface GroqEnv {
  GROQ_API_KEY?: string;
}

const GROQ_API = "https://api.groq.com/openai/v1";

function requireGroqKey(env: GroqEnv): string {
  if (!env.GROQ_API_KEY) {
    throw new Error(
      "GROQ_API_KEY is not configured on this Worker. Get a key at https://console.groq.com/keys, then run: " +
        "wrangler secret put GROQ_API_KEY",
    );
  }
  return env.GROQ_API_KEY;
}

async function groqFetch(
  env: GroqEnv,
  method: "GET" | "POST",
  path: string,
  body?: unknown,
): Promise<string> {
  const key = requireGroqKey(env);
  const resp = await fetch(`${GROQ_API}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${key}`,
      "Content-Type": "application/json",
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Groq API ${method} ${path} returned ${resp.status}: ${text.slice(0, 500)}`);
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

export interface GroqChatMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export async function groqChatCompletion(
  env: GroqEnv,
  model: string,
  messages: GroqChatMessage[],
  opts?: { temperature?: number; maxTokens?: number },
): Promise<string> {
  try {
    const body: Record<string, unknown> = { model, messages };
    if (opts?.temperature !== undefined) body.temperature = opts.temperature;
    if (opts?.maxTokens !== undefined) body.max_tokens = opts.maxTokens;

    const raw = await groqFetch(env, "POST", "/chat/completions", body);
    const data = JSON.parse(raw);
    const content = data?.choices?.[0]?.message?.content;
    if (typeof content === "string") return content;
    return prettyJson(raw); // fall back to full response if shape is unexpected
  } catch (e) {
    return `Error calling Groq chat completion: ${e}`;
  }
}

export async function groqListModels(env: GroqEnv): Promise<string> {
  try {
    const raw = await groqFetch(env, "GET", "/models");
    const data = JSON.parse(raw);
    const models = Array.isArray(data?.data) ? data.data : [];
    if (models.length === 0) return prettyJson(raw);
    return models
      .map((m: any) => `${m.id}  (owned_by=${m.owned_by}, context_window=${m.context_window ?? "?"})`)
      .join("\n");
  } catch (e) {
    return `Error listing Groq models: ${e}`;
  }
}
