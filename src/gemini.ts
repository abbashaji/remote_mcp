// gemini.ts
//
// Direct port onto the Gemini API's generateContent/embedContent REST
// endpoints (https://generativelanguage.googleapis.com/v1beta). Same
// shape as groq.ts: stateless, one fetch() per call, single bearer-style
// key (sent as x-goog-api-key rather than Authorization).
//
// Scope: this ONE module covers several rows of the stack doc's
// component matrix, because they're all the same underlying API with a
// different `model` string or `tools` flag:
//   - Fast Worker fallback tiers (Gemini Flash/Flash-Lite) -- Section 4b
//   - Gemma 4 31B / Gemma 4 26B (tagging + fallback floor) -- Sections 4c/4b
//     (Gemma models are served from the same generateContent endpoint,
//     just under `models/gemma-...` instead of `models/gemini-...`)
//   - response_schema discipline -- Section 4e (responseSchema param)
//   - google_search grounding -- Section 11 (groundingWithGoogleSearch flag)
//   - Gemini Embedding 1 & 2 -- Section 7a (geminiEmbedContent)
//
// Auth: single key, GEMINI_API_KEY (Worker secret, from
// https://aistudio.google.com/apikey), sent as the x-goog-api-key header.

export interface GeminiEnv {
  GEMINI_API_KEY?: string;
}

const GEMINI_API = "https://generativelanguage.googleapis.com/v1beta";

function requireGeminiKey(env: GeminiEnv): string {
  if (!env.GEMINI_API_KEY) {
    throw new Error(
      "GEMINI_API_KEY is not configured on this Worker. Get a key at https://aistudio.google.com/apikey, then run: " +
        "wrangler secret put GEMINI_API_KEY",
    );
  }
  return env.GEMINI_API_KEY;
}

async function geminiFetch(env: GeminiEnv, path: string, body: unknown): Promise<string> {
  const key = requireGeminiKey(env);
  const resp = await fetch(`${GEMINI_API}${path}`, {
    method: "POST",
    headers: {
      "x-goog-api-key": key,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });
  const text = await resp.text();
  if (!resp.ok) {
    throw new Error(`Gemini API POST ${path} returned ${resp.status}: ${text.slice(0, 500)}`);
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

export interface GeminiMessage {
  role: "user" | "model";
  content: string;
}

export async function geminiGenerateContent(
  env: GeminiEnv,
  model: string,
  messages: GeminiMessage[],
  opts?: {
    systemInstruction?: string;
    temperature?: number;
    maxOutputTokens?: number;
    responseMimeType?: string; // e.g. "application/json" -- Section 4e discipline
    responseSchema?: Record<string, unknown>; // paired with responseMimeType=application/json
    groundingWithGoogleSearch?: boolean; // Section 11
  },
): Promise<string> {
  try {
    const body: Record<string, unknown> = {
      contents: messages.map((m) => ({ role: m.role, parts: [{ text: m.content }] })),
    };
    if (opts?.systemInstruction) {
      body.systemInstruction = { parts: [{ text: opts.systemInstruction }] };
    }
    const generationConfig: Record<string, unknown> = {};
    if (opts?.temperature !== undefined) generationConfig.temperature = opts.temperature;
    if (opts?.maxOutputTokens !== undefined) generationConfig.maxOutputTokens = opts.maxOutputTokens;
    if (opts?.responseMimeType) generationConfig.responseMimeType = opts.responseMimeType;
    if (opts?.responseSchema) generationConfig.responseSchema = opts.responseSchema;
    if (Object.keys(generationConfig).length > 0) body.generationConfig = generationConfig;
    if (opts?.groundingWithGoogleSearch) {
      body.tools = [{ google_search: {} }];
    }

    const modelPath = model.startsWith("models/") ? model : `models/${model}`;
    const raw = await geminiFetch(env, `/${modelPath}:generateContent`, body);
    const data = JSON.parse(raw);
    const parts = data?.candidates?.[0]?.content?.parts;
    const textOut = Array.isArray(parts) ? parts.map((p: any) => p?.text ?? "").join("") : undefined;
    if (typeof textOut === "string" && textOut.length > 0) {
      const grounding = data?.candidates?.[0]?.groundingMetadata;
      if (grounding) {
        return `${textOut}\n\n[grounding: ${JSON.stringify(grounding.webSearchQueries ?? [])}]`;
      }
      return textOut;
    }
    return prettyJson(raw); // fall back to full response if shape is unexpected (e.g. blocked, function call)
  } catch (e) {
    return `Error calling Gemini generateContent: ${e}`;
  }
}

export async function geminiEmbedContent(
  env: GeminiEnv,
  model: string,
  text: string,
  taskType?: string, // e.g. "RETRIEVAL_DOCUMENT", "RETRIEVAL_QUERY", "SEMANTIC_SIMILARITY"
): Promise<string> {
  try {
    const modelPath = model.startsWith("models/") ? model : `models/${model}`;
    const body: Record<string, unknown> = {
      model: modelPath,
      content: { parts: [{ text }] },
    };
    if (taskType) body.taskType = taskType;
    const raw = await geminiFetch(env, `/${modelPath}:embedContent`, body);
    const data = JSON.parse(raw);
    const values = data?.embedding?.values;
    if (Array.isArray(values)) {
      return JSON.stringify(values);
    }
    return prettyJson(raw);
  } catch (e) {
    return `Error calling Gemini embedContent: ${e}`;
  }
}
