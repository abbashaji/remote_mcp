// posthog.ts
//
// Proxies PostHog's own hosted remote MCP server
// (https://mcp.posthog.com/mcp, or https://mcp-eu.posthog.com/mcp for
// EU-region accounts) through this Worker. Unlike Cloudflare's "code
// mode" server (2 fixed tools -- search/execute -- covering their whole
// API), PostHog's MCP exposes a real catalog of many named tools
// (list_projects, create_annotation, query_insight, feature-flag/
// experiment management, HogQL queries, error tracking, etc.) that can
// change over time. So rather than hardcoding each one as a separate
// MCP tool here (and having to keep that list in sync with PostHog's),
// this proxies generically: list_tools() to discover what's currently
// available, call_tool(name, arguments) to invoke any of them.
//
// Auth: PostHog Personal API key as a bearer token (same convention as
// PostHog's regular REST API: "Authorization: Bearer <key>"). Create one
// at PostHog -> Settings -> Personal API keys, scoped to whatever this
// connector should be allowed to touch (PostHog's personal API keys
// support fine-grained scopes, unlike Upstash's). Stored as the
// POSTHOG_API_KEY Worker secret -- never anything the MCP client
// (Claude) sees directly.
//
// This hand-rolls the same minimal Streamable HTTP JSON-RPC client as
// cloudflare.ts (initialize -> notifications/initialized -> tools/call)
// rather than pulling in the full MCP SDK client. Session id cached
// per-isolate, re-negotiated once on failure -- see cloudflare.ts for
// the rationale (identical shape, just generalized to any tool name
// instead of hardcoded "search"/"execute").

export interface PostHogEnv {
  POSTHOG_API_KEY?: string;
  // Override if your PostHog account lives in the EU region:
  // "https://mcp-eu.posthog.com/mcp". Defaults to the US endpoint.
  POSTHOG_MCP_URL?: string;
}

const DEFAULT_POSTHOG_MCP_URL = "https://mcp.posthog.com/mcp";
const PROTOCOL_VERSION = "2025-06-18";

let cachedSessionId: string | null = null;
let cachedTokenFingerprint: string | null = null;

function requirePostHogKey(env: PostHogEnv): string {
  if (!env.POSTHOG_API_KEY) {
    throw new Error(
      "POSTHOG_API_KEY is not configured on this Worker. Create a Personal API key at " +
        "PostHog -> Settings -> Personal API keys (scope it to whatever this connector should touch), " +
        "then run: wrangler secret put POSTHOG_API_KEY",
    );
  }
  return env.POSTHOG_API_KEY;
}

function mcpUrl(env: PostHogEnv): string {
  return env.POSTHOG_MCP_URL || DEFAULT_POSTHOG_MCP_URL;
}

async function parseMcpResponse(resp: Response): Promise<any> {
  const ctype = resp.headers.get("content-type") || "";
  const bodyText = await resp.text();
  if (!bodyText) return null;

  if (ctype.includes("application/json")) {
    return JSON.parse(bodyText);
  }

  // text/event-stream: pull out "data: {...}" lines and return the last
  // one that looks like a JSON-RPC response (has an "id").
  const messages: any[] = [];
  for (const rawLine of bodyText.split("\n")) {
    const line = rawLine.trim();
    if (!line.startsWith("data:")) continue;
    const payload = line.slice(5).trim();
    if (!payload) continue;
    try {
      messages.push(JSON.parse(payload));
    } catch {
      // ignore malformed SSE chunks
    }
  }
  const withId = messages.filter((m) => m && Object.prototype.hasOwnProperty.call(m, "id"));
  return withId.length ? withId[withId.length - 1] : (messages[messages.length - 1] ?? null);
}

async function phFetch(
  env: PostHogEnv,
  token: string,
  body: unknown,
  sessionId?: string | null,
): Promise<{ data: any; sessionId: string | null; status: number }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Accept: "application/json, text/event-stream",
    Authorization: `Bearer ${token}`,
    "MCP-Protocol-Version": PROTOCOL_VERSION,
  };
  if (sessionId) headers["Mcp-Session-Id"] = sessionId;

  const resp = await fetch(mcpUrl(env), { method: "POST", headers, body: JSON.stringify(body) });
  const newSessionId = resp.headers.get("mcp-session-id");

  if (resp.status === 202) {
    return { data: null, sessionId: newSessionId ?? sessionId ?? null, status: resp.status };
  }
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`PostHog MCP server returned ${resp.status}: ${text.slice(0, 500)}`);
  }
  const data = await parseMcpResponse(resp);
  return { data, sessionId: newSessionId ?? sessionId ?? null, status: resp.status };
}

async function negotiateSession(env: PostHogEnv, token: string): Promise<string | null> {
  const init = await phFetch(env, token, {
    jsonrpc: "2.0",
    id: "init-1",
    method: "initialize",
    params: {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: { name: "turso-github-mcp-posthog-proxy", version: "1.0.0" },
    },
  });
  const sessionId = init.sessionId;
  await phFetch(env, token, { jsonrpc: "2.0", method: "notifications/initialized" }, sessionId).catch(() => {});
  cachedSessionId = sessionId;
  cachedTokenFingerprint = token;
  return sessionId;
}

async function ensureSession(env: PostHogEnv, token: string): Promise<string | null> {
  if (cachedSessionId && cachedTokenFingerprint === token) return cachedSessionId;
  return negotiateSession(env, token);
}

async function rpc(env: PostHogEnv, token: string, method: string, params?: unknown): Promise<any> {
  const attempt = async (sessionId: string | null) => {
    const { data } = await phFetch(
      env,
      token,
      { jsonrpc: "2.0", id: `call-${Date.now()}`, method, params },
      sessionId,
    );
    return data;
  };

  let sessionId = await ensureSession(env, token);
  let data: any;
  try {
    data = await attempt(sessionId);
  } catch (e) {
    cachedSessionId = null;
    sessionId = await ensureSession(env, token);
    data = await attempt(sessionId);
  }

  if (data?.error) {
    throw new Error(`PostHog MCP error ${data.error.code}: ${data.error.message}`);
  }
  return data?.result;
}

export async function posthogListTools(env: PostHogEnv): Promise<string> {
  try {
    const token = requirePostHogKey(env);
    const result = await rpc(env, token, "tools/list");
    const tools = Array.isArray(result?.tools) ? result.tools : [];
    if (tools.length === 0) return "No tools returned.";
    return tools
      .map((t: any) => `${t.name}${t.description ? " -- " + t.description : ""}`)
      .join("\n");
  } catch (e) {
    return `Error listing PostHog MCP tools: ${e}`;
  }
}

export async function posthogCallTool(
  env: PostHogEnv,
  toolName: string,
  toolArguments: Record<string, unknown>,
): Promise<string> {
  try {
    const token = requirePostHogKey(env);
    const result = await rpc(env, token, "tools/call", { name: toolName, arguments: toolArguments });
    const content = Array.isArray(result?.content) ? result.content : [];
    const rendered = content
      .map((c: any) => (c?.type === "text" ? c.text : `[${c?.type ?? "unknown"} content]`))
      .join("\n");
    if (result?.isError) {
      throw new Error(rendered || "PostHog MCP tool returned an error with no message.");
    }
    return rendered || JSON.stringify(result ?? {}, null, 2);
  } catch (e) {
    return `Error calling PostHog MCP tool '${toolName}': ${e}`;
  }
}
