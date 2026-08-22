// cloudflare.ts
//
// Proxies two tools -- search() and execute() -- from Cloudflare's own
// remote "Code Mode" MCP server (https://mcp.cloudflare.com/mcp) through
// this Worker. That server exposes Cloudflare's *entire* REST API (2,500+
// endpoints -- Workers scripts, Workflows, Durable Objects, DNS, KV, R2,
// everything) via just those two tools, so proxying them is all that's
// needed to fold "the whole Cloudflare account" into this one connector.
//
// Auth: Cloudflare's docs note that for non-browser (automation) clients
// you can skip the interactive OAuth flow and just pass a Cloudflare API
// token as a bearer token. That's what this does -- CLOUDFLARE_API_TOKEN
// is a Worker secret, never anything the MCP client (Claude) sees.
//
// This hand-rolls a minimal Streamable HTTP JSON-RPC client (initialize
// -> notifications/initialized -> tools/call) rather than pulling in the
// full @modelcontextprotocol/sdk client, since only these two calls are
// needed. The session id is cached per-isolate (mirrors turso.ts's
// cachedClient) and re-negotiated once on failure.
//
// IMPORTANT -- calling convention for the `code` argument these two
// tools forward to Cloudflare: it must be a single bare, UNINVOKED
// async function literal, e.g. "async () => { return await
// cloudflare.request({ method: 'GET', path: '/accounts' }); }". The
// remote server itself calls the function -- it does NOT eval
// top-level statements, and it does NOT expect you to self-invoke (no
// trailing "()"). `cloudflare` (execute) and `spec` (search) are
// ambient globals available inside the function body, not parameters
// passed to it. Confirmed by trial: top-level statements -> "Unexpected
// token 'const'"; a bare expression -> "<value> is not a function"
// (the server does `(code)(...)`, calling whatever your code evaluates
// to); a self-invoking IIFE -> "(intermediate value)(...) is not a
// function" (it calls the IIFE's already-resolved result again).
//
// Also note: `cloudflare.request()` takes a SINGLE options object --
// { method, path, query?, body?, contentType?, rawBody? } -- not
// (path, options). Passing a bare path string throws "Cannot read
// properties of undefined (reading 'split')" since it destructures
// `path` off of what you passed as `options`.

export interface CloudflareEnv {
  CLOUDFLARE_API_TOKEN?: string;
}

const CF_MCP_URL = "https://mcp.cloudflare.com/mcp";
const PROTOCOL_VERSION = "2025-06-18";

let cachedSessionId: string | null = null;
let cachedTokenFingerprint: string | null = null;

function requireCfToken(env: CloudflareEnv): string {
  if (!env.CLOUDFLARE_API_TOKEN) {
    throw new Error(
      "CLOUDFLARE_API_TOKEN is not configured on this Worker. Create a scoped API token at " +
        "https://dash.cloudflare.com/profile/api-tokens (e.g. Workers Scripts:Edit, Workflows:Edit, " +
        "Account Settings:Read -- whatever this connector should be allowed to touch), then run: " +
        "wrangler secret put CLOUDFLARE_API_TOKEN",
    );
  }
  return env.CLOUDFLARE_API_TOKEN;
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

async function cfFetch(
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

  const resp = await fetch(CF_MCP_URL, { method: "POST", headers, body: JSON.stringify(body) });
  const newSessionId = resp.headers.get("mcp-session-id");

  if (resp.status === 202) {
    // Accepted, no body (e.g. response to a notification).
    return { data: null, sessionId: newSessionId ?? sessionId ?? null, status: resp.status };
  }
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`Cloudflare MCP server returned ${resp.status}: ${text.slice(0, 500)}`);
  }
  const data = await parseMcpResponse(resp);
  return { data, sessionId: newSessionId ?? sessionId ?? null, status: resp.status };
}

async function negotiateSession(token: string): Promise<string | null> {
  const init = await cfFetch(token, {
    jsonrpc: "2.0",
    id: "init-1",
    method: "initialize",
    params: {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: {},
      clientInfo: { name: "turso-github-mcp-cf-proxy", version: "1.0.0" },
    },
  });
  const sessionId = init.sessionId;
  // Best-effort: some servers require the initialized notification before
  // accepting tools/call; ignore failures since it's a notification (no
  // response expected either way).
  await cfFetch(token, { jsonrpc: "2.0", method: "notifications/initialized" }, sessionId).catch(() => {});
  cachedSessionId = sessionId;
  cachedTokenFingerprint = token;
  return sessionId;
}

async function ensureSession(token: string): Promise<string | null> {
  if (cachedSessionId && cachedTokenFingerprint === token) return cachedSessionId;
  return negotiateSession(token);
}

async function callCfTool(env: CloudflareEnv, toolName: "search" | "execute", code: string): Promise<string> {
  const token = requireCfToken(env);

  const attempt = async (sessionId: string | null) => {
    const { data } = await cfFetch(
      token,
      {
        jsonrpc: "2.0",
        id: `call-${Date.now()}`,
        method: "tools/call",
        params: { name: toolName, arguments: { code } },
      },
      sessionId,
    );
    return data;
  };

  let sessionId = await ensureSession(token);
  let data: any;
  try {
    data = await attempt(sessionId);
  } catch (e) {
    // Session may have expired between calls -- re-handshake once and retry.
    cachedSessionId = null;
    sessionId = await ensureSession(token);
    data = await attempt(sessionId);
  }

  if (data?.error) {
    throw new Error(`Cloudflare MCP error ${data.error.code}: ${data.error.message}`);
  }

  const result = data?.result;
  const content = Array.isArray(result?.content) ? result.content : [];
  const rendered = content
    .map((c: any) => (c?.type === "text" ? c.text : `[${c?.type ?? "unknown"} content]`))
    .join("\n");

  if (result?.isError) {
    throw new Error(rendered || "Cloudflare MCP tool returned an error with no message.");
  }

  return rendered || JSON.stringify(result ?? data ?? {}, null, 2);
}

export async function cloudflareSearch(env: CloudflareEnv, code: string): Promise<string> {
  try {
    return await callCfTool(env, "search", code);
  } catch (e) {
    return `Error calling Cloudflare search: ${e}`;
  }
}

export async function cloudflareExecute(env: CloudflareEnv, code: string): Promise<string> {
  try {
    return await callCfTool(env, "execute", code);
  } catch (e) {
    return `Error calling Cloudflare execute: ${e}`;
  }
}
