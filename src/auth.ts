// auth.ts
//
// The "defaultHandler" for @cloudflare/workers-oauth-provider: handles
// every request that isn't an already-authenticated call to /mcp. In
// practice that's just GET/POST /authorize -- the human-in-the-loop
// consent step of the OAuth dance -- plus a couple of machine-to-machine
// webhook routes that resolve CodeCellWorkflow's durable waits, and (as
// of Section 7) two more that back Neo4j's embedding-backfill and
// keepalive QStash schedules.
//
// Since this server has exactly one user (you), "consent" is just:
// prove you know MCP_AUTH_TOKEN. No accounts, no database of users --
// the library's OAuthProvider handles all client registration (DCR),
// code exchange, and access-token issuance/verification on its own,
// backed by the OAUTH_KV namespace.

import type { Env } from "./index";
import { generateCode, type FastWorkerEventPayload } from "./code_cell_workflow";
import { backfillPendingEmbeddings, touchHeartbeat } from "./graph";

function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]!));
}

function page(body: string): Response {
  return new Response(
    `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>turso-github-mcp</title>
    <style>
      body{font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;max-width:420px;margin:80px auto;padding:0 20px;color:#1a1a1a}
      h1{font-size:18px}
      input[type=password]{width:100%;padding:10px;font-size:15px;box-sizing:border-box;border:1px solid #ccc;border-radius:6px;margin:12px 0}
      button{width:100%;padding:10px;font-size:15px;background:#1a1a1a;color:#fff;border:none;border-radius:6px;cursor:pointer}
      .err{color:#b00020;font-size:14px}
      .meta{color:#666;font-size:13px}
    </style></head><body>${body}</body></html>`,
    { headers: { "Content-Type": "text/html; charset=utf-8" } },
  );
}

// Section 7's two cron routes share one machine-to-machine secret
// (GRAPH_CRON_TOKEN, deliberately separate from every other callback
// token in this file -- same reasoning as FAST_WORKER_CALLBACK_TOKEN vs
// HEAVY_WORKER_CALLBACK_TOKEN: nothing external needs to know this value,
// it's only ever set here and forwarded by QStash's own schedule).
function checkGraphCronAuth(request: Request, env: Env): Response | null {
  if (!env.GRAPH_CRON_TOKEN) {
    return new Response("Server misconfigured: GRAPH_CRON_TOKEN not set.", { status: 500 });
  }
  const auth = request.headers.get("Authorization") || "";
  if (auth !== `Bearer ${env.GRAPH_CRON_TOKEN}`) {
    return new Response("Unauthorized", { status: 401 });
  }
  return null;
}

export const AuthHandler = {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("turso-github-mcp: ok\n", { status: 200 });
    }

    // Section 4f's QStash pacing layer. CodeCellWorkflow's
    // fast-worker-dispatch step (code_cell_workflow.ts) publishes here
    // via QStash instead of calling generateCode() in-process, tagged
    // with a shared Upstash-Flow-Control-Key -- QStash releases the
    // request to this route only once it's within the configured
    // rate/parallelism for that key, which is enforced across EVERY
    // concurrently-running CodeCellWorkflow instance, not just one (the
    // cross-instance pacing gap Section 4f says Workflows has no
    // primitive for). This route runs the actual Groq -> Gemini cascade
    // and resolves the waiting step via sendEvent -- same shape as
    // /webhook/heavy-worker-result below, just self-triggered by our own
    // paced publish rather than an external GitHub Actions runner.
    //
    // Auth: a shared secret (FAST_WORKER_CALLBACK_TOKEN, separate from
    // both MCP_AUTH_TOKEN and HEAVY_WORKER_CALLBACK_TOKEN) forwarded by
    // QStash via the Upstash-Forward-Authorization header on the publish
    // call -- QStash strips the "Upstash-Forward-" prefix and delivers
    // the rest verbatim, so this route sees a normal Authorization
    // header, same check as the heavy-worker route below.
    if (url.pathname === "/qstash/fast-worker-generate" && request.method === "POST") {
      if (!env.FAST_WORKER_CALLBACK_TOKEN) {
        return new Response("Server misconfigured: FAST_WORKER_CALLBACK_TOKEN not set.", { status: 500 });
      }
      const auth = request.headers.get("Authorization") || "";
      if (auth !== `Bearer ${env.FAST_WORKER_CALLBACK_TOKEN}`) {
        return new Response("Unauthorized", { status: 401 });
      }
      let body: { cell_id?: number; spec?: string; workflow_instance_id?: string };
      try {
        body = await request.json();
      } catch {
        return new Response("Malformed JSON body.", { status: 400 });
      }
      if (!body.workflow_instance_id || !body.spec) {
        return new Response("workflow_instance_id and spec required.", { status: 400 });
      }
      let instance;
      try {
        instance = await env.CODE_CELL_WORKFLOW.get(body.workflow_instance_id);
      } catch (e) {
        return new Response(`Error looking up workflow instance: ${e}`, { status: 500 });
      }
      try {
        const draft = await generateCode(env, body.spec);
        const payload: FastWorkerEventPayload = draft;
        await instance.sendEvent({ type: "fast-worker-result", payload });
      } catch (e) {
        // All Fast Worker tiers exhausted inside generateCode() itself
        // (Groq -> every Gemini/Gemma fallback tier). This is a
        // legitimate terminal failure, not a transient delivery
        // problem, so report it to the waiting step via sendEvent
        // rather than throwing and letting QStash redeliver the same
        // already-exhausted cascade.
        const payload: FastWorkerEventPayload = { error: String((e as any)?.message ?? e) };
        try {
          await instance.sendEvent({ type: "fast-worker-result", payload });
        } catch (sendErr) {
          return new Response(`Error resolving workflow instance after cascade failure: ${sendErr}`, { status: 500 });
        }
      }
      return new Response("ok\n", { status: 200 });
    }

    // Section 4 step 6 callback: test.yml's last step (scripts/run_heavy_worker.py)
    // posts the actual pass/fail + log here once the Ubuntu runner finishes.
    // This resolves CodeCellWorkflow's step.waitForEvent("heavy-worker-result")
    // (code_cell_workflow.ts). Separate machine-to-machine secret rather than
    // MCP_AUTH_TOKEN, which gates the human /authorize consent screen and
    // shouldn't be handed to a CI runner.
    if (url.pathname === "/webhook/heavy-worker-result" && request.method === "POST") {
      if (!env.HEAVY_WORKER_CALLBACK_TOKEN) {
        return new Response("Server misconfigured: HEAVY_WORKER_CALLBACK_TOKEN not set.", { status: 500 });
      }
      const auth = request.headers.get("Authorization") || "";
      if (auth !== `Bearer ${env.HEAVY_WORKER_CALLBACK_TOKEN}`) {
        return new Response("Unauthorized", { status: 401 });
      }
      let body: { workflow_instance_id?: string; passed?: boolean; log?: string };
      try {
        body = await request.json();
      } catch {
        return new Response("Malformed JSON body.", { status: 400 });
      }
      if (!body.workflow_instance_id) {
        return new Response("workflow_instance_id required.", { status: 400 });
      }
      try {
        const instance = await env.CODE_CELL_WORKFLOW.get(body.workflow_instance_id);
        await instance.sendEvent({
          type: "heavy-worker-result",
          payload: { passed: !!body.passed, log: body.log || "" },
        });
        return new Response("ok\n", { status: 200 });
      } catch (e) {
        return new Response(`Error resolving workflow instance: ${e}`, { status: 500 });
      }
    }

    // Section 7d: scans for embedding_pending Neo4j nodes and retries the
    // Gemini Embedding 1<->2 pair, clearing the flag on success. Wired to
    // a low-frequency (every 15-30 min) QStash schedule -- see the
    // deploy summary for the actual cron this project's instance uses.
    if (url.pathname === "/webhook/graph-embedding-backfill" && request.method === "POST") {
      const authError = checkGraphCronAuth(request, env);
      if (authError) return authError;
      try {
        const result = await backfillPendingEmbeddings(env);
        return new Response(JSON.stringify(result), { status: 200, headers: { "Content-Type": "application/json" } });
      } catch (e) {
        return new Response(`Error backfilling embeddings: ${e}`, { status: 500 });
      }
    }

    // Section 7f: touches a dedicated _heartbeat node so a quiet month of
    // real project work doesn't let the free AuraDB instance cross its
    // 30-day inactivity window and get deleted outright. Wired to a
    // weekly QStash schedule -- comfortably inside that 30-day margin.
    if (url.pathname === "/webhook/graph-heartbeat" && request.method === "POST") {
      const authError = checkGraphCronAuth(request, env);
      if (authError) return authError;
      try {
        const result = await touchHeartbeat(env);
        return new Response(JSON.stringify(result), { status: 200, headers: { "Content-Type": "application/json" } });
      } catch (e) {
        return new Response(`Error touching heartbeat: ${e}`, { status: 500 });
      }
    }

    if (url.pathname === "/authorize" && request.method === "GET") {
      if (!env.MCP_AUTH_TOKEN) {
        return new Response(
          "Server misconfigured: MCP_AUTH_TOKEN secret is not set. Run: wrangler secret put MCP_AUTH_TOKEN",
          { status: 500 },
        );
      }
      const oauthReqInfo = await env.OAUTH_PROVIDER.parseAuthRequest(request);
      const clientInfo = await env.OAUTH_PROVIDER.lookupClient(oauthReqInfo.clientId);
      const clientName = clientInfo?.clientName || oauthReqInfo.clientId;

      return page(`
        <h1>Authorize ${escapeHtml(clientName)}</h1>
        <p class="meta">This grants access to your github_*/turso_*/neo4j_*/graph_*/cloudflare_*/workflow_*/runner_* MCP tools.</p>
        <form method="POST" action="/authorize">
          <input type="hidden" name="oauth_req" value='${escapeHtml(JSON.stringify(oauthReqInfo))}'>
          <input type="password" name="password" placeholder="Server password" autofocus required>
          <button type="submit">Approve</button>
        </form>
      `);
    }

    if (url.pathname === "/authorize" && request.method === "POST") {
      const form = await request.formData();
      const password = String(form.get("password") || "");
      const oauthReqJson = String(form.get("oauth_req") || "");

      let oauthReqInfo;
      try {
        oauthReqInfo = JSON.parse(oauthReqJson);
      } catch {
        return page(`<h1>Bad request</h1><p class="err">Malformed authorization request.</p>`);
      }

      if (!env.MCP_AUTH_TOKEN || password !== env.MCP_AUTH_TOKEN) {
        return page(`
          <h1>Authorize</h1>
          <p class="err">Incorrect password.</p>
          <form method="POST" action="/authorize">
            <input type="hidden" name="oauth_req" value='${escapeHtml(oauthReqJson)}'>
            <input type="password" name="password" placeholder="Server password" autofocus required>
            <button type="submit">Approve</button>
          </form>
        `);
      }

      const { redirectTo } = await env.OAUTH_PROVIDER.completeAuthorization({
        request: oauthReqInfo,
        userId: "owner",
        metadata: { label: "turso-github-mcp" },
        scope: oauthReqInfo.scope,
        props: {},
      });

      return Response.redirect(redirectTo, 302);
    }

    return new Response("Not found", { status: 404 });
  },
};
