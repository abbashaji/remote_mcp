// auth.ts
//
// The "defaultHandler" for @cloudflare/workers-oauth-provider: handles
// every request that isn't an already-authenticated call to /mcp. In
// practice that's just GET/POST /authorize -- the human-in-the-loop
// consent step of the OAuth dance.
//
// Since this server has exactly one user (you), "consent" is just:
// prove you know MCP_AUTH_TOKEN. No accounts, no database of users --
// the library's OAuthProvider handles all client registration (DCR),
// code exchange, and access-token issuance/verification on its own,
// backed by the OAUTH_KV namespace.

import type { Env } from "./index";

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

export const AuthHandler = {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("turso-github-mcp: ok\n", { status: 200 });
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
        <p class="meta">This grants access to your github_*/turso_*/cloudflare_*/workflow_*/runner_* MCP tools.</p>
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
