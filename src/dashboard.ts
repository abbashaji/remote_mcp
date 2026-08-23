// dashboard.ts
//
// Section 12: the operator dashboard's HTTP surface -- served from
// AuthHandler (auth.ts) alongside /authorize, since this whole project
// has exactly one custom-connector slot and everything lives on one
// Worker. Section 12a: gated by the SAME env.MCP_AUTH_TOKEN /authorize
// already checks, not a second auth system -- there's no OAuth flow
// here (a full OAuth dance doesn't make sense for a single HTML page),
// just a direct password check against the one secret this project
// already has.
//
// Three routes (wired into AuthHandler.fetch in auth.ts):
//   GET /dashboard       -- the HTML page (inline <script>, no build
//                           step, no external assets beyond its own
//                           WebSocket connection back to this Worker)
//   GET /dashboard/data  -- one-shot JSON snapshot (computeDashboardPayload
//                           run synchronously per request) -- what
//                           curl/web_fetch-style verification hits
//                           directly without needing a real WebSocket
//                           client, and also this Worker's cheapest
//                           smoke test after a deploy
//   GET /dashboard/ws    -- WebSocket upgrade, proxied straight through
//                           to the DASHBOARD_HUB Durable Object singleton
//                           (Section 12c's poll-once-broadcast-to-N-tabs
//                           pattern -- see dashboard_do.ts)
//
// Auth mechanism (Section 12a: "use your judgment on the exact
// mechanism, but don't invent a new secret"): a `?token=` query param,
// OR an `Authorization: Bearer <token>` header, OR HTTP Basic auth (any
// username, password = the token) -- any one of the three, checked
// against env.MCP_AUTH_TOKEN. The query param is the one that actually
// matters for /dashboard/ws: a browser's native WebSocket() constructor
// cannot set custom request headers, so that's the only mechanism a
// browser client can use for the socket itself. Basic/Bearer exist for
// curl-style verification (`curl -u :$MCP_AUTH_TOKEN .../dashboard/data`)
// and work equally well for the initial page-load request.

import type { Env } from "./index";
import { computeDashboardPayload } from "./dashboard_signals";

export function checkDashboardAuth(request: Request, env: Env): boolean {
  if (!env.MCP_AUTH_TOKEN) return false; // fail closed, same as checkGraphCronAuth's missing-secret case
  const url = new URL(request.url);
  const qp = url.searchParams.get("token");
  if (qp && qp === env.MCP_AUTH_TOKEN) return true;

  const auth = request.headers.get("Authorization") || "";
  if (auth === `Bearer ${env.MCP_AUTH_TOKEN}`) return true;
  if (auth.startsWith("Basic ")) {
    try {
      const decoded = atob(auth.slice(6));
      const idx = decoded.indexOf(":");
      const password = idx >= 0 ? decoded.slice(idx + 1) : decoded;
      if (password === env.MCP_AUTH_TOKEN) return true;
    } catch {
      // malformed Basic header -- fall through to false below
    }
  }
  return false;
}

function unauthorized(): Response {
  return new Response(
    "Unauthorized. Pass ?token=<MCP_AUTH_TOKEN>, an Authorization: Bearer header, or HTTP Basic auth (any username, password = the token).\n",
    { status: 401, headers: { "WWW-Authenticate": 'Basic realm="ondine-dashboard"' } },
  );
}

export async function handleDashboardData(request: Request, env: Env): Promise<Response> {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  try {
    const payload = await computeDashboardPayload(env);
    return new Response(JSON.stringify(payload, null, 2), {
      headers: { "Content-Type": "application/json" },
    });
  } catch (e) {
    return new Response(JSON.stringify({ error: String(e) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export function handleDashboardPage(request: Request, env: Env): Response {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  const url = new URL(request.url);
  const token = url.searchParams.get("token") || "";
  return new Response(dashboardHtml(token), {
    headers: { "Content-Type": "text/html; charset=utf-8" },
  });
}

export async function handleDashboardWs(request: Request, env: Env): Promise<Response> {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  if (request.headers.get("Upgrade") !== "websocket") {
    return new Response("Expected WebSocket upgrade (Upgrade: websocket header missing).", { status: 426 });
  }
  const id = env.DASHBOARD_HUB.idFromName("singleton");
  const stub = env.DASHBOARD_HUB.get(id);
  return stub.fetch(request);
}

// Deliberately plain (Section 12c: "this is an operator tool, not a
// polished product; don't over-invest in visual design at the expense
// of correctness"). One table, a status pill, and a WebSocket that
// re-renders the whole table on every broadcast -- no framework, no
// external CSS/JS, so the page has nothing to fetch beyond itself and
// its own /dashboard/ws connection.
function dashboardHtml(token: string): string {
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ondine — Operator Dashboard</title>
<style>
  body{font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",monospace;max-width:920px;margin:32px auto;padding:0 16px;color:#1a1a1a;background:#fafafa}
  h1{font-size:18px;margin:0 0 4px}
  .meta{color:#666;font-size:12px;margin-bottom:16px}
  .status{display:inline-block;padding:4px 12px;border-radius:4px;font-weight:600;font-size:13px;margin-bottom:20px}
  .status-nominal{background:#d7f2dd;color:#166534}
  .status-degraded{background:#fef3c7;color:#92400e}
  .status-stalled{background:#fee2e2;color:#991b1b}
  table{width:100%;border-collapse:collapse}
  td,th{text-align:left;padding:8px 6px;border-bottom:1px solid #e5e5e5;vertical-align:top;font-size:13px}
  th{color:#666;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.03em}
  .dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-top:4px}
  .dot-ok{background:#22c55e}
  .dot-warn{background:#f59e0b}
  .dot-critical{background:#ef4444}
  .dot-unknown{background:#cbd5e1}
  .value{font-weight:600}
  .detail{color:#555;font-size:12px;margin-top:2px}
  .note{color:#888;font-size:12px;margin-top:2px;font-style:italic}
  .conn{font-size:12px;color:#888;float:right;font-weight:normal}
</style></head><body>
<h1>Ondine — Operator Dashboard<span class="conn" id="conn">connecting…</span></h1>
<div class="meta" id="meta">Section 12 — full visibility, private. Reuses the MCP_AUTH_TOKEN gate.</div>
<div id="status" class="status">…</div>
<table>
  <thead><tr><th style="width:26px"></th><th>Signal</th><th>Value</th></tr></thead>
  <tbody id="rows"><tr><td colspan="3">Waiting for first poll…</td></tr></tbody>
</table>
<script>
(function () {
  var token = ${JSON.stringify(token)};
  var proto = location.protocol === "https:" ? "wss:" : "ws:";
  var wsUrl = proto + "//" + location.host + "/dashboard/ws?token=" + encodeURIComponent(token);
  var connEl = document.getElementById("conn");
  var statusEl = document.getElementById("status");
  var rowsEl = document.getElementById("rows");
  var metaEl = document.getElementById("meta");

  function statusClass(s) {
    if (s === "Nominal") return "status-nominal";
    if (s === "Degraded") return "status-degraded";
    return "status-stalled";
  }
  function dotClass(configured, state) {
    return "dot-" + (configured ? state : "unknown");
  }
  function render(payload) {
    statusEl.className = "status " + statusClass(payload.overallStatus);
    statusEl.textContent = payload.overallStatus;
    metaEl.textContent = "Last updated: " + payload.generatedAt;
    rowsEl.innerHTML = "";
    (payload.signals || []).forEach(function (s) {
      var tr = document.createElement("tr");

      var dotTd = document.createElement("td");
      var dot = document.createElement("span");
      dot.className = "dot " + dotClass(s.configured, s.state);
      dotTd.appendChild(dot);

      var labelTd = document.createElement("td");
      labelTd.textContent = s.label;

      var valTd = document.createElement("td");
      var valDiv = document.createElement("div");
      valDiv.className = "value";
      valDiv.textContent = s.configured ? s.value : "not configured";
      valTd.appendChild(valDiv);
      if (s.detail) {
        var d = document.createElement("div");
        d.className = "detail";
        d.textContent = s.detail;
        valTd.appendChild(d);
      }
      if (s.note) {
        var n = document.createElement("div");
        n.className = "note";
        n.textContent = s.note;
        valTd.appendChild(n);
      }

      tr.appendChild(dotTd);
      tr.appendChild(labelTd);
      tr.appendChild(valTd);
      rowsEl.appendChild(tr);
    });
  }

  function connect() {
    var ws = new WebSocket(wsUrl);
    ws.onopen = function () { connEl.textContent = "live"; };
    ws.onmessage = function (ev) {
      if (ev.data === "pong") return;
      try { render(JSON.parse(ev.data)); } catch (e) { /* ignore malformed frame */ }
    };
    ws.onclose = function () { connEl.textContent = "reconnecting…"; setTimeout(connect, 2000); };
    ws.onerror = function () { ws.close(); };
    var pingTimer = setInterval(function () {
      if (ws.readyState === WebSocket.OPEN) ws.send("ping");
    }, 25000);
  }
  connect();
})();
</script>
</body></html>`;
}
