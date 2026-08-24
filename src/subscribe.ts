// subscribe.ts
//
// Phase 4 of the Web Push migration: the one static "surface" that
// replaces "join the Discord server" -- a page the operator visits once
// per alert-receiving device to subscribe to push notifications. Hosted
// the same way Section 12's dashboard is hosted (Worker-served static
// asset, same Worker), gated by the same checkDashboardAuth mechanism
// the dashboard already uses (Phase 3's "same Worker, same Bearer/
// Cloudflare Access gate... do not stand up a separate auth path").
//
// Four routes (wired into AuthHandler.fetch in auth.ts):
//   GET  /subscribe            -- the HTML page + inline JS that
//                                  registers /sw.js, subscribes via
//                                  pushManager, and POSTs the result to
//                                  /subscribe/register
//   GET  /vapid-public-key     -- plain-text public key, for the page's
//                                  pushManager.subscribe() call (Phase 3)
//   POST /subscribe/register   -- receives the browser's PushSubscription
//                                  object and upserts it into Turso's
//                                  Push_Subscriptions table (push.ts)
//   GET  /sw.js                -- the service worker itself, served at
//                                  the root scope (not /subscribe/sw.js)
//                                  so its push-event scope covers the
//                                  whole origin. Deliberately unauthed --
//                                  it's static, non-secret JS the browser
//                                  fetches automatically, same as any
//                                  other public service-worker script.

import type { Env } from "./index";
import { checkDashboardAuth } from "./dashboard";
import { getVapidPublicKey, upsertPushSubscription } from "./push";

function unauthorized(): Response {
  return new Response(
    "Unauthorized. Pass ?token=<MCP_AUTH_TOKEN>, an Authorization: Bearer header, or HTTP Basic auth (any username, password = the token).\n",
    { status: 401, headers: { "WWW-Authenticate": 'Basic realm="ondine-subscribe"' } },
  );
}

export function handleVapidPublicKey(request: Request, env: Env): Response {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  try {
    return new Response(getVapidPublicKey(env), { headers: { "Content-Type": "text/plain; charset=utf-8" } });
  } catch (e) {
    return new Response(`Error: ${e}`, { status: 500 });
  }
}

export async function handleSubscribeRegister(request: Request, env: Env): Promise<Response> {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  let body: { endpoint?: string; keys?: { p256dh?: string; auth?: string } };
  try {
    body = await request.json();
  } catch {
    return new Response("Malformed JSON body.", { status: 400 });
  }
  if (!body.endpoint || !body.keys?.p256dh || !body.keys?.auth) {
    return new Response("endpoint, keys.p256dh, and keys.auth are required.", { status: 400 });
  }
  try {
    const id = await upsertPushSubscription(env, {
      endpoint: body.endpoint,
      p256dh: body.keys.p256dh,
      auth: body.keys.auth,
    });
    return new Response(JSON.stringify({ ok: true, id }), { headers: { "Content-Type": "application/json" } });
  } catch (e) {
    return new Response(JSON.stringify({ ok: false, error: String(e) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

export function handleSubscribePage(request: Request, env: Env): Response {
  if (!checkDashboardAuth(request, env)) return unauthorized();
  const url = new URL(request.url);
  const token = url.searchParams.get("token") || "";
  return new Response(subscribePageHtml(token), { headers: { "Content-Type": "text/html; charset=utf-8" } });
}

export function handleServiceWorker(): Response {
  return new Response(serviceWorkerJs(), { headers: { "Content-Type": "application/javascript; charset=utf-8" } });
}

function subscribePageHtml(token: string): string {
  return `<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ondine — Subscribe to Alerts</title>
<style>
  body{font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;max-width:420px;margin:80px auto;padding:0 20px;color:#1a1a1a}
  h1{font-size:18px}
  button{width:100%;padding:10px;font-size:15px;background:#1a1a1a;color:#fff;border:none;border-radius:6px;cursor:pointer;margin-top:16px}
  button:disabled{opacity:.5;cursor:default}
  .status{margin-top:16px;font-size:14px}
  .err{color:#b00020}
  .ok{color:#166534}
  .meta{color:#666;font-size:13px}
</style></head><body>
<h1>Subscribe to CodeCell Alerts</h1>
<p class="meta">Registers this device for push notifications when a cell enters <code>Failed</code> or <code>Dead_Letter</code>. Replaces the old Discord alert channel — see web-push-migration-instructions.md.</p>
<p class="meta">On iPhone: needs Safari 16.4+, and this page must be added to the Home Screen first (Share → Add to Home Screen), then opened from the Home Screen icon before subscribing — it won't work from a regular Safari tab.</p>
<button id="btn">Enable Notifications</button>
<div id="status" class="status"></div>
<script>
(function () {
  var token = ${JSON.stringify(token)};
  var btn = document.getElementById("btn");
  var statusEl = document.getElementById("status");

  function urlBase64ToUint8Array(base64String) {
    var padding = "=".repeat((4 - (base64String.length % 4)) % 4);
    var base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
    var rawData = atob(base64);
    var outputArray = new Uint8Array(rawData.length);
    for (var i = 0; i < rawData.length; ++i) outputArray[i] = rawData.charCodeAt(i);
    return outputArray;
  }

  function setStatus(msg, cls) {
    statusEl.textContent = msg;
    statusEl.className = "status " + (cls || "");
  }

  if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
    setStatus("This browser doesn't support Web Push.", "err");
    btn.disabled = true;
    return;
  }

  btn.addEventListener("click", async function () {
    btn.disabled = true;
    try {
      setStatus("Registering service worker…");
      var reg = await navigator.serviceWorker.register("/sw.js");
      await navigator.serviceWorker.ready;

      setStatus("Fetching VAPID public key…");
      var keyResp = await fetch("/vapid-public-key?token=" + encodeURIComponent(token));
      if (!keyResp.ok) throw new Error("vapid-public-key: " + keyResp.status);
      var vapidKey = (await keyResp.text()).trim();

      setStatus("Requesting subscription…");
      var sub = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(vapidKey),
      });

      setStatus("Registering with server…");
      var regResp = await fetch("/subscribe/register?token=" + encodeURIComponent(token), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(sub.toJSON()),
      });
      if (!regResp.ok) throw new Error("subscribe/register: " + regResp.status);

      setStatus("Subscribed. This device will now receive push alerts.", "ok");
    } catch (e) {
      setStatus("Error: " + (e && e.message ? e.message : e), "err");
      btn.disabled = false;
    }
  });
})();
</script>
</body></html>`;
}

function serviceWorkerJs(): string {
  return `self.addEventListener("push", function (event) {
  var data = {};
  try { data = event.data ? event.data.json() : {}; } catch (e) { data = { title: "Ondine Alert", body: event.data ? event.data.text() : "" }; }
  var title = data.title || "Ondine Alert";
  var options = { body: data.body || "", tag: data.tag || undefined };
  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", function (event) {
  event.notification.close();
});
`;
}
