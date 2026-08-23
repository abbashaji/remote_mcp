// dashboard_do.ts
//
// Section 12c: "poll once, broadcast to N tabs." A personal dashboard
// tab left open all day, polling Turso directly on every render, is
// exactly the steady background read load Section 5a's row budget
// doesn't need -- a Durable Object polling on its own Alarm API timer
// and holding WebSocket connections open costs the same whether one
// dashboard tab is connected or fifty.
//
// Uses hibernatable WebSockets (this.ctx.acceptWebSocket) rather than
// holding sockets open via a long-lived in-memory event loop -- current
// Cloudflare Workers idiom for a Durable Object that mostly just needs
// to push occasional broadcasts: the DO can evict from memory between
// alarms and hibernating sockets reconnect transparently, so an idle
// dashboard tab doesn't pin this instance active (and billed) between
// polls. See https://developers.cloudflare.com/durable-objects/api/alarms/
// and the hibernatable WebSockets API docs.
//
// One singleton instance for the whole dashboard (auth.ts/dashboard.ts
// route every /dashboard/ws request to env.DASHBOARD_HUB.idFromName
// ("singleton")) -- there's exactly one operator, so there's no reason
// to shard this by session/user the way RUNNER (runner.ts) shards by
// runner_id.
//
// Bound in wrangler.toml as DASHBOARD_HUB (class_name = "DashboardHub"),
// same SQLite-backed-storage pattern as TaskRunner, own migration tag.

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./index";
import { computeDashboardPayload, type DashboardPayload } from "./dashboard_signals";

// 20s: frequent enough that the dashboard feels live, infrequent enough
// to stay well inside Turso's row-read budget (Section 5a) and Neo4j's
// free-tier request ceiling even with this DO active continuously during
// operator hours. Same interval regardless of how many tabs are
// connected -- that's the whole point of this pattern (Section 12c).
const POLL_INTERVAL_MS = 20_000;

export class DashboardHub extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected WebSocket upgrade (Upgrade: websocket header missing).", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Hibernatable accept -- the runtime manages the socket's lifecycle
    // independently of this DO instance staying resident in memory.
    this.ctx.acceptWebSocket(server);

    // Arm the poll loop if it isn't already running. Idempotent: if an
    // alarm is already scheduled (another tab connected earlier), this
    // is a no-op rather than resetting the timer -- keeps the broadcast
    // cadence stable regardless of how many tabs connect/disconnect.
    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + 250);
    }

    return new Response(null, { status: 101, webSocket: client });
  }

  // No real client->server protocol -- a plain "ping" is the only
  // message a dashboard tab ever sends (keeps the socket active through
  // idle periods in some browsers/proxies), answered with "pong".
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message === "string" && message === "ping") {
      ws.send("pong");
    }
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, wasClean: boolean): Promise<void> {
    try {
      ws.close(code, reason);
    } catch {
      // already closed -- fine, this.ctx.getWebSockets() drops it on the next alarm regardless
    }
  }

  async webSocketError(ws: WebSocket): Promise<void> {
    try {
      ws.close();
    } catch {
      // already closed
    }
  }

  async alarm(): Promise<void> {
    const sockets = this.ctx.getWebSockets();

    // No open tabs -- don't bother polling, and don't reschedule. A new
    // /dashboard/ws connection re-arms the alarm in fetch() above, so
    // this never leaves the dashboard permanently stalled, just idle
    // when genuinely nobody's watching (which is also the point: don't
    // poll Turso/Neo4j on a schedule with nobody connected to see it).
    if (sockets.length === 0) return;

    let payload: DashboardPayload;
    try {
      payload = await computeDashboardPayload(this.env);
    } catch (e) {
      // computeDashboardPayload's own signal functions already catch
      // everything they touch -- reaching this branch means something
      // outside any individual signal broke (e.g. Promise.all itself
      // throwing on a bug, not a provider outage). Surface it as a
      // single critical signal rather than silently dropping this poll.
      payload = {
        generatedAt: new Date().toISOString(),
        overallStatus: "Stalled",
        signals: [
          {
            key: "poll_error",
            label: "Dashboard poll error",
            configured: true,
            state: "critical",
            value: "error",
            detail: String(e),
          },
        ],
      };
    }

    const body = JSON.stringify(payload);
    for (const ws of sockets) {
      try {
        ws.send(body);
      } catch {
        // socket is likely already closing/closed -- the next alarm's
        // getWebSockets() call naturally drops it, nothing to clean up here
      }
    }

    await this.ctx.storage.setAlarm(Date.now() + POLL_INTERVAL_MS);
  }
}
