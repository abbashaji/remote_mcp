// push.ts
//
// Self-owned Web Push (VAPID), replacing Discord Webhooks' one job --
// out-of-band alerts to the operator -- per web-push-migration-
// instructions.md. Sent directly from this Worker via
// @block65/webcrypto-web-push (a Workers/Web-Crypto-runtime build;
// Node's `web-push` package depends on Node crypto APIs Workers don't
// have). No new account, no new Section 2 row -- this is the existing
// Cloudflare Workers Gateway calling browser push services directly.
//
// Subscriptions live in Turso's Push_Subscriptions table (migration
// doc Phase 2) -- a genuinely new table, not a repurposed one, flagged
// here and in the Zero-Cost-Stack doc's Section 2 update for the same
// "why does this table exist" trail the doc already keeps for 7e/7b.

import { createClient, type Client } from "@libsql/client/web";
import webpush from "@block65/webcrypto-web-push";
import type { Env } from "./index";

let cachedClient: Client | null = null;

function getClient(env: Env): Client {
  if (cachedClient) return cachedClient;
  if (!env.TURSO_DATABASE_URL) {
    throw new Error(
      "TURSO_DATABASE_URL is not configured -- Push_Subscriptions lives in the same Turso database as everything else.",
    );
  }
  cachedClient = createClient(
    env.TURSO_AUTH_TOKEN
      ? { url: env.TURSO_DATABASE_URL, authToken: env.TURSO_AUTH_TOKEN }
      : { url: env.TURSO_DATABASE_URL },
  );
  return cachedClient;
}

export interface PushSubscriptionRow {
  id: string;
  endpoint: string;
  p256dh: string;
  auth: string;
}

async function hashEndpoint(endpoint: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(endpoint));
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

// Phase 3's POST /subscribe (wired as /subscribe/register in this repo
// alongside the subscribe page itself, see subscribe.ts). Upserts by
// endpoint -- a browser can re-subscribe with a rotated endpoint at any
// time; `id` is a hash of the endpoint so re-subscribing the same
// endpoint updates the existing row instead of accumulating duplicates.
export async function upsertPushSubscription(
  env: Env,
  sub: { endpoint: string; p256dh: string; auth: string },
): Promise<string> {
  const id = await hashEndpoint(sub.endpoint);
  const now = Date.now();
  const client = getClient(env);
  await client.execute({
    sql: `INSERT INTO Push_Subscriptions (id, endpoint, p256dh, auth, created_at, last_seen_at)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(endpoint) DO UPDATE SET p256dh = excluded.p256dh, auth = excluded.auth, last_seen_at = excluded.last_seen_at`,
    args: [id, sub.endpoint, sub.p256dh, sub.auth, now, now],
  });
  return id;
}

function requireVapid(env: Env): { subject: string; publicKey: string; privateKey: string } {
  if (!env.VAPID_PUBLIC_KEY || !env.VAPID_PRIVATE_KEY || !env.VAPID_SUBJECT) {
    throw new Error(
      "VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY / VAPID_SUBJECT not fully configured on this Worker. Run: " +
        "wrangler secret put VAPID_PRIVATE_KEY / VAPID_PUBLIC_KEY / VAPID_SUBJECT " +
        "(see web-push-migration-instructions.md Phase 1).",
    );
  }
  return { subject: env.VAPID_SUBJECT, publicKey: env.VAPID_PUBLIC_KEY, privateKey: env.VAPID_PRIVATE_KEY };
}

// GET /vapid-public-key's handler reads this directly.
export function getVapidPublicKey(env: Env): string {
  if (!env.VAPID_PUBLIC_KEY) {
    throw new Error("VAPID_PUBLIC_KEY is not configured on this Worker.");
  }
  return env.VAPID_PUBLIC_KEY;
}

// Section 4f/4a's push half of notify() (code_cell_workflow.ts). Sends
// to every subscribed device; prunes any subscription the push service
// reports as expired/revoked (404/410) -- new bookkeeping Discord
// webhooks never needed, since a Discord webhook URL doesn't expire on
// its own (migration doc Phase 5 note). Non-expiry errors are NOT
// swallowed here -- they propagate so notify()'s caller can let them
// follow the same Failed/Dead_Letter path everything else in this repo
// already uses.
export async function sendWebPushToAll(
  env: Env,
  payload: { title: string; body: string; tag: string },
): Promise<void> {
  const vapid = requireVapid(env);
  const client = getClient(env);
  const rs = await client.execute("SELECT id, endpoint, p256dh, auth FROM Push_Subscriptions");
  const subs = rs.rows as unknown as PushSubscriptionRow[];

  const errors: string[] = [];

  await Promise.all(
    subs.map(async (sub) => {
      try {
        await webpush.sendWebPush(
          JSON.stringify(payload),
          { endpoint: sub.endpoint, keys: { p256dh: sub.p256dh, auth: sub.auth } },
          { subject: vapid.subject, publicKey: vapid.publicKey, privateKey: vapid.privateKey },
        );
      } catch (err: any) {
        if (err?.statusCode === 404 || err?.statusCode === 410) {
          await client.execute({ sql: "DELETE FROM Push_Subscriptions WHERE id = ?", args: [sub.id] });
        } else {
          errors.push(`Push send failed for subscription ${sub.id}: ${err}`);
        }
      }
    }),
  );

  if (errors.length > 0) {
    throw new Error(errors.join("; "));
  }
}
