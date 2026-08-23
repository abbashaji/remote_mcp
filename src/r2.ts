// r2.ts
//
// Section 9/9a of Zero-Cost-Stack-v11.md: Cloudflare R2 as the durable
// blob store for anything too large or too binary for a Turso row (a
// build artifact, a scraped page's screenshot, a headless-test failure
// screenshot) -- distinct from Gemini's File Storage API (Section 9a),
// which is ephemeral single-call model-input staging, not a durable
// store.
//
// Uses wrangler's native R2 binding (env.ARTIFACTS -- see [[r2_buckets]]
// in wrangler.toml), not the S3-compatible REST API: same "let the
// platform's own binding do the auth" choice this project already makes
// for KV/Workflows/Durable Objects, rather than introducing yet another
// credential.
//
// Non-overlap rule (Section 9, same shape as 7b): R2 owns bytes, Turso
// owns state about those bytes. This module does not write any status/
// pipeline-state fields into R2 object metadata -- the only customMetadata
// it sets is `encoding` ("text" | "base64"), which describes the bytes
// themselves (how to decode them back), not anything about a cell's
// lifecycle. That distinction is what keeps this from duplicating what
// Turso's `status` column already owns.
//
// An MCP tool call can't easily carry raw binary, so the tool boundary in
// index.ts always sends/receives a string; base64 vs. plain text is
// chosen by the caller per-object and remembered in customMetadata so
// r2GetObject knows how to hand it back.

export interface R2Env {
  ARTIFACTS: R2Bucket;
}

function requireBucket(env: R2Env): R2Bucket {
  if (!env.ARTIFACTS) {
    throw new Error(
      "ARTIFACTS R2 bucket is not bound on this Worker. Add a [[r2_buckets]] binding to wrangler.toml " +
        "(bucket must already exist -- wrangler r2 bucket create <name>) and run: wrangler deploy",
    );
  }
  return env.ARTIFACTS;
}

function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const CHUNK = 0x8000; // avoid blowing the call stack on String.fromCharCode(...bigArray)
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode(...bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

function generateKey(prefix?: string): string {
  const ts = new Date().toISOString().replace(/[:.]/g, "-");
  const rand = crypto.randomUUID().slice(0, 8);
  const base = `${ts}-${rand}`;
  return prefix ? `${prefix.replace(/\/+$/, "")}/${base}` : base;
}

export async function r2PutObject(
  env: R2Env,
  args: {
    key?: string;
    content: string;
    encoding?: "text" | "base64"; // "base64" for binary payloads -- MCP calls can't carry raw bytes
    content_type?: string;
  },
): Promise<string> {
  try {
    const bucket = requireBucket(env);
    const encoding = args.encoding ?? "text";
    const key = args.key && args.key.trim() ? args.key.trim() : generateKey();
    const bytes =
      encoding === "base64" ? base64ToBytes(args.content) : new TextEncoder().encode(args.content);

    await bucket.put(key, bytes, {
      httpMetadata: args.content_type ? { contentType: args.content_type } : undefined,
      customMetadata: { encoding },
    });

    return `Wrote object '${key}' (${bytes.byteLength} bytes, encoding=${encoding}).`;
  } catch (e) {
    return `Error writing R2 object: ${e}`;
  }
}

export async function r2GetObject(env: R2Env, key: string): Promise<string> {
  try {
    const bucket = requireBucket(env);
    const obj = await bucket.get(key);
    if (!obj) {
      return `Error reading R2 object: no object found at key '${key}'.`;
    }
    const encoding: "text" | "base64" = obj.customMetadata?.encoding === "base64" ? "base64" : "text";
    const bytes = new Uint8Array(await obj.arrayBuffer());
    const content = encoding === "base64" ? bytesToBase64(bytes) : new TextDecoder().decode(bytes);

    return JSON.stringify(
      {
        key,
        size: bytes.byteLength,
        encoding,
        content_type: obj.httpMetadata?.contentType ?? null,
        uploaded: obj.uploaded,
        content,
      },
      null,
      2,
    );
  } catch (e) {
    return `Error reading R2 object: ${e}`;
  }
}

export async function r2ListObjects(env: R2Env, prefix?: string, limit?: number): Promise<string> {
  try {
    const bucket = requireBucket(env);
    const res = await bucket.list({ prefix: prefix || undefined, limit: limit && limit > 0 ? limit : 100 });
    const objects = res.objects.map((o) => ({
      key: o.key,
      size: o.size,
      uploaded: o.uploaded,
      content_type: o.httpMetadata?.contentType ?? null,
    }));
    return JSON.stringify({ objects, truncated: res.truncated }, null, 2);
  } catch (e) {
    return `Error listing R2 objects: ${e}`;
  }
}

export async function r2DeleteObject(env: R2Env, key: string): Promise<string> {
  try {
    const bucket = requireBucket(env);
    await bucket.delete(key);
    return `Deleted object '${key}'.`;
  } catch (e) {
    return `Error deleting R2 object: ${e}`;
  }
}
