// b2.ts
//
// Section 9 (revised): Backblaze B2 half of the object-storage split.
// Owns everything that actually needs real object-storage semantics --
// arbitrary keys, frequent/high-churn writes, prefix-based listing,
// individual deletes -- the job GitHub Releases (github_release.ts)
// isn't shaped for. Per the cell prompt's "if genuinely unsure, default
// to B2" rule, this is the general-purpose bucket for intermediate
// artifacts (headless-test screenshots, scraped page snapshots, a Heavy
// Worker's mid-run output) that aren't yet "the deliverable."
//
// Uses B2's S3-COMPATIBLE API, not B2's native API -- deliberately, per
// the cell prompt: it means plain authenticated HTTP out (same shape as
// upstash.ts/qstash.ts) instead of hand-rolling B2's native auth-token
// exchange. No wrangler binding needed, no AWS SDK dependency -- this
// module hand-signs requests with AWS Signature Version 4 using the Web
// Crypto API (available natively in the Workers runtime), which is all
// S3-compatible auth actually requires.
//
// Same conventions as every other module here: every exported function
// returns an error string (prefixed "Error ...") rather than throwing,
// stateless (fresh fetch() per call).
//
// New secrets (Worker, via `wrangler secret put`): B2_KEY_ID,
// B2_APPLICATION_KEY, B2_BUCKET_NAME, B2_ENDPOINT (the region-specific
// S3-compatible endpoint from the bucket's "Endpoint" field in the B2
// console, e.g. "s3.us-west-004.backblazeb2.com" -- with or without a
// leading "https://", both are accepted below).

export interface B2Env {
  B2_KEY_ID?: string;
  B2_APPLICATION_KEY?: string;
  B2_BUCKET_NAME?: string;
  B2_ENDPOINT?: string;
}

interface B2Config {
  keyId: string;
  applicationKey: string;
  bucket: string;
  host: string; // bare host, no scheme -- e.g. "s3.us-west-004.backblazeb2.com"
  region: string; // e.g. "us-west-004"
}

function requireB2Config(env: B2Env): B2Config {
  if (!env.B2_KEY_ID || !env.B2_APPLICATION_KEY || !env.B2_BUCKET_NAME || !env.B2_ENDPOINT) {
    throw new Error(
      "B2_KEY_ID / B2_APPLICATION_KEY / B2_BUCKET_NAME / B2_ENDPOINT are not all configured on this " +
        "Worker. Create a Backblaze account, a B2 bucket, and an Application Key scoped to that bucket " +
        "at https://secure.backblaze.com/b2_buckets.htm, then run: wrangler secret put B2_KEY_ID && " +
        "wrangler secret put B2_APPLICATION_KEY && wrangler secret put B2_BUCKET_NAME && " +
        "wrangler secret put B2_ENDPOINT (the bucket's S3-compatible endpoint, e.g. " +
        "\"s3.us-west-004.backblazeb2.com\").",
    );
  }
  const host = env.B2_ENDPOINT.replace(/^https?:\/\//, "").replace(/\/+$/, "");
  const regionMatch = host.match(/^s3\.([a-z0-9-]+)\.backblazeb2\.com$/i);
  if (!regionMatch) {
    throw new Error(
      `B2_ENDPOINT '${env.B2_ENDPOINT}' doesn't look like a Backblaze B2 S3-compatible endpoint ` +
        `(expected something like "s3.us-west-004.backblazeb2.com" -- check the bucket's "Endpoint" ` +
        `field in the B2 console, not the native-API "download URL").`,
    );
  }
  return {
    keyId: env.B2_KEY_ID,
    applicationKey: env.B2_APPLICATION_KEY,
    bucket: env.B2_BUCKET_NAME,
    host,
    region: regionMatch[1],
  };
}

// ---- base64 <-> bytes (same approach as github_release.ts -- btoa/atob
// operate on JS strings, not byte arrays, so chunk through
// String.fromCharCode instead of spreading, which blows the call stack
// on anything more than a few thousand bytes). Exported so other
// modules (e.g. codecells.ts's optional checkpoint-artifact routing)
// don't have to reimplement this.

export function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

export function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

// ---- AWS Signature Version 4, hand-rolled over Web Crypto ------------

function toHex(buf: ArrayBuffer): string {
  return Array.from(new Uint8Array(buf))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  return toHex(await crypto.subtle.digest("SHA-256", data as BufferSource));
}

async function hmac(key: ArrayBuffer | Uint8Array, data: string): Promise<ArrayBuffer> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    key as BufferSource,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  return crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(data));
}

async function signingKey(secretKey: string, dateStamp: string, region: string, service: string): Promise<ArrayBuffer> {
  const kDate = await hmac(new TextEncoder().encode("AWS4" + secretKey), dateStamp);
  const kRegion = await hmac(kDate, region);
  const kService = await hmac(kRegion, service);
  return hmac(kService, "aws4_request");
}

// RFC 3986 encoding -- encodeURIComponent leaves !'()* unescaped, AWS's
// canonical form requires them percent-encoded too.
function rfc3986(s: string): string {
  return encodeURIComponent(s).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
}

function canonicalUri(bucket: string, key: string): string {
  if (!key) return "/" + rfc3986(bucket);
  const encodedKey = key.split("/").map(rfc3986).join("/");
  return "/" + rfc3986(bucket) + "/" + encodedKey;
}

function canonicalQueryString(params?: Record<string, string>): string {
  if (!params) return "";
  return Object.keys(params)
    .sort()
    .map((k) => `${rfc3986(k)}=${rfc3986(params[k])}`)
    .join("&");
}

const SERVICE = "s3";

async function b2SignedFetch(
  env: B2Env,
  method: "GET" | "PUT" | "DELETE",
  key: string,
  opts?: { query?: Record<string, string>; body?: Uint8Array; contentType?: string },
): Promise<Response> {
  const cfg = requireB2Config(env);
  const body = opts?.body ?? new Uint8Array(0);
  const payloadHash = await sha256Hex(body);

  const now = new Date();
  const amzDate = now.toISOString().replace(/[:-]|\.\d{3}/g, ""); // YYYYMMDDTHHMMSSZ
  const dateStamp = amzDate.slice(0, 8);

  const uri = canonicalUri(cfg.bucket, key);
  const query = canonicalQueryString(opts?.query);

  const canonicalHeaders = `host:${cfg.host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`;
  const signedHeaders = "host;x-amz-content-sha256;x-amz-date";

  const canonicalRequest = [method, uri, query, canonicalHeaders, signedHeaders, payloadHash].join("\n");

  const credentialScope = `${dateStamp}/${cfg.region}/${SERVICE}/aws4_request`;
  const canonicalRequestHash = toHex(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(canonicalRequest)));
  const stringToSign = ["AWS4-HMAC-SHA256", amzDate, credentialScope, canonicalRequestHash].join("\n");

  const key_ = await signingKey(cfg.applicationKey, dateStamp, cfg.region, SERVICE);
  const signature = toHex(await hmac(key_, stringToSign));

  const authorization =
    `AWS4-HMAC-SHA256 Credential=${cfg.keyId}/${credentialScope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const url = `https://${cfg.host}${uri}${query ? "?" + query : ""}`;
  const headers: Record<string, string> = {
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate,
    Authorization: authorization,
  };
  if (method === "PUT" && opts?.contentType) headers["Content-Type"] = opts.contentType;

  return fetch(url, {
    method,
    headers,
    body: method === "PUT" ? (body as BodyInit) : undefined,
  });
}

// ---- XML parsing for ListObjectsV2 ------------------------------------
// Workers has no DOMParser/XML parser built in, and this response shape
// is simple enough (flat <Contents> blocks) that a small regex scan is
// more honest than pulling in an XML library for four fields.

function xmlUnescape(s: string): string {
  return s
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&amp;/g, "&");
}

function extractTag(xml: string, tag: string): string | undefined {
  const m = xml.match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`));
  return m ? xmlUnescape(m[1]) : undefined;
}

interface B2ObjectSummary {
  key: string;
  size: number;
  lastModified: string;
}

function parseListObjectsXml(xml: string): B2ObjectSummary[] {
  const blocks = xml.match(/<Contents>[\s\S]*?<\/Contents>/g) || [];
  return blocks.map((block) => ({
    key: extractTag(block, "Key") || "",
    size: Number(extractTag(block, "Size") || "0"),
    lastModified: extractTag(block, "LastModified") || "",
  }));
}

// ---- Public API --------------------------------------------------------

/**
 * Write a blob to B2 under `key`, overwriting any existing object at
 * that key. `contentBase64` is base64-encoded bytes -- an MCP tool call
 * can't easily send raw binary, so callers base64-encode at the tool
 * boundary, same approach used for GitHub Release assets.
 */
export async function b2PutObject(
  env: B2Env,
  key: string,
  contentBase64: string,
  contentType: string = "application/octet-stream",
): Promise<string> {
  try {
    if (!key) throw new Error("key must not be empty");
    const bytes = base64ToBytes(contentBase64);
    const resp = await b2SignedFetch(env, "PUT", key, { body: bytes, contentType });
    if (!resp.ok) {
      throw new Error(`B2 PUT '${key}' returned ${resp.status}: ${(await resp.text()).slice(0, 500)}`);
    }
    const etag = resp.headers.get("etag") || "";
    return `Wrote object '${key}' (${bytes.length} bytes, ${contentType}) to B2 bucket.${etag ? ` etag=${etag}` : ""}`;
  } catch (e) {
    return `Error putting B2 object '${key}': ${e}`;
  }
}

/**
 * Read a blob back by key, returned as a JSON string with base64-encoded
 * content plus metadata -- same reasoning as the put side (no raw-binary
 * return channel over MCP).
 */
export async function b2GetObject(env: B2Env, key: string): Promise<string> {
  try {
    if (!key) throw new Error("key must not be empty");
    const resp = await b2SignedFetch(env, "GET", key);
    if (resp.status === 404) return `Object '${key}' not found in B2 bucket.`;
    if (!resp.ok) {
      throw new Error(`B2 GET '${key}' returned ${resp.status}: ${(await resp.text()).slice(0, 500)}`);
    }
    const buf = new Uint8Array(await resp.arrayBuffer());
    const contentType = resp.headers.get("content-type") || "application/octet-stream";
    return JSON.stringify({
      key,
      size: buf.length,
      content_type: contentType,
      content_base64: bytesToBase64(buf),
    });
  } catch (e) {
    return `Error getting B2 object '${key}': ${e}`;
  }
}

/** List objects in the bucket, optionally filtered by key prefix. */
export async function b2ListObjects(env: B2Env, prefix?: string): Promise<string> {
  try {
    const query: Record<string, string> = { "list-type": "2" };
    if (prefix) query.prefix = prefix;
    const resp = await b2SignedFetch(env, "GET", "", { query });
    if (!resp.ok) {
      throw new Error(`B2 List returned ${resp.status}: ${(await resp.text()).slice(0, 500)}`);
    }
    const xml = await resp.text();
    const objects = parseListObjectsXml(xml);
    if (objects.length === 0) {
      return prefix ? `No objects found with prefix '${prefix}'.` : "Bucket is empty.";
    }
    return objects
      .map((o) => `key=${o.key}  size=${o.size}  last_modified=${o.lastModified}`)
      .join("\n");
  } catch (e) {
    return `Error listing B2 objects: ${e}`;
  }
}

/** Delete an object by key. */
export async function b2DeleteObject(env: B2Env, key: string): Promise<string> {
  try {
    if (!key) throw new Error("key must not be empty");
    const resp = await b2SignedFetch(env, "DELETE", key);
    if (resp.status === 204 || resp.status === 200) return `Deleted object '${key}' from B2 bucket.`;
    if (resp.status === 404) return `Object '${key}' not found in B2 bucket (already deleted?).`;
    throw new Error(`${resp.status} ${(await resp.text()).slice(0, 500)}`);
  } catch (e) {
    return `Error deleting B2 object '${key}': ${e}`;
  }
}
