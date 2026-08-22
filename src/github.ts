// github.ts
//
// Direct port of github_tools.py's 6 tools. Stateless: every call opens
// its own fetch() to the GitHub REST API, same as the original used a
// fresh httpx.Client per call. No local state, so nothing here cares
// whether this Worker cold-starts between calls.

const GITHUB_API = "https://api.github.com";

function headers(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    // GitHub rejects any request with no User-Agent with a 403 --
    // this was the missing header causing "missing User-Agent header"
    // failures across every github_* tool.
    "User-Agent": "turso-github-mcp (Cloudflare Worker)",
  };
}

function b64encode(text: string): string {
  return btoa(unescape(encodeURIComponent(text)));
}

function b64decode(b64: string): string {
  return decodeURIComponent(escape(atob(b64)));
}

// GitHub Models inference API (https://models.github.ai/inference) --
// Section 2's "Pre-Filter Tagging (secondary/optional)" row. Deliberately
// reuses the same GITHUB_TOKEN already required for the github_* tools
// above rather than a new secret -- the doc calls this out as the whole
// point of keeping it around ("if you specifically want the
// workflow-scoped GITHUB_TOKEN instead of a separate Gemini credential").
// Only wired up as a fallback if Gemma 4's pool (gemini.ts) is saturated.
const GITHUB_MODELS_API = "https://models.github.ai/inference";

export interface GithubModelsMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export async function githubModelsChatCompletion(
  token: string,
  model: string, // e.g. "openai/gpt-4o-mini" -- publisher-prefixed model id
  messages: GithubModelsMessage[],
  opts?: { temperature?: number; maxTokens?: number },
): Promise<string> {
  try {
    const body: Record<string, unknown> = { model, messages };
    if (opts?.temperature !== undefined) body.temperature = opts.temperature;
    if (opts?.maxTokens !== undefined) body.max_tokens = opts.maxTokens;

    const resp = await fetch(`${GITHUB_MODELS_API}/chat/completions`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/vnd.github+json",
      },
      body: JSON.stringify(body),
    });
    const text = await resp.text();
    if (!resp.ok) {
      throw new Error(`GitHub Models API returned ${resp.status}: ${text.slice(0, 500)}`);
    }
    const data = JSON.parse(text);
    const content = data?.choices?.[0]?.message?.content;
    if (typeof content === "string") return content;
    return JSON.stringify(data, null, 2);
  } catch (e) {
    return `Error calling GitHub Models chat completion: ${e}`;
  }
}

export async function githubTriggerWorkflow(
  token: string,
  repo: string,
  workflowFile: string,
  ref: string = "main",
  inputs?: Record<string, unknown>,
): Promise<string> {
  const body: Record<string, unknown> = { ref };
  if (inputs && Object.keys(inputs).length > 0) {
    body.inputs = Object.fromEntries(
      Object.entries(inputs).map(([k, v]) => [k, String(v)]),
    );
  }
  const url = `${GITHUB_API}/repos/${repo}/actions/workflows/${workflowFile}/dispatches`;
  const resp = await fetch(url, {
    method: "POST",
    headers: headers(token),
    body: JSON.stringify(body),
  });
  if (resp.status === 204) {
    const suffix = body.inputs ? ` with inputs=${JSON.stringify(body.inputs)}` : "";
    return `Triggered ${workflowFile} on ${repo}@${ref}${suffix}.`;
  }
  return `Failed to trigger workflow: ${resp.status} ${await resp.text()}`;
}

export async function githubListRecentRuns(
  token: string,
  repo: string,
  workflowFile: string,
  limit: number = 5,
): Promise<string> {
  const url = `${GITHUB_API}/repos/${repo}/actions/workflows/${workflowFile}/runs?per_page=${limit}`;
  const resp = await fetch(url, { headers: headers(token) });
  if (!resp.ok) return `Failed to list runs: ${resp.status} ${await resp.text()}`;
  const data = (await resp.json()) as { workflow_runs: any[] };
  const runs = data.workflow_runs ?? [];
  if (runs.length === 0) return "No runs found.";
  return runs
    .map(
      (r) =>
        `run_id=${r.id}  status=${r.status}  conclusion=${r.conclusion}  created_at=${r.created_at}`,
    )
    .join("\n");
}

export async function githubGetRunStatus(
  token: string,
  repo: string,
  runId: number,
): Promise<string> {
  const url = `${GITHUB_API}/repos/${repo}/actions/runs/${runId}`;
  const resp = await fetch(url, { headers: headers(token) });
  if (!resp.ok) return `Failed to get run: ${resp.status} ${await resp.text()}`;
  const r = (await resp.json()) as any;
  return `status=${r.status}  conclusion=${r.conclusion}  html_url=${r.html_url}`;
}

export async function githubGetArtifactText(
  token: string,
  repo: string,
  runId: number,
  artifactName: string = "test-results",
  fileInZip: string = "output.log",
): Promise<string> {
  const listUrl = `${GITHUB_API}/repos/${repo}/actions/runs/${runId}/artifacts`;
  const listResp = await fetch(listUrl, { headers: headers(token) });
  if (!listResp.ok) return `Failed to list artifacts: ${listResp.status} ${await listResp.text()}`;
  const data = (await listResp.json()) as { artifacts: any[] };
  const match = (data.artifacts ?? []).find((a) => a.name === artifactName);
  if (!match) {
    const names = (data.artifacts ?? []).map((a) => a.name);
    return `Artifact '${artifactName}' not found. Available: ${JSON.stringify(names)}`;
  }

  const dlResp = await fetch(match.archive_download_url, { headers: headers(token) });
  if (!dlResp.ok) return `Failed to download artifact: ${dlResp.status}`;

  // Workers has no built-in zip reader; unzip the (uncompressed-friendly)
  // central directory ourselves is overkill here -- use a tiny inline
  // STORED/DEFLATE-aware reader via DecompressionStream for the one
  // entry we need.
  const buf = new Uint8Array(await dlResp.arrayBuffer());
  const text = await extractFileFromZip(buf, fileInZip);
  if (text === null) {
    const names = listZipEntries(buf);
    return `'${fileInZip}' not in artifact. Files present: ${JSON.stringify(names)}`;
  }
  return text;
}

export async function githubPushFile(
  token: string,
  repo: string,
  path: string,
  content: string,
  message: string,
  branch: string = "main",
): Promise<string> {
  const url = `${GITHUB_API}/repos/${repo}/contents/${path}`;
  const existing = await fetch(`${url}?ref=${encodeURIComponent(branch)}`, {
    headers: headers(token),
  });
  const sha = existing.ok ? ((await existing.json()) as any).sha : undefined;

  const body: Record<string, unknown> = {
    message,
    content: b64encode(content),
    branch,
  };
  if (sha) body.sha = sha;

  const resp = await fetch(url, {
    method: "PUT",
    headers: headers(token),
    body: JSON.stringify(body),
  });
  if (resp.status === 200 || resp.status === 201) {
    return `Pushed ${path} to ${repo}@${branch}.`;
  }
  return `Failed to push file: ${resp.status} ${await resp.text()}`;
}

export async function githubReadFile(
  token: string,
  repo: string,
  path: string,
  ref: string = "main",
): Promise<string> {
  const url = `${GITHUB_API}/repos/${repo}/contents/${path}?ref=${encodeURIComponent(ref)}`;
  const resp = await fetch(url, { headers: headers(token) });
  if (!resp.ok) return `Failed to read '${path}': ${resp.status} ${await resp.text()}`;
  const data = (await resp.json()) as any;

  if (Array.isArray(data)) {
    if (data.length === 0) return "(empty directory)";
    return data.map((entry) => `${entry.type}  ${entry.name}`).join("\n");
  }

  if (data.encoding !== "base64" || !data.content) {
    return `'${path}' is not a plain text-readable file (type=${data.type}).`;
  }
  try {
    return b64decode(data.content.replace(/\n/g, ""));
  } catch (e) {
    return `Failed to decode content of '${path}': ${e}`;
  }
}

// --- minimal ZIP reader (STORED + DEFLATE entries only, which is all
// GitHub Actions artifact zips use) -------------------------------------

function readU16(buf: Uint8Array, off: number) {
  return buf[off] | (buf[off + 1] << 8);
}
function readU32(buf: Uint8Array, off: number) {
  return (
    (buf[off] | (buf[off + 1] << 8) | (buf[off + 2] << 16) | (buf[off + 3] << 24)) >>> 0
  );
}

interface ZipEntry {
  name: string;
  compressionMethod: number;
  compressedSize: number;
  localHeaderOffset: number;
}

function readCentralDirectory(buf: Uint8Array): ZipEntry[] {
  // Find End Of Central Directory record (search from the end; comment
  // is rare/short in CI artifacts so this is safe in practice).
  let eocd = -1;
  for (let i = buf.length - 22; i >= 0; i--) {
    if (readU32(buf, i) === 0x06054b50) {
      eocd = i;
      break;
    }
  }
  if (eocd === -1) return [];
  const cdOffset = readU32(buf, eocd + 16);
  const cdEntries = readU16(buf, eocd + 10);

  const entries: ZipEntry[] = [];
  let p = cdOffset;
  for (let i = 0; i < cdEntries; i++) {
    if (readU32(buf, p) !== 0x02014b50) break;
    const compressionMethod = readU16(buf, p + 10);
    const compressedSize = readU32(buf, p + 20);
    const nameLen = readU16(buf, p + 28);
    const extraLen = readU16(buf, p + 30);
    const commentLen = readU16(buf, p + 32);
    const localHeaderOffset = readU32(buf, p + 42);
    const name = new TextDecoder().decode(buf.slice(p + 46, p + 46 + nameLen));
    entries.push({ name, compressionMethod, compressedSize, localHeaderOffset });
    p += 46 + nameLen + extraLen + commentLen;
  }
  return entries;
}

function listZipEntries(buf: Uint8Array): string[] {
  return readCentralDirectory(buf).map((e) => e.name);
}

async function extractFileFromZip(buf: Uint8Array, fileName: string): Promise<string | null> {
  const entries = readCentralDirectory(buf);
  const entry = entries.find((e) => e.name === fileName);
  if (!entry) return null;

  const lh = entry.localHeaderOffset;
  const nameLen = readU16(buf, lh + 26);
  const extraLen = readU16(buf, lh + 28);
  const dataStart = lh + 30 + nameLen + extraLen;
  const raw = buf.slice(dataStart, dataStart + entry.compressedSize);

  if (entry.compressionMethod === 0) {
    return new TextDecoder("utf-8", { fatal: false, ignoreBOM: false }).decode(raw);
  }
  if (entry.compressionMethod === 8) {
    const ds = new DecompressionStream("deflate-raw");
    const stream = new Blob([raw]).stream().pipeThrough(ds);
    const decompressed = await new Response(stream).arrayBuffer();
    return new TextDecoder("utf-8", { fatal: false, ignoreBOM: false }).decode(decompressed);
  }
  return `(unsupported zip compression method ${entry.compressionMethod} for '${fileName}')`;
}
