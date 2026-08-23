// github_release.ts
//
// Section 9 (revised): GitHub Releases half of the object-storage split.
// Owns artifacts tied to a specific build/deliverable, produced
// infrequently -- a CodeCell's final build output once it reaches
// Completed, a packaged deliverable a Reviewer might download. NOT for
// high write-frequency or prefix-based listing -- see b2.ts for that job.
//
// All assets accumulate under a single rolling release (tag
// v0-artifacts, NOT tied to actual source versioning) rather than
// minting a new release per artifact, per the cell prompt. Reuses
// GITHUB_TOKEN -- same credential already powering github.ts's
// github_push_file/github_read_file -- as long as it has `repo` scope
// (it should, since it's already pushing files).
//
// Same conventions as github.ts: every exported function returns an
// error string (prefixed "Error ...") rather than throwing, stateless
// (fresh fetch() per call, no local state to worry about across
// Worker cold starts).

const GITHUB_API = "https://api.github.com";
const DEFAULT_TAG = "v0-artifacts";

function headers(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "turso-github-mcp (Cloudflare Worker)",
  };
}

// btoa/atob operate on JS strings, not byte arrays, and choke on
// anything outside Latin1 -- chunk through String.fromCharCode instead
// of spreading the whole array (spread blows the call stack on
// anything more than a few thousand bytes).
function bytesToBase64(bytes: Uint8Array): string {
  let binary = "";
  const chunkSize = 0x8000;
  for (let i = 0; i < bytes.length; i += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(i, i + chunkSize));
  }
  return btoa(binary);
}

function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  return bytes;
}

interface ReleaseRef {
  id: number;
  uploadUrlBase: string; // upload_url with the {?name,label} template stripped
  htmlUrl: string;
}

// GET the rolling artifacts release by tag. Returns null (not an error)
// if it doesn't exist yet -- callers decide whether "doesn't exist" is
// an error (list/get/delete) or something to create (put).
async function getArtifactsRelease(
  token: string,
  repo: string,
  tag: string,
): Promise<ReleaseRef | null> {
  const resp = await fetch(`${GITHUB_API}/repos/${repo}/releases/tags/${encodeURIComponent(tag)}`, {
    headers: headers(token),
  });
  if (resp.status === 404) return null;
  if (!resp.ok) {
    throw new Error(`Failed to look up release '${tag}': ${resp.status} ${await resp.text()}`);
  }
  const data = (await resp.json()) as any;
  return {
    id: data.id,
    uploadUrlBase: String(data.upload_url).replace(/\{.*$/, ""),
    htmlUrl: data.html_url,
  };
}

async function createArtifactsRelease(token: string, repo: string, tag: string): Promise<ReleaseRef> {
  const resp = await fetch(`${GITHUB_API}/repos/${repo}/releases`, {
    method: "POST",
    headers: headers(token),
    body: JSON.stringify({
      tag_name: tag,
      name: "Artifacts",
      body:
        "Rolling artifacts release managed by Ondine (Zero-Cost-Stack Section 9). " +
        "Assets accumulate here under unique names; this release is not tied to " +
        "actual source versioning -- don't treat its tag as a semver.",
      draft: false,
      prerelease: false,
    }),
  });
  if (!resp.ok) {
    throw new Error(`Failed to create release '${tag}': ${resp.status} ${await resp.text()}`);
  }
  const data = (await resp.json()) as any;
  return {
    id: data.id,
    uploadUrlBase: String(data.upload_url).replace(/\{.*$/, ""),
    htmlUrl: data.html_url,
  };
}

async function getOrCreateArtifactsRelease(token: string, repo: string, tag: string): Promise<ReleaseRef> {
  const existing = await getArtifactsRelease(token, repo, tag);
  if (existing) return existing;
  return createArtifactsRelease(token, repo, tag);
}

interface ReleaseAsset {
  id: number;
  name: string;
  size: number;
  contentType: string;
  browserDownloadUrl: string;
}

async function listAssetsRaw(token: string, repo: string, releaseId: number): Promise<ReleaseAsset[]> {
  const resp = await fetch(`${GITHUB_API}/repos/${repo}/releases/${releaseId}/assets?per_page=100`, {
    headers: headers(token),
  });
  if (!resp.ok) {
    throw new Error(`Failed to list assets: ${resp.status} ${await resp.text()}`);
  }
  const data = (await resp.json()) as any[];
  return data.map((a) => ({
    id: a.id,
    name: a.name,
    size: a.size,
    contentType: a.content_type,
    browserDownloadUrl: a.browser_download_url,
  }));
}

/**
 * Upload (or overwrite) an asset on the rolling artifacts release.
 * `contentBase64` is base64-encoded bytes -- an MCP tool call can't
 * easily send raw binary, so callers base64-encode at the tool
 * boundary, same approach used for B2 blobs.
 *
 * Upsert semantics: if an asset with this name already exists on the
 * release, it's deleted first and re-uploaded -- GitHub's upload API
 * 422s on a duplicate name within a release, and "put" should behave
 * like a put, not a "create if absent."
 */
export async function githubReleasePutAsset(
  token: string,
  repo: string,
  assetName: string,
  contentBase64: string,
  contentType: string = "application/octet-stream",
  tag: string = DEFAULT_TAG,
): Promise<string> {
  try {
    const release = await getOrCreateArtifactsRelease(token, repo, tag);

    const existing = await listAssetsRaw(token, repo, release.id);
    const dup = existing.find((a) => a.name === assetName);
    if (dup) {
      const delResp = await fetch(`${GITHUB_API}/repos/${repo}/releases/assets/${dup.id}`, {
        method: "DELETE",
        headers: headers(token),
      });
      if (!delResp.ok && delResp.status !== 404) {
        throw new Error(
          `Failed to delete existing asset '${assetName}' before overwrite: ${delResp.status} ${await delResp.text()}`,
        );
      }
    }

    const bytes = base64ToBytes(contentBase64);
    const uploadUrl = `${release.uploadUrlBase}?name=${encodeURIComponent(assetName)}`;
    const resp = await fetch(uploadUrl, {
      method: "POST",
      headers: {
        ...headers(token),
        "Content-Type": contentType,
      },
      body: bytes as BodyInit,
    });
    if (!resp.ok) {
      throw new Error(`Failed to upload asset '${assetName}': ${resp.status} ${await resp.text()}`);
    }
    const data = (await resp.json()) as any;
    return (
      `Uploaded '${assetName}' (${bytes.length} bytes) to ${repo}@${tag} release. ` +
      `asset_id=${data.id}  download_url=${data.browser_download_url}`
    );
  } catch (e) {
    return `Error putting release asset '${assetName}' on ${repo}: ${e}`;
  }
}

/** List assets on the rolling artifacts release. */
export async function githubReleaseListAssets(
  token: string,
  repo: string,
  tag: string = DEFAULT_TAG,
): Promise<string> {
  try {
    const release = await getArtifactsRelease(token, repo, tag);
    if (!release) return `No '${tag}' release found on ${repo} yet (nothing uploaded).`;
    const assets = await listAssetsRaw(token, repo, release.id);
    if (assets.length === 0) return `Release '${tag}' on ${repo} exists but has no assets.`;
    return assets
      .map(
        (a) =>
          `id=${a.id}  name=${a.name}  size=${a.size}  content_type=${a.contentType}  url=${a.browserDownloadUrl}`,
      )
      .join("\n");
  } catch (e) {
    return `Error listing release assets on ${repo}: ${e}`;
  }
}

/**
 * Fetch one asset's bytes by id, returned as a JSON string containing
 * base64-encoded content plus metadata (name/size/content_type) -- the
 * MCP tool boundary problem again: no raw-binary return channel, so
 * base64 it is, same as the put side.
 */
export async function githubReleaseGetAsset(token: string, repo: string, assetId: number): Promise<string> {
  try {
    const metaResp = await fetch(`${GITHUB_API}/repos/${repo}/releases/assets/${assetId}`, {
      headers: headers(token),
    });
    if (!metaResp.ok) {
      throw new Error(`Failed to look up asset ${assetId}: ${metaResp.status} ${await metaResp.text()}`);
    }
    const meta = (await metaResp.json()) as any;

    // Public repo -> browser_download_url serves the raw bytes with no
    // auth needed, which is also the exact behavior the acceptance
    // criteria wants verified (plain HTTPS GET, no re-auth for a
    // Reviewer session pulling this later). Fetch that directly rather
    // than the API asset endpoint with an octet-stream Accept header,
    // so this code path matches what an unauthenticated caller gets.
    const bytesResp = await fetch(meta.browser_download_url);
    if (!bytesResp.ok) {
      throw new Error(`Failed to download asset ${assetId} bytes: ${bytesResp.status}`);
    }
    const buf = new Uint8Array(await bytesResp.arrayBuffer());
    const contentBase64 = bytesToBase64(buf);

    return JSON.stringify({
      id: meta.id,
      name: meta.name,
      size: meta.size,
      content_type: meta.content_type,
      download_url: meta.browser_download_url,
      content_base64: contentBase64,
    });
  } catch (e) {
    return `Error getting release asset ${assetId} from ${repo}: ${e}`;
  }
}

/** Delete an asset by id from the rolling artifacts release. */
export async function githubReleaseDeleteAsset(token: string, repo: string, assetId: number): Promise<string> {
  try {
    const resp = await fetch(`${GITHUB_API}/repos/${repo}/releases/assets/${assetId}`, {
      method: "DELETE",
      headers: headers(token),
    });
    if (resp.status === 204) return `Deleted asset ${assetId} from ${repo}.`;
    if (resp.status === 404) return `Asset ${assetId} not found on ${repo} (already deleted?).`;
    throw new Error(`${resp.status} ${await resp.text()}`);
  } catch (e) {
    return `Error deleting release asset ${assetId} from ${repo}: ${e}`;
  }
}
