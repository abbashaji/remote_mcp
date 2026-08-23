// code_cell_workflow.ts
//
// Section 4f: one Workflow instance per CodeCell. Deliberately built on
// this Worker's OWN existing helper modules (groq.ts, gemini.ts,
// github.ts, discord.ts, codecells.ts) rather than a second Worker with
// its own copies of the same calls -- this project already has exactly
// one Cloudflare account, one set of provider secrets, and (per Claude's
// one custom-connector-slot limit) needs to stay reachable at a single
// MCP endpoint. See workflows.ts for the pre-existing generic JobWorkflow
// this project already had; CodeCellWorkflow is the same durable-Workflow
// pattern, just with real business logic instead of generic GET/sleep steps.
//
// QStash pacing addendum (Section 4f / Section 2's QStash row): the
// fast-worker-generate step does NOT call generateCode() directly
// in-process anymore. Cloudflare Workflows durable-executes a SINGLE
// instance's steps -- it has no primitive for "don't send more than N
// requests/minute to Groq across every concurrently-running instance."
// That's a cross-instance concern, and it's exactly QStash's job as of
// this revision. So this step instead publishes to this same Worker's
// own /qstash/fast-worker-generate route (auth.ts) via qstashPublish,
// tagged with a shared Upstash-Flow-Control-Key -- every CodeCell's
// dispatch shares one wait list, regardless of which instance sent it.
// That route runs the actual generateCode() cascade and resolves this
// step's step.waitForEvent("fast-worker-result"), the same "webhook
// callback resolves a durable wait" pattern already used for
// heavy-worker-result, just self-triggered by our own QStash publish
// rather than an external GitHub Actions runner.

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import type { Env } from "./index";
import { groqChatCompletion } from "./groq";
import { geminiGenerateContent } from "./gemini";
import { githubTriggerWorkflow } from "./github";
import { discordSendMessage } from "./discord";
import { qstashPublish } from "./qstash";
import { updateCell, getCell } from "./codecells";
import { postHogCaptureException, postHogCaptureCodeCellResolution } from "./posthog_events";

export interface CodeCellWorkflowParams {
  cell_id: number;
  spec: string;
}

interface HeavyWorkerResult {
  passed: boolean;
  log: string;
}

export interface FastWorkerDraft {
  code: string;
  provider: string;
}

export type FastWorkerEventPayload = FastWorkerDraft | { error: string };

// ---------------------------------------------------------------------
// Section 4b: Fast Worker cascade -- Groq primary, tiered Gemini/Gemma
// fallback. Reuses groqChatCompletion/geminiGenerateContent verbatim;
// both already return an error STRING rather than throwing on failure
// (matching this project's existing tool convention), so a tier is
// treated as failed if its result starts with "Error ".
//
// Exported (not step-local) because /qstash/fast-worker-generate in
// auth.ts is now the actual caller -- the QStash-paced dispatch route,
// not this Workflow's own step.do(), runs this cascade. See the file
// header above.
// ---------------------------------------------------------------------
const FALLBACK_TIERS: Array<{ name: string; model: string }> = [
  { name: "gemini-3.5-flash-lite", model: "gemini-3.5-flash-lite" },
  { name: "gemini-3.1-flash-lite", model: "gemini-3.1-flash-lite" },
  { name: "gemini-2.5-flash-lite", model: "gemini-2.5-flash-lite" },
  { name: "gemma-4-26b", model: "gemma-4-26b" },
];

const CODE_GEN_INSTRUCTION =
  "Respond with ONLY the finished code in a single fenced code block " +
  "(``` ... ```). No preamble, no explanation, no commentary before or " +
  "after the block -- the block's contents are saved and executed as-is.";

// Extracts the code payload from a Fast Worker response. Providers
// routinely wrap output in prose ("Here is the function...") and/or a
// ```lang fenced block even when explicitly told not to -- this strips
// both so code_cells.code holds only runnable code, never markdown or
// commentary. Falls back to the trimmed raw response if no fence is
// found, on the assumption the provider returned bare code.
function extractCode(raw: string): string {
  const fenced = raw.match(/```(?:[a-zA-Z0-9_+-]*\n)?([\s\S]*?)```/);
  if (fenced) {
    return fenced[1].trim();
  }
  return raw.trim();
}

export async function generateCode(env: Env, spec: string): Promise<FastWorkerDraft> {
  const prompt = `${spec}\n\n${CODE_GEN_INSTRUCTION}`;

  const groqResult = await groqChatCompletion(env, "llama-3.3-70b-versatile", [{ role: "user", content: prompt }]);
  if (!groqResult.startsWith("Error ")) {
    return { code: extractCode(groqResult), provider: "groq" };
  }

  let lastErr = groqResult;
  for (const tier of FALLBACK_TIERS) {
    const result = await geminiGenerateContent(env, tier.model, [{ role: "user", content: prompt }]);
    if (!result.startsWith("Error ")) {
      return { code: extractCode(result), provider: tier.name };
    }
    lastErr = result;
  }
  throw new Error(`All Fast Worker tiers exhausted. Last error: ${lastErr}`);
}

// ---------------------------------------------------------------------
// Section 4c: pre-filter tagging via Gemma 4 31B, constrained with
// response_schema (Section 4e) so the result is always one of a fixed
// enum, never free text this code would have to parse defensively.
// ---------------------------------------------------------------------
async function classifyFailure(env: Env, log: string): Promise<{ tag: "known_flake_pattern" | "needs_human"; reason: string }> {
  const raw = await geminiGenerateContent(
    env,
    "gemma-4-31b",
    [{ role: "user", content: `Classify this Heavy Worker test failure log:\n\n${log.slice(0, 4000)}` }],
    {
      responseMimeType: "application/json",
      responseSchema: {
        type: "OBJECT",
        properties: {
          tag: { type: "STRING", enum: ["known_flake_pattern", "needs_human"] },
          reason: { type: "STRING" },
        },
        required: ["tag", "reason"],
      },
    },
  );
  try {
    const parsed = JSON.parse(raw);
    if (parsed.tag === "known_flake_pattern" || parsed.tag === "needs_human") {
      return parsed;
    }
  } catch {
    // fall through to the safe default below
  }
  // Non-overlap rule (4c): a tagging failure defaults to needs_human,
  // never to silently suppressing an alert.
  return { tag: "needs_human", reason: "classification unavailable, defaulting to human review" };
}

// ---------------------------------------------------------------------
// Section 4 step 7/8: urgent tags (needs_human, dead_letter) alert
// Discord immediately; known_flake_pattern is logged but left for a
// batched digest (Section 3b) rather than paging anyone.
//
// Section 10 (third destination, wired here): the same call that would
// otherwise only alert Discord now ALSO posts to PostHog's error-
// tracking capture endpoint (posthog_events.ts), on every Failed/
// Dead_Letter transition -- a strictly wider condition than Discord's
// "urgent" (needs_human/dead_letter) gate, since Section 10 wants
// known_flake_pattern failures in the trend data too even though they
// don't page anyone. This is intentionally the ONLY new write added
// here -- no new trigger logic, per Section 10's "same Worker call...
// no new trigger logic, just an additional write alongside two that
// already exist."
//
// Non-overlap rule (Section 10): PostHog is a read/trend surface only.
// Nothing downstream of this call reads PostHog to decide pipeline
// behavior -- Turso's `status` column (already updated by the caller
// before notify() runs) remains the only source of truth orchestration
// logic reads.
//
// Optional, not load-bearing (Section 10): a PostHog capture failure is
// caught and logged here, never rethrown -- it must not fail this step
// or block the Failed/Dead_Letter transition it's attached to. This is
// why postHogCaptureException itself also never throws (returns an
// error STRING, matching this project's convention) -- belt and
// suspenders against a PostHog outage cascading into a Workflow step
// failure.
// ---------------------------------------------------------------------
async function notify(
  env: Env,
  cellId: number,
  tag: string,
  details?: { status?: string; lastError?: string; provider?: string },
): Promise<void> {
  const discordUrgent = tag === "needs_human" || tag === "dead_letter";
  if (discordUrgent && env.DISCORD_BOT_TOKEN && env.DISCORD_ALERT_CHANNEL_ID) {
    await discordSendMessage(env.DISCORD_BOT_TOKEN, env.DISCORD_ALERT_CHANNEL_ID, `⚠️ CodeCell #${cellId} — \`${tag}\`, needs a look.`);
  }

  // PostHog: every Failed/Dead_Letter transition, not just the subset
  // that pages Discord -- see the block comment above.
  const isFailureTransition = tag !== "passed";
  if (isFailureTransition) {
    try {
      const result = await postHogCaptureException(env, {
        cellId,
        tag,
        status: details?.status ?? tag,
        provider: details?.provider,
        message: details?.lastError ?? `CodeCell #${cellId} reached '${tag}'.`,
      });
      if (result.startsWith("Error ")) {
        console.error(`PostHog capture failed for CodeCell #${cellId} (non-blocking): ${result}`);
      }
    } catch (e) {
      // Belt-and-suspenders: postHogCaptureException already catches
      // internally and returns an error string rather than throwing,
      // but this second layer guarantees a PostHog-side surprise (a
      // bug in this file, a runtime exception the string-return
      // convention didn't anticipate) still can't take down notify(),
      // and therefore can't take down the Failed/Dead_Letter transition
      // notify() is attached to.
      console.error(`PostHog capture threw unexpectedly for CodeCell #${cellId} (non-blocking): ${e}`);
    }
  }
}

function requireWorkerUrl(env: Env): string {
  if (!env.WORKER_URL) {
    throw new Error(
      "WORKER_URL is not configured on this Worker (needed so fast-worker-generate can publish a QStash " +
        "message back to this same Worker's own /qstash/fast-worker-generate route). Set it in wrangler.toml's " +
        "[vars] block to this Worker's own https://....workers.dev base URL.",
    );
  }
  return env.WORKER_URL;
}

function requireFastWorkerCallbackToken(env: Env): string {
  if (!env.FAST_WORKER_CALLBACK_TOKEN) {
    throw new Error(
      "FAST_WORKER_CALLBACK_TOKEN is not configured on this Worker. Run: wrangler secret put FAST_WORKER_CALLBACK_TOKEN " +
        "(a random secret, e.g. `openssl rand -hex 32` -- gates /qstash/fast-worker-generate, see auth.ts).",
    );
  }
  return env.FAST_WORKER_CALLBACK_TOKEN;
}

export class CodeCellWorkflow extends WorkflowEntrypoint<Env, CodeCellWorkflowParams> {
  async run(event: WorkflowEvent<CodeCellWorkflowParams>, step: WorkflowStep) {
    const { cell_id, spec } = event.payload;
    const instanceId = (event as any).instanceId as string;

    try {
      // Dispatch: publish to our own /qstash/fast-worker-generate route
      // via QStash rather than calling generateCode() in-process. The
      // Upstash-Flow-Control-Key is shared across every CodeCellWorkflow
      // instance's dispatch, so QStash -- not this single instance -- is
      // what enforces "don't burst past Groq/Gemini's actual RPM caps"
      // when many cells go Pending at once (Section 4f's non-overlap
      // rule; see Section 2's QStash row for the disputed Groq daily cap
      // this is deliberately conservative against).
      await step.do("fast-worker-dispatch", async () => {
        const workerUrl = requireWorkerUrl(this.env);
        const token = requireFastWorkerCallbackToken(this.env);
        const rate = Number(this.env.FAST_WORKER_RATE_PER_MINUTE ?? "20");
        const parallelismRaw = this.env.FAST_WORKER_PARALLELISM;
        const result = await qstashPublish(
          this.env,
          `${workerUrl}/qstash/fast-worker-generate`,
          { cell_id, spec, workflow_instance_id: instanceId },
          {
            retries: 2,
            flowControl: {
              key: "fast-worker-generate",
              rate,
              period: "1m",
              parallelism: parallelismRaw ? Number(parallelismRaw) : undefined,
            },
            // QStash forwards any Upstash-Forward-* header to the
            // destination with the prefix stripped -- this is how
            // /qstash/fast-worker-generate authenticates the request as
            // actually having come from our own paced publish, not an
            // arbitrary POST to a guessable route.
            extraHeaders: { "Upstash-Forward-Authorization": `Bearer ${token}` },
          },
        );
        if (result.startsWith("Error ")) throw new Error(result);
        return result;
      });

      // Durable wait: /qstash/fast-worker-generate (auth.ts) runs the
      // actual Groq -> Gemini cascade once QStash releases it per the
      // flow-control key above, then resolves this via sendEvent --
      // same "webhook resolves step.waitForEvent" shape as
      // heavy-worker-result below, just for the Fast Worker leg.
      const fastWorkerEvent = await step.waitForEvent<FastWorkerEventPayload>("fast-worker-result", {
        type: "fast-worker-result",
        timeout: "10 minutes",
      });
      if ("error" in fastWorkerEvent.payload) {
        throw new Error(`Fast Worker dispatch failed: ${fastWorkerEvent.payload.error}`);
      }
      const draft: FastWorkerDraft = fastWorkerEvent.payload;

      await step.do("persist-code-ready", async () => {
        await updateCell(this.env, cell_id, { status: "Code_Ready", code: draft.code, provider: draft.provider });
      });

      await step.do("heavy-worker-dispatch", async () => {
        await updateCell(this.env, cell_id, { status: "Testing" });
        const result = await githubTriggerWorkflow(this.env.GITHUB_TOKEN!, this.env.HEAVY_WORKER_REPO!, "test.yml", "main", {
          cell_id: String(cell_id),
          workflow_instance_id: String(instanceId),
        });
        if (result.startsWith("Failed to trigger")) throw new Error(result);
      });

      // Durable wait: survives a Worker restart between dispatch and
      // result, unlike a synchronous poll loop would. Resolved by
      // /webhook/heavy-worker-result in auth.ts.
      const testEvent = await step.waitForEvent<HeavyWorkerResult>("heavy-worker-result", {
        type: "heavy-worker-result",
        timeout: "30 minutes",
      });

      const tag = await step.do("tag-result", async () => {
        const result = testEvent.payload;

        // Section 10a (approximation -- see this cell's summary and the
        // header comment on postHogCaptureCodeCellResolution for the
        // honest scope of what this is and isn't): posted once per
        // CodeCell resolution, at CodeCell granularity rather than
        // Section 3b's actual "cycle" (a batch of N cells a Reviewer/
        // Architect session looks at together), since that batching
        // concept doesn't exist as implemented infrastructure yet.
        // `escalated` uses "resolved via automatic tagging alone
        // (passed / known_flake_pattern) vs. needing a human
        // (needs_human)" as the proxy for "no Context Slot involvement
        // was needed." Wrapped and logged, never thrown -- same
        // optional/non-load-bearing discipline as the exception capture
        // in notify() below.
        const captureResolution = async (resolutionTag: string, escalated: boolean) => {
          try {
            const r = await postHogCaptureCodeCellResolution(this.env, {
              cellId: cell_id,
              tag: resolutionTag,
              escalated,
              provider: draft.provider,
            });
            if (r.startsWith("Error ")) {
              console.error(`PostHog resolution capture failed for CodeCell #${cell_id} (non-blocking): ${r}`);
            }
          } catch (e) {
            console.error(`PostHog resolution capture threw unexpectedly for CodeCell #${cell_id} (non-blocking): ${e}`);
          }
        };

        if (result.passed) {
          await updateCell(this.env, cell_id, { status: "Completed", tag: "passed", last_error: null });
          await captureResolution("passed", false);
          return "passed";
        }

        const cell = await getCell(this.env, cell_id);
        const nextRetries = (cell?.retry_count ?? 0) + 1;
        const classification = await classifyFailure(this.env, result.log);
        const status = nextRetries > 3 ? "Dead_Letter" : "Failed";
        await updateCell(this.env, cell_id, {
          status,
          tag: classification.tag,
          retry_count: nextRetries,
          last_error: `${classification.reason}\n\n${result.log}`.slice(0, 4000),
        });
        const finalTag = status === "Dead_Letter" ? "dead_letter" : classification.tag;
        await captureResolution(finalTag, finalTag !== "known_flake_pattern");
        return finalTag;
      });

      await step.do("notify", async () => {
        const cell = await getCell(this.env, cell_id);
        await notify(this.env, cell_id, tag, {
          status: cell?.status,
          lastError: cell?.last_error ?? undefined,
          provider: cell?.provider ?? draft.provider,
        });
      });
    } catch (err: any) {
      // Terminal failure -- retry budget exhausted, the Fast Worker
      // dispatch itself failed (all providers exhausted, or QStash
      // couldn't deliver), or the Heavy Worker never called back before
      // the 30-minute timeout. Section 4a's Dead_Letter path, expressed
      // as this Workflow's own error handling.
      await step.do("dead-letter", async () => {
        const message = String(err?.message ?? err).slice(0, 4000);
        await updateCell(this.env, cell_id, { status: "Dead_Letter", last_error: message });
        await notify(this.env, cell_id, "dead_letter", { status: "Dead_Letter", lastError: message });
      });
      throw err;
    }
  }
}
