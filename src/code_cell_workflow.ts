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
// ---------------------------------------------------------------------
async function notify(env: Env, cellId: number, tag: string): Promise<void> {
  const urgent = tag === "needs_human" || tag === "dead_letter";
  if (urgent && env.DISCORD_BOT_TOKEN && env.DISCORD_ALERT_CHANNEL_ID) {
    await discordSendMessage(env.DISCORD_BOT_TOKEN, env.DISCORD_ALERT_CHANNEL_ID, `⚠️ CodeCell #${cellId} — \`${tag}\`, needs a look.`);
  }
  // PostHog: intentionally not wired here yet. posthog.ts proxies
  // PostHog's own remote MCP tool catalog (annotations/insights/error
  // tracking), which needs a specific tool name confirmed via
  // posthog_list_tools before this workflow calls it blind -- left as
  // a follow-up rather than guessing a tool name that might not exist.
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
        if (result.passed) {
          await updateCell(this.env, cell_id, { status: "Completed", tag: "passed", last_error: null });
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
        return status === "Dead_Letter" ? "dead_letter" : classification.tag;
      });

      await step.do("notify", async () => {
        await notify(this.env, cell_id, tag);
      });
    } catch (err: any) {
      // Terminal failure -- retry budget exhausted, the Fast Worker
      // dispatch itself failed (all providers exhausted, or QStash
      // couldn't deliver), or the Heavy Worker never called back before
      // the 30-minute timeout. Section 4a's Dead_Letter path, expressed
      // as this Workflow's own error handling.
      await step.do("dead-letter", async () => {
        await updateCell(this.env, cell_id, { status: "Dead_Letter", last_error: String(err?.message ?? err).slice(0, 4000) });
        await notify(this.env, cell_id, "dead_letter");
      });
      throw err;
    }
  }
}
