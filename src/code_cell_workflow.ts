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

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import type { Env } from "./index";
import { groqChatCompletion } from "./groq";
import { geminiGenerateContent } from "./gemini";
import { githubTriggerWorkflow } from "./github";
import { discordSendMessage } from "./discord";
import { updateCell, getCell } from "./codecells";

export interface CodeCellWorkflowParams {
  cell_id: number;
  spec: string;
}

interface HeavyWorkerResult {
  passed: boolean;
  log: string;
}

// ---------------------------------------------------------------------
// Section 4b: Fast Worker cascade -- Groq primary, tiered Gemini/Gemma
// fallback. Reuses groqChatCompletion/geminiGenerateContent verbatim;
// both already return an error STRING rather than throwing on failure
// (matching this project's existing tool convention), so a tier is
// treated as failed if its result starts with "Error ".
// ---------------------------------------------------------------------
const FALLBACK_TIERS: Array<{ name: string; model: string }> = [
  { name: "gemini-3.5-flash-lite", model: "gemini-3.5-flash-lite" },
  { name: "gemini-3.1-flash-lite", model: "gemini-3.1-flash-lite" },
  { name: "gemini-2.5-flash-lite", model: "gemini-2.5-flash-lite" },
  { name: "gemma-4-26b", model: "gemma-4-26b" },
];

async function generateCode(env: Env, spec: string): Promise<{ code: string; provider: string }> {
  const groqResult = await groqChatCompletion(env, "llama-3.3-70b-versatile", [{ role: "user", content: spec }]);
  if (!groqResult.startsWith("Error ")) {
    return { code: groqResult, provider: "groq" };
  }

  let lastErr = groqResult;
  for (const tier of FALLBACK_TIERS) {
    const result = await geminiGenerateContent(env, tier.model, [{ role: "user", content: spec }]);
    if (!result.startsWith("Error ")) {
      return { code: result, provider: tier.name };
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

export class CodeCellWorkflow extends WorkflowEntrypoint<Env, CodeCellWorkflowParams> {
  async run(event: WorkflowEvent<CodeCellWorkflowParams>, step: WorkflowStep) {
    const { cell_id, spec } = event.payload;
    const instanceId = (event as any).instanceId as string;

    try {
      const draft = await step.do("fast-worker-generate", async () => {
        return await generateCode(this.env, spec);
      });

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
      // Terminal failure -- retry budget exhausted, or the Heavy Worker
      // never called back before the 30-minute timeout. Section 4a's
      // Dead_Letter path, expressed as this Workflow's own error handling.
      await step.do("dead-letter", async () => {
        await updateCell(this.env, cell_id, { status: "Dead_Letter", last_error: String(err?.message ?? err).slice(0, 4000) });
        await notify(this.env, cell_id, "dead_letter");
      });
      throw err;
    }
  }
}
