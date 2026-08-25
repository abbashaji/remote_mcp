// code_cell_workflow.ts
//
// Section 4f: one Workflow instance per CodeCell. Deliberately built on
// this Worker's OWN existing helper modules (groq.ts, gemini.ts,
// github.ts, push.ts, codecells.ts) rather than a second Worker with its
// own copies of the same calls -- this project already has exactly one
// Cloudflare account, one set of provider secrets, and (per Claude's one
// custom-connector-slot limit) needs to stay reachable at a single MCP
// endpoint. See workflows.ts for the pre-existing generic JobWorkflow
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
//
// Web Push migration addendum (web-push-migration-instructions.md):
// notify()'s alert half no longer posts to Discord. It calls push.ts's
// sendWebPushToAll() instead, sending to every device registered via
// the /subscribe surface (subscribe.ts). Deliberately NOT wrapped in a
// try/catch here the way the PostHog capture below is -- push.ts's own
// header comment is explicit that a non-expiry send failure should
// propagate "so notify()'s caller can let them follow the same Failed/
// Dead_Letter path everything else in this repo already uses," i.e. a
// genuine alert-delivery failure is allowed to fail the "notify" step
// itself (Workflows' own step-retry behavior applies), unlike a PostHog
// trend-data hiccup, which must never block anything.
//
// Section 4g addendum: OpenHands (open-source, model-agnostic autonomous
// coding agent) as a SECOND generation front-end, alongside the Fast
// Worker cascade above -- not a patch layered on top of it, and not a
// wholesale replacement of this file's own retry/tag/notify wiring.
// Where this pipeline's own hand-built machinery is doing something
// genuinely valuable (rate-limit-aware pacing across instances, the
// deterministic Heavy Worker test as the actual pass/fail authority,
// tagging/notify/checkpoint discipline, Section 3b's judgment boundary),
// none of that changes. Where the JOB is "iterate on this code until it
// works," OpenHands' own maintained plan->write->run->iterate loop is a
// more capable version of what generateCode() does in one shot -- so it
// slots in as an alternate front-end for THAT specific job only, then
// hands off to the exact same Heavy Worker / tag-result / notify tail
// every other cell already goes through. See runGenerateTestTagCycle()
// below for the shared tail, and CodeCellWorkflow.run() for the two ways
// into it: execution_mode='openhands' set at cell_create time, or a
// 'pipeline' cell's first test failure auto-escalating to one OpenHands
// attempt before falling to Failed/Debugger. Either way OpenHands never
// decides a cell's status itself -- Heavy Worker's test result, run the
// same way regardless of which front-end produced the code, still does
// that. This is the same non-overlap discipline Section 4a already
// applies to Antigravity's (currently unwired) fix-attempt concept and
// Section 4c applies to tagging: a more capable tool gets to try, never
// to self-certify.

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import type { Env } from "./index";
import { groqChatCompletion } from "./groq";
import { geminiGenerateContent } from "./gemini";
import { githubTriggerWorkflow } from "./github";
import { sendWebPushToAll } from "./push";
import { qstashPublish } from "./qstash";
import { updateCell, getCell } from "./codecells";
import { postHogCaptureException, postHogCaptureCodeCellResolution } from "./posthog_events";

export interface CodeCellWorkflowParams {
  cell_id: number;
  spec: string;
  execution_mode?: string; // 'pipeline' (default) | 'openhands' -- Section 4g
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

// Section 4g: what /webhook/openhands-result (auth.ts) forwards. run_id
// travels on both branches so it's captured even on a failed run (useful
// for pulling logs afterward), not just a successful one.
export type OpenHandsResultPayload =
  | { code: string; run_id?: string }
  | { error: string; run_id?: string };

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
// Section 4/10: classify a Heavy Worker test result and record it --
// Turso status/tag/retry_count, PostHog resolution capture. Extracted
// as a standalone function (Section 4g) so BOTH the primary
// generate-test cycle and an OpenHands escalation cycle can call the
// exact same classification/recording logic rather than maintaining two
// copies -- the provider that produced the code is the only thing that
// varies between callers.
// ---------------------------------------------------------------------
async function classifyAndRecordResult(
  env: Env,
  cellId: number,
  result: HeavyWorkerResult,
  provider: string,
): Promise<string> {
  const captureResolution = async (resolutionTag: string, escalated: boolean) => {
    try {
      const r = await postHogCaptureCodeCellResolution(env, {
        cellId,
        tag: resolutionTag,
        escalated,
        provider,
      });
      if (r.startsWith("Error ")) {
        console.error(`PostHog resolution capture failed for CodeCell #${cellId} (non-blocking): ${r}`);
      }
    } catch (e) {
      console.error(`PostHog resolution capture threw unexpectedly for CodeCell #${cellId} (non-blocking): ${e}`);
    }
  };

  if (result.passed) {
    await updateCell(env, cellId, { status: "Completed", tag: "passed", last_error: null });
    await captureResolution("passed", false);
    return "passed";
  }

  const cell = await getCell(env, cellId);
  const nextRetries = (cell?.retry_count ?? 0) + 1;
  const classification = await classifyFailure(env, result.log);
  const status = nextRetries > 3 ? "Dead_Letter" : "Failed";
  await updateCell(env, cellId, {
    status,
    tag: classification.tag,
    retry_count: nextRetries,
    last_error: `${classification.reason}\n\n${result.log}`.slice(0, 4000),
  });
  const finalTag = status === "Dead_Letter" ? "dead_letter" : classification.tag;
  await captureResolution(finalTag, finalTag !== "known_flake_pattern");
  return finalTag;
}

// ---------------------------------------------------------------------
// Section 4 step 7/8: urgent tags (needs_human, dead_letter) alert the
// operator's subscribed devices via Web Push (push.ts's
// sendWebPushToAll) -- replaces the earlier Discord-webhook alert per
// web-push-migration-instructions.md. known_flake_pattern is logged
// (via the PostHog write below) but left for a batched digest (Section
// 3b) rather than pushing a notification to anyone.
//
// Section 10 (third destination, wired here): the same call that would
// otherwise only push a notification now ALSO posts to PostHog's error-
// tracking capture endpoint (posthog_events.ts), on every Failed/
// Dead_Letter transition -- a strictly wider condition than the push
// gate's "urgent" (needs_human/dead_letter) scope, since Section 10
// wants known_flake_pattern failures in the trend data too even though
// they don't push a notification to anyone.
//
// Non-overlap rule (Section 10): PostHog is a read/trend surface only.
// Nothing downstream of this call reads PostHog to decide pipeline
// behavior -- Turso's `status` column (already updated by the caller
// before notify() runs) remains the only source of truth orchestration
// logic reads.
//
// Push-failure handling is intentionally asymmetric with the PostHog
// block below it: sendWebPushToAll() is awaited directly, NOT wrapped
// in a try/catch here, so a genuine delivery failure (not a pruned
// expired subscription -- push.ts already handles that case internally
// and doesn't throw for it) propagates out of notify() and fails
// whichever step.do("notify"/"dead-letter", ...) call is currently
// running it, exactly as push.ts's own header comment describes.
// PostHog capture failures, by contrast, are always caught and logged
// here (and inside postHogCaptureException itself, which also never
// throws) -- a trend-data hiccup must never block anything, matching
// Section 10's "optional, not load-bearing" framing.
// ---------------------------------------------------------------------
async function notify(
  env: Env,
  cellId: number,
  tag: string,
  details?: { status?: string; lastError?: string; provider?: string },
): Promise<void> {
  const pushUrgent = tag === "needs_human" || tag === "dead_letter";
  if (pushUrgent) {
    await sendWebPushToAll(env, {
      title: `CodeCell #${cellId}`,
      body: `Reached '${tag}'${details?.lastError ? `: ${details.lastError.slice(0, 200)}` : ""}`,
      tag: `codecell-${cellId}`,
    });
  }

  // PostHog: every Failed/Dead_Letter transition, not just the subset
  // that pushes a notification -- see the block comment above.
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
        "message back to this same Worker's own /qstash/fast-worker-generate route, and so the OpenHands " +
        "generation lane knows where to POST its result). Set it in wrangler.toml's [vars] block to this " +
        "Worker's own https://....workers.dev base URL.",
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

function requireOpenHandsCallbackToken(env: Env): string {
  if (!env.OPENHANDS_CALLBACK_TOKEN) {
    throw new Error(
      "OPENHANDS_CALLBACK_TOKEN is not configured on this Worker. Run: wrangler secret put OPENHANDS_CALLBACK_TOKEN " +
        "(a random secret, e.g. `openssl rand -hex 32`). Also set the SAME value as a repo secret named " +
        "OPENHANDS_CALLBACK_TOKEN on whichever repo runs openhands-run.yml -- that job forwards it as a " +
        "bearer token when it POSTs back to /webhook/openhands-result (auth.ts).",
    );
  }
  return env.OPENHANDS_CALLBACK_TOKEN;
}

// Section 4g: the repo containing .github/workflows/openhands-run.yml.
// Deliberately falls back to HEAVY_WORKER_REPO rather than requiring a
// separate secret in the common case -- both workflows want the same
// "public repo, unmetered runners" property, and this project already
// has exactly one such repo (abbashaji/ondine). Override with
// OPENHANDS_REPO only if OpenHands' workflow file should live somewhere
// else.
function resolveOpenHandsRepo(env: Env): string {
  const repo = env.OPENHANDS_REPO || env.HEAVY_WORKER_REPO;
  if (!repo) {
    throw new Error(
      "Neither OPENHANDS_REPO nor HEAVY_WORKER_REPO is set on this Worker -- need a repo containing " +
        ".github/workflows/openhands-run.yml.",
    );
  }
  return repo;
}

// ---------------------------------------------------------------------
// Section 4g: shared generate -> persist -> test -> tag cycle. Two
// callers use this: the primary attempt (generator matches whatever
// execution_mode the cell was created with) and, for a 'pipeline' cell
// whose primary attempt's test failed, a single OpenHands escalation
// attempt. `label` disambiguates step/event names between the two calls
// on the same Workflow instance -- Cloudflare Workflows step identity is
// per-name, and both calls can occur sequentially within one run() (see
// below), so each needs its own step names even though the code path is
// otherwise identical. `type` values passed to step.waitForEvent are
// NOT suffixed -- they must match exactly what the external webhook
// (auth.ts) always sends, regardless of which labeled step is currently
// waiting; only one wait of a given type is ever active on an instance
// at once in this design, so there's no ambiguity.
// ---------------------------------------------------------------------
async function runGenerateTestTagCycle(
  env: Env,
  step: WorkflowStep,
  cellId: number,
  spec: string,
  instanceId: string,
  generator: "pipeline" | "openhands",
  label: "primary" | "escalation",
): Promise<{ tag: string; provider: string }> {
  let draft: FastWorkerDraft;

  if (generator === "openhands") {
    await step.do(`openhands-generate-dispatch-${label}`, async () => {
      await updateCell(env, cellId, { status: "Processing_Drafting" });
      const workerUrl = requireWorkerUrl(env);
      requireOpenHandsCallbackToken(env); // presence check only -- the token itself lives as a repo secret, see openhands-run.yml
      const repo = resolveOpenHandsRepo(env);

      let task = spec;
      if (label === "escalation") {
        const cell = await getCell(env, cellId);
        task =
          `${spec}\n\nA previous automated attempt at this task produced code that failed its test run. ` +
          `Failure context from that attempt:\n${(cell?.last_error ?? "(none captured)").slice(0, 2000)}\n\n` +
          `Iterate past whatever that attempt got wrong.`;
      }

      const result = await githubTriggerWorkflow(env.GITHUB_TOKEN!, repo, "openhands-run.yml", "main", {
        cell_id: String(cellId),
        workflow_instance_id: instanceId,
        task,
        callback_url: `${workerUrl}/webhook/openhands-result`,
      });
      if (result.startsWith("Failed to trigger")) throw new Error(result);
    });

    const ohEvent = await step.waitForEvent<OpenHandsResultPayload>(`openhands-generate-wait-${label}`, {
      type: "openhands-result",
      timeout: "60 minutes",
    });

    await step.do(`record-openhands-run-${label}`, async () => {
      await updateCell(env, cellId, {
        openhands_run_id: ohEvent.payload.run_id ?? null,
        openhands_result: "error" in ohEvent.payload ? `${label}_error` : `${label}_generated`,
      });
    });

    if ("error" in ohEvent.payload) {
      throw new Error(`OpenHands generation failed: ${ohEvent.payload.error}`);
    }
    draft = { code: ohEvent.payload.code, provider: "openhands" };
  } else {
    await step.do(`fast-worker-dispatch-${label}`, async () => {
      const workerUrl = requireWorkerUrl(env);
      const token = requireFastWorkerCallbackToken(env);
      const rate = Number(env.FAST_WORKER_RATE_PER_MINUTE ?? "20");
      const parallelismRaw = env.FAST_WORKER_PARALLELISM;
      const result = await qstashPublish(
        env,
        `${workerUrl}/qstash/fast-worker-generate`,
        { cell_id: cellId, spec, workflow_instance_id: instanceId },
        {
          retries: 2,
          flowControl: {
            key: "fast-worker-generate",
            rate,
            period: "1m",
            parallelism: parallelismRaw ? Number(parallelismRaw) : undefined,
          },
          extraHeaders: { "Upstash-Forward-Authorization": `Bearer ${token}` },
        },
      );
      if (result.startsWith("Error ")) throw new Error(result);
      return result;
    });

    const fastWorkerEvent = await step.waitForEvent<FastWorkerEventPayload>(`fast-worker-wait-${label}`, {
      type: "fast-worker-result",
      timeout: "10 minutes",
    });
    if ("error" in fastWorkerEvent.payload) {
      throw new Error(`Fast Worker dispatch failed: ${fastWorkerEvent.payload.error}`);
    }
    draft = fastWorkerEvent.payload;
  }

  await step.do(`persist-code-ready-${label}`, async () => {
    await updateCell(env, cellId, { status: "Code_Ready", code: draft.code, provider: draft.provider });
  });

  await step.do(`heavy-worker-dispatch-${label}`, async () => {
    await updateCell(env, cellId, { status: "Testing" });
    const result = await githubTriggerWorkflow(env.GITHUB_TOKEN!, env.HEAVY_WORKER_REPO!, "test.yml", "main", {
      cell_id: String(cellId),
      workflow_instance_id: String(instanceId),
    });
    if (result.startsWith("Failed to trigger")) throw new Error(result);
  });

  // Durable wait: survives a Worker restart between dispatch and
  // result, unlike a synchronous poll loop would. Resolved by
  // /webhook/heavy-worker-result in auth.ts -- same route, same event
  // type, regardless of which generator produced the code being tested.
  const testEvent = await step.waitForEvent<HeavyWorkerResult>(`heavy-worker-wait-${label}`, {
    type: "heavy-worker-result",
    timeout: "30 minutes",
  });

  const tag = await step.do(`tag-result-${label}`, async () =>
    classifyAndRecordResult(env, cellId, testEvent.payload, draft.provider),
  );

  return { tag, provider: draft.provider };
}

export class CodeCellWorkflow extends WorkflowEntrypoint<Env, CodeCellWorkflowParams> {
  async run(event: WorkflowEvent<CodeCellWorkflowParams>, step: WorkflowStep) {
    const { cell_id, spec, execution_mode } = event.payload;
    const instanceId = (event as any).instanceId as string;
    const mode: "pipeline" | "openhands" = execution_mode === "openhands" ? "openhands" : "pipeline";

    try {
      let { tag } = await runGenerateTestTagCycle(this.env, step, cell_id, spec, instanceId, mode, "primary");

      // Section 4g auto-escalation. Deliberately NOT a judgment call
      // about whether this task "looks OpenHands-shaped" -- that kind of
      // a-priori fit guess is exactly the sort of thing Section 3b keeps
      // off Layer 0. This is a mechanical, outcome-triggered rule
      // instead: a 'pipeline' cell whose primary attempt actually failed
      // its test (not a malformed-spec Dead_Letter -- that's an
      // Architect problem a longer autonomous loop can't fix either)
      // gets exactly one OpenHands turn before settling to Failed and
      // waiting for a Debugger, gated by openhands_attempted so it can
      // never fire twice on the same cell.
      if (mode === "pipeline" && tag !== "passed" && tag !== "dead_letter") {
        const cell = await step.do("check-openhands-escalation-eligibility", async () => getCell(this.env, cell_id));
        const eligible = !!cell && !cell.openhands_attempted && cell.status === "Failed";
        if (eligible) {
          await step.do("mark-openhands-attempted", async () => {
            await updateCell(this.env, cell_id, { openhands_attempted: true });
          });
          const escalated = await runGenerateTestTagCycle(this.env, step, cell_id, spec, instanceId, "openhands", "escalation");
          tag = escalated.tag;
        }
      }

      await step.do("notify", async () => {
        const cell = await getCell(this.env, cell_id);
        await notify(this.env, cell_id, tag, {
          status: cell?.status,
          lastError: cell?.last_error ?? undefined,
          provider: cell?.provider ?? undefined,
        });
      });
    } catch (err: any) {
      // Terminal failure -- retry budget exhausted, the Fast Worker
      // dispatch itself failed (all providers exhausted, or QStash
      // couldn't deliver), the OpenHands generation lane errored or
      // timed out, or the Heavy Worker never called back before the
      // 30-minute timeout. Section 4a's Dead_Letter path, expressed as
      // this Workflow's own error handling -- unchanged by Section 4g,
      // since an OpenHands failure surfaces the same way any other
      // generation-lane failure always has.
      await step.do("dead-letter", async () => {
        const message = String(err?.message ?? err).slice(0, 4000);
        await updateCell(this.env, cell_id, { status: "Dead_Letter", last_error: message });
        await notify(this.env, cell_id, "dead_letter", { status: "Dead_Letter", lastError: message });
      });
      throw err;
    }
  }
}
