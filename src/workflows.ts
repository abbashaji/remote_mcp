// workflows.ts
//
// Fresh scaffold for a Cloudflare Workflow bound to this Worker. Workflows
// give you durable, multi-step execution: each step.do() is retried and
// checkpointed independently, and the whole run survives Worker restarts.
//
// JobWorkflow here is intentionally generic (a named, ordered list of
// steps, each either an HTTP call or a sleep) so it's runnable out of the
// box. Swap runStep()'s body for whatever real multi-step process this
// server should own -- e.g. "clone repo -> run tests -> write results to
// Turso -> push a summary file to GitHub" is a natural fit given the
// github_*/turso_* tools already in this project.
//
// Bound in wrangler.toml as JOB_WORKFLOW; triggered via the workflow_*
// MCP tools in index.ts, which call env.JOB_WORKFLOW.create()/.get().

import { WorkflowEntrypoint, type WorkflowEvent, type WorkflowStep } from "cloudflare:workers";
import type { Env } from "./index";

export interface JobStep {
  name: string;
  /** If set, this step does a GET against `url` and records status/ok. */
  url?: string;
  /** If set, this step sleeps for this many seconds before running. */
  sleepSeconds?: number;
}

export interface JobWorkflowParams {
  label?: string;
  steps?: JobStep[];
}

interface JobStepResult {
  url?: string;
  status?: number;
  ok?: boolean;
  ran?: boolean;
  note?: string;
}

export class JobWorkflow extends WorkflowEntrypoint<Env, JobWorkflowParams> {
  async run(event: WorkflowEvent<JobWorkflowParams>, step: WorkflowStep) {
    const label = event.payload?.label ?? "job";
    const steps = event.payload?.steps ?? [];

    const results: Record<string, unknown> = {};

    for (const [i, s] of steps.entries()) {
      const stepId = `${i + 1}-${s.name}`;

      if (s.sleepSeconds) {
        await step.sleep(`${stepId}-sleep`, `${s.sleepSeconds} seconds`);
      }

      results[stepId] = await step.do(stepId, async () => this.runStep(s));
    }

    return { label, stepCount: steps.length, results };
  }

  // Placeholder work for a single step. Replace with whatever this
  // workflow should actually do -- this default just does an optional
  // HTTP GET so the scaffold is runnable/testable immediately. Return
  // type is constrained to plain JSON-serializable data, since step.do()
  // results get checkpointed.
  private async runStep(s: JobStep): Promise<JobStepResult> {
    if (s.url) {
      const resp = await fetch(s.url);
      return { url: s.url, status: resp.status, ok: resp.ok };
    }
    return { ran: true, note: `No url set for step '${s.name}' -- edit workflows.ts to give it real work.` };
  }
}
