// runner.ts
//
// Fresh scaffold for a Durable Object "runner" -- a small stateful actor
// that owns a task queue and processes it one item at a time via the
// alarm API. Unlike Workflows (multi-step, long-running, one instance per
// run), a runner is a persistent per-name actor: call runner_enqueue with
// the same runner_id repeatedly and every task lands in that same actor's
// queue, sharing its storage.
//
// Good fit for things like "a single background worker that drains a
// queue of GitHub/Turso jobs one at a time" or "hold some running state
// between MCP calls" -- swap runTask()'s body for real work.
//
// Bound in wrangler.toml as RUNNER (class_name = "TaskRunner"); driven by
// the runner_* MCP tools in index.ts via env.RUNNER.idFromName(runnerId).

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./index";

export interface RunnerTask {
  id: string;
  command: string;
  payload?: unknown;
  status: "queued" | "running" | "done" | "error";
  result?: unknown;
  error?: string;
  createdAt: number;
  updatedAt: number;
}

export class TaskRunner extends DurableObject<Env> {
  async enqueue(command: string, payload?: unknown): Promise<string> {
    const id = crypto.randomUUID();
    const task: RunnerTask = {
      id,
      command,
      payload,
      status: "queued",
      createdAt: Date.now(),
      updatedAt: Date.now(),
    };
    await this.ctx.storage.put(`task:${id}`, task);

    if ((await this.ctx.storage.getAlarm()) === null) {
      await this.ctx.storage.setAlarm(Date.now() + 50);
    }
    return id;
  }

  async status(id: string): Promise<RunnerTask | null> {
    return (await this.ctx.storage.get<RunnerTask>(`task:${id}`)) ?? null;
  }

  async list(limit: number = 20): Promise<RunnerTask[]> {
    const entries = await this.ctx.storage.list<RunnerTask>({
      prefix: "task:",
      limit,
      reverse: true,
    });
    return Array.from(entries.values());
  }

  async alarm(): Promise<void> {
    const entries = await this.ctx.storage.list<RunnerTask>({ prefix: "task:" });
    const all = Array.from(entries.values());
    const queued = all.find((t) => t.status === "queued");
    if (!queued) return;

    queued.status = "running";
    queued.updatedAt = Date.now();
    await this.ctx.storage.put(`task:${queued.id}`, queued);

    try {
      queued.result = await this.runTask(queued.command, queued.payload);
      queued.status = "done";
    } catch (e) {
      queued.status = "error";
      queued.error = String(e);
    }
    queued.updatedAt = Date.now();
    await this.ctx.storage.put(`task:${queued.id}`, queued);

    const stillQueued = all.some((t) => t.id !== queued.id && t.status === "queued");
    if (stillQueued) await this.ctx.storage.setAlarm(Date.now() + 50);
  }

  // Placeholder command dispatch. Add real commands here (or replace
  // entirely) -- 'ping' and 'fetch' exist just so the scaffold works
  // out of the box.
  private async runTask(command: string, payload?: unknown): Promise<unknown> {
    switch (command) {
      case "ping":
        return { pong: true, echo: payload ?? null };
      case "fetch": {
        const url = typeof payload === "string" ? payload : (payload as { url?: string } | undefined)?.url;
        if (!url) throw new Error("fetch task requires payload.url or payload = { url }");
        const resp = await fetch(url);
        const body = await resp.text();
        return { url, status: resp.status, ok: resp.ok, body: body.slice(0, 5000) };
      }
      default:
        throw new Error(`Unknown runner command '${command}'. Add a case in runner.ts's runTask().`);
    }
  }
}
