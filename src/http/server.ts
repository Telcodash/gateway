import { timingSafeEqual } from "node:crypto";
import type { Job, QueueEvents } from "bullmq";
import type IORedis from "ioredis";
import { z } from "zod";
import { isAllowed, queueNames } from "../catalog";
import type { Config } from "../config";
import type { QueueRegistry } from "../queues";
import type { TaskData } from "../types";

const DispatchSchema = z.object({
  /** Optional client-supplied id; makes dispatch idempotent. BullMQ forbids ":" in custom ids. */
  id: z
    .string()
    .min(1)
    .max(128)
    .regex(/^[A-Za-z0-9_\-.]+$/)
    .refine((id) => !/^\d+$/.test(id), "id must not be purely numeric")
    .optional(),
  queue: z.string().default("default"),
  name: z.string().min(1).max(255),
  payload: z.record(z.string(), z.unknown()).default({}),
  callback_url: z.url({ protocol: /^https?$/ }).optional(),
  meta: z.record(z.string(), z.unknown()).optional(),
  options: z
    .object({
      attempts: z.number().int().min(1).max(50).optional(),
      backoff_ms: z.number().int().min(0).optional(),
      delay_ms: z.number().int().min(0).optional(),
      priority: z.number().int().min(1).max(2_097_152).optional(),
      /**
       * For reads that may return personal data: the result is deleted from Redis as soon as
       * it is produced (it is only returned to a `?wait=` caller), and failures are kept briefly.
       */
      ephemeral: z.boolean().optional(),
    })
    .default({}),
});

const json = (body: unknown, status = 200) => Response.json(body, { status });
const error = (message: string, status: number, extra: Record<string, unknown> = {}) =>
  json({ error: message, ...extra }, status);

const base = (job: Job<TaskData>) => ({
  id: job.id,
  queue: job.queueName,
  name: job.name,
});

async function serialize(job: Job<TaskData>) {
  return {
    ...base(job),
    state: await job.getState(),
    progress: job.progress,
    result: job.returnvalue ?? null,
    error: job.failedReason ?? null,
    attempts_made: job.attemptsMade,
    max_attempts: job.opts.attempts ?? 1,
    created_at: new Date(job.timestamp).toISOString(),
    started_at: job.processedOn ? new Date(job.processedOn).toISOString() : null,
    finished_at: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
  };
}

const PENDING_STATES = ["waiting", "delayed", "prioritized", "waiting-children"];

type Outcome = { state: "completed"; result: unknown } | { state: "failed"; reason: string };

/**
 * Catch a job's outcome from the queue's event stream. Start watching *before* adding the job:
 * until `outcome(jobId)` names it, every outcome is buffered (only for that brief window, in memory),
 * so a job that finishes before `queue.add()` even returns is still caught.
 *
 * This replaces BullMQ's `job.waitUntilFinished()`, which, after subscribing, polls the job once in
 * case it already finished. An ephemeral job is deleted the moment it completes, so that poll can
 * find no job at all and reports "Missing key for job N. isFinished", which looked like a failure.
 */
function watchOutcomes(events: QueueEvents): { outcome: (jobId: string) => Promise<Outcome>; stop: () => void } {
  const early = new Map<string, Outcome>();
  let wanted: string | undefined;
  let deliver: (outcome: Outcome) => void = () => undefined;
  const settled = new Promise<Outcome>((resolve) => (deliver = resolve));

  const record = (jobId: string, outcome: Outcome) => {
    if (wanted === undefined) early.set(jobId, outcome);
    else if (jobId === wanted) deliver(outcome);
  };
  const onCompleted = ({ jobId, returnvalue }: { jobId: string; returnvalue: unknown }) =>
    record(jobId, { state: "completed", result: returnvalue });
  const onFailed = ({ jobId, failedReason }: { jobId: string; failedReason: string }) =>
    record(jobId, { state: "failed", reason: failedReason });

  events.on("completed", onCompleted);
  events.on("failed", onFailed);

  return {
    outcome(jobId) {
      wanted = jobId;
      const seen = early.get(jobId);
      early.clear();
      if (seen) deliver(seen);

      return settled;
    },
    stop() {
      events.off("completed", onCompleted);
      events.off("failed", onFailed);
      early.clear();
    },
  };
}

export function createServer(config: Config, registry: QueueRegistry, redis: IORedis) {
  const tokens = config.API_TOKENS.map((token) => Buffer.from(token));

  const authorized = (req: Request): boolean => {
    const header = req.headers.get("authorization") ?? "";
    const given = Buffer.from(header.startsWith("Bearer ") ? header.slice(7) : "");

    return tokens.some((token) => token.length === given.length && timingSafeEqual(token, given));
  };

  /** Wrap a handler with auth and uniform error handling. */
  const guarded =
    <R extends Request>(handler: (req: R) => Promise<Response>) =>
    async (req: R): Promise<Response> => {
      if (!authorized(req)) return error("Unauthorized", 401);

      try {
        return await handler(req);
      } catch (e) {
        console.error(`[api] ${req.method} ${new URL(req.url).pathname}:`, e);

        return error("Queue backend unavailable", 503);
      }
    };

  const findJob = async (queue: string, id: string) => (registry.has(queue) ? registry.get(queue).getJob(id) : undefined);

  /** Block until the job finishes or `waitMs` passes. */
  async function waitFor(
    job: Job<TaskData>,
    outcome: Promise<Outcome>,
    waitMs: number,
    ephemeral: boolean,
  ): Promise<Response> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const result = await Promise.race([
      outcome,
      new Promise<null>((resolve) => (timer = setTimeout(() => resolve(null), waitMs))),
    ]);
    clearTimeout(timer);

    if (result?.state === "completed") {
      return json({ ...base(job), state: "completed", result: result.result, error: null }, 200);
    }

    if (result?.state === "failed") {
      return json({ ...base(job), state: "failed", result: null, error: result.reason || job.failedReason }, 200);
    }

    const state = await job.getState();

    // Nobody is waiting for an ephemeral read any more: drop it if no worker has started it.
    if (ephemeral && PENDING_STATES.includes(state)) await job.remove().catch(() => undefined);

    return error(`Task did not finish within ${waitMs}ms`, 504, { ...base(job), state });
  }

  return Bun.serve({
    hostname: config.HOST,
    port: config.PORT,
    routes: {
      "/health": {
        GET: async () => {
          try {
            await redis.ping();

            const queues = await Promise.all(
              queueNames().map(async (name) => {
                const queue = registry.get(name);
                const [workers, counts] = await Promise.all([
                  queue.getWorkersCount(),
                  queue.getJobCounts("waiting", "prioritized", "active", "delayed", "failed"),
                ]);

                return {
                  name,
                  workers,
                  waiting: (counts.waiting ?? 0) + (counts.prioritized ?? 0),
                  active: counts.active ?? 0,
                  delayed: counts.delayed ?? 0,
                  failed: counts.failed ?? 0,
                };
              }),
            );

            return json({ status: "ok", queues });
          } catch {
            return error("Redis unavailable", 503);
          }
        },
      },

      "/tasks": {
        POST: guarded(async (req) => {
          const raw = await req.json().catch(() => undefined);
          const parsed = DispatchSchema.safeParse(raw);

          if (!parsed.success) return error("Validation failed", 422, { issues: z.treeifyError(parsed.error) });

          const input = parsed.data;
          const waitParam = new URL(req.url).searchParams.get("wait");
          const waitMs = waitParam === null ? 0 : Math.min(Math.max(Number.parseInt(waitParam, 10) || 0, 0), config.MAX_WAIT_MS);
          const ephemeral = input.options.ephemeral === true;

          if (!registry.has(input.queue)) return error(`Unknown queue [${input.queue}]`, 422);
          if (!isAllowed(input.queue, input.name)) {
            return error(`Task [${input.name}] is not allowed on queue [${input.queue}]`, 422);
          }

          const queue = registry.get(input.queue);

          if (input.id) {
            const existing = await queue.getJob(input.id);
            if (existing) return json(await serialize(existing), 200);
          }

          // A caller about to block on the result should fail fast if nothing can produce it.
          if (waitMs > 0 && (await queue.getWorkersCount()) === 0) {
            return error(`No workers are consuming queue [${input.queue}]`, 503);
          }

          // Watch before adding so the outcome can't be missed, however fast the worker is.
          const watch = waitMs > 0 ? watchOutcomes(await registry.queueEvents(input.queue)) : undefined;

          try {
            const job = await queue.add(
              input.name,
              { payload: input.payload, callbackUrl: input.callback_url, meta: input.meta },
              {
                jobId: input.id,
                attempts: input.options.attempts,
                backoff:
                  input.options.backoff_ms === undefined ? undefined : { type: "exponential", delay: input.options.backoff_ms },
                delay: input.options.delay_ms,
                priority: input.options.priority,
                ...(ephemeral && {
                  removeOnComplete: true,
                  removeOnFail: config.EPHEMERAL_KEEP_FAILED_SECONDS > 0 ? { age: config.EPHEMERAL_KEEP_FAILED_SECONDS } : true,
                }),
              },
            );

            return watch ? await waitFor(job, watch.outcome(job.id!), waitMs, ephemeral) : json(await serialize(job), 202);
          } finally {
            watch?.stop();
          }
        }),
      },

      "/tasks/:queue/:id": {
        GET: guarded(async (req) => {
          const job = await findJob(req.params.queue, req.params.id);

          return job ? json(await serialize(job)) : error("Task not found", 404);
        }),

        DELETE: guarded(async (req) => {
          const job = await findJob(req.params.queue, req.params.id);

          if (!job) return error("Task not found", 404);
          if (await job.isActive()) return error("Task is running and cannot be cancelled", 409);

          await job.remove();

          return new Response(null, { status: 204 });
        }),
      },
    },

    fetch: () => error("Not found", 404),
  });
}
