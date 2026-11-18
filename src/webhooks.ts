import { UnrecoverableError, Worker, type Job } from "bullmq";
import type IORedis from "ioredis";
import { WEBHOOK_QUEUE, type Config } from "./config";
import type { QueueRegistry } from "./queues";
import { SIGNATURE_HEADER, sign } from "./signature";
import type { TaskData, TaskEvent, WebhookBody, WebhookJobData } from "./types";

/**
 * Queue a webhook for a task lifecycle event. Delivery happens on the internal webhook queue,
 * so a slow or unavailable receiver never blocks task processing and failed deliveries are retried.
 */
export async function enqueueWebhook(
  registry: QueueRegistry,
  job: Job<TaskData>,
  event: TaskEvent,
  extra: Pick<WebhookBody, "progress" | "result" | "error"> = {},
  /** Redis stream id of the source event: identical across relays, so duplicates collapse. */
  eventId?: string,
): Promise<void> {
  const url = job.data.callbackUrl;

  if (!url || !job.id) return;

  const body: WebhookBody = {
    event,
    task_id: job.id,
    queue: job.queueName,
    name: job.name,
    attempts_made: job.attemptsMade,
    max_attempts: job.opts.attempts ?? 1,
    ...extra,
    meta: job.data.meta,
    timestamp: new Date().toISOString(),
  };

  const jobId = eventId
    ? `${job.queueName}-${job.id}-${eventId}`
    : event === "progress"
      ? undefined
      : `${job.queueName}-${job.id}-${event}-${job.attemptsMade}`;

  await registry.webhooks().add(event, { url, body }, { jobId });
}

/**
 * Turn queue-wide BullMQ events into webhooks for every catalog queue. Because the events come
 * from Redis, this works for jobs run by external daemons as well as in-process workers.
 * Ephemeral jobs (removed on completion, no callback URL) are skipped.
 */
export async function startEventRelay(registry: QueueRegistry, queues: string[]): Promise<void> {
  for (const queueName of queues) {
    const events = await registry.queueEvents(queueName);
    const queue = registry.get(queueName);

    const relay = (event: TaskEvent, jobId: string, eventId: string, extra: (job: Job<TaskData>) => Pick<WebhookBody, "progress" | "result" | "error">) => {
      queue
        .getJob(jobId)
        .then((job) => (job?.data.callbackUrl ? enqueueWebhook(registry, job, event, extra(job), eventId) : undefined))
        .catch((error: Error) => console.error(`[relay:${queueName}] ${event} for ${jobId}: ${error.message}`));
    };

    events.on("active", ({ jobId }, id) => relay("active", jobId, id, () => ({})));
    events.on("progress", ({ jobId, data }, id) => relay("progress", jobId, id, () => ({ progress: data })));
    events.on("completed", ({ jobId }, id) => relay("completed", jobId, id, (job) => ({ result: job.returnvalue })));
    // Queue-wide "failed" is only emitted once a job is finally failed, never between retries.
    events.on("failed", ({ jobId, failedReason }, id) => relay("failed", jobId, id, () => ({ error: failedReason })));
  }
}

/** Status codes worth retrying. Any other 4xx means the receiver rejected the event outright. */
const RETRYABLE_CLIENT_ERRORS = new Set([408, 409, 425, 429]);

export async function deliverWebhook(data: WebhookJobData, config: Config): Promise<{ status: number }> {
  const body = JSON.stringify(data.body);

  let response: Response;

  try {
    response = await fetch(data.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        [SIGNATURE_HEADER]: sign(body, config.WEBHOOK_SECRET),
      },
      body,
      signal: AbortSignal.timeout(config.WEBHOOK_TIMEOUT_MS),
      redirect: "manual",
    });
  } catch (error) {
    throw new Error(`Webhook request to ${data.url} failed: ${(error as Error).message}`);
  }

  if (response.ok) return { status: response.status };

  const message = `Webhook to ${data.url} returned HTTP ${response.status}`;

  if (response.status >= 400 && response.status < 500 && !RETRYABLE_CLIENT_ERRORS.has(response.status)) {
    throw new UnrecoverableError(message);
  }

  throw new Error(message);
}

export function startWebhookWorker(config: Config, connection: IORedis): Worker<WebhookJobData> {
  const worker = new Worker<WebhookJobData>(WEBHOOK_QUEUE, (job) => deliverWebhook(job.data, config), {
    connection,
    prefix: config.QUEUE_PREFIX,
    concurrency: 10,
  });

  worker.on("failed", (job, error) => {
    console.warn(`[webhooks] ${job?.data.body.event} for task ${job?.data.body.task_id}: ${error.message}`);
  });

  return worker;
}
