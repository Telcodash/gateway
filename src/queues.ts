import { Queue, QueueEvents } from "bullmq";
import type IORedis from "ioredis";
import { queueNames } from "./catalog";
import { WEBHOOK_QUEUE, type Config } from "./config";
import { createRedis } from "./redis";
import type { TaskData, WebhookJobData } from "./types";

/**
 * Lazily created BullMQ Queue (producer) and QueueEvents (listener) instances per queue name.
 * Producers share one connection; each QueueEvents needs its own, since it blocks on XREAD.
 */
export class QueueRegistry {
  private readonly queues = new Map<string, Queue<TaskData>>();
  private readonly events = new Map<string, QueueEvents>();
  private webhookQueue?: Queue<WebhookJobData>;

  constructor(
    private readonly config: Config,
    private readonly connection: IORedis,
  ) {}

  has(name: string): boolean {
    return queueNames().includes(name);
  }

  get(name: string): Queue<TaskData> {
    if (!this.has(name)) throw new Error(`Unknown queue [${name}]`);

    let queue = this.queues.get(name);

    if (!queue) {
      queue = new Queue<TaskData>(name, {
        connection: this.connection,
        prefix: this.config.QUEUE_PREFIX,
        defaultJobOptions: {
          attempts: this.config.DEFAULT_ATTEMPTS,
          backoff: { type: "exponential", delay: this.config.DEFAULT_BACKOFF_MS },
          removeOnComplete: { age: this.config.KEEP_COMPLETED_SECONDS },
          removeOnFail: { age: this.config.KEEP_FAILED_SECONDS },
        },
      });
      this.queues.set(name, queue);
    }

    return queue;
  }

  /** Queue-wide lifecycle events, emitted whichever process (gateway or daemon) ran the job. */
  async queueEvents(name: string): Promise<QueueEvents> {
    if (!this.has(name)) throw new Error(`Unknown queue [${name}]`);

    let events = this.events.get(name);

    if (!events) {
      events = new QueueEvents(name, { connection: createRedis(this.config, "worker"), prefix: this.config.QUEUE_PREFIX });
      events.on("error", (error) => console.error(`[events:${name}] ${error.message}`));
      // Every in-flight ?wait= request listens here until its job finishes (removed afterwards).
      events.setMaxListeners(0);
      this.events.set(name, events);
    }

    await events.waitUntilReady();

    return events;
  }

  webhooks(): Queue<WebhookJobData> {
    this.webhookQueue ??= new Queue<WebhookJobData>(WEBHOOK_QUEUE, {
      connection: this.connection,
      prefix: this.config.QUEUE_PREFIX,
      defaultJobOptions: {
        attempts: this.config.WEBHOOK_ATTEMPTS,
        backoff: { type: "exponential", delay: 2_000 },
        removeOnComplete: { age: 60 * 60 },
        removeOnFail: { age: this.config.KEEP_FAILED_SECONDS },
      },
    });

    return this.webhookQueue;
  }

  async close(): Promise<void> {
    await Promise.all([...this.events.values()].map((e) => e.close()));
    await Promise.all([...this.queues.values(), ...(this.webhookQueue ? [this.webhookQueue] : [])].map((q) => q.close()));
  }
}
