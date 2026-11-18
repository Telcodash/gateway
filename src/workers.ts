import { UnrecoverableError, Worker, type Job } from "bullmq";
import type IORedis from "ioredis";
import type { Config } from "./config";
import { findProcessor, processors } from "./processors";
import type { TaskData } from "./types";

/**
 * Start one in-process BullMQ Worker per queue that has processors in src/processors.
 * Queues served by external daemons are consumed by those daemons, not here.
 * Webhooks are emitted by the event relay (src/webhooks.ts), not by these workers.
 */
export function startTaskWorkers(config: Config, connection: IORedis): Worker<TaskData>[] {
  return Object.keys(processors).map((queueName) => {
    const worker = new Worker<TaskData>(
      queueName,
      async (job: Job<TaskData>) => {
        const processor = findProcessor(queueName, job.name);

        if (!processor) {
          throw new UnrecoverableError(`No processor registered for [${job.name}] on queue [${queueName}]`);
        }

        return processor(job.data.payload, job);
      },
      { connection, prefix: config.QUEUE_PREFIX, concurrency: config.WORKER_CONCURRENCY },
    );

    worker.on("error", (error) => console.error(`[workers:${queueName}] ${error.message}`));

    return worker;
  });
}
