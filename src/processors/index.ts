import type { Job } from "bullmq";
import type { TaskData } from "../types";
import { example } from "./example";

/**
 * A task processor. Receive the validated payload and return a JSON-serialisable result.
 *
 * - Throw to fail the attempt (it will be retried up to `attempts`).
 * - Throw `UnrecoverableError` from bullmq to fail immediately with no further retries.
 * - Call `job.updateProgress(...)` to push a `progress` webhook.
 */
export type Processor = (payload: Record<string, unknown>, job: Job<TaskData>) => Promise<unknown>;

/**
 * Registry of task name → processor, per queue.
 * Add a module per domain (see ./example.ts) and spread it in here.
 */
export const processors: Record<string, Record<string, Processor>> = {
  default: {
    ...example,
  },
};

export function findProcessor(queue: string, name: string): Processor | undefined {
  return processors[queue]?.[name];
}
