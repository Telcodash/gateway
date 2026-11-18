import IORedis from "ioredis";
import type { Config } from "./config";

/**
 * Create a Redis connection suitable for BullMQ.
 *
 * Workers need `maxRetriesPerRequest: null` so blocking commands never time out.
 * Producers (the API) fail fast instead, so a Redis outage turns into a 503 and not a hung request.
 */
export function createRedis(config: Config, role: "producer" | "worker"): IORedis {
  return new IORedis(config.REDIS_URL, {
    maxRetriesPerRequest: role === "worker" ? null : 1,
    enableOfflineQueue: role === "worker",
  });
}
