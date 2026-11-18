import type { Worker } from "bullmq";
import { queueNames } from "./catalog";
import { loadConfig } from "./config";
import { createServer } from "./http/server";
import { QueueRegistry } from "./queues";
import { createRedis } from "./redis";
import { startEventRelay, startWebhookWorker } from "./webhooks";
import { startTaskWorkers } from "./workers";

const config = loadConfig();

const producerConnection = createRedis(config, "producer");
const registry = new QueueRegistry(config, producerConnection);

const workers: Worker[] = [];
let server: ReturnType<typeof createServer> | undefined;

if (config.MODE === "all" || config.MODE === "worker") {
  const workerConnection = createRedis(config, "worker");
  workers.push(...startTaskWorkers(config, workerConnection), startWebhookWorker(config, workerConnection));
  await startEventRelay(registry, queueNames());
  console.log(`[queue] in-process workers + webhook relay started for: ${queueNames().join(", ")}`);
}

if (config.MODE === "all" || config.MODE === "api") {
  server = createServer(config, registry, producerConnection);
  console.log(`[queue] API listening on http://${server.hostname}:${server.port}`);
}

let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;

  console.log(`[queue] ${signal} received, finishing active jobs...`);

  await server?.stop();
  await Promise.all(workers.map((worker) => worker.close()));
  await registry.close();
  producerConnection.disconnect();

  process.exit(0);
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));
