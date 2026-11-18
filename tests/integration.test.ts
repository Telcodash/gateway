/**
 * End-to-end: HTTP dispatch → BullMQ worker → signed webhook delivery.
 * Requires a Redis at REDIS_URL (default redis://127.0.0.1:6379). Uses an isolated key prefix.
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { UnrecoverableError, Worker } from "bullmq";
import { queueNames } from "../src/catalog";
import { loadConfig } from "../src/config";
import { createServer } from "../src/http/server";
import { QueueRegistry } from "../src/queues";
import { createRedis } from "../src/redis";
import { SIGNATURE_HEADER, verify } from "../src/signature";
import type { WebhookBody } from "../src/types";
import { startEventRelay, startWebhookWorker } from "../src/webhooks";
import { startTaskWorkers } from "../src/workers";

const prefix = `gateway-test-${crypto.randomUUID()}`;
const config = loadConfig({
  REDIS_URL: process.env.REDIS_URL,
  PORT: "0",
  HOST: "127.0.0.1",
  QUEUE_PREFIX: prefix,
  API_TOKENS: "test-token-0123456789",
  WEBHOOK_SECRET: "test-webhook-secret-0123",
  DEFAULT_BACKOFF_MS: "10",
});

const auth = { Authorization: "Bearer test-token-0123456789", "Content-Type": "application/json" };

const received: WebhookBody[] = [];
let receiver: ReturnType<typeof Bun.serve>;
let api: ReturnType<typeof createServer>;
let workers: Worker[];
const producer = createRedis(config, "producer");
const workerConn = createRedis(config, "worker");
const registry = new QueueRegistry(config, producer);

async function waitFor<T>(fn: () => T | undefined, timeout = 8_000): Promise<T> {
  const started = Date.now();
  while (Date.now() - started < timeout) {
    const value = fn();
    if (value !== undefined) return value;
    await Bun.sleep(25);
  }
  throw new Error("Timed out waiting for condition");
}

const eventFor = (taskId: string, event: string) => () =>
  received.find((body) => body.task_id === taskId && body.event === event);

const dispatch = (body: Record<string, unknown>) =>
  fetch(`${api.url}tasks`, {
    method: "POST",
    headers: auth,
    body: JSON.stringify({ callback_url: `${receiver.url}hook`, ...body }),
  });

/** Stands in for an external daemon (e.g. ../customer-daemon) consuming its own queue. */
function startFakeCustomerDaemon() {
  return new Worker(
    "customers",
    async (job) => {
      if (job.name === "customers.get") {
        await job.updateProgress(50);
        return { id: job.data.payload.id, first_name: "Ada" };
      }
      throw new UnrecoverableError("Invalid payload");
    },
    { connection: createRedis(config, "worker"), prefix },
  );
}

beforeAll(async () => {
  receiver = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    async fetch(req) {
      const body = await req.text();
      if (!verify(body, req.headers.get(SIGNATURE_HEADER) ?? "", config.WEBHOOK_SECRET)) {
        return new Response("bad signature", { status: 401 });
      }
      received.push(JSON.parse(body));
      return new Response(null, { status: 204 });
    },
  });
  api = createServer(config, registry, producer);
  workers = [...startTaskWorkers(config, workerConn), startWebhookWorker(config, workerConn)];
  await startEventRelay(registry, queueNames());
});

afterAll(async () => {
  await Promise.all(workers.map((w) => w.close()));
  await Promise.all(queueNames().map((q) => registry.get(q).obliterate({ force: true })));
  await registry.webhooks().obliterate({ force: true });
  await registry.close();
  await api.stop(true);
  await receiver.stop(true);
  producer.disconnect();
  workerConn.disconnect();
});

describe("HTTP API", () => {
  test("health is public and reports workers per queue", async () => {
    const res = await fetch(`${api.url}health`);
    expect(res.status).toBe(200);

    const { queues } = (await res.json()) as { queues: { name: string; workers: number }[] };
    expect(queues.find((q) => q.name === "default")?.workers).toBeGreaterThan(0);
    expect(queues.find((q) => q.name === "customers")?.workers).toBe(0);
  });

  test("only allow-listed task names are accepted per queue", async () => {
    expect((await dispatch({ queue: "customers", name: "customers.delete_everything" })).status).toBe(422);
    expect((await dispatch({ queue: "customers", name: "example.echo" })).status).toBe(422);
  });

  test("rejects missing or wrong tokens", async () => {
    expect((await fetch(`${api.url}tasks`, { method: "POST" })).status).toBe(401);
    expect(
      (await fetch(`${api.url}tasks`, { method: "POST", headers: { Authorization: "Bearer nope" } })).status,
    ).toBe(401);
  });

  test("validates input, queue and processor", async () => {
    expect((await dispatch({})).status).toBe(422);
    expect((await dispatch({ name: "example.echo", queue: "nope" })).status).toBe(422);
    expect((await dispatch({ name: "does.not.exist" })).status).toBe(422);
  });

  test("dispatch is idempotent on client-supplied id", async () => {
    const id = crypto.randomUUID();
    const first = await dispatch({ id, name: "example.sleep", payload: { ms: 400 }, options: { delay_ms: 5_000 } });
    const second = await dispatch({ id, name: "example.sleep", payload: { ms: 400 } });

    expect(first.status).toBe(202);
    expect(second.status).toBe(200);
    expect(((await second.json()) as { id: string }).id).toBe(id);

    const del = await fetch(`${api.url}tasks/default/${id}`, { method: "DELETE", headers: auth });
    expect(del.status).toBe(204);
    expect((await fetch(`${api.url}tasks/default/${id}`, { headers: auth })).status).toBe(404);
  });
});

describe("?wait", () => {
  test("returns the result in the same request", async () => {
    const res = await fetch(`${api.url}tasks?wait=5000`, {
      method: "POST",
      headers: auth,
      body: JSON.stringify({ name: "example.echo", payload: { hi: 1 }, options: { ephemeral: true } }),
    });
    const body = (await res.json()) as { id: string; state: string; result: unknown };

    expect(res.status).toBe(200);
    expect(body.state).toBe("completed");
    expect(body.result).toEqual({ echo: { hi: 1 } });

    // Ephemeral: the result is not left behind in Redis.
    expect((await fetch(`${api.url}tasks/default/${body.id}`, { headers: auth })).status).toBe(404);
  });

  test("reports failures", async () => {
    const res = await fetch(`${api.url}tasks?wait=5000`, {
      method: "POST",
      headers: auth,
      body: JSON.stringify({ name: "example.fail", payload: { unrecoverable: true, message: "bad" }, options: { ephemeral: true } }),
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ state: "failed", error: "bad" });
  });

  test("fails fast with 503 when no worker consumes the queue", async () => {
    const res = await fetch(`${api.url}tasks?wait=5000`, {
      method: "POST",
      headers: auth,
      body: JSON.stringify({ queue: "customers", name: "customers.get", payload: { id: "cus_x" } }),
    });

    expect(res.status).toBe(503);
  });

  test("times out with 504 and drops an unstarted ephemeral task", async () => {
    const res = await fetch(`${api.url}tasks?wait=300`, {
      method: "POST",
      headers: auth,
      body: JSON.stringify({ name: "example.sleep", payload: { ms: 200 }, options: { ephemeral: true, delay_ms: 60_000 } }),
    });
    const body = (await res.json()) as { id: string; state: string };

    expect(res.status).toBe(504);
    expect(body.state).toBe("delayed");
    expect((await fetch(`${api.url}tasks/default/${body.id}`, { headers: auth })).status).toBe(404);
  });
});

describe("external daemon", () => {
  test("a burst of ephemeral reads all complete, even when the worker beats the waiter", async () => {
    // An instant worker finishes (and BullMQ deletes) ephemeral jobs before the gateway is waiting
    // on them; each must still be reported completed, not "Missing key for job".
    const daemon = startFakeCustomerDaemon();
    await daemon.waitUntilReady();

    try {
      const responses = await Promise.all(
        Array.from({ length: 150 }, (_, i) =>
          fetch(`${api.url}tasks?wait=5000`, {
            method: "POST",
            headers: auth,
            body: JSON.stringify({ queue: "customers", name: "customers.get", payload: { id: `cus_${i}` }, options: { ephemeral: true } }),
          }).then(async (res) => ({ status: res.status, body: (await res.json()) as { state: string; error: unknown; result: { id: string } } })),
        ),
      );

      expect(responses.filter((r) => r.body.state !== "completed").map((r) => r.body.error)).toEqual([]);
      expect(responses.map((r) => r.body.result.id)).toEqual(Array.from({ length: 150 }, (_, i) => `cus_${i}`));
    } finally {
      await daemon.close();
    }
  });


  test("jobs run by another process still produce webhooks and wait results", async () => {
    const daemon = startFakeCustomerDaemon();
    await daemon.waitUntilReady();

    try {
      const res = await fetch(`${api.url}tasks?wait=5000`, {
        method: "POST",
        headers: auth,
        body: JSON.stringify({ queue: "customers", name: "customers.get", payload: { id: "cus_1" }, options: { ephemeral: true } }),
      });
      expect(await res.json()).toMatchObject({ state: "completed", result: { id: "cus_1", first_name: "Ada" } });

      const id = crypto.randomUUID();
      await dispatch({ id, queue: "customers", name: "customers.get", payload: { id: "cus_2" } });

      const completed = await waitFor(eventFor(id, "completed"));
      expect(completed.result).toEqual({ id: "cus_2", first_name: "Ada" });
      expect(eventFor(id, "active")()).toBeDefined();
      expect(eventFor(id, "progress")()?.progress).toBe(50);
    } finally {
      await daemon.close();
    }
  });
});

describe("round trip", () => {
  test("completed task posts active → progress → completed webhooks", async () => {
    const id = crypto.randomUUID();
    const res = await dispatch({ id, name: "example.sleep", payload: { ms: 200 }, meta: { source: "test" } });
    expect(res.status).toBe(202);

    const completed = await waitFor(eventFor(id, "completed"));
    expect(completed.result).toEqual({ slept: 200 });
    expect(completed.meta).toEqual({ source: "test" });
    expect(eventFor(id, "active")()).toBeDefined();
    expect(received.filter((b) => b.task_id === id && b.event === "progress").length).toBeGreaterThan(0);

    const status = (await (await fetch(`${api.url}tasks/default/${id}`, { headers: auth })).json()) as { state: string };
    expect(status.state).toBe("completed");
  });

  test("failed webhook is only sent once retries are exhausted", async () => {
    const id = crypto.randomUUID();
    await dispatch({ id, name: "example.fail", payload: { message: "boom" }, options: { attempts: 2, backoff_ms: 10 } });

    const failed = await waitFor(eventFor(id, "failed"));
    expect(failed.error).toBe("boom");
    expect(failed.attempts_made).toBe(2);
    expect(received.filter((b) => b.task_id === id && b.event === "failed")).toHaveLength(1);
    expect(received.filter((b) => b.task_id === id && b.event === "active")).toHaveLength(2);
  });

  test("unrecoverable errors skip retries", async () => {
    const id = crypto.randomUUID();
    await dispatch({ id, name: "example.fail", payload: { unrecoverable: true }, options: { attempts: 5 } });

    const failed = await waitFor(eventFor(id, "failed"));
    expect(failed.attempts_made).toBe(1);
  });
});
