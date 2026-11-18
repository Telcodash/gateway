# gateway

BullMQ task gateway (Bun + TypeScript). Other services dispatch tasks over an authenticated HTTP API. Tasks are run either by in-process processors or by **external daemons** that each consume their own queue, such as `../customer-daemon` on `customers`. Lifecycle events are POSTed back to each task's `callback_url` as HMAC-signed webhooks.

```
Core ──POST /tasks──▶ API ──▶ Redis (BullMQ) ──▶ in-process worker (queue "default")
  ▲                             │            └─▶ external daemon (queue "customers", …)
  │                             ▼
  └── signed webhook ◀── __webhooks queue ◀── event relay (queue-wide events, any worker)
```

Every queue and its allowed task names are declared in [`src/catalog.ts`](src/catalog.ts). A dispatch for anything not listed there gets a `422`.

## Running

```bash
cp .env.example .env   # fill API_TOKENS and WEBHOOK_SECRET
bun install
bun run dev            # API + workers, with reload
bun test               # needs Redis at REDIS_URL
bun run typecheck
```

In production you can split the API and workers into separate processes: `bun run start:api` and `bun run start:worker` (scale workers horizontally). On SIGTERM, each process finishes its active jobs before it exits.

## HTTP API

Every route except `/health` requires `Authorization: Bearer <one of API_TOKENS>`.

| Method | Path | |
|---|---|---|
| GET | `/health` | Redis ping, plus worker count and job counts per queue. `workers: 0` means the daemon for that queue is down. |
| POST | `/tasks` | Dispatch. `202` when created, `200` when `id` already exists (idempotent), `422` on validation errors |
| POST | `/tasks?wait=<ms>` | Dispatch and block until the task finishes (max `MAX_WAIT_MS`). Returns `200` with `state: completed\|failed`. Returns `503` if no worker consumes the queue, and `504` on timeout, in which case an unstarted ephemeral task is dropped. |
| GET | `/tasks/:queue/:id` | State, progress, result, error, attempts |
| DELETE | `/tasks/:queue/:id` | Remove a task that isn't running (`409` if it is) |

```json
POST /tasks
{
  "id": "0198f…",                       // optional, recommended: makes dispatch idempotent (no ":" allowed)
  "queue": "default",
  "name": "example.sleep",
  "payload": { "ms": 2000 },
  "callback_url": "https://core.test/webhooks/queue",
  "meta": { "source": "core" },         // echoed back in every webhook
  "options": { "attempts": 3, "backoff_ms": 5000, "delay_ms": 0, "priority": 1, "ephemeral": false }
}
```

`ephemeral: true` is for reads that return personal data: the result is deleted from Redis as soon as it's produced. Use it with `?wait=`, since that's the only way the caller receives the result. Failed ephemeral tasks are kept for `EPHEMERAL_KEEP_FAILED_SECONDS` for debugging; they hold the request payload but no result.

## Webhooks

The service sends `POST callback_url` with a JSON body:

```json
{ "event": "completed", "task_id": "…", "queue": "default", "name": "example.sleep",
  "attempts_made": 1, "max_attempts": 3, "result": { "slept": 2000 }, "meta": {…}, "timestamp": "…" }
```

- Events: `active` (each attempt), `progress`, `completed`, and `failed`. `failed` is sent only after the last retry fails.
- Header `X-Queue-Signature: t=<unix>,v1=<hex>`, where `v1 = HMAC-SHA256(WEBHOOK_SECRET, "<t>.<raw body>")`. Receivers should reject any timestamp more than 5 minutes old.
- Webhooks come from BullMQ's queue-wide events (Redis streams), so they fire whichever process ran the job. Each one is keyed by its stream event id, so running several gateway instances doesn't send duplicates.
- Deliveries run on an internal `__webhooks` queue, retried with exponential backoff (`WEBHOOK_ATTEMPTS`). A `4xx` other than 408/409/425/429 is treated as permanent. Events can arrive out of order, so receivers should not move a task backwards out of a terminal state.

## Adding an external daemon

1. The daemon runs a BullMQ `Worker` on its own queue name, with the same `QUEUE_PREFIX` and Redis. Its task input is `job.data.payload`.
2. Add the queue and its task names to `src/catalog.ts`.
3. The gateway now validates, relays webhooks for, and reports health on that queue.

## Adding an in-process task type

1. Write a processor in `src/processors/<domain>.ts`:
   ```ts
   export const reports: Record<string, Processor> = {
     "reports.generate": async (payload, job) => {
       await job.updateProgress({ percent: 50 });
       return { url: "…" };            // becomes the webhook `result`
     },
   };
   ```
2. Register it under its queue in `src/processors/index.ts`. To add a new queue, also add it to `QUEUES`.
3. Throw an error to retry the attempt. Throw `UnrecoverableError` (from `bullmq`) to fail with no further retries.
