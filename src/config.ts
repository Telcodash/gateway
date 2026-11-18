import { z } from "zod";

const csv = z
  .string()
  .transform((value) =>
    value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean),
  );

const EnvSchema = z.object({
  /** Which parts of the service to run in this process. */
  MODE: z.enum(["all", "api", "worker"]).default("all"),
  HOST: z.string().default("0.0.0.0"),
  PORT: z.coerce.number().int().nonnegative().default(3100),

  REDIS_URL: z.string().default("redis://127.0.0.1:6379"),
  /** Key prefix for every BullMQ key in Redis. */
  QUEUE_PREFIX: z.string().default("queue"),

  /** Concurrency for the in-process workers (queues with processors in src/processors). */
  WORKER_CONCURRENCY: z.coerce.number().int().positive().default(5),
  /** Upper bound for `POST /tasks?wait=<ms>`. */
  MAX_WAIT_MS: z.coerce.number().int().positive().default(30_000),
  /** Failed ephemeral tasks are kept this long for debugging (they contain the request payload). */
  EPHEMERAL_KEEP_FAILED_SECONDS: z.coerce.number().int().nonnegative().default(600),

  /** Bearer tokens accepted by the HTTP API (comma separated, one per client service). */
  API_TOKENS: csv.pipe(z.array(z.string().min(16)).min(1, "API_TOKENS must contain at least one token")),

  /** Shared secret used to HMAC-sign outbound webhooks. */
  WEBHOOK_SECRET: z.string().min(16),
  WEBHOOK_TIMEOUT_MS: z.coerce.number().int().positive().default(10_000),
  WEBHOOK_ATTEMPTS: z.coerce.number().int().positive().default(8),

  /** Defaults applied to every task unless overridden per dispatch. */
  DEFAULT_ATTEMPTS: z.coerce.number().int().positive().default(3),
  DEFAULT_BACKOFF_MS: z.coerce.number().int().nonnegative().default(5_000),
  KEEP_COMPLETED_SECONDS: z.coerce.number().int().positive().default(60 * 60 * 24),
  KEEP_FAILED_SECONDS: z.coerce.number().int().positive().default(60 * 60 * 24 * 7),
});

export type Config = z.infer<typeof EnvSchema>;

export function loadConfig(env: Record<string, string | undefined> = process.env): Config {
  const parsed = EnvSchema.safeParse(env);

  if (!parsed.success) {
    const issues = parsed.error.issues.map((issue) => `  - ${issue.path.join(".")}: ${issue.message}`).join("\n");
    throw new Error(`Invalid environment configuration:\n${issues}`);
  }

  return parsed.data;
}

/** Name of the internal queue used to deliver webhooks reliably. */
export const WEBHOOK_QUEUE = "__webhooks";
