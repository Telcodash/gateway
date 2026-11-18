import { processors } from "./processors";

/**
 * Every queue the gateway accepts tasks for, and the task names allowed on each.
 *
 * - Queues with in-process processors (src/processors) are listed automatically.
 * - External daemons consume their own queue from Redis; list their tasks here.
 *   A dispatch for an unlisted queue/name is rejected with 422 before it reaches Redis.
 */
export const catalog: Record<string, readonly string[]> = {
  ...Object.fromEntries(Object.entries(processors).map(([queue, handlers]) => [queue, Object.keys(handlers)])),

  /** ../customer-daemon. (`customers.refs` is internal: daemon-to-daemon only, so not listed.) */
  customers: ["customers.search", "customers.get", "customers.get_many"],

  /** ../billing-daemon. (`billing.run`, `billing.daily_run` and `accounts.refs` are ops/scheduler/internal, so not listed.) */
  billing: ["accounts.get", "accounts.for_customer", "accounts.for_customers", "invoices.list", "billing.runs"],

  /** ../msisdn-daemon. (`numbers.daily` and `numbers.assignments` are scheduler/internal, so not listed.) */
  msisdn: ["numbers.for_accounts", "numbers.get", "numbers.available", "numbers.stats", "numbers.rename"],

  /** ../subscription-daemon. Tariff writes are admin-only in Core. */
  subscriptions: ["tariffs.list", "tariffs.create", "tariffs.update", "lines.for_accounts"],

  /** ../usage-daemon. (`usage.totals` is internal: billing rates it for out-of-bundle extras.) */
  usage: ["usage.for_number"],
};

export const queueNames = (): string[] => Object.keys(catalog);

export function isAllowed(queue: string, name: string): boolean {
  return catalog[queue]?.includes(name) ?? false;
}
