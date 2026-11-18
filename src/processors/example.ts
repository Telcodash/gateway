import { UnrecoverableError } from "bullmq";
import type { Processor } from "./index";

/** Example processors, useful for smoke testing the Core ↔ gateway round trip. */
export const example: Record<string, Processor> = {
  /** Return the payload unchanged. */
  "example.echo": async (payload) => ({ echo: payload }),

  /** Sleep for `ms` (default 2000) while reporting progress in 4 steps. */
  "example.sleep": async (payload, job) => {
    const ms = typeof payload.ms === "number" ? payload.ms : 2_000;
    const steps = 4;

    for (let step = 1; step <= steps; step++) {
      await Bun.sleep(ms / steps);
      await job.updateProgress({ percent: Math.round((step / steps) * 100) });
    }

    return { slept: ms };
  },

  /** Always fail. `unrecoverable: true` skips retries. */
  "example.fail": async (payload) => {
    const message = typeof payload.message === "string" ? payload.message : "Intentional failure";

    throw payload.unrecoverable === true ? new UnrecoverableError(message) : new Error(message);
  },
};
