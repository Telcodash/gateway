/** Data stored on every BullMQ job created through the API. */
export interface TaskData {
  payload: Record<string, unknown>;
  /** Where lifecycle webhooks for this task are delivered. Optional: fire-and-forget tasks omit it. */
  callbackUrl?: string;
  /** Opaque metadata echoed back in every webhook (e.g. the dispatching service's name). */
  meta?: Record<string, unknown>;
}

export type TaskEvent = "active" | "progress" | "completed" | "failed";

export interface WebhookBody {
  event: TaskEvent;
  task_id: string;
  queue: string;
  name: string;
  attempts_made: number;
  max_attempts: number;
  progress?: unknown;
  result?: unknown;
  error?: string;
  meta?: Record<string, unknown>;
  timestamp: string;
}

export interface WebhookJobData {
  url: string;
  body: WebhookBody;
}
