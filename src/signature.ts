import { createHmac, timingSafeEqual } from "node:crypto";

export const SIGNATURE_HEADER = "X-Queue-Signature";

/**
 * Sign a webhook body. Format (Stripe-style): `t=<unix seconds>,v1=<hex hmac-sha256 of "<t>.<body>">`.
 * Including the timestamp in the signed content lets the receiver reject replays.
 */
export function sign(body: string, secret: string, timestamp = Math.floor(Date.now() / 1000)): string {
  const digest = createHmac("sha256", secret).update(`${timestamp}.${body}`).digest("hex");

  return `t=${timestamp},v1=${digest}`;
}

/** Verify a signature header. Exposed for tests and for any TS service receiving these webhooks. */
export function verify(body: string, header: string, secret: string, toleranceSeconds = 300): boolean {
  const parts = Object.fromEntries(header.split(",").map((part) => part.split("=", 2) as [string, string]));
  const timestamp = Number(parts.t);

  if (!Number.isInteger(timestamp) || !parts.v1) return false;
  if (Math.abs(Date.now() / 1000 - timestamp) > toleranceSeconds) return false;

  const expected = Buffer.from(sign(body, secret, timestamp).split("v1=")[1]!, "hex");
  const given = Buffer.from(parts.v1, "hex");

  return expected.length === given.length && timingSafeEqual(expected, given);
}
