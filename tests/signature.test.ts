import { describe, expect, test } from "bun:test";
import { sign, verify } from "../src/signature";

const secret = "test-secret-at-least-16";

describe("signature", () => {
  test("round trips", () => {
    const body = JSON.stringify({ hello: "world" });

    expect(verify(body, sign(body, secret), secret)).toBe(true);
  });

  test("rejects tampered body, wrong secret and stale timestamps", () => {
    const body = '{"a":1}';
    const header = sign(body, secret);

    expect(verify('{"a":2}', header, secret)).toBe(false);
    expect(verify(body, header, "another-secret-123456")).toBe(false);
    expect(verify(body, sign(body, secret, Math.floor(Date.now() / 1000) - 600), secret)).toBe(false);
    expect(verify(body, "garbage", secret)).toBe(false);
  });

  test("matches the PHP implementation's format", () => {
    // hash_hmac('sha256', '1700000000.{}', 'test-secret-at-least-16')
    expect(sign("{}", secret, 1_700_000_000)).toMatch(/^t=1700000000,v1=[0-9a-f]{64}$/);
  });
});
