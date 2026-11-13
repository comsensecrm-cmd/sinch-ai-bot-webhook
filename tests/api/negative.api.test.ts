/** Negative testing: invalid input and malformed requests must be rejected cleanly, never crash the server. */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";

describe("Negative: bad requests", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  const post = (body: string, contentType = "application/json") =>
    h.request("/api/webhook", { method: "POST", headers: { "content-type": contentType }, body });

  it("rejects malformed JSON with 400 and a generic error", async () => {
    for (const body of ['{"id": ', "{'id': 1}", "{id: 1}", '{"a":1,}', "not json", '{"a":1}}']) {
      const res = await post(body);
      assert.strictEqual(res.status, 400, body);
      assert.deepStrictEqual(res.json, { error: "Invalid request body" }, body);
    }
  });

  it("rejects malformed JSON whatever the Content-Type", async () => {
    for (const type of ["text/plain", "application/xml", "application/json; charset=utf-8"]) {
      const res = await post("{broken", type);
      assert.strictEqual(res.status, 400, type);
    }
  });

  it("rejects JSON that is not an object or array", async () => {
    for (const body of ['"a string"', "123", "true", "null"]) {
      const res = await post(body);
      assert.strictEqual(res.status, 400, body);
    }
  });

  it("rejects a body over 10 MB with 413 and keeps serving", async () => {
    const res = await post(JSON.stringify({ call_id: "x", call_summary: "y".repeat(11 * 1024 * 1024) }));
    assert.strictEqual(res.status, 413);
    assert.deepStrictEqual(res.json, { error: "Invalid request body" });
    const next = await h.postJson("/api/webhook", uniquePayload());
    assert.strictEqual(next.status, 200);
  });

  it("rejects an unsupported charset without crashing", async () => {
    const res = await post(JSON.stringify(uniquePayload()), "application/json; charset=utf-16");
    assert.ok(res.status >= 400 && res.status < 500, `status ${res.status}`);
  });

  it("rejects a bad Content-Encoding", async () => {
    const res = await h.request("/api/webhook", {
      method: "POST",
      headers: { "content-type": "application/json", "content-encoding": "gzip" },
      body: "this is not gzip",
    });
    assert.ok(res.status >= 400 && res.status < 500, `status ${res.status}`);
  });

  it("never leaks internals in error bodies", async () => {
    const res = await post('{"a": <script>');
    assert.deepStrictEqual(Object.keys(res.json as object), ["error"]);
    assert.doesNotMatch(res.text, /SyntaxError|node_modules|at .*\(|\.ts|\.js:/);
  });

  it("answers an empty body with 200 and queues an empty record (validation happens in the worker)", async () => {
    // Documents current behaviour: ingestion is permissive and the worker routes the record to the failure queue.
    const res = await h.request("/api/webhook", { method: "POST" });
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, { message: "Webhook received", count: 1 });
  });

  it("survives a burst of bad requests and still accepts good ones", async () => {
    await Promise.all(Array.from({ length: 50 }, () => post("{oops")));
    const res = await h.postJson("/api/webhook", uniquePayload());
    assert.strictEqual(res.status, 200);
  });
});

describe("Negative: invalid records are accepted at the door and rejected by validation", () => {
  // The webhook acknowledges first and validates in the worker (so Sinch never retries bad data forever).
  // The worker-side outcome is asserted in tests/e2e; here the validator rules are checked per field.
  const { validatePayload } = require("../../src/validation/payloadValidator") as typeof import("../../src/validation/payloadValidator");

  const cases: Array<[string, Record<string, unknown>, RegExp]> = [
    ["missing call_id", { call_id: undefined }, /call_id is required/],
    ["null call_id", { call_id: null }, /call_id is required/],
    ["blank call_id", { call_id: "   " }, /call_id must not be empty/],
    ["numeric call_id", { call_id: 12345 }, /call_id must be a string/],
    ["string duration seconds", { call_duration: "165" }, /call_duration must be a number/],
    ["boolean as string", { call_connected: "true" }, /call_connected must be a boolean/],
    ["unparseable date", { created_at: "yesterday-ish" }, /created_at must be a date string/],
    ["numeric date", { created_at: 1779108787 }, /created_at must be a date string/],
    ["object for array", { all_dispositions: {} }, /all_dispositions must be an array/],
    ["text over max length", { bot_id: "x".repeat(51) }, /bot_id exceeds 50 characters/],
    ["object for text", { contact_name: { first: "A" } }, /contact_name must be a string/],
    ["array for text", { contact_name: ["A"] }, /contact_name must be a string/],
  ];

  for (const [name, overrides, expected] of cases) {
    it(`flags ${name}`, () => {
      const payload = { ...uniquePayload(), ...overrides };
      if (overrides.call_id === undefined && "call_id" in overrides) delete payload.call_id;
      const result = validatePayload(payload);
      assert.strictEqual(result.valid, false);
      assert.ok(result.errors.some((e: string) => expected.test(e)), result.errors.join("; "));
    });
  }

  it("reports every problem in one pass, not just the first", () => {
    const result = validatePayload({ call_id: 1, call_duration: "x", created_at: "nope" });
    assert.strictEqual(result.errors.length, 3);
  });

  it("rejects NaN and Infinity as numbers", () => {
    for (const bad of [NaN, Infinity, -Infinity]) {
      assert.strictEqual(validatePayload({ ...uniquePayload(), call_duration: bad }).valid, false);
    }
  });

  it("accepts values exactly at the maximum length", () => {
    assert.strictEqual(validatePayload({ ...uniquePayload(), bot_id: "x".repeat(50) }).valid, true);
  });
});
