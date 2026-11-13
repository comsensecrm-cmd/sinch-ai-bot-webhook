/** Webhook testing: the kinds of requests Sinch actually sends to /api/webhook. */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import zlib from "zlib";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload, uniquePayloads } from "../helpers/payloads";
import { samplePayload } from "../samplePayload";

describe("Webhook: incoming Sinch events", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  const ok = { message: "Webhook received", count: 1 };

  it("acknowledges the full sample call-log payload", async () => {
    const res = await h.postJson("/api/webhook", samplePayload());
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, ok);
  });

  it("acknowledges a payload whose optional fields are all null", async () => {
    const nulls = Object.fromEntries(Object.keys(samplePayload()).map((k) => [k, null]));
    const res = await h.postJson("/api/webhook", { ...nulls, call_id: "only-call-id" });
    assert.deepStrictEqual(res.json, ok);
  });

  it("acknowledges only a call_id", async () => {
    assert.deepStrictEqual((await h.postJson("/api/webhook", { call_id: "x" })).json, ok);
  });

  it("acknowledges (and queues) an invalid record: validation is the worker's job, not the door's", async () => {
    const res = await h.postJson("/api/webhook", { call_duration: "not a number" });
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, ok);
  });

  it("keeps unicode, emoji, quotes, commas and line breaks intact", async () => {
    const payload = uniquePayload({
      contact_name: "रमेश 🏍️ Müller \"Ace\", Jr.",
      call_summary: "Line one\nLine two\r\nLine three, with, commas and \"quotes\" 😀",
    });
    await h.postJson("/api/webhook", payload);
    const job = (await h.queues.inputQueue.getJobs(["waiting"])).find(
      (j) => (j.data as { id?: string }).id === payload.id,
    );
    assert.deepStrictEqual(job?.data, payload);
  });

  it("accepts a batch of 1000 records in one request", async () => {
    const res = await h.postJson("/api/webhook", uniquePayloads(1000));
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, { message: "Webhook received", count: 1000 });
  });

  it("queues a redelivered payload again (de-duplication is the worker's job)", async () => {
    const payload = uniquePayload();
    const first = await h.postJson("/api/webhook", payload);
    const second = await h.postJson("/api/webhook", payload);
    assert.deepStrictEqual(first.json, ok);
    assert.deepStrictEqual(second.json, ok);
  });

  it("accepts a charset in the Content-Type", async () => {
    const res = await h.request("/api/webhook", {
      method: "POST",
      headers: { "content-type": "application/json; charset=utf-8" },
      body: JSON.stringify(uniquePayload()),
    });
    assert.deepStrictEqual(res.json, ok);
  });

  it("accepts a gzip-compressed body", async () => {
    const res = await h.request("/api/webhook", {
      method: "POST",
      headers: { "content-type": "application/json", "content-encoding": "gzip" },
      body: zlib.gzipSync(JSON.stringify(uniquePayload())),
    });
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, ok);
  });

  it("ignores query strings and tolerates a trailing slash", async () => {
    for (const url of ["/api/webhook?source=sinch&x=1", "/api/webhook/"]) {
      const res = await h.postJson(url, uniquePayload());
      assert.strictEqual(res.status, 200, url);
    }
  });

  it("handles many simultaneous deliveries with no lost acknowledgements", async () => {
    const results = await Promise.all(Array.from({ length: 100 }, () => h.postJson("/api/webhook", uniquePayload())));
    assert.ok(results.every((r) => r.status === 200 && (r.json as { count: number }).count === 1));
  });
});
