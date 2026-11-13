/** API testing: POST /api/webhook, GET /api/health and unknown routes, over real HTTP. No auth anywhere. */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload, uniquePayloads } from "../helpers/payloads";

describe("API", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  describe("API: POST /api/webhook", () => {
    it("accepts a single payload with 200 and the documented body", async () => {
      const res = await h.postJson("/api/webhook", uniquePayload());
      assert.strictEqual(res.status, 200);
      assert.deepStrictEqual(res.json, { message: "Webhook received", count: 1 });
      assert.match(res.headers.get("content-type") ?? "", /application\/json/);
    });

    it("needs no credentials of any kind", async () => {
      // The endpoint is live in production and must stay open to Sinch.
      const res = await h.request("/api/webhook", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(uniquePayload()),
      });
      assert.strictEqual(res.status, 200);
    });

    it("accepts an array and reports one job per record", async () => {
      const res = await h.postJson("/api/webhook", uniquePayloads(7));
      assert.strictEqual(res.status, 200);
      assert.deepStrictEqual(res.json, { message: "Webhook received", count: 7 });
    });

    it("accepts an empty array (count 0) without queuing anything", async () => {
      const before = await h.queues.inputQueue.getJobCounts("waiting");
      const res = await h.postJson("/api/webhook", []);
      assert.strictEqual(res.status, 200);
      assert.deepStrictEqual(res.json, { message: "Webhook received", count: 0 });
      const after = await h.queues.inputQueue.getJobCounts("waiting");
      assert.strictEqual(after.waiting, before.waiting);
    });

    it("queues the payload exactly as received (no mutation, extra keys kept)", async () => {
      const payload = uniquePayload({ brand_new_key: { nested: [1, 2, 3] } });
      await h.postJson("/api/webhook", payload);
      const jobs = await h.queues.inputQueue.getJobs(["waiting"]);
      const job = jobs.find((j) => (j.data as { id?: string }).id === payload.id);
      assert.ok(job, "job for the payload is in the queue");
      assert.deepStrictEqual(job.data, payload);
      assert.strictEqual(job.name, "call-log");
    });

    it("parses the JSON body whatever Content-Type the sender uses", async () => {
      const payload = uniquePayload();
      for (const contentType of ["text/plain", "application/x-www-form-urlencoded", "application/octet-stream"]) {
        const res = await h.request("/api/webhook", {
          method: "POST",
          headers: { "content-type": contentType },
          body: JSON.stringify({ ...payload, id: `${payload.id}-${contentType}` }),
        });
        assert.strictEqual(res.status, 200, contentType);
        assert.deepStrictEqual(res.json, { message: "Webhook received", count: 1 }, contentType);
      }
    });

    it("returns an X-Request-Id and echoes a safe one supplied by the caller", async () => {
      const generated = await h.postJson("/api/webhook", uniquePayload());
      assert.match(generated.headers.get("x-request-id") ?? "", /^[0-9a-f-]{36}$/);

      const echoed = await h.postJson("/api/webhook", uniquePayload(), { "x-request-id": "sinch-req_123.abc:9" });
      assert.strictEqual(echoed.headers.get("x-request-id"), "sinch-req_123.abc:9");

      const viaCorrelation = await h.postJson("/api/webhook", uniquePayload(), { "x-correlation-id": "corr-77" });
      assert.strictEqual(viaCorrelation.headers.get("x-request-id"), "corr-77");
    });

    it("tags each queued job with the request id", async () => {
      const payload = uniquePayload();
      await h.postJson("/api/webhook", payload, { "x-request-id": "trace-me-1" });
      const jobs = await h.queues.inputQueue.getJobs(["waiting"]);
      const job = jobs.find((j) => (j.data as { id?: string }).id === payload.id);
      assert.strictEqual(job?.opts.telemetry?.metadata, "trace-me-1");
    });

    it("accepts a body of about 5 MB (the limit is 10 MB)", async () => {
      const res = await h.postJson("/api/webhook", uniquePayload({ call_summary: "x".repeat(5 * 1024 * 1024) }));
      assert.strictEqual(res.status, 200);
    });
  });

  describe("API: GET /api/health", () => {
    it("reports ok while Redis is up", async () => {
      const res = await h.request("/api/health");
      assert.strictEqual(res.status, 200);
      assert.deepStrictEqual(res.json, { status: "ok", redis: "up" });
    });

    it("needs no credentials", async () => {
      const res = await h.request("/api/health", { headers: {} });
      assert.strictEqual(res.status, 200);
    });
  });

  describe("API: routing", () => {
    it("answers unknown paths with a JSON 404", async () => {
      const res = await h.request("/api/does-not-exist");
      assert.strictEqual(res.status, 404);
      assert.deepStrictEqual(res.json, { error: "Not found" });
    });

    it("does not serve the webhook on other methods", async () => {
      for (const method of ["GET", "PUT", "PATCH", "DELETE"]) {
        const res = await h.request("/api/webhook", { method });
        assert.strictEqual(res.status, 404, method);
      }
    });

    it("answers favicon requests quietly", async () => {
      assert.strictEqual((await h.request("/favicon.ico")).status, 204);
    });

    it("keeps the dashboard off unless enabled (it is off in this run)", async () => {
      assert.strictEqual((await h.request("/admin/queues")).status, 404);
    });
  });
});
