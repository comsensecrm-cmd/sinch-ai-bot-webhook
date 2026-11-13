/**
 * Reliability testing: retries, recovery and fault tolerance. Faults are injected by replacing a
 * method on the real object for one test (and restoring it), so everything else stays real.
 */

import { describe, it, before, after, afterEach } from "node:test";
import assert from "node:assert";
import { startHarness, waitFor, type Harness } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";

describe("Reliability", () => {
  let h: Harness;
  let inputCsvWriter: typeof import("../../src/csv/reportWriters").inputCsvWriter;
  const restores: Array<() => void> = [];

  /** Replaces obj[key] for the current test only. */
  function patch<T extends object, K extends keyof T>(obj: T, key: K, replacement: T[K]): void {
    const original = obj[key];
    obj[key] = replacement;
    restores.push(() => {
      obj[key] = original;
    });
  }

  before(async () => {
    h = await startHarness();
    ({ inputCsvWriter } = await import("../../src/csv/reportWriters"));
  });
  afterEach(async () => {
    while (restores.length) restores.pop()!();
    await h.stopWorkers();
    await h.queues.redisConnection.flushdb();
  });
  after(async () => {
    await h.stop();
  });

  describe("webhook when the queue is unavailable", () => {
    it("answers 500 with a generic body (and no internals), then recovers", async () => {
      const original = h.queues.inputQueue.addBulk.bind(h.queues.inputQueue);
      patch(h.queues.inputQueue, "addBulk", (async () => {
        throw new Error("connect ECONNREFUSED redis://:s3cret@10.0.0.5:4112");
      }) as typeof h.queues.inputQueue.addBulk);

      const failed = await h.postJson("/api/webhook", uniquePayload());
      assert.strictEqual(failed.status, 500);
      assert.deepStrictEqual(failed.json, { error: "Failed to queue webhook" });
      assert.doesNotMatch(failed.text, /s3cret|ECONNREFUSED|10\.0\.0\.5/);

      restores.pop()!();
      patch(h.queues.inputQueue, "addBulk", original);
      const recovered = await h.postJson("/api/webhook", uniquePayload());
      assert.strictEqual(recovered.status, 200, "the next request succeeds with no restart");
    });

    it("keeps answering other requests while enqueueing fails", async () => {
      patch(h.queues.inputQueue, "addBulk", (async () => {
        throw new Error("boom");
      }) as typeof h.queues.inputQueue.addBulk);
      const results = await Promise.all([
        h.postJson("/api/webhook", uniquePayload()),
        h.request("/api/health"),
        h.request("/api/nope"),
      ]);
      assert.deepStrictEqual(results.map((r) => r.status), [500, 200, 404]);
    });

    it("reports /api/health as degraded (503) when Redis does not answer, and recovers", async () => {
      patch(h.queues.redisConnection, "ping", (async () => {
        throw new Error("Connection is closed.");
      }) as typeof h.queues.redisConnection.ping);
      const down = await h.request("/api/health");
      assert.strictEqual(down.status, 503);
      assert.deepStrictEqual(down.json, { status: "degraded", redis: "down" });

      restores.pop()!();
      assert.strictEqual((await h.request("/api/health")).status, 200);
    });
  });

  describe("input worker retries", () => {
    it("retries a failed CSV write and writes the record exactly once", async () => {
      let calls = 0;
      const real = inputCsvWriter.writeRecord.bind(inputCsvWriter);
      patch(inputCsvWriter, "writeRecord", async (row) => {
        calls += 1;
        if (calls <= 2) throw new Error("disk hiccup");
        return real(row);
      });

      const payload = uniquePayload();
      await h.startWorkers();
      await h.postJson("/api/webhook", payload);
      await h.waitForIdle();

      assert.strictEqual(calls, 3, "two failures, then a success on the last allowed attempt");
      const reports = await h.flushReports();
      assert.strictEqual(reports.input.filter((r) => r.id === payload.id).length, 1);
      assert.strictEqual(reports.dlq.length, 0, "nothing reaches the DLQ when a retry succeeds");
    });

    it("moves the record to the DLQ when every attempt fails, and keeps the payload", async () => {
      let calls = 0;
      patch(inputCsvWriter, "writeRecord", async () => {
        calls += 1;
        throw new Error("disk full");
      });

      const payload = uniquePayload({ coms_id: "COMS-DLQ-1" });
      await h.startWorkers();
      await h.postJson("/api/webhook", payload);
      await h.waitForIdle();

      assert.strictEqual(calls, 3, "QUEUE_MAX_ATTEMPTS attempts, no more");
      restores.pop()!();
      const reports = await h.flushReports();
      assert.strictEqual(reports.input.length, 0);
      assert.strictEqual(reports.dlq.length, 1);
      assert.match(reports.dlq[0].Reason, /CSV write failed: disk full/);
      assert.strictEqual(reports.dlq[0].Attempts, "3");
      assert.strictEqual(reports.dlq[0].coms_id, "COMS-DLQ-1");
      assert.deepStrictEqual(JSON.parse(reports.dlq[0].RawPayload), payload);
    });

    it("lets a DLQ'd record be replayed afterwards (its claim is released, not stuck as 'in progress')", async () => {
      const real = inputCsvWriter.writeRecord.bind(inputCsvWriter);
      patch(inputCsvWriter, "writeRecord", async () => {
        throw new Error("disk full");
      });
      const payload = uniquePayload();
      await h.startWorkers();
      await h.postJson("/api/webhook", payload);
      await h.waitForIdle();

      restores.pop()!();
      patch(inputCsvWriter, "writeRecord", real);
      await h.postJson("/api/webhook", payload);
      await h.waitForIdle();

      const reports = await h.flushReports();
      assert.strictEqual(reports.input.filter((r) => r.id === payload.id).length, 1, "the replay is written");
    });
  });

  describe("isolation and durability", () => {
    it("is not blocked by a poison record: later records still flow", async () => {
      const poison = { call_duration: "x".repeat(10), call_id: 123 };
      const goods = [uniquePayload(), uniquePayload(), uniquePayload()];
      await h.startWorkers();
      await h.postJson("/api/webhook", [poison, ...goods]);
      await h.waitForIdle();

      const reports = await h.flushReports();
      assert.strictEqual(reports.input.length, 3);
      assert.strictEqual(reports.failure.length, 1);
    });

    it("keeps records received while no worker is running, and processes them once one starts", async () => {
      const payloads = Array.from({ length: 25 }, () => uniquePayload());
      const res = await h.postJson("/api/webhook", payloads);
      assert.strictEqual(res.status, 200);
      assert.strictEqual((await h.queues.inputQueue.getJobCounts("waiting")).waiting, 25);

      await h.startWorkers(); // e.g. the app restarting
      await h.waitForIdle();
      const reports = await h.flushReports();
      assert.strictEqual(reports.input.length, 25);
    });

    it("lets a restarted worker pick up where the previous one stopped", async () => {
      await h.startWorkers();
      await h.postJson("/api/webhook", Array.from({ length: 5 }, () => uniquePayload()));
      await h.waitForIdle();
      await h.stopWorkers();

      await h.postJson("/api/webhook", Array.from({ length: 5 }, () => uniquePayload()));
      await h.startWorkers();
      await h.waitForIdle();
      assert.strictEqual((await h.flushReports()).input.length, 10);
    });

    it("skips a record whose id is claimed by another in-flight job (cross-instance de-duplication)", async () => {
      const payload = uniquePayload();
      await h.queues.redisConnection.set(
        `sinch-ai-bot-webhook:csv-written:${payload.id}`,
        "pending:some-other-instance-job",
        "EX",
        600,
      );
      await h.startWorkers();
      await h.postJson("/api/webhook", payload);
      await h.waitForIdle();
      assert.strictEqual((await h.flushReports()).input.length, 0);
    });

    it("marks a processed record as written, which is the key other instances de-duplicate on", async () => {
      const payload = uniquePayload();
      await h.startWorkers();
      await h.postJson("/api/webhook", payload);
      await waitFor(
        async () => (await h.queues.redisConnection.get(`sinch-ai-bot-webhook:csv-written:${payload.id}`)) === "1",
        { label: "the record to be marked written" },
      );
      assert.strictEqual((await h.flushReports()).input.length, 1);
    });
  });
});
