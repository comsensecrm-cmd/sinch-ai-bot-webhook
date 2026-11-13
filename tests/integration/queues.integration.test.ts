/** Integration testing: the app's own modules against a real Redis (BullMQ queues, claims, CSV writers). */

import { describe, it, before, after, beforeEach } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload, uniquePayloads } from "../helpers/payloads";

describe("Integration: queues and Redis", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness({ listen: false });
  });
  beforeEach(async () => {
    await h.stopWorkers();
    await h.queues.redisConnection.flushdb();
  });
  after(async () => {
    await h.stop();
  });

  describe("producer", () => {
    it("adds one job per record, in order, and returns their ids", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      const records = uniquePayloads(5);
      const ids = await enqueueRecords(records, "req-1");
      assert.strictEqual(ids.length, 5);
      assert.strictEqual(new Set(ids).size, 5, "ids are unique");
      const jobs = await Promise.all(ids.map((id) => h.queues.inputQueue.getJob(id)));
      assert.deepStrictEqual(jobs.map((j) => j?.data), records);
    });

    it("does nothing for an empty list", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      assert.deepStrictEqual(await enqueueRecords([]), []);
      assert.strictEqual((await h.queues.inputQueue.getJobCounts("waiting")).waiting, 0);
    });

    it("tags jobs with the correlation id, and falls back to the job id without one", async () => {
      const { enqueueRecords, correlationIdOf } = await import("../../src/queue/producer");
      const [withId] = await enqueueRecords([uniquePayload()], "corr-9");
      const [without] = await enqueueRecords([uniquePayload()]);
      assert.strictEqual(correlationIdOf((await h.queues.inputQueue.getJob(withId))!), "corr-9");
      assert.strictEqual(correlationIdOf((await h.queues.inputQueue.getJob(without))!), without);
    });

    it("stores any JSON value as received, including non-objects (validation is later)", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      const odd = [null, "text", 42, [], { a: { b: [1, null, "x"] } }];
      const ids = await enqueueRecords(odd);
      const stored = await Promise.all(ids.map(async (id) => (await h.queues.inputQueue.getJob(id))?.data));
      assert.deepStrictEqual(stored, odd);
    });

    it("handles a 5000-record batch", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      const ids = await enqueueRecords(uniquePayloads(5000));
      assert.strictEqual(ids.length, 5000);
      assert.strictEqual((await h.queues.inputQueue.getJobCounts("waiting")).waiting, 5000);
    });
  });

  describe("input worker with Redis claims", () => {
    it("writes each record once when two app instances consume the same queue", async () => {
      const { createInputWorker } = await import("../../src/queue/inputWorker");
      const { createFailureWorker, createDlqWorker } = await import("../../src/queue/reportWorkers");
      const { enqueueRecords } = await import("../../src/queue/producer");

      // Two input consumers = two instances. The duplicates are queued up front so they run concurrently.
      const unique = uniquePayloads(60);
      const duplicates = unique.slice(0, 30).concat(unique.slice(0, 30));
      await enqueueRecords([...unique, ...duplicates]);
      const workers = [createInputWorker(), createInputWorker(), createFailureWorker(), createDlqWorker()];
      await Promise.all(workers.map((w) => w.waitUntilReady()));
      try {
        await h.waitForIdle();
      } finally {
        await Promise.all(workers.map((w) => w.close()));
      }
      const reports = await h.flushReports();
      const counts = new Map<string, number>();
      for (const row of reports.input) counts.set(row.id, (counts.get(row.id) ?? 0) + 1);
      assert.strictEqual(counts.size, 60, "every distinct record is written");
      assert.ok([...counts.values()].every((n) => n === 1), "and none is written twice");
    });

    it("routes an invalid record to the failure queue and never to the input CSV", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      await h.startWorkers();
      await enqueueRecords([{ id: "bad-1", call_id: "bad-1", call_connected: "yes" }]);
      await h.waitForIdle();
      const reports = await h.flushReports();
      assert.strictEqual(reports.input.length, 0);
      assert.strictEqual(reports.failure.length, 1);
      assert.match(reports.failure[0].Reason, /call_connected must be a boolean/);
    });

    it("handles non-object job data (null, string, number, array) via the failure queue", async () => {
      const { enqueueRecords } = await import("../../src/queue/producer");
      await h.startWorkers();
      await enqueueRecords([null, "text", 42, []]);
      await h.waitForIdle();
      const reports = await h.flushReports();
      assert.strictEqual(reports.failure.length, 4);
      assert.ok(reports.failure.every((r) => /Payload must be a JSON object/.test(r.Reason)));
    });
  });

  describe("report workers", () => {
    it("turn failure and DLQ jobs into CSV rows", async () => {
      const { routeToFailureQueue, routeToDlq } = await import("../../src/queue/routing");
      await h.startWorkers();
      await routeToFailureQueue("f-1", { coms_id: "C-F", call_id: "x" }, ["a", "b"], { correlationId: "r-1" });
      await routeToDlq("d-1", { coms_id: "C-D", call_id: "y" }, "kaput", 3, { correlationId: "r-2" });
      await h.waitForIdle();
      const reports = await h.flushReports();
      assert.deepStrictEqual(
        reports.failure.map((r) => [r.JobId, r.Reason, r.coms_id]),
        [["f-1", "a; b", "C-F"]],
      );
      assert.deepStrictEqual(
        reports.dlq.map((r) => [r.JobId, r.Reason, r.Attempts, r.coms_id]),
        [["d-1", "kaput", "3", "C-D"]],
      );
    });
  });

  describe("Redis", () => {
    it("answers PING and persists keys for the claim flow", async () => {
      const redis = h.queues.redisConnection;
      assert.strictEqual(await redis.ping(), "PONG");
      assert.strictEqual(await redis.set("k", "v", "EX", 60, "NX"), "OK");
      assert.strictEqual(await redis.set("k", "other", "EX", 60, "NX"), null, "NX refuses to overwrite");
    });
  });
});
