/**
 * Performance testing: a quick in-process check that catches regressions (a slow handler, a serialised
 * queue write, a memory blow-up). It is NOT a capacity test.
 *
 * Thresholds are deliberately loose so a busy laptop or CI box does not flake; override with
 *   PERF_P95_MS, PERF_MIN_RPS
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { percentile, uniquePayload, uniquePayloads } from "../helpers/payloads";

const P95_MS = Number(process.env.PERF_P95_MS || 1500);
const MIN_RPS = Number(process.env.PERF_MIN_RPS || 50);

/** Runs `total` tasks with at most `limit` in flight; returns each task's latency in ms. */
async function pool(total: number, limit: number, task: (i: number) => Promise<number>): Promise<number[]> {
  const latencies: number[] = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: limit }, async () => {
      while (next < total) {
        const i = next++;
        const started = performance.now();
        const status = await task(i);
        assert.strictEqual(status, 200, `request ${i}`);
        latencies.push(performance.now() - started);
      }
    }),
  );
  return latencies;
}

describe("Performance", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  it("serves 2000 single-record requests at 100 concurrent connections within the latency budget", async (t) => {
    const total = 2000;
    const started = performance.now();
    const latencies = await pool(total, 100, async () => (await h.postJson("/api/webhook", uniquePayload())).status);
    const seconds = (performance.now() - started) / 1000;
    const rps = total / seconds;
    const p50 = percentile(latencies, 50);
    const p95 = percentile(latencies, 95);
    const p99 = percentile(latencies, 99);
    t.diagnostic(`${total} requests in ${seconds.toFixed(2)}s = ${rps.toFixed(0)} req/s; p50 ${p50.toFixed(0)}ms p95 ${p95.toFixed(0)}ms p99 ${p99.toFixed(0)}ms`);
    assert.ok(p95 < P95_MS, `p95 ${p95.toFixed(0)}ms exceeds ${P95_MS}ms`);
    assert.ok(rps > MIN_RPS, `${rps.toFixed(0)} req/s is below ${MIN_RPS}`);
    assert.strictEqual((await h.queues.inputQueue.getJobCounts("waiting")).waiting, total, "every request became a job");
  });

  it("accepts a 5000-record batch quickly", async (t) => {
    const started = performance.now();
    const res = await h.postJson("/api/webhook", uniquePayloads(5000));
    const ms = performance.now() - started;
    t.diagnostic(`5000 records in one request: ${ms.toFixed(0)}ms`);
    assert.strictEqual(res.status, 200);
    assert.strictEqual((res.json as { count: number }).count, 5000);
    assert.ok(ms < 15_000, `${ms.toFixed(0)}ms`);
  });

  it("keeps health checks responsive while the webhook is under load", async (t) => {
    const load = pool(1000, 80, async () => (await h.postJson("/api/webhook", uniquePayload())).status);
    const healthLatencies: number[] = [];
    while (healthLatencies.length < 20) {
      const started = performance.now();
      const res = await h.request("/api/health");
      assert.strictEqual(res.status, 200);
      healthLatencies.push(performance.now() - started);
    }
    await load;
    const worst = Math.max(...healthLatencies);
    t.diagnostic(`health during load: worst ${worst.toFixed(0)}ms`);
    assert.ok(worst < 3000, `health took ${worst.toFixed(0)}ms`);
  });

  it("does not grow memory without bound across many requests", async (t) => {
    const heap = () => {
      global.gc?.();
      return process.memoryUsage().heapUsed / 1024 / 1024;
    };
    await pool(500, 50, async () => (await h.postJson("/api/webhook", uniquePayload())).status);
    const before = heap();
    await pool(3000, 50, async () => (await h.postJson("/api/webhook", uniquePayload())).status);
    const grown = heap() - before;
    t.diagnostic(`heap growth over 3000 requests: ${grown.toFixed(1)} MB`);
    assert.ok(grown < 150, `heap grew ${grown.toFixed(0)} MB`);
  });

  it("drains 1000 queued records through the workers into the CSV", async (t) => {
    await h.queues.redisConnection.flushdb();
    const records = uniquePayloads(1000);
    await h.postJson("/api/webhook", records);
    await h.startWorkers();
    const started = performance.now();
    await h.waitForIdle(90_000);
    const seconds = (performance.now() - started) / 1000;
    const reports = await h.flushReports();
    t.diagnostic(`worker drained 1000 records in ${seconds.toFixed(1)}s (${(1000 / seconds).toFixed(0)} rec/s)`);
    assert.strictEqual(reports.input.length, 1000);
    assert.strictEqual(new Set(reports.input.map((r) => r.id)).size, 1000);
  });
});
