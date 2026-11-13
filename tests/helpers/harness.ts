/**
 * Shared harness for the Redis-backed suites. Requires tests/setupRedis.ts to be preloaded (the npm
 * scripts do this), which points the app at the throwaway test Redis and a temp CSV directory.
 *
 * src/ is imported lazily, after the Redis probe, so a missing Redis fails with one clear message
 * instead of leaving connection retries running in the background.
 */

import fs from "fs";
import path from "path";
import type { Server } from "http";
import type { AddressInfo } from "net";
import type { Worker } from "bullmq";
import { Redis } from "ioredis";

type Queues = typeof import("../../src/queue/queues");

export const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

export async function waitFor<T>(
  check: () => Promise<T | false | undefined | null> | T | false | undefined | null,
  { timeoutMs = 15_000, intervalMs = 50, label = "condition" } = {},
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting for ${label}`);
    }
    await sleep(intervalMs);
  }
}

/** Fails fast, and without starting the app, when the test Redis is not running. */
async function assertTestRedisRunning(): Promise<void> {
  const url = process.env.REDIS_URL;
  const probe = new Redis(url as string, {
    lazyConnect: true,
    retryStrategy: () => null,
    connectTimeout: 2_000,
    maxRetriesPerRequest: 0,
  });
  probe.on("error", () => undefined);
  try {
    await probe.connect();
    await probe.ping();
  } catch {
    throw new Error(
      `Test Redis is not reachable at ${url}. Start it with "npm run test:redis:up" ` +
        `(or set TEST_REDIS_URL to another local throwaway Redis).`,
    );
  } finally {
    probe.disconnect();
  }
}

export interface Harness {
  baseUrl: string;
  queues: Queues;
  /** The unbound `fetch` against the running app. */
  request(
    path: string,
    init?: RequestInit,
  ): Promise<{ status: number; headers: Headers; text: string; json: unknown }>;
  postJson(path: string, body: unknown, headers?: Record<string, string>): ReturnType<Harness["request"]>;
  startWorkers(): Promise<void>;
  stopWorkers(): Promise<void>;
  /** Waits until the input, failure and DLQ queues have nothing waiting, active or delayed. */
  waitForIdle(timeoutMs?: number): Promise<void>;
  /** Closes the CSV writers (which hands the open files over) and returns, then deletes, the rows written since the last call. */
  flushReports(): Promise<Reports>;
  stop(): Promise<void>;
}

export type CsvRecordMap = Record<string, string>;
export interface Reports {
  input: CsvRecordMap[];
  failure: CsvRecordMap[];
  dlq: CsvRecordMap[];
}

export async function startHarness(
  options: { listen?: boolean } = {},
): Promise<Harness> {
  await assertTestRedisRunning();

  const queues = await import("../../src/queue/queues");
  await queues.redisConnection.flushdb();

  let server: Server | undefined;
  let baseUrl = "";
  if (options.listen !== false) {
    const { createApp } = await import("../../src/app");
    server = await new Promise<Server>((resolve, reject) => {
      const s = createApp().listen(0, "127.0.0.1", () => resolve(s));
      s.once("error", reject);
    });
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  }

  let workers: Worker[] = [];

  const request: Harness["request"] = async (urlPath, init) => {
    const res = await fetch(baseUrl + urlPath, init);
    const text = await res.text();
    let json: unknown;
    try {
      json = JSON.parse(text);
    } catch {
      json = undefined;
    }
    return { status: res.status, headers: res.headers, text, json };
  };

  const waitForIdle: Harness["waitForIdle"] = async (timeoutMs = 20_000) => {
    const all = [queues.inputQueue, queues.failureQueue, queues.dlqQueue];
    let quiet = 0;
    await waitFor(
      async () => {
        let busy = 0;
        for (const q of all) {
          const c = await q.getJobCounts("waiting", "active", "delayed", "prioritized");
          busy += c.waiting + c.active + c.delayed + c.prioritized;
        }
        quiet = busy === 0 ? quiet + 1 : 0;
        // Three quiet polls in a row: a job moving between queues is never seen as "gone".
        return quiet >= 3;
      },
      { timeoutMs, intervalMs: 40, label: "the queues to drain" },
    );
  };

  const readReport = (name: string): CsvRecordMap[] => {
    const dir = path.join(process.env.CSV_OUTPUT_DIR as string, name);
    if (!fs.existsSync(dir)) return [];
    const { parseCsv } = require("../../src/reprocess/csv") as typeof import("../../src/reprocess/csv");
    const rows: CsvRecordMap[] = [];
    for (const file of fs.readdirSync(dir).filter((f) => f.endsWith(".csv")).sort()) {
      const [header, ...body] = parseCsv(fs.readFileSync(path.join(dir, file), "utf8"));
      for (const cells of body) {
        rows.push(Object.fromEntries(header.map((h, i) => [h, cells[i] ?? ""])));
      }
      // Consumed: the next flush returns only rows written after this one.
      fs.rmSync(path.join(dir, file), { force: true });
    }
    return rows;
  };

  return {
    baseUrl,
    queues,
    request,
    postJson: (urlPath, body, headers = {}) =>
      request(urlPath, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(body),
      }),

    async startWorkers() {
      const { createInputWorker } = await import("../../src/queue/inputWorker");
      const { createDlqWorker, createFailureWorker } = await import("../../src/queue/reportWorkers");
      workers = [createInputWorker(), createFailureWorker(), createDlqWorker()];
      await Promise.all(workers.map((w) => w.waitUntilReady()));
    },
    async stopWorkers() {
      await Promise.allSettled(workers.map((w) => w.close()));
      workers = [];
    },
    waitForIdle,

    async flushReports() {
      const { closeReportWriters } = await import("../../src/csv/reportWriters");
      await closeReportWriters();
      return { input: readReport("input"), failure: readReport("failure"), dlq: readReport("dlq") };
    },

    async stop() {
      if (server) {
        await new Promise<void>((resolve) => {
          server!.close(() => resolve());
          server!.closeAllConnections();
        });
      }
      await Promise.allSettled(workers.map((w) => w.close()));
      const { closeReportWriters } = await import("../../src/csv/reportWriters");
      const { sftpUploader } = await import("../../src/sftp/sftpUploader");
      await closeReportWriters();
      await sftpUploader.drain(2_000);
      await Promise.allSettled([
        queues.inputQueue.close(),
        queues.failureQueue.close(),
        queues.dlqQueue.close(),
      ]);
      queues.redisConnection.disconnect();
      const { closeLogger } = await import("../../src/utils/logger");
      await closeLogger().catch(() => undefined);
    },
  };
}
