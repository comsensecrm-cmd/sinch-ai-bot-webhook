/** Entry point: webhook API and the three queue consumers. */

import type { Server } from "http";
import type { Worker } from "bullmq";
import config from "./config/env";
import logger, { checkCloudWatchConnection, closeLogger } from "./utils/logger";
import { EVENTS } from "./utils/logEvents";
import { createApp } from "./app";
import { closeReportWriters, recoverPendingReports } from "./csv/reportWriters";
import { sftpUploader } from "./sftp/sftpUploader";
import {
  dlqQueue,
  failureQueue,
  inputQueue,
  redisConnection,
} from "./queue/queues";
import { createInputWorker } from "./queue/inputWorker";
import { createDlqWorker, createFailureWorker } from "./queue/reportWorkers";

const app = createApp();

let workers: Worker[] = [];

const STARTUP_CHECK_TIMEOUT_MS = 15_000;

function withTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error(`${label} timed out after ${STARTUP_CHECK_TIMEOUT_MS}ms`),
        ),
      STARTUP_CHECK_TIMEOUT_MS,
    );
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

async function checkRedis(): Promise<void> {
  const pong = await withTimeout(redisConnection.ping(), "Redis connection");
  if (pong !== "PONG") throw new Error(`Unexpected Redis reply: ${pong}`);
  logger.info("Redis connection verified", {
    component: "server.startup",
    event: EVENTS.SERVER_STARTUP_REDIS_CONNECTED,
    outcome: "success",
    tags: ["startup", "redis", "connected"],
  });
}

async function checkCloudWatch(): Promise<void> {
  const status = await withTimeout(
    checkCloudWatchConnection(),
    "AWS CloudWatch connection",
  );
  if (!status.configured) {
    logger.info(
      "AWS CloudWatch logging not configured (AWS_LOG_GROUP / AWS_LOG_STREAM empty) - skipped",
      {
        component: "server.startup",
        event: EVENTS.SERVER_STARTUP_CLOUDWATCH_SKIPPED,
        outcome: "skipped",
        tags: ["startup", "cloudwatch", "skipped"],
      },
    );
    return;
  }
  if (!status.connected) {
    throw new Error(
      `AWS CloudWatch connection failed: ${status.error ?? "unknown error"}`,
    );
  }
  logger.info("AWS CloudWatch connection verified", {
    component: "server.startup",
    event: EVENTS.SERVER_STARTUP_CLOUDWATCH_CONNECTED,
    outcome: "success",
    log_group: config.aws.logGroup,
    log_stream: config.aws.logStream,
    tags: ["startup", "cloudwatch", "connected"],
  });
}

let server: Server | undefined;

function listen(): Promise<void> {
  return new Promise((resolve, reject) => {
    const s = app.listen(config.port, config.host, () => resolve());
    s.once("error", reject);
    server = s;
  });
}

/** Stops accepting connections and waits for in-flight requests to finish. */
function closeServer(): Promise<void> {
  const s = server;
  if (!s) return Promise.resolve();
  return new Promise((resolve) => {
    s.close(() => resolve());
    s.closeIdleConnections();
  });
}

async function start(): Promise<void> {
  // Fail fast: if a dependency is unreachable the application exits.
  await checkRedis();
  await checkCloudWatch();

  const recovered = recoverPendingReports();
  if (recovered > 0) {
    logger.info("Startup recovery: pending CSV files queued for upload", {
      component: "server.startup",
      event: EVENTS.SERVER_STARTUP_RECOVERY,
      file_count: recovered,
      tags: ["startup", "recovery"],
      severity: 3,
    });
  }

  workers = [createInputWorker(), createFailureWorker(), createDlqWorker()];

  await listen();

  logger.info("Application started successfully", {
    component: "server.startup",
    event: EVENTS.SERVER_STARTUP_READY,
    outcome: "success",
    url: `http://${config.host}:${config.port}/api/webhook`,
    cloudwatch: Boolean(config.aws.logGroup && config.aws.logStream),
    tags: ["startup", "ready"],
    severity: 3,
  });
}

async function stop(): Promise<void> {
  logger.info("Graceful shutdown initiated", {
    component: "server.shutdown",
    event: EVENTS.SERVER_SHUTDOWN_STARTED,
    tags: ["shutdown"],
    severity: 3,
  });
  // First, so no new webhook can arrive once the queues start closing.
  await closeServer();
  await Promise.allSettled(workers.map((w) => w.close()));
  await closeReportWriters();
  await sftpUploader.drain();
  await Promise.allSettled([
    inputQueue.close(),
    failureQueue.close(),
    dlqQueue.close(),
  ]);
  redisConnection.disconnect();
  logger.info("Shutdown complete", {
    component: "server.shutdown",
    event: EVENTS.SERVER_SHUTDOWN_COMPLETE,
    outcome: "success",
    tags: ["shutdown"],
    severity: 3,
  });
  await closeLogger();
}

let stopping: Promise<void> | undefined;

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    // A repeated signal must not start a second shutdown.
    stopping ??= stop().finally(() => process.exit(0));
  });
}

start().catch((err) => {
  logger.error("Fatal startup error", {
    component: "server.startup",
    event: EVENTS.SERVER_STARTUP_FATAL,
    outcome: "failure",
    error: err,
    error_code: "STARTUP_FATAL",
    severity: 10,
  });
  void closeLogger()
    .catch(() => undefined)
    .finally(() => process.exit(1));
});
