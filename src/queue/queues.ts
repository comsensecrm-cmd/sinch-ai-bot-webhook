/** The three BullMQ queues and the shared Redis connection. */

import { Queue, type ConnectionOptions, type JobsOptions } from "bullmq";
import { Redis } from "ioredis";
import config from "../config/env";
import logger from "../utils/logger";
import { EVENTS } from "../utils/logEvents";
import { shouldLogQueueError } from "./errorThrottle";

export const INPUT_QUEUE_NAME = "sinch-ai-bot-input-queue";
export const FAILURE_QUEUE_NAME = "sinch-ai-bot-failure-queue";
export const DLQ_QUEUE_NAME = "sinch-ai-bot-dlq-queue";

export const redisConnection = new Redis(config.redis.url, {
  password: config.redis.password,
  maxRetriesPerRequest: null,
  enableReadyCheck: false,
  // Keeps idle connections from being reset by proxies and load balancers (ECONNRESET).
  keepAlive: 10_000,
  retryStrategy: (times) => Math.min(times * 100, 3_000),
});

let redisDown = false;
redisConnection.on("connect", () => {
  redisDown = false;
  logger.info("Redis connected", {
    component: "redis",
    event: EVENTS.REDIS_CONNECTED,
    outcome: "success",
  });
});
redisConnection.on("error", (err) => {
  if (redisDown) return;
  redisDown = true;
  logger.error("Redis error", {
    component: "redis",
    event: EVENTS.REDIS_ERROR,
    outcome: "failure",
    error: err,
    error_code: "REDIS_ERROR",
  });
});

const connection = redisConnection as unknown as ConnectionOptions;

function jobDefaults(
  removeOnComplete: JobsOptions["removeOnComplete"],
  removeOnFail: JobsOptions["removeOnFail"] = false,
): JobsOptions {
  return {
    attempts: config.queue.maxAttempts,
    backoff: { type: "exponential", delay: config.queue.retryDelayMs },
    removeOnComplete,
    removeOnFail,
  };
}

const DAY = 24 * 60 * 60;

// A failed input job has already been copied to the DLQ, so it is only kept for
// a week (to inspect, or to retry from Bull Board if the DLQ hand-off failed)
// instead of piling up in Redis forever.
export const inputQueue = new Queue(INPUT_QUEUE_NAME, {
  connection,
  defaultJobOptions: jobDefaults({ age: DAY }, { age: 7 * DAY }),
});
// Report jobs complete only after the CSV row is written. The latest
// REPORT_JOBS_KEPT completed jobs are kept so they can be inspected in Bull
// Board; older ones are trimmed. A failed write leaves the job in the queue
// (removeOnFail: false) for retry.
const REPORT_JOBS_KEPT = 500;
export const failureQueue = new Queue(FAILURE_QUEUE_NAME, {
  connection,
  defaultJobOptions: jobDefaults({ count: REPORT_JOBS_KEPT }),
});
export const dlqQueue = new Queue(DLQ_QUEUE_NAME, {
  connection,
  defaultJobOptions: jobDefaults({ count: REPORT_JOBS_KEPT }),
});

for (const queue of [inputQueue, failureQueue, dlqQueue]) {
  queue.on("error", (err) => {
    if (!shouldLogQueueError(`queue:${queue.name}`)) return;
    logger.error("Queue error", {
      component: "queue",
      event: EVENTS.QUEUE_ERROR,
      outcome: "failure",
      queue_name: queue.name,
      error: err,
      error_code: "QUEUE_ERROR",
    });
  });
}
