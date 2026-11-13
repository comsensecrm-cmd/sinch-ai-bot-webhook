/** Input queue consumer: validates, writes the CSV row, or hands off to the failure/DLQ queue. */

import { Worker, type Job, type ConnectionOptions } from "bullmq";
import { inputCsvWriter } from "../csv/reportWriters";
import {
  createFlow,
  failedStepOf,
  flowLog,
  tagFailedStep,
  type FailedStep,
} from "../utils/flowLogger";
import { EVENTS } from "../utils/logEvents";
import { flattenPayload } from "../validation/flatten";
import { validatePayload } from "../validation/payloadValidator";
import { correlationIdOf } from "./producer";
import { shouldLogQueueError } from "./errorThrottle";
import { INPUT_QUEUE_NAME, redisConnection } from "./queues";
import { routeToDlq, routeToFailureQueue } from "./routing";

const COMPONENT = "queue.inputWorker";
const WRITTEN_MARKER_TTL_SECONDS = 7 * 24 * 60 * 60;
// How long a job's claim on a record id holds while it is still being written.
const PENDING_CLAIM_TTL_SECONDS = 10 * 60;
// Keyed on the payload id (the PK), so a Sinch redelivery of the same call is
// recognised even though BullMQ gives it a new job id.
const writtenKey = (recordId: string) =>
  `sinch-ai-bot-webhook:csv-written:${recordId}`;
const pendingValue = (jobId: string) => `pending:${jobId}`;

const RELEASE_CLAIM_SCRIPT = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;

/**
 * Atomically claims the record id for this job (SET NX), so two jobs for the same
 * id - even on different instances - cannot both write it. Returns false when the
 * id is already written or is being written by a different job. The same job
 * re-running after a crash or stall keeps its own claim and proceeds.
 */
async function claimRecord(recordId: string, jobId: string): Promise<boolean> {
  const key = writtenKey(recordId);
  const mine = pendingValue(jobId);
  const claimed = await redisConnection.set(
    key,
    mine,
    "EX",
    PENDING_CLAIM_TTL_SECONDS,
    "NX",
  );
  if (claimed === "OK") return true;
  const current = await redisConnection.get(key);
  return current === null || current === mine;
}

/** Drops this job's pending claim so a retry or replay isn't mistaken for a duplicate. */
async function releaseClaim(recordId: string, jobId: string): Promise<void> {
  await redisConnection
    .eval(RELEASE_CLAIM_SCRIPT, 1, writtenKey(recordId), pendingValue(jobId))
    .catch(() => undefined);
}

const errorMessage = (err: unknown) =>
  err instanceof Error ? err.message : String(err);
const maxAttempts = (job: Job) => job.opts.attempts ?? 1;

/** The record's id (the PK), or undefined when the payload has none. */
function recordIdOf(payload: unknown): string | undefined {
  if (typeof payload !== "object" || payload === null) return undefined;
  const raw = (payload as Record<string, unknown>).id;
  return typeof raw === "string" && raw.trim() !== "" ? raw : undefined;
}

/** How long BullMQ waits before retrying after the given failed attempt (mirrors its backoff rule). */
function retryDelayMs(job: Job, failedAttempt: number): number | undefined {
  const backoff = job.opts.backoff;
  if (backoff === undefined) return undefined;
  const delay = typeof backoff === "number" ? backoff : (backoff.delay ?? 0);
  const exponential =
    typeof backoff === "object" && backoff.type === "exponential";
  return exponential ? Math.round(2 ** (failedAttempt - 1) * delay) : delay;
}

/** Runs one step; if it throws, the error is tagged with that step before it propagates (see tagFailedStep). */
async function runStep<T>(
  step: FailedStep,
  work: () => Promise<T> | T,
): Promise<T> {
  try {
    return await work();
  } catch (err) {
    throw tagFailedStep(err, step);
  }
}

async function processJob(job: Job): Promise<void> {
  try {
    await runJob(job);
  } catch (err) {
    // Anything not already tagged by a specific step.
    throw tagFailedStep(err, {
      functionName: "processJob",
      stage: "JOB_EXECUTION",
      category: "API_EXECUTION",
    });
  }
}

async function runJob(job: Job): Promise<void> {
  const startedAt = Date.now();
  const jobId = String(job.id);
  const payload: unknown = job.data;
  const attempt = job.attemptsMade + 1;
  const correlationId = correlationIdOf(job);
  const recordId = recordIdOf(payload);
  const attemptInfo = {
    attempt,
    max_attempts: maxAttempts(job),
    retry_count: attempt - 1,
  };
  const flow = createFlow({
    type: attempt > 1 ? "RETRY" : "INPUT",
    component: COMPONENT,
    correlationId,
    jobId,
    recordId,
    queueName: INPUT_QUEUE_NAME,
  });
  const routeCtx = { correlationId, recordId };
  const finished = (result: string) =>
    flow({
      functionName: "processJob",
      stage: "JOB_FINISHED",
      status: "SUCCESS",
      message: `Input job finished: ${result}`,
      extra: { ...attemptInfo, result, duration_ms: Date.now() - startedAt },
    });

  flow({
    functionName: "processJob",
    stage: "JOB_STARTED",
    status: "STARTED",
    message: attempt > 1 ? "Input job retry started" : "Input job started",
    requestBody: payload,
    extra: {
      ...attemptInfo,
      ...(attempt > 1 && { retry_reason: job.failedReason }),
    },
  });

  const validation = await runStep(
    {
      functionName: "validatePayload",
      stage: "VALIDATE",
      category: "VALIDATION",
    },
    () => validatePayload(payload),
  );
  if (!validation.valid) {
    await runStep(
      {
        functionName: "routeToFailureQueue",
        stage: "FAILURE_QUEUE_INSERT",
        category: "DATABASE_OPERATION",
      },
      () => routeToFailureQueue(jobId, payload, validation.errors, routeCtx),
    );
    finished("routed to failure queue");
    return;
  }
  flow({
    functionName: "validatePayload",
    stage: "VALIDATE",
    status: "SUCCESS",
  });

  // id is optional. Without one there is nothing to de-duplicate on, so the
  // record is written as-is (claiming "undefined" would collapse all id-less rows).
  if (
    recordId !== undefined &&
    !(await runStep(
      {
        functionName: "claimRecord",
        stage: "DEDUP_CLAIM",
        category: "DATABASE_OPERATION",
      },
      () => claimRecord(recordId, jobId),
    ))
  ) {
    flow({
      functionName: "claimRecord",
      stage: "DEDUP_CLAIM",
      status: "SUCCESS",
      message: "Record already written or being written; skipped",
      event: EVENTS.QUEUE_DUPLICATE_SKIPPED,
      outcome: "skipped",
      requestBody: payload,
      tags: ["input-queue", "duplicate-skipped"],
    });
    finished("duplicate skipped");
    return;
  }

  let row: Record<string, string>;
  try {
    row = flattenPayload(payload as Record<string, unknown>);
  } catch (err) {
    if (recordId) await releaseClaim(recordId, jobId);
    // routeToDlq logs this failure, with the stack, as the flatten step's.
    await routeToDlq(
      jobId,
      payload,
      `Flatten failed: ${errorMessage(err)}`,
      attempt,
      {
        ...routeCtx,
        failedStep: {
          functionName: "flattenPayload",
          stage: "FLATTEN",
          category: "DATA_TRANSFORMATION",
        },
        error: err,
      },
    );
    finished("moved to DLQ");
    return;
  }
  flow({
    functionName: "flattenPayload",
    stage: "FLATTEN",
    status: "SUCCESS",
    extra: { column_count: Object.keys(row).length },
  });

  const csvStep: FailedStep = {
    functionName: "inputCsvWriter.writeRecord",
    stage: "CSV_WRITE",
    category: "FILE_OPERATION",
  };
  try {
    await inputCsvWriter.writeRecord(row);
  } catch (err) {
    if (attempt >= maxAttempts(job)) {
      if (recordId) await releaseClaim(recordId, jobId);
      await routeToDlq(
        jobId,
        payload,
        `CSV write failed: ${errorMessage(err)}`,
        attempt,
        { ...routeCtx, failedStep: csvStep, error: err },
      );
      finished("moved to DLQ");
      return;
    }
    // Rethrown for BullMQ to retry; the worker's "failed" handler logs it, naming this step.
    throw tagFailedStep(err, csvStep);
  }
  flow({
    functionName: "inputCsvWriter.writeRecord",
    stage: "CSV_WRITE",
    status: "SUCCESS",
  });

  if (recordId)
    await redisConnection
      .set(writtenKey(recordId), "1", "EX", WRITTEN_MARKER_TTL_SECONDS)
      .catch((err) =>
        flow({
          functionName: "processJob",
          stage: "WRITTEN_MARKER",
          status: "FAILED",
          level: "warn",
          message: "Could not set CSV-written marker",
          event: EVENTS.QUEUE_MARKER_SET_FAILED,
          outcome: "failure",
          requestBody: payload,
          error: err,
          errorCode: "MARKER_SET_FAILED",
          failureCategory: "DATABASE_OPERATION",
          severity: 6,
        }),
      );

  flow({
    functionName: "processJob",
    stage: "JOB_FINISHED",
    status: "SUCCESS",
    message: "Input job completed",
    event: EVENTS.QUEUE_JOB_COMPLETED,
    outcome: "success",
    requestBody: payload,
    extra: {
      ...attemptInfo,
      result: "csv written",
      duration_ms: Date.now() - startedAt,
    },
  });
}

async function onFailed(job: Job | undefined, err: Error): Promise<void> {
  if (!job) return;
  const exhausted =
    job.attemptsMade >= maxAttempts(job) || err.message.includes("stalled");
  if (!exhausted) return;

  const data: unknown = job.data;
  const id = recordIdOf(data);
  // The job is giving up; a later DLQ replay must not be skipped as "in progress".
  if (id !== undefined) await releaseClaim(id, String(job.id));

  const correlationId = correlationIdOf(job);
  try {
    await routeToDlq(String(job.id), job.data, err.message, job.attemptsMade, {
      correlationId,
      recordId: id,
      failedStep: failedStepOf(err),
      error: err,
    });
  } catch (routeErr) {
    flowLog({
      type: "DLQ",
      component: COMPONENT,
      correlationId,
      jobId: String(job.id),
      recordId: id,
      queueName: INPUT_QUEUE_NAME,
      functionName: "onFailed",
      stage: "DLQ_HANDOFF",
      status: "FAILED",
      message:
        "Could not move failed job to DLQ; it remains in the input queue's failed set",
      event: EVENTS.QUEUE_DLQ_HANDOFF_FAILED,
      outcome: "failure",
      requestBody: job.data,
      error: routeErr,
      errorCode: "DLQ_HANDOFF_FAILED",
      failureCategory: "DLQ_PROCESSING",
      failedStep: failedStepOf(err),
      severity: 9,
      extra: {
        original_error_message: err.message,
        original_stack_trace: err.stack,
      },
    });
  }
}

export function createInputWorker(): Worker {
  const worker = new Worker(INPUT_QUEUE_NAME, processJob, {
    connection: redisConnection as unknown as ConnectionOptions,
    concurrency: 1,
  });

  worker.on("failed", (job, err) => {
    const attempt = job?.attemptsMade;
    const willRetry = job
      ? job.attemptsMade < maxAttempts(job) && !err.message.includes("stalled")
      : undefined;
    flowLog({
      type: attempt !== undefined && attempt > 1 ? "RETRY" : "INPUT",
      component: COMPONENT,
      correlationId: job ? correlationIdOf(job) : undefined,
      jobId: job?.id,
      recordId: recordIdOf(job?.data),
      queueName: INPUT_QUEUE_NAME,
      functionName: "inputWorker.failed",
      stage: "JOB_FAILED",
      status: "FAILED",
      message: "Input job failed",
      event: EVENTS.QUEUE_JOB_FAILED,
      outcome: willRetry ? "retry" : "failure",
      requestBody: job?.data,
      error: err,
      errorCode: "INPUT_JOB_FAILED",
      failureCategory: "RETRY_HANDLING",
      failedStep: failedStepOf(err),
      extra: {
        attempt,
        max_attempts: job ? maxAttempts(job) : undefined,
        will_retry: willRetry,
        retry_reason: willRetry ? err.message : undefined,
        retry_delay_ms:
          willRetry && job && attempt ? retryDelayMs(job, attempt) : undefined,
      },
    });
    void onFailed(job, err);
  });
  worker.on("error", (err) => {
    if (!shouldLogQueueError(`worker:${INPUT_QUEUE_NAME}`)) return;
    flowLog({
      type: "INPUT",
      component: COMPONENT,
      queueName: INPUT_QUEUE_NAME,
      functionName: "inputWorker.error",
      stage: "WORKER",
      status: "FAILED",
      message: "Input worker error",
      event: EVENTS.QUEUE_WORKER_ERROR,
      outcome: "failure",
      error: err,
      errorCode: "INPUT_WORKER_ERROR",
      failureCategory: "DATABASE_OPERATION",
    });
  });

  return worker;
}
