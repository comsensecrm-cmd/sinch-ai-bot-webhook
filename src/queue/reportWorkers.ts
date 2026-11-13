/** Failure and DLQ consumers: one CSV row per job. */

import { Worker, type Job, type ConnectionOptions } from "bullmq";
import { dlqCsvWriter, failureCsvWriter } from "../csv/reportWriters";
import {
  createFlow,
  failedStepOf,
  flowLog,
  tagFailedStep,
  type ProcessingType,
} from "../utils/flowLogger";
import { EVENTS } from "../utils/logEvents";
import { shouldLogQueueError } from "./errorThrottle";
import { DLQ_QUEUE_NAME, FAILURE_QUEUE_NAME, redisConnection } from "./queues";
import type { DlqJobData, FailureJobData } from "./routing";

const COMPONENT = "queue.reportWorker";

/** coms_id of the original payload, or "" when absent or the payload isn't an object. */
function comsIdOf(payload: unknown): string {
  const value =
    typeof payload === "object" && payload !== null
      ? (payload as Record<string, unknown>).coms_id
      : undefined;
  return typeof value === "string" ? value : "";
}

/** The fields of a report job that identify and reproduce the record it carries. */
function describeJob(job: Job | undefined) {
  const data = job?.data as
    | { jobId?: string; correlationId?: string; originalPayload?: unknown }
    | undefined;
  return {
    jobId: data?.jobId ?? (job?.id !== undefined ? String(job.id) : undefined),
    correlationId: data?.correlationId ?? data?.jobId ?? job?.id,
    originalPayload: data?.originalPayload,
  };
}

function createReportWorker(
  queueName: string,
  type: ProcessingType,
  writerName: string,
  write: (job: Job) => Promise<void>,
): Worker {
  const worker = new Worker(
    queueName,
    async (job) => {
      const { jobId, correlationId, originalPayload } = describeJob(job);
      const flow = createFlow({
        type,
        component: COMPONENT,
        correlationId:
          correlationId === undefined ? undefined : String(correlationId),
        jobId,
        queueName,
      });
      const attempt = job.attemptsMade + 1;
      const attemptInfo = {
        attempt,
        max_attempts: job.opts.attempts ?? 1,
        retry_count: attempt - 1,
      };
      flow({
        functionName: "reportWorker",
        stage: "JOB_STARTED",
        status: "STARTED",
        message: "Report job started",
        requestBody: originalPayload,
        extra: attemptInfo,
      });
      try {
        await write(job);
      } catch (err) {
        throw tagFailedStep(err, {
          functionName: writerName,
          stage: "CSV_WRITE",
          category: "FILE_OPERATION",
        });
      }
      flow({
        functionName: "reportWorker",
        stage: "JOB_FINISHED",
        status: "SUCCESS",
        message: "Record added to report",
        event: EVENTS.QUEUE_JOB_RECORDED,
        outcome: "success",
        requestBody: originalPayload,
        tags: [queueName, "recorded"],
        extra: attemptInfo,
      });
    },
    {
      connection: redisConnection as unknown as ConnectionOptions,
      concurrency: 1,
    },
  );

  worker.on("failed", (job, err) => {
    const maxAttempts = job?.opts.attempts ?? 1;
    const willRetry = job ? job.attemptsMade < maxAttempts : undefined;
    const { jobId, correlationId, originalPayload } = describeJob(job);
    flowLog({
      type,
      component: COMPONENT,
      correlationId:
        correlationId === undefined ? undefined : String(correlationId),
      jobId,
      queueName,
      functionName: "reportWorker.failed",
      stage: "JOB_FAILED",
      status: "FAILED",
      message: "Report job failed",
      event: EVENTS.QUEUE_JOB_FAILED,
      outcome: willRetry ? "retry" : "failure",
      requestBody: originalPayload,
      error: err,
      errorCode: "REPORT_JOB_FAILED",
      failureCategory: "RETRY_HANDLING",
      failedStep: failedStepOf(err),
      tags: [queueName],
      extra: {
        attempt: job?.attemptsMade,
        max_attempts: maxAttempts,
        will_retry: willRetry,
        retry_reason: willRetry ? err.message : undefined,
      },
    });
  });
  worker.on("error", (err) => {
    if (!shouldLogQueueError(`worker:${queueName}`)) return;
    flowLog({
      type,
      component: COMPONENT,
      queueName,
      functionName: "reportWorker.error",
      stage: "WORKER",
      status: "FAILED",
      message: "Report worker error",
      event: EVENTS.QUEUE_WORKER_ERROR,
      outcome: "failure",
      error: err,
      errorCode: "REPORT_WORKER_ERROR",
      failureCategory: "DATABASE_OPERATION",
      tags: [queueName],
    });
  });

  return worker;
}

export const createFailureWorker = (): Worker =>
  createReportWorker(
    FAILURE_QUEUE_NAME,
    "FAILURE",
    "failureCsvWriter.writeRecord",
    async (job) => {
      const d = job.data as FailureJobData;
      await failureCsvWriter.writeRecord({
        JobId: d.jobId,
        Reason: d.errors.join("; "),
        FailedAt: d.failedAt,
        RawPayload: JSON.stringify(d.originalPayload),
        coms_id: comsIdOf(d.originalPayload),
      });
    },
  );

export const createDlqWorker = (): Worker =>
  createReportWorker(
    DLQ_QUEUE_NAME,
    "DLQ",
    "dlqCsvWriter.writeRecord",
    async (job) => {
      const d = job.data as DlqJobData;
      await dlqCsvWriter.writeRecord({
        JobId: d.jobId,
        Reason: d.reason,
        Attempts: d.attempts,
        FailedAt: d.failedAt,
        RawPayload: JSON.stringify(d.originalPayload),
        coms_id: comsIdOf(d.originalPayload),
      });
    },
  );
