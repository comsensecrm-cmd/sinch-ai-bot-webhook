/** Hand-off from the input queue to the failure or DLQ queue. */

import { flowLog, type FailedStep } from "../utils/flowLogger";
import { EVENTS } from "../utils/logEvents";
import {
  DLQ_QUEUE_NAME,
  FAILURE_QUEUE_NAME,
  dlqQueue,
  failureQueue,
} from "./queues";

export interface FailureJobData {
  jobId: string;
  errors: string[];
  failedAt: string;
  originalPayload: unknown;
  /** Request id the record arrived under; absent on jobs queued before it was recorded. */
  correlationId?: string;
}

export interface DlqJobData {
  jobId: string;
  reason: string;
  attempts: number;
  failedAt: string;
  originalPayload: unknown;
  correlationId?: string;
}

export interface RouteContext {
  correlationId?: string;
  recordId?: string;
}

export interface DlqRouteContext extends RouteContext {
  /** The step whose failure sent the record here; defaults to the DLQ hand-off itself. */
  failedStep?: FailedStep;
  /** The error behind `reason`, so its type and stack trace are logged with the DLQ entry. */
  error?: unknown;
}

export async function routeToFailureQueue(
  jobId: string,
  originalPayload: unknown,
  errors: string[],
  ctx: RouteContext = {},
): Promise<void> {
  const base = {
    type: "FAILURE" as const,
    component: "queue.routing",
    functionName: "routeToFailureQueue",
    correlationId: ctx.correlationId ?? jobId,
    jobId,
    recordId: ctx.recordId,
    queueName: FAILURE_QUEUE_NAME,
    requestBody: originalPayload,
  };
  const data: FailureJobData = {
    jobId,
    errors,
    failedAt: new Date().toISOString(),
    originalPayload,
    ...(ctx.correlationId && { correlationId: ctx.correlationId }),
  };

  flowLog({
    ...base,
    stage: "FAILURE_QUEUE_INSERT",
    status: "STARTED",
    message: "Routing record to failure queue",
    extra: { errors },
  });
  try {
    await failureQueue.add("invalid-record", data, {
      jobId: `failure_${jobId}`,
    });
  } catch (err) {
    flowLog({
      ...base,
      stage: "FAILURE_QUEUE_INSERT",
      status: "FAILED",
      message: "Could not add record to the failure queue",
      error: err,
      errorCode: "FAILURE_QUEUE_INSERT_FAILED",
      failureCategory: "DATABASE_OPERATION",
      event: EVENTS.QUEUE_ERROR,
      outcome: "failure",
      extra: { errors },
    });
    throw err;
  }
  flowLog({
    ...base,
    stage: "ROUTED_TO_FAILURE_QUEUE",
    status: "FAILED",
    level: "warn",
    message: "Record routed to failure queue",
    event: EVENTS.QUEUE_ROUTED_TO_FAILURE,
    outcome: "failure",
    errorCode: "INVALID_PAYLOAD",
    errorMessage: errors.join("; "),
    failureCategory: "VALIDATION",
    tags: ["failure-queue"],
    severity: 6,
    extra: { errors },
  });
}

export async function routeToDlq(
  jobId: string,
  originalPayload: unknown,
  reason: string,
  attempts: number,
  ctx: DlqRouteContext = {},
): Promise<void> {
  const base = {
    type: "DLQ" as const,
    component: "queue.routing",
    functionName: "routeToDlq",
    correlationId: ctx.correlationId ?? jobId,
    jobId,
    recordId: ctx.recordId,
    queueName: DLQ_QUEUE_NAME,
    requestBody: originalPayload,
  };
  const data: DlqJobData = {
    jobId,
    reason,
    attempts,
    failedAt: new Date().toISOString(),
    originalPayload,
    ...(ctx.correlationId && { correlationId: ctx.correlationId }),
  };

  flowLog({
    ...base,
    stage: "DLQ_INSERT",
    status: "STARTED",
    message: "Moving record to DLQ",
    extra: { attempt: attempts, reason },
  });
  try {
    await dlqQueue.add("dead-letter", data, { jobId: `dlq_${jobId}` });
  } catch (err) {
    flowLog({
      ...base,
      stage: "DLQ_INSERT",
      status: "FAILED",
      message: "Could not add record to the DLQ",
      error: err,
      errorCode: "DLQ_INSERT_FAILED",
      failureCategory: "DLQ_PROCESSING",
      event: EVENTS.QUEUE_ERROR,
      outcome: "failure",
      extra: { attempt: attempts, reason },
    });
    throw err;
  }
  flowLog({
    ...base,
    stage: "MOVED_TO_DLQ",
    status: "FAILED",
    message: "Record moved to DLQ",
    event: EVENTS.QUEUE_MOVED_TO_DLQ,
    outcome: "failure",
    error: ctx.error,
    errorMessage: reason,
    errorCode: "MOVED_TO_DLQ",
    failureCategory: "DLQ_PROCESSING",
    failedStep: ctx.failedStep,
    tags: ["dlq-queue"],
    severity: 8,
    extra: { attempt: attempts },
  });
}
