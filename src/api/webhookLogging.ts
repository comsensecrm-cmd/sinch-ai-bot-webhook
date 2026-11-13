/** Request-scoped logging for /api/webhook: request id, received/started/completed/failed, and non-2xx responses. */

import { randomUUID } from "crypto";
import type { NextFunction, Request, Response } from "express";
import { sanitizeForLog } from "../utils/logger";
import { EVENTS } from "../utils/logEvents";
import { flowLog } from "../utils/flowLogger";

const COMPONENT = "api.webhook";
const MAX_REQUEST_ID_LENGTH = 128;
const SAFE_REQUEST_ID = /^[A-Za-z0-9._:-]+$/;

/** Runs the log call after the current I/O step so logging never delays webhook handling. */
function defer(fn: () => void): void {
  setImmediate(() => {
    try {
      fn();
    } catch (err) {
      console.error(
        "[webhookLogging] log failed:",
        err instanceof Error ? err.message : String(err),
      );
    }
  });
}

export function getRequestId(res: Response): string | undefined {
  return res.locals.requestId as string | undefined;
}

function resolveRequestId(req: Request): string {
  const incoming = req.get("x-request-id") ?? req.get("x-correlation-id");
  if (
    incoming &&
    incoming.length <= MAX_REQUEST_ID_LENGTH &&
    SAFE_REQUEST_ID.test(incoming)
  ) {
    return incoming;
  }
  return randomUUID();
}

function contentLength(req: Request): number | undefined {
  const value = Number(req.get("content-length"));
  return Number.isFinite(value) ? value : undefined;
}

/** What is needed to place a request; headers and query are left to the failure entry. */
function receivedContext(req: Request): Record<string, unknown> {
  return {
    method: req.method,
    endpoint: req.originalUrl.split("?")[0],
    content_type: req.get("content-type"),
    content_length: contentLength(req),
    ip: req.ip,
    forwarded_for: req.get("x-forwarded-for"),
  };
}

function requestContext(req: Request): Record<string, unknown> {
  return {
    ...receivedContext(req),
    query: sanitizeForLog(req.query),
    headers: sanitizeForLog(req.headers),
  };
}

/** Mount before the body parser so unparseable requests are still logged. */
export function webhookRequestLogger(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const startedAt = process.hrtime.bigint();
  const requestId = resolveRequestId(req);
  res.locals.requestId = requestId;
  res.locals.startedAt = startedAt;
  res.setHeader("X-Request-Id", requestId);

  const base = {
    component: COMPONENT,
    correlationId: requestId,
  };

  // The body is not parsed yet at this point; "processing started" (below) is the entry that carries it.
  defer(() =>
    flowLog({
      ...base,
      type: "INPUT",
      functionName: "webhookRequestLogger",
      stage: "REQUEST_RECEIVED",
      status: "STARTED",
      message: "Webhook request received",
      tags: ["webhook", "request-received"],
      event: EVENTS.WEBHOOK_REQUEST_RECEIVED,
      extra: receivedContext(req),
    }),
  );

  let logged = false;
  const onDone = (aborted: boolean) => () => {
    if (logged) return;
    logged = true;
    const duration_ms = Number(process.hrtime.bigint() - startedAt) / 1e6;
    const status_code = res.statusCode;
    const incomplete = aborted && !res.writableFinished;
    // A successful response is already covered by processing_completed.
    if (!incomplete && status_code < 400) return;
    defer(() => {
      const entry = {
        ...base,
        type: "INPUT" as const,
        functionName: "webhookRequestLogger",
        stage: "RESPONSE_SENT",
        status: "FAILED" as const,
        method: req.method,
        endpoint: req.originalUrl.split("?")[0],
        statusCode: status_code,
        outcome: "failure" as const,
        extra: { duration_ms: Math.round(duration_ms * 100) / 100 },
      };
      if (incomplete) {
        flowLog({
          ...entry,
          stage: "CONNECTION_CLOSED",
          message: "Webhook connection closed before response was sent",
          tags: ["webhook", "request-aborted"],
          event: EVENTS.WEBHOOK_REQUEST_ABORTED,
          errorCode: "CLIENT_ABORTED",
          errorMessage:
            "Client closed the connection before the response was sent",
          failureCategory: "API_EXECUTION",
          level: "warn",
          severity: 5,
        });
      } else if (status_code >= 500) {
        flowLog({
          ...entry,
          message: "Webhook request completed with server error",
          tags: ["webhook", "response"],
          event: EVENTS.WEBHOOK_RESPONSE_SENT,
          errorCode: "SERVER_ERROR_RESPONSE",
          errorMessage: `Webhook responded with HTTP ${status_code}`,
          failureCategory: "API_EXECUTION",
        });
      } else {
        // A 4xx is the sender's problem, not ours: recorded, but not as an error.
        flowLog({
          ...entry,
          message: "Webhook response sent",
          tags: ["webhook", "response"],
          event: EVENTS.WEBHOOK_RESPONSE_SENT,
          errorCode: "CLIENT_ERROR_RESPONSE",
          errorMessage: `Webhook responded with HTTP ${status_code}`,
          failureCategory: "VALIDATION",
          level: "info",
        });
      }
    });
  };
  res.once("finish", onDone(false));
  res.once("close", onDone(true));

  next();
}

function elapsedMs(res: Response): number | undefined {
  const startedAt = res.locals.startedAt as bigint | undefined;
  return startedAt
    ? Math.round((Number(process.hrtime.bigint() - startedAt) / 1e6) * 100) /
        100
    : undefined;
}

/**
 * Called by the route once the body is parsed, before anything is done with it. The body logged here is
 * `req.body` itself, i.e. exactly what is handed to the queue.
 */
export function logProcessingStarted(
  req: Request,
  res: Response,
  recordCount: number,
): void {
  const correlationId = getRequestId(res);
  defer(() =>
    flowLog({
      type: "INPUT",
      component: COMPONENT,
      correlationId,
      functionName: "webhookHandler",
      stage: "PROCESSING_STARTED",
      status: "STARTED",
      message: "Webhook processing started",
      tags: ["webhook", "processing-started"],
      event: EVENTS.WEBHOOK_PROCESSING_STARTED,
      requestBody: req.body,
      method: req.method,
      endpoint: req.originalUrl.split("?")[0],
      extra: { record_count: recordCount },
    }),
  );
}

/** Called by the route once the records are enqueued; job_ids join this request to the queue logs. */
export function logProcessingCompleted(
  req: Request,
  res: Response,
  jobIds: string[],
  responseBody: unknown,
): void {
  const correlationId = getRequestId(res);
  const duration_ms = elapsedMs(res);
  defer(() =>
    flowLog({
      type: "INPUT",
      component: COMPONENT,
      correlationId,
      functionName: "webhookHandler",
      stage: "ENQUEUED",
      status: "SUCCESS",
      message: "Webhook processing completed",
      tags: ["webhook", "processing-completed", "enqueued"],
      event: EVENTS.WEBHOOK_PROCESSING_COMPLETED,
      outcome: "success",
      method: req.method,
      endpoint: req.originalUrl.split("?")[0],
      statusCode: res.statusCode,
      responseBody,
      extra: {
        duration_ms,
        record_count: jobIds.length,
        success_count: jobIds.length,
        failed_count: 0,
        job_ids: jobIds,
      },
    }),
  );
}

export function logProcessingFailed(
  req: Request,
  res: Response,
  err: unknown,
  errorCode: string,
  recordCount: number,
  responseBody: unknown,
): void {
  // Not deferred: a failure must be recorded even if the process is about to exit.
  flowLog({
    type: "INPUT",
    component: COMPONENT,
    correlationId: getRequestId(res),
    functionName: "enqueueRecords",
    stage: "ENQUEUE",
    status: "FAILED",
    message: "Webhook processing failed",
    tags: ["webhook", "processing-failed"],
    event: EVENTS.WEBHOOK_PROCESSING_FAILED,
    outcome: "failure",
    error: err,
    errorCode,
    failureCategory: "DATABASE_OPERATION",
    // Needed to replay a request that never reached the queue.
    requestBody: req.body,
    responseBody,
    statusCode: 500,
    extra: {
      duration_ms: elapsedMs(res),
      record_count: recordCount,
      success_count: 0,
      failed_count: recordCount,
      ...requestContext(req),
    },
  });
}
