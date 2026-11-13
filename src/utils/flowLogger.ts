/**
 * One logging path for every processing flow (webhook intake, input worker and its retries, failure and
 * DLQ queues, failure/DLQ reprocessing), so a request reads the same wherever it is and can be followed
 * from receipt to its final result by `correlation_id` / `job_id`.
 *
 * Every entry carries: processing_type, function_name, stage, flow_status (STARTED | SUCCESS | FAILED),
 * the request body (and response body where there is one), and for failures the error type, message,
 * stack trace and the category of step that failed. Logging never throws into the code being logged.
 */

import logger, { sanitizeForLog, type InfoMeta } from "./logger";
import { EVENTS, type LogEvent, type LogOutcome } from "./logEvents";

export type ProcessingType =
  | "INPUT"
  | "RETRY"
  | "FAILURE"
  | "FAILURE_REPROCESS"
  | "DLQ"
  | "DLQ_REPROCESS";

export type FlowStatus = "STARTED" | "SUCCESS" | "FAILED";

/** What kind of step a failure came from. */
export type FailureCategory =
  | "API_EXECUTION"
  | "DATA_TRANSFORMATION"
  | "VALIDATION"
  | "DATABASE_OPERATION"
  | "FILE_OPERATION"
  | "RETRY_HANDLING"
  | "DLQ_PROCESSING";

/** A body above this is logged as a size summary plus a preview instead of whole. */
const MAX_LOGGED_BODY_BYTES = 64 * 1024;
const BODY_PREVIEW_CHARS = 2048;

export interface FlowEntry {
  type: ProcessingType;
  /** Function (or handler) that is logging; on a failure, the function that failed. */
  functionName: string;
  stage: string;
  status: FlowStatus;
  /** Defaults to "<functionName> <stage> <status>". Keep stable on failures so log entries can be grouped. */
  message?: string;
  component: string;
  /** Request id of the originating HTTP call; for queue jobs, the id carried on the job or else the job id. */
  correlationId?: string;
  jobId?: string;
  recordId?: string;
  queueName?: string;
  /** The body exactly as being processed. Pass the key (even as undefined) only when there is one. */
  requestBody?: unknown;
  responseBody?: unknown;
  method?: string;
  endpoint?: string;
  statusCode?: number;
  /** Failures: the thrown value, or `errorMessage` when only a reason string exists. */
  error?: unknown;
  errorMessage?: string;
  errorCode?: string;
  failureCategory?: FailureCategory;
  /**
   * When the entry is written by a different function than the one that failed (e.g. the DLQ hand-off
   * reporting a flatten error), the step that really failed; reported as failed_function/failed_stage.
   */
  failedStep?: FailedStep;
  /** A FAILED entry is an error by default; `warn` or `info` for failures that are handled and expected. */
  level?: "warn" | "info";
  event?: LogEvent;
  outcome?: LogOutcome;
  severity?: number;
  tags?: string[];
  /** Anything else for the entry (attempt, will_retry, counts, ...). Never overrides the fields above. */
  extra?: Record<string, unknown>;
}

/** The body as sanitized JSON, or a size summary when it is too large to log safely. */
export function describeBody(
  field: "request_body" | "response_body",
  body: unknown,
): Record<string, unknown> {
  if (body === undefined || body === null) return { [field]: null };
  let raw: string;
  try {
    raw = JSON.stringify(body) ?? "";
  } catch {
    return { [field]: "[unserializable]" };
  }
  const bytes = Buffer.byteLength(raw);
  if (bytes > MAX_LOGGED_BODY_BYTES) {
    return {
      [`${field}_truncated`]: true,
      [`${field}_bytes`]: bytes,
      [`${field}_preview`]: sanitizeForLog(raw.slice(0, BODY_PREVIEW_CHARS)),
    };
  }
  return { [field]: sanitizeForLog(body), [`${field}_bytes`]: bytes };
}

function errorFields(entry: FlowEntry): Record<string, unknown> {
  const { error } = entry;
  if (error === undefined || error === null) {
    return entry.errorMessage ? { error_message: entry.errorMessage } : {};
  }
  const err = error instanceof Error ? error : undefined;
  const message =
    entry.errorMessage ??
    (err ? err.message : typeof error === "string" ? error : safeString(error));
  const cause = err?.cause;
  return {
    error_type: err ? err.constructor.name || "Error" : typeof error,
    error_message: message,
    stack_trace: err?.stack,
    ...(cause !== undefined
      ? {
          error_cause:
            cause instanceof Error ? cause.message : safeString(cause),
        }
      : {}),
  };
}

function safeString(value: unknown): string {
  try {
    return typeof value === "string"
      ? value
      : (JSON.stringify(value) ?? String(value));
  } catch {
    return String(value);
  }
}

function build(entry: FlowEntry): {
  message: string;
  meta: Record<string, unknown>;
} {
  const failed = entry.status === "FAILED";
  const fields: Record<string, unknown> = {
    component: entry.component,
    processing_type: entry.type,
    function_name: entry.functionName,
    stage: entry.stage,
    flow_status: entry.status,
    request_id: entry.correlationId,
    correlation_id: entry.correlationId,
    job_id: entry.jobId,
    record_id: entry.recordId,
    queue_name: entry.queueName,
    method: entry.method,
    endpoint: entry.endpoint,
    status_code: entry.statusCode,
    event: entry.event ?? EVENTS.FLOW_STEP,
    outcome: entry.outcome,
    severity: entry.severity,
    tags: entry.tags ?? [],
  };
  // Fields left undefined must not wipe a same-named key supplied through `extra`.
  const meta: Record<string, unknown> = { ...entry.extra };
  for (const [key, value] of Object.entries(fields)) {
    if (value !== undefined) meta[key] = value;
  }
  if ("requestBody" in entry)
    Object.assign(meta, describeBody("request_body", entry.requestBody));
  if ("responseBody" in entry)
    Object.assign(meta, describeBody("response_body", entry.responseBody));
  if (failed) {
    Object.assign(meta, errorFields(entry), {
      error_code: entry.errorCode,
      failure_category: entry.failedStep?.category ?? entry.failureCategory,
      failed_function: entry.failedStep?.functionName ?? entry.functionName,
      failed_stage: entry.failedStep?.stage ?? entry.stage,
    });
  } else {
    meta.status = entry.status === "STARTED" ? "processing" : "success";
  }
  return {
    message:
      entry.message ?? `${entry.functionName} ${entry.stage} ${entry.status}`,
    meta,
  };
}

/** Writes one flow entry. Safe to call anywhere: a logging problem is reported on stderr and swallowed. */
export function flowLog(entry: FlowEntry): void {
  try {
    const { message, meta } = build(entry);
    if (entry.status !== "FAILED") {
      logger.info(message, meta);
    } else if (entry.level === "warn") {
      logger.warn(message, {
        warning_code: entry.errorCode,
        warning_message:
          typeof meta.error_message === "string"
            ? meta.error_message
            : undefined,
        ...meta,
      });
    } else if (entry.level === "info") {
      logger.info(message, { ...meta, status: "failed" } as InfoMeta);
    } else {
      logger.error(message, meta);
    }
  } catch (logErr) {
    try {
      console.error(
        "[flowLogger] could not write log entry:",
        logErr instanceof Error ? logErr.message : String(logErr),
      );
    } catch {
      /* nothing left to report to */
    }
  }
}

type BoundEntry = Omit<
  FlowEntry,
  "type" | "component" | "correlationId" | "jobId" | "recordId" | "queueName"
> &
  Partial<
    Pick<
      FlowEntry,
      | "type"
      | "component"
      | "correlationId"
      | "jobId"
      | "recordId"
      | "queueName"
    >
  >;

/** A logger with the identifying fields of one flow filled in once; each call can still override them. */
export function createFlow(
  base: Pick<FlowEntry, "type" | "component"> &
    Partial<
      Pick<FlowEntry, "correlationId" | "jobId" | "recordId" | "queueName">
    >,
): (entry: BoundEntry) => void {
  return (entry) => flowLog({ ...base, ...entry });
}

export interface FailedStep {
  functionName: string;
  stage: string;
  category: FailureCategory;
}

const failedSteps = new WeakMap<object, FailedStep>();

/**
 * Records which step an error came from, without altering the error. A step that lets its error
 * propagate (so BullMQ can retry the job) tags it, and whoever finally logs the failure reports the
 * step that really failed instead of the generic job-level handler. The first tag wins.
 */
export function tagFailedStep<T>(err: T, step: FailedStep): T {
  if (typeof err === "object" && err !== null && !failedSteps.has(err)) {
    failedSteps.set(err, step);
  }
  return err;
}

export function failedStepOf(err: unknown): FailedStep | undefined {
  return typeof err === "object" && err !== null
    ? failedSteps.get(err)
    : undefined;
}
