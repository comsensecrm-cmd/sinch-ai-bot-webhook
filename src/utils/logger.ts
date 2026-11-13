/** Structured JSON logger (Winston) with optional CloudWatch shipping. */

import winston from "winston";
import Transport from "winston-transport";
import os from "os";
import { randomUUID } from "crypto";
import {
  CloudWatchLogsClient,
  PutLogEventsCommand,
  CreateLogStreamCommand,
} from "@aws-sdk/client-cloudwatch-logs";
import config from "../config/env";
import { EVENTS, type LogEvent, type LogOutcome } from "./logEvents";

const SERVICE = "sinch-ai-bot-webhook";
const HOSTNAME = os.hostname();
const ENV = config.nodeEnv;
const IS_PROD = ENV === "production";

/** Identifies this process run; with `log_seq` it lets a reader drop duplicates and spot lost entries. */
const BOOT_ID = randomUUID().slice(0, 8);
let logSeq = 0;

interface BaseLogMeta {
  component?: string;
  /** Stable machine-readable event name; see logEvents.ts. */
  event?: LogEvent;
  outcome?: LogOutcome;
  correlation_id?: string;
  request_id?: string;
  span_id?: string;
  trace_id?: string;
  parent_span_id?: string;
  tags?: string[];
  duration_ms?: number;
  job_id?: string;
  record_id?: string;
  queue_name?: string;
  report_type?: string;
  file_id?: string;
  file_name?: string;
  file_path?: string;
  remote_dir?: string;
  remote_path?: string;
  file_size_bytes?: number;
  file_count?: number;
  record_count?: number;
  success_count?: number;
  failed_count?: number;
  attempt?: number;
  max_attempts?: number;
  /** Retries already made before this entry's attempt (attempt - 1). */
  retry_count?: number;
  retry_delay_ms?: number;
  will_retry?: boolean;
  status_code?: number;
  [extra: string]: unknown;
}

export interface InfoMeta extends BaseLogMeta {
  status?: "success" | "pending" | "processing" | "failed";
  severity?: 1 | 2 | 3;
}

export interface WarnMeta extends BaseLogMeta {
  status?: "slow" | "degraded" | "at-risk";
  warning_code?: string;
  warning_message?: string;
  warning_type?: string;
  threshold_ms?: number;
  actual_ms?: number;
  severity?: 5 | 6;
}

export interface ErrorMeta extends BaseLogMeta {
  status?: "failed";
  error?: Error | unknown;
  error_code?: string;
  error_message?: string;
  error_type?: string;
  stack_trace?: string;
  severity?: 7 | 8 | 9 | 10;
}

const SENSITIVE_KEYS = new Set([
  "accesskeyid",
  "secretaccesskey",
  "access_key_id",
  "secret_access_key",
  "aws_access_key_id",
  "aws_secret_access_key",
  "password",
  "passwd",
  "redis_password",
  "client_secret",
  "clientsecret",
  "sfmc_client_secret",
  "access_token",
  "accesstoken",
  "token",
  "bearer_token",
  "authorization",
  "x-authorization",
  "secret",
  "api_key",
  "apikey",
  "private_key",
  "privatekey",
  "cookie",
  "set-cookie",
  "proxy-authorization",
  "x-api-key",
  "api-key",
  "x-auth-token",
  "refresh_token",
  "id_token",
  "ssn",
  "cvv",
  "card_number",
  "cardnumber",
]);

const SENSITIVE_STRING_PATTERNS: RegExp[] = [
  /\b(AKIA|ASIA|AROA|AIDA|ANPA|ANVA|APKA)[A-Z0-9]{16}\b/g,
  /(?:secret[_-]?access[_-]?key|aws[_-]?secret)[^=\s]*\s*[:=]\s*\S+/gi,
  /(?:password|passwd|client_secret|clientsecret|access_token|accesstoken|bearer|api_key|apikey|private_key)\s*[:=]\s*\S+/gi,
];

function sanitizeString(str: string): string {
  let result = str;
  for (const pattern of SENSITIVE_STRING_PATTERNS) {
    pattern.lastIndex = 0;
    result = result.replace(pattern, "[REDACTED]");
  }
  return result;
}

export function sanitizeForLog(value: unknown, depth = 0): unknown {
  return sanitize(value, depth);
}

function sanitize(value: unknown, depth = 0): unknown {
  if (value === null || value === undefined) return value;

  if (typeof value === "string") return sanitizeString(value);
  if (typeof value !== "object") return value;
  // Too deep to mask reliably: drop it rather than risk logging a secret.
  if (depth > 12) return "[TRUNCATED]";

  if (Array.isArray(value)) {
    return value.map((item) => sanitize(item, depth + 1));
  }

  const result: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(value as Record<string, unknown>)) {
    result[key] = SENSITIVE_KEYS.has(key.toLowerCase())
      ? "[REDACTED]"
      : sanitize(val, depth + 1);
  }
  return result;
}

function compact(obj: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(obj).filter(([, v]) => v !== undefined),
  );
}

function fromError(
  err: Error,
): Pick<ErrorMeta, "error_type" | "error_message" | "stack_trace"> {
  return {
    error_type: err.constructor.name || "Error",
    error_message: err.message,
    stack_trace: err.stack,
  };
}

/** Domains whose events are all reported under the one "system" category. */
const SYSTEM_DOMAINS = new Set(["server", "express", "logger"]);

function categoryOf(event: unknown): string | undefined {
  if (typeof event !== "string") return undefined;
  const domain = event.split(".", 1)[0] ?? "";
  return SYSTEM_DOMAINS.has(domain) ? "system" : domain;
}

const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;

/** The instant as an IST wall-clock ISO string, e.g. 2026-11-03T15:07:16.655+05:30. */
export function toIst(date: Date): string {
  return new Date(date.getTime() + IST_OFFSET_MS)
    .toISOString()
    .replace("Z", "+05:30");
}

function buildEntry(
  level: string,
  message: string,
  extra: Record<string, unknown>,
): winston.LogEntry {
  const category = categoryOf(extra.event);
  const now = new Date();
  return sanitize({
    level,
    message: sanitizeString(message),
    timestamp: now.toISOString(),
    timestamp_ist: toIst(now),
    service: SERVICE,
    hostname: HOSTNAME,
    environment: ENV,
    ...(category ? { category } : {}),
    ...compact(extra),
    boot_id: BOOT_ID,
    log_seq: ++logSeq,
  }) as winston.LogEntry;
}

interface CloudWatchTransportOpts {
  region: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  logGroup: string;
  logStream: string;
  /** Replaces the AWS client; for tests. */
  client?: { send(command: unknown): Promise<unknown> };
}

interface BufferedEvent {
  timestamp: number;
  message: string;
  seq?: number;
}

/** In-memory events kept while CloudWatch is slow or unreachable; the oldest are dropped beyond this. */
const MAX_BUFFERED_EVENTS = 10_000;
// PutLogEvents limits: 10,000 events and 1,048,576 bytes per call (26 bytes of overhead per event).
const MAX_BATCH_EVENTS = 10_000;
const MAX_BATCH_BYTES = 1_000_000;
const EVENT_OVERHEAD_BYTES = 26;
// A single event may not exceed 256 KiB.
const MAX_EVENT_BYTES = 200_000;
const INIT_RETRY_MS = 30_000;

/** Fields kept when an entry is too large to ship whole. */
const IDENTITY_KEYS = [
  "level",
  "message",
  "timestamp",
  "timestamp_ist",
  "service",
  "hostname",
  "environment",
  "boot_id",
  "log_seq",
  "event",
  "category",
  "component",
  "processing_type",
  "function_name",
  "stage",
  "flow_status",
  "failure_category",
  "status",
  "severity",
  "outcome",
  "request_id",
  "correlation_id",
  "job_id",
  "file_id",
  "file_name",
  "error_code",
  "warning_code",
];

export class CloudWatchTransport extends Transport {
  private readonly client: { send(command: unknown): Promise<unknown> };
  private readonly logGroup: string;
  private readonly logStream: string;
  private buffer: BufferedEvent[] = [];
  private ready = false;
  private flushing: Promise<void> | null = null;
  private readonly flushTimer: NodeJS.Timeout;

  private initStatus: "connecting" | "connected" | "failed" = "connecting";
  private initError?: string;
  private initInFlight = false;
  private lastInitAttempt = 0;
  private readonly initPromise: Promise<void>;
  private initResolve!: () => void;

  private dropped = 0;
  private firstDroppedSeq?: number;
  private lastDroppedSeq?: number;
  private dropWarned = false;

  constructor(opts: CloudWatchTransportOpts) {
    super({ level: "info" });

    this.initPromise = new Promise<void>((resolve) => {
      this.initResolve = resolve;
    });

    this.client =
      opts.client ??
      new CloudWatchLogsClient({
        region: opts.region,
        ...(opts.accessKeyId && opts.secretAccessKey
          ? {
              credentials: {
                accessKeyId: opts.accessKeyId,
                secretAccessKey: opts.secretAccessKey,
              },
            }
          : {}),
      });
    this.logGroup = opts.logGroup;
    this.logStream = opts.logStream;

    this.ensureLogStream().catch((err: Error) =>
      console.error(
        "[CloudWatchTransport] Init error:",
        sanitizeString(err.message),
      ),
    );

    this.flushTimer = setInterval(() => {
      if (!this.ready) {
        this.retryInitIfDue();
        return;
      }
      this.flush().catch(() => {});
    }, 2_000);
    this.flushTimer.unref?.();
  }

  async getStatus(): Promise<{ connected: boolean; error?: string }> {
    await this.initPromise;
    return this.initStatus === "connected"
      ? { connected: true }
      : { connected: false, error: this.initError };
  }

  private async ensureLogStream(): Promise<void> {
    if (this.initInFlight) return;
    this.initInFlight = true;
    this.lastInitAttempt = Date.now();
    try {
      await this.client.send(
        new CreateLogStreamCommand({
          logGroupName: this.logGroup,
          logStreamName: this.logStream,
        }),
      );
      this.initStatus = "connected";
      this.initError = undefined;
    } catch (err) {
      const error = err as { name?: string; message?: string } | undefined;
      if (error?.name === "ResourceAlreadyExistsException") {
        this.initStatus = "connected";
        this.initError = undefined;
      } else {
        this.initStatus = "failed";
        this.initError = sanitizeString(error?.message ?? String(err));
        console.error(
          "[CloudWatchTransport] Could not create log group/stream:",
          this.initError,
        );
      }
    } finally {
      this.initInFlight = false;
    }
    // Only a connected stream is flushed to; a failed one is retried by the timer.
    this.ready = this.initStatus === "connected";
    this.initResolve();
  }

  private retryInitIfDue(): void {
    if (
      this.initInFlight ||
      Date.now() - this.lastInitAttempt < INIT_RETRY_MS
    ) {
      return;
    }
    this.ensureLogStream().catch(() => {});
  }

  log(info: Record<string | symbol, unknown>, callback: () => void): void {
    let entry: Record<string, unknown> = {};
    for (const key of Object.keys(info)) {
      entry[key] = info[key];
    }
    let message = JSON.stringify(entry);
    if (Buffer.byteLength(message) > MAX_EVENT_BYTES) {
      const shrunk: Record<string, unknown> = {
        truncated: true,
        original_bytes: Buffer.byteLength(message),
      };
      for (const key of IDENTITY_KEYS) {
        if (entry[key] !== undefined) shrunk[key] = entry[key];
      }
      entry = shrunk;
      message = JSON.stringify(entry);
    }
    // The entry's own time, so CloudWatch time-range filters agree with `timestamp`.
    const parsed =
      typeof entry.timestamp === "string" ? Date.parse(entry.timestamp) : NaN;
    this.buffer.push({
      timestamp: Number.isNaN(parsed) ? Date.now() : parsed,
      message,
      seq: typeof entry.log_seq === "number" ? entry.log_seq : undefined,
    });
    this.enforceCap();
    if (this.buffer.length >= 100) this.flush().catch(() => {});
    callback();
  }

  private enforceCap(): void {
    const excess = this.buffer.length - MAX_BUFFERED_EVENTS;
    if (excess > 0) this.recordDropped(this.buffer.splice(0, excess));
  }

  private recordDropped(events: BufferedEvent[]): void {
    for (const e of events) {
      if (e.seq === undefined) continue;
      this.firstDroppedSeq = Math.min(this.firstDroppedSeq ?? e.seq, e.seq);
      this.lastDroppedSeq = Math.max(this.lastDroppedSeq ?? e.seq, e.seq);
    }
    this.dropped += events.length;
    if (!this.dropWarned) {
      this.dropWarned = true;
      // Not via the logger: it would only land in the buffer that just overflowed.
      console.warn(
        JSON.stringify({
          level: "warn",
          message:
            "CloudWatch log buffer overflowed; dropping the oldest log events",
          event: EVENTS.LOGGER_EVENTS_DROPPED,
          timestamp: new Date().toISOString(),
          service: SERVICE,
        }),
      );
    }
  }

  /** Once shipping works again, records in the stream itself how much was lost and where. */
  private appendDropSummary(): void {
    if (this.dropped === 0) return;
    const entry = buildEntry(
      "warn",
      "Log events were dropped before reaching CloudWatch",
      {
        component: "logger",
        event: EVENTS.LOGGER_EVENTS_DROPPED,
        status: "degraded",
        severity: 6,
        warning_code: "LOG_EVENTS_DROPPED",
        dropped_count: this.dropped,
        first_dropped_seq: this.firstDroppedSeq,
        last_dropped_seq: this.lastDroppedSeq,
        tags: ["logger", "cloudwatch"],
      },
    ) as unknown as Record<string, unknown>;
    this.buffer.push({
      timestamp: Date.now(),
      message: JSON.stringify(entry),
      seq: typeof entry.log_seq === "number" ? entry.log_seq : undefined,
    });
    this.dropped = 0;
    this.firstDroppedSeq = undefined;
    this.lastDroppedSeq = undefined;
    this.dropWarned = false;
  }

  private takeBatch(): BufferedEvent[] {
    let bytes = 0;
    let count = 0;
    while (count < this.buffer.length && count < MAX_BATCH_EVENTS) {
      const size =
        Buffer.byteLength(this.buffer[count]!.message) + EVENT_OVERHEAD_BYTES;
      if (count > 0 && bytes + size > MAX_BATCH_BYTES) break;
      bytes += size;
      count++;
    }
    // PutLogEvents needs a batch in chronological order.
    return this.buffer
      .splice(0, count)
      .sort((a, b) => a.timestamp - b.timestamp);
  }

  /** One flush at a time, so a batch is never in flight twice; a concurrent caller waits for the running one. */
  flush(): Promise<void> {
    if (this.flushing) return this.flushing;
    if (!this.ready) return Promise.resolve();
    this.flushing = this.drain().finally(() => {
      this.flushing = null;
    });
    return this.flushing;
  }

  private async drain(): Promise<void> {
    this.appendDropSummary();
    while (this.buffer.length > 0) {
      const events = this.takeBatch();
      try {
        await this.client.send(
          new PutLogEventsCommand({
            logGroupName: this.logGroup,
            logStreamName: this.logStream,
            logEvents: events,
          }),
        );
      } catch (err) {
        const error = err as { name?: string; message?: string } | undefined;
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          "[CloudWatchTransport] PutLogEvents error:",
          sanitizeString(message),
        );
        if (error?.name === "InvalidParameterException") {
          // Retrying a batch CloudWatch rejects outright would block every later one.
          this.recordDropped(events);
        } else {
          if (error?.name === "ResourceNotFoundException") {
            // Stream or group removed: have the timer recreate it.
            this.ready = false;
            this.initStatus = "failed";
            this.initError = sanitizeString(message);
            this.lastInitAttempt = 0;
          }
          this.buffer.unshift(...events);
          this.enforceCap();
        }
        return;
      }
    }
  }

  async close(): Promise<void> {
    clearInterval(this.flushTimer);
    await this.flush();
  }
}

const transports: winston.transport[] = [
  new winston.transports.Console({
    format: winston.format.json(),
  }),
];

const { aws } = config;
let cwTransport: CloudWatchTransport | null = null;

if (aws.logGroup && aws.logStream) {
  cwTransport = new CloudWatchTransport({
    region: aws.region,
    accessKeyId: aws.accessKeyId || undefined,
    secretAccessKey: aws.secretAccessKey || undefined,
    logGroup: aws.logGroup,
    logStream: aws.logStream,
  });
  transports.push(cwTransport);
}

export async function checkCloudWatchConnection(): Promise<{
  connected: boolean;
  configured: boolean;
  error?: string;
}> {
  if (!cwTransport) {
    return { connected: false, configured: false };
  }
  const status = await cwTransport.getStatus();
  return { configured: true, ...status };
}

export async function closeLogger(): Promise<void> {
  if (cwTransport) {
    await cwTransport.close();
  }
}

const winstonLogger = winston.createLogger({
  level: IS_PROD ? "info" : config.logging.level || "debug",
  format: winston.format.json(),
  transports,
});

/** Builds and writes an entry; filtered levels are skipped first so they never consume a log_seq. */
function emit(
  level: "info" | "warn" | "error" | "debug",
  message: string,
  extra: Record<string, unknown>,
): void {
  if (!winstonLogger.isLevelEnabled(level)) return;
  winstonLogger.log(buildEntry(level, message, extra));
}

/*
 * Severity scale (info 1-3, warn 5-6, error 7-10; 4 is unused):
 *  1 debug | 2 normal operation | 3 notable lifecycle
 *  5 recoverable or client-caused | 6 degraded, needs attention, nothing lost yet
 *  7 error, no data loss | 8 data diverted or at risk | 9 data-loss risk | 10 process cannot run
 */
export const logger = {
  info(message: string, meta: InfoMeta = {}): void {
    emit("info", message, {
      ...meta,
      component: meta.component ?? "app",
      correlation_id: meta.correlation_id,
      request_id: meta.request_id,
      status: meta.status ?? "success",
      duration_ms: meta.duration_ms,
      span_id: meta.span_id,
      trace_id: meta.trace_id,
      parent_span_id: meta.parent_span_id,
      tags: meta.tags ?? [],
      severity: meta.severity ?? 2,
    });
  },

  warn(message: string, meta: WarnMeta = {}): void {
    emit("warn", message, {
      ...meta,
      component: meta.component ?? "app",
      correlation_id: meta.correlation_id,
      request_id: meta.request_id,
      status: meta.status ?? "degraded",
      warning_code: meta.warning_code,
      warning_message: meta.warning_message,
      warning_type: meta.warning_type,
      duration_ms: meta.duration_ms,
      threshold_ms: meta.threshold_ms,
      actual_ms: meta.actual_ms,
      span_id: meta.span_id,
      trace_id: meta.trace_id,
      parent_span_id: meta.parent_span_id,
      tags: meta.tags ?? [],
      severity: meta.severity ?? 5,
    });
  },

  error(message: string, metaOrError: ErrorMeta | Error | unknown = {}): void {
    let meta: ErrorMeta;

    if (metaOrError instanceof Error) {
      meta = fromError(metaOrError);
    } else {
      meta = (metaOrError ?? {}) as ErrorMeta;
      if (meta.error instanceof Error) {
        meta = {
          ...fromError(meta.error),
          error_message: meta.error_message ?? meta.error.message,
          ...meta,
          error: undefined,
        };
      }
    }

    emit("error", message, {
      ...meta,
      component: meta.component ?? "app",
      correlation_id: meta.correlation_id,
      request_id: meta.request_id,
      status: "failed",
      error_code: meta.error_code,
      error_message: meta.error_message,
      error_type: meta.error_type,
      stack_trace: meta.stack_trace,
      duration_ms: meta.duration_ms,
      retry_count: meta.retry_count,
      span_id: meta.span_id,
      trace_id: meta.trace_id,
      parent_span_id: meta.parent_span_id,
      tags: meta.tags ?? [],
      severity: meta.severity ?? 7,
    });
  },

  debug(message: string, meta: Record<string, unknown> = {}): void {
    if (!IS_PROD) {
      emit("debug", message, {
        component: meta.component ?? "app",
        tags: meta.tags ?? [],
        severity: 1,
        ...compact(meta),
      });
    }
  },
};

export default logger;
