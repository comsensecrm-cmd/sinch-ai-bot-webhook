/**
 * Shared engine for the Failure and DLQ reprocessing scripts.
 *
 * For each CSV in the SFTP directory: validate every record, push valid ones to the
 * input queue, merge invalid ones into the "reprocessing" CSV in the same directory,
 * and delete the original only once everything above has succeeded. Any earlier
 * failure leaves the original in place; re-running is safe because the input worker
 * skips any payload id it has already written to the CSV, and the reprocessing CSV is
 * de-duplicated by JobId.
 */

import { randomUUID } from "crypto";
import SftpClient from "ssh2-sftp-client";
import config from "../config/env";
import { correlationOpts } from "../queue/producer";
import { inputQueue, redisConnection } from "../queue/queues";
import {
  createFlow,
  tagFailedStep,
  type FailedStep,
  type ProcessingType,
} from "../utils/flowLogger";
import { EVENTS } from "../utils/logEvents";
import { validatePayload } from "../validation/payloadValidator";
import { parseCsv, toCsv, type CsvRow } from "./csv";

export interface ReprocessSpec {
  type: "failure" | "dlq";
  remoteDir: string;
  /** Only files matching this pattern are ever touched. */
  filePattern: RegExp;
  /** Name of the CSV that collects records that are still invalid. */
  outputFileName: string;
  columns: readonly string[];
}

export interface FileResult {
  file: string;
  status: "processed" | "failed";
  totalRecords: number;
  queued: number;
  invalid: number;
  enqueueFailed: number;
  alreadyRetained: number;
  originalDeleted: boolean;
  error?: string;
}

export interface ReprocessSummary {
  type: ReprocessSpec["type"];
  filesFound: number;
  filesProcessed: number;
  filesFailed: number;
  totalRecords: number;
  queued: number;
  invalid: number;
  enqueueFailed: number;
  files: FileResult[];
}

export class ReprocessBusyError extends Error {
  constructor(type: string) {
    super(`A ${type} reprocessing run is already in progress`);
  }
}

export class ReprocessConfigError extends Error {}
export class ReprocessFileError extends Error {}

const LOCK_TTL_MS = 30 * 60 * 1000;
// Files modified more recently than this may still be mid-upload by the live uploader.
const SETTLE_MS = 30 * 1000;

const RELEASE_LOCK_SCRIPT = `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`;

const errorMessage = (err: unknown) =>
  err instanceof Error ? err.message : String(err);

async function acquireLock(type: string): Promise<() => Promise<void>> {
  const key = `sinch-ai-bot-webhook:reprocess-lock:${type}`;
  const token = randomUUID();
  const ok = await redisConnection.set(key, token, "PX", LOCK_TTL_MS, "NX");
  if (ok !== "OK") throw new ReprocessBusyError(type);
  return async () => {
    await redisConnection
      .eval(RELEASE_LOCK_SCRIPT, 1, key, token)
      .catch(() => undefined);
  };
}

const processingType = (spec: ReprocessSpec): ProcessingType =>
  spec.type === "dlq" ? "DLQ_REPROCESS" : "FAILURE_REPROCESS";

/** The logger for one reprocess run; `requestId` is the id of the HTTP call that started it. */
function flowFor(spec: ReprocessSpec, requestId: string | undefined) {
  return createFlow({
    type: processingType(spec),
    component: `reprocess.${spec.type}`,
    correlationId: requestId,
  });
}

export async function reprocess(
  spec: ReprocessSpec,
  fileName?: string,
  requestId?: string,
): Promise<ReprocessSummary> {
  const { host, username } = config.sftp;
  if (!host || !username || !spec.remoteDir) {
    throw new ReprocessConfigError(
      `SFTP is not configured for ${spec.type} reprocessing (host, username and remote directory required)`,
    );
  }
  if (fileName !== undefined && !spec.filePattern.test(fileName)) {
    throw new ReprocessFileError(
      `"${fileName}" is not a ${spec.type} CSV file name`,
    );
  }

  const release = await acquireLock(spec.type);
  const sftp = new SftpClient();
  const flow = flowFor(spec, requestId);
  const tags = ["reprocess", spec.type];

  try {
    try {
      await sftp.connect({
        host,
        port: config.sftp.port,
        username,
        password: config.sftp.password,
      });
    } catch (err) {
      throw tagFailedStep(err, {
        functionName: "sftp.connect",
        stage: "SFTP_CONNECT",
        category: "FILE_OPERATION",
      });
    }

    let listed: Awaited<ReturnType<SftpClient["list"]>>;
    try {
      listed = await sftp.list(spec.remoteDir);
    } catch (err) {
      throw tagFailedStep(err, {
        functionName: "sftp.list",
        stage: "SFTP_LIST",
        category: "FILE_OPERATION",
      });
    }
    const now = Date.now();
    let files = listed
      .filter((f) => f.type === "-" && spec.filePattern.test(f.name))
      .filter((f) => fileName === undefined || f.name === fileName);

    if (fileName !== undefined && files.length === 0) {
      throw new ReprocessFileError(
        `${fileName} not found in ${spec.remoteDir}`,
      );
    }
    const unsettled = files.filter((f) => now - f.modifyTime < SETTLE_MS);
    if (unsettled.length > 0) {
      flow({
        functionName: "reprocess",
        stage: "FILE_SETTLE_CHECK",
        status: "FAILED",
        level: "warn",
        message: `Skipping ${unsettled.length} recently modified file(s) that may still be uploading`,
        errorCode: "REPROCESS_FILE_NOT_SETTLED",
        errorMessage: "Files modified in the last 30s may still be mid-upload",
        failureCategory: "FILE_OPERATION",
        tags,
        extra: { files: unsettled.map((f) => f.name) },
      });
      files = files.filter((f) => !unsettled.includes(f));
    }
    const names = files.map((f) => f.name).sort();

    flow({
      functionName: "reprocess",
      stage: "FILES_LISTED",
      status: "STARTED",
      message: `Reprocessing started: ${names.length} file(s) found`,
      tags,
      extra: { files: names },
    });

    const results: FileResult[] = [];
    for (const name of names) {
      results.push(await processFile(sftp, spec, name, requestId));
    }

    const summary = summarize(spec, names.length, results);
    flow({
      functionName: "reprocess",
      stage: "RUN_FINISHED",
      status: "SUCCESS",
      message: "Reprocessing finished",
      outcome: "success",
      responseBody: summary,
      tags,
      extra: { summary },
    });
    return summary;
  } finally {
    await sftp.end().catch(() => undefined);
    await release();
  }
}

function summarize(
  spec: ReprocessSpec,
  filesFound: number,
  files: FileResult[],
): ReprocessSummary {
  const sum = (pick: (f: FileResult) => number) =>
    files.reduce((total, f) => total + pick(f), 0);
  return {
    type: spec.type,
    filesFound,
    filesProcessed: files.filter((f) => f.status === "processed").length,
    filesFailed: files.filter((f) => f.status === "failed").length,
    totalRecords: sum((f) => f.totalRecords),
    queued: sum((f) => f.queued),
    invalid: sum((f) => f.invalid),
    enqueueFailed: sum((f) => f.enqueueFailed),
    files,
  };
}

async function processFile(
  sftp: SftpClient,
  spec: ReprocessSpec,
  file: string,
  requestId: string | undefined,
): Promise<FileResult> {
  const result: FileResult = {
    file,
    status: "failed",
    totalRecords: 0,
    queued: 0,
    invalid: 0,
    enqueueFailed: 0,
    alreadyRetained: 0,
    originalDeleted: false,
  };
  const remotePath = `${spec.remoteDir}/${file}`;
  const flow = flowFor(spec, requestId);
  const fileCtx = { file, file_name: file, report_type: spec.type };
  const tags = ["reprocess", spec.type];
  // The step in progress, so a failure names the one that really failed.
  let step: FailedStep = {
    functionName: "sftp.get",
    stage: "FILE_DOWNLOAD",
    category: "FILE_OPERATION",
  };

  try {
    flow({
      functionName: "processFile",
      stage: "FILE_STARTED",
      status: "STARTED",
      message: "Reprocessing file started",
      tags,
      extra: fileCtx,
    });
    const content = (await sftp.get(remotePath)) as Buffer;
    step = {
      functionName: "readRecords",
      stage: "CSV_PARSE",
      category: "DATA_TRANSFORMATION",
    };
    const records = readRecords(content.toString("utf8"), spec, file);
    result.totalRecords = records.length;

    const retained: CsvRow[] = [];
    const { batchSize } = config.reprocess;

    for (let start = 0; start < records.length; start += batchSize) {
      const batch: { record: CsvRow; payload: unknown }[] = [];

      for (const record of records.slice(start, start + batchSize)) {
        step = {
          functionName: "parsePayload",
          stage: "PAYLOAD_PARSE",
          category: "DATA_TRANSFORMATION",
        };
        const parsed = parsePayload(record.RawPayload);
        step = {
          functionName: "validatePayload",
          stage: "VALIDATE",
          category: "VALIDATION",
        };
        const validation = parsed.ok ? validatePayload(parsed.payload) : null;
        if (!parsed.ok || !validation?.valid) {
          const reason = !parsed.ok
            ? parsed.error
            : validation!.errors.join("; ");
          retained.push({ ...record, Reason: reason });
          result.invalid++;
          flow({
            functionName: !parsed.ok ? "parsePayload" : "validatePayload",
            stage: !parsed.ok ? "PAYLOAD_PARSE" : "VALIDATE",
            status: "FAILED",
            level: "warn",
            message: "Reprocess record is invalid; kept in reprocessing CSV",
            event: EVENTS.REPROCESS_RECORD_INVALID,
            outcome: "failure",
            jobId: record.JobId,
            requestBody: parsed.ok ? parsed.payload : record.RawPayload,
            errorCode: "REPROCESS_INVALID_RECORD",
            errorMessage: reason,
            failureCategory: !parsed.ok ? "DATA_TRANSFORMATION" : "VALIDATION",
            tags: [...tags, "record-invalid"],
            severity: 6,
            extra: fileCtx,
          });
          continue;
        }
        batch.push({ record, payload: parsed.payload });
      }
      if (batch.length === 0) continue;

      step = {
        functionName: "inputQueue.addBulk",
        stage: "INPUT_QUEUE_INSERT",
        category: "DATABASE_OPERATION",
      };
      try {
        // No custom jobId: BullMQ silently ignores an add whose id already
        // exists, which would count the record as queued while dropping it.
        const opts = correlationOpts(requestId);
        const jobs = await inputQueue.addBulk(
          batch.map(({ payload }) => ({
            name: "call-log",
            data: payload,
            ...(opts && { opts }),
          })),
        );
        result.queued += batch.length;
        // One entry per record, each with the exact payload put on the input queue.
        batch.forEach(({ record, payload }, i) =>
          flow({
            functionName: "processFile",
            stage: "INPUT_QUEUE_INSERT",
            status: "SUCCESS",
            message: "Record re-queued to the input queue",
            jobId: jobs[i]?.id,
            requestBody: payload,
            tags,
            extra: { ...fileCtx, source_job_id: record.JobId },
          }),
        );
      } catch (err) {
        const reason = `Input queue insertion failed: ${errorMessage(err)}`;
        for (const { record } of batch)
          retained.push({ ...record, Reason: reason });
        result.enqueueFailed += batch.length;
        flow({
          functionName: "inputQueue.addBulk",
          stage: "INPUT_QUEUE_INSERT",
          status: "FAILED",
          message: "Could not add records to the input queue",
          event: EVENTS.REPROCESS_ENQUEUE_FAILED,
          outcome: "failure",
          requestBody: batch.map(({ payload }) => payload),
          error: err,
          errorCode: "REPROCESS_ENQUEUE_FAILED",
          failureCategory: "DATABASE_OPERATION",
          tags: [...tags, "enqueue-failed"],
          extra: {
            ...fileCtx,
            record_count: batch.length,
            success_count: 0,
            failed_count: batch.length,
            source_job_ids: batch.map(({ record }) => record.JobId),
          },
        });
      }
    }

    if (retained.length > 0) {
      step = {
        functionName: "mergeAndUpload",
        stage: "RETAINED_UPLOAD",
        category: "FILE_OPERATION",
      };
      result.alreadyRetained = await mergeAndUpload(sftp, spec, retained);
    }

    step = {
      functionName: "sftp.delete",
      stage: "FILE_DELETE",
      category: "FILE_OPERATION",
    };
    await sftp.delete(remotePath);
    result.originalDeleted = true;
    result.status = "processed";
    flow({
      functionName: "processFile",
      stage: "FILE_FINISHED",
      status: "SUCCESS",
      message: "Reprocessed file",
      event: EVENTS.REPROCESS_FILE_DONE,
      outcome: "success",
      responseBody: result,
      tags: [...tags, "file-done"],
      extra: {
        ...fileCtx,
        record_count: result.totalRecords,
        success_count: result.queued,
        failed_count: result.invalid + result.enqueueFailed,
      },
    });
  } catch (err) {
    result.error = errorMessage(err);
    flow({
      functionName: "processFile",
      stage: "FILE_FAILED",
      status: "FAILED",
      message: "Reprocessing file failed; original file kept on SFTP",
      event: EVENTS.REPROCESS_FILE_FAILED,
      outcome: "failure",
      responseBody: result,
      error: err,
      errorCode: "REPROCESS_FILE_FAILED",
      failureCategory: step.category,
      failedStep: step,
      tags: [...tags, "file-failed"],
      extra: {
        ...fileCtx,
        record_count: result.totalRecords,
        success_count: result.queued,
        failed_count: result.invalid + result.enqueueFailed,
      },
    });
  }
  return result;
}

function readRecords(
  text: string,
  spec: ReprocessSpec,
  file: string,
): CsvRow[] {
  const [header, ...rows] = parseCsv(text);
  if (!header) throw new Error(`${file} is empty`);
  // coms_id was added later; files written before that still need to be replayable.
  const missing = spec.columns.filter(
    (c) => c !== "coms_id" && !header.includes(c),
  );
  if (missing.length > 0) {
    throw new Error(`${file} is missing column(s): ${missing.join(", ")}`);
  }
  return rows.map((cells) =>
    Object.fromEntries(header.map((name, i) => [name, cells[i] ?? ""])),
  );
}

function parsePayload(
  raw: string,
): { ok: true; payload: unknown } | { ok: false; error: string } {
  try {
    return { ok: true, payload: JSON.parse(raw) };
  } catch {
    return { ok: false, error: "RawPayload is not valid JSON" };
  }
}

/**
 * Merges rows into the existing reprocessing CSV (if any) and uploads it via a temp
 * file so a failed upload can never truncate rows retained by an earlier run.
 * Returns how many rows were already present (de-duplicated by JobId).
 */
async function mergeAndUpload(
  sftp: SftpClient,
  spec: ReprocessSpec,
  rows: CsvRow[],
): Promise<number> {
  const finalPath = `${spec.remoteDir}/${spec.outputFileName}`;
  const tmpPath = `${finalPath}.tmp`;

  let existing: CsvRow[] = [];
  if (await sftp.exists(finalPath)) {
    const text = ((await sftp.get(finalPath)) as Buffer).toString("utf8");
    existing = readRecords(text, spec, spec.outputFileName);
  }

  const known = new Set(existing.map((r) => r.JobId));
  const fresh = rows.filter((r) => !known.has(r.JobId));
  const merged = [...existing, ...fresh];
  const body = Buffer.from(toCsv(spec.columns, merged), "utf8");

  await sftp.put(body, tmpPath);
  const uploaded = await sftp.stat(tmpPath);
  if (uploaded.size !== body.length) {
    throw new Error(
      `Upload verification failed for ${spec.outputFileName} (${uploaded.size} of ${body.length} bytes)`,
    );
  }
  try {
    await sftp.posixRename(tmpPath, finalPath);
  } catch {
    // Server lacks posix-rename: replace the old copy; the new one is safe in tmpPath.
    await sftp.delete(finalPath).catch(() => undefined);
    await sftp.rename(tmpPath, finalPath);
  }
  return rows.length - fresh.length;
}
