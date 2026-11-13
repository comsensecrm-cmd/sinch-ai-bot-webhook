/** Uploads finished CSV files to SFTP with retries. */

import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";
import SftpClient from "ssh2-sftp-client";
import config from "../config/env";
import logger from "../utils/logger";
import { EVENTS } from "../utils/logEvents";
import { fileStamp } from "../csv/csvWriter";
import { parseCsv, toCsv } from "../reprocess/csv";

export interface UploadOptions {
  /** Preferred remote name, used while free; otherwise `<its base>_<yyyyMMddHHmm>` is used. */
  defaultName?: string;
  /** Prepend a `filename` column holding the final remote name without `.csv`. */
  addFilenameColumn?: boolean;
  /** Unique per file even when the name repeats; generated when the caller has none (e.g. startup recovery). */
  fileId?: string;
  /** Which report the file belongs to (input, failure, dlq); only used for logging. */
  reportType?: string;
}

interface UploadTask extends UploadOptions {
  localPath: string;
  remoteDir: string;
  fileId: string;
}

/** The fields that identify an upload in every SFTP log entry. */
function taskContext(task: UploadTask): Record<string, unknown> {
  return {
    component: "sftp.uploader",
    file_id: task.fileId,
    file_name: path.basename(task.localPath),
    file_path: task.localPath,
    report_type: task.reportType,
    remote_dir: task.remoteDir,
  };
}

const FILENAME_COLUMN = "filename";

/** The local CSV with a leading `filename` column set to `name` (without extension) on every row. */
function withFilenameColumn(
  header: string[],
  rows: string[][],
  name: string,
): Buffer {
  const filename = name.replace(/\.csv$/i, "");
  const records = rows.map((cells) => ({
    [FILENAME_COLUMN]: filename,
    ...Object.fromEntries(header.map((h, i) => [h, cells[i] ?? ""])),
  }));
  return Buffer.from(toCsv([FILENAME_COLUMN, ...header], records), "utf8");
}

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

const MAX_NAME_ATTEMPTS = 1000;

export class SftpUploader {
  private readonly queue: UploadTask[] = [];
  private running = false;
  /** Remote paths currently being uploaded, so no two uploads claim the same name. */
  private readonly inFlight = new Set<string>();
  /** Files whose retries ran out; queued again by `retryTimer` so they don't wait for a restart. */
  private readonly failed: UploadTask[] = [];
  private retryTimer: ReturnType<typeof setTimeout> | null = null;

  enqueue(
    localPath: string,
    remoteDir: string,
    options: UploadOptions = {},
  ): void {
    const task: UploadTask = {
      localPath,
      remoteDir,
      ...options,
      fileId: options.fileId ?? randomUUID(),
    };
    this.queue.push(task);
    logger.info("SFTP upload enqueued", {
      ...taskContext(task),
      event: EVENTS.SFTP_UPLOAD_ENQUEUED,
      status: "pending",
      tags: ["sftp", "enqueued"],
    });
    void this.run();
  }

  async drain(timeoutMs = 120_000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while ((this.running || this.queue.length > 0) && Date.now() < deadline) {
      await sleep(250);
    }
    if (this.queue.length > 0) {
      logger.warn("SFTP drain timed out; files not uploaded", {
        component: "sftp.uploader",
        event: EVENTS.SFTP_DRAIN_TIMEOUT,
        outcome: "failure",
        file_count: this.queue.length,
        warning_code: "SFTP_DRAIN_TIMEOUT",
        tags: ["sftp", "drain"],
        severity: 6,
      });
    }
  }

  private async run(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      for (let task = this.queue.shift(); task; task = this.queue.shift()) {
        await this.upload(task);
      }
    } finally {
      this.running = false;
    }
  }

  private scheduleFailedRetry(task: UploadTask): void {
    this.failed.push(task);
    if (this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      const tasks = this.failed.splice(0);
      for (const t of tasks) {
        if (fs.existsSync(t.localPath)) this.queue.push(t);
      }
      void this.run();
    }, config.sftp.failedRetryIntervalMs);
    // Must not keep the process alive; the file is picked up at startup anyway.
    this.retryTimer.unref();
  }

  private async upload(task: UploadTask): Promise<void> {
    const { localPath, remoteDir, ...options } = task;
    const { host, username, maxRetries, retryDelayMs } = config.sftp;
    const maxAttempts = maxRetries + 1;
    const ctx = taskContext(task);
    let fileSizeBytes: number | undefined;
    try {
      fileSizeBytes = fs.statSync(localPath).size;
    } catch {
      // The upload attempt below reports a missing file.
    }

    if (!host || !username || !remoteDir) {
      logger.warn("SFTP not configured; file kept locally", {
        ...ctx,
        event: EVENTS.SFTP_NOT_CONFIGURED,
        outcome: "skipped",
        file_size_bytes: fileSizeBytes,
        warning_code: "SFTP_NOT_CONFIGURED",
        warning_message: "host, username and remote directory are required",
        tags: ["sftp", "skipped"],
        severity: 6,
      });
      return;
    }

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        const remotePath = await this.put(localPath, remoteDir, options);
        logger.info("SFTP upload succeeded", {
          ...ctx,
          event: EVENTS.SFTP_UPLOAD_SUCCEEDED,
          outcome: "success",
          remote_path: remotePath,
          file_size_bytes: fileSizeBytes,
          attempt: attempt + 1,
          max_attempts: maxAttempts,
          retry_count: attempt,
          tags: ["sftp", "upload", "success"],
        });
        // Outside the retry path: the file is already on the server, so a failed
        // delete must not trigger a second upload under a different name.
        try {
          fs.unlinkSync(localPath);
        } catch (err) {
          logger.warn("Uploaded but could not delete local file", {
            ...ctx,
            event: EVENTS.SFTP_LOCAL_DELETE_FAILED,
            outcome: "failure",
            remote_path: remotePath,
            warning_code: "LOCAL_DELETE_FAILED",
            warning_message: err instanceof Error ? err.message : String(err),
            tags: ["sftp", "upload", "cleanup"],
            severity: 6,
          });
        }
        return;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (attempt === maxRetries) {
          // Not final: the file is queued again after failedRetryIntervalMs.
          logger.error(
            "SFTP upload failed; file kept locally and will be retried later",
            {
              ...ctx,
              event: EVENTS.SFTP_UPLOAD_FAILED,
              outcome: "failure",
              file_size_bytes: fileSizeBytes,
              attempt: attempt + 1,
              max_attempts: maxAttempts,
              retry_count: attempt,
              retry_delay_ms: config.sftp.failedRetryIntervalMs,
              will_retry: true,
              error: err,
              error_code: "SFTP_UPLOAD_FAILED",
              tags: ["sftp", "upload", "failed"],
            },
          );
          this.scheduleFailedRetry(task);
          return;
        }
        const delayMs = retryDelayMs * 2 ** attempt;
        logger.warn("SFTP upload attempt failed; retrying", {
          ...ctx,
          event: EVENTS.SFTP_UPLOAD_RETRY,
          outcome: "retry",
          attempt: attempt + 1,
          max_attempts: maxAttempts,
          retry_count: attempt,
          retry_delay_ms: delayMs,
          will_retry: true,
          warning_code: "SFTP_UPLOAD_RETRY",
          warning_message: message,
          tags: ["sftp", "upload", "retry"],
        });
        await sleep(delayMs);
      }
    }
  }

  /**
   * Uploads to a temporary `.uploading` name, then renames it to the first free
   * candidate name, so an existing remote file is never overwritten and a
   * half-written file is never visible under its final name.
   */
  private async put(
    localPath: string,
    remoteDir: string,
    { defaultName, addFilenameColumn }: UploadOptions,
  ): Promise<string> {
    const localName = path.basename(localPath);
    let parsed: { header: string[]; rows: string[][] } | undefined;
    if (addFilenameColumn) {
      const [header = [], ...rows] = parseCsv(
        fs.readFileSync(localPath, "utf8"),
      );
      if (header[0] === FILENAME_COLUMN) {
        throw new Error(`${localName} already has a ${FILENAME_COLUMN} column`);
      }
      parsed = { header, rows };
    }
    const tempPath = path.posix.join(remoteDir, `${localName}.uploading`);
    const sftp = new SftpClient();
    try {
      await sftp.connect({
        host: config.sftp.host,
        port: config.sftp.port,
        username: config.sftp.username,
        password: config.sftp.password,
      });
      await sftp.mkdir(remoteDir, true).catch(() => undefined);

      try {
        for (const name of candidateNames(localName, defaultName)) {
          const remotePath = path.posix.join(remoteDir, name);
          if (
            this.inFlight.has(remotePath) ||
            (await sftp.exists(remotePath))
          ) {
            continue;
          }
          this.inFlight.add(remotePath);
          try {
            // The filename column depends on the final name, so upload per candidate.
            await sftp.put(
              parsed
                ? withFilenameColumn(parsed.header, parsed.rows, name)
                : localPath,
              tempPath,
            );
            await sftp.rename(tempPath, remotePath);
            return remotePath;
          } catch (err) {
            // Lost a race for this name: try the next one. Anything else is a real failure.
            if (!(await sftp.exists(remotePath))) throw err;
          } finally {
            this.inFlight.delete(remotePath);
          }
        }
        throw new Error(`No free remote file name found for ${localName}`);
      } catch (err) {
        await sftp.delete(tempPath).catch(() => undefined);
        throw err;
      }
    } finally {
      await sftp.end().catch(() => undefined);
    }
  }
}

/**
 * With a default name: the default, then `<base>_<upload-time stamp>`, then that with _1, _2, …
 * Without one: the local name, then the local name with _1, _2, …
 */
function* candidateNames(
  localName: string,
  defaultName?: string,
): Generator<string> {
  const ext = path.posix.extname(defaultName ?? localName);
  let base = (defaultName ?? localName).slice(
    0,
    (defaultName ?? localName).length - ext.length,
  );
  if (defaultName) {
    yield defaultName;
    base = `${base}_${fileStamp()}`;
  }
  yield `${base}${ext}`;
  for (let n = 1; n <= MAX_NAME_ATTEMPTS; n++) yield `${base}_${n}${ext}`;
}

export const sftpUploader = new SftpUploader();
export default sftpUploader;
