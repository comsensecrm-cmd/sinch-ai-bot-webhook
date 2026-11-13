/**
 * Rotating CSV writer. With `primaryName` it writes straight into that .csv (created exclusively, never
 * overwritten) and falls back to a timestamped .csv only while the primary is taken (still being uploaded)
 * or when the size limit forces a rotation. Without it, rows go to a timestamped .tmp renamed to .csv.
 * Either way the file is closed and handed to onFileReady on the schedule.
 */

import fs from "fs";
import path from "path";
import { randomUUID } from "crypto";
import config from "../config/env";
import logger from "../utils/logger";
import { EVENTS } from "../utils/logEvents";
import { ScheduledFlushTimer, flushSchedule } from "../utils/scheduledFlush";

export type CsvRecord = Record<string, string | number | null | undefined>;

export interface CsvWriterOptions {
  name: string;
  dir: string;
  filePrefix: string;
  columns: readonly string[];
  /** Preferred file name (e.g. `x_input.csv`), used whenever it is free. */
  primaryName?: string;
  /** Rotate once the open file reaches this many bytes; 0 or unset disables. */
  maxFileBytes?: number;
  /** `fileId` is unique per file, even when the file name repeats. */
  onFileReady: (filePath: string, fileId: string) => void;
}

function escapeField(value: string): string {
  return /[,"\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;
}

export function fileStamp(): string {
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: flushSchedule.timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date());
  const get = (type: string) =>
    parts.find((p) => p.type === type)?.value ?? "00";
  return `${get("year")}${get("month")}${get("day")}${get("hour")}${get("minute")}`;
}

export class CsvWriter {
  private readonly header: string;
  private readonly timer: ScheduledFlushTimer;
  private chain: Promise<void> = Promise.resolve();

  private stream: fs.WriteStream | null = null;
  /** The open file: a .csv when `primaryName` is set, otherwise a .tmp. */
  private openPath = "";
  private fileId = "";
  private recordCount = 0;
  private bytes = 0;

  constructor(private readonly opts: CsvWriterOptions) {
    this.header = opts.columns.map(escapeField).join(",") + "\r\n";
    fs.mkdirSync(opts.dir, { recursive: true });

    this.timer = new ScheduledFlushTimer(flushSchedule, () =>
      this.enqueue(() => this.rotate()).catch((err) =>
        this.logError(
          "Scheduled CSV rotation failed",
          "CSV_ROTATION_FAILED",
          err,
        ),
      ),
    );
    this.timer.start();
  }

  writeRecord(record: CsvRecord): Promise<void> {
    return this.enqueue(() => this.write(record));
  }

  async close(): Promise<void> {
    this.timer.stop();
    await this.enqueue(() => this.rotate());
  }

  scanPendingFiles(): string[] {
    const files: string[] = [];
    for (const entry of fs.readdirSync(this.opts.dir)) {
      const full = path.join(this.opts.dir, entry);
      if (entry.endsWith(".tmp")) {
        // Left by a hard stop (or an older version); never replace an existing .csv.
        const base = full.replace(/\.tmp$/, "");
        let recovered = `${base}.csv`;
        for (let n = 1; fs.existsSync(recovered); n++) {
          recovered = `${base}_${n}.csv`;
        }
        fs.renameSync(full, recovered);
        files.push(recovered);
      } else if (entry.endsWith(".csv")) {
        files.push(full);
      }
    }
    // Oldest first, so files are handed on in the order they were written.
    return files.sort(
      (a, b) => fs.statSync(a).mtimeMs - fs.statSync(b).mtimeMs,
    );
  }

  private enqueue(task: () => Promise<void>): Promise<void> {
    const run = this.chain.then(task);
    this.chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  /** Creates `filePath` exclusively; null if it already exists, so no file is ever overwritten. */
  private tryCreate(filePath: string): fs.WriteStream | null {
    let fd: number;
    try {
      fd = fs.openSync(filePath, "wx");
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "EEXIST") return null;
      throw err;
    }
    return fs.createWriteStream(filePath, { fd, encoding: "utf8" });
  }

  private *candidateNames(ext: string): Generator<string> {
    const { primaryName, filePrefix } = this.opts;
    const stamped = `${filePrefix}_${fileStamp()}`;
    if (primaryName) yield primaryName;
    yield `${stamped}${ext}`;
    for (let n = 1; ; n++) yield `${stamped}_${n}${ext}`;
  }

  private open(): fs.WriteStream {
    const { dir, primaryName } = this.opts;
    const ext = primaryName ? ".csv" : ".tmp";
    const otherExt = primaryName ? ".tmp" : ".csv";

    let stream: fs.WriteStream | null = null;
    let name = "";
    for (const candidate of this.candidateNames(ext)) {
      // A same-named file of the other kind is also off limits.
      const sibling = candidate.slice(0, -ext.length) + otherExt;
      if (fs.existsSync(path.join(dir, sibling))) continue;
      stream = this.tryCreate(path.join(dir, candidate));
      if (stream) {
        name = candidate;
        break;
      }
    }
    if (!stream) throw new Error("Could not create a CSV file");

    this.openPath = path.join(dir, name);
    this.fileId = randomUUID();
    this.recordCount = 0;
    this.bytes = Buffer.byteLength(this.header);
    stream.on("error", (err) =>
      this.logError("CSV stream error", "CSV_STREAM_ERROR", err),
    );
    stream.write(this.header);
    this.stream = stream;

    logger.info("CSV file opened", {
      component: "csv.writer",
      event: EVENTS.CSV_FILE_OPENED,
      outcome: "success",
      report_type: this.opts.name,
      file_id: this.fileId,
      file_name: name,
      file_path: this.openPath,
      tags: ["csv", this.opts.name, "file-open"],
    });
    return stream;
  }

  private async write(record: CsvRecord): Promise<void> {
    const stream = this.stream ?? this.open();
    const row =
      this.opts.columns
        .map((col) => escapeField(String(record[col] ?? "")))
        .join(",") + "\r\n";

    try {
      await new Promise<void>((resolve, reject) =>
        stream.write(row, (err) => (err ? reject(err) : resolve())),
      );
    } catch (err) {
      await this.rotate().catch(() => undefined);
      throw err;
    }
    this.recordCount++;
    this.bytes += Buffer.byteLength(row);

    const { maxFileBytes } = this.opts;
    if (maxFileBytes && this.bytes >= maxFileBytes) {
      // This row is already on disk; the next one opens a fresh file.
      await this.rotate();
    }
  }

  private async rotate(): Promise<void> {
    const stream = this.stream;
    if (!stream) return;

    const openPath = this.openPath;
    const fileId = this.fileId;
    const recordCount = this.recordCount;
    const fileSizeBytes = this.bytes;
    this.stream = null;
    this.recordCount = 0;
    this.bytes = 0;

    if (!stream.destroyed) {
      await new Promise<void>((resolve) => stream.end(resolve));
    }

    if (recordCount === 0) {
      fs.rmSync(openPath, { force: true });
      return;
    }

    let csvPath = openPath;
    if (openPath.endsWith(".tmp")) {
      // Named by rotation time (e.g. ..._202610301430), not by when the file was opened.
      const base = `${this.opts.filePrefix}_${fileStamp()}`;
      csvPath = path.join(this.opts.dir, `${base}.csv`);
      for (let n = 1; fs.existsSync(csvPath); n++) {
        csvPath = path.join(this.opts.dir, `${base}_${n}.csv`);
      }
      fs.renameSync(openPath, csvPath);
    }
    logger.info("CSV file ready", {
      component: "csv.writer",
      event: EVENTS.CSV_FILE_READY,
      outcome: "success",
      report_type: this.opts.name,
      file_id: fileId,
      file_name: path.basename(csvPath),
      file_path: csvPath,
      record_count: recordCount,
      success_count: recordCount,
      failed_count: 0,
      file_size_bytes: fileSizeBytes,
      tags: ["csv", this.opts.name, "file-ready"],
    });
    this.opts.onFileReady(csvPath, fileId);
  }

  private logError(message: string, code: string, err: unknown): void {
    logger.error(message, {
      component: "csv.writer",
      event: EVENTS.CSV_WRITE_ERROR,
      outcome: "failure",
      report_type: this.opts.name,
      file_id: this.fileId || undefined,
      file_name: this.openPath ? path.basename(this.openPath) : undefined,
      error: err,
      error_code: code,
      tags: ["csv", this.opts.name, "error"],
      severity: 8,
    });
  }
}

export const csvOutputDir = (subDir: string): string =>
  path.join(config.csv.outputDir, subDir);
