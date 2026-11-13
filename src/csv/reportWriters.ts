/** The input, failure and DLQ CSV reports and their SFTP directories. */

import config from "../config/env";
import { sftpUploader, type UploadOptions } from "../sftp/sftpUploader";
import { CSV_COLUMNS } from "../validation/payloadSchema";
import { CsvWriter, csvOutputDir } from "./csvWriter";

export const FAILURE_COLUMNS = [
  "JobId",
  "Reason",
  "FailedAt",
  "RawPayload",
  "coms_id",
] as const;
export const DLQ_COLUMNS = [
  "JobId",
  "Reason",
  "Attempts",
  "FailedAt",
  "RawPayload",
  "coms_id",
] as const;

function createReport(
  name: string,
  columns: readonly string[],
  remoteDir: string,
  uploadOptions: UploadOptions = {},
  primaryName?: string,
): { writer: CsvWriter; remoteDir: string; uploadOptions: UploadOptions } {
  const options: UploadOptions = { ...uploadOptions, reportType: name };
  const writer = new CsvWriter({
    name,
    dir: csvOutputDir(name),
    filePrefix: `sinch_ai_bot_webhook_${name}`,
    columns,
    primaryName,
    maxFileBytes: config.csv.maxFileBytes,
    onFileReady: (filePath, fileId) =>
      sftpUploader.enqueue(filePath, remoteDir, { ...options, fileId }),
  });
  return { writer, remoteDir, uploadOptions: options };
}

// The SFTP copy gets a leading `filename` column (40 columns); the local file keeps the 39.
const INPUT_FILE_NAME = "sinch_ai_bot_webhook_input.csv";
const inputReport = createReport(
  "input",
  CSV_COLUMNS,
  config.sftp.remoteDir,
  { defaultName: INPUT_FILE_NAME, addFilenameColumn: true },
  INPUT_FILE_NAME,
);
const failureReport = createReport(
  "failure",
  FAILURE_COLUMNS,
  config.sftp.failureRemoteDir,
);
const dlqReport = createReport("dlq", DLQ_COLUMNS, config.sftp.dlqRemoteDir);

export const inputCsvWriter = inputReport.writer;
export const failureCsvWriter = failureReport.writer;
export const dlqCsvWriter = dlqReport.writer;

export function recoverPendingReports(): number {
  let count = 0;
  for (const { writer, remoteDir, uploadOptions } of [
    inputReport,
    failureReport,
    dlqReport,
  ]) {
    for (const filePath of writer.scanPendingFiles()) {
      sftpUploader.enqueue(filePath, remoteDir, uploadOptions);
      count++;
    }
  }
  return count;
}

export async function closeReportWriters(): Promise<void> {
  await Promise.all([
    inputCsvWriter.close(),
    failureCsvWriter.close(),
    dlqCsvWriter.close(),
  ]);
}
