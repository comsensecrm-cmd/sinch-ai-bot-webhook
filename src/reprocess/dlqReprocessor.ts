/** Reprocesses DLQ CSVs (sinch_ai_bot_webhook_dlq_*.csv) from the DLQ SFTP directory. */

import config from "../config/env";
import { DLQ_COLUMNS } from "../csv/reportWriters";
import { reprocess, type ReprocessSummary } from "./reprocessor";

export const reprocessDlqFiles = (
  fileName?: string,
  requestId?: string,
): Promise<ReprocessSummary> =>
  reprocess(
    {
      type: "dlq",
      remoteDir: config.sftp.dlqRemoteDir,
      filePattern: /^sinch_ai_bot_webhook_dlq_[A-Za-z0-9_]+\.csv$/,
      outputFileName: "reprocessing_dlq_records.csv",
      columns: DLQ_COLUMNS,
    },
    fileName,
    requestId,
  );
