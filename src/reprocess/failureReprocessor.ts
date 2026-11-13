/** Reprocesses Failure CSVs (sinch_ai_bot_webhook_failure_*.csv) from the failure SFTP directory. */

import config from "../config/env";
import { FAILURE_COLUMNS } from "../csv/reportWriters";
import { reprocess, type ReprocessSummary } from "./reprocessor";

export const reprocessFailureFiles = (
  fileName?: string,
  requestId?: string,
): Promise<ReprocessSummary> =>
  reprocess(
    {
      type: "failure",
      remoteDir: config.sftp.failureRemoteDir,
      filePattern: /^sinch_ai_bot_webhook_failure_[A-Za-z0-9_]+\.csv$/,
      outputFileName: "reprocessing_failure_records.csv",
      columns: FAILURE_COLUMNS,
    },
    fileName,
    requestId,
  );
