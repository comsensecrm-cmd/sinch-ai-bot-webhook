import dotenv from "dotenv";

dotenv.config();

function int(name: string, fallback: number): number {
  return parseInt(process.env[name] || String(fallback), 10);
}

const config = {
  nodeEnv: process.env.NODE_ENV || "development",

  port: int("PORT", 3000),
  host: process.env.HOST || "0.0.0.0",

  redis: {
    url: process.env.REDIS_URL || "redis://localhost:4001",
    password: process.env.REDIS_PASSWORD || undefined,
  },

  // The dashboard can retry and delete jobs and has no auth, so it is off in production unless enabled.
  bullBoard: {
    enabled: process.env.BULL_BOARD_ENABLED
      ? process.env.BULL_BOARD_ENABLED === "true"
      : (process.env.NODE_ENV || "development") !== "production",
    path: process.env.BULL_BOARD_PATH || "/admin/queues",
  },

  queue: {
    maxAttempts: int("QUEUE_MAX_ATTEMPTS", 3),
    retryDelayMs: int("QUEUE_RETRY_DELAY_MS", 2000),
  },

  csv: {
    outputDir: process.env.CSV_OUTPUT_DIR || "./csv_output",
    flushIntervalMinutes: int("CSV_FLUSH_INTERVAL_MINUTES", 30),
    // An open file is rotated once it reaches this size (MB); 0 disables.
    maxFileBytes: int("CSV_MAX_FILE_SIZE_MB", 50) * 1024 * 1024,
    flushTimeZone: process.env.CSV_FLUSH_TIMEZONE || "Asia/Kolkata",
  },

  sftp: {
    host: process.env.SFTP_HOST || "",
    port: int("SFTP_PORT", 22),
    username: process.env.SFTP_USERNAME || "",
    password: process.env.SFTP_PASSWORD || "",
    remoteDir: process.env.SFTP_REMOTE_DIR || "/import",
    failureRemoteDir: process.env.SFTP_FAILURE_REMOTE_DIR || "",
    dlqRemoteDir: process.env.SFTP_DLQ_REMOTE_DIR || "",
    maxRetries: int("SFTP_MAX_RETRIES", 5),
    retryDelayMs: int("SFTP_RETRY_DELAY_MS", 2000),
    // How long a file whose retries were exhausted waits before it is queued again.
    failedRetryIntervalMs: int("SFTP_FAILED_RETRY_INTERVAL_MS", 300_000),
  },

  reprocess: {
    batchSize: int("REPROCESS_BATCH_SIZE", 500),
  },

  logging: {
    level: process.env.LOG_LEVEL || "info",
  },

  aws: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
    region: process.env.AWS_REGION || "ap-south-1",
    logGroup: process.env.AWS_LOG_GROUP || "",
    logStream: process.env.AWS_LOG_STREAM || "",
  },
};

export default config;
