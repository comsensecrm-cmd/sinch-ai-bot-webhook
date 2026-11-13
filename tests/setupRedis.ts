// Preloaded (after setup.ts) for every Redis-backed suite: integration, api, e2e, reliability,
// performance and regression. It runs before any src/ module is imported, so config picks these up.
//
// dotenv never overrides a variable that is already set, so a developer's .env (real Redis, real
// SFTP, real CloudWatch) cannot leak into a test run.

import fs from "fs";
import os from "os";
import path from "path";

const DEFAULT_TEST_REDIS_URL = "redis://127.0.0.1:4998";
// The real instances: docker-compose.yml (4112) and the .env.example default (4001).
const FORBIDDEN_PORTS = new Set(["4112", "4001", "6379"]);
const LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "::1", "[::1]"]);

const redisUrl = process.env.TEST_REDIS_URL || DEFAULT_TEST_REDIS_URL;
const parsed = new URL(redisUrl);
if (!LOCAL_HOSTS.has(parsed.hostname) || FORBIDDEN_PORTS.has(parsed.port)) {
  // The suites flush the database they are given.
  throw new Error(
    `Refusing to run tests against ${redisUrl}: TEST_REDIS_URL must be a local, throwaway Redis ` +
      `(default ${DEFAULT_TEST_REDIS_URL}, start it with "npm run test:redis:up").`,
  );
}

Object.assign(process.env, {
  NODE_ENV: "test",
  REDIS_URL: redisUrl,
  REDIS_PASSWORD: "",
  // Each test process gets its own CSV directory.
  CSV_OUTPUT_DIR: fs.mkdtempSync(path.join(os.tmpdir(), "sinch-webhook-test-")),
  // Short retries, so reliability tests don't wait on the production backoff.
  QUEUE_MAX_ATTEMPTS: "3",
  QUEUE_RETRY_DELAY_MS: "50",
  BULL_BOARD_ENABLED: "false",
  // Failures are provoked on purpose; keep the output readable.
  LOG_LEVEL: "error",
  AWS_ACCESS_KEY_ID: "",
  AWS_SECRET_ACCESS_KEY: "",
  SFTP_PASSWORD: "",
  SFTP_REMOTE_DIR: "",
  SFTP_FAILURE_REMOTE_DIR: "",
  SFTP_DLQ_REMOTE_DIR: "",
});

import "./quietLogs";
