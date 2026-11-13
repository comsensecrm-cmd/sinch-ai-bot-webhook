# sinch-ai-bot-webhook

Receives Sinch AI bot call-log webhooks, queues them with BullMQ (Redis), validates and flattens them into CSV files, and uploads those files to SFTP.

**Requirements:** Node.js >= 22.22.1 and Redis.

## Contents

[Architecture](#architecture) · [Quick start](#quick-start) · [npm commands](#npm-commands) · [API](#api) · [Processing behaviour](#processing-behaviour) · [Logging](#logging) · [Configuration](#configuration) · [Project layout](#project-layout) · [Further documentation](#further-documentation)

## Architecture

```
POST /api/webhook ──► sinch-ai-bot-input-queue ──► input worker (concurrency 1)
   (no validation)                                   │
                                valid ───────────────┼──► input CSV   ──► SFTP_REMOTE_DIR (/import)
                                invalid structure ───┼──► sinch-ai-bot-failure-queue ──► failure CSV ──► SFTP_FAILURE_REMOTE_DIR
                                retries exhausted /  │
                                unrecoverable ───────┴──► sinch-ai-bot-dlq-queue     ──► DLQ CSV     ──► SFTP_DLQ_REMOTE_DIR
```

The webhook only enqueues. Validation, CSV writing and SFTP upload happen asynchronously in the workers.

## Quick start

```bash
npm install
docker-compose up -d      # Redis 7.2 on host port 4001 (AOF persistence, password-protected)
cp .env.example .env      # set REDIS_URL / REDIS_PASSWORD to match the Redis you use
npm run dev               # http://localhost:3000
npm test                  # no Redis or network needed
```

### npm commands

```bash
npm install            # install dependencies
npm run dev            # run with tsx (src/index.ts)
npm run dev:watch      # run with tsx in watch mode (restarts on change)
npm run build          # compile TypeScript to dist/
npm start              # run the compiled build (node dist/index.js)
npm test               # run tests/**/*.test.ts with the Node test runner
npm run lint           # ESLint on src
npm run format         # Prettier (write) on src
npm audit              # check dependencies for known vulnerabilities
```

## API

| Method & path | Behaviour |
| --- | --- |
| `POST /api/webhook` | Enqueues the body exactly as received. A JSON array is a batch: one queue job per element. Returns `200 {"message":"Webhook received","count":N}` only after the jobs are in Redis, `500` if enqueueing fails (nothing is accepted). Malformed JSON gets `400`; body limit is 10 MB. |
| `GET /api/health` | Pings Redis: `200 {"status":"ok","redis":"up"}` or `503 {"status":"degraded","redis":"down"}`. |
| `POST /api/reprocess/failure` | Replays failure CSVs (`sinch_ai_bot_webhook_failure_*.csv`) from `SFTP_FAILURE_REMOTE_DIR` through the input queue. Optional body `{"fileName":"..."}` limits it to one file. |
| `POST /api/reprocess/dlq` | Same for DLQ CSVs (`sinch_ai_bot_webhook_dlq_*.csv`) from `SFTP_DLQ_REMOTE_DIR`. |
| `GET /admin/queues` | Bull Board queue dashboard (`BULL_BOARD_PATH`). |
| anything else | `404 {"error":"Not found"}` |

The dashboard has no auth by default and is **off in production** unless enabled with `BULL_BOARD_ENABLED`.

**Reprocessing** validates every record in each CSV, queues the valid ones, merges the invalid ones into `reprocessing_failure_records.csv` / `reprocessing_dlq_records.csv` in the same SFTP directory, and deletes the original only after all of that succeeds. Responses: `200` (all files processed), `207` (some files failed), `400` (bad `fileName`), `409` (a run of that type is already in progress), `503` (SFTP not configured). Files modified in the last 30 s are skipped. Re-running is safe because duplicate payload ids are skipped.

## Processing behaviour

- **Input worker** validates each record against [payloadSchema.ts](src/validation/payloadSchema.ts) (the Data Extension columns: type, length, nullability). `call_id` is the only required field; other fields may be absent/null but must have the right type and fit their length when present (arrays/JSON are measured by their serialised length); unknown keys are ignored. Valid records are flattened ([flatten.ts](src/validation/flatten.ts)) into the input CSV: booleans are `true`/`false`, null/absent is empty, `all_dispositions` and `extracted_additional_details` are JSON text, dates are written as received.
- **Failure queue:** records that fail validation (including a non-object payload) are handed to the failure queue and written to the failure CSV (`JobId, Reason, FailedAt, RawPayload, coms_id`). They are not retried.
- **Retries / DLQ:** a failing job is retried `QUEUE_MAX_ATTEMPTS` times with exponential back-off (`QUEUE_RETRY_DELAY_MS` base). When attempts are exhausted (or the job stalls) it is moved to the DLQ, as are records that cannot be flattened and CSV write failures on the last attempt. The DLQ CSV has `JobId, Reason, Attempts, FailedAt, RawPayload, coms_id`. A job is acknowledged only after its record is in the CSV or has been handed to the failure/DLQ queue.
- **No duplicates:** failure/DLQ jobs use a jobId derived from the input job id (`failure_<id>` / `dlq_<id>`, so a repeat hand-off is ignored), and a Redis marker (7 days) keyed on the payload `id` stops a re-run or redelivery of an already-written record from adding a second CSV row. Records without an `id` are written as-is.
- **Retention:** completed input jobs are kept 1 day, completed failure/DLQ jobs are limited to the latest 500 (visible in Bull Board). Failed input jobs are kept 7 days; failed failure/DLQ jobs are never auto-removed.
- **CSV files** are written to `CSV_OUTPUT_DIR/{input,failure,dlq}/` as `sinch_ai_bot_webhook_<report>_<yyyyMMddHHmm>.csv` (open file is `.tmp`). They are rotated on the wall clock every `CSV_FLUSH_INTERVAL_MINUTES` in `CSV_FLUSH_TIMEZONE` (the interval must divide 1440, e.g. 15, 30, 60; otherwise startup fails), then uploaded. Empty files are discarded. Local files are named by rotation time. Files left on disk (including crashed `.tmp` files) are re-uploaded at startup.
- **SFTP upload:**
  - On SFTP the input report is uploaded as `sinch_ai_bot_webhook_input.csv` when that name is free (not being uploaded and not already on the server); otherwise as `sinch_ai_bot_webhook_input_<yyyyMMddHHmm>.csv` using the upload time, with a `_1`, `_2`, … suffix only if that name exists too.
  - The uploaded input CSV has a leading `filename` column (40 columns) holding the final SFTP file name without `.csv` on every row; the local copy keeps the 39 columns.
  - Uploads go to a `.uploading` temp name first and are renamed only to a free name, so existing remote files are never overwritten.
  - Uploads are sequential and retried `SFTP_MAX_RETRIES` times with exponential back-off (`SFTP_RETRY_DELAY_MS` base). The local file is deleted only after a successful upload; a file whose retries are exhausted is queued again after `SFTP_FAILED_RETRY_INTERVAL_MS`.
  - An upload is skipped, and the file kept locally, until `SFTP_HOST`, `SFTP_USERNAME` and that report's remote directory are set. Failure/DLQ directories default to empty.
- **Startup checks:** before listening, the app pings Redis and, if `AWS_LOG_GROUP` and `AWS_LOG_STREAM` are set, verifies the CloudWatch log group/stream (15 s timeout each). If either fails it logs the error and exits with code 1. On success it logs `Redis connection verified`, `AWS CloudWatch connection verified` and finally `Application started successfully`. A port already in use also stops the app.
- **Graceful shutdown** (`SIGINT`/`SIGTERM`): closes the workers, flushes the open CSV files, drains pending SFTP uploads (up to 2 minutes), closes the queues and Redis, then flushes the logger.

## Logging

Structured JSON via Winston to the console; sensitive keys and secret-looking strings are redacted. Logs are also shipped to CloudWatch when `AWS_LOG_GROUP` and `AWS_LOG_STREAM` are set. Example entries are in [Documents/sample_logs.md](Documents/sample_logs.md).

**Request lifecycle logging.** Every flow (webhook intake, input worker and its retries, failure queue, DLQ, failure/DLQ reprocessing) logs through one helper, [flowLogger.ts](src/utils/flowLogger.ts), so a request can be followed from receipt to its result.

| Field | Meaning |
| --- | --- |
| `timestamp_ist`, `timestamp` | IST and UTC time of the entry |
| `correlation_id` / `request_id` | Id of the originating request (see below) |
| `processing_type` | `INPUT`, `RETRY`, `FAILURE`, `DLQ`, `FAILURE_REPROCESS`, `DLQ_REPROCESS` |
| `function_name`, `stage` | Where the entry was written |
| `flow_status` | `STARTED`, `SUCCESS` or `FAILED` |
| `request_body`, `response_body`, `status_code` | Present where applicable. The request body is logged on start, on completion and on every failure, not on each intermediate step |
| `attempt`, `will_retry`, `retry_reason`, `retry_delay_ms` | Retry details on failed attempts |
| `error_type`, `error_message`, `stack_trace` | `FAILED` entries only |
| `failure_category` | `API_EXECUTION`, `DATA_TRANSFORMATION`, `VALIDATION`, `DATABASE_OPERATION`, `FILE_OPERATION`, `RETRY_HANDLING`, `DLQ_PROCESSING` |
| `failed_function`, `failed_stage` | The step that actually failed |

- The webhook's request id travels with each queue job (in the job options, not the payload), so worker, failure and DLQ entries carry the same `correlation_id`. Reprocess runs get their own id (returned in `X-Request-Id`), shared by the jobs they re-queue.
- Bodies over 64 KB are logged as a size and preview.
- A logging problem is reported on stderr and never interrupts processing.

## Configuration

All settings are environment variables (loaded from `.env`); see [.env.example](.env.example).

| Variable | Default | Purpose |
| --- | --- | --- |
| `NODE_ENV` | `development` | `production` fixes the log level at `info` and disables debug logs |
| `PORT` / `HOST` | `3000` / `0.0.0.0` | HTTP listener |
| `REDIS_URL` / `REDIS_PASSWORD` | `redis://localhost:4001` / – | Redis connection |
| `BULL_BOARD_ENABLED` / `BULL_BOARD_PATH` | on outside production / `/admin/queues` | Queue dashboard |
| `QUEUE_MAX_ATTEMPTS` | `3` | Attempts per job before it goes to the DLQ |
| `QUEUE_RETRY_DELAY_MS` | `2000` | Base delay of the exponential retry back-off |
| `CSV_OUTPUT_DIR` | `./csv_output` | Where CSV files are written |
| `CSV_FLUSH_INTERVAL_MINUTES` | `30` | CSV rotation interval (must divide 1440) |
| `CSV_FLUSH_TIMEZONE` | `Asia/Kolkata` | Time zone for rotation times and file names |
| `SFTP_HOST` / `SFTP_PORT` | – / `22` | SFTP server |
| `SFTP_USERNAME` / `SFTP_PASSWORD` | – | SFTP credentials |
| `SFTP_REMOTE_DIR` | `/import` | Input CSV destination |
| `SFTP_FAILURE_REMOTE_DIR` / `SFTP_DLQ_REMOTE_DIR` | empty | Failure / DLQ CSV destinations (upload skipped when empty) |
| `SFTP_MAX_RETRIES` / `SFTP_RETRY_DELAY_MS` | `5` / `2000` | Retries after the first attempt, and base back-off |
| `SFTP_FAILED_RETRY_INTERVAL_MS` | `300000` | Wait before a file whose retries were exhausted is queued again |
| `REPROCESS_BATCH_SIZE` | `500` | Records per queue batch when reprocessing |
| `LOG_LEVEL` | `info` | Log level outside production |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | – | Optional; the default AWS credential chain is used when empty |
| `AWS_REGION` | `ap-south-1` | CloudWatch region |
| `AWS_LOG_GROUP` / `AWS_LOG_STREAM` | – / – | CloudWatch destination; shipping is off unless both are set (`.env.example` suggests the stream `sinch-ai-bot-webhook-log-stream`) |

## Project layout

```
src/index.ts                 Express app, worker startup, graceful shutdown
src/api/                     webhook, reprocess and Bull Board routes; webhook request logging
src/queue/                   queues, producer, input worker, failure/DLQ workers, routing
src/reprocess/               failure/DLQ CSV reprocessing from SFTP
src/validation/              schema, validator, flattener
src/csv/                     rotating CSV writer + the three report writers
src/sftp/sftpUploader.ts     upload with retries
src/config/env.ts            environment configuration
src/utils/                   logger (Winston + optional CloudWatch), flow logger, event names, wall-clock scheduler
tests/                       unit tests, sample payload
docker-compose.yml           local Redis
bitbucket-pipelines.yml      on push to main, runs the server's deploy.sh over SSH
```

## Further documentation

- [Documents/Architecture_Document/ARCHITECTURE.md](Documents/Architecture_Document/ARCHITECTURE.md): system architecture
- [Documents/Architecture_Document/DEVELOPER_GUIDE.md](Documents/Architecture_Document/DEVELOPER_GUIDE.md): developer guide
- [Documents/postmanCollection/](Documents/postmanCollection/): Postman collection and environment
- [Documents/sample_logs.md](Documents/sample_logs.md): sample log entries
