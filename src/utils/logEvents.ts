/** Stable event names (`<domain>.<action>`). Group on these, never on message text. */

export const EVENTS = {
  // webhook
  WEBHOOK_REQUEST_RECEIVED: "webhook.request_received",
  WEBHOOK_PROCESSING_STARTED: "webhook.processing_started",
  WEBHOOK_PROCESSING_COMPLETED: "webhook.processing_completed",
  WEBHOOK_PROCESSING_FAILED: "webhook.processing_failed",
  WEBHOOK_RESPONSE_SENT: "webhook.response_sent",
  WEBHOOK_REQUEST_ABORTED: "webhook.request_aborted",

  // express
  EXPRESS_UNHANDLED_ERROR: "express.unhandled_error",
  EXPRESS_REQUEST_REJECTED: "express.request_rejected",

  // server lifecycle
  SERVER_STARTUP_REDIS_CONNECTED: "server.startup.redis_connected",
  SERVER_STARTUP_CLOUDWATCH_CONNECTED: "server.startup.cloudwatch_connected",
  SERVER_STARTUP_CLOUDWATCH_SKIPPED: "server.startup.cloudwatch_skipped",
  SERVER_STARTUP_RECOVERY: "server.startup.recovery",
  SERVER_STARTUP_READY: "server.startup.ready",
  SERVER_STARTUP_FATAL: "server.startup.fatal",
  SERVER_SHUTDOWN_STARTED: "server.shutdown.started",
  SERVER_SHUTDOWN_COMPLETE: "server.shutdown.complete",

  // redis
  REDIS_CONNECTED: "redis.connected",
  REDIS_ERROR: "redis.error",

  // queue
  QUEUE_ERROR: "queue.error",
  QUEUE_WORKER_ERROR: "queue.worker_error",
  QUEUE_JOB_COMPLETED: "queue.job_completed",
  QUEUE_JOB_FAILED: "queue.job_failed",
  QUEUE_JOB_RECORDED: "queue.job_recorded",
  QUEUE_DUPLICATE_SKIPPED: "queue.duplicate_skipped",
  QUEUE_MARKER_SET_FAILED: "queue.marker_set_failed",
  QUEUE_ROUTED_TO_FAILURE: "queue.routed_to_failure",
  QUEUE_MOVED_TO_DLQ: "queue.moved_to_dlq",
  QUEUE_DLQ_HANDOFF_FAILED: "queue.dlq_handoff_failed",

  // csv
  CSV_FILE_OPENED: "csv.file_opened",
  CSV_FILE_READY: "csv.file_ready",
  CSV_WRITE_ERROR: "csv.write_error",

  // sftp
  SFTP_UPLOAD_ENQUEUED: "sftp.upload_enqueued",
  SFTP_UPLOAD_SUCCEEDED: "sftp.upload_succeeded",
  SFTP_UPLOAD_RETRY: "sftp.upload_retry",
  SFTP_UPLOAD_FAILED: "sftp.upload_failed",
  SFTP_NOT_CONFIGURED: "sftp.not_configured",
  SFTP_LOCAL_DELETE_FAILED: "sftp.local_delete_failed",
  SFTP_DRAIN_TIMEOUT: "sftp.drain_timeout",

  // reprocess
  REPROCESS_FILE_DONE: "reprocess.file_done",
  REPROCESS_FILE_FAILED: "reprocess.file_failed",
  REPROCESS_ENQUEUE_FAILED: "reprocess.enqueue_failed",
  REPROCESS_RECORD_INVALID: "reprocess.record_invalid",
  REPROCESS_ROUTE_FAILED: "reprocess.route_failed",

  // step-by-step lifecycle entries written through flowLogger
  FLOW_STEP: "flow.step",

  // the logger itself
  LOGGER_EVENTS_DROPPED: "logger.events_dropped",
} as const;

export type LogEvent = (typeof EVENTS)[keyof typeof EVENTS];

export type LogOutcome = "success" | "failure" | "retry" | "skipped";
