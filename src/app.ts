/**
 * Builds the Express app (routes and error handling) without starting anything, so tests can mount it
 * on an ephemeral port. index.ts owns startup, workers and shutdown.
 */

import express from "express";
import config from "./config/env";
import { EVENTS } from "./utils/logEvents";
import { flowLog } from "./utils/flowLogger";
import webhookRouter from "./api/webhook.route";
import { getRequestId, webhookRequestLogger } from "./api/webhookLogging";
import reprocessRouter from "./api/reprocess.route";
import { createBullBoardRouter } from "./api/bullBoard";

export function createApp(): express.Express {
  const app = express();
  // Before the body parser, so requests that fail to parse are still logged.
  app.use("/api/webhook", webhookRequestLogger);
  app.use(express.json({ limit: "10mb" }));
  app.use("/api", webhookRouter);
  app.use("/api", reprocessRouter);
  if (config.bullBoard.enabled) {
    app.use(config.bullBoard.path, createBullBoardRouter());
  }
  // Browsers opening the dashboard request this; answer quietly instead of a logged 404.
  app.get("/favicon.ico", (_req, res) => res.status(204).end());
  app.use((_req, res) => res.status(404).json({ error: "Not found" }));
  app.use(
    (
      err: Error & { status?: number; body?: unknown },
      req: express.Request,
      res: express.Response,
      _next: express.NextFunction,
    ) => {
      const status =
        err.status && err.status >= 400 && err.status < 500 ? err.status : 500;
      const correlationId = getRequestId(res);
      const base = {
        type: "INPUT" as const,
        component: "express",
        correlationId,
        functionName: "expressErrorHandler",
        method: req.method,
        endpoint: req.originalUrl.split("?")[0],
        statusCode: status,
        outcome: "failure" as const,
      };
      if (status === 500) {
        flowLog({
          ...base,
          stage: "UNHANDLED_ERROR",
          status: "FAILED",
          message: "Unhandled express error",
          event: EVENTS.EXPRESS_UNHANDLED_ERROR,
          error: err,
          errorCode: "UNHANDLED_EXPRESS_ERROR",
          failureCategory: "API_EXECUTION",
          responseBody: { error: "Internal server error" },
        });
      } else {
        // e.g. malformed JSON or oversized body, rejected before the route runs.
        const length = Number(req.get("content-length"));
        flowLog({
          ...base,
          stage: "REQUEST_REJECTED",
          status: "FAILED",
          level: "warn",
          message: "Request rejected before processing",
          event: EVENTS.EXPRESS_REQUEST_REJECTED,
          errorCode: "REQUEST_REJECTED",
          errorMessage: err.message,
          failureCategory: "VALIDATION",
          // body-parser keeps the raw text it could not parse; it is all there is to replay.
          ...(typeof err.body === "string" && { requestBody: err.body }),
          responseBody: { error: "Invalid request body" },
          severity: 5,
          extra: {
            content_type: req.get("content-type"),
            content_length: Number.isFinite(length) ? length : undefined,
          },
        });
      }
      res.status(status).json({
        error:
          status === 500 ? "Internal server error" : "Invalid request body",
      });
    },
  );
  return app;
}
