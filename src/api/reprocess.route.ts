/** POST /reprocess/failure and POST /reprocess/dlq: replay CSVs stored on SFTP through the input queue. */

import { randomUUID } from "crypto";
import express, { Request, Response } from "express";
import { reprocessDlqFiles } from "../reprocess/dlqReprocessor";
import { reprocessFailureFiles } from "../reprocess/failureReprocessor";
import {
  ReprocessBusyError,
  ReprocessConfigError,
  ReprocessFileError,
  type ReprocessSummary,
} from "../reprocess/reprocessor";
import {
  failedStepOf,
  flowLog,
  type FailureCategory,
  type FlowEntry,
} from "../utils/flowLogger";
import { EVENTS } from "../utils/logEvents";

const router = express.Router();

function handler(
  type: "failure" | "dlq",
  run: (fileName?: string, requestId?: string) => Promise<ReprocessSummary>,
) {
  const base = {
    type:
      type === "dlq"
        ? ("DLQ_REPROCESS" as const)
        : ("FAILURE_REPROCESS" as const),
    component: `api.reprocess.${type}`,
    functionName: "reprocessHandler",
    method: "POST",
    endpoint: `/api/reprocess/${type}`,
    tags: ["reprocess", type],
  };

  return async (req: Request, res: Response) => {
    const requestId = randomUUID();
    res.setHeader("X-Request-Id", requestId);
    const requestBody: unknown = req.body ?? null;
    const startedAt = Date.now();
    const entry = { ...base, correlationId: requestId, requestBody };

    // Every response goes through here, so each is logged with the body that was sent back.
    const respond = (
      status: number,
      body: unknown,
      failure?: Partial<FlowEntry> & { category?: FailureCategory },
    ) => {
      const duration_ms = Date.now() - startedAt;
      if (failure) {
        flowLog({
          ...entry,
          stage: "REQUEST_REJECTED",
          status: "FAILED",
          outcome: "failure",
          statusCode: status,
          responseBody: body,
          failureCategory: failure.category,
          ...failure,
          extra: { duration_ms, ...failure.extra },
        });
      } else {
        flowLog({
          ...entry,
          stage: "RESPONSE_SENT",
          status: "SUCCESS",
          message: "Reprocess request completed",
          outcome: "success",
          statusCode: status,
          responseBody: body,
          extra: { duration_ms },
        });
      }
      return res.status(status).json(body);
    };

    flowLog({
      ...entry,
      stage: "REQUEST_RECEIVED",
      status: "STARTED",
      message: "Reprocess request received",
      extra: { ip: req.ip },
    });

    const fileName: unknown = req.body?.fileName;
    if (fileName !== undefined && typeof fileName !== "string") {
      return respond(
        400,
        { error: "fileName must be a string" },
        {
          functionName: "reprocessHandler",
          message: "Reprocess request rejected: invalid fileName",
          errorCode: "REPROCESS_BAD_REQUEST",
          errorMessage: "fileName must be a string",
          category: "VALIDATION",
          level: "warn",
        },
      );
    }

    try {
      const summary = await run(fileName, requestId);
      return respond(summary.filesFailed > 0 ? 207 : 200, summary);
    } catch (err) {
      // Expected outcomes the caller can act on: logged as warnings, not as bugs.
      const expected = (
        status: number,
        code: string,
        category: FailureCategory,
      ) =>
        respond(
          status,
          { error: (err as Error).message },
          {
            message: `Reprocess request rejected: ${(err as Error).message}`,
            error: err,
            errorCode: code,
            category,
            level: "warn",
          },
        );
      if (err instanceof ReprocessBusyError) {
        return expected(409, "REPROCESS_BUSY", "RETRY_HANDLING");
      }
      if (err instanceof ReprocessFileError) {
        return expected(400, "REPROCESS_BAD_FILE", "VALIDATION");
      }
      if (err instanceof ReprocessConfigError) {
        return expected(503, "REPROCESS_NOT_CONFIGURED", "API_EXECUTION");
      }
      const step = failedStepOf(err);
      return respond(
        500,
        { error: `${type} reprocessing failed` },
        {
          functionName: step?.functionName ?? "reprocess",
          message: "Reprocessing failed",
          event: EVENTS.REPROCESS_ROUTE_FAILED,
          error: err,
          errorCode: "REPROCESS_FAILED",
          category: step?.category ?? "API_EXECUTION",
          failedStep: step,
        },
      );
    }
  };
}

router.post("/reprocess/failure", handler("failure", reprocessFailureFiles));
router.post("/reprocess/dlq", handler("dlq", reprocessDlqFiles));

export default router;
