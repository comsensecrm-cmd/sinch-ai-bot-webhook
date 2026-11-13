/** POST /webhook enqueues the payload as received; GET /health pings Redis. */

import express, { Request, Response } from "express";
import { enqueueRecords } from "../queue/producer";
import { redisConnection } from "../queue/queues";
import {
  getRequestId,
  logProcessingCompleted,
  logProcessingFailed,
  logProcessingStarted,
} from "./webhookLogging";

const router = express.Router();

// The global express.json() only parses application/json. Parse the webhook body
// as JSON whatever Content-Type the sender uses, so it never arrives as `{}`.
// (A no-op when the global parser already handled the request.)
router.post(
  "/webhook",
  express.json({ limit: "10mb", type: () => true }),
  async (req: Request, res: Response) => {
    const records: unknown[] = Array.isArray(req.body) ? req.body : [req.body];
    logProcessingStarted(req, res, records.length);

    try {
      const jobIds = await enqueueRecords(records, getRequestId(res));
      const body = { message: "Webhook received", count: jobIds.length };
      logProcessingCompleted(req, res, jobIds, body);
      return res.status(200).json(body);
    } catch (err) {
      const body = { error: "Failed to queue webhook" };
      logProcessingFailed(
        req,
        res,
        err,
        "ENQUEUE_FAILED",
        records.length,
        body,
      );
      return res.status(500).json(body);
    }
  },
);

router.get("/health", async (_req: Request, res: Response) => {
  const redisUp = await redisConnection
    .ping()
    .then((pong) => pong === "PONG")
    .catch(() => false);
  return res.status(redisUp ? 200 : 503).json({
    status: redisUp ? "ok" : "degraded",
    redis: redisUp ? "up" : "down",
  });
});

export default router;
