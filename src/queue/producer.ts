/** Enqueues webhook records as received. */

import type { Job } from "bullmq";
import { inputQueue } from "./queues";

/**
 * Tags a job with the id of the request that created it. It rides in the job options (BullMQ's
 * `telemetry.metadata`, which is inert here) rather than the job data, so the payload that is
 * validated and written is exactly the one received.
 */
export function correlationOpts(correlationId?: string) {
  return correlationId ? { telemetry: { metadata: correlationId } } : undefined;
}

/** The originating request id of a job; jobs created without one fall back to their own id. */
export function correlationIdOf(job: Job): string {
  return job.opts?.telemetry?.metadata ?? String(job.id);
}

export async function enqueueRecords(
  records: unknown[],
  correlationId?: string,
): Promise<string[]> {
  if (records.length === 0) return [];
  const opts = correlationOpts(correlationId);
  const jobs = await inputQueue.addBulk(
    records.map((data) => ({ name: "call-log", data, ...(opts && { opts }) })),
  );
  return jobs.map((job) => job.id ?? "");
}
