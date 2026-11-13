/**
 * Regression testing: behaviour other systems depend on, pinned so a refactor cannot change it by accident.
 * Marketing Cloud imports the CSVs by column name and order, Sinch reads the webhook response, and Redis
 * keys written by one release must be understood by the next (in-flight de-duplication claims).
 *
 * If one of these fails on purpose, update it in the same change and tell the downstream owners.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";

describe("Regression: contracts", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  it("input CSV columns: names and order", async () => {
    const { CSV_COLUMNS } = await import("../../src/validation/payloadSchema");
    assert.deepStrictEqual([...CSV_COLUMNS], [
      "id", "contact_name", "contact_id", "campaign_name", "duration", "call_start_time", "bot_name",
      "call_type", "recording_url", "transcript_url", "disposition", "disposition_criticality",
      "next_actions", "next_action_status", "call_summary", "call_ended_reason", "all_dispositions",
      "next_retry_scheduled", "call_attempt_number", "extracted_additional_details", "created_at",
      "contacts_audience_name", "sub_disposition", "updated_at", "call_connected", "campaign_group_name",
      "unix_call_time", "bot_id", "campaign_id", "contact_phone", "campaign_group_id", "call_duration",
      "customer_id", "call_id", "call_engaged", "call_transferred", "vin_number", "dealer_region", "coms_id",
    ]);
  });

  it("failure and DLQ CSV columns: names and order", async () => {
    const { FAILURE_COLUMNS, DLQ_COLUMNS } = await import("../../src/csv/reportWriters");
    assert.deepStrictEqual([...FAILURE_COLUMNS], ["JobId", "Reason", "FailedAt", "RawPayload", "coms_id"]);
    assert.deepStrictEqual([...DLQ_COLUMNS], ["JobId", "Reason", "Attempts", "FailedAt", "RawPayload", "coms_id"]);
  });

  it("queue names", async () => {
    const q = await import("../../src/queue/queues");
    assert.strictEqual(q.INPUT_QUEUE_NAME, "sinch-ai-bot-input-queue");
    assert.strictEqual(q.FAILURE_QUEUE_NAME, "sinch-ai-bot-failure-queue");
    assert.strictEqual(q.DLQ_QUEUE_NAME, "sinch-ai-bot-dlq-queue");
  });

  it("webhook response body and status", async () => {
    const res = await h.postJson("/api/webhook", uniquePayload());
    assert.strictEqual(res.status, 200);
    assert.deepStrictEqual(res.json, { message: "Webhook received", count: 1 });
  });

  it("route paths: /api/webhook, /api/health, /api/reprocess/{failure,dlq}", async () => {
    assert.strictEqual((await h.postJson("/api/webhook", uniquePayload())).status, 200);
    assert.strictEqual((await h.request("/api/health")).status, 200);
    for (const type of ["failure", "dlq"]) {
      assert.notStrictEqual((await h.postJson(`/api/reprocess/${type}`, {})).status, 404, type);
    }
    // The pre-/api path was never served.
    assert.strictEqual((await h.postJson("/webhook", uniquePayload())).status, 404);
  });

  it("queued jobs: name, retry policy and retention", async () => {
    const payload = uniquePayload();
    await h.postJson("/api/webhook", payload);
    const job = (await h.queues.inputQueue.getJobs(["waiting"])).find((j) => (j.data as { id?: string }).id === payload.id);
    assert.strictEqual(job?.name, "call-log");
    assert.strictEqual(job?.opts.attempts, 3);
    assert.deepStrictEqual(job?.opts.backoff, { type: "exponential", delay: 50 });
    assert.deepStrictEqual(job?.opts.removeOnComplete, { age: 86_400 });
    assert.deepStrictEqual(job?.opts.removeOnFail, { age: 7 * 86_400 });
  });

  it("failure and DLQ job ids and data shape", async () => {
    const { routeToFailureQueue, routeToDlq } = await import("../../src/queue/routing");
    await routeToFailureQueue("job-1", { a: 1 }, ["x is required"], { correlationId: "req-1" });
    await routeToDlq("job-2", { b: 2 }, "boom", 3, { correlationId: "req-2" });

    const f = await h.queues.failureQueue.getJob("failure_job-1");
    assert.strictEqual(f?.name, "invalid-record");
    assert.deepStrictEqual(Object.keys(f?.data).sort(), ["correlationId", "errors", "failedAt", "jobId", "originalPayload"]);
    assert.deepStrictEqual(f?.data.originalPayload, { a: 1 });

    const d = await h.queues.dlqQueue.getJob("dlq_job-2");
    assert.strictEqual(d?.name, "dead-letter");
    assert.deepStrictEqual(Object.keys(d?.data).sort(), ["attempts", "correlationId", "failedAt", "jobId", "originalPayload", "reason"]);
    assert.strictEqual(d?.data.attempts, 3);
  });

  it("routing the same job twice does not create a second failure/DLQ entry", async () => {
    const { routeToFailureQueue, routeToDlq } = await import("../../src/queue/routing");
    await routeToFailureQueue("dup-1", {}, ["e"]);
    await routeToFailureQueue("dup-1", {}, ["e"]);
    await routeToDlq("dup-2", {}, "r", 1);
    await routeToDlq("dup-2", {}, "r", 1);
    assert.strictEqual((await h.queues.failureQueue.getJobs(["waiting"])).filter((j) => j.id === "failure_dup-1").length, 1);
    assert.strictEqual((await h.queues.dlqQueue.getJobs(["waiting"])).filter((j) => j.id === "dlq_dup-2").length, 1);
  });
});
