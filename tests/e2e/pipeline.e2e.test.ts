/**
 * End-to-end testing: the full business workflow over HTTP, with the real input, failure and DLQ
 * workers, real Redis and real CSV files.
 *
 *   Sinch POST -> input queue -> validate -> de-duplicate -> flatten -> input CSV
 *                                   \-> invalid -> failure queue -> failure CSV
 *
 * Only the SFTP hop is left out (not configured, so finished files stay on disk, which is what is read).
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness, type Reports } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";
import { CSV_COLUMNS } from "../../src/validation/payloadSchema";

describe("E2E: webhook to CSV pipeline", () => {
  let h: Harness;
  let reports: Reports;

  const valid = uniquePayload({
    contact_name: 'Sameer "Sam" Doe, Jr.',
    call_summary: "Line one\nLine two, with a comma and \"quotes\" 😀",
  });
  const badType = uniquePayload({ call_duration: "165", coms_id: "COMS-BAD-TYPE" });
  const noCallId = uniquePayload({ coms_id: "COMS-NO-CALLID" });
  delete noCallId.call_id;
  const duplicate = uniquePayload();
  const noId = uniquePayload({ id: undefined });
  const requestId = "e2e-request-0001";
  const batchValid = Array.from({ length: 10 }, () => uniquePayload());
  const batchInvalid = Array.from({ length: 5 }, () => uniquePayload({ created_at: "not a date" }));

  before(async () => {
    h = await startHarness();
    await h.startWorkers();

    await h.postJson("/api/webhook", valid, { "x-request-id": requestId });
    await h.postJson("/api/webhook", badType);
    await h.postJson("/api/webhook", noCallId);
    // The same call delivered twice, in one request and again later (Sinch redelivery).
    await h.postJson("/api/webhook", [duplicate, duplicate]);
    await h.waitForIdle();
    await h.postJson("/api/webhook", duplicate);
    // No payload id: nothing to de-duplicate on, so both copies are written.
    await h.postJson("/api/webhook", [noId, noId]);
    await h.postJson("/api/webhook", [...batchValid, ...batchInvalid]);

    await h.waitForIdle();
    reports = await h.flushReports();
  });
  after(async () => {
    await h.stop();
  });

  const inputRow = (id: unknown) => reports.input.filter((r) => r.id === id);

  it("writes a valid record to the input CSV with every column", () => {
    const rows = inputRow(valid.id);
    assert.strictEqual(rows.length, 1);
    assert.deepStrictEqual(Object.keys(rows[0]), [...CSV_COLUMNS]);
    assert.strictEqual(rows[0].call_id, valid.call_id);
    assert.strictEqual(rows[0].contact_phone, "919311661234");
    assert.strictEqual(rows[0].call_duration, "165");
    assert.strictEqual(rows[0].call_connected, "true");
  });

  it("round-trips commas, quotes, emoji and line breaks through the CSV", () => {
    const row = inputRow(valid.id)[0];
    assert.strictEqual(row.contact_name, valid.contact_name);
    assert.strictEqual(row.call_summary, valid.call_summary);
  });

  it("stores array fields as JSON in a single cell", () => {
    const row = inputRow(valid.id)[0];
    assert.deepStrictEqual(JSON.parse(row.all_dispositions), valid.all_dispositions);
    assert.strictEqual(row.extracted_additional_details, "");
  });

  it("routes a wrong-typed record to the failure CSV, with the reason and the raw payload", () => {
    const rows = reports.failure.filter((r) => r.coms_id === "COMS-BAD-TYPE");
    assert.strictEqual(rows.length, 1);
    assert.match(rows[0].Reason, /call_duration must be a number/);
    assert.deepStrictEqual(JSON.parse(rows[0].RawPayload), badType);
    assert.ok(rows[0].JobId);
    assert.ok(!Number.isNaN(Date.parse(rows[0].FailedAt)));
    assert.strictEqual(inputRow(badType.id).length, 0, "an invalid record never reaches the input CSV");
  });

  it("routes a record without call_id to the failure CSV", () => {
    const rows = reports.failure.filter((r) => r.coms_id === "COMS-NO-CALLID");
    assert.strictEqual(rows.length, 1);
    assert.match(rows[0].Reason, /call_id is required/);
    assert.strictEqual(inputRow(noCallId.id).length, 0);
  });

  it("writes a record delivered more than once exactly one time", () => {
    assert.strictEqual(inputRow(duplicate.id).length, 1);
  });

  it("writes every copy of a record that has no id", () => {
    const rows = reports.input.filter((r) => r.id === "" && r.call_id === noId.call_id);
    assert.strictEqual(rows.length, 2);
  });

  it("splits a mixed batch correctly", () => {
    for (const p of batchValid) assert.strictEqual(inputRow(p.id).length, 1, String(p.id));
    for (const p of batchInvalid) assert.strictEqual(inputRow(p.id).length, 0, String(p.id));
    const failed = reports.failure.filter((r) => /created_at must be a date string/.test(r.Reason));
    assert.strictEqual(failed.length, batchInvalid.length);
  });

  it("accounts for every record: nothing is lost or invented", () => {
    // Input: valid 1 + duplicate 1 + no-id 2 + valid batch 10. Failures: bad type 1 + no call_id 1 + invalid batch 5.
    assert.strictEqual(reports.input.length, 1 + 1 + 2 + batchValid.length);
    assert.strictEqual(reports.failure.length, 1 + 1 + batchInvalid.length);
    assert.strictEqual(reports.dlq.length, 0);
  });

  it("marks written records in Redis for about a week", async () => {
    const key = `sinch-ai-bot-webhook:csv-written:${valid.id}`;
    assert.strictEqual(await h.queues.redisConnection.get(key), "1");
    const ttl = await h.queues.redisConnection.ttl(key);
    assert.ok(ttl > 6 * 86_400 && ttl <= 7 * 86_400, `ttl ${ttl}`);
  });

  it("carries the request id onto the failure queue job", async () => {
    const jobs = await h.queues.failureQueue.getJobs(["completed"]);
    assert.ok(jobs.length > 0);
    for (const job of jobs) assert.ok(job.data.correlationId, "failure job keeps its correlation id");
  });

  it("leaves the queues empty and nothing stuck or failed", async () => {
    for (const q of [h.queues.inputQueue, h.queues.failureQueue, h.queues.dlqQueue]) {
      const c = await q.getJobCounts("waiting", "active", "delayed", "failed");
      assert.deepStrictEqual(
        [c.waiting, c.active, c.delayed, c.failed],
        [0, 0, 0, 0],
        `${q.name}: waiting/active/delayed/failed`,
      );
    }
  });
});
