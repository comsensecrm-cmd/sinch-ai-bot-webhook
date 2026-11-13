/**
 * Regression testing: bugs that were fixed once (see git history) must stay fixed. Runs the real workers.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";

describe("Regression: fixed bugs stay fixed", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
    await h.startWorkers();
  });
  after(async () => {
    await h.stop();
  });

  // 4eb66af "Make call_id the only required field so payloads without it go to the failure queue"
  it("call_id is the only required field; without it the record goes to the failure queue", async () => {
    const { validatePayload } = await import("../../src/validation/payloadValidator");
    const { FIELDS } = await import("../../src/validation/payloadSchema");
    assert.deepStrictEqual(FIELDS.filter((f) => f.required).map((f) => f.name), ["call_id"]);
    assert.strictEqual(validatePayload({ call_id: "only" }).valid, true);

    await h.postJson("/api/webhook", { id: "reg-no-callid", contact_name: "No Call Id", coms_id: "REG-1" });
    await h.postJson("/api/webhook", { id: "reg-callid-only", call_id: "reg-callid-only" });
    await h.waitForIdle();
    const reports = await h.flushReports();
    assert.strictEqual(reports.failure.filter((r) => r.coms_id === "REG-1").length, 1);
    assert.strictEqual(reports.input.filter((r) => r.id === "reg-callid-only").length, 1);
    assert.strictEqual(reports.input.filter((r) => r.id === "reg-no-callid").length, 0);
  });

  // Webhook parses JSON whatever Content-Type the sender uses, so the body never arrives as {}.
  it("a JSON body sent as text/plain is queued with its content, not as {}", async () => {
    const payload = uniquePayload();
    const res = await h.request("/api/webhook", { method: "POST", headers: { "content-type": "text/plain" }, body: JSON.stringify(payload) });
    assert.strictEqual(res.status, 200);
    await h.waitForIdle();
    const reports = await h.flushReports();
    assert.strictEqual(reports.input.filter((r) => r.id === payload.id).length, 1);
  });

  // The record id (the PK) is the de-duplication key: a redelivery gets a new BullMQ job id.
  it("a redelivered record is written once even though it gets a new job id", async () => {
    const payload = uniquePayload();
    await h.postJson("/api/webhook", payload);
    await h.waitForIdle();
    await h.postJson("/api/webhook", payload);
    await h.waitForIdle();
    const reports = await h.flushReports();
    assert.strictEqual(reports.input.filter((r) => r.id === payload.id).length, 1);
  });

  // Redis key written by earlier releases; changing it would let in-flight duplicates through after a deploy.
  it("de-duplication key format is unchanged", async () => {
    const payload = uniquePayload();
    await h.postJson("/api/webhook", payload);
    await h.waitForIdle();
    assert.strictEqual(await h.queues.redisConnection.get(`sinch-ai-bot-webhook:csv-written:${payload.id}`), "1");
  });

  // Failure CSV carries the raw payload and coms_id so a replay is possible from the file alone.
  it("failure rows keep the full raw payload and coms_id", async () => {
    const payload = uniquePayload({ call_duration: "oops", coms_id: "REG-RAW-1", extra: { deep: [1, { two: 2 }] } });
    await h.postJson("/api/webhook", payload);
    await h.waitForIdle();
    const reports = await h.flushReports();
    const row = reports.failure.find((r) => r.coms_id === "REG-RAW-1");
    assert.deepStrictEqual(JSON.parse(row?.RawPayload ?? "null"), payload);
  });
});
