import { test } from "node:test";
import assert from "node:assert";
import { flattenPayload } from "../src/validation/flatten";
import { CSV_COLUMNS } from "../src/validation/payloadSchema";
import { samplePayload } from "./samplePayload";

test("Flatten", async (t) => {
  await t.test("produces every CSV column, in schema order", () => {
    const row = flattenPayload(samplePayload());
    assert.deepStrictEqual(Object.keys(row), [...CSV_COLUMNS]);
    assert.strictEqual(CSV_COLUMNS.length, 39);
  });

  await t.test("writes scalars as text, booleans as true/false, null as empty", () => {
    const row = flattenPayload(samplePayload());
    assert.strictEqual(row.id, "call-_919311661234_V9ha7hCrgDT5");
    assert.strictEqual(row.call_duration, "165");
    assert.strictEqual(row.call_connected, "true");
    assert.strictEqual(row.call_transferred, "false");
    assert.strictEqual(row.campaign_id, "");
    assert.strictEqual(row.extracted_additional_details, "");
    assert.strictEqual(row.call_start_time, "2026-05-18T12:53:07.429000+00:00");
  });

  await t.test("serialises all_dispositions as JSON text", () => {
    const payload = samplePayload();
    const row = flattenPayload(payload);
    assert.deepStrictEqual(JSON.parse(row.all_dispositions), payload.all_dispositions);
  });

  await t.test("fills absent fields with empty strings", () => {
    const row = flattenPayload({ id: "x" });
    assert.strictEqual(row.id, "x");
    assert.strictEqual(row.bot_name, "");
  });
});
