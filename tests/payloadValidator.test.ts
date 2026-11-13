import { test } from "node:test";
import assert from "node:assert";
import { validatePayload } from "../src/validation/payloadValidator";
import { samplePayload } from "./samplePayload";

test("Payload Validator", async (t) => {
  await t.test("accepts the sample payload", () => {
    assert.deepStrictEqual(validatePayload(samplePayload()), { valid: true, errors: [] });
  });

  await t.test("accepts a payload with only a call_id (other fields are nullable)", () => {
    assert.strictEqual(validatePayload({ call_id: "x" }).valid, true);
  });

  await t.test("rejects non-objects", () => {
    for (const bad of [null, "text", 5, [], [{ id: "x" }]]) {
      assert.strictEqual(validatePayload(bad).valid, false);
    }
  });

  await t.test("rejects a missing, null, blank or non-string call_id", () => {
    for (const call_id of [undefined, null, "", "   ", 5]) {
      const result = validatePayload({ ...samplePayload(), call_id });
      assert.strictEqual(result.valid, false);
      assert.match(result.errors[0], /^call_id /);
    }
    assert.deepStrictEqual(validatePayload({}).errors, ["call_id is required"]);
  });

  await t.test("id is optional", () => {
    for (const id of [undefined, null, ""]) {
      assert.strictEqual(validatePayload({ ...samplePayload(), id }).valid, true);
    }
  });

  await t.test("rejects wrong types", () => {
    const result = validatePayload({
      ...samplePayload(),
      call_duration: "165",
      call_connected: "true",
      contact_phone: 919311661234,
      all_dispositions: {},
      created_at: "not a date",
    });
    assert.strictEqual(result.errors.length, 5);
  });

  await t.test("rejects values longer than the column length", () => {
    const result = validatePayload({ ...samplePayload(), duration: "x".repeat(21) });
    assert.deepStrictEqual(result.errors, ["duration exceeds 20 characters"]);
  });

  await t.test("does not reject large all_dispositions / extracted_additional_details", () => {
    const result = validatePayload({
      ...samplePayload(),
      all_dispositions: Array.from({ length: 100 }, () => ({ reason: "x".repeat(100) })),
      extracted_additional_details: { notes: "y".repeat(5000) },
    });
    assert.deepStrictEqual(result, { valid: true, errors: [] });
  });

  await t.test("ignores keys outside the schema", () => {
    assert.strictEqual(validatePayload({ ...samplePayload(), brand_new_key: 1 }).valid, true);
  });
});
