import assert from "node:assert/strict";
import { test } from "node:test";
import { parseCsv, toCsv } from "../src/reprocess/csv";

const columns = ["JobId", "Reason", "RawPayload"] as const;

test("toCsv/parseCsv round-trip quotes, commas and line breaks", () => {
  const record = {
    JobId: "1",
    Reason: "a, b",
    RawPayload: '{"x":"he said \\"hi\\"\\nline"}\nsecond line',
  };
  const rows = parseCsv(toCsv(columns, [record]));
  assert.deepEqual(rows[0], [...columns]);
  assert.deepEqual(rows[1], [record.JobId, record.Reason, record.RawPayload]);
});

test("parseCsv ignores BOM, blank lines and handles LF endings", () => {
  const rows = parseCsv("\uFEFFa,b\n1,2\n\n3,4");
  assert.deepEqual(rows, [
    ["a", "b"],
    ["1", "2"],
    ["3", "4"],
  ]);
});

test("parseCsv rejects an unterminated quoted field", () => {
  assert.throws(() => parseCsv('a,b\n1,"oops'));
});
