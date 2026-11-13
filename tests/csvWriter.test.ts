import "./quietLogs";
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert";
import fs from "fs";
import os from "os";
import path from "path";
import { CsvWriter } from "../src/csv/csvWriter";
import { parseCsv } from "../src/reprocess/csv";

describe("CsvWriter", () => {
  let dir: string;
  let ready: string[];
  const make = (extra: Partial<ConstructorParameters<typeof CsvWriter>[0]> = {}) =>
    new CsvWriter({
      name: "unit",
      dir,
      filePrefix: "unit_report",
      columns: ["a", "b"],
      onFileReady: (file) => ready.push(file),
      ...extra,
    });

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), "csvwriter-"));
    ready = [];
  });
  afterEach(() => fs.rmSync(dir, { recursive: true, force: true }));

  it("writes the header and rows, and hands the finished .csv over on close", async () => {
    const w = make();
    await w.writeRecord({ a: "1", b: "x" });
    await w.writeRecord({ a: "2", b: "y" });
    await w.close();
    assert.strictEqual(ready.length, 1);
    assert.ok(ready[0].endsWith(".csv"));
    assert.deepStrictEqual(parseCsv(fs.readFileSync(ready[0], "utf8")), [["a", "b"], ["1", "x"], ["2", "y"]]);
  });

  it("escapes commas, quotes and line breaks, and writes missing values as empty", async () => {
    const w = make();
    await w.writeRecord({ a: 'he said "hi", then\nleft', b: undefined });
    await w.close();
    assert.deepStrictEqual(parseCsv(fs.readFileSync(ready[0], "utf8"))[1], ['he said "hi", then\nleft', ""]);
  });

  it("uses CRLF line endings", async () => {
    const w = make();
    await w.writeRecord({ a: "1", b: "2" });
    await w.close();
    assert.strictEqual(fs.readFileSync(ready[0], "utf8"), "a,b\r\n1,2\r\n");
  });

  it("produces no file when nothing was written", async () => {
    const w = make();
    await w.close();
    assert.deepStrictEqual(ready, []);
    assert.deepStrictEqual(fs.readdirSync(dir), []);
  });

  it("rotates when the size limit is reached, keeping every row", async () => {
    const w = make({ maxFileBytes: 40 });
    for (let i = 0; i < 10; i++) await w.writeRecord({ a: String(i), b: "padding-padding" });
    await w.close();
    assert.ok(ready.length > 1, `rotated into ${ready.length} files`);
    const rows = ready.flatMap((f) => parseCsv(fs.readFileSync(f, "utf8")).slice(1));
    assert.deepStrictEqual(rows.map((r) => r[0]), Array.from({ length: 10 }, (_, i) => String(i)));
  });

  it("never overwrites an existing primary file", async () => {
    fs.writeFileSync(path.join(dir, "primary.csv"), "keep me");
    const w = make({ primaryName: "primary.csv" });
    await w.writeRecord({ a: "1", b: "2" });
    await w.close();
    assert.strictEqual(fs.readFileSync(path.join(dir, "primary.csv"), "utf8"), "keep me");
    assert.notStrictEqual(ready[0], path.join(dir, "primary.csv"));
  });

  it("keeps rows in write order under concurrent writes", async () => {
    const w = make();
    await Promise.all(Array.from({ length: 200 }, (_, i) => w.writeRecord({ a: String(i), b: "" })));
    await w.close();
    const rows = parseCsv(fs.readFileSync(ready[0], "utf8")).slice(1);
    assert.deepStrictEqual(rows.map((r) => r[0]), Array.from({ length: 200 }, (_, i) => String(i)));
  });

  it("recovers a leftover .tmp file left by a hard stop", async () => {
    fs.writeFileSync(path.join(dir, "unit_report_202601010000.tmp"), "a,b\r\n1,2\r\n");
    const w = make();
    const found = w.scanPendingFiles();
    assert.deepStrictEqual(found.map((f) => path.basename(f)), ["unit_report_202601010000.csv"]);
    assert.ok(!fs.existsSync(path.join(dir, "unit_report_202601010000.tmp")));
    await w.close();
  });
});
