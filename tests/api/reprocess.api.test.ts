/**
 * API testing: POST /api/reprocess/failure and /api/reprocess/dlq. No real SFTP server is involved:
 * the "configured" cases point SFTP at a closed local port, so connecting fails fast and predictably.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";

describe("API: reprocess endpoints", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  describe("SFTP not configured", () => {
    it("answers 503 for both endpoints", async () => {
      for (const type of ["failure", "dlq"]) {
        const res = await h.postJson(`/api/reprocess/${type}`, {});
        assert.strictEqual(res.status, 503, type);
        assert.match((res.json as { error: string }).error, /SFTP is not configured/);
      }
    });

    it("answers 400 when fileName is not a string", async () => {
      for (const fileName of [123, { a: 1 }, ["x.csv"], true]) {
        const res = await h.postJson("/api/reprocess/failure", { fileName });
        assert.strictEqual(res.status, 400);
        assert.deepStrictEqual(res.json, { error: "fileName must be a string" });
      }
    });

    it("returns an X-Request-Id", async () => {
      const res = await h.postJson("/api/reprocess/dlq", {});
      assert.match(res.headers.get("x-request-id") ?? "", /^[0-9a-f-]{36}$/);
    });

    it("does not serve GET", async () => {
      assert.strictEqual((await h.request("/api/reprocess/failure")).status, 404);
    });
  });
});
