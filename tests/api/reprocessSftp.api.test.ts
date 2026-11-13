/**
 * Reprocess endpoints with SFTP "configured" (pointing at a closed local port, so no real server is
 * contacted): file-name validation, the single-run lock, and clean failure when SFTP is unreachable.
 */

// Set before anything from src/ is imported (the harness imports it lazily), so config sees them.
process.env.SFTP_HOST = "127.0.0.1";
process.env.SFTP_PORT = "1";
process.env.SFTP_USERNAME = "test-user";
process.env.SFTP_FAILURE_REMOTE_DIR = "/failure";
process.env.SFTP_DLQ_REMOTE_DIR = "/dlq";

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";

const lockKey = (type: string) => `sinch-ai-bot-webhook:reprocess-lock:${type}`;

describe("API: reprocess endpoints with SFTP configured", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  it("rejects file names that are not generated report names (path traversal included)", async () => {
    const bad = ["../../etc/passwd", "..\\..\\windows\\win.ini", "notes.txt", "sinch_ai_bot_webhook_dlq_x.csv/../../a", "x.csv"];
    for (const fileName of bad) {
      for (const type of ["failure", "dlq"]) {
        const res = await h.postJson(`/api/reprocess/${type}`, { fileName });
        assert.strictEqual(res.status, 400, `${type} ${fileName}`);
        assert.match((res.json as { error: string }).error, /is not a (failure|dlq) CSV file name/);
      }
    }
  });

  it("fails with 500 and a generic message when SFTP is unreachable, and frees the lock", async () => {
    const res = await h.postJson("/api/reprocess/dlq", {});
    assert.strictEqual(res.status, 500);
    assert.deepStrictEqual(res.json, { error: "dlq reprocessing failed" });
    assert.strictEqual(await h.queues.redisConnection.get(lockKey("dlq")), null, "lock released");
    // ...so the next call is not told "busy".
    const again = await h.postJson("/api/reprocess/dlq", {});
    assert.strictEqual(again.status, 500);
  });

  it("answers 409 while another run holds the lock, without touching SFTP", async () => {
    await h.queues.redisConnection.set(lockKey("failure"), "someone-else", "PX", 60_000);
    const res = await h.postJson("/api/reprocess/failure", {});
    assert.strictEqual(res.status, 409);
    assert.match((res.json as { error: string }).error, /already in progress/);
    // Not ours to release.
    assert.strictEqual(await h.queues.redisConnection.get(lockKey("failure")), "someone-else");
  });

  it("locks failure and dlq runs independently", async () => {
    await h.queues.redisConnection.set(lockKey("failure"), "someone-else", "PX", 60_000);
    const res = await h.postJson("/api/reprocess/dlq", {});
    assert.strictEqual(res.status, 500, "dlq is not blocked by the failure lock");
  });
});
