import { test } from "node:test";
import assert from "node:assert";
import { shouldLogQueueError } from "../src/queue/errorThrottle";

test("Queue error throttle", async (t) => {
  await t.test(
    "logs the first error, then suppresses repeats for 30 seconds",
    () => {
      assert.strictEqual(shouldLogQueueError("a", 1_000), true);
      assert.strictEqual(shouldLogQueueError("a", 1_001), false);
      assert.strictEqual(shouldLogQueueError("a", 30_999), false);
      assert.strictEqual(shouldLogQueueError("a", 31_000), true);
    },
  );

  await t.test("tracks each source separately", () => {
    assert.strictEqual(shouldLogQueueError("b", 5_000), true);
    assert.strictEqual(shouldLogQueueError("c", 5_000), true);
    assert.strictEqual(shouldLogQueueError("b", 5_001), false);
  });
});
