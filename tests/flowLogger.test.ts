import { test } from "node:test";
import assert from "node:assert";

// dotenv never overrides a variable that is already set, so a local .env cannot enable CloudWatch here.
process.env.AWS_LOG_GROUP = "";
process.env.AWS_LOG_STREAM = "";

type Flow = typeof import("../src/utils/flowLogger");
type Logger = typeof import("../src/utils/logger");
let loaded: { flow: Flow; logger: Logger } | undefined;
async function load() {
  loaded ??= {
    flow: await import("../src/utils/flowLogger"),
    logger: await import("../src/utils/logger"),
  };
  return loaded;
}

type Entry = Record<string, unknown>;

/** Collects the logger's console output; anything else (the test reporter's own output) passes through. */
function captureLogs() {
  const entries: Entry[] = [];
  const original = process.stdout.write.bind(process.stdout);
  process.stdout.write = ((chunk: string | Uint8Array, ...rest: unknown[]) => {
    const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString();
    if (text.includes('"service":"sinch-ai-bot-webhook"')) {
      for (const line of text.split("\n")) {
        if (line.trim()) entries.push(JSON.parse(line) as Entry);
      }
      return true;
    }
    return (original as (...a: unknown[]) => boolean)(chunk, ...rest);
  }) as typeof process.stdout.write;
  return { entries, stop: () => void (process.stdout.write = original) };
}

const BODY = { id: "call-1", call_id: "c-1", contact_name: "Sameer Doe" };
const base = {
  type: "RETRY" as const,
  component: "test",
  functionName: "doWork",
  stage: "STEP",
  correlationId: "req-1",
  jobId: "7",
};

test("Flow logger", async (t) => {
  const { flow, logger } = await load();

  await t.test("every entry carries the common fields and the request body", () => {
    const cap = captureLogs();
    try {
      flow.flowLog({ ...base, status: "STARTED", requestBody: BODY });
      flow.flowLog({ ...base, status: "SUCCESS", requestBody: BODY, responseBody: { ok: true }, statusCode: 200 });
    } finally {
      cap.stop();
    }
    const [started, success] = cap.entries;
    assert.ok(started && success);
    for (const e of [started, success]) {
      assert.strictEqual(e.processing_type, "RETRY");
      assert.strictEqual(e.function_name, "doWork");
      assert.strictEqual(e.stage, "STEP");
      assert.strictEqual(e.correlation_id, "req-1");
      assert.strictEqual(e.request_id, "req-1");
      assert.strictEqual(e.job_id, "7");
      assert.deepStrictEqual(e.request_body, BODY);
      assert.match(String(e.timestamp_ist), /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}\+05:30$/);
    }
    assert.strictEqual(started.flow_status, "STARTED");
    assert.strictEqual(success.flow_status, "SUCCESS");
    assert.deepStrictEqual(success.response_body, { ok: true });
    assert.strictEqual(success.status_code, 200);
  });

  await t.test("timestamp_ist is the same instant as timestamp, shifted by +05:30", () => {
    assert.strictEqual(
      logger.toIst(new Date("2026-11-03T09:37:16.655Z")),
      "2026-11-03T15:07:16.655+05:30",
    );
  });

  await t.test("a failure logs error type, message, stack, category and the step that failed", () => {
    const cap = captureLogs();
    const err = new Error("disk full");
    try {
      flow.flowLog({
        ...base,
        status: "FAILED",
        requestBody: BODY,
        error: err,
        errorCode: "X",
        failureCategory: "FILE_OPERATION",
        failedStep: { functionName: "writeRow", stage: "CSV_WRITE", category: "FILE_OPERATION" },
      });
    } finally {
      cap.stop();
    }
    const [e] = cap.entries;
    assert.ok(e);
    assert.strictEqual(e.level, "error");
    assert.strictEqual(e.flow_status, "FAILED");
    assert.strictEqual(e.error_type, "Error");
    assert.strictEqual(e.error_message, "disk full");
    assert.strictEqual(e.stack_trace, err.stack);
    assert.strictEqual(e.failure_category, "FILE_OPERATION");
    assert.strictEqual(e.failed_function, "writeRow");
    assert.strictEqual(e.failed_stage, "CSV_WRITE");
    assert.strictEqual(e.function_name, "doWork");
    assert.deepStrictEqual(e.request_body, BODY);
  });

  await t.test("a failure without a thrown error keeps its reason, and warn/info levels are honoured", () => {
    const cap = captureLogs();
    try {
      flow.flowLog({ ...base, status: "FAILED", level: "warn", errorCode: "W", errorMessage: "bad input" });
      flow.flowLog({ ...base, status: "FAILED", level: "info", errorMessage: "client error" });
    } finally {
      cap.stop();
    }
    const [warn, info] = cap.entries;
    assert.ok(warn && info);
    assert.strictEqual(warn.level, "warn");
    assert.strictEqual(warn.error_message, "bad input");
    assert.strictEqual(warn.warning_code, "W");
    assert.strictEqual(info.level, "info");
    assert.strictEqual(info.status, "failed");
  });

  await t.test("fields left unset do not wipe values supplied through extra", () => {
    const cap = captureLogs();
    try {
      flow.flowLog({ ...base, status: "SUCCESS", extra: { method: "POST", attempt: 2 } });
    } finally {
      cap.stop();
    }
    assert.strictEqual(cap.entries[0]?.method, "POST");
    assert.strictEqual(cap.entries[0]?.attempt, 2);
  });

  await t.test("secrets in a body are redacted; an oversized body becomes a size summary", () => {
    const cap = captureLogs();
    try {
      flow.flowLog({ ...base, status: "STARTED", requestBody: { id: "a", password: "hunter2" } });
      flow.flowLog({ ...base, status: "STARTED", requestBody: { blob: "x".repeat(70_000) } });
    } finally {
      cap.stop();
    }
    const [secret, big] = cap.entries;
    assert.ok(secret && big);
    assert.deepStrictEqual(secret.request_body, { id: "a", password: "[REDACTED]" });
    assert.strictEqual(big.request_body_truncated, true);
    assert.ok((big.request_body_bytes as number) > 64 * 1024);
    assert.strictEqual(big.request_body, undefined);
  });

  await t.test("logging never throws into the caller, even for a body that cannot be serialized", () => {
    const circular: Record<string, unknown> = {};
    circular.self = circular;
    const cap = captureLogs();
    try {
      assert.doesNotThrow(() =>
        flow.flowLog({ ...base, status: "STARTED", requestBody: circular }),
      );
      assert.doesNotThrow(() =>
        flow.flowLog({ ...base, status: "FAILED", error: Object.create(null) }),
      );
    } finally {
      cap.stop();
    }
    assert.strictEqual(cap.entries[0]?.request_body, "[unserializable]");
    assert.strictEqual(cap.entries.length, 2);
  });

  await t.test("createFlow fills in the identifying fields once, and each call can override them", () => {
    const cap = captureLogs();
    try {
      const log = flow.createFlow({ type: "DLQ", component: "c", correlationId: "r", jobId: "9" });
      log({ functionName: "f", stage: "S1", status: "STARTED" });
      log({ functionName: "f", stage: "S2", status: "SUCCESS", jobId: "10" });
    } finally {
      cap.stop();
    }
    const [one, two] = cap.entries;
    assert.ok(one && two);
    assert.strictEqual(one.processing_type, "DLQ");
    assert.strictEqual(one.job_id, "9");
    assert.strictEqual(two.job_id, "10");
    assert.strictEqual(two.correlation_id, "r");
  });

  await t.test("tagFailedStep names the failing step without altering the error; the first tag wins", () => {
    const err = new Error("boom");
    const out = flow.tagFailedStep(err, { functionName: "a", stage: "A", category: "VALIDATION" });
    flow.tagFailedStep(err, { functionName: "b", stage: "B", category: "API_EXECUTION" });
    assert.strictEqual(out, err);
    assert.strictEqual(err.message, "boom");
    assert.strictEqual(flow.failedStepOf(err)?.functionName, "a");
    assert.strictEqual(flow.failedStepOf("not an object"), undefined);
    assert.doesNotThrow(() => flow.tagFailedStep("a string", { functionName: "a", stage: "A", category: "VALIDATION" }));
  });
});
