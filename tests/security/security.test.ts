/**
 * Security testing. The webhook and API routes are deliberately open (the service is live and Sinch
 * does not authenticate), so nothing here expects credentials. These tests cover what can be checked
 * without auth: hostile input, information leakage, and resilience to abuse.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert";
import { startHarness, type Harness } from "../helpers/harness";
import { uniquePayload } from "../helpers/payloads";

describe("Security", () => {
  let h: Harness;
  before(async () => {
    h = await startHarness();
  });
  after(async () => {
    await h.stop();
  });

  const queued = async (id: unknown) =>
    (await h.queues.inputQueue.getJobs(["waiting"])).find((j) => (j.data as { id?: string }).id === id);

  describe("hostile input is treated as data", () => {
    const hostile: Record<string, string> = {
      "SQL injection": "'; DROP TABLE calls; --",
      "script tag": "<script>alert(document.cookie)</script>",
      "HTML/img onerror": '<img src=x onerror="fetch(\'//evil\')">',
      "template injection": "${process.env.REDIS_PASSWORD} {{7*7}} #{7*7}",
      "path traversal": "../../../../etc/passwd",
      "shell metacharacters": "$(rm -rf /); `id`; | cat /etc/shadow",
      "null byte": "before\u0000after",
      "log forging (CRLF)": 'ok\r\n{"level":"error","message":"forged"}',
      "RTL override": "\u202Eevil",
      "very long unicode": "😀".repeat(1500),
    };
    for (const [name, value] of Object.entries(hostile)) {
      it(`accepts and stores ${name} unchanged`, async () => {
        const payload = uniquePayload({ call_summary: value, contact_name: value });
        const res = await h.postJson("/api/webhook", payload);
        assert.strictEqual(res.status, 200);
        assert.deepStrictEqual((await queued(payload.id))?.data, payload);
      });
    }

    it("does not echo request content back in any response", async () => {
      const marker = "REFLECTED-MARKER-12345";
      const ok = await h.postJson("/api/webhook", uniquePayload({ call_summary: marker }));
      const bad = await h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body: `{"x":"${marker}` });
      const missing = await h.request(`/api/${marker}`);
      for (const res of [ok, bad, missing]) assert.doesNotMatch(res.text, new RegExp(marker));
    });
  });

  describe("prototype pollution", () => {
    it("ignores __proto__ / constructor keys at the door", async () => {
      const body = '{"call_id":"pp-1","__proto__":{"polluted":"yes"},"constructor":{"prototype":{"polluted2":"yes"}}}';
      const res = await h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body });
      assert.strictEqual(res.status, 200);
      assert.strictEqual(({} as Record<string, unknown>).polluted, undefined);
      assert.strictEqual(({} as Record<string, unknown>).polluted2, undefined);
    });

    it("stays unpolluted after the worker has processed such a record", async () => {
      await h.startWorkers();
      const body = '{"id":"pp-2","call_id":"pp-2","__proto__":{"polluted3":"yes"},"all_dispositions":[{"__proto__":{"polluted4":"yes"}}]}';
      await h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body });
      await h.waitForIdle();
      const reports = await h.flushReports();
      assert.strictEqual(reports.input.filter((r) => r.id === "pp-2").length, 1);
      assert.strictEqual(({} as Record<string, unknown>).polluted3, undefined);
      assert.strictEqual(({} as Record<string, unknown>).polluted4, undefined);
      await h.stopWorkers();
    });
  });

  describe("request id handling", () => {
    it("replaces a request id containing unsafe characters", async () => {
      for (const id of ["<script>x</script>", "a b c", "id;DROP", "../../x", "a".repeat(129)]) {
        const res = await h.postJson("/api/webhook", uniquePayload(), { "x-request-id": id });
        assert.notStrictEqual(res.headers.get("x-request-id"), id, id);
        assert.match(res.headers.get("x-request-id") ?? "", /^[0-9a-f-]{36}$/);
      }
    });
  });

  describe("information disclosure", () => {
    it("exposes only status and redis state on /api/health", async () => {
      const res = await h.request("/api/health");
      assert.deepStrictEqual(Object.keys(res.json as object).sort(), ["redis", "status"]);
    });

    it("does not expose stack traces or file paths on any error path", async () => {
      const responses = await Promise.all([
        h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body: "{" }),
        h.request("/api/missing"),
        h.postJson("/api/reprocess/failure", { fileName: 5 }),
        h.postJson("/api/reprocess/dlq", {}),
      ]);
      for (const res of responses) {
        assert.doesNotMatch(res.text, /node_modules|\.ts:|\.js:|\bat .+\(|C:\\|\/home\//, res.text);
      }
    });

    it("does not serve source, env or dependency files", async () => {
      for (const p of ["/.env", "/package.json", "/src/index.ts", "/node_modules/express/package.json", "/csv_output/"]) {
        assert.strictEqual((await h.request(p)).status, 404, p);
      }
    });

    it("does not expose the admin dashboard when it is disabled", async () => {
      assert.strictEqual((await h.request("/admin/queues")).status, 404);
    });
  });

  describe("abuse resilience", () => {
    it("survives an oversized body and keeps serving", async () => {
      const res = await h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body: "x".repeat(12 * 1024 * 1024) });
      assert.ok(res.status === 413 || res.status === 400, `status ${res.status}`);
      assert.strictEqual((await h.request("/api/health")).status, 200);
    });

    it("survives deeply nested JSON: a generic answer, no crash, and the server keeps serving", async () => {
      // Around 5,000 levels the queue layer overflows the stack and the webhook answers 500 (a 4xx would be
      // kinder, but Sinch never sends nesting anywhere near this). What matters is that it stays up and generic.
      for (const depth of [50, 2000, 8000, 20000]) {
        const body = '{"call_id":"deep","x":' + "[".repeat(depth) + "]".repeat(depth) + "}";
        const res = await h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body });
        assert.ok([200, 400, 413, 500].includes(res.status), `depth ${depth}: status ${res.status}`);
        assert.doesNotMatch(res.text, /call stack|RangeError/i);
      }
      assert.strictEqual((await h.request("/api/health")).status, 200);
    });

    it("survives a client that drops the connection mid-request", async () => {
      const net = await import("net");
      const { port } = new URL(h.baseUrl);
      await new Promise<void>((resolve) => {
        const socket = net.connect(Number(port), "127.0.0.1", () => {
          socket.write("POST /api/webhook HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"call_id\":");
          setTimeout(() => {
            socket.destroy();
            resolve();
          }, 50);
        });
      });
      assert.strictEqual((await h.request("/api/health")).status, 200);
    });

    it("handles a burst of malformed requests without degrading", async () => {
      await Promise.all(
        Array.from({ length: 200 }, (_, i) =>
          h.request("/api/webhook", { method: "POST", headers: { "content-type": "application/json" }, body: `{bad${i}` }),
        ),
      );
      assert.strictEqual((await h.postJson("/api/webhook", uniquePayload())).status, 200);
    });
  });

  describe("known gaps (tracked, not failing)", () => {
    it.todo("hide the X-Powered-By: Express header (app.disable('x-powered-by'))");
    it.todo("neutralise spreadsheet formula prefixes (=, +, -, @) in CSV cells, if the files are ever opened in Excel");
    it.todo("protect /api/reprocess/* (it can replay SFTP files into the queue; currently open like the rest of the API)");
  });
});
