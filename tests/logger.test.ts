import { test } from "node:test";
import assert from "node:assert";

// dotenv never overrides a variable that is already set, so a local .env cannot enable CloudWatch here.
process.env.AWS_LOG_GROUP = "";
process.env.AWS_LOG_STREAM = "";

type Mod = typeof import("../src/utils/logger");
type Events = typeof import("../src/utils/logEvents");
let loaded: { mod: Mod; events: Events } | undefined;
async function load() {
  loaded ??= {
    mod: await import("../src/utils/logger"),
    events: await import("../src/utils/logEvents"),
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
  return {
    entries,
    stop: () => {
      process.stdout.write = original;
    },
  };
}

test("Logger entries", async (t) => {
  const { mod, events } = await load();

  await t.test("carry event, category, boot_id and a rising log_seq", async () => {
    const cap = captureLogs();
    try {
      mod.logger.info("one", {
        event: events.EVENTS.CSV_FILE_READY,
        file_name: "a.csv",
        record_count: 3,
      });
      mod.logger.warn("two", { event: events.EVENTS.SERVER_STARTUP_READY });
      await new Promise((r) => setImmediate(r));
    } finally {
      cap.stop();
    }
    const [one, two] = cap.entries;
    assert.ok(one && two);
    assert.strictEqual(one.event, "csv.file_ready");
    assert.strictEqual(one.category, "csv");
    assert.strictEqual(one.file_name, "a.csv");
    assert.strictEqual(one.record_count, 3);
    assert.strictEqual(typeof one.boot_id, "string");
    assert.strictEqual(one.boot_id, two.boot_id);
    assert.strictEqual(two.category, "system");
    assert.strictEqual(two.log_seq, (one.log_seq as number) + 1);
  });

  await t.test("a level that is filtered out does not use up a log_seq", async () => {
    const cap = captureLogs();
    try {
      mod.logger.info("before");
      mod.logger.debug("filtered out");
      mod.logger.info("after");
      await new Promise((r) => setImmediate(r));
    } finally {
      cap.stop();
    }
    const [before, after] = cap.entries;
    assert.ok(before && after);
    assert.strictEqual(after.log_seq, (before.log_seq as number) + 1);
  });

  await t.test("still redacts secrets in the new fields' neighbours", async () => {
    const cap = captureLogs();
    try {
      mod.logger.info("secret", { password: "hunter2", file_name: "a.csv" });
      await new Promise((r) => setImmediate(r));
    } finally {
      cap.stop();
    }
    assert.strictEqual(cap.entries[0]?.password, "[REDACTED]");
    assert.strictEqual(cap.entries[0]?.file_name, "a.csv");
  });
});

interface PutEvent {
  timestamp: number;
  message: string;
}

function stubClient(opts: {
  createError?: Error;
  putError?: (putIndex: number) => Error | undefined;
  gate?: Promise<void>;
}) {
  const puts: PutEvent[][] = [];
  let attempts = 0;
  return {
    puts,
    send: async (command: unknown) => {
      const c = command as { constructor: { name: string }; input?: { logEvents: PutEvent[] } };
      if (c.constructor.name === "CreateLogStreamCommand") {
        if (opts.createError) throw opts.createError;
        return {};
      }
      if (opts.gate) await opts.gate;
      const err = opts.putError?.(attempts++);
      if (err) throw err;
      puts.push(c.input!.logEvents);
      return {};
    },
  };
}

async function transportWith(client: ReturnType<typeof stubClient>) {
  const { mod } = await load();
  const transport = new mod.CloudWatchTransport({
    region: "test",
    logGroup: "g",
    logStream: "s",
    client,
  });
  await transport.getStatus();
  return transport;
}

function put(transport: { log: (i: Record<string, unknown>, cb: () => void) => void }, seq: number, extra: Entry = {}) {
  transport.log(
    {
      level: "info",
      message: "m",
      timestamp: new Date(1_700_000_000_000 + seq).toISOString(),
      log_seq: seq,
      ...extra,
    },
    () => {},
  );
}

test("CloudWatch transport", async (t) => {
  const quiet = { error: console.error, warn: console.warn };
  console.error = () => {};
  console.warn = () => {};
  t.after(() => {
    console.error = quiet.error;
    console.warn = quiet.warn;
  });

  await t.test("does not send while the log stream failed to initialise", async () => {
    const client = stubClient({ createError: new Error("denied") });
    const transport = await transportWith(client);
    assert.deepStrictEqual(await transport.getStatus(), { connected: false, error: "denied" });
    for (let i = 1; i <= 5; i++) put(transport, i);
    await transport.flush();
    assert.strictEqual(client.puts.length, 0);
    await transport.close();
  });

  await t.test("sends each batch in timestamp order, using the entry's own time", async () => {
    const client = stubClient({});
    const transport = await transportWith(client);
    put(transport, 3);
    put(transport, 1);
    put(transport, 2);
    await transport.flush();
    assert.strictEqual(client.puts.length, 1);
    assert.deepStrictEqual(
      client.puts[0]!.map((e) => e.timestamp),
      [1, 2, 3].map((n) => 1_700_000_000_000 + n),
    );
    await transport.close();
  });

  await t.test("splits batches under the PutLogEvents size limit", async () => {
    const client = stubClient({});
    const transport = await transportWith(client);
    for (let i = 1; i <= 8; i++) put(transport, i, { message: "x".repeat(190_000) });
    await transport.flush();
    assert.ok(client.puts.length > 1);
    assert.strictEqual(client.puts.flat().length, 8);
    for (const batch of client.puts) {
      const bytes = batch.reduce((n, e) => n + Buffer.byteLength(e.message) + 26, 0);
      assert.ok(bytes <= 1_048_576, `batch of ${bytes} bytes`);
    }
    await transport.close();
  });

  await t.test("shrinks an entry too large to ship, keeping it valid JSON", async () => {
    const client = stubClient({});
    const transport = await transportWith(client);
    put(transport, 1, { message: "y".repeat(300_000), event: "csv.write_error" });
    await transport.flush();
    const sent = JSON.parse(client.puts[0]![0]!.message) as Entry;
    assert.strictEqual(sent.truncated, true);
    assert.ok((sent.original_bytes as number) > 300_000);
    assert.strictEqual(sent.event, "csv.write_error");
    assert.strictEqual(sent.log_seq, 1);
    await transport.close();
  });

  await t.test("caps the buffer, drops the oldest, then records what was lost", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => (release = resolve));
    const client = stubClient({ gate });
    const transport = await transportWith(client);

    // The first 100 entries start a flush that blocks on the gate; the rest pile up behind it.
    for (let i = 1; i <= 10_300; i++) put(transport, i);
    release();
    await transport.flush(); // lets the blocked flush finish, then
    await transport.flush(); // appends the drop summary and sends it

    const sent = client.puts.flat().map((e) => JSON.parse(e.message) as Entry);
    const summary = sent.find((e) => e.event === "logger.events_dropped");
    assert.ok(summary, "drop summary missing");
    assert.strictEqual(summary.dropped_count, 200);
    assert.strictEqual(summary.first_dropped_seq, 101);
    assert.strictEqual(summary.last_dropped_seq, 300);
    // 100 sent early + 10,000 kept + the summary
    assert.strictEqual(sent.length, 10_101);
    await transport.close();
  });

  await t.test("drops a batch CloudWatch rejects as invalid instead of retrying it forever", async () => {
    const rejected = Object.assign(new Error("bad batch"), { name: "InvalidParameterException" });
    const client = stubClient({ putError: (n) => (n === 0 ? rejected : undefined) });
    const transport = await transportWith(client);
    for (let i = 1; i <= 3; i++) put(transport, i);
    await transport.flush();
    await transport.flush();
    assert.strictEqual(client.puts.length, 1);
    const only = JSON.parse(client.puts[0]![0]!.message) as Entry;
    assert.strictEqual(only.event, "logger.events_dropped");
    assert.strictEqual(only.dropped_count, 3);
    await transport.close();
  });

  await t.test("keeps events for the next flush when PutLogEvents fails transiently", async () => {
    const client = stubClient({ putError: (n) => (n === 0 ? new Error("throttled") : undefined) });
    const transport = await transportWith(client);
    for (let i = 1; i <= 3; i++) put(transport, i);
    await transport.flush();
    assert.strictEqual(client.puts.length, 0);
    await transport.flush();
    assert.strictEqual(client.puts.flat().length, 3);
    await transport.close();
  });
});
