/**
 * The Bull Board dashboard can retry/delete jobs and show payloads. It must stay OFF in
 * production unless explicitly enabled. Config is read at import time, so each case runs in a fresh process.
 */

import { describe, it } from "node:test";
import assert from "node:assert";
import { execFileSync } from "child_process";

function configFor(env: Record<string, string>): { bull: boolean } {
  const out = execFileSync(
    process.execPath,
    [
      "--import",
      "tsx",
      "-e",
      'const c=require("./src/config/env").default;console.log(JSON.stringify({bull:c.bullBoard.enabled}))',
    ],
    {
      cwd: process.cwd(),
      encoding: "utf8",
      // Blank (not unset) so a developer's .env cannot fill them in.
      env: { ...process.env, BULL_BOARD_ENABLED: "", ...env },
    },
  );
  return JSON.parse(out.trim().split("\n").pop() as string);
}

describe("Security: admin dashboards by environment", () => {
  it("are off in production by default", () => {
    assert.deepStrictEqual(configFor({ NODE_ENV: "production" }), { bull: false });
  });

  it("are on outside production by default", () => {
    assert.deepStrictEqual(configFor({ NODE_ENV: "development" }), { bull: true });
  });

  it("can be enabled in production only explicitly", () => {
    assert.deepStrictEqual(configFor({ NODE_ENV: "production", BULL_BOARD_ENABLED: "true" }), { bull: true });
  });

  it("treats anything other than 'true' as off when set", () => {
    assert.deepStrictEqual(configFor({ NODE_ENV: "development", BULL_BOARD_ENABLED: "yes" }), { bull: false });
  });
});
