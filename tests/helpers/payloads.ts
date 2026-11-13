import { samplePayload } from "../samplePayload";

let counter = 0;

/** A valid payload with an id/call_id nothing else in the run uses (id drives de-duplication). */
export function uniquePayload(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  counter += 1;
  const tag = `${Date.now().toString(36)}-${counter}`;
  return { ...samplePayload(), id: `call-test-${tag}`, call_id: `call-test-${tag}`, ...overrides };
}

export function uniquePayloads(count: number): Record<string, unknown>[] {
  return Array.from({ length: count }, () => uniquePayload());
}

/** Percentile of a numeric sample (nearest rank). */
export function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}
