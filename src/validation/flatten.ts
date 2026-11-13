/** Flattens a validated payload into one CSV row. */

import { FIELDS, stringifyJson } from "./payloadSchema";

export function flattenPayload(
  payload: Record<string, unknown>,
): Record<string, string> {
  const row: Record<string, string> = {};
  for (const field of FIELDS) {
    const value = payload[field.name];
    if (value === undefined || value === null) {
      row[field.name] = "";
    } else if (field.type === "array" || field.type === "json") {
      row[field.name] = stringifyJson(value);
    } else {
      row[field.name] = String(value);
    }
  }
  return row;
}
