/**
 * Field list that drives validation, flattening and the CSV header.
 *
 * maxLength applies to text fields only. The array/json fields are serialised
 * into one CSV cell and are never rejected for size: cutting the JSON would
 * corrupt it, and refusing a valid call just because it has many dispositions
 * would send good data to the failure queue.
 */

export type FieldType =
  | "text"
  | "date"
  | "boolean"
  | "number"
  | "array"
  | "json";

export interface FieldSpec {
  name: string;
  type: FieldType;
  maxLength?: number;
  required?: boolean;
}

export const FIELDS: readonly FieldSpec[] = [
  { name: "id", type: "text", maxLength: 100 },
  { name: "contact_name", type: "text", maxLength: 200 },
  { name: "contact_id", type: "text", maxLength: 100 },
  { name: "campaign_name", type: "text", maxLength: 200 },
  { name: "duration", type: "text", maxLength: 20 },
  { name: "call_start_time", type: "date" },
  { name: "bot_name", type: "text", maxLength: 200 },
  { name: "call_type", type: "text", maxLength: 50 },
  { name: "recording_url", type: "text", maxLength: 500 },
  { name: "transcript_url", type: "text", maxLength: 500 },
  { name: "disposition", type: "text", maxLength: 200 },
  { name: "disposition_criticality", type: "text", maxLength: 50 },
  { name: "next_actions", type: "text", maxLength: 500 },
  { name: "next_action_status", type: "text", maxLength: 50 },
  { name: "call_summary", type: "text", maxLength: 4000 },
  { name: "call_ended_reason", type: "text", maxLength: 100 },
  { name: "all_dispositions", type: "array" },
  { name: "next_retry_scheduled", type: "boolean" },
  { name: "call_attempt_number", type: "number" },
  { name: "extracted_additional_details", type: "json" },
  { name: "created_at", type: "date" },
  { name: "contacts_audience_name", type: "text", maxLength: 200 },
  { name: "sub_disposition", type: "text", maxLength: 200 },
  { name: "updated_at", type: "date" },
  { name: "call_connected", type: "boolean" },
  { name: "campaign_group_name", type: "text", maxLength: 200 },
  { name: "unix_call_time", type: "number" },
  { name: "bot_id", type: "text", maxLength: 50 },
  { name: "campaign_id", type: "text", maxLength: 100 },
  { name: "contact_phone", type: "text", maxLength: 20 },
  { name: "campaign_group_id", type: "text", maxLength: 100 },
  { name: "call_duration", type: "number" },
  { name: "customer_id", type: "text", maxLength: 100 },
  { name: "call_id", type: "text", maxLength: 100, required: true },
  { name: "call_engaged", type: "boolean" },
  { name: "call_transferred", type: "boolean" },
  { name: "vin_number", type: "text", maxLength: 50 },
  { name: "dealer_region", type: "text", maxLength: 50 },
  { name: "coms_id", type: "text", maxLength: 100 },
];

export const CSV_COLUMNS: readonly string[] = FIELDS.map((f) => f.name);

export function stringifyJson(value: unknown): string {
  return typeof value === "string" ? value : JSON.stringify(value);
}
