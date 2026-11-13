/** Validates a payload against payloadSchema. */

import { FIELDS, type FieldSpec } from "./payloadSchema";

export interface ValidationResult {
  valid: boolean;
  errors: string[];
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function checkField(field: FieldSpec, value: unknown): string | null {
  const { name, type, maxLength, required } = field;

  switch (type) {
    case "text":
      if (typeof value !== "string") return `${name} must be a string`;
      if (required && value.trim() === "") return `${name} must not be empty`;
      break;
    case "date":
      if (typeof value !== "string" || Number.isNaN(Date.parse(value))) {
        return `${name} must be a date string`;
      }
      break;
    case "boolean":
      if (typeof value !== "boolean") return `${name} must be a boolean`;
      break;
    case "number":
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return `${name} must be a number`;
      }
      break;
    case "array":
      if (!Array.isArray(value)) return `${name} must be an array`;
      break;
    case "json":
      break;
  }

  if (
    type === "text" &&
    maxLength !== undefined &&
    (value as string).length > maxLength
  ) {
    return `${name} exceeds ${maxLength} characters`;
  }
  return null;
}

export function validatePayload(payload: unknown): ValidationResult {
  if (!isPlainObject(payload)) {
    return { valid: false, errors: ["Payload must be a JSON object"] };
  }

  const errors: string[] = [];
  for (const field of FIELDS) {
    const value = payload[field.name];
    if (value === undefined || value === null) {
      if (field.required) errors.push(`${field.name} is required`);
      continue;
    }
    const error = checkField(field, value);
    if (error) errors.push(error);
  }
  return { valid: errors.length === 0, errors };
}
