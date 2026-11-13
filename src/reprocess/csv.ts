/** Minimal RFC 4180 CSV parser/serializer (quoted fields, embedded commas and line breaks). */

export type CsvRow = Record<string, string>;

export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  const input = text.charCodeAt(0) === 0xfeff ? text.slice(1) : text;
  const endRow = () => {
    row.push(field);
    field = "";
    if (row.length > 1 || row[0] !== "") rows.push(row);
    row = [];
  };

  for (let i = 0; i < input.length; i++) {
    const ch = input[i];
    if (inQuotes) {
      if (ch !== '"') field += ch;
      else if (input[i + 1] === '"') {
        field += '"';
        i++;
      } else inQuotes = false;
    } else if (ch === '"') inQuotes = true;
    else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n" || ch === "\r") {
      if (ch === "\r" && input[i + 1] === "\n") i++;
      endRow();
    } else field += ch;
  }
  if (inQuotes) throw new Error("Malformed CSV: unterminated quoted field");
  if (field !== "" || row.length > 0) endRow();
  return rows;
}

const escapeField = (value: string): string =>
  /[,"\r\n]/.test(value) ? `"${value.replace(/"/g, '""')}"` : value;

export function toCsv(columns: readonly string[], records: CsvRow[]): string {
  const lines = [columns.map(escapeField).join(",")];
  for (const record of records) {
    lines.push(columns.map((c) => escapeField(record[c] ?? "")).join(","));
  }
  return lines.join("\r\n") + "\r\n";
}
