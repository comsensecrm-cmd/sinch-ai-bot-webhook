/** Wall-clock scheduler for CSV rotation. */

import config from "../config/env";

export interface FlushSchedule {
  times: Array<{ hour: number; minute: number }>;
  timeZone: string;
}

function buildIntervalSchedule(
  intervalMinutes: number,
  timeZone: string,
): FlushSchedule {
  if (
    !Number.isInteger(intervalMinutes) ||
    intervalMinutes < 1 ||
    1440 % intervalMinutes !== 0
  ) {
    throw new Error(
      `Invalid CSV_FLUSH_INTERVAL_MINUTES: "${intervalMinutes}" (must be a whole number of minutes that divides 1440 evenly, e.g. 15, 30, 60)`,
    );
  }
  const times: Array<{ hour: number; minute: number }> = [];
  for (let m = 0; m < 1440; m += intervalMinutes) {
    times.push({ hour: Math.floor(m / 60), minute: m % 60 });
  }
  return { times, timeZone };
}

export const flushSchedule: FlushSchedule = buildIntervalSchedule(
  config.csv.flushIntervalMinutes,
  config.csv.flushTimeZone,
);

function getZonedParts(date: Date, timeZone: string) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(date);

  const get = (type: string) =>
    Number(parts.find((p) => p.type === type)?.value ?? "0");
  return {
    year: get("year"),
    month: get("month"),
    day: get("day"),
    hour: get("hour") % 24, // Intl can emit "24" for midnight in some environments
    minute: get("minute"),
    second: get("second"),
  };
}

export function msUntilNextFlush(
  schedule: FlushSchedule,
  now: Date = new Date(),
): number {
  const zoned = getZonedParts(now, schedule.timeZone);
  const nowMinutesOfDay = zoned.hour * 60 + zoned.minute + zoned.second / 60;

  let bestDayOffset = 1;
  let bestTime = schedule.times[0];

  for (const t of schedule.times) {
    if (t.hour * 60 + t.minute > nowMinutesOfDay) {
      bestDayOffset = 0;
      bestTime = t;
      break;
    }
  }

  const utcMs = now.getTime();
  const zonedAsUtcMs = Date.UTC(
    zoned.year,
    zoned.month - 1,
    zoned.day,
    zoned.hour,
    zoned.minute,
    zoned.second,
  );
  const tzOffsetMs = zonedAsUtcMs - utcMs;

  const targetZonedAsUtcMs =
    Date.UTC(
      zoned.year,
      zoned.month - 1,
      zoned.day,
      bestTime.hour,
      bestTime.minute,
      0,
    ) +
    bestDayOffset * 24 * 60 * 60 * 1000;

  const targetUtcMs = targetZonedAsUtcMs - tzOffsetMs;

  const delay = targetUtcMs - utcMs;
  return delay > 0 ? delay : delay + 24 * 60 * 60 * 1000;
}

export class ScheduledFlushTimer {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private firing = false;

  constructor(
    private readonly schedule: FlushSchedule,
    private readonly onFlush: () => void | Promise<void>,
  ) {}

  start(): void {
    this._scheduleNext();
  }

  stop(): void {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private _scheduleNext(): void {
    this.stop();
    const delay = msUntilNextFlush(this.schedule);
    const t = setTimeout(() => this._fire(), delay);
    t.unref();
    this.timer = t;
  }

  private async _fire(): Promise<void> {
    this.timer = null;

    if (this.firing) {
      this._scheduleNext();
      return;
    }

    this.firing = true;
    try {
      await this.onFlush();
    } finally {
      this.firing = false;
      this._scheduleNext();
    }
  }
}
