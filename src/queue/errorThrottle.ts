/**
 * Redis outages make every queue and worker emit an error event on every retry, which used to
 * produce hundreds of identical error log entries within minutes. Log the first one per source,
 * then at most one more per interval, so an outage shows up once instead of burying the logs.
 */

const INTERVAL_MS = 30_000;

const lastLogged = new Map<string, number>();

export function shouldLogQueueError(
  source: string,
  now: number = Date.now(),
): boolean {
  const last = lastLogged.get(source);
  if (last !== undefined && now - last < INTERVAL_MS) return false;
  lastLogged.set(source, now);
  return true;
}
