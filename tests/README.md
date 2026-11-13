# Tests

Node's built-in test runner (`node:test`) via `tsx`, no extra test framework. Nothing here adds auth to any route.

## Running

```powershell
npm test                  # unit tests: fast, no Redis needed

npm run test:redis:up     # start a throwaway Redis on 127.0.0.1:4998 (Docker)
npm run test:all          # unit + every Redis-backed suite + performance
npm run test:redis:down   # stop it when done
```

Run one kind: `npm run test:integration | test:api | test:webhook | test:security | test:e2e | test:reliability | test:regression | test:performance`.

## Safety: tests never touch the real Redis

The Redis-backed suites flush their database, so `tests/setupRedis.ts` refuses to run unless `REDIS_URL` is a **local** host and **not** 4112 (docker-compose), 4001 (`.env.example`) or 6379. It also blanks SFTP and AWS, and points CSV output at a temp folder, so a developer's `.env` can't leak in. If the test Redis is not running the suites fail with a message saying how to start it. They never skip silently.

App logs for failures that tests provoke on purpose are hidden; set `TEST_VERBOSE_LOGS=1` to see them.

## What covers what

| # | Testing type | Where | What it checks |
|---|---|---|---|
| 1 | Unit | `tests/*.test.ts` | validator, flatten, CSV writer and parser, logger, flow logger, error throttle |
| 2 | Integration | `tests/integration/` | producer and queues, input/failure/DLQ workers, Redis claims, two instances sharing a queue |
| 3 | API | `tests/api/webhook.api.test.ts`, `reprocess*.api.test.ts` | `/api/webhook`, `/api/health`, `/api/reprocess/*`, 404s, headers, request ids |
| 4 | Webhook | `tests/webhook/` | what Sinch sends: full payloads, nulls, unicode, gzip, batches of 1000, redelivery, content types |
| 5 | Security | `tests/security/` | hostile input, prototype pollution, info leaks, oversized/deeply nested/aborted requests, admin dashboards off in production |
| 6 | Negative | `tests/api/negative.api.test.ts` | malformed JSON, wrong types per field, over-length values, over 10 MB |
| 7 | Performance | `tests/performance/` | 2000 req at 100 concurrent, 5000-record batch, health under load, memory growth, worker drain |
| 8 | Regression | `tests/regression/` | CSV column contracts, response body, Redis key formats, past bug fixes |
| 9 | End-to-end | `tests/e2e/` | HTTP to workers to real CSV files, including failure routing and de-duplication |
| 10 | Reliability | `tests/reliability/` | Redis/queue failure, retries then DLQ, replay after DLQ, poison records, restart durability |

`Performance` here is a regression check with loose limits (override with `PERF_P95_MS`, `PERF_MIN_RPS`).
## Known gaps

Tracked as `todo` tests in `tests/security/security.test.ts` (shown as TODO in the report, not failures):
`X-Powered-By` header, CSV formula-prefix neutralising, and `/api/reprocess/*` being unauthenticated.

## Adding tests

Redis-backed test files share one set of module singletons per process, so use one `startHarness()` in a single top-level `describe` per file (see `tests/helpers/harness.ts`). Use `uniquePayload()` for records: the record `id` is the de-duplication key.
