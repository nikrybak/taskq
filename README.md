# taskq

A background job queue backed by PostgreSQL, with an HTTP API and a worker pool, written in Go.

You enqueue jobs over HTTP: a type, a JSON payload, a priority, an optional start time and an optional idempotency key. Workers in one or more taskq instances claim the jobs from PostgreSQL, run them through handlers registered by job type, retry failures with exponential backoff, and move jobs that keep failing to a dead-letter state. Recurring jobs come from cron expressions, and each tick is enqueued exactly once however many instances are running.

The only infrastructure needed is the PostgreSQL you probably already have. There is no broker to run and no second source of truth: a job's state and your business data can live in the same database.

```
          +-------------------- taskq instance ---------------------+
 HTTP --> | transport/httpapi --> service --> repository/postgres   | --> PostgreSQL
          |                         ^                               |
          |  worker: pool + heartbeats, reaper, cron scheduler -----+
          +---------------------------------------------------------+
```

## Contents

- [Design](#design)
  - [Claiming with SKIP LOCKED](#claiming-with-skip-locked)
  - [Leases and heartbeats](#leases-and-heartbeats)
  - [Retries, backoff and dead-lettering](#retries-backoff-and-dead-lettering)
  - [Job states](#job-states)
  - [Recurring jobs without duplicates](#recurring-jobs-without-duplicates)
  - [Graceful shutdown](#graceful-shutdown)
- [Project layout](#project-layout)
- [Running](#running)
- [Configuration](#configuration)
- [API](#api)
- [Testing](#testing)
- [Trade-offs and possible extensions](#trade-offs-and-possible-extensions)

## Design

### Claiming with SKIP LOCKED

Workers claim jobs with a single statement:

```sql
WITH next AS (
    SELECT id FROM jobs
    WHERE state = 'queued' AND run_at <= now() AND type = ANY($1)
    ORDER BY priority DESC, run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs j SET state = 'running', attempts = j.attempts + 1, locked_by = $3,
                  lease_expires_at = now() + $4 * interval '1 microsecond', ...
FROM next WHERE j.id = next.id
RETURNING j.*;
```

`FOR UPDATE` alone is correct but slow under concurrency. Every worker selects the same first row, and all but one wait on its row lock, so they end up working one at a time. `SKIP LOCKED` makes each claimer pass over rows another transaction has locked and take the next ones, so N workers get N different jobs without waiting on each other. The select and the state change happen in one statement, which means a job cannot be seen as claimable by two workers.

The partial index `jobs (priority DESC, run_at, id) WHERE state = 'queued'` matches the `ORDER BY`, so the planner walks the index in dispatch order and stops after `LIMIT` unlocked rows. Finished jobs are not in the index, so it stays small however much history the table holds. The pool claims a batch the size of its free slots, so a busy worker makes one round trip per batch, not one per job.

Workers only claim types they have handlers for (`type = ANY($1)`). Instances with different handler sets can therefore share one queue, for example a fleet of CPU-heavy workers that only run `checksum`.

### Leases and heartbeats

Claiming a job grants a lease: `lease_expires_at = now() + TASKQ_LEASE`. While the handler runs, a heartbeat goroutine extends the lease every `TASKQ_HEARTBEAT_INTERVAL`. Config validation requires the interval to be at most half the lease, so one slow or failed heartbeat does not cost the job.

If a worker crashes, is partitioned from the database or stalls, its heartbeats stop and the lease runs out. Every instance runs a reaper that moves expired `running` jobs back to `queued`, or to `dead` if that was the last attempt. The reaper's query also uses `SKIP LOCKED` and changes nothing when run twice, so the reapers in different instances do not need to coordinate.

Every write a worker makes for a running job (heartbeat, complete, reschedule, bury) is fenced by the lease:

```sql
UPDATE jobs SET ... WHERE id = $1 AND state = 'running' AND locked_by = $2 AND attempts = $3
```

`attempts` acts as a fencing token, since each claim increments it. Suppose worker A stalls, its lease expires and worker B claims the job. When A wakes up, its write matches zero rows and returns `ErrLeaseLost`, so it cannot overwrite B's result. A heartbeat that hits `ErrLeaseLost` also cancels the handler's context, and A stops spending effort on work it no longer owns.

All time comparisons use the database's `now()`, so clock skew between instances does not affect leases or `run_at`.

Delivery is at least once. If a worker finishes a job but dies before it records the result, the job runs again. Handlers should be idempotent; the webhook handler sends a stable `Idempotency-Key: taskq-<job id>` header so the receiver can drop duplicates.

### Retries, backoff and dead-lettering

When a handler returns an error, the service decides what happens next (`service.Fail`):

- if the error is wrapped with `domain.Permanent`, or the job has used `max_attempts`, the job moves to `dead` with `last_error` set;
- otherwise it goes back to `queued` with `run_at = now() + backoff(attempt)`.

Backoff is exponential with "equal jitter": `d = min(max, base * 2^(attempt-1))` and the delay is `d/2 + rand[0, d/2)`. The fixed half guarantees a minimum gap between retries. The random half spreads out jobs that failed together, for example when a downstream service went down, so they do not all retry at the same moment. The default is 2 s base and 10 min cap.

Handlers decide what counts as permanent. The webhook handler retries network errors, timeouts, `429` and `5xx`, and treats other `4xx` responses and invalid payloads as permanent. A panic in a handler is recovered and recorded as an ordinary, retryable failure, stack trace included.

Dead jobs stay in the table and can be inspected with `GET /jobs?state=dead`. `POST /jobs/{id}/retry` puts one back in the queue with a fresh attempt budget.

### Job states

```
             claim               success
  queued ------------> running ----------> succeeded
   |  ^                 |   |
   |  | failure with    |   | failure on the last attempt,
   |  | attempts left,  |   | permanent error, or lease lost
   |  | or lease lost   |   v
   |  +-----------------+  dead ----+
   |                                | POST /jobs/{id}/retry
   | DELETE /jobs/{id}              v
   +----------> cancelled ------> queued
```

The transition table is in `internal/domain/state.go`. The service checks it before every operator action, and the database enforces the set of states with a `CHECK` constraint and the invariant "a job has a lease if and only if it is running" with another. Operator actions read the job, validate the transition, then run a conditional `UPDATE ... WHERE state = <state read>`. A job that a worker claims between the read and the write therefore yields `409 conflict` and is never left both cancelled and running.

### Recurring jobs without duplicates

Schedules are loaded from a JSON file (`TASKQ_SCHEDULES_FILE`, see `schedules.example.json`) and parsed by a small five-field cron parser in `internal/cron`. It supports lists, ranges, steps, `@daily` and similar descriptors, and the usual rule that when both day fields are restricted, either one matching is enough.

Every instance runs the scheduler. Duplicates are prevented by the database, not by electing a leader:

1. Tick times are computed purely from the cron expression in UTC, so all instances agree on them.
2. Each tick is enqueued with the idempotency key `schedule:<name>:<tick unix time>`.
3. The unique index on `idempotency_key` lets exactly one insert win. The others hit `ON CONFLICT DO NOTHING` and log at debug level.

An advisory lock would also work, but a lock-holding leader then becomes a special instance: if it hangs while holding the lock, no instance schedules anything. With the unique constraint every instance is equal, any surviving instance keeps the schedule running, and the guarantee holds even across restarts and deploys. If the scheduler falls behind (a paused process or a clock jump), it enqueues one catch-up job and skips the rest of the missed ticks.

### Graceful shutdown

On `SIGINT` or `SIGTERM`:

1. the worker pool stops claiming, and so do the scheduler and the reaper;
2. running jobs keep their contexts and are allowed to finish for up to `TASKQ_SHUTDOWN_TIMEOUT`. Their heartbeats keep running, so nothing gets reclaimed while it drains;
3. jobs still running after that have their contexts cancelled. Well-behaved handlers return promptly and the failure is recorded as a retryable attempt;
4. results are written with a context detached from the shutdown signal, so a job that just finished is still marked succeeded;
5. the HTTP server stops, and the database pool closes last.

A process killed with `SIGKILL` loses nothing either: its leases expire and the jobs are reclaimed.

## Project layout

```
cmd/taskq/                 entry point: config, wiring, serve/migrate subcommands, schedule file loading
internal/domain/           Job, State, transition table, typed errors (ErrNotFound, *TransitionError, ...)
internal/service/          business rules: defaults, validation, idempotency, retry/dead-letter policy
internal/repository/postgres/  all SQL (pgx/v5), migrations runner, integration tests
internal/transport/httpapi/    handlers, DTOs, request validation, error-to-status mapping in one place
internal/worker/           pool, heartbeats, reaper, cron scheduler, handler registry
internal/handlers/         demo handlers: send-webhook (I/O with timeout), checksum (CPU-bound)
internal/backoff/          exponential backoff with jitter
internal/cron/             cron expression parser
internal/config/           environment configuration with validation
migrations/                SQL migrations, embedded with embed.FS
```

Dependencies point inward. `transport` and `worker` depend on small interfaces they define themselves (`httpapi.JobService`, `worker.Queue`), which the service satisfies. The service depends on a `Repository` interface, which the PostgreSQL package implements. Every component gets its dependencies through its constructor, and the only place that knows about all of them is `cmd/taskq`.

## Running

With Docker:

```sh
docker compose up --build        # PostgreSQL plus two taskq replicas on :8080 and :8081
```

Locally, with Go 1.25+ and a PostgreSQL 13+ database:

```sh
cp .env.example .env              # edit TASKQ_DATABASE_URL
export $(grep -v '^#' .env | xargs)
make migrate                      # optional: migrations also run on start unless TASKQ_AUTO_MIGRATE=false
make run
```

`taskq migrate` applies the embedded migrations and exits. Migrations take a PostgreSQL advisory lock, so replicas that start at the same time do not race each other.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `TASKQ_DATABASE_URL` | required | PostgreSQL connection string |
| `TASKQ_HTTP_ADDR` | `:8080` | listen address |
| `TASKQ_AUTO_MIGRATE` | `true` | apply migrations on start |
| `TASKQ_WORKER_ID` | hostname + random suffix | lease owner name, unique per instance |
| `TASKQ_CONCURRENCY` | `8` | jobs run in parallel per instance |
| `TASKQ_POLL_INTERVAL` | `1s` | wait before claiming again when the queue is empty |
| `TASKQ_LEASE` | `30s` | lease granted on claim and on every heartbeat |
| `TASKQ_HEARTBEAT_INTERVAL` | `10s` | must be at most half of the lease |
| `TASKQ_REAP_INTERVAL` | `15s` | how often expired leases are reclaimed |
| `TASKQ_SHUTDOWN_TIMEOUT` | `30s` | drain period for running jobs |
| `TASKQ_BACKOFF_BASE` / `TASKQ_BACKOFF_MAX` | `2s` / `10m` | retry backoff |
| `TASKQ_SCHEDULES_FILE` | empty | JSON file with recurring jobs |
| `TASKQ_LOG_LEVEL` / `TASKQ_LOG_FORMAT` | `info` / `json` | `log/slog` settings |

Invalid values are reported all at once on start, not one at a time.

## API

All bodies are JSON. Errors have one shape, produced in one place (`internal/transport/httpapi/errors.go`):

```json
{"error": {"code": "validation_failed", "message": "priority must be between -100 and 100", "field": "priority"}}
```

| Code | Status |
|---|---|
| `validation_failed` | 400 |
| `not_found` | 404 |
| `invalid_transition`, `conflict`, `idempotency_mismatch` | 409 |
| `body_too_large` | 413 |
| `internal` | 500 (details are logged, never returned) |

### Enqueue a job

```sh
curl -s -X POST localhost:8080/jobs \
  -H 'Idempotency-Key: order-1042-receipt' \
  -d '{
        "type": "send-webhook",
        "payload": {"url": "https://example.com/hooks/order", "body": {"order": 1042}},
        "priority": 10,
        "max_attempts": 8
      }'
```

`201 Created` with the job and a `Location` header. Sending the same key again returns the existing job with `200 OK`. Reusing a key for a different job type returns `409 idempotency_mismatch`. The key can also go in the body as `idempotency_key`.

A delayed job:

```sh
curl -s -X POST localhost:8080/jobs \
  -d '{"type": "checksum", "payload": {"data": "report-2026-03", "rounds": 2000000}, "run_at": "2026-12-01T09:00:00Z"}'
```

| Field | Default | Rules |
|---|---|---|
| `type` | required | up to 128 characters |
| `payload` | `{}` | any JSON, up to 1 MiB |
| `priority` | `0` | -100..100, higher runs first |
| `run_at` | now | RFC 3339 |
| `max_attempts` | `5` | 1..100 |
| `idempotency_key` | none | up to 256 characters |

### Inspect, list, retry, cancel

```sh
curl -s localhost:8080/jobs/42
curl -s 'localhost:8080/jobs?state=dead&type=send-webhook&limit=20'
curl -s 'localhost:8080/jobs?state=dead&cursor=aWQ6MjE'      # next_cursor from the previous page
curl -s -X POST localhost:8080/jobs/42/retry                 # dead or cancelled -> queued
curl -s -X DELETE localhost:8080/jobs/43                     # queued -> cancelled; 409 if running
```

Listing is newest first, with keyset pagination: `next_cursor` is an opaque token, absent on the last page. Keyset pagination, unlike `OFFSET`, stays fast deep into the table and does not skip or repeat rows while jobs are being inserted.

### Stats and health

```sh
curl -s localhost:8080/stats
```

```json
{
  "counts": {"queued": 4, "running": 2, "succeeded": 1830, "dead": 3, "cancelled": 1},
  "oldest_ready_seconds": 0.8,
  "avg_wait_seconds": 0.12
}
```

`oldest_ready_seconds` is how long the oldest runnable job has been waiting, which is the number to alert on. `avg_wait_seconds` is the mean time from `run_at` to start for jobs started in the last five minutes.

`GET /healthz` returns `200` when the database answers within 2 s, otherwise `503`.

### Writing a handler

```go
registry.Register("resize-image", worker.HandlerFunc(func(ctx context.Context, job domain.Job) error {
    var p struct{ Key string `json:"key"` }
    if err := json.Unmarshal(job.Payload, &p); err != nil {
        return domain.Permanent(err) // retrying will not fix a bad payload
    }
    return resize(ctx, p.Key) // must return soon after ctx is cancelled
}), 2*time.Minute)
```

The context is cancelled on the per-type timeout, when the worker loses the lease, and at the end of the shutdown drain period. I/O-bound code gets this for free from `http.NewRequestWithContext` and similar. CPU-bound loops must check `ctx.Err()` themselves; `handlers.Stretch` shows how.

## Testing

```sh
make test                 # unit tests with -race
make test-integration     # against PostgreSQL at TEST_DATABASE_URL
make lint                 # gofmt, go vet, staticcheck
```

The unit tests need no database:

- `domain`: the full transition matrix and payload validation;
- `service`: retry, backoff and dead-letter policy, idempotency, lease fencing and pagination, against an in-memory fake repository with the same conditional-update semantics as the PostgreSQL one;
- `worker`: drain on shutdown, drain-timeout cancellation, handler timeouts, panics, lease loss cancelling the handler, and several scheduler instances enqueueing each tick once;
- `backoff` and `cron`: table-driven, including overflow and leap-day cases;
- `httpapi`: every route through `httptest`, plus the error mapping, body limits, cursors and malformed input;
- `handlers`: webhook retry classification and timeouts against an `httptest` server, and cancellation of the CPU-bound handler.

The integration tests in `internal/repository/postgres` run only when `TEST_DATABASE_URL` is set. They truncate the `jobs` table, so point them at a throwaway database. They cover:

- four worker pools with five slots each processing 300 jobs, each job exactly once;
- an expired lease being reclaimed, the stale worker's writes being rejected, and the job being dead-lettered when the lease expires on its last attempt;
- heartbeats keeping a lease alive;
- 16 concurrent enqueues with the same idempotency key producing one job;
- backoff delaying the next claim, cancel and retry transitions, keyset pagination, and the `CHECK` constraint.

CI (`.github/workflows/ci.yml`) runs lint, then all tests with `-race` against a PostgreSQL service container, then builds the Docker image.

## Trade-offs and possible extensions

- **Polling.** An idle worker polls every `TASKQ_POLL_INTERVAL`, and a full batch triggers an immediate re-claim. `LISTEN/NOTIFY` on enqueue would cut pickup latency to near zero; polling stays as the fallback either way.
- **Table growth.** Finished jobs are kept for inspection. A periodic job that deletes `succeeded` rows older than N days, or partitioning by `created_at`, keeps the table bounded. The claim index is partial and does not grow with history.
- **Throughput.** Every claim and every result is a write to one primary, and each finished job leaves a dead tuple for autovacuum. That goes a long way for typical background work. For sustained very high volume, a dedicated broker is the better tool.
- **Webhook targets.** The demo handler calls any URL it is given. A multi-tenant deployment needs an allow-list or egress proxy to prevent SSRF.
- **Authentication.** The API has none. Put it behind your gateway or add middleware.

## License

MIT, see [LICENSE](LICENSE).
