// Package postgres implements job storage on PostgreSQL with pgx. All SQL in
// taskq lives in this package.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nikrybak/taskq/internal/domain"
)

// Repository stores jobs in PostgreSQL.
type Repository struct {
	pool *pgxpool.Pool
}

// New returns a Repository backed by pool.
func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

const jobColumns = `id, type, payload, priority, state, attempts, max_attempts, run_at,
	idempotency_key, last_error, locked_by, lease_expires_at, started_at, finished_at,
	created_at, updated_at`

func scanJob(row pgx.Row) (domain.Job, error) {
	var (
		j                        domain.Job
		state                    string
		key, lastErr, lockedBy   *string
		lease, started, finished *time.Time
	)
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Priority, &state, &j.Attempts, &j.MaxAttempts, &j.RunAt,
		&key, &lastErr, &lockedBy, &lease, &started, &finished, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return domain.Job{}, err
	}
	j.State = domain.State(state)
	j.IdempotencyKey = deref(key)
	j.LastError = deref(lastErr)
	j.LockedBy = deref(lockedBy)
	j.LeaseExpiresAt, j.StartedAt, j.FinishedAt = lease, started, finished
	return j, nil
}

func scanJobs(rows pgx.Rows) ([]domain.Job, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (domain.Job, error) { return scanJob(r) })
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Create inserts a job. When the idempotency key already exists it returns
// the existing job and created=false instead of inserting a duplicate.
func (r *Repository) Create(ctx context.Context, n domain.NewJob) (job domain.Job, created bool, err error) {
	payload := string(n.Payload)
	if payload == "" {
		payload = "{}"
	}
	var runAt *time.Time
	if !n.RunAt.IsZero() {
		runAt = &n.RunAt
	}

	// ON CONFLICT DO NOTHING instead of a pre-check keeps this race-free: two
	// concurrent requests with the same key cannot both insert.
	row := r.pool.QueryRow(ctx, `
		INSERT INTO jobs (type, payload, priority, run_at, max_attempts, idempotency_key)
		VALUES ($1, $2::jsonb, $3, COALESCE($4, now()), $5, $6)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING `+jobColumns,
		n.Type, payload, n.Priority, runAt, n.MaxAttempts, nullable(n.IdempotencyKey))
	job, err = scanJob(row)
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) || n.IdempotencyKey == "" {
		return domain.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	job, err = scanJob(r.pool.QueryRow(ctx,
		`SELECT `+jobColumns+` FROM jobs WHERE idempotency_key = $1`, n.IdempotencyKey))
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("load job by idempotency key: %w", mapNoRows(err))
	}
	return job, false, nil
}

// Get returns a job by ID.
func (r *Repository) Get(ctx context.Context, id int64) (domain.Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job %d: %w", id, mapNoRows(err))
	}
	return job, nil
}

// List returns jobs matching f, newest first.
func (r *Repository) List(ctx context.Context, f domain.ListFilter) ([]domain.Job, error) {
	// Built dynamically rather than with "($1 = '' OR state = $1)" so that the
	// planner sees concrete predicates and can use the (state, id) and
	// (type, id) indexes.
	var (
		where []string
		args  []any
	)
	add := func(cond string, arg any) {
		args = append(args, arg)
		where = append(where, strings.ReplaceAll(cond, "?", "$"+strconv.Itoa(len(args))))
	}
	if f.State != "" {
		add("state = ?", string(f.State))
	}
	if f.Type != "" {
		add("type = ?", f.Type)
	}
	if f.AfterID > 0 {
		add("id < ?", f.AfterID)
	}

	q := `SELECT ` + jobColumns + ` FROM jobs`
	if len(where) > 0 {
		q += ` WHERE ` + strings.Join(where, " AND ")
	}
	args = append(args, f.Limit)
	q += ` ORDER BY id DESC LIMIT $` + strconv.Itoa(len(args))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

// Claim atomically moves up to limit runnable jobs of the given types to the
// running state and leases them to workerID.
func (r *Repository) Claim(ctx context.Context, workerID string, types []string, limit int, lease time.Duration) ([]domain.Job, error) {
	// SKIP LOCKED lets concurrent claimers pass over rows another transaction
	// is already claiming instead of queueing behind its row locks, so N
	// workers get N different jobs without contention. now() is the database
	// clock, which keeps lease and run_at comparisons consistent across
	// instances with skewed clocks.
	rows, err := r.pool.Query(ctx, `
		WITH next AS (
			SELECT id FROM jobs
			WHERE state = 'queued' AND run_at <= now() AND type = ANY($1)
			ORDER BY priority DESC, run_at, id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j SET
			state            = 'running',
			attempts         = j.attempts + 1,
			locked_by        = $3,
			lease_expires_at = now() + $4::bigint * interval '1 microsecond',
			started_at       = now(),
			updated_at       = now()
		FROM next
		WHERE j.id = next.id
		RETURNING `+qualified("j"),
		types, limit, workerID, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return jobs, nil
}

// ExtendLease pushes the lease of a running job forward.
func (r *Repository) ExtendLease(ctx context.Context, l domain.Lease, lease time.Duration) error {
	return r.execFenced(ctx, l, "extend lease", `
		UPDATE jobs SET lease_expires_at = now() + $4::bigint * interval '1 microsecond', updated_at = now()
		WHERE id = $1 AND state = 'running' AND locked_by = $2 AND attempts = $3`,
		lease.Microseconds())
}

// Complete marks a running job as succeeded.
func (r *Repository) Complete(ctx context.Context, l domain.Lease) error {
	return r.execFenced(ctx, l, "complete job", `
		UPDATE jobs SET state = 'succeeded', last_error = NULL, locked_by = NULL, lease_expires_at = NULL,
			finished_at = now(), updated_at = now()
		WHERE id = $1 AND state = 'running' AND locked_by = $2 AND attempts = $3`)
}

// Reschedule returns a running job to the queue to be retried after delay.
func (r *Repository) Reschedule(ctx context.Context, l domain.Lease, delay time.Duration, lastErr string) error {
	return r.execFenced(ctx, l, "reschedule job", `
		UPDATE jobs SET state = 'queued', last_error = $4, locked_by = NULL, lease_expires_at = NULL,
			run_at = now() + $5::bigint * interval '1 microsecond', updated_at = now()
		WHERE id = $1 AND state = 'running' AND locked_by = $2 AND attempts = $3`,
		lastErr, delay.Microseconds())
}

// Bury moves a running job to the dead-letter state.
func (r *Repository) Bury(ctx context.Context, l domain.Lease, lastErr string) error {
	return r.execFenced(ctx, l, "bury job", `
		UPDATE jobs SET state = 'dead', last_error = $4, locked_by = NULL, lease_expires_at = NULL,
			finished_at = now(), updated_at = now()
		WHERE id = $1 AND state = 'running' AND locked_by = $2 AND attempts = $3`,
		lastErr)
}

// execFenced runs an update that only applies while the lease is still held.
// Zero affected rows means someone else owns the job now.
func (r *Repository) execFenced(ctx context.Context, l domain.Lease, op, sql string, extra ...any) error {
	args := append([]any{l.JobID, l.WorkerID, l.Attempt}, extra...)
	tag, err := r.pool.Exec(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("%s %d: %w", op, l.JobID, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s %d: %w", op, l.JobID, domain.ErrLeaseLost)
	}
	return nil
}

// Requeue moves a job from state from back to queued for immediate execution
// with a fresh attempt budget. It returns domain.ErrConflict if the job is no
// longer in state from.
func (r *Repository) Requeue(ctx context.Context, id int64, from domain.State) (domain.Job, error) {
	return r.updateFrom(ctx, id, from, `
		UPDATE jobs SET state = 'queued', attempts = 0, run_at = now(), finished_at = NULL, updated_at = now()
		WHERE id = $1 AND state = $2
		RETURNING `+jobColumns)
}

// Cancel moves a job from state from to cancelled. It returns
// domain.ErrConflict if the job is no longer in state from.
func (r *Repository) Cancel(ctx context.Context, id int64, from domain.State) (domain.Job, error) {
	return r.updateFrom(ctx, id, from, `
		UPDATE jobs SET state = 'cancelled', finished_at = now(), updated_at = now()
		WHERE id = $1 AND state = $2
		RETURNING `+jobColumns)
}

func (r *Repository) updateFrom(ctx context.Context, id int64, from domain.State, sql string) (domain.Job, error) {
	job, err := scanJob(r.pool.QueryRow(ctx, sql, id, string(from)))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, fmt.Errorf("update job %d: %w", id, domain.ErrConflict)
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("update job %d: %w", id, err)
	}
	return job, nil
}

// ReclaimExpired returns jobs whose lease ran out to the queue, or buries
// them if they have used every attempt. A lease only expires when its worker
// stopped heartbeating (crash, network partition, stalled process), so the
// lost run counts as a failed attempt.
func (r *Repository) ReclaimExpired(ctx context.Context, batch int) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs SET
			state            = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
			finished_at      = CASE WHEN attempts >= max_attempts THEN now() END,
			last_error       = 'lease expired: worker ' || COALESCE(locked_by, 'unknown') || ' stopped heartbeating',
			locked_by        = NULL,
			lease_expires_at = NULL,
			run_at           = now(),
			updated_at       = now()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE state = 'running' AND lease_expires_at < now()
			ORDER BY lease_expires_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)`, batch)
	if err != nil {
		return 0, fmt.Errorf("reclaim expired leases: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Stats returns job counts per state and queue latency figures.
func (r *Repository) Stats(ctx context.Context) (domain.Stats, error) {
	stats := domain.Stats{Counts: make(map[domain.State]int64, len(domain.AllStates))}
	for _, s := range domain.AllStates {
		stats.Counts[s] = 0
	}

	rows, err := r.pool.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return domain.Stats{}, fmt.Errorf("count jobs: %w", err)
	}
	var (
		state string
		count int64
	)
	if _, err := pgx.ForEachRow(rows, []any{&state, &count}, func() error {
		stats.Counts[domain.State(state)] = count
		return nil
	}); err != nil {
		return domain.Stats{}, fmt.Errorf("count jobs: %w", err)
	}

	var oldest, avgWait float64
	err = r.pool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT EXTRACT(EPOCH FROM now() - min(run_at))
			          FROM jobs WHERE state = 'queued' AND run_at <= now()), 0)::float8,
			COALESCE((SELECT EXTRACT(EPOCH FROM avg(started_at - run_at))
			          FROM jobs WHERE started_at > now() - interval '5 minutes'), 0)::float8`,
	).Scan(&oldest, &avgWait)
	if err != nil {
		return domain.Stats{}, fmt.Errorf("queue latency: %w", err)
	}
	stats.OldestReadyAge = time.Duration(oldest * float64(time.Second))
	stats.AvgWait = time.Duration(avgWait * float64(time.Second))
	return stats, nil
}

// Ping checks database connectivity.
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func mapNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	return err
}

func qualified(alias string) string {
	cols := strings.Split(jobColumns, ",")
	for i, c := range cols {
		cols[i] = alias + "." + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}
