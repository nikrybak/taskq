// Package service implements the business rules of the queue: enqueue
// defaults and validation, idempotency, the retry and dead-letter policy, and
// which state transitions operators and workers may perform.
package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nikrybak/taskq/internal/backoff"
	"github.com/nikrybak/taskq/internal/domain"
)

// Repository is the storage the service needs. Methods that act on a running
// job are fenced by its lease and return domain.ErrLeaseLost when the lease
// is no longer held; Requeue and Cancel return domain.ErrConflict when the
// job left the expected state concurrently.
type Repository interface {
	Create(ctx context.Context, n domain.NewJob) (domain.Job, bool, error)
	Get(ctx context.Context, id int64) (domain.Job, error)
	List(ctx context.Context, f domain.ListFilter) ([]domain.Job, error)
	Claim(ctx context.Context, workerID string, types []string, limit int, lease time.Duration) ([]domain.Job, error)
	ExtendLease(ctx context.Context, l domain.Lease, lease time.Duration) error
	Complete(ctx context.Context, l domain.Lease) error
	Reschedule(ctx context.Context, l domain.Lease, delay time.Duration, lastErr string) error
	Bury(ctx context.Context, l domain.Lease, lastErr string) error
	Requeue(ctx context.Context, id int64, from domain.State) (domain.Job, error)
	Cancel(ctx context.Context, id int64, from domain.State) (domain.Job, error)
	ReclaimExpired(ctx context.Context, batch int) (int64, error)
	Stats(ctx context.Context) (domain.Stats, error)
}

// Config tunes the service.
type Config struct {
	// Lease is how long a claimed job stays owned without a heartbeat.
	Lease   time.Duration
	Backoff backoff.Policy
}

// List page sizes.
const (
	DefaultPageSize = 50
	MaxPageSize     = 200
)

const (
	maxErrorLen  = 4 << 10
	reclaimBatch = 500
)

// Service is the job queue application service.
type Service struct {
	repo Repository
	cfg  Config
	log  *slog.Logger
}

// New returns a Service.
func New(repo Repository, cfg Config, log *slog.Logger) *Service {
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	return &Service{repo: repo, cfg: cfg, log: log}
}

// Enqueue validates and stores a new job. If the idempotency key was seen
// before, the original job is returned with created=false.
func (s *Service) Enqueue(ctx context.Context, n domain.NewJob) (job domain.Job, created bool, err error) {
	if n.MaxAttempts == 0 {
		n.MaxAttempts = domain.DefaultMaxAttempts
	}
	if err := n.Validate(); err != nil {
		return domain.Job{}, false, err
	}

	job, created, err = s.repo.Create(ctx, n)
	if err != nil {
		return domain.Job{}, false, err
	}
	if !created && job.Type != n.Type {
		return domain.Job{}, false, fmt.Errorf("key %q belongs to job %d of type %q: %w",
			n.IdempotencyKey, job.ID, job.Type, domain.ErrIdempotencyMismatch)
	}
	if created {
		s.log.InfoContext(ctx, "job enqueued", "job_id", job.ID, "type", job.Type, "run_at", job.RunAt)
	}
	return job, created, nil
}

// Get returns a job by ID.
func (s *Service) Get(ctx context.Context, id int64) (domain.Job, error) {
	return s.repo.Get(ctx, id)
}

// Page is one page of a job listing. NextAfterID is zero on the last page.
type Page struct {
	Jobs        []domain.Job
	NextAfterID int64
}

// List returns a page of jobs, newest first.
func (s *Service) List(ctx context.Context, f domain.ListFilter) (Page, error) {
	if f.State != "" && !f.State.Valid() {
		return Page{}, &domain.ValidationError{Field: "state", Reason: "is not a known state"}
	}
	switch {
	case f.Limit == 0:
		f.Limit = DefaultPageSize
	case f.Limit < 0 || f.Limit > MaxPageSize:
		return Page{}, &domain.ValidationError{Field: "limit", Reason: fmt.Sprintf("must be between 1 and %d", MaxPageSize)}
	}

	pageSize := f.Limit
	f.Limit++ // one extra row tells us whether another page exists
	jobs, err := s.repo.List(ctx, f)
	if err != nil {
		return Page{}, err
	}
	page := Page{Jobs: jobs}
	if len(jobs) > pageSize {
		page.Jobs = jobs[:pageSize]
		page.NextAfterID = page.Jobs[pageSize-1].ID
	}
	return page, nil
}

// Retry puts a dead or cancelled job back in the queue with a fresh attempt
// budget. Workers retry failed jobs on their own; this is the operator path.
func (s *Service) Retry(ctx context.Context, id int64) (domain.Job, error) {
	job, err := s.repo.Get(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	// running -> queued is a legal transition, but only for the worker that
	// owns the lease; an operator must not yank a job out from under it.
	if job.State == domain.StateRunning {
		return domain.Job{}, &domain.TransitionError{From: job.State, To: domain.StateQueued}
	}
	if err := domain.CheckTransition(job.State, domain.StateQueued); err != nil {
		return domain.Job{}, err
	}
	job, err = s.repo.Requeue(ctx, id, job.State)
	if err != nil {
		return domain.Job{}, err
	}
	s.log.InfoContext(ctx, "job requeued by operator", "job_id", id)
	return job, nil
}

// Cancel cancels a job that has not started yet.
func (s *Service) Cancel(ctx context.Context, id int64) (domain.Job, error) {
	job, err := s.repo.Get(ctx, id)
	if err != nil {
		return domain.Job{}, err
	}
	if err := domain.CheckTransition(job.State, domain.StateCancelled); err != nil {
		return domain.Job{}, err
	}
	// The repository re-checks the state in the UPDATE, so a worker claiming
	// the job between our read and write yields ErrConflict, not a cancelled
	// job that is also running.
	job, err = s.repo.Cancel(ctx, id, job.State)
	if err != nil {
		return domain.Job{}, err
	}
	s.log.InfoContext(ctx, "job cancelled", "job_id", id)
	return job, nil
}

// Stats returns queue statistics.
func (s *Service) Stats(ctx context.Context) (domain.Stats, error) {
	return s.repo.Stats(ctx)
}

// Claim leases up to limit runnable jobs of the given types to workerID.
func (s *Service) Claim(ctx context.Context, workerID string, types []string, limit int) ([]domain.Job, error) {
	if limit <= 0 || len(types) == 0 {
		return nil, nil
	}
	return s.repo.Claim(ctx, workerID, types, limit, s.cfg.Lease)
}

// Heartbeat extends the lease of a running job.
func (s *Service) Heartbeat(ctx context.Context, l domain.Lease) error {
	return s.repo.ExtendLease(ctx, l, s.cfg.Lease)
}

// LeaseDuration is the lease granted on claim and on every heartbeat.
func (s *Service) LeaseDuration() time.Duration { return s.cfg.Lease }

// Complete records a successful run.
func (s *Service) Complete(ctx context.Context, job domain.Job) error {
	return s.repo.Complete(ctx, job.Lease())
}

// Fail records a failed run. The job is retried with backoff while it has
// attempts left and the error is not permanent; otherwise it goes to the
// dead-letter state. It returns the state the job ended up in.
func (s *Service) Fail(ctx context.Context, job domain.Job, cause error) (domain.State, error) {
	msg := truncate(cause.Error(), maxErrorLen)

	if domain.IsPermanent(cause) || job.Attempts >= job.MaxAttempts {
		if err := s.repo.Bury(ctx, job.Lease(), msg); err != nil {
			return "", err
		}
		return domain.StateDead, nil
	}

	delay := s.cfg.Backoff.Delay(job.Attempts)
	if err := s.repo.Reschedule(ctx, job.Lease(), delay, msg); err != nil {
		return "", err
	}
	return domain.StateQueued, nil
}

// ReclaimExpired requeues jobs whose workers stopped heartbeating.
func (s *Service) ReclaimExpired(ctx context.Context) (int64, error) {
	var total int64
	for {
		n, err := s.repo.ReclaimExpired(ctx, reclaimBatch)
		total += n
		if err != nil {
			return total, err
		}
		if n < reclaimBatch {
			return total, nil
		}
		if err := ctx.Err(); err != nil {
			return total, err
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Back off to a rune boundary so the stored message stays valid UTF-8.
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}
