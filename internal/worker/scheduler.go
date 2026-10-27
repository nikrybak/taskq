package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nikrybak/taskq/internal/cron"
	"github.com/nikrybak/taskq/internal/domain"
)

// Schedule is a recurring job definition.
type Schedule struct {
	// Name identifies the schedule and is part of every tick's idempotency
	// key, so it must be stable across deploys.
	Name string
	Cron *cron.Schedule
	// Job is the template for each enqueued job. RunAt and IdempotencyKey
	// are set per tick.
	Job domain.NewJob
}

// Enqueuer stores new jobs.
type Enqueuer interface {
	Enqueue(ctx context.Context, n domain.NewJob) (domain.Job, bool, error)
}

// Scheduler enqueues recurring jobs. Every instance runs one. Tick times are
// a pure function of the cron expression, so all instances compute the same
// ticks, and the per-tick idempotency key turns the duplicate inserts from
// the other instances into no-ops. This needs no leader election and keeps
// working when any instance dies.
type Scheduler struct {
	enq       Enqueuer
	schedules []Schedule
	next      []time.Time
	now       func() time.Time
	log       *slog.Logger
}

// NewScheduler returns a Scheduler for the given schedules.
func NewScheduler(enq Enqueuer, schedules []Schedule, log *slog.Logger) *Scheduler {
	return &Scheduler{
		enq:       enq,
		schedules: schedules,
		now:       func() time.Time { return time.Now().UTC() },
		log:       log,
	}
}

// TickKey is the idempotency key of one schedule tick.
func TickKey(name string, tick time.Time) string {
	return fmt.Sprintf("schedule:%s:%d", name, tick.Unix())
}

// Run enqueues jobs as their ticks come due until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	if len(s.schedules) == 0 {
		return
	}
	s.init(s.now())
	for {
		wait := time.Until(s.earliest())
		t := time.NewTimer(max(wait, 0))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		s.fireDue(ctx, s.now())
	}
}

func (s *Scheduler) init(now time.Time) {
	s.next = make([]time.Time, len(s.schedules))
	for i, sch := range s.schedules {
		s.next[i] = sch.Cron.Next(now)
	}
}

func (s *Scheduler) earliest() time.Time {
	var e time.Time
	for _, t := range s.next {
		if !t.IsZero() && (e.IsZero() || t.Before(e)) {
			e = t
		}
	}
	if e.IsZero() {
		// Every schedule is exhausted; park until shutdown.
		return s.now().Add(24 * time.Hour)
	}
	return e
}

// fireDue enqueues every tick at or before now and advances the schedules.
func (s *Scheduler) fireDue(ctx context.Context, now time.Time) {
	for i, sch := range s.schedules {
		tick := s.next[i]
		if tick.IsZero() || tick.After(now) {
			continue
		}
		s.fire(ctx, sch, tick)

		next := sch.Cron.Next(tick)
		if !next.IsZero() && !next.After(now) {
			// We fell behind (process paused, clock jump). Skip the missed
			// ticks rather than flooding the queue with a backlog.
			s.log.Warn("skipping missed schedule ticks", "schedule", sch.Name, "from", next)
			next = sch.Cron.Next(now)
		}
		s.next[i] = next
	}
}

func (s *Scheduler) fire(ctx context.Context, sch Schedule, tick time.Time) {
	n := sch.Job
	n.RunAt = tick
	n.IdempotencyKey = TickKey(sch.Name, tick)

	job, created, err := s.enq.Enqueue(ctx, n)
	switch {
	case err != nil:
		s.log.Error("enqueue scheduled job", "schedule", sch.Name, "tick", tick, "err", err)
	case created:
		s.log.Info("scheduled job enqueued", "schedule", sch.Name, "tick", tick, "job_id", job.ID)
	default:
		s.log.Debug("tick already enqueued by another instance", "schedule", sch.Name, "tick", tick, "job_id", job.ID)
	}
}
