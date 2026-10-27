package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nikrybak/taskq/internal/cron"
	"github.com/nikrybak/taskq/internal/domain"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeQueue hands out a fixed set of jobs and records outcomes.
type fakeQueue struct {
	mu        sync.Mutex
	pending   []domain.Job
	completed []int64
	failed    map[int64]error
	heartbeat func(domain.Lease) error
}

func newFakeQueue(jobs ...domain.Job) *fakeQueue {
	return &fakeQueue{pending: jobs, failed: map[int64]error{}}
}

func (q *fakeQueue) Claim(_ context.Context, workerID string, _ []string, limit int) ([]domain.Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := min(limit, len(q.pending))
	out := make([]domain.Job, n)
	copy(out, q.pending[:n])
	q.pending = q.pending[n:]
	for i := range out {
		out[i].State, out[i].LockedBy = domain.StateRunning, workerID
		out[i].Attempts++
	}
	return out, nil
}

func (q *fakeQueue) Heartbeat(_ context.Context, l domain.Lease) error {
	if q.heartbeat != nil {
		return q.heartbeat(l)
	}
	return nil
}

func (q *fakeQueue) Complete(_ context.Context, job domain.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, job.ID)
	return nil
}

func (q *fakeQueue) Fail(_ context.Context, job domain.Job, cause error) (domain.State, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failed[job.ID] = cause
	return domain.StateQueued, nil
}

func (q *fakeQueue) outcomes() (completed int, failed map[int64]error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f := make(map[int64]error, len(q.failed))
	for k, v := range q.failed {
		f[k] = v
	}
	return len(q.completed), f
}

func testPool(q Queue, reg *Registry, drain time.Duration) *Pool {
	return NewPool(q, reg, PoolConfig{
		WorkerID:          "test",
		Concurrency:       4,
		PollInterval:      5 * time.Millisecond,
		HeartbeatInterval: 5 * time.Millisecond,
		DrainTimeout:      drain,
	}, discard)
}

func runPool(ctx context.Context, p *Pool) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPoolRunsJobsAndRecordsOutcomes(t *testing.T) {
	q := newFakeQueue(
		domain.Job{ID: 1, Type: "ok"}, domain.Job{ID: 2, Type: "ok"},
		domain.Job{ID: 3, Type: "fail"}, domain.Job{ID: 4, Type: "panic"},
	)
	reg := NewRegistry()
	reg.Register("ok", HandlerFunc(func(context.Context, domain.Job) error { return nil }), 0)
	reg.Register("fail", HandlerFunc(func(context.Context, domain.Job) error { return errors.New("boom") }), 0)
	reg.Register("panic", HandlerFunc(func(context.Context, domain.Job) error { panic("kaboom") }), 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, testPool(q, reg, time.Second))
	waitFor(t, func() bool { c, f := q.outcomes(); return c+len(f) == 4 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	completed, failed := q.outcomes()
	if completed != 2 {
		t.Fatalf("completed %d jobs, want 2", completed)
	}
	if failed[3] == nil || failed[3].Error() != "boom" {
		t.Fatalf("job 3 failure = %v", failed[3])
	}
	if failed[4] == nil || !strings.Contains(failed[4].Error(), "handler panic: kaboom") {
		t.Fatalf("job 4 failure = %v", failed[4])
	}
}

func TestPoolTimeoutCancelsHandler(t *testing.T) {
	q := newFakeQueue(domain.Job{ID: 1, Type: "slow"})
	reg := NewRegistry()
	reg.Register("slow", HandlerFunc(func(ctx context.Context, _ domain.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}), 20*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, testPool(q, reg, time.Second))
	waitFor(t, func() bool { _, f := q.outcomes(); return len(f) == 1 })
	cancel()
	<-done

	if _, f := q.outcomes(); !errors.Is(f[1], context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", f[1])
	}
}

func TestPoolShutdownWaitsForRunningJobs(t *testing.T) {
	q := newFakeQueue(domain.Job{ID: 1, Type: "block"})
	started, release := make(chan struct{}), make(chan struct{})
	reg := NewRegistry()
	reg.Register("block", HandlerFunc(func(ctx context.Context, _ domain.Job) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, testPool(q, reg, 5*time.Second))
	<-started
	cancel()

	select {
	case <-done:
		t.Fatal("Run returned while a job was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-done
	if c, f := q.outcomes(); c != 1 || len(f) != 0 {
		t.Fatalf("completed=%d failed=%v; the drained job must be recorded as success", c, f)
	}
}

func TestPoolDrainTimeoutCancelsJobs(t *testing.T) {
	q := newFakeQueue(domain.Job{ID: 1, Type: "stuck"})
	started := make(chan struct{})
	reg := NewRegistry()
	reg.Register("stuck", HandlerFunc(func(ctx context.Context, _ domain.Job) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}), 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, testPool(q, reg, 20*time.Millisecond))
	<-started
	cancel()
	<-done
	if _, f := q.outcomes(); !errors.Is(f[1], context.Canceled) {
		t.Fatalf("want Canceled, got %v", f[1])
	}
}

func TestPoolLeaseLostCancelsAndDiscardsResult(t *testing.T) {
	q := newFakeQueue(domain.Job{ID: 1, Type: "long"})
	q.heartbeat = func(domain.Lease) error { return domain.ErrLeaseLost }
	stopped := make(chan struct{})
	reg := NewRegistry()
	reg.Register("long", HandlerFunc(func(ctx context.Context, _ domain.Job) error {
		<-ctx.Done()
		close(stopped)
		return ctx.Err()
	}), 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, testPool(q, reg, time.Second))
	<-stopped
	cancel()
	<-done
	if c, f := q.outcomes(); c != 0 || len(f) != 0 {
		t.Fatalf("a job whose lease was lost must not record an outcome: completed=%d failed=%v", c, f)
	}
}

func TestRegistry(t *testing.T) {
	reg := NewRegistry()
	noop := HandlerFunc(func(context.Context, domain.Job) error { return nil })
	reg.Register("b", noop, 0)
	reg.Register("a", noop, 0)
	if got := reg.Types(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("Types() = %v", got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("duplicate registration must panic")
		}
	}()
	reg.Register("a", noop, 0)
}

// dedupEnqueuer behaves like the database: one job per idempotency key.
type dedupEnqueuer struct {
	mu   sync.Mutex
	jobs map[string]domain.NewJob
}

func (e *dedupEnqueuer) Enqueue(_ context.Context, n domain.NewJob) (domain.Job, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.jobs[n.IdempotencyKey]; ok {
		return domain.Job{}, false, nil
	}
	e.jobs[n.IdempotencyKey] = n
	return domain.Job{}, true, nil
}

func TestSchedulersOnSeveralInstancesEnqueueEachTickOnce(t *testing.T) {
	enq := &dedupEnqueuer{jobs: map[string]domain.NewJob{}}
	start := time.Date(2026, 3, 10, 12, 0, 30, 0, time.UTC)
	schedules := []Schedule{{Name: "report", Cron: cron.MustParse("*/5 * * * *"), Job: domain.NewJob{Type: "report"}}}

	instances := []*Scheduler{
		NewScheduler(enq, schedules, discard),
		NewScheduler(enq, schedules, discard),
		NewScheduler(enq, schedules, discard),
	}
	for _, s := range instances {
		s.init(start)
	}
	// Each instance observes the clock at slightly different moments.
	for minute := 1; minute <= 15; minute++ {
		for i, s := range instances {
			s.fireDue(context.Background(), start.Add(time.Duration(minute)*time.Minute+time.Duration(i)*time.Second))
		}
	}

	if len(enq.jobs) != 3 {
		t.Fatalf("enqueued %d jobs, want 3 (12:05, 12:10, 12:15)", len(enq.jobs))
	}
	for _, m := range []int{5, 10, 15} {
		tick := time.Date(2026, 3, 10, 12, m, 0, 0, time.UTC)
		n, ok := enq.jobs[TickKey("report", tick)]
		if !ok || !n.RunAt.Equal(tick) || n.Type != "report" {
			t.Fatalf("tick %s missing or wrong: %+v", tick, n)
		}
	}
}

func TestSchedulerSkipsMissedTicks(t *testing.T) {
	enq := &dedupEnqueuer{jobs: map[string]domain.NewJob{}}
	start := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	s := NewScheduler(enq, []Schedule{{Name: "m", Cron: cron.MustParse("* * * * *"), Job: domain.NewJob{Type: "m"}}}, discard)
	s.init(start)

	// The process was paused for an hour: only one catch-up job, not sixty.
	s.fireDue(context.Background(), start.Add(time.Hour))
	if len(enq.jobs) != 1 {
		t.Fatalf("enqueued %d jobs after a pause, want 1", len(enq.jobs))
	}
	if want := start.Add(time.Hour + time.Minute); !s.next[0].Equal(want) {
		t.Fatalf("next tick %s, want %s", s.next[0], want)
	}
}
