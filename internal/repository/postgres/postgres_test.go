package postgres_test

// Integration tests against a real PostgreSQL. They run only when
// TEST_DATABASE_URL is set and they truncate the jobs table, so point it at
// a throwaway database.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nikrybak/taskq/internal/backoff"
	"github.com/nikrybak/taskq/internal/domain"
	"github.com/nikrybak/taskq/internal/repository/postgres"
	"github.com/nikrybak/taskq/internal/service"
	"github.com/nikrybak/taskq/internal/worker"
	"github.com/nikrybak/taskq/migrations"
)

var (
	testPool *pgxpool.Pool
	discard  = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "connect:", err)
		os.Exit(1)
	}
	if err := postgres.Migrate(ctx, pool, migrations.FS, discard); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
	testPool = pool
	code := m.Run()
	pool.Close()
	os.Exit(code)
}

func setup(t *testing.T) *postgres.Repository {
	t.Helper()
	if testPool == nil {
		t.Skip("TEST_DATABASE_URL not set")
	}
	if _, err := testPool.Exec(context.Background(), "TRUNCATE jobs RESTART IDENTITY"); err != nil {
		t.Fatal(err)
	}
	return postgres.New(testPool)
}

func newService(repo *postgres.Repository, lease time.Duration) *service.Service {
	return service.New(repo, service.Config{
		Lease:   lease,
		Backoff: backoff.Policy{Base: time.Hour, Max: time.Hour},
	}, discard)
}

func TestMigrateIsIdempotent(t *testing.T) {
	setup(t)
	if err := postgres.Migrate(context.Background(), testPool, migrations.FS, discard); err != nil {
		t.Fatalf("second migration run: %v", err)
	}
}

func TestStateCheckConstraint(t *testing.T) {
	setup(t)
	_, err := testPool.Exec(context.Background(),
		"INSERT INTO jobs (type, max_attempts, state) VALUES ('t', 1, 'exploded')")
	if err == nil || !strings.Contains(err.Error(), "jobs_state_check") {
		t.Fatalf("expected CHECK violation, got %v", err)
	}
}

func TestConcurrentWorkersNeverDoubleProcess(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, 30*time.Second)

	const total = 300
	for i := range total {
		if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "count", Priority: i % 7}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu        sync.Mutex
		runs      = make(map[int64]int, total)
		completed atomic.Int64
	)
	reg := worker.NewRegistry()
	reg.Register("count", worker.HandlerFunc(func(_ context.Context, job domain.Job) error {
		mu.Lock()
		runs[job.ID]++
		mu.Unlock()
		completed.Add(1)
		return nil
	}), 0)

	// Several pools stand in for several taskq instances sharing the database.
	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for i := range 4 {
		p := worker.NewPool(svc, reg, worker.PoolConfig{
			WorkerID:          fmt.Sprintf("instance-%d", i),
			Concurrency:       5,
			PollInterval:      10 * time.Millisecond,
			HeartbeatInterval: time.Second,
			DrainTimeout:      5 * time.Second,
		}, discard)
		wg.Go(func() { _ = p.Run(runCtx) })
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		st, err := svc.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if st.Counts[domain.StateSucceeded] == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %v", st.Counts)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	if len(runs) != total || completed.Load() != total {
		t.Fatalf("ran %d distinct jobs in %d runs, want %d", len(runs), completed.Load(), total)
	}
	for id, n := range runs {
		if n != 1 {
			t.Fatalf("job %d ran %d times", id, n)
		}
	}
}

func TestLeaseExpiryReclaimsJob(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, 50*time.Millisecond)

	job, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "t", MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.Claim(ctx, "crashed-worker", []string{"t"}, 1)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim: %v %v", first, err)
	}
	// Leased jobs are invisible to other claimers.
	if again, _ := svc.Claim(ctx, "other", []string{"t"}, 1); len(again) != 0 {
		t.Fatal("a leased job was claimed twice")
	}

	time.Sleep(150 * time.Millisecond)
	n, err := svc.ReclaimExpired(ctx)
	if err != nil || n != 1 {
		t.Fatalf("reclaimed %d, err %v", n, err)
	}
	got, _ := svc.Get(ctx, job.ID)
	if got.State != domain.StateQueued || !strings.Contains(got.LastError, "lease expired") || got.LockedBy != "" {
		t.Fatalf("after reclaim: %+v", got)
	}

	second, err := svc.Claim(ctx, "healthy-worker", []string{"t"}, 1)
	if err != nil || len(second) != 1 || second[0].Attempts != 2 {
		t.Fatalf("reclaim then claim: %+v %v", second, err)
	}
	if err := svc.Complete(ctx, first[0]); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale worker completing: want ErrLeaseLost, got %v", err)
	}
	if err := svc.Heartbeat(ctx, first[0].Lease()); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale worker heartbeat: want ErrLeaseLost, got %v", err)
	}
	if err := svc.Complete(ctx, second[0]); err != nil {
		t.Fatalf("new owner completing: %v", err)
	}
}

func TestLeaseExpiryOnLastAttemptBuries(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, 10*time.Millisecond)

	job, _, _ := svc.Enqueue(ctx, domain.NewJob{Type: "t", MaxAttempts: 1})
	if _, err := svc.Claim(ctx, "w", []string{"t"}, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if _, err := svc.ReclaimExpired(ctx); err != nil {
		t.Fatal(err)
	}
	got, _ := svc.Get(ctx, job.ID)
	if got.State != domain.StateDead || got.FinishedAt == nil {
		t.Fatalf("want dead job, got %+v", got)
	}
}

func TestHeartbeatKeepsLease(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, 200*time.Millisecond)

	_, _, _ = svc.Enqueue(ctx, domain.NewJob{Type: "t"})
	jobs, _ := svc.Claim(ctx, "w", []string{"t"}, 1)
	for range 4 {
		time.Sleep(100 * time.Millisecond)
		if err := svc.Heartbeat(ctx, jobs[0].Lease()); err != nil {
			t.Fatal(err)
		}
		if n, _ := svc.ReclaimExpired(ctx); n != 0 {
			t.Fatal("job with a live heartbeat was reclaimed")
		}
	}
}

func TestIdempotencyKeyUnderConcurrency(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, time.Minute)

	const callers = 16
	var (
		wg      sync.WaitGroup
		created atomic.Int64
		ids     sync.Map
		errs    = make(chan error, callers)
	)
	for range callers {
		wg.Go(func() {
			job, isNew, err := svc.Enqueue(ctx, domain.NewJob{Type: "email", IdempotencyKey: "order-42"})
			if err != nil {
				errs <- err
				return
			}
			if isNew {
				created.Add(1)
			}
			ids.Store(job.ID, true)
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	distinct := 0
	ids.Range(func(any, any) bool { distinct++; return true })
	if created.Load() != 1 || distinct != 1 {
		t.Fatalf("created=%d distinct ids=%d, want 1 and 1", created.Load(), distinct)
	}
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "sms", IdempotencyKey: "order-42"}); !errors.Is(err, domain.ErrIdempotencyMismatch) {
		t.Fatalf("reuse for another type: want ErrIdempotencyMismatch, got %v", err)
	}
}

func TestRetryBackoffDelaysNextClaim(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, time.Minute)

	job, _, _ := svc.Enqueue(ctx, domain.NewJob{Type: "t", MaxAttempts: 3})
	claimed, _ := svc.Claim(ctx, "w", []string{"t"}, 1)
	state, err := svc.Fail(ctx, claimed[0], errors.New("downstream unavailable"))
	if err != nil || state != domain.StateQueued {
		t.Fatalf("state=%s err=%v", state, err)
	}
	got, _ := svc.Get(ctx, job.ID)
	if time.Until(got.RunAt) < 25*time.Minute || got.LastError != "downstream unavailable" {
		t.Fatalf("expected run_at pushed out by backoff: %+v", got)
	}
	if again, _ := svc.Claim(ctx, "w", []string{"t"}, 1); len(again) != 0 {
		t.Fatal("job claimed before its backoff elapsed")
	}

	// Only dead or cancelled jobs can be retried by an operator.
	if _, err := svc.Retry(ctx, job.ID); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("retrying a queued job: want ErrInvalidTransition, got %v", err)
	}
}

func TestCancelAndListPagination(t *testing.T) {
	repo := setup(t)
	ctx := context.Background()
	svc := newService(repo, time.Minute)

	for i := range 5 {
		typ := "a"
		if i%2 == 1 {
			typ = "b"
		}
		if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: typ}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.Cancel(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Cancel(ctx, 1); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("double cancel: %v", err)
	}
	retried, err := svc.Retry(ctx, 1)
	if err != nil || retried.State != domain.StateQueued {
		t.Fatalf("retry cancelled job: %+v %v", retried, err)
	}

	page, err := svc.List(ctx, domain.ListFilter{Type: "a", Limit: 2})
	if err != nil || len(page.Jobs) != 2 || page.Jobs[0].ID != 5 || page.NextAfterID != 3 {
		t.Fatalf("first page: %+v %v", page, err)
	}
	page, err = svc.List(ctx, domain.ListFilter{Type: "a", Limit: 2, AfterID: page.NextAfterID})
	if err != nil || len(page.Jobs) != 1 || page.Jobs[0].ID != 1 || page.NextAfterID != 0 {
		t.Fatalf("second page: %+v %v", page, err)
	}

	st, err := svc.Stats(ctx)
	if err != nil || st.Counts[domain.StateQueued] != 5 {
		t.Fatalf("stats: %+v %v", st, err)
	}
}
