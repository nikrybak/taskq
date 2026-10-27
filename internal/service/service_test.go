package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nikrybak/taskq/internal/backoff"
	"github.com/nikrybak/taskq/internal/domain"
)

func newTestService(t *testing.T) (*Service, *fakeRepo) {
	t.Helper()
	repo := newFakeRepo()
	svc := New(repo, Config{
		Lease:   30 * time.Second,
		Backoff: backoff.Policy{Base: time.Second, Max: time.Minute, Rand: func() float64 { return 0 }},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return svc, repo
}

func TestEnqueueDefaultsAndValidation(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()

	job, created, err := svc.Enqueue(ctx, domain.NewJob{Type: "email", Payload: json.RawMessage(`{}`)})
	if err != nil || !created {
		t.Fatalf("Enqueue: created=%v err=%v", created, err)
	}
	if job.MaxAttempts != domain.DefaultMaxAttempts || job.State != domain.StateQueued {
		t.Fatalf("defaults not applied: %+v", job)
	}

	var ve *domain.ValidationError
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: ""}); !errors.As(err, &ve) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestEnqueueIdempotency(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	n := domain.NewJob{Type: "email", IdempotencyKey: "order-42"}

	first, created, err := svc.Enqueue(ctx, n)
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	second, created, err := svc.Enqueue(ctx, n)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("second enqueue: id=%d created=%v err=%v", second.ID, created, err)
	}

	n.Type = "sms"
	if _, _, err := svc.Enqueue(ctx, n); !errors.Is(err, domain.ErrIdempotencyMismatch) {
		t.Fatalf("expected ErrIdempotencyMismatch, got %v", err)
	}
}

func TestFailRetriesThenBuries(t *testing.T) {
	svc, repo := newTestService(t)
	ctx := context.Background()
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "flaky", MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}

	wantStates := []domain.State{domain.StateQueued, domain.StateQueued, domain.StateDead}
	wantDelays := []time.Duration{500 * time.Millisecond, time.Second}
	for attempt, want := range wantStates {
		jobs, err := svc.Claim(ctx, "w1", []string{"flaky"}, 1)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("attempt %d: claim returned %d jobs, err=%v", attempt+1, len(jobs), err)
		}
		got, err := svc.Fail(ctx, jobs[0], errors.New("boom"))
		if err != nil {
			t.Fatalf("attempt %d: Fail: %v", attempt+1, err)
		}
		if got != want {
			t.Fatalf("attempt %d: state %s, want %s", attempt+1, got, want)
		}
		if want == domain.StateQueued {
			if repo.lastDelay != wantDelays[attempt] {
				t.Fatalf("attempt %d: delay %v, want %v", attempt+1, repo.lastDelay, wantDelays[attempt])
			}
			repo.advance(repo.lastDelay)
		}
	}

	job, _ := svc.Get(ctx, 1)
	if job.State != domain.StateDead || job.LastError != "boom" || job.Attempts != 3 {
		t.Fatalf("unexpected final job: %+v", job)
	}
}

func TestFailPermanentSkipsRetries(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "t", MaxAttempts: 10}); err != nil {
		t.Fatal(err)
	}
	jobs, _ := svc.Claim(ctx, "w1", []string{"t"}, 1)
	state, err := svc.Fail(ctx, jobs[0], domain.Permanent(errors.New("bad payload")))
	if err != nil || state != domain.StateDead {
		t.Fatalf("state=%s err=%v", state, err)
	}
}

func TestFailTruncatesLongErrors(t *testing.T) {
	svc, _ := newTestService(t)
	ctx := context.Background()
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "t", MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	jobs, _ := svc.Claim(ctx, "w1", []string{"t"}, 1)
	if _, err := svc.Fail(ctx, jobs[0], errors.New(strings.Repeat("é", maxErrorLen))); err != nil {
		t.Fatal(err)
	}
	job, _ := svc.Get(ctx, jobs[0].ID)
	if len(job.LastError) > maxErrorLen || !json.Valid([]byte(`"`+job.LastError+`"`)) {
		t.Fatalf("error not truncated to valid UTF-8: len=%d", len(job.LastError))
	}
}

func TestStaleWorkerCannotComplete(t *testing.T) {
	svc, repo := newTestService(t)
	ctx := context.Background()
	if _, _, err := svc.Enqueue(ctx, domain.NewJob{Type: "t"}); err != nil {
		t.Fatal(err)
	}
	stale, _ := svc.Claim(ctx, "w1", []string{"t"}, 1)

	repo.advance(time.Minute)
	if n, err := svc.ReclaimExpired(ctx); err != nil || n != 1 {
		t.Fatalf("reclaimed %d, err=%v", n, err)
	}
	fresh, _ := svc.Claim(ctx, "w2", []string{"t"}, 1)
	if len(fresh) != 1 {
		t.Fatal("reclaimed job was not claimable")
	}

	if err := svc.Complete(ctx, stale[0]); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale complete: want ErrLeaseLost, got %v", err)
	}
	if err := svc.Heartbeat(ctx, stale[0].Lease()); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale heartbeat: want ErrLeaseLost, got %v", err)
	}
	if err := svc.Complete(ctx, fresh[0]); err != nil {
		t.Fatalf("fresh complete: %v", err)
	}
}

func TestRetryAndCancelTransitions(t *testing.T) {
	tests := []struct {
		state      domain.State
		retryErr   error
		cancelErr  error
		afterRetry domain.State
	}{
		{domain.StateQueued, domain.ErrInvalidTransition, nil, ""},
		{domain.StateRunning, domain.ErrInvalidTransition, domain.ErrInvalidTransition, ""},
		{domain.StateSucceeded, domain.ErrInvalidTransition, domain.ErrInvalidTransition, ""},
		{domain.StateDead, nil, domain.ErrInvalidTransition, domain.StateQueued},
		{domain.StateCancelled, nil, domain.ErrInvalidTransition, domain.StateQueued},
	}
	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			svc, repo := newTestService(t)
			ctx := context.Background()
			job := repo.put(domain.Job{Type: "t", State: tt.state, Attempts: 3, MaxAttempts: 3})

			got, err := svc.Retry(ctx, job.ID)
			if !errors.Is(err, tt.retryErr) {
				t.Fatalf("Retry error = %v, want %v", err, tt.retryErr)
			}
			if err == nil && (got.State != tt.afterRetry || got.Attempts != 0) {
				t.Fatalf("Retry result = %+v", got)
			}

			job = repo.put(domain.Job{Type: "t", State: tt.state, MaxAttempts: 3})
			if _, err := svc.Cancel(ctx, job.ID); !errors.Is(err, tt.cancelErr) {
				t.Fatalf("Cancel error = %v, want %v", err, tt.cancelErr)
			}
		})
	}
}

func TestCancelMissingJob(t *testing.T) {
	svc, _ := newTestService(t)
	if _, err := svc.Cancel(context.Background(), 99); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestListPagination(t *testing.T) {
	svc, repo := newTestService(t)
	ctx := context.Background()
	for range 5 {
		repo.put(domain.Job{Type: "t", State: domain.StateQueued})
	}

	var seen []int64
	var after int64
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination did not terminate")
		}
		page, err := svc.List(ctx, domain.ListFilter{Limit: 2, AfterID: after})
		if err != nil {
			t.Fatal(err)
		}
		for _, j := range page.Jobs {
			seen = append(seen, j.ID)
		}
		if page.NextAfterID == 0 {
			break
		}
		after = page.NextAfterID
	}
	want := []int64{5, 4, 3, 2, 1}
	if len(seen) != len(want) {
		t.Fatalf("seen %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("seen %v, want %v", seen, want)
		}
	}

	var ve *domain.ValidationError
	if _, err := svc.List(ctx, domain.ListFilter{Limit: MaxPageSize + 1}); !errors.As(err, &ve) {
		t.Fatalf("expected limit validation error, got %v", err)
	}
}

func TestClaimRespectsPriorityAndRunAt(t *testing.T) {
	svc, repo := newTestService(t)
	ctx := context.Background()
	low, _, _ := svc.Enqueue(ctx, domain.NewJob{Type: "t", Priority: 0})
	high, _, _ := svc.Enqueue(ctx, domain.NewJob{Type: "t", Priority: 10})
	_, _, _ = svc.Enqueue(ctx, domain.NewJob{Type: "t", Priority: 50, RunAt: repo.now.Add(time.Hour)})
	_, _, _ = svc.Enqueue(ctx, domain.NewJob{Type: "other", Priority: 99})

	jobs, err := svc.Claim(ctx, "w", []string{"t"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].ID != high.ID || jobs[1].ID != low.ID {
		t.Fatalf("unexpected claim order: %+v", jobs)
	}
}
