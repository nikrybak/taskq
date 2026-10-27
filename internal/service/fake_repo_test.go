package service

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

// fakeRepo is an in-memory Repository that mirrors the fencing and
// conditional-update semantics of the PostgreSQL implementation.
type fakeRepo struct {
	mu     sync.Mutex
	now    time.Time
	nextID int64
	jobs   map[int64]*domain.Job
	keys   map[string]int64

	lastDelay time.Duration
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		now:  time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC),
		jobs: map[int64]*domain.Job{},
		keys: map[string]int64{},
	}
}

func (r *fakeRepo) put(j domain.Job) domain.Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextID++
	j.ID = r.nextID
	r.jobs[j.ID] = &j
	return j
}

func (r *fakeRepo) Create(_ context.Context, n domain.NewJob) (domain.Job, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n.IdempotencyKey != "" {
		if id, ok := r.keys[n.IdempotencyKey]; ok {
			return *r.jobs[id], false, nil
		}
	}
	r.nextID++
	runAt := n.RunAt
	if runAt.IsZero() {
		runAt = r.now
	}
	j := &domain.Job{
		ID: r.nextID, Type: n.Type, Payload: n.Payload, Priority: n.Priority, State: domain.StateQueued,
		MaxAttempts: n.MaxAttempts, RunAt: runAt, IdempotencyKey: n.IdempotencyKey, CreatedAt: r.now, UpdatedAt: r.now,
	}
	r.jobs[j.ID] = j
	if n.IdempotencyKey != "" {
		r.keys[n.IdempotencyKey] = j.ID
	}
	return *j, true, nil
}

func (r *fakeRepo) Get(_ context.Context, id int64) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok {
		return domain.Job{}, domain.ErrNotFound
	}
	return *j, nil
}

func (r *fakeRepo) List(_ context.Context, f domain.ListFilter) ([]domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.Job
	for _, j := range r.jobs {
		if (f.State == "" || j.State == f.State) && (f.Type == "" || j.Type == f.Type) && (f.AfterID == 0 || j.ID < f.AfterID) {
			out = append(out, *j)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].ID > out[b].ID })
	if len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (r *fakeRepo) Claim(_ context.Context, workerID string, types []string, limit int, lease time.Duration) ([]domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ready []*domain.Job
	for _, j := range r.jobs {
		if j.State == domain.StateQueued && !j.RunAt.After(r.now) && contains(types, j.Type) {
			ready = append(ready, j)
		}
	}
	sort.Slice(ready, func(a, b int) bool {
		if ready[a].Priority != ready[b].Priority {
			return ready[a].Priority > ready[b].Priority
		}
		return ready[a].ID < ready[b].ID
	})
	var out []domain.Job
	for _, j := range ready {
		if len(out) == limit {
			break
		}
		exp := r.now.Add(lease)
		j.State, j.Attempts, j.LockedBy, j.LeaseExpiresAt = domain.StateRunning, j.Attempts+1, workerID, &exp
		out = append(out, *j)
	}
	return out, nil
}

func (r *fakeRepo) fenced(l domain.Lease, apply func(*domain.Job)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[l.JobID]
	if !ok || j.State != domain.StateRunning || j.LockedBy != l.WorkerID || j.Attempts != l.Attempt {
		return domain.ErrLeaseLost
	}
	apply(j)
	return nil
}

func (r *fakeRepo) ExtendLease(_ context.Context, l domain.Lease, lease time.Duration) error {
	return r.fenced(l, func(j *domain.Job) { exp := r.now.Add(lease); j.LeaseExpiresAt = &exp })
}

func (r *fakeRepo) Complete(_ context.Context, l domain.Lease) error {
	return r.fenced(l, func(j *domain.Job) {
		j.State, j.LockedBy, j.LeaseExpiresAt = domain.StateSucceeded, "", nil
	})
}

func (r *fakeRepo) Reschedule(_ context.Context, l domain.Lease, delay time.Duration, lastErr string) error {
	return r.fenced(l, func(j *domain.Job) {
		r.lastDelay = delay
		j.State, j.LockedBy, j.LeaseExpiresAt, j.LastError, j.RunAt = domain.StateQueued, "", nil, lastErr, r.now.Add(delay)
	})
}

func (r *fakeRepo) Bury(_ context.Context, l domain.Lease, lastErr string) error {
	return r.fenced(l, func(j *domain.Job) {
		j.State, j.LockedBy, j.LeaseExpiresAt, j.LastError = domain.StateDead, "", nil, lastErr
	})
}

func (r *fakeRepo) conditional(id int64, from domain.State, apply func(*domain.Job)) (domain.Job, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	j, ok := r.jobs[id]
	if !ok || j.State != from {
		return domain.Job{}, domain.ErrConflict
	}
	apply(j)
	return *j, nil
}

func (r *fakeRepo) Requeue(_ context.Context, id int64, from domain.State) (domain.Job, error) {
	return r.conditional(id, from, func(j *domain.Job) { j.State, j.Attempts, j.RunAt = domain.StateQueued, 0, r.now })
}

func (r *fakeRepo) Cancel(_ context.Context, id int64, from domain.State) (domain.Job, error) {
	return r.conditional(id, from, func(j *domain.Job) { j.State = domain.StateCancelled })
}

func (r *fakeRepo) ReclaimExpired(_ context.Context, batch int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int64
	for _, j := range r.jobs {
		if int(n) == batch {
			break
		}
		if j.State == domain.StateRunning && j.LeaseExpiresAt.Before(r.now) {
			j.State, j.LockedBy, j.LeaseExpiresAt = domain.StateQueued, "", nil
			if j.Attempts >= j.MaxAttempts {
				j.State = domain.StateDead
			}
			n++
		}
	}
	return n, nil
}

func (r *fakeRepo) Stats(context.Context) (domain.Stats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := domain.Stats{Counts: map[domain.State]int64{}}
	for _, j := range r.jobs {
		st.Counts[j.State]++
	}
	return st, nil
}

func (r *fakeRepo) advance(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.now = r.now.Add(d)
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
