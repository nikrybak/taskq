package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

// Queue is the part of the job service the pool depends on.
type Queue interface {
	Claim(ctx context.Context, workerID string, types []string, limit int) ([]domain.Job, error)
	Heartbeat(ctx context.Context, l domain.Lease) error
	Complete(ctx context.Context, job domain.Job) error
	Fail(ctx context.Context, job domain.Job, cause error) (domain.State, error)
}

// PoolConfig tunes a Pool.
type PoolConfig struct {
	// WorkerID identifies this process in leases; it must be unique per
	// running instance.
	WorkerID    string
	Concurrency int
	// PollInterval is how long to wait before claiming again when the queue
	// is empty.
	PollInterval time.Duration
	// HeartbeatInterval must be comfortably shorter than the lease so a
	// single slow heartbeat does not lose the job.
	HeartbeatInterval time.Duration
	// DrainTimeout is how long shutdown waits for running jobs before
	// cancelling their contexts.
	DrainTimeout time.Duration
}

// resultTimeout bounds the write that records a job outcome. It is detached
// from shutdown so a finished job is still recorded while the process stops.
const resultTimeout = 10 * time.Second

// Pool claims jobs and runs them concurrently.
type Pool struct {
	queue Queue
	reg   *Registry
	cfg   PoolConfig
	log   *slog.Logger
}

// NewPool returns a Pool.
func NewPool(q Queue, reg *Registry, cfg PoolConfig, log *slog.Logger) *Pool {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	return &Pool{queue: q, reg: reg, cfg: cfg, log: log.With("worker_id", cfg.WorkerID)}
}

// Run claims and executes jobs until ctx is cancelled. It then stops
// claiming, waits up to DrainTimeout for running jobs, cancels whatever is
// still running and returns once every job goroutine has exited.
func (p *Pool) Run(ctx context.Context) error {
	types := p.reg.Types()
	if len(types) == 0 {
		return errors.New("worker: no handlers registered")
	}

	// Jobs run under a context that survives ctx so that shutdown can drain
	// them instead of aborting them mid-flight.
	jobsCtx, cancelJobs := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelJobs()

	slots := make(chan struct{}, p.cfg.Concurrency)
	var wg sync.WaitGroup

	p.log.Info("worker pool started", "concurrency", p.cfg.Concurrency, "types", types)
	for ctx.Err() == nil {
		// Block until at least one slot is free, then grab every other free
		// slot so one query can claim a batch.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			continue
		}
		free := 1
		for free < p.cfg.Concurrency && tryAcquire(slots) {
			free++
		}

		jobs, err := p.queue.Claim(ctx, p.cfg.WorkerID, types, free)
		for range free - len(jobs) {
			<-slots
		}
		if err != nil && ctx.Err() == nil {
			p.log.Error("claim jobs", "err", err)
		}

		for _, job := range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				p.execute(jobsCtx, job)
			}()
		}

		// A full batch suggests more work is waiting; otherwise back off.
		if len(jobs) < free {
			sleep(ctx, p.cfg.PollInterval)
		}
	}

	p.log.Info("worker pool draining", "running", len(slots))
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(p.cfg.DrainTimeout):
		p.log.Warn("drain timeout exceeded, cancelling running jobs", "running", len(slots))
		cancelJobs()
		<-done
	}
	p.log.Info("worker pool stopped")
	return nil
}

func (p *Pool) execute(ctx context.Context, job domain.Job) {
	log := p.log.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempts)
	reg, ok := p.reg.lookup(job.Type)
	if !ok {
		// Unreachable while Claim filters by registered types, but a missing
		// handler must never be silently treated as success.
		reg.handler = HandlerFunc(func(context.Context, domain.Job) error {
			return domain.Permanent(fmt.Errorf("no handler registered for type %q", job.Type))
		})
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if reg.timeout > 0 {
		var cancelTimeout context.CancelFunc
		runCtx, cancelTimeout = context.WithTimeout(runCtx, reg.timeout)
		defer cancelTimeout()
	}

	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		p.heartbeat(ctx, job.Lease(), stopHeartbeat, cancel)
	}()

	start := time.Now()
	err := invoke(runCtx, reg.handler, job)
	close(stopHeartbeat)
	<-heartbeatDone

	if errors.Is(context.Cause(runCtx), domain.ErrLeaseLost) {
		// Another worker owns the job now; whatever we write would be fenced
		// off anyway, and the new owner's result is the one that counts.
		log.Warn("lease lost while running, discarding result", "elapsed", time.Since(start))
		return
	}

	resCtx, cancelRes := context.WithTimeout(context.WithoutCancel(ctx), resultTimeout)
	defer cancelRes()
	if err == nil {
		if cerr := p.queue.Complete(resCtx, job); cerr != nil {
			log.Error("record success", "err", cerr)
			return
		}
		log.Info("job succeeded", "elapsed", time.Since(start))
		return
	}

	state, ferr := p.queue.Fail(resCtx, job, err)
	if ferr != nil {
		log.Error("record failure", "err", ferr, "job_err", err)
		return
	}
	log.Warn("job failed", "err", err, "next_state", state, "elapsed", time.Since(start))
}

// heartbeat extends the lease until stop is closed. If the lease turns out
// to be lost it cancels the run so the handler stops wasting work.
func (p *Pool) heartbeat(ctx context.Context, lease domain.Lease, stop <-chan struct{}, cancel context.CancelCauseFunc) {
	if p.cfg.HeartbeatInterval <= 0 {
		return
	}
	t := time.NewTicker(p.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-t.C:
		}
		err := p.queue.Heartbeat(ctx, lease)
		switch {
		case err == nil:
		case errors.Is(err, domain.ErrLeaseLost):
			cancel(domain.ErrLeaseLost)
			return
		default:
			// Transient: the current lease is still valid until it expires,
			// and the next tick tries again.
			p.log.Warn("heartbeat failed", "job_id", lease.JobID, "err", err)
		}
	}
}

func invoke(ctx context.Context, h Handler, job domain.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h.Handle(ctx, job)
}

func tryAcquire(slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
