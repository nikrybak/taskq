// Package worker runs jobs: a pool that claims and executes them with
// heartbeats, a reaper for expired leases, and a scheduler for recurring
// jobs.
package worker

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

// Handler executes one job. Implementations must return promptly once ctx is
// done: it is cancelled on timeout, on shutdown after the drain period, and
// when the worker loses the job's lease. Wrap an error with domain.Permanent
// to skip the remaining retries.
type Handler interface {
	Handle(ctx context.Context, job domain.Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, job domain.Job) error

// Handle calls f.
func (f HandlerFunc) Handle(ctx context.Context, job domain.Job) error { return f(ctx, job) }

type registration struct {
	handler Handler
	timeout time.Duration
}

// Registry maps job types to handlers. It is populated during start-up and
// read-only afterwards, so it needs no locking.
type Registry struct {
	handlers map[string]registration
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]registration)}
}

// Register adds a handler for jobType. A positive timeout bounds each run.
// Registering the same type twice is a programming error and panics.
func (r *Registry) Register(jobType string, h Handler, timeout time.Duration) {
	if _, dup := r.handlers[jobType]; dup {
		panic(fmt.Sprintf("worker: handler for %q registered twice", jobType))
	}
	r.handlers[jobType] = registration{handler: h, timeout: timeout}
}

// Types returns the registered job types in sorted order. A pool only claims
// these, so instances with different handler sets can share one queue.
func (r *Registry) Types() []string {
	types := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	sort.Strings(types)
	return types
}

func (r *Registry) lookup(jobType string) (registration, bool) {
	reg, ok := r.handlers[jobType]
	return reg, ok
}
