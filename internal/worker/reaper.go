package worker

import (
	"context"
	"log/slog"
	"time"
)

// Reclaimer returns jobs with expired leases to the queue.
type Reclaimer interface {
	ReclaimExpired(ctx context.Context) (int64, error)
}

// RunReaper reclaims expired leases every interval until ctx is cancelled.
// Every instance runs one; the reclaim query uses SKIP LOCKED and is
// idempotent, so concurrent reapers do not need coordination.
func RunReaper(ctx context.Context, r Reclaimer, interval time.Duration, log *slog.Logger) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		n, err := r.ReclaimExpired(ctx)
		if err != nil && ctx.Err() == nil {
			log.Error("reclaim expired leases", "err", err)
		}
		if n > 0 {
			log.Warn("reclaimed jobs with expired leases", "count", n)
		}
	}
}
