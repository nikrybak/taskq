// Package backoff computes retry delays.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy is exponential backoff with "equal jitter": the delay for attempt n
// is d/2 + rand[0, d/2) where d = min(Max, Base * 2^(n-1)). Keeping half of
// the delay fixed guarantees a minimum spacing between retries, while the
// random half spreads out jobs that failed together (for example, when a
// downstream dependency went away) so they do not retry in lockstep.
type Policy struct {
	Base time.Duration
	Max  time.Duration
	// Rand returns a value in [0, 1). It defaults to math/rand/v2 and is
	// replaceable for deterministic tests.
	Rand func() float64
}

// Default is a reasonable policy for network-bound jobs.
var Default = Policy{Base: 2 * time.Second, Max: 10 * time.Minute}

// Delay returns the wait before the given attempt number. Attempt counts
// from 1, the first retry after the initial failure.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if p.Base <= 0 {
		return 0
	}
	maxDelay := p.Max
	if maxDelay < p.Base {
		maxDelay = p.Base
	}

	d := p.Base
	for i := 1; i < attempt; i++ {
		// Compare before doubling so large attempt numbers cannot overflow.
		if d > maxDelay/2 {
			d = maxDelay
			break
		}
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}

	rnd := p.Rand
	if rnd == nil {
		rnd = rand.Float64
	}
	half := d / 2
	return half + time.Duration(rnd()*float64(d-half))
}
