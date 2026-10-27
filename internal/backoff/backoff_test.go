package backoff

import (
	"testing"
	"time"
)

func TestPolicyDelay(t *testing.T) {
	fixed := func(v float64) func() float64 { return func() float64 { return v } }
	p := Policy{Base: time.Second, Max: time.Minute}

	tests := []struct {
		name    string
		attempt int
		rnd     float64
		want    time.Duration
	}{
		{"first attempt, no jitter", 1, 0, 500 * time.Millisecond},
		{"first attempt, half jitter", 1, 0.5, 750 * time.Millisecond},
		{"second attempt doubles", 2, 0, time.Second},
		{"third attempt", 3, 0.5, 3 * time.Second},
		{"capped at max", 10, 0, 30 * time.Second},
		{"huge attempt does not overflow", 10_000, 0, 30 * time.Second},
		{"zero attempt treated as first", 0, 0, 500 * time.Millisecond},
		{"negative attempt treated as first", -3, 0, 500 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := p
			p.Rand = fixed(tt.rnd)
			if got := p.Delay(tt.attempt); got != tt.want {
				t.Fatalf("Delay(%d) = %v, want %v", tt.attempt, got, tt.want)
			}
		})
	}
}

func TestPolicyDelayBounds(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: 5 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		for range 50 {
			d := p.Delay(attempt)
			if d < 50*time.Millisecond || d > p.Max {
				t.Fatalf("attempt %d: delay %v out of bounds", attempt, d)
			}
		}
	}
}

func TestPolicyDegenerate(t *testing.T) {
	if d := (Policy{}).Delay(3); d != 0 {
		t.Fatalf("zero policy should not wait, got %v", d)
	}
	// Max below Base is treated as Base rather than producing a negative range.
	p := Policy{Base: time.Second, Max: time.Millisecond, Rand: func() float64 { return 0 }}
	if d := p.Delay(5); d != 500*time.Millisecond {
		t.Fatalf("got %v", d)
	}
}
