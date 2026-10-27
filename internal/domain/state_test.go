package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCanTransition(t *testing.T) {
	allowed := map[[2]State]bool{
		{StateQueued, StateRunning}:    true,
		{StateQueued, StateCancelled}:  true,
		{StateRunning, StateSucceeded}: true,
		{StateRunning, StateQueued}:    true,
		{StateRunning, StateDead}:      true,
		{StateDead, StateQueued}:       true,
		{StateCancelled, StateQueued}:  true,
	}
	for _, from := range AllStates {
		for _, to := range AllStates {
			want := allowed[[2]State{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestCheckTransitionError(t *testing.T) {
	err := CheckTransition(StateSucceeded, StateQueued)
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
	var te *TransitionError
	if !errors.As(err, &te) || te.From != StateSucceeded || te.To != StateQueued {
		t.Fatalf("unexpected error detail: %#v", err)
	}
	if CheckTransition(StateQueued, StateRunning) != nil {
		t.Fatal("queued -> running must be allowed")
	}
}

func TestParseState(t *testing.T) {
	if s, err := ParseState("dead"); err != nil || s != StateDead {
		t.Fatalf("ParseState(dead) = %q, %v", s, err)
	}
	var ve *ValidationError
	if _, err := ParseState("done"); !errors.As(err, &ve) || ve.Field != "state" {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestNewJobValidate(t *testing.T) {
	valid := NewJob{Type: "email", Payload: json.RawMessage(`{"a":1}`), MaxAttempts: 3}
	tests := []struct {
		name  string
		edit  func(*NewJob)
		field string
	}{
		{"valid", func(*NewJob) {}, ""},
		{"empty payload is allowed", func(n *NewJob) { n.Payload = nil }, ""},
		{"missing type", func(n *NewJob) { n.Type = "" }, "type"},
		{"long type", func(n *NewJob) { n.Type = strings.Repeat("x", MaxTypeLength+1) }, "type"},
		{"priority too high", func(n *NewJob) { n.Priority = MaxPriority + 1 }, "priority"},
		{"priority too low", func(n *NewJob) { n.Priority = MinPriority - 1 }, "priority"},
		{"zero attempts", func(n *NewJob) { n.MaxAttempts = 0 }, "max_attempts"},
		{"too many attempts", func(n *NewJob) { n.MaxAttempts = MaxAllowedAttempts + 1 }, "max_attempts"},
		{"bad json", func(n *NewJob) { n.Payload = json.RawMessage(`{`) }, "payload"},
		{"long key", func(n *NewJob) { n.IdempotencyKey = strings.Repeat("k", MaxIdempotencyKeyLen+1) }, "idempotency_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := valid
			tt.edit(&n)
			err := n.Validate()
			if tt.field == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var ve *ValidationError
			if !errors.As(err, &ve) || ve.Field != tt.field {
				t.Fatalf("expected validation error on %q, got %v", tt.field, err)
			}
		})
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("bad payload")
	err := fmt.Errorf("handler: %w", Permanent(base))
	if !IsPermanent(err) {
		t.Fatal("wrapped permanent error not detected")
	}
	if !errors.Is(err, base) {
		t.Fatal("permanent error must unwrap to its cause")
	}
	if IsPermanent(base) || Permanent(nil) != nil {
		t.Fatal("unexpected permanent classification")
	}
}
