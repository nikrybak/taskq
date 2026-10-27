package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors. Layers wrap them with context; callers test with errors.Is.
var (
	ErrNotFound = errors.New("job not found")
	// ErrInvalidTransition is matched by every *TransitionError.
	ErrInvalidTransition = errors.New("invalid state transition")
	// ErrConflict means the job changed between read and write.
	ErrConflict = errors.New("job was modified concurrently")
	// ErrLeaseLost means the worker no longer owns the job, usually because
	// its lease expired and the job was reclaimed.
	ErrLeaseLost = errors.New("job lease lost")
	// ErrIdempotencyMismatch means an idempotency key was reused for a job of
	// a different type.
	ErrIdempotencyMismatch = errors.New("idempotency key already used for a different job")
)

// TransitionError describes a rejected state change.
type TransitionError struct {
	From, To State
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("cannot move job from %s to %s", e.From, e.To)
}

// Is makes errors.Is(err, ErrInvalidTransition) succeed.
func (e *TransitionError) Is(target error) bool {
	return target == ErrInvalidTransition
}

// ValidationError reports invalid input for a single field.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return e.Field + " " + e.Reason
}
