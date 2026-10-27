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

// permanentError marks a failure that retrying cannot fix.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent wraps err so the job is moved to the dead-letter state right away
// instead of being retried. Handlers use it for failures such as malformed
// payloads or a 4xx response from a webhook target.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// IsPermanent reports whether err, or any error it wraps, was marked with
// Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}
