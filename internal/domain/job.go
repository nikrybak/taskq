// Package domain holds the job model, its state machine and the typed errors
// shared by every layer of taskq.
package domain

import (
	"encoding/json"
	"time"
)

// Job is a unit of background work as stored in the queue.
type Job struct {
	ID             int64
	Type           string
	Payload        json.RawMessage
	Priority       int
	State          State
	Attempts       int
	MaxAttempts    int
	RunAt          time.Time
	IdempotencyKey string
	LastError      string
	LockedBy       string
	LeaseExpiresAt *time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Lease identifies one claimed execution of a job. Every write a worker makes
// on behalf of a running job is fenced by the lease, so a worker whose lease
// expired and was reclaimed cannot overwrite the result of the new owner.
type Lease struct {
	JobID    int64
	WorkerID string
	Attempt  int
}

// Lease returns the lease held by the worker currently running the job.
func (j Job) Lease() Lease {
	return Lease{JobID: j.ID, WorkerID: j.LockedBy, Attempt: j.Attempts}
}

// NewJob is the input for enqueuing a job.
type NewJob struct {
	Type           string
	Payload        json.RawMessage
	Priority       int
	RunAt          time.Time
	MaxAttempts    int
	IdempotencyKey string
}

// Limits for user-supplied job attributes.
const (
	MinPriority          = -100
	MaxPriority          = 100
	MaxAllowedAttempts   = 100
	DefaultMaxAttempts   = 5
	MaxTypeLength        = 128
	MaxIdempotencyKeyLen = 256
	MaxPayloadBytes      = 1 << 20
)

// Validate checks the business invariants of a new job. It does not fill in
// defaults; callers are expected to do that first.
func (n NewJob) Validate() error {
	switch {
	case n.Type == "":
		return &ValidationError{Field: "type", Reason: "is required"}
	case len(n.Type) > MaxTypeLength:
		return &ValidationError{Field: "type", Reason: "is too long"}
	case n.Priority < MinPriority || n.Priority > MaxPriority:
		return &ValidationError{Field: "priority", Reason: "must be between -100 and 100"}
	case n.MaxAttempts < 1 || n.MaxAttempts > MaxAllowedAttempts:
		return &ValidationError{Field: "max_attempts", Reason: "must be between 1 and 100"}
	case len(n.IdempotencyKey) > MaxIdempotencyKeyLen:
		return &ValidationError{Field: "idempotency_key", Reason: "is too long"}
	case len(n.Payload) > MaxPayloadBytes:
		return &ValidationError{Field: "payload", Reason: "exceeds 1 MiB"}
	case len(n.Payload) > 0 && !json.Valid(n.Payload):
		return &ValidationError{Field: "payload", Reason: "must be valid JSON"}
	}
	return nil
}

// ListFilter selects jobs for listing. Results are ordered by descending ID
// and AfterID is an exclusive keyset cursor.
type ListFilter struct {
	State   State
	Type    string
	AfterID int64
	Limit   int
}

// Stats summarises the queue.
type Stats struct {
	Counts map[State]int64
	// OldestReadyAge is how long the oldest runnable job has been waiting.
	OldestReadyAge time.Duration
	// AvgWait is the mean time between run_at and start for jobs started in
	// the last five minutes.
	AvgWait time.Duration
}
