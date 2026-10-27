package httpapi

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

type enqueueRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	RunAt          *time.Time      `json:"run_at"`
	MaxAttempts    int             `json:"max_attempts"`
	IdempotencyKey string          `json:"idempotency_key"`
}

func (r enqueueRequest) toDomain() domain.NewJob {
	n := domain.NewJob{
		Type:           r.Type,
		Payload:        r.Payload,
		Priority:       r.Priority,
		MaxAttempts:    r.MaxAttempts,
		IdempotencyKey: r.IdempotencyKey,
	}
	if r.RunAt != nil {
		n.RunAt = r.RunAt.UTC()
	}
	return n
}

type jobResponse struct {
	ID             int64           `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	State          domain.State    `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	LockedBy       string          `json:"locked_by,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

func newJobResponse(j domain.Job) jobResponse {
	payload := j.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	return jobResponse{
		ID: j.ID, Type: j.Type, Payload: payload, Priority: j.Priority, State: j.State,
		Attempts: j.Attempts, MaxAttempts: j.MaxAttempts, RunAt: j.RunAt.UTC(),
		IdempotencyKey: j.IdempotencyKey, LastError: j.LastError, LockedBy: j.LockedBy,
		LeaseExpiresAt: utc(j.LeaseExpiresAt), StartedAt: utc(j.StartedAt), FinishedAt: utc(j.FinishedAt),
		CreatedAt: j.CreatedAt.UTC(), UpdatedAt: j.UpdatedAt.UTC(),
	}
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

type listResponse struct {
	Jobs       []jobResponse `json:"jobs"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type statsResponse struct {
	Counts             map[domain.State]int64 `json:"counts"`
	OldestReadySeconds float64                `json:"oldest_ready_seconds"`
	AvgWaitSeconds     float64                `json:"avg_wait_seconds"`
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// Cursors are opaque to clients so the pagination scheme can change without
// breaking them; today they wrap the last seen job ID.
func encodeCursor(afterID int64) string {
	if afterID == 0 {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte("id:" + strconv.FormatInt(afterID, 10)))
}

func decodeCursor(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	invalid := &domain.ValidationError{Field: "cursor", Reason: "is invalid"}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil || len(raw) < 4 || string(raw[:3]) != "id:" {
		return 0, invalid
	}
	id, err := strconv.ParseInt(string(raw[3:]), 10, 64)
	if err != nil || id <= 0 {
		return 0, invalid
	}
	return id, nil
}
