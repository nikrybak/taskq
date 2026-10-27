// Package httpapi exposes the job service over HTTP with JSON bodies.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
	"github.com/nikrybak/taskq/internal/service"
)

// JobService is the application service used by the API.
type JobService interface {
	Enqueue(ctx context.Context, n domain.NewJob) (domain.Job, bool, error)
	Get(ctx context.Context, id int64) (domain.Job, error)
	List(ctx context.Context, f domain.ListFilter) (service.Page, error)
	Retry(ctx context.Context, id int64) (domain.Job, error)
	Cancel(ctx context.Context, id int64) (domain.Job, error)
	Stats(ctx context.Context) (domain.Stats, error)
}

// Pinger reports whether a dependency is reachable.
type Pinger interface {
	Ping(ctx context.Context) error
}

// maxBodyBytes leaves room for the JSON envelope around the largest payload.
const maxBodyBytes = domain.MaxPayloadBytes + 64<<10

type api struct {
	svc    JobService
	health Pinger
	log    *slog.Logger
}

// NewHandler returns the HTTP handler for the API.
func NewHandler(svc JobService, health Pinger, log *slog.Logger) http.Handler {
	a := &api{svc: svc, health: health, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", a.enqueue)
	mux.HandleFunc("GET /jobs", a.list)
	mux.HandleFunc("GET /jobs/{id}", a.get)
	mux.HandleFunc("POST /jobs/{id}/retry", a.retry)
	mux.HandleFunc("DELETE /jobs/{id}", a.cancel)
	mux.HandleFunc("GET /stats", a.stats)
	mux.HandleFunc("GET /healthz", a.healthz)
	return recoverer(log, accessLog(log, mux))
}

func (a *api) enqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, r, a.log, err)
		return
	}
	if hdr := r.Header.Get("Idempotency-Key"); hdr != "" {
		if req.IdempotencyKey != "" && req.IdempotencyKey != hdr {
			writeError(w, r, a.log, &domain.ValidationError{Field: "idempotency_key", Reason: "differs from the Idempotency-Key header"})
			return
		}
		req.IdempotencyKey = hdr
	}

	job, created, err := a.svc.Enqueue(r.Context(), req.toDomain())
	if err != nil {
		writeError(w, r, a.log, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("Location", fmt.Sprintf("/jobs/%d", job.ID))
	writeJSON(w, status, newJobResponse(job))
}

func (a *api) get(w http.ResponseWriter, r *http.Request) {
	a.withJob(w, r, a.svc.Get)
}

func (a *api) retry(w http.ResponseWriter, r *http.Request) {
	a.withJob(w, r, a.svc.Retry)
}

func (a *api) cancel(w http.ResponseWriter, r *http.Request) {
	a.withJob(w, r, a.svc.Cancel)
}

// withJob parses the {id} path value, runs op and writes the resulting job.
func (a *api) withJob(w http.ResponseWriter, r *http.Request, op func(context.Context, int64) (domain.Job, error)) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, r, a.log, &domain.ValidationError{Field: "id", Reason: "must be a positive integer"})
		return
	}
	job, err := op(r.Context(), id)
	if err != nil {
		writeError(w, r, a.log, err)
		return
	}
	writeJSON(w, http.StatusOK, newJobResponse(job))
}

func (a *api) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := domain.ListFilter{Type: q.Get("type")}
	if s := q.Get("state"); s != "" {
		st, err := domain.ParseState(s)
		if err != nil {
			writeError(w, r, a.log, err)
			return
		}
		f.State = st
	}
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			writeError(w, r, a.log, &domain.ValidationError{Field: "limit", Reason: "must be an integer"})
			return
		}
		f.Limit = n
	}
	after, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		writeError(w, r, a.log, err)
		return
	}
	f.AfterID = after

	page, err := a.svc.List(r.Context(), f)
	if err != nil {
		writeError(w, r, a.log, err)
		return
	}
	resp := listResponse{Jobs: make([]jobResponse, 0, len(page.Jobs)), NextCursor: encodeCursor(page.NextAfterID)}
	for _, j := range page.Jobs {
		resp.Jobs = append(resp.Jobs, newJobResponse(j))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *api) stats(w http.ResponseWriter, r *http.Request) {
	st, err := a.svc.Stats(r.Context())
	if err != nil {
		writeError(w, r, a.log, err)
		return
	}
	writeJSON(w, http.StatusOK, statsResponse{
		Counts:             st.Counts,
		OldestReadySeconds: st.OldestReadyAge.Seconds(),
		AvgWaitSeconds:     st.AvgWait.Seconds(),
	})
}

func (a *api) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := a.health.Ping(ctx); err != nil {
		a.log.WarnContext(ctx, "health check failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return err
		}
		return &domain.ValidationError{Field: "body", Reason: "is not valid JSON: " + err.Error()}
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return &domain.ValidationError{Field: "body", Reason: "must contain a single JSON object"}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// The status line is already sent, so an encoding error can only be a
	// broken connection; there is nothing useful left to tell the client.
	_ = json.NewEncoder(w).Encode(v)
}
