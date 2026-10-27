package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
	"github.com/nikrybak/taskq/internal/service"
)

type fakeService struct {
	enqueue func(domain.NewJob) (domain.Job, bool, error)
	get     func(int64) (domain.Job, error)
	list    func(domain.ListFilter) (service.Page, error)
	retry   func(int64) (domain.Job, error)
	cancel  func(int64) (domain.Job, error)
	stats   func() (domain.Stats, error)
}

func (f *fakeService) Enqueue(_ context.Context, n domain.NewJob) (domain.Job, bool, error) {
	return f.enqueue(n)
}
func (f *fakeService) Get(_ context.Context, id int64) (domain.Job, error) { return f.get(id) }
func (f *fakeService) List(_ context.Context, fl domain.ListFilter) (service.Page, error) {
	return f.list(fl)
}
func (f *fakeService) Retry(_ context.Context, id int64) (domain.Job, error)  { return f.retry(id) }
func (f *fakeService) Cancel(_ context.Context, id int64) (domain.Job, error) { return f.cancel(id) }
func (f *fakeService) Stats(context.Context) (domain.Stats, error)            { return f.stats() }

type pingFunc func() error

func (p pingFunc) Ping(context.Context) error { return p() }

func newServer(svc *fakeService, ping pingFunc) http.Handler {
	if ping == nil {
		ping = func() error { return nil }
	}
	return NewHandler(svc, ping, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func do(t *testing.T, h http.Handler, method, target, body string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func TestEnqueue(t *testing.T) {
	runAt := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	var got domain.NewJob
	svc := &fakeService{enqueue: func(n domain.NewJob) (domain.Job, bool, error) {
		got = n
		return domain.Job{ID: 7, Type: n.Type, Payload: n.Payload, State: domain.StateQueued, RunAt: n.RunAt}, true, nil
	}}
	rec := do(t, newServer(svc, nil), http.MethodPost, "/jobs",
		`{"type":"send-webhook","payload":{"url":"http://x"},"priority":5,"run_at":"2026-03-10T15:00:00+03:00","max_attempts":3}`,
		"Idempotency-Key", "abc")

	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/jobs/7" {
		t.Fatalf("Location = %q", loc)
	}
	if got.Type != "send-webhook" || got.Priority != 5 || got.MaxAttempts != 3 || got.IdempotencyKey != "abc" || !got.RunAt.Equal(runAt) {
		t.Fatalf("service received %+v", got)
	}
	resp := decode[jobResponse](t, rec)
	if resp.ID != 7 || resp.State != domain.StateQueued || string(resp.Payload) != `{"url":"http://x"}` {
		t.Fatalf("response %+v", resp)
	}
}

func TestEnqueueExistingReturns200(t *testing.T) {
	svc := &fakeService{enqueue: func(n domain.NewJob) (domain.Job, bool, error) {
		return domain.Job{ID: 3, Type: n.Type}, false, nil
	}}
	rec := do(t, newServer(svc, nil), http.MethodPost, "/jobs", `{"type":"t","idempotency_key":"k"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestEnqueueBadRequests(t *testing.T) {
	svc := &fakeService{enqueue: func(n domain.NewJob) (domain.Job, bool, error) {
		return domain.Job{}, false, n.Validate()
	}}
	tests := []struct {
		name  string
		body  string
		hdr   []string
		field string
	}{
		{"malformed json", `{"type":`, nil, "body"},
		{"unknown field", `{"type":"t","colour":"red"}`, nil, "body"},
		{"trailing data", `{"type":"t"} {}`, nil, "body"},
		{"bad run_at", `{"type":"t","run_at":"tomorrow"}`, nil, "body"},
		{"key mismatch", `{"type":"t","idempotency_key":"a"}`, []string{"Idempotency-Key", "b"}, "idempotency_key"},
		{"service validation", `{"type":"t","max_attempts":1000}`, nil, "max_attempts"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := do(t, newServer(svc, nil), http.MethodPost, "/jobs", tt.body, tt.hdr...)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d: %s", rec.Code, rec.Body)
			}
			if e := decode[errorBody](t, rec); e.Error.Code != "validation_failed" || e.Error.Field != tt.field {
				t.Fatalf("error %+v, want field %q", e.Error, tt.field)
			}
		})
	}
}

func TestEnqueueBodyTooLarge(t *testing.T) {
	svc := &fakeService{}
	body := `{"type":"t","payload":"` + strings.Repeat("x", maxBodyBytes) + `"}`
	if rec := do(t, newServer(svc, nil), http.MethodPost, "/jobs", body); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d", rec.Code)
	}
}

func TestErrorMapping(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{fmt.Errorf("get: %w", domain.ErrNotFound), http.StatusNotFound, "not_found"},
		{&domain.TransitionError{From: domain.StateRunning, To: domain.StateCancelled}, http.StatusConflict, "invalid_transition"},
		{fmt.Errorf("x: %w", domain.ErrConflict), http.StatusConflict, "conflict"},
		{domain.ErrIdempotencyMismatch, http.StatusConflict, "idempotency_mismatch"},
		{errors.New("connection reset by peer"), http.StatusInternalServerError, "internal"},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			svc := &fakeService{cancel: func(int64) (domain.Job, error) { return domain.Job{}, tt.err }}
			rec := do(t, newServer(svc, nil), http.MethodDelete, "/jobs/1", "")
			if rec.Code != tt.status {
				t.Fatalf("status %d, want %d", rec.Code, tt.status)
			}
			e := decode[errorBody](t, rec)
			if e.Error.Code != tt.code {
				t.Fatalf("code %q, want %q", e.Error.Code, tt.code)
			}
			if tt.status == http.StatusInternalServerError && strings.Contains(e.Error.Message, "peer") {
				t.Fatal("internal error details leaked to the client")
			}
		})
	}
}

func TestJobByIDRoutes(t *testing.T) {
	var calls []string
	job := func(op string) func(int64) (domain.Job, error) {
		return func(id int64) (domain.Job, error) {
			calls = append(calls, fmt.Sprintf("%s:%d", op, id))
			return domain.Job{ID: id}, nil
		}
	}
	h := newServer(&fakeService{get: job("get"), retry: job("retry"), cancel: job("cancel")}, nil)

	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/jobs/5"}, {http.MethodPost, "/jobs/6/retry"}, {http.MethodDelete, "/jobs/7"},
	} {
		if rec := do(t, h, c.method, c.path, ""); rec.Code != http.StatusOK {
			t.Fatalf("%s %s: status %d", c.method, c.path, rec.Code)
		}
	}
	if want := "get:5 retry:6 cancel:7"; strings.Join(calls, " ") != want {
		t.Fatalf("calls %v, want %s", calls, want)
	}
	if rec := do(t, h, http.MethodGet, "/jobs/abc", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("non-numeric id: status %d", rec.Code)
	}
	if rec := do(t, h, http.MethodPut, "/jobs/5", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT: status %d", rec.Code)
	}
}

func TestListPassesFiltersAndCursor(t *testing.T) {
	var got domain.ListFilter
	svc := &fakeService{list: func(f domain.ListFilter) (service.Page, error) {
		got = f
		return service.Page{Jobs: []domain.Job{{ID: 9}, {ID: 8}}, NextAfterID: 8}, nil
	}}
	h := newServer(svc, nil)

	rec := do(t, h, http.MethodGet, "/jobs?state=dead&type=email&limit=2", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	resp := decode[listResponse](t, rec)
	if got.State != domain.StateDead || got.Type != "email" || got.Limit != 2 || got.AfterID != 0 {
		t.Fatalf("filter %+v", got)
	}
	if len(resp.Jobs) != 2 || resp.NextCursor == "" {
		t.Fatalf("response %+v", resp)
	}

	if rec := do(t, h, http.MethodGet, "/jobs?cursor="+resp.NextCursor, ""); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if got.AfterID != 8 {
		t.Fatalf("cursor decoded to %d, want 8", got.AfterID)
	}

	for _, q := range []string{"state=finished", "limit=ten", "cursor=!!!", "cursor=" + encodeCursor(1)[:3]} {
		if rec := do(t, h, http.MethodGet, "/jobs?"+q, ""); rec.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d", q, rec.Code)
		}
	}
}

func TestListEmptyIsArray(t *testing.T) {
	svc := &fakeService{list: func(domain.ListFilter) (service.Page, error) { return service.Page{}, nil }}
	rec := do(t, newServer(svc, nil), http.MethodGet, "/jobs", "")
	if body := strings.TrimSpace(rec.Body.String()); body != `{"jobs":[]}` {
		t.Fatalf("body %s", body)
	}
}

func TestStats(t *testing.T) {
	svc := &fakeService{stats: func() (domain.Stats, error) {
		return domain.Stats{
			Counts:         map[domain.State]int64{domain.StateQueued: 4, domain.StateDead: 1},
			OldestReadyAge: 1500 * time.Millisecond,
		}, nil
	}}
	rec := do(t, newServer(svc, nil), http.MethodGet, "/stats", "")
	resp := decode[statsResponse](t, rec)
	if resp.Counts[domain.StateQueued] != 4 || resp.OldestReadySeconds != 1.5 {
		t.Fatalf("stats %+v", resp)
	}
}

func TestHealthz(t *testing.T) {
	ok := do(t, newServer(&fakeService{}, nil), http.MethodGet, "/healthz", "")
	if ok.Code != http.StatusOK {
		t.Fatalf("healthy: status %d", ok.Code)
	}
	down := do(t, newServer(&fakeService{}, func() error { return errors.New("db down") }), http.MethodGet, "/healthz", "")
	if down.Code != http.StatusServiceUnavailable {
		t.Fatalf("unhealthy: status %d", down.Code)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	svc := &fakeService{get: func(int64) (domain.Job, error) { panic("bug") }}
	if rec := do(t, newServer(svc, nil), http.MethodGet, "/jobs/1", ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("status %d", rec.Code)
	}
}
