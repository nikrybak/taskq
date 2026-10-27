package handlers

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

func webhookJob(t *testing.T, p WebhookPayload) domain.Job {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	return domain.Job{ID: 42, Type: WebhookType, Payload: raw}
}

func TestWebhook(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		delay     time.Duration
		wantErr   bool
		permanent bool
	}{
		{"success", http.StatusNoContent, 0, false, false},
		{"server error is retried", http.StatusBadGateway, 0, true, false},
		{"rate limit is retried", http.StatusTooManyRequests, 0, true, false},
		{"client error is permanent", http.StatusUnprocessableEntity, 0, true, true},
		{"timeout is retried", http.StatusOK, 500 * time.Millisecond, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotKey, gotAuth string
			var gotBody []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotKey, gotAuth = r.Header.Get("Idempotency-Key"), r.Header.Get("Authorization")
				gotBody, _ = io.ReadAll(r.Body)
				select {
				case <-time.After(tt.delay):
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(tt.status)
			}))

			h := Webhook{Client: srv.Client(), Timeout: 100 * time.Millisecond}
			defer srv.Close()
			err := h.Handle(context.Background(), webhookJob(t, WebhookPayload{
				URL: srv.URL, Headers: map[string]string{"Authorization": "Bearer t"}, Body: json.RawMessage(`{"a":1}`),
			}))

			if (err != nil) != tt.wantErr || domain.IsPermanent(err) != tt.permanent {
				t.Fatalf("err = %v (permanent=%v), want error=%v permanent=%v", err, domain.IsPermanent(err), tt.wantErr, tt.permanent)
			}
			if tt.delay > 0 && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want DeadlineExceeded, got %v", err)
			}
			// Close waits for the server handler, which makes its writes visible here.
			srv.Close()
			if tt.delay > 0 {
				return
			}
			if gotKey != "taskq-42" || gotAuth != "Bearer t" || string(gotBody) != `{"a":1}` {
				t.Fatalf("request: key=%q auth=%q body=%s", gotKey, gotAuth, gotBody)
			}
		})
	}
}

func TestWebhookInvalidPayloadIsPermanent(t *testing.T) {
	h := Webhook{}
	for _, payload := range []string{`{`, `{"url":"ftp://example.com"}`, `{"url":"/relative"}`} {
		err := h.Handle(context.Background(), domain.Job{Payload: json.RawMessage(payload)})
		if !domain.IsPermanent(err) {
			t.Errorf("payload %s: want permanent error, got %v", payload, err)
		}
	}
}

func TestStretch(t *testing.T) {
	one, err := Stretch(context.Background(), []byte("abc"), 1)
	if err != nil || one != sha256.Sum256([]byte("abc")) {
		t.Fatalf("one round: %x, %v", one, err)
	}
	two, _ := Stretch(context.Background(), []byte("abc"), 2)
	if two != sha256.Sum256(one[:]) {
		t.Fatalf("two rounds: %x", two)
	}
}

func TestStretchHonoursCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	_, err := Stretch(ctx, []byte("x"), MaxChecksumRounds)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want Canceled, got %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("cancellation was not prompt")
	}
}

func TestChecksumValidatesRounds(t *testing.T) {
	h := Checksum{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	for _, p := range []string{`{"data":"x","rounds":0}`, `{"data":"x","rounds":999999999}`, `[]`} {
		if err := h.Handle(context.Background(), domain.Job{Payload: json.RawMessage(p)}); !domain.IsPermanent(err) {
			t.Errorf("payload %s: want permanent error, got %v", p, err)
		}
	}
	if err := h.Handle(context.Background(), domain.Job{Payload: json.RawMessage(`{"data":"x","rounds":10}`)}); err != nil {
		t.Fatal(err)
	}
}
