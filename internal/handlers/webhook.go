// Package handlers contains example job handlers.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/nikrybak/taskq/internal/domain"
)

// WebhookType is the job type served by Webhook.
const WebhookType = "send-webhook"

// WebhookPayload is the payload of a send-webhook job.
type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
	Body    json.RawMessage   `json:"body"`
}

// Webhook delivers an HTTP request. Network errors, timeouts, 429 and 5xx
// responses are retried; other 4xx responses mean the request itself is wrong
// and fail permanently.
//
// In a multi-tenant deployment the target URL must be checked against an
// allow-list, since this handler will call any address it is given.
type Webhook struct {
	Client *http.Client
	// Timeout bounds a single delivery attempt, independently of the job
	// timeout, so a slow endpoint cannot hold a worker slot indefinitely.
	Timeout time.Duration
}

// Handle implements worker.Handler.
func (h Webhook) Handle(ctx context.Context, job domain.Job) error {
	var p WebhookPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return domain.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	u, err := url.Parse(p.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return domain.Permanent(fmt.Errorf("invalid url %q", p.URL))
	}
	if p.Method == "" {
		p.Method = http.MethodPost
	}

	if h.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, p.Method, u.String(), bytes.NewReader(p.Body))
	if err != nil {
		return domain.Permanent(fmt.Errorf("build request: %w", err))
	}
	if len(p.Body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range p.Headers {
		req.Header.Set(k, v)
	}
	// Lets the receiver deduplicate deliveries: a retry after a lost
	// response carries the same key.
	req.Header.Set("Idempotency-Key", fmt.Sprintf("taskq-%d", job.ID))

	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("deliver webhook: %w", err)
	}
	defer resp.Body.Close()
	// Drain a bounded amount so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("webhook returned %s", resp.Status)
	default:
		return domain.Permanent(errors.New("webhook rejected request: " + resp.Status))
	}
}
