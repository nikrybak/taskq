package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/nikrybak/taskq/internal/cron"
	"github.com/nikrybak/taskq/internal/domain"
	"github.com/nikrybak/taskq/internal/worker"
)

type scheduleFileEntry struct {
	Name        string          `json:"name"`
	Cron        string          `json:"cron"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority"`
	MaxAttempts int             `json:"max_attempts"`
}

// loadSchedules reads recurring job definitions from a JSON file. An empty
// path means no recurring jobs.
func loadSchedules(path string) ([]worker.Schedule, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schedules: %w", err)
	}
	return parseSchedules(raw)
}

func parseSchedules(raw []byte) ([]worker.Schedule, error) {
	var entries []scheduleFileEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("parse schedules: %w", err)
	}

	seen := make(map[string]bool, len(entries))
	out := make([]worker.Schedule, 0, len(entries))
	var errs []error
	for i, e := range entries {
		if e.Name == "" {
			errs = append(errs, fmt.Errorf("schedule #%d: name is required", i))
			continue
		}
		if seen[e.Name] {
			errs = append(errs, fmt.Errorf("schedule %q: duplicate name", e.Name))
			continue
		}
		seen[e.Name] = true

		spec, err := cron.Parse(e.Cron)
		if err != nil {
			errs = append(errs, fmt.Errorf("schedule %q: %w", e.Name, err))
			continue
		}
		job := domain.NewJob{Type: e.Type, Payload: e.Payload, Priority: e.Priority, MaxAttempts: e.MaxAttempts}
		if job.MaxAttempts == 0 {
			job.MaxAttempts = domain.DefaultMaxAttempts
		}
		if err := job.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("schedule %q: %w", e.Name, err))
			continue
		}
		out = append(out, worker.Schedule{Name: e.Name, Cron: spec, Job: job})
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}
