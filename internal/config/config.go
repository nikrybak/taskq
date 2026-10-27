// Package config loads runtime configuration from environment variables.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the complete runtime configuration.
type Config struct {
	DatabaseURL string
	HTTPAddr    string
	AutoMigrate bool

	WorkerID          string
	Concurrency       int
	PollInterval      time.Duration
	Lease             time.Duration
	HeartbeatInterval time.Duration
	ReapInterval      time.Duration
	ShutdownTimeout   time.Duration

	BackoffBase time.Duration
	BackoffMax  time.Duration

	// SchedulesFile optionally points to a JSON file of recurring jobs.
	SchedulesFile string

	LogLevel  slog.Level
	LogFormat string
}

// Load reads the configuration using getenv, typically os.Getenv. It reports
// every invalid variable at once rather than stopping at the first.
func Load(getenv func(string) string) (Config, error) {
	p := parser{getenv: getenv}
	cfg := Config{
		DatabaseURL:       p.str("TASKQ_DATABASE_URL", ""),
		HTTPAddr:          p.str("TASKQ_HTTP_ADDR", ":8080"),
		AutoMigrate:       p.boolean("TASKQ_AUTO_MIGRATE", true),
		WorkerID:          p.str("TASKQ_WORKER_ID", ""),
		Concurrency:       p.integer("TASKQ_CONCURRENCY", 8),
		PollInterval:      p.duration("TASKQ_POLL_INTERVAL", time.Second),
		Lease:             p.duration("TASKQ_LEASE", 30*time.Second),
		HeartbeatInterval: p.duration("TASKQ_HEARTBEAT_INTERVAL", 10*time.Second),
		ReapInterval:      p.duration("TASKQ_REAP_INTERVAL", 15*time.Second),
		ShutdownTimeout:   p.duration("TASKQ_SHUTDOWN_TIMEOUT", 30*time.Second),
		BackoffBase:       p.duration("TASKQ_BACKOFF_BASE", 2*time.Second),
		BackoffMax:        p.duration("TASKQ_BACKOFF_MAX", 10*time.Minute),
		SchedulesFile:     p.str("TASKQ_SCHEDULES_FILE", ""),
		LogFormat:         strings.ToLower(p.str("TASKQ_LOG_FORMAT", "json")),
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(p.str("TASKQ_LOG_LEVEL", "info"))); err != nil {
		p.fail("TASKQ_LOG_LEVEL", err)
	}

	if cfg.DatabaseURL == "" {
		p.fail("TASKQ_DATABASE_URL", errors.New("is required"))
	}
	if cfg.Concurrency < 1 {
		p.fail("TASKQ_CONCURRENCY", errors.New("must be at least 1"))
	}
	// A heartbeat must land well before the lease runs out, otherwise a
	// healthy worker loses its jobs to the reaper.
	if cfg.HeartbeatInterval*2 > cfg.Lease {
		p.fail("TASKQ_HEARTBEAT_INTERVAL", fmt.Errorf("must be at most half of TASKQ_LEASE (%s)", cfg.Lease))
	}
	if cfg.BackoffMax < cfg.BackoffBase {
		p.fail("TASKQ_BACKOFF_MAX", errors.New("must not be less than TASKQ_BACKOFF_BASE"))
	}
	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		p.fail("TASKQ_LOG_FORMAT", errors.New(`must be "json" or "text"`))
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = defaultWorkerID()
	}
	return cfg, errors.Join(p.errs...)
}

// defaultWorkerID combines the hostname with a random suffix. The suffix
// matters in containers, where every replica may report the same PID.
func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "taskq"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}

type parser struct {
	getenv func(string) string
	errs   []error
}

func (p *parser) fail(key string, err error) {
	p.errs = append(p.errs, fmt.Errorf("%s: %w", key, err))
}

func (p *parser) str(key, def string) string {
	if v := strings.TrimSpace(p.getenv(key)); v != "" {
		return v
	}
	return def
}

func (p *parser) integer(key string, def int) int {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		p.fail(key, err)
		return def
	}
	return n
}

func (p *parser) boolean(key string, def bool) bool {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.fail(key, err)
		return def
	}
	return b
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := p.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		p.fail(key, err)
		return def
	}
	if d <= 0 {
		p.fail(key, errors.New("must be positive"))
		return def
	}
	return d
}
