package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(env(map[string]string{"TASKQ_DATABASE_URL": "postgres://localhost/taskq"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.Concurrency != 8 || cfg.Lease != 30*time.Second || !cfg.AutoMigrate {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.LogLevel != slog.LevelInfo || cfg.WorkerID == "" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"TASKQ_DATABASE_URL":       "postgres://db/taskq",
		"TASKQ_CONCURRENCY":        "2",
		"TASKQ_LEASE":              "1m",
		"TASKQ_HEARTBEAT_INTERVAL": "20s",
		"TASKQ_LOG_LEVEL":          "debug",
		"TASKQ_LOG_FORMAT":         "TEXT",
		"TASKQ_WORKER_ID":          "w-1",
		"TASKQ_AUTO_MIGRATE":       "false",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Concurrency != 2 || cfg.Lease != time.Minute || cfg.HeartbeatInterval != 20*time.Second ||
		cfg.LogLevel != slog.LevelDebug || cfg.LogFormat != "text" || cfg.WorkerID != "w-1" || cfg.AutoMigrate {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	_, err := Load(env(map[string]string{
		"TASKQ_CONCURRENCY":        "0",
		"TASKQ_POLL_INTERVAL":      "soon",
		"TASKQ_LEASE":              "10s",
		"TASKQ_HEARTBEAT_INTERVAL": "6s",
		"TASKQ_LOG_LEVEL":          "loud",
	}))
	if err == nil {
		t.Fatal("expected an error")
	}
	for _, key := range []string{
		"TASKQ_DATABASE_URL", "TASKQ_CONCURRENCY", "TASKQ_POLL_INTERVAL", "TASKQ_HEARTBEAT_INTERVAL", "TASKQ_LOG_LEVEL",
	} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not mention %s: %v", key, err)
		}
	}
}
