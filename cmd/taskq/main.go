// Command taskq runs the job queue: HTTP API, worker pool, lease reaper and
// recurring-job scheduler in one process.
//
// Usage:
//
//	taskq [serve]   run the service (default)
//	taskq migrate   apply database migrations and exit
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nikrybak/taskq/internal/backoff"
	"github.com/nikrybak/taskq/internal/config"
	"github.com/nikrybak/taskq/internal/handlers"
	"github.com/nikrybak/taskq/internal/repository/postgres"
	"github.com/nikrybak/taskq/internal/service"
	"github.com/nikrybak/taskq/internal/transport/httpapi"
	"github.com/nikrybak/taskq/internal/worker"
	"github.com/nikrybak/taskq/migrations"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "taskq:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	if cmd != "serve" && cmd != "migrate" {
		return fmt.Errorf("unknown command %q (want serve or migrate)", cmd)
	}

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log := newLogger(cfg)

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}

	if cmd == "migrate" || cfg.AutoMigrate {
		if err := postgres.Migrate(ctx, db, migrations.FS, log); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	if cmd == "migrate" {
		return nil
	}
	return serve(ctx, cfg, db, log)
}

func serve(ctx context.Context, cfg config.Config, db *pgxpool.Pool, log *slog.Logger) error {
	// Cancelled on a signal (via the parent) or when the HTTP server fails,
	// so the background components always stop together.
	ctx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	repo := postgres.New(db)
	svc := service.New(repo, service.Config{
		Lease:   cfg.Lease,
		Backoff: backoff.Policy{Base: cfg.BackoffBase, Max: cfg.BackoffMax},
	}, log)

	registry := worker.NewRegistry()
	registry.Register(handlers.WebhookType, handlers.Webhook{
		Client:  &http.Client{Timeout: time.Minute},
		Timeout: 15 * time.Second,
	}, 30*time.Second)
	registry.Register(handlers.ChecksumType, handlers.Checksum{Log: log}, 2*time.Minute)

	schedules, err := loadSchedules(cfg.SchedulesFile)
	if err != nil {
		return err
	}

	pool := worker.NewPool(svc, registry, worker.PoolConfig{
		WorkerID:          cfg.WorkerID,
		Concurrency:       cfg.Concurrency,
		PollInterval:      cfg.PollInterval,
		HeartbeatInterval: cfg.HeartbeatInterval,
		DrainTimeout:      cfg.ShutdownTimeout,
	}, log)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpapi.NewHandler(svc, repo, log),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	// Background components stop claiming or scheduling as soon as ctx is
	// cancelled; the pool then drains running jobs on its own.
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := pool.Run(ctx); err != nil {
			log.Error("worker pool", "err", err)
		}
	})
	wg.Go(func() { worker.RunReaper(ctx, svc, cfg.ReapInterval, log) })
	wg.Go(func() { worker.NewScheduler(svc, schedules, log).Run(ctx) })

	serveErr := make(chan error, 1)
	go func() {
		log.Info("http server listening", "addr", cfg.HTTPAddr, "worker_id", cfg.WorkerID)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
		close(serveErr)
	}()

	var runErr error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested")
	case err := <-serveErr:
		runErr = fmt.Errorf("http server: %w", err)
	}

	// Stop claiming first and let running jobs drain while the API keeps
	// answering status queries; the pool's DrainTimeout bounds the wait.
	cancelRun()
	wg.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("http shutdown: %w", err))
	}
	log.Info("shutdown complete")
	return runErr
}

func newLogger(cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	var h slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
