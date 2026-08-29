package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bronzedior/settleflow/internal/migrations"
	"github.com/bronzedior/settleflow/internal/queue"
	"github.com/bronzedior/settleflow/internal/worker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("DATABASE_URL not set")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := applyMigrations(ctx, databaseURL); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}

	registry := queue.NewRegistry(logger)
	if err := registerHandlers(registry); err != nil {
		return fmt.Errorf("register handlers: %w", err)
	}

	config := &queue.PoolConfig{
		WorkerID:          workerID,
		Queues:            []string{"default"},
		Concurrency:       10,
		MaxBatch:          10,
		PollInterval:      1 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		ReaperThreshold:   60 * time.Second,
		JobTimeout:        30 * time.Second,
		Logger:            logger,
	}

	supervisor, err := worker.CreateWorkerStack(pool, config, registry, logger)
	if err != nil {
		return fmt.Errorf("create worker stack: %w", err)
	}

	logger.Info("Starting worker", "workerID", workerID)
	if err := supervisor.StartAll(ctx); err != nil {
		return fmt.Errorf("start worker stack: %w", err)
	}

	<-ctx.Done()
	logger.Info("Shutdown signal received, draining in-flight jobs")

	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return supervisor.StopAll(stopCtx)
}

func applyMigrations(ctx context.Context, databaseURL string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect for migrations: %w", err)
	}
	defer conn.Close(ctx)

	return migrations.RunMigrations(ctx, conn)
}

type TestJobArgs struct {
	Index    int    `json:"index"`
	Duration string `json:"duration"`
}

func registerHandlers(registry *queue.Registry) error {
	return queue.Register(registry, "test", 1, 20, func(ctx context.Context, args TestJobArgs) error {
		if d, err := time.ParseDuration(args.Duration); err == nil {
			time.Sleep(d)
		}
		return nil
	})
}
