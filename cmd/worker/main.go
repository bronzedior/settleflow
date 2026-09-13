package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/bronzedior/custodian/internal/graph"
	"github.com/bronzedior/custodian/internal/jobs"
	"github.com/bronzedior/custodian/internal/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://custodian:custodian@localhost:5432/custodian?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	if err := migrations.Apply(ctx, pool); err != nil {
		return err
	}

	workers := river.NewWorkers()
	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault:  {MaxWorkers: 10},
			jobs.QueueDiscovery: {MaxWorkers: 10},
		},
		Workers: workers,
	})
	if err != nil {
		return err
	}

	river.AddWorker(workers, &jobs.PingWorker{})

	if cred, err := azidentity.NewWorkloadIdentityCredential(nil); err != nil {
		slog.Warn("discovery workers disabled: no workload identity credential", "error", err)
	} else {
		graphClient := graph.NewClient(cred)
		river.AddWorker(workers, &jobs.DiscoverPageWorker{DB: pool, Graph: graphClient, River: riverClient})
		river.AddWorker(workers, &jobs.DiscoverDetailWorker{DB: pool, Graph: graphClient})
		river.AddWorker(workers, &jobs.EvaluateScanRunWorker{DB: pool})
	}

	if err := riverClient.Start(ctx); err != nil {
		return err
	}

	slog.Info("worker started", "database_url", databaseURL)
	<-ctx.Done()

	slog.Info("worker shutting down")
	return riverClient.Stop(context.Background())
}
