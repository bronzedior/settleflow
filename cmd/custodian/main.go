package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/bronzedior/custodian/internal/jobs"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("custodian failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: custodian <ping>")
	}

	switch args[0] {
	case "ping":
		return ping(context.Background())
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ping enqueues a PingArgs job and prints its id, proving the CLI can reach
// PostgreSQL and hand work to the queue (Phase 0 done criterion).
func ping(ctx context.Context) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://custodian:custodian@localhost:5432/custodian?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		return err
	}

	result, err := riverClient.Insert(ctx, jobs.PingArgs{Message: "ping"}, nil)
	if err != nil {
		return err
	}

	fmt.Println(result.Job.ID)
	return nil
}
