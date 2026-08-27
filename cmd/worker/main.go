package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/bronzedior/settleflow/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	poolURL := os.Getenv("DATABASE_URL")
	if poolURL == "" {
		return fmt.Errorf("DATABASE_URL not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, poolURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := queue.NewStore(queue.WrapPgx(pool))

	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", os.Getpid())
	}

	queues := []string{"default"}
	maxBatch := 1

	logger.Info("Starting worker", "workerID", workerID)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			jobs, err := store.ClaimJobs(ctx, workerID, queues, maxBatch)
			if err != nil {
				logger.Error("Failed to claim jobs", "err", err)
				continue
			}

			for _, job := range jobs {
				logger.Info("Claimed job", "jobID", job.ID, "jobType", job.JobType)

				err := store.CompleteJob(ctx, job.ID, workerID)
				if err != nil {
					logger.Error("Failed to complete job", "jobID", job.ID, "err", err)
					err = store.FailJob(ctx, job.ID, workerID, err.Error(), queue.ErrorClassPermanent)
					if err != nil {
						logger.Error("Failed to mark job as failed", "jobID", job.ID, "err", err)
					}
				} else {
					logger.Info("Completed job", "jobID", job.ID)
				}
			}
		}
	}
}
