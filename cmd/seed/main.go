package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/bronzedior/settleflow/internal/queue"
	"github.com/jackc/pgx/v5"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	count := flag.Int("count", 100, "number of jobs to seed")
	jobType := flag.String("type", "test", "job type")
	duration := flag.Duration("duration", 100*time.Millisecond, "simulated job duration")
	directURL := flag.String("url", "", "direct database URL")
	flag.Parse()

	if *directURL == "" {
		return fmt.Errorf("--url is required")
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *directURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer conn.Close(ctx)

	store := queue.NewStore(queue.WrapPgx(conn))

	for i := 0; i < *count; i++ {
		payload := map[string]interface{}{
			"index":    i,
			"duration": duration.String(),
		}

		_, err := store.Enqueue(ctx, "default", *jobType, 1, payload)
		if err != nil {
			return fmt.Errorf("enqueue job %d: %w", i, err)
		}

		if (i+1)%100 == 0 {
			fmt.Printf("Enqueued %d jobs\n", i+1)
		}
	}

	fmt.Printf("Successfully enqueued %d jobs\n", *count)
	return nil
}
