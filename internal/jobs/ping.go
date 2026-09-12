package jobs

import (
	"context"
	"log/slog"

	"github.com/riverqueue/river"
)

type PingArgs struct {
	Message string `json:"message"`
}

func (PingArgs) Kind() string { return "ping" }

type PingWorker struct {
	river.WorkerDefaults[PingArgs]
}

func (w *PingWorker) Work(ctx context.Context, job *river.Job[PingArgs]) error {
	slog.InfoContext(ctx, "pong", "message", job.Args.Message, "attempt", job.Attempt)
	return nil
}
