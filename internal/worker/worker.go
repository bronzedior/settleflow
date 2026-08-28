package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/bronzedior/settleflow/internal/lifecycle"
	"github.com/bronzedior/settleflow/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Worker struct {
	lifecycle.ComponentBase
	pool      *queue.Pool
	heartbeat *queue.Heartbeat
	reaper    *queue.Reaper
	handler   queue.Handler
	logger    *slog.Logger
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func NewWorker(
	pool *queue.Pool,
	heartbeat *queue.Heartbeat,
	reaper *queue.Reaper,
	handler queue.Handler,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		ComponentBase: lifecycle.NewComponentBase("worker"),
		pool:          pool,
		heartbeat:     heartbeat,
		reaper:        reaper,
		handler:       handler,
		logger:        logger,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	w.logger.Info("Worker started")
	go w.run(ctx)
	return nil
}

func (w *Worker) Stop(ctx context.Context) error {
	w.logger.Info("Worker stopping")
	w.pool.Drain()
	close(w.stopCh)

	select {
	case <-w.doneCh:
		w.logger.Info("Worker stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) run(ctx context.Context) {
	defer close(w.doneCh)

	ticker := time.NewTicker(w.pool.Config().PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Worker context cancelled")
			return
		case <-w.stopCh:
			w.logger.Debug("Worker stop signal received")
			return
		case <-w.pool.DrainChannel():
			w.logger.Debug("Worker drain requested, stopping claims")
		case <-ticker.C:
			if w.pool.Draining() {
				continue
			}

			jobs, err := w.pool.Claim()
			if err != nil {
				w.logger.Error("Claim failed", "err", err)
				continue
			}

			for _, job := range jobs {
				w.handleJob(ctx, job)
			}
		}
	}
}

func (w *Worker) handleJob(ctx context.Context, job queue.Job) {
	jobCtx, cancel := w.makeJobContext(job)
	defer cancel()

	meta := queue.JobMeta{
		ID:          job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Checkpoint:  job.Checkpoint,
		EnqueuedAt:  job.CreatedAt,
	}

	activeJob := w.pool.RegisterActive(job.ID, meta)
	defer w.pool.UnregisterActive(job.ID)
	defer w.pool.ReleaseSlot()

	w.logger.Info("Handling job", "jobID", job.ID, "jobType", job.JobType, "attempt", job.Attempt)

	err := w.handler(jobCtx, meta)
	if err != nil {
		w.logger.Error("Job handler failed", "jobID", job.ID, "err", err)
		_ = w.pool.Store().FailJob(ctx, job.ID, w.pool.Config().WorkerID, err.Error(), queue.ErrorClassRetryable)
		activeJob.Mu.Lock()
		activeJob.IsDraining = true
		activeJob.Mu.Unlock()
	} else {
		w.logger.Info("Job completed", "jobID", job.ID)
		_ = w.pool.Store().CompleteJob(ctx, job.ID, w.pool.Config().WorkerID)
	}
}

func (w *Worker) makeJobContext(job queue.Job) (context.Context, context.CancelFunc) {
	ctx := w.pool.BaseContext()
	ctx, cancel := context.WithTimeout(ctx, w.pool.Config().JobTimeout)

	ctx = context.WithValue(ctx, jobMetaKey, queue.JobMeta{
		ID:          job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: job.MaxAttempts,
		Checkpoint:  job.Checkpoint,
		EnqueuedAt:  job.CreatedAt,
	})

	isDraining := w.pool.Draining()
	ctx = context.WithValue(ctx, isDrainingKey, isDraining)

	return ctx, cancel
}

var jobMetaKey = struct{}{}
var isDrainingKey = struct{}{}

func CreateWorkerStack(
	pool *pgxpool.Pool,
	config *queue.PoolConfig,
	handler queue.Handler,
	logger *slog.Logger,
) (*lifecycle.Supervisor, error) {
	store := queue.NewStore(queue.WrapPgx(pool))

	jobPool := queue.NewPool(store, config)
	heartbeat := queue.NewHeartbeat(
		jobPool,
		store,
		5*time.Second,
		logger,
	)
	reaper := queue.NewReaper(
		store,
		60*time.Second,
		30*time.Second,
		logger,
	)
	worker := NewWorker(jobPool, heartbeat, reaper, handler, logger)

	supervisor := lifecycle.NewSupervisor(logger)
	supervisor.Register(jobPool)
	supervisor.Register(reaper)
	supervisor.Register(heartbeat)
	supervisor.Register(worker)

	return supervisor, nil
}
