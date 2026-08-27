package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/bronzedior/settleflow/internal/lifecycle"
)

type Reaper struct {
	lifecycle.ComponentBase
	store     *Store
	logger    *slog.Logger
	interval  time.Duration
	threshold time.Duration
	batchSize int
	stopCh    chan struct{}
	doneCh    chan struct{}
}

func NewReaper(store *Store, threshold time.Duration, interval time.Duration, logger *slog.Logger) *Reaper {
	return &Reaper{
		ComponentBase: lifecycle.NewComponentBase("reaper"),
		store:         store,
		logger:        logger,
		interval:      interval,
		threshold:     threshold,
		batchSize:     100,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (r *Reaper) Start(ctx context.Context) error {
	r.logger.Info("Reaper started", "threshold", r.threshold, "interval", r.interval)
	go r.run(ctx)
	return nil
}

func (r *Reaper) Stop(ctx context.Context) error {
	r.logger.Info("Reaper stopping")
	close(r.stopCh)

	select {
	case <-r.doneCh:
		r.logger.Info("Reaper stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reaper) run(ctx context.Context) {
	defer close(r.doneCh)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for{
		select {
		case <-ctx.Done():
			r.logger.Debug("Reaper context cancelled")
			return
		case <-r.stopCh:
			r.logger.Debug("Reaper stop signal received")
			return
		case <-ticker.C:
			r.reapOnce(ctx)
		}
	}
}

func (r *Reaper) reapOnce(ctx context.Context) {
	jobs, err := r.store.ReclaimStaleJobs(ctx, r.threshold, r.batchSize)
	if err != nil {
		r.logger.Error("Reap failed", "err", err)
		return
	}

	if len(jobs) > 0 {
		r.logger.Info("Reclaimed stale jobs", "count", len(jobs))
	}
}
