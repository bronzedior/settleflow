package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/bronzedior/settleflow/internal/lifecycle"
)

type Heartbeat struct {
	lifecycle.ComponentBase
	pool     *Pool
	store    *Store
	logger   *slog.Logger
	interval time.Duration
	stopCh   chan struct{}
	doneCh   chan struct{}
}

func NewHeartbeat(pool *Pool, store *Store, interval time.Duration, logger *slog.Logger) *Heartbeat {
	return &Heartbeat{
		ComponentBase: lifecycle.NewComponentBase("heartbeat"),
		pool:          pool,
		store:         store,
		logger:        logger,
		interval:      interval,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

func (h *Heartbeat) Start(ctx context.Context) error {
	h.logger.Info("Heartbeat started", "interval", h.interval)
	go h.run(ctx)
	return nil
}

func (h *Heartbeat) Stop(ctx context.Context) error {
	h.logger.Info("Heartbeat stopping")
	close(h.stopCh)

	select {
	case <-h.doneCh:
		h.logger.Info("Heartbeat stopped")
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Heartbeat) run(ctx context.Context) {
	defer close(h.doneCh)

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			h.logger.Debug("Heartbeat context cancelled")
			return
		case <-h.stopCh:
			h.logger.Debug("Heartbeat stop signal received")
			return
		case <-ticker.C:
			h.beat(ctx)
		}
	}
}

func (h *Heartbeat) beat(ctx context.Context) {
	ids, checkpoints := h.pool.GetActiveJobsForHeartbeat()

	if len(ids) == 0 {
		return
	}

	err := h.store.UpdateHeartbeat(ctx, h.pool.config.WorkerID, ids, checkpoints)
	if err != nil {
		h.logger.Error("Heartbeat update failed", "jobCount", len(ids), "err", err)
		return
	}

	h.logger.Debug("Heartbeat updated", "jobCount", len(ids))
}
