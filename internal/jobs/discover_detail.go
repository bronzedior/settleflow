package jobs

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/bronzedior/custodian/internal/graph"
	"github.com/bronzedior/custodian/internal/inventory"
)

type DiscoverDetailWorker struct {
	river.WorkerDefaults[DiscoverDetailArgs]
	DB    *pgxpool.Pool
	Graph *graph.Client
}

func (w *DiscoverDetailWorker) Work(ctx context.Context, job *river.Job[DiscoverDetailArgs]) error {
	a := job.Args

	obj, err := w.Graph.GetDetail(ctx, a.Resource, a.ObjectID)
	var apiErr *graph.APIError
	if errors.As(err, &apiErr) && apiErr.NotFound() {
		return w.completeOnly(ctx, a)
	}
	if err != nil {
		return err
	}

	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	identityID, err := inventory.UpsertIdentity(ctx, tx, a.ObjectKind, obj)
	if err != nil {
		return err
	}
	if err := inventory.WriteSnapshot(ctx, tx, a.ScanRunID, identityID, obj); err != nil {
		return err
	}
	if err := inventory.WriteCredentials(ctx, tx, a.ScanRunID, identityID, obj); err != nil {
		return err
	}
	if err := inventory.CompleteTask(ctx, tx, a.ScanRunID, TaskKey(a)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *DiscoverDetailWorker) completeOnly(ctx context.Context, a DiscoverDetailArgs) error {
	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := inventory.CompleteTask(ctx, tx, a.ScanRunID, TaskKey(a)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
