package jobs

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/bronzedior/custodian/internal/graph"
	"github.com/bronzedior/custodian/internal/inventory"
)

type DiscoverPageWorker struct {
	river.WorkerDefaults[DiscoverPageArgs]
	DB    *pgxpool.Pool
	Graph *graph.Client
	River *river.Client[pgx.Tx]
}

func (w *DiscoverPageWorker) Work(ctx context.Context, job *river.Job[DiscoverPageArgs]) error {
	a := job.Args

	page, err := w.Graph.ListPage(ctx, a.Resource, a.NextLink)
	if err != nil {
		return err
	}

	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, obj := range page.Value {
		if _, err := inventory.UpsertIdentity(ctx, tx, a.ObjectKind, obj); err != nil {
			return err
		}

		objectID, _ := obj["id"].(string)
		child := DiscoverDetailArgs{ScanRunID: a.ScanRunID, Resource: a.Resource, ObjectKind: a.ObjectKind, ObjectID: objectID}
		if err := inventory.ClaimTask(ctx, tx, a.ScanRunID, TaskKey(child)); err != nil {
			return err
		}
		if _, err := w.River.InsertTx(ctx, tx, child, &river.InsertOpts{
			UniqueOpts: river.UniqueOpts{ByArgs: true},
		}); err != nil {
			return err
		}
	}

	if page.NextLink != "" {
		next := DiscoverPageArgs{ScanRunID: a.ScanRunID, Resource: a.Resource, ObjectKind: a.ObjectKind, NextLink: page.NextLink}
		if err := inventory.ClaimTask(ctx, tx, a.ScanRunID, TaskKey(next)); err != nil {
			return err
		}
		if _, err := w.River.InsertTx(ctx, tx, next, &river.InsertOpts{
			UniqueOpts: river.UniqueOpts{ByArgs: true},
		}); err != nil {
			return err
		}
	}

	if err := inventory.CompleteTask(ctx, tx, a.ScanRunID, TaskKey(a)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("discover.page: commit: %w", err)
	}
	return nil
}
