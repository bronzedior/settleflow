package migrations

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5/pgxpool"
)

var initialSchema string

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version int PRIMARY KEY)`); err != nil {
		return err
	}

	var applied bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1)`).Scan(&applied); err != nil {
		return err
	}
	if applied {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, initialSchema); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES (1)`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
