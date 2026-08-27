package queue

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PgxExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, arguments ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, arguments ...any) pgx.Row
}

type pgxAdapter struct {
	executor PgxExecutor
}

func WrapPgx(executor PgxExecutor) Execer {
	return &pgxAdapter{executor: executor}
}

func (a *pgxAdapter) Exec(ctx context.Context, sql string, args ...any) (interface{}, error) {
	tag, err := a.executor.Exec(ctx, sql, args...)
	return tag, err
}

func (a *pgxAdapter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return a.executor.Query(ctx, sql, args...)
}

func (a *pgxAdapter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return a.executor.QueryRow(ctx, sql, args...)
}
