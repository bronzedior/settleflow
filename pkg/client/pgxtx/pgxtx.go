package pgxtx

import (
	"context"

	"github.com/jackc/pgx/v5/pgconn"
)

type PgxTx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type Adapter struct {
	tx PgxTx
}

func Wrap(tx PgxTx) *Adapter {
	return &Adapter{tx: tx}
}

func (a *Adapter) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := a.tx.Exec(ctx, sql, args...)
	return err
}