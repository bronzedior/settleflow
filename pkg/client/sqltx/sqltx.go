package sqltx

import (
	"context"
	"database/sql"
)

type SqlTx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type Adapter struct {
	tx SqlTx
}

func Wrap(tx SqlTx) *Adapter {
	return &Adapter{tx: tx}
}

func (a *Adapter) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := a.tx.ExecContext(ctx, sql, args...)
	return err
}