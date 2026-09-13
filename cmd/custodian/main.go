package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bronzedior/custodian/internal/migrations"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("custodian failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: custodian <migrate|ping|scan|runs|findings|show>")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		return err
	}
	defer pool.Close()

	switch args[0] {
	case "migrate":
		return migrations.Apply(ctx, pool)
	case "ping":
		return ping(ctx, pool)
	case "scan":
		return scan(ctx, pool)
	case "runs":
		return runs(ctx, pool)
	case "findings":
		return findings(ctx, pool, args[1:])
	case "show":
		if len(args) < 2 {
			return fmt.Errorf("usage: custodian show <fingerprint>")
		}
		return show(ctx, pool, args[1])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://custodian:custodian@localhost:5432/custodian?sslmode=disable"
}
