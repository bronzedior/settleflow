package migrations

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

var migrations embed.FS

func RunMigrations(ctx context.Context, conn *pgx.Conn) error {
	entries, err := migrations.ReadDir(".")
	if err != nil {
		return fmt.Errorf("reading migrations: %w", err)
	}

	var upMigrations []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), "_up.sql") {
			upMigrations = append(upMigrations, e.Name())
		}
	}

	if len(upMigrations) == 0 {
		return nil
	}

	for _, name := range upMigrations {
		sql, err := migrations.ReadFile(name)
		if err != nil {
			return fmt.Errorf("reading migration %s: %w", name, err)
		}

		if _, err := conn.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("executing migration %s: %w", name, err)
		}
	}

	return nil
}
