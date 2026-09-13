package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"

	"github.com/bronzedior/custodian/internal/inventory"
	"github.com/bronzedior/custodian/internal/jobs"
)

func ping(ctx context.Context, pool *pgxpool.Pool) error {
	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		return err
	}
	result, err := riverClient.Insert(ctx, jobs.PingArgs{Message: "ping"}, nil)
	if err != nil {
		return err
	}
	fmt.Println(result.Job.ID)
	return nil
}

var discoveryResources = []struct{ resource, kind string }{
	{"applications", "application"},
	{"servicePrincipals", "service_principal"},
}

func scan(ctx context.Context, pool *pgxpool.Pool) error {
	tenantID := os.Getenv("AZURE_TENANT_ID")
	if tenantID == "" {
		tenantID = "unknown"
	}

	riverClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{})
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var scanRunID uuid.UUID
	if err := tx.QueryRow(ctx,
		`INSERT INTO scan_runs (tenant_id) VALUES ($1) RETURNING id`, tenantID,
	).Scan(&scanRunID); err != nil {
		return err
	}

	for _, r := range discoveryResources {
		page := jobs.DiscoverPageArgs{ScanRunID: scanRunID, Resource: r.resource, ObjectKind: r.kind}
		if err := inventory.ClaimTask(ctx, tx, scanRunID, jobs.TaskKey(page)); err != nil {
			return err
		}
		if _, err := riverClient.InsertTx(ctx, tx, page, &river.InsertOpts{
			UniqueOpts: river.UniqueOpts{ByArgs: true},
		}); err != nil {
			return err
		}
	}

	if _, err := riverClient.InsertTx(ctx, tx, jobs.EvaluateScanRunArgs{ScanRunID: scanRunID}, &river.InsertOpts{
		ScheduledAt: time.Now().Add(30 * time.Second),
	}); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	fmt.Println(scanRunID)
	return nil
}

func runs(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT id, tenant_id, state, started_at, finished_at
		FROM scan_runs ORDER BY started_at DESC LIMIT 20`)
	if err != nil {
		return err
	}
	defer rows.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "ID\tTENANT\tSTATE\tSTARTED\tFINISHED")
	for rows.Next() {
		var id uuid.UUID
		var tenantID, state string
		var startedAt string
		var finishedAt *string
		if err := rows.Scan(&id, &tenantID, &state, &startedAt, &finishedAt); err != nil {
			return err
		}
		finished := "-"
		if finishedAt != nil {
			finished = *finishedAt
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", id, tenantID, state, startedAt, finished)
	}
	return rows.Err()
}

func findings(ctx context.Context, pool *pgxpool.Pool, args []string) error {
	fs := flag.NewFlagSet("findings", flag.ContinueOnError)
	severity := fs.String("severity", "", "filter by severity (critical|high|medium|low|info)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	sql := `
		SELECT f.fingerprint, f.rule_id, f.severity, i.external_id
		FROM findings f
		JOIN identities i ON i.id = f.identity_id
		LEFT JOIN finding_states fs ON fs.fingerprint = f.fingerprint
		WHERE COALESCE(fs.state, 'open') = 'open'`
	var queryArgs []any
	if *severity != "" {
		sql += ` AND f.severity = $1`
		queryArgs = append(queryArgs, *severity)
	}
	sql += ` ORDER BY f.severity, f.created_at DESC`

	rows, err := pool.Query(ctx, sql, queryArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()
	fmt.Fprintln(w, "FINGERPRINT\tRULE\tSEVERITY\tIDENTITY")
	for rows.Next() {
		var fingerprint, ruleID, sev, externalID string
		if err := rows.Scan(&fingerprint, &ruleID, &sev, &externalID); err != nil {
			return err
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", fingerprint, ruleID, sev, externalID)
	}
	return rows.Err()
}

func show(ctx context.Context, pool *pgxpool.Pool, fingerprint string) error {
	var ruleID, sev, externalID string
	var evidence []byte
	err := pool.QueryRow(ctx, `
		SELECT f.rule_id, f.severity, i.external_id, f.evidence
		FROM findings f
		JOIN identities i ON i.id = f.identity_id
		WHERE f.fingerprint = $1
		ORDER BY f.created_at DESC LIMIT 1`, fingerprint,
	).Scan(&ruleID, &sev, &externalID, &evidence)
	if err != nil {
		return err
	}

	var pretty map[string]any
	if err := json.Unmarshal(evidence, &pretty); err != nil {
		return err
	}
	out, err := json.MarshalIndent(map[string]any{
		"rule":     ruleID,
		"severity": sev,
		"identity": externalID,
		"evidence": pretty,
	}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
