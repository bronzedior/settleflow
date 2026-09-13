package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/bronzedior/custodian/internal/rules"
)

func marshalEvidence(evidence map[string]any) ([]byte, error) {
	return json.Marshal(evidence)
}

type EvaluateScanRunWorker struct {
	river.WorkerDefaults[EvaluateScanRunArgs]
	DB *pgxpool.Pool
}

func (w *EvaluateScanRunWorker) NextRetry(job *river.Job[EvaluateScanRunArgs]) time.Time {
	return time.Now().Add(2 * time.Second)
}

func (w *EvaluateScanRunWorker) Work(ctx context.Context, job *river.Job[EvaluateScanRunArgs]) error {
	scanRunID := job.Args.ScanRunID

	var pending int
	err := w.DB.QueryRow(ctx,
		`SELECT count(*) FROM scan_tasks WHERE scan_run_id = $1 AND state = 'pending'`,
		scanRunID).Scan(&pending)
	if err != nil {
		return err
	}
	if pending > 0 {
		return fmt.Errorf("barrier not met: %d tasks pending", pending)
	}

	snapshot, err := rules.Load(ctx, w.DB, scanRunID)
	if err != nil {
		return err
	}
	findings := rules.Evaluate(snapshot, rules.Phase1())

	tx, err := w.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, f := range findings {
		fingerprint := rules.Fingerprint(f.RuleID, "entra", f.ExternalID, f.Discriminator)
		evidence, err := marshalEvidence(f.Evidence)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO findings (scan_run_id, identity_id, rule_id, severity, fingerprint, evidence)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (scan_run_id, fingerprint) DO NOTHING`,
			scanRunID, f.IdentityID, f.RuleID, string(f.Severity), fingerprint, evidence,
		); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx, `
		UPDATE scan_runs SET state = 'complete', finished_at = now() WHERE id = $1`,
		scanRunID,
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
