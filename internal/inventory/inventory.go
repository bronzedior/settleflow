package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func marshalRaw(obj map[string]any) ([]byte, error) {
	return json.Marshal(obj)
}

func UpsertIdentity(ctx context.Context, tx pgx.Tx, kind string, obj map[string]any) (uuid.UUID, error) {
	externalID, _ := obj["id"].(string)
	if externalID == "" {
		return uuid.Nil, fmt.Errorf("inventory: object has no id")
	}
	displayName, _ := obj["displayName"].(string)
	appID, _ := obj["appId"].(string)

	var id uuid.UUID
	err := tx.QueryRow(ctx, `
		INSERT INTO identities (source, external_id, kind, display_name, app_id, last_seen_at)
		VALUES ('entra', $1, $2, $3, $4, now())
		ON CONFLICT (source, external_id) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			app_id       = EXCLUDED.app_id,
			last_seen_at = now()
		RETURNING id`,
		externalID, kind, displayName, nullIfEmpty(appID),
	).Scan(&id)
	return id, err
}

func ClaimTask(ctx context.Context, tx pgx.Tx, scanRunID uuid.UUID, taskKey string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO scan_tasks (scan_run_id, task_key) VALUES ($1, $2)
		ON CONFLICT (scan_run_id, task_key) DO NOTHING`,
		scanRunID, taskKey)
	return err
}

func CompleteTask(ctx context.Context, tx pgx.Tx, scanRunID uuid.UUID, taskKey string) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO scan_tasks (scan_run_id, task_key, state, updated_at)
		VALUES ($1, $2, 'done', now())
		ON CONFLICT (scan_run_id, task_key)
		DO UPDATE SET state = 'done', updated_at = now()`,
		scanRunID, taskKey)
	return err
}

func WriteSnapshot(ctx context.Context, tx pgx.Tx, scanRunID, identityID uuid.UUID, obj map[string]any) error {
	accountEnabled, _ := obj["accountEnabled"].(bool)
	owners, _ := obj["owners"].([]any)

	raw, err := marshalRaw(obj)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO identity_snapshots (scan_run_id, identity_id, account_enabled, owner_count, raw)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (scan_run_id, identity_id) DO UPDATE SET
			account_enabled = EXCLUDED.account_enabled,
			owner_count     = EXCLUDED.owner_count,
			raw             = EXCLUDED.raw`,
		scanRunID, identityID, accountEnabled, len(owners), raw)
	return err
}

func WriteCredentials(ctx context.Context, tx pgx.Tx, scanRunID, identityID uuid.UUID, obj map[string]any) error {
	for _, c := range asSlice(obj["passwordCredentials"]) {
		if err := upsertCredential(ctx, tx, scanRunID, identityID, "password", c); err != nil {
			return err
		}
	}
	for _, c := range asSlice(obj["keyCredentials"]) {
		if err := upsertCredential(ctx, tx, scanRunID, identityID, "certificate", c); err != nil {
			return err
		}
	}
	for _, c := range asSlice(obj["federatedIdentityCredentials"]) {
		if err := upsertCredential(ctx, tx, scanRunID, identityID, "federated", c); err != nil {
			return err
		}
	}
	return nil
}

func upsertCredential(ctx context.Context, tx pgx.Tx, scanRunID, identityID uuid.UUID, kind string, c map[string]any) error {
	keyID, _ := c["keyId"].(string)
	if keyID == "" {
		keyID, _ = c["id"].(string)
	}
	displayName, _ := c["displayName"].(string)
	notBefore := parseTime(c["startDateTime"])
	notAfter := parseTime(c["endDateTime"])
	subject, _ := c["subject"].(string)
	issuer, _ := c["issuer"].(string)

	_, err := tx.Exec(ctx, `
		INSERT INTO credentials (scan_run_id, identity_id, key_id, kind, display_name, not_before, not_after, subject, issuer)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (scan_run_id, identity_id, key_id) DO UPDATE SET
			display_name = EXCLUDED.display_name,
			not_before   = EXCLUDED.not_before,
			not_after    = EXCLUDED.not_after,
			subject      = EXCLUDED.subject,
			issuer       = EXCLUDED.issuer`,
		scanRunID, identityID, keyID, kind, nullIfEmpty(displayName), notBefore, notAfter, nullIfEmpty(subject), nullIfEmpty(issuer))
	return err
}

func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func asSlice(v any) []map[string]any {
	raw, _ := v.([]any)
	out := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			out = append(out, m)
		}
	}
	return out
}

func parseTime(v any) *time.Time {
	s, _ := v.(string)
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}
