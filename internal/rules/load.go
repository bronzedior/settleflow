package rules

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func Load(ctx context.Context, db Querier, scanRunID uuid.UUID) (*Snapshot, error) {
	s := &Snapshot{
		ScanRunID:   scanRunID,
		Now:         time.Now().UTC(),
		Credentials: map[uuid.UUID][]Credential{},
		Privileges:  map[uuid.UUID][]Privilege{},
		Declared:    map[string]string{},
	}

	rows, err := db.Query(ctx, `
		SELECT i.id, i.external_id, i.kind, i.display_name, s.account_enabled, s.owner_count
		FROM identity_snapshots s
		JOIN identities i ON i.id = s.identity_id
		WHERE s.scan_run_id = $1`, scanRunID)
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer rows.Close()
		for rows.Next() {
			var id Identity
			if err := rows.Scan(&id.ID, &id.ExternalID, &id.Kind, &id.DisplayName, &id.AccountEnabled, &id.OwnerCount); err != nil {
				return err
			}
			s.Identities = append(s.Identities, id)
		}
		return rows.Err()
	}()
	if err != nil {
		return nil, err
	}

	credRows, err := db.Query(ctx, `
		SELECT identity_id, key_id, kind, not_before, not_after, subject, issuer
		FROM credentials WHERE scan_run_id = $1`, scanRunID)
	if err != nil {
		return nil, err
	}
	err = func() error {
		defer credRows.Close()
		for credRows.Next() {
			var identityID uuid.UUID
			var c Credential
			var subject, issuer *string
			if err := credRows.Scan(&identityID, &c.KeyID, &c.Kind, &c.NotBefore, &c.NotAfter, &subject, &issuer); err != nil {
				return err
			}
			if subject != nil {
				c.Subject = *subject
			}
			if issuer != nil {
				c.Issuer = *issuer
			}
			s.Credentials[identityID] = append(s.Credentials[identityID], c)
		}
		return credRows.Err()
	}()
	if err != nil {
		return nil, err
	}

	return s, nil
}

type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}
