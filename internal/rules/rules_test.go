package rules

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPhase1Rules(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	id := uuid.New()

	expiredNotBefore := now.Add(-40 * 24 * time.Hour)
	expiredNotAfter := now.Add(-1 * 24 * time.Hour)

	expiringNotBefore := now.Add(-10 * 24 * time.Hour)
	expiringNotAfter := now.Add(10 * 24 * time.Hour)

	longLivedNotBefore := now.Add(-400 * 24 * time.Hour)
	longLivedNotAfter := now.Add(200 * 24 * time.Hour)

	s := &Snapshot{
		Now: now,
		Identities: []Identity{
			{ID: id, ExternalID: "obj-1", OwnerCount: 0},
		},
		Credentials: map[uuid.UUID][]Credential{
			id: {
				{KeyID: "expired", NotBefore: &expiredNotBefore, NotAfter: &expiredNotAfter},
				{KeyID: "expiring-soon", NotBefore: &expiringNotBefore, NotAfter: &expiringNotAfter},
				{KeyID: "long-lived", NotBefore: &longLivedNotBefore, NotAfter: &longLivedNotAfter},
			},
		},
	}

	findings := Evaluate(s, Phase1())

	byRule := map[string]int{}
	for _, f := range findings {
		byRule[f.RuleID]++
	}

	want := map[string]int{
		"CREDENTIAL_EXPIRED":    1,
		"CREDENTIAL_EXPIRING":   1,
		"CREDENTIAL_LONG_LIVED": 1,
		"IDENTITY_ORPHANED":     1,
	}
	for rule, count := range want {
		if byRule[rule] != count {
			t.Errorf("rule %s: got %d findings, want %d (all: %+v)", rule, byRule[rule], count, byRule)
		}
	}

	for _, f := range findings {
		fp := Fingerprint(f.RuleID, "entra", f.ExternalID, f.Discriminator)
		if fp == "" {
			t.Errorf("empty fingerprint for %s", f.RuleID)
		}
	}
}
