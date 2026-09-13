package rules

import "time"

const (
	expiringWindow = 30 * 24 * time.Hour
	longLivedSpan  = 180 * 24 * time.Hour
)

type CredentialExpired struct{}

func (CredentialExpired) ID() string         { return "CREDENTIAL_EXPIRED" }
func (CredentialExpired) Severity() Severity { return Medium }

func (CredentialExpired) Eval(s *Snapshot, emit func(Finding)) {
	for _, id := range s.Identities {
		for _, c := range s.Credentials[id.ID] {
			if c.NotAfter == nil || !c.NotAfter.Before(s.Now) {
				continue
			}
			emit(Finding{
				IdentityID:    id.ID,
				ExternalID:    id.ExternalID,
				Discriminator: c.KeyID,
				Evidence:      map[string]any{"not_after": c.NotAfter, "kind": c.Kind},
			})
		}
	}
}

type CredentialExpiring struct{}

func (CredentialExpiring) ID() string         { return "CREDENTIAL_EXPIRING" }
func (CredentialExpiring) Severity() Severity { return High }

func (CredentialExpiring) Eval(s *Snapshot, emit func(Finding)) {
	threshold := s.Now.Add(expiringWindow)
	for _, id := range s.Identities {
		for _, c := range s.Credentials[id.ID] {
			if c.NotAfter == nil || c.NotAfter.Before(s.Now) || !c.NotAfter.Before(threshold) {
				continue
			}
			emit(Finding{
				IdentityID:    id.ID,
				ExternalID:    id.ExternalID,
				Discriminator: c.KeyID,
				Evidence:      map[string]any{"not_after": c.NotAfter, "kind": c.Kind},
			})
		}
	}
}

type CredentialLongLived struct{}

func (CredentialLongLived) ID() string         { return "CREDENTIAL_LONG_LIVED" }
func (CredentialLongLived) Severity() Severity { return Medium }

func (CredentialLongLived) Eval(s *Snapshot, emit func(Finding)) {
	for _, id := range s.Identities {
		for _, c := range s.Credentials[id.ID] {
			if c.NotBefore == nil || c.NotAfter == nil {
				continue
			}
			if c.NotAfter.Sub(*c.NotBefore) <= longLivedSpan {
				continue
			}
			emit(Finding{
				IdentityID:    id.ID,
				ExternalID:    id.ExternalID,
				Discriminator: c.KeyID,
				Evidence:      map[string]any{"not_before": c.NotBefore, "not_after": c.NotAfter, "kind": c.Kind},
			})
		}
	}
}
