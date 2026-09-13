package rules

type IdentityOrphaned struct{}

func (IdentityOrphaned) ID() string         { return "IDENTITY_ORPHANED" }
func (IdentityOrphaned) Severity() Severity { return High }

func (IdentityOrphaned) Eval(s *Snapshot, emit func(Finding)) {
	for _, id := range s.Identities {
		if id.OwnerCount > 0 {
			continue
		}
		emit(Finding{
			IdentityID: id.ID,
			ExternalID: id.ExternalID,
			Evidence:   map[string]any{"owner_count": id.OwnerCount},
		})
	}
}
