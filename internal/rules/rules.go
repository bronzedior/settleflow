package rules

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

type Severity string

const (
	Critical Severity = "critical"
	High     Severity = "high"
	Medium   Severity = "medium"
	Low      Severity = "low"
	Info     Severity = "info"
)

type CredentialKind string

const (
	Password    CredentialKind = "password"
	Certificate CredentialKind = "certificate"
	Federated   CredentialKind = "federated"
)

type Identity struct {
	ID             uuid.UUID
	ExternalID     string
	Kind           string
	DisplayName    string
	AccountEnabled bool
	OwnerCount     int
}

type Credential struct {
	KeyID     string
	Kind      CredentialKind
	NotBefore *time.Time
	NotAfter  *time.Time
	Subject   string
	Issuer    string
}

type Privilege struct {
	System string
	Role   string
	Scope  string
}

type Capability struct {
	SignInActivity bool
	DeclaredState  bool
}

type Snapshot struct {
	ScanRunID   uuid.UUID
	Now         time.Time
	Identities  []Identity
	Credentials map[uuid.UUID][]Credential
	Privileges  map[uuid.UUID][]Privilege
	Declared    map[string]string
	Capability  Capability
}

type Finding struct {
	RuleID        string
	Severity      Severity
	IdentityID    uuid.UUID
	ExternalID    string
	Discriminator string
	Evidence      map[string]any
}

type Rule interface {
	ID() string
	Severity() Severity
	Eval(s *Snapshot, emit func(Finding))
}

func Fingerprint(ruleID, source, externalID, discriminator string) string {
	h := sha256.Sum256([]byte(ruleID + "\x00" + source + "\x00" + externalID + "\x00" + discriminator))
	return hex.EncodeToString(h[:])
}

func Phase1() []Rule {
	return []Rule{
		CredentialExpired{},
		CredentialExpiring{},
		CredentialLongLived{},
		IdentityOrphaned{},
	}
}

func Evaluate(s *Snapshot, rs []Rule) []Finding {
	var findings []Finding
	for _, r := range rs {
		r.Eval(s, func(f Finding) {
			f.RuleID = r.ID()
			f.Severity = r.Severity()
			findings = append(findings, f)
		})
	}
	return findings
}
