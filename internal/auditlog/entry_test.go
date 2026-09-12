package auditlog

import (
	"bytes"
	"crypto/sha512"
	"testing"
	"time"

	_ "github.com/jdillenkofer/pithos/internal/testing"
)

func TestPrincipalIDHashCoverageStartsAtVersion5(t *testing.T) {
	entry := &Entry{
		Version:      5,
		Timestamp:    time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC),
		Type:         EntryTypeLog,
		PreviousHash: make([]byte, sha512.Size),
		Details: &LogDetails{
			Operation: OpGetObject,
			Phase:     PhaseComplete,
			Actor: ActorDetails{
				CredentialID: "rotated-key",
				PrincipalID:  "stable-principal",
				AuthType:     AuthTypeSigV4Header,
			},
		},
	}

	v5Hash := entry.CalculateHash()
	entry.Details.(*LogDetails).Actor.PrincipalID = "tampered-principal"
	if bytes.Equal(v5Hash, entry.CalculateHash()) {
		t.Fatal("changing a format-5 principal ID must change the entry hash")
	}

	entry.Version = 4
	v4Hash := entry.CalculateHash()
	entry.Details.(*LogDetails).Actor.PrincipalID = "another-principal"
	if !bytes.Equal(v4Hash, entry.CalculateHash()) {
		t.Fatal("principal ID must not change historical format-4 hashes")
	}
}
