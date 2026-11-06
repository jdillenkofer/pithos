package audit

import (
	"bytes"
	"crypto/ed25519"
	"crypto/mldsa"
	"testing"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/auditlog/sink"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	"github.com/stretchr/testify/require"
)

func TestAdminEventsThroughOuterWrappersShareVerifiedChain(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	ml, err := mldsa.GenerateKey(mldsa.MLDSA87())
	require.NoError(t, err)
	var buf bytes.Buffer
	writer := sink.NewWriterSink(&buf, &serialization.JsonSerializer{})
	middleware := NewAuditLogMiddleware(&mockStorage{}, writer, signing.NewEd25519Signer(priv), signing.NewMlDsa87Signer(ml), nil, nil)
	inner := delegator.Wrap(middleware)
	outer := delegator.Wrap(&inner)
	ctx := auditlog.WithAdminActor(t.Context(), auditlog.ActorDetails{CredentialID: "admin-key", AccountID: "account", PrincipalID: "admin-principal", AuthType: "admin-session"})
	// Include a full block so administrative events exercise grounding as well.
	for range auditlog.GroundingBlockSize {
		outer.RecordAdminEvent(ctx, "AdminVerifyAuditLog")
	}
	validator := auditlog.NewValidator(signing.NewEd25519Verifier(pub), signing.NewMlDsa87Verifier(ml.PublicKey()))
	decoder := (&serialization.JsonSerializer{}).NewDecoder(&buf)
	for index := 0; index < auditlog.GroundingBlockSize+2; index++ {
		entry, err := decoder.Decode()
		require.NoError(t, err)
		require.NoError(t, validator.ValidateEntry(entry))
		if entry.Type == auditlog.EntryTypeLog {
			d := entry.Details.(*auditlog.LogDetails)
			require.Equal(t, auditlog.Operation("AdminVerifyAuditLog"), d.Operation)
			require.Equal(t, "admin-principal", d.Actor.PrincipalID)
			require.Equal(t, "admin-key", d.Actor.CredentialID)
			require.Equal(t, "account", d.Actor.AccountID)
			require.Equal(t, auditlog.OutcomeSuccess, d.Outcome.Outcome)
		}
	}
	require.Equal(t, auditlog.GroundingBlockSize+2, validator.Index)
	outer.RecordAdminEvent(auditlog.WithAdminStatus(ctx, 401), "AdminLoginFailed")
	entry, err := decoder.Decode()
	require.NoError(t, err)
	require.NoError(t, validator.ValidateEntry(entry))
	details := entry.Details.(*auditlog.LogDetails)
	require.Equal(t, auditlog.OutcomeDenied, details.Outcome.Outcome)
	require.Equal(t, int32(401), details.Outcome.StatusCode)
}
