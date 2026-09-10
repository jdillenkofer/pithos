package audit

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type captureSink struct{ entries []*auditlog.Entry }

func (s *captureSink) WriteEntry(e *auditlog.Entry) error {
	s.entries = append(s.entries, e)
	return nil
}
func (s *captureSink) Close() error { return nil }

type mixedDeleteStorage struct{ storage.Storage }

func (s *mixedDeleteStorage) DeleteObjects(ctx context.Context, b storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	retention := &storage.ObjectRetention{Mode: storage.RetentionModeGovernance, RetainUntilDate: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	for i, e := range entries {
		storage.ObserveObjectLock(ctx, storage.ObjectLockObservation{Key: e.Key.String(), VersionID: e.VersionID, Effective: storage.ObjectLock{Retention: retention}, BypassUsed: i == 1})
	}
	// Remote backends can return results in a different order.
	return &storage.DeleteObjectsResult{Entries: []storage.DeleteObjectsEntry{
		{Key: entries[1].Key, VersionID: entries[1].VersionID, Deleted: true},
		{Key: entries[0].Key, VersionID: entries[0].VersionID, ErrCode: "AccessDenied", ErrMsg: "protected"},
	}}, nil
}
func TestAuditMultiDeleteVersionsAndBypass(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sink := &captureSink{}
	middleware := NewAuditLogMiddleware(&mixedDeleteStorage{}, sink, signing.NewEd25519Signer(priv), nil, nil, nil)
	first, second := "protected-version", "bypassed-version"
	key := storage.MustNewObjectKey("same-key")
	_, err = middleware.DeleteObjects(t.Context(), storage.MustNewBucketName("bucket"), []storage.DeleteObjectsInputEntry{{Key: key, VersionID: &first}, {Key: key, VersionID: &second, BypassGovernanceRetention: true}})
	require.NoError(t, err)
	var completed []*auditlog.LogDetails
	for _, entry := range sink.entries {
		require.True(t, entry.Verify(signing.NewEd25519Verifier(pub)))
		if details, ok := entry.Details.(*auditlog.LogDetails); ok && details.Phase == auditlog.PhaseComplete {
			completed = append(completed, details)
		}
	}
	require.Len(t, completed, 2)
	require.Equal(t, first, completed[0].Resource.VersionID)
	require.Equal(t, auditlog.OutcomeDenied, completed[0].Outcome.Outcome)
	require.False(t, completed[0].ObjectLock.BypassUsed)
	require.Equal(t, second, completed[1].Resource.VersionID)
	require.Equal(t, auditlog.OutcomeSuccess, completed[1].Outcome.Outcome)
	require.True(t, completed[1].ObjectLock.BypassRequested)
	require.True(t, completed[1].ObjectLock.BypassAuthorized)
	require.True(t, completed[1].ObjectLock.BypassUsed)
	require.Equal(t, "GOVERNANCE", completed[1].ObjectLock.Effective.Mode)
}
