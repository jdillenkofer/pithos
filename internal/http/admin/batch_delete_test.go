package admin

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type batchDeleteStore struct {
	storage.Storage
	deleted []objectRef
	bypass  bool
	calls   int
}

func (s *batchDeleteStore) DeleteObject(_ context.Context, _ storage.BucketName, k storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	if k.String() == "locked.txt" {
		return nil, storage.ErrObjectLockAccessDenied
	}
	ref := objectRef{Key: k.String()}
	if opts.VersionID != nil {
		ref.Version = *opts.VersionID
	}
	s.deleted = append(s.deleted, ref)
	s.bypass = s.bypass || opts.BypassGovernanceRetention
	return &storage.DeleteObjectResult{}, nil
}

func (s *batchDeleteStore) DeleteObjects(ctx context.Context, b storage.BucketName, entries []storage.DeleteObjectsInputEntry) (*storage.DeleteObjectsResult, error) {
	s.calls++
	return deleteEach(ctx, b, entries, s.DeleteObject), nil
}

// deleteEach emulates a backend's DeleteObjects with single deletes, reporting
// entries in reverse order to exercise matching by key and version.
func deleteEach(ctx context.Context, b storage.BucketName, entries []storage.DeleteObjectsInputEntry, deleteObject func(context.Context, storage.BucketName, storage.ObjectKey, *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error)) *storage.DeleteObjectsResult {
	result := &storage.DeleteObjectsResult{}
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		row := storage.DeleteObjectsEntry{Key: entry.Key, VersionID: entry.VersionID, Deleted: true}
		if _, err := deleteObject(ctx, b, entry.Key, &storage.DeleteObjectOptions{VersionID: entry.VersionID, BypassGovernanceRetention: entry.BypassGovernanceRetention}); err != nil {
			row.Deleted, row.ErrCode, row.ErrMsg = false, "AccessDenied", err.Error()
		}
		result.Entries = append(result.Entries, row)
	}
	return result
}

func TestBatchObjectDeletion(t *testing.T) {
	store := &batchDeleteStore{}
	a, b, locked := objectRef{Key: "docs/a.txt"}, objectRef{Key: "b.txt"}, objectRef{Key: "locked.txt"}
	query := url.Values{"bucket": {"bucket"}, "object": {a.Value(), b.Value(), a.Value(), locked.Value()}}
	confirm := versionsTestRequest(t, store, http.MethodGet, "/objects/delete-objects?"+query.Encode(), nil)
	require.Equal(t, http.StatusOK, confirm.Code, confirm.Body.String())
	require.Contains(t, confirm.Body.String(), "Specified objects <span class=\"count\">(3)</span>", "duplicates collapse")
	require.Contains(t, confirm.Body.String(), `placeholder="delete"`)

	form := url.Values{"bucket": {"bucket"}, "object": {a.Value(), b.Value(), locked.Value()}}
	require.Equal(t, http.StatusBadRequest, versionsTestRequest(t, store, http.MethodPost, "/objects/delete-objects", form).Code)
	require.Empty(t, store.deleted, "nothing is deleted without the typed confirmation")

	form.Set("confirm_text", "delete")
	result := versionsTestRequest(t, store, http.MethodPost, "/objects/delete-objects", form)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	require.ElementsMatch(t, []objectRef{a, b}, store.deleted)
	require.Equal(t, 1, store.calls, "one DeleteObjects call per request")
	require.False(t, store.bypass)
	require.Contains(t, result.Body.String(), "Successfully deleted 2 of 3 objects.")
	require.Contains(t, result.Body.String(), "Object retention or legal hold prevents this operation")
}

func TestBatchVersionDeletionNeedsPermanentConfirmation(t *testing.T) {
	store := &batchDeleteStore{}
	version := objectRef{Key: "a.txt", Version: "v1"}
	form := url.Values{"bucket": {"bucket"}, "object": {version.Value()}, "confirm_text": {"delete"}}
	require.Equal(t, http.StatusBadRequest, versionsTestRequest(t, store, http.MethodPost, "/objects/delete-objects", form).Code)
	form.Set("confirm_text", "permanently delete")
	form.Set("bypass_governance", "yes")
	result := versionsTestRequest(t, store, http.MethodPost, "/objects/delete-objects", form)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	require.Equal(t, []objectRef{version}, store.deleted)
	require.True(t, store.bypass)
}

func TestSelectedObjectsRejectsInvalidReferences(t *testing.T) {
	for _, value := range []string{"not json", `{"k":""}`, `{"k":"a","v":"` + string(make([]byte, 1100)) + `"}`} {
		_, ok := selectedObjects(url.Values{"object": {value}})
		require.False(t, ok, value)
	}
	_, ok := selectedObjects(url.Values{})
	require.False(t, ok)
}
