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
	require.Equal(t, []objectRef{a, b}, store.deleted)
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
