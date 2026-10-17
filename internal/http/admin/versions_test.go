package admin

import (
	"context"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type versionsTestStore struct {
	storage.Storage
	deleted []string
	bypass  []bool
}

func (s *versionsTestStore) ListObjectVersions(_ context.Context, _ storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	return &storage.ListObjectVersionsResult{Versions: []storage.ObjectVersion{
		{Key: storage.MustNewObjectKey("doc.txt"), VersionID: "v3", IsLatest: true, IsDeleteMarker: true},
		{Key: storage.MustNewObjectKey("doc.txt"), VersionID: "v2", Size: 2048},
		{Key: storage.MustNewObjectKey("doc.txt"), VersionID: "locked", Size: 1},
		{Key: storage.MustNewObjectKey("doc.txt.bak"), VersionID: "other"},
	}}, nil
}

func (s *versionsTestStore) DeleteObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	if *opts.VersionID == "locked" {
		return nil, errors.Join(storage.ErrObjectLockAccessDenied, errors.New("postgres://user:SECRET@db"))
	}
	s.deleted = append(s.deleted, *opts.VersionID)
	s.bypass = append(s.bypass, opts.BypassGovernanceRetention)
	return &storage.DeleteObjectResult{}, nil
}

func versionsTestRequest(t *testing.T, store storage.Storage, method, target string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	pages, err := template.ParseFS(assets, "templates/*.html")
	require.NoError(t, err)
	app := &App{options: Options{Storage: store}, pages: pages}
	mux := http.NewServeMux()
	app.registerObjects(mux)
	request := httptest.NewRequest(method, target, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request = request.WithContext(context.WithValue(request.Context(), stateKey{}, requestState{Session: session.Record{CSRFToken: "csrf"}}))
	if method == http.MethodPost {
		require.NoError(t, request.ParseForm())
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func TestObjectVersionsListAndDelete(t *testing.T) {
	store := &versionsTestStore{}
	page := versionsTestRequest(t, store, http.MethodGet, "/objects/versions?bucket=bucket&key=doc.txt", nil)
	require.Equal(t, http.StatusOK, page.Code, page.Body.String())
	for _, version := range []string{"v3", "v2", "locked"} {
		require.Contains(t, page.Body.String(), `name="version" value="`+version+`"`)
	}
	require.NotContains(t, page.Body.String(), "other", "versions of other keys sharing the prefix are excluded")
	require.Contains(t, page.Body.String(), "Delete marker")
	require.Contains(t, page.Body.String(), "2.0 KiB")

	confirm := versionsTestRequest(t, store, http.MethodGet, "/objects/delete-versions?bucket=bucket&key=doc.txt&version=v2&version=v3&version=v2", nil)
	require.Equal(t, http.StatusOK, confirm.Code, confirm.Body.String())
	require.Equal(t, 2, strings.Count(confirm.Body.String(), `type="hidden" name="version"`), "duplicate selections collapse")
	require.Contains(t, confirm.Body.String(), `name="csrf_token" value="csrf"`)
	require.Equal(t, http.StatusBadRequest, versionsTestRequest(t, store, http.MethodGet, "/objects/delete-versions?bucket=bucket&key=doc.txt", nil).Code)

	form := url.Values{"bucket": {"bucket"}, "key": {"doc.txt"}, "version": {"v2", "v3", "locked"}}
	require.Equal(t, http.StatusBadRequest, versionsTestRequest(t, store, http.MethodPost, "/objects/delete-versions", form).Code)
	require.Empty(t, store.deleted, "nothing is deleted without the typed confirmation")

	form.Set("confirm_text", "permanently delete")
	result := versionsTestRequest(t, store, http.MethodPost, "/objects/delete-versions", form)
	require.Equal(t, http.StatusOK, result.Code, result.Body.String())
	require.Equal(t, []string{"v2", "v3"}, store.deleted)
	require.Equal(t, []bool{false, false}, store.bypass)
	require.Contains(t, result.Body.String(), "Deleted 2 of 3 versions.")
	require.Contains(t, result.Body.String(), "Object retention or legal hold prevents this operation")
	require.NotContains(t, result.Body.String(), "SECRET")
}

func TestObjectDeleteRequiresConfirmation(t *testing.T) {
	store := &versionsTestStore{}
	form := url.Values{"bucket": {"bucket"}, "key": {"doc.txt"}, "version": {"v2"}, "note": {`"><script>alert(1)</script>`}}
	page := versionsTestRequest(t, store, http.MethodPost, "/objects/delete", form)
	require.Equal(t, http.StatusOK, page.Code, page.Body.String())
	require.Empty(t, store.deleted)
	require.Contains(t, page.Body.String(), "Permanently delete version v2?")
	require.Contains(t, page.Body.String(), `name="version" value="v2"`)
	require.Contains(t, page.Body.String(), `name="confirm" value="yes"`)
	require.NotContains(t, page.Body.String(), "<script>alert")

	form.Set("confirm", "yes")
	done := versionsTestRequest(t, store, http.MethodPost, "/objects/delete", form)
	require.Equal(t, http.StatusSeeOther, done.Code, done.Body.String())
	require.Equal(t, []string{"v2"}, store.deleted)
}
