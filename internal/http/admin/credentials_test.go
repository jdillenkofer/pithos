package admin

import (
	"context"
	dbsql "database/sql"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	sqlcredentials "github.com/jdillenkofer/pithos/internal/http/server/authentication/sql"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/stretchr/testify/require"
)

type credentialManagementFake struct {
	credentials   map[string]authentication.Credential
	mutationError error
	deletes       int
}

func (p *credentialManagementFake) Lookup(_ context.Context, key string) (authentication.Credential, bool, error) {
	c, ok := p.credentials[key]
	return c, ok, nil
}
func (p *credentialManagementFake) List(context.Context) ([]authentication.CredentialInfo, error) {
	return authentication.CredentialMetadata(p.credentials), nil
}
func (p *credentialManagementFake) Create(_ context.Context, c authentication.Credential) error {
	if p.mutationError != nil {
		return p.mutationError
	}
	if _, exists := p.credentials[c.AccessKeyID]; exists {
		return authentication.ErrCredentialExists
	}
	p.credentials[c.AccessKeyID] = c
	return nil
}
func (p *credentialManagementFake) Delete(_ context.Context, key string) error {
	p.deletes++
	if p.mutationError != nil {
		return p.mutationError
	}
	if _, exists := p.credentials[key]; !exists {
		return authentication.ErrCredentialNotFound
	}
	delete(p.credentials, key)
	return nil
}

type credentialLookupOnlyFake struct{}

func (credentialLookupOnlyFake) Lookup(context.Context, string) (authentication.Credential, bool, error) {
	return authentication.Credential{}, false, nil
}

func credentialTestHandler(t *testing.T, provider authentication.CredentialProvider) http.Handler {
	t.Helper()
	pages, err := template.ParseFiles("templates/credentials.html", "templates/credentials-created.html", "templates/confirm.html")
	require.NoError(t, err)
	_, err = pages.Parse(`{{define "layout"}}{{.Content}}{{end}}`)
	require.NoError(t, err)
	app := &App{options: Options{CredentialProvider: provider}, pages: pages}
	mux := http.NewServeMux()
	app.registerCredentials(mux)
	return mux
}

func credentialTestRequest(method, path string, values url.Values) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(values.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req.WithContext(context.WithValue(req.Context(), stateKey{}, requestState{
		Credential: authentication.Credential{AccessKeyID: "current"},
		Session:    session.Record{CSRFToken: "csrf-test-token"},
	}))
}

func TestCredentialHandlersCreateListDelete(t *testing.T) {
	provider := &credentialManagementFake{credentials: make(map[string]authentication.Credential)}
	handler := credentialTestHandler(t, provider)
	form := url.Values{"accessKeyId": {"new-key"}, "accountId": {"account"}, "principalId": {"principal"}, "secretAccessKey": {"client-chosen-secret"}}
	created := httptest.NewRecorder()
	handler.ServeHTTP(created, credentialTestRequest("POST", "/credentials/create", form))
	require.Equal(t, http.StatusOK, created.Code)
	secret := provider.credentials["new-key"].SecretAccessKey
	require.Len(t, secret, 43)
	require.NotEqual(t, "client-chosen-secret", secret)
	require.Contains(t, created.Body.String(), secret)
	require.Equal(t, 1, strings.Count(created.Body.String(), secret))
	require.Equal(t, "no-store", created.Header().Get("Cache-Control"))
	listed := httptest.NewRecorder()
	handler.ServeHTTP(listed, credentialTestRequest("GET", "/credentials", nil))
	require.Equal(t, http.StatusOK, listed.Code)
	require.NotContains(t, listed.Body.String(), secret)
	require.Contains(t, listed.Body.String(), `name="csrf_token" value="csrf-test-token"`)
	repeated := httptest.NewRecorder()
	handler.ServeHTTP(repeated, credentialTestRequest("POST", "/credentials/create", form))
	require.Equal(t, http.StatusConflict, repeated.Code)
	require.NotContains(t, repeated.Body.String(), secret)
	deleted := httptest.NewRecorder()
	handler.ServeHTTP(deleted, credentialTestRequest("POST", "/credentials/delete", url.Values{"accessKeyId": {"new-key"}}))
	require.Equal(t, http.StatusOK, deleted.Code, "an unconfirmed deletion only asks for confirmation")
	require.Contains(t, deleted.Body.String(), "Delete access key new-key?")
	require.Len(t, provider.credentials, 1)
	deleted = httptest.NewRecorder()
	handler.ServeHTTP(deleted, credentialTestRequest("POST", "/credentials/delete", url.Values{"accessKeyId": {"new-key"}, "confirm": {"yes"}}))
	require.Equal(t, http.StatusSeeOther, deleted.Code)
	require.Equal(t, "/credentials", deleted.Header().Get("Location"))
	require.Empty(t, provider.credentials)
}

func TestCredentialHandlersRejectUnsafeRequestsAndHideProviderErrors(t *testing.T) {
	provider := &credentialManagementFake{credentials: make(map[string]authentication.Credential)}
	handler := credentialTestHandler(t, provider)
	own := httptest.NewRecorder()
	handler.ServeHTTP(own, credentialTestRequest("POST", "/credentials/delete", url.Values{"accessKeyId": {"current"}}))
	require.Equal(t, http.StatusBadRequest, own.Code)
	require.Zero(t, provider.deletes)
	unsafe := httptest.NewRecorder()
	handler.ServeHTTP(unsafe, credentialTestRequest("POST", "/credentials/create", url.Values{"accessKeyId": {"key"}, "accountId": {"../unsafe"}, "principalId": {"principal"}}))
	require.Equal(t, http.StatusBadRequest, unsafe.Code)
	require.Empty(t, provider.credentials)
	provider.mutationError = errors.New("database failure includes sensitive-secret")
	failed := httptest.NewRecorder()
	handler.ServeHTTP(failed, credentialTestRequest("POST", "/credentials/create", url.Values{"accessKeyId": {"key"}, "accountId": {"account"}, "principalId": {"principal"}}))
	require.Equal(t, http.StatusInternalServerError, failed.Code)
	require.NotContains(t, failed.Body.String(), "sensitive-secret")
	readOnly := credentialTestHandler(t, credentialLookupOnlyFake{})
	for _, route := range []string{"/credentials/create", "/credentials/delete"} {
		out := httptest.NewRecorder()
		readOnly.ServeHTTP(out, credentialTestRequest("POST", route, nil))
		require.Equal(t, http.StatusForbidden, out.Code)
	}
	out := httptest.NewRecorder()
	readOnly.ServeHTTP(out, credentialTestRequest("GET", "/credentials", nil))
	require.Equal(t, http.StatusOK, out.Code)
	require.Contains(t, out.Body.String(), "read-only")
	require.Contains(t, out.Body.String(), "does not support credential listing")
}

func TestCredentialTemplateEscapesMetadata(t *testing.T) {
	provider := &credentialManagementFake{credentials: map[string]authentication.Credential{
		"key": {AccessKeyID: "key", AccountID: `<script>`, PrincipalID: `<script>`, SecretAccessKey: "hidden-secret"},
	}}
	out := httptest.NewRecorder()
	credentialTestHandler(t, provider).ServeHTTP(out, credentialTestRequest("GET", "/credentials", nil))
	require.Equal(t, http.StatusOK, out.Code)
	require.NotContains(t, out.Body.String(), "<script>")
	require.NotContains(t, out.Body.String(), "hidden-secret")
}

// reloadFailingDatabase fails the read-only transactions used by credential
// snapshot reloads while letting credential mutations commit.
type reloadFailingDatabase struct {
	database.Database
	failReloads atomic.Bool
}

func (d *reloadFailingDatabase) BeginTx(ctx context.Context, opts *dbsql.TxOptions) (*database.TxController, error) {
	if opts != nil && opts.ReadOnly && d.failReloads.Load() {
		return nil, errors.New("injected reload failure")
	}
	return d.Database.BeginTx(ctx, opts)
}

func TestSQLCredentialDeletionInvalidatesSessionWhenReloadFails(t *testing.T) {
	for _, interval := range []time.Duration{0, time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			ctx := context.Background()
			inner, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "credentials.db"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = inner.Close() })
			db := &reloadFailingDatabase{Database: inner}
			provider, err := sqlcredentials.NewCredentialProvider(ctx, db, interval)
			require.NoError(t, err)
			t.Cleanup(func() { _ = provider.Close() })
			for _, key := range []string{"admin", "second"} {
				require.NoError(t, provider.Create(ctx, authentication.Credential{AccessKeyID: key, SecretAccessKey: key + "-secret", AccountID: "account", PrincipalID: "principal"}))
			}
			h, _ := newSessionTestHandlerWithProvider(t, "admin,second", provider)
			_, adminCookie := testLogin(t, h, "admin", "admin-secret")
			_, secondCookie := testLogin(t, h, "second", "second-secret")
			require.NotNil(t, adminCookie)
			require.NotNil(t, secondCookie)
			csrf := sessionCSRF(t, h, adminCookie)
			db.failReloads.Store(true)

			deleteSecond := func() *httptest.ResponseRecorder {
				form := url.Values{"accessKeyId": {"second"}, "csrf_token": {csrf}, "confirm": {"yes"}}
				req := httptest.NewRequest(http.MethodPost, "/credentials/delete", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.AddCookie(adminCookie)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, req)
				return w
			}
			w := deleteSecond()
			require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())

			_, found, err := provider.Lookup(ctx, "second")
			require.NoError(t, err)
			require.False(t, found, "S3 authentication must reject the deleted key")
			req := httptest.NewRequest(http.MethodGet, "/buckets", nil)
			req.AddCookie(secondCookie)
			w = httptest.NewRecorder()
			h.ServeHTTP(w, req)
			require.Equal(t, http.StatusSeeOther, w.Code)
			require.Equal(t, "/login", w.Header().Get("Location"), "existing session of the deleted key must be invalidated")
			response, cookie := testLogin(t, h, "second", "second-secret")
			require.Equal(t, http.StatusUnauthorized, response.Code)
			require.Nil(t, cookie)

			// Retrying the deletion reports the missing row and keeps the key revoked.
			w = deleteSecond()
			require.Equal(t, http.StatusNotFound, w.Code)
			_, found, err = provider.Lookup(ctx, "second")
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}
