package admin

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	_ "github.com/mattn/go-sqlite3"
)

type sessionTestProvider struct {
	mu          sync.Mutex
	credentials map[string]authentication.Credential
}

func (p *sessionTestProvider) Lookup(ctx context.Context, key string) (authentication.Credential, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c, ok := p.credentials[key]
	return c, ok, nil
}

type browserTestStore struct {
	storage.Storage
	mu      sync.Mutex
	data    []byte
	deleted bool
	events  []string
}

func (s *browserTestStore) ListBuckets(context.Context) ([]storage.Bucket, error) {
	return []storage.Bucket{{Name: storage.MustNewBucketName("foreign-bucket"), OwnerAccountID: "foreign-account"}}, nil
}
func (s *browserTestStore) ListObjects(ctx context.Context, b storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	return &storage.ListBucketResult{Objects: []storage.Object{{Key: storage.MustNewObjectKey("<script>.html"), Size: int64(len(s.data))}}}, nil
}
func (s *browserTestStore) HeadObject(ctx context.Context, b storage.BucketName, k storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	return &storage.Object{Key: k, Size: int64(len(s.data)), ETag: "etag"}, nil
}
func (s *browserTestStore) GetObject(ctx context.Context, b storage.BucketName, k storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	return &storage.Object{Key: k, Size: int64(len(s.data))}, []io.ReadCloser{io.NopCloser(bytes.NewReader(s.data))}, nil
}
func (s *browserTestStore) PutObject(ctx context.Context, b storage.BucketName, k storage.ObjectKey, contentType *string, data io.Reader, checksum *storage.ChecksumInput, opts *storage.PutObjectOptions) (*storage.PutObjectResult, error) {
	bts, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.data = bts
	s.mu.Unlock()
	return &storage.PutObjectResult{}, nil
}
func (s *browserTestStore) GetObjectTagging(context.Context, storage.BucketName, storage.ObjectKey, *storage.ObjectTaggingOptions) (map[string]string, error) {
	return map[string]string{"key": "<script>"}, nil
}
func (s *browserTestStore) DeleteObject(ctx context.Context, b storage.BucketName, k storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	s.deleted = true
	return &storage.DeleteObjectResult{}, nil
}
func (s *browserTestStore) RecordAdminEvent(ctx context.Context, op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, op)
}

func newSessionTestHandler(t *testing.T) (http.Handler, *sessionTestProvider, *browserTestStore) {
	t.Helper()
	p := &sessionTestProvider{credentials: map[string]authentication.Credential{"admin": {AccessKeyID: "admin", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "principal"}, "ordinary": {AccessKeyID: "ordinary", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "principal"}}}
	handler, store := newSessionTestHandlerWithProvider(t, "admin", p)
	return handler, p, store
}

func newSessionTestHandlerWithProvider(t *testing.T, adminAccessKeys string, p authentication.CredentialProvider) (http.Handler, *browserTestStore) {
	t.Helper()
	return newSessionTestHandlerWithSecureCookie(t, adminAccessKeys, p, false)
}

func newSessionTestHandlerWithSecureCookie(t *testing.T, adminAccessKeys string, p authentication.CredentialProvider, secureCookie bool) (http.Handler, *browserTestStore) {
	t.Helper()
	t.Setenv("PITHOS_AUTHENTICATION_ENABLED", "true")
	t.Setenv("PITHOS_ADMIN_ACCESS_KEYS", adminAccessKeys)
	t.Setenv("PITHOS_ADMIN_DATABASE_INDEX", "0")
	t.Setenv("PITHOS_ADMIN_SESSION_IDLE_MINUTES", "30")
	t.Setenv("PITHOS_ADMIN_SESSION_MAX_HOURS", "12")
	t.Setenv("PITHOS_ADMIN_SECURE_COOKIE", strconv.FormatBool(secureCookie))
	cfg, err := settings.LoadSettings(nil)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "admin.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); _ = db.Close() })
	store := &browserTestStore{data: []byte("<script>alert('xss')</script>")}
	handler, err := NewHandler(Options{Context: ctx, Settings: cfg, Storage: store, Databases: []database.Database{db}, CredentialProvider: p, StorageStarted: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.(io.Closer).Close() })
	return handler, store
}

var csrfInput = regexp.MustCompile(`name="csrf_token" value="([^"]+)"`)
var csrfMeta = regexp.MustCompile(`name="csrf-token" content="([^"]+)"`)

func testLogin(t *testing.T, h http.Handler, key, secret string) (*httptest.ResponseRecorder, *http.Cookie) {
	t.Helper()
	page := httptest.NewRecorder()
	h.ServeHTTP(page, httptest.NewRequest("GET", "/login", nil))
	match := csrfInput.FindStringSubmatch(page.Body.String())
	if len(match) != 2 {
		t.Fatalf("missing login CSRF: %s", page.Body.String())
	}
	form := url.Values{"csrf_token": {match[1]}, "access_key": {key}, "secret_key": {secret}}
	req := httptest.NewRequest("POST", "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for _, cookie := range page.Result().Cookies() {
		req.AddCookie(cookie)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	var sessionCookie *http.Cookie
	for _, cookie := range response.Result().Cookies() {
		if cookie.Name == sessionCookieName {
			sessionCookie = cookie
		}
	}
	return response, sessionCookie
}
func sessionCSRF(t *testing.T, h http.Handler, cookie *http.Cookie) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/", nil)
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("dashboard: %d %s", w.Code, w.Body.String())
	}
	match := csrfMeta.FindStringSubmatch(w.Body.String())
	if len(match) != 2 {
		t.Fatal("missing session CSRF")
	}
	return match[1]
}
func TestSecureCookieLogin(t *testing.T) {
	p := &sessionTestProvider{credentials: map[string]authentication.Credential{"admin": {AccessKeyID: "admin", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "principal"}}}
	h, _ := newSessionTestHandlerWithSecureCookie(t, "admin", p, true)
	response, cookie := testLogin(t, h, "admin", "secret")
	if response.Code != 303 || cookie == nil || !cookie.Secure {
		t.Fatalf("secure login: %d %+v", response.Code, cookie)
	}

	// A browser on plain HTTP drops the Secure login CSRF cookie.
	page := httptest.NewRecorder()
	h.ServeHTTP(page, httptest.NewRequest("GET", "/login", nil))
	match := csrfInput.FindStringSubmatch(page.Body.String())
	if len(match) != 2 {
		t.Fatal("missing login CSRF")
	}
	form := url.Values{"csrf_token": {match[1]}, "access_key": {"admin"}, "secret_key": {"secret"}}
	req := httptest.NewRequest("POST", "/login", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 403 || !strings.Contains(w.Body.String(), "PITHOS_ADMIN_SECURE_COOKIE=false") {
		t.Fatalf("missing cookie hint: %d %s", w.Code, w.Body.String())
	}
}

func TestLoginActionsLogout(t *testing.T) {
	h, _, store := newSessionTestHandler(t)
	response, cookie := testLogin(t, h, "admin", "secret")
	if response.Code != 303 || cookie == nil {
		t.Fatalf("login: %d %s", response.Code, response.Body.String())
	}
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Secure {
		t.Fatalf("cookie flags: %+v", cookie)
	}
	csrf := sessionCSRF(t, h, cookie)
	req := httptest.NewRequest("GET", "/buckets", nil)
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "foreign-bucket") {
		t.Fatalf("global bucket view: %d %s", w.Code, w.Body.String())
	}
	form := url.Values{"bucket": {"foreign-bucket"}, "key": {"object"}, "confirm": {"yes"}, "csrf_token": {csrf}}
	req = httptest.NewRequest("POST", "/objects/delete", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 303 || !store.deleted {
		t.Fatalf("delete: %d %s", w.Code, w.Body.String())
	}
	req = httptest.NewRequest("POST", "/logout", strings.NewReader(url.Values{"csrf_token": {csrf}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 303 {
		t.Fatalf("logout: %d", w.Code)
	}
	req = httptest.NewRequest("GET", "/buckets", nil)
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 303 || w.Header().Get("Location") != "/login" {
		t.Fatal("logged-out session still valid")
	}
	if len(store.events) < 3 {
		t.Fatalf("missing audit events: %v", store.events)
	}
}
func TestLoginRejectsNonAdminAndWrongSecret(t *testing.T) {
	h, _, _ := newSessionTestHandler(t)
	for _, tc := range []struct{ key, secret string }{{"ordinary", "secret"}, {"admin", "wrong"}, {"missing", "secret"}} {
		response, cookie := testLogin(t, h, tc.key, tc.secret)
		if response.Code != 401 || cookie != nil {
			t.Fatalf("login %s: %d", tc.key, response.Code)
		}
	}
	req := httptest.NewRequest("POST", "/login", strings.NewReader("access_key=admin&secret_key=secret"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("login CSRF accepted")
	}
}
func TestSessionCredentialRotationAndDeletion(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "rotated", true: "deleted"}[deleted], func(t *testing.T) {
			h, p, _ := newSessionTestHandler(t)
			_, cookie := testLogin(t, h, "admin", "secret")
			if cookie == nil {
				t.Fatal("no session")
			}
			p.mu.Lock()
			if deleted {
				delete(p.credentials, "admin")
			} else {
				c := p.credentials["admin"]
				c.SecretAccessKey = "rotated"
				p.credentials["admin"] = c
			}
			p.mu.Unlock()
			req := httptest.NewRequest("GET", "/buckets", nil)
			req.AddCookie(cookie)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != 303 || w.Header().Get("Location") != "/login" {
				t.Fatal("stale credential session survived")
			}
		})
	}
}
func TestCSRFAndDownloadSafety(t *testing.T) {
	h, _, store := newSessionTestHandler(t)
	_, cookie := testLogin(t, h, "admin", "secret")
	csrf := sessionCSRF(t, h, cookie)
	for _, origin := range []string{"", "http://attacker.example"} {
		form := url.Values{"bucket": {"foreign-bucket"}, "key": {"object"}, "confirm": {"yes"}}
		if origin != "" {
			form.Set("csrf_token", csrf)
		}
		req := httptest.NewRequest("POST", "/objects/delete", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		req.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != 403 {
			t.Fatalf("CSRF accepted: %d", w.Code)
		}
	}
	if store.deleted {
		t.Fatal("CSRF performed action")
	}
	req := httptest.NewRequest("GET", objectURL("/objects/download", "foreign-bucket", "<script>.html", ""), nil)
	req.AddCookie(cookie)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/octet-stream" || !strings.HasPrefix(w.Header().Get("Content-Disposition"), "attachment;") || !strings.Contains(w.Header().Get("Content-Security-Policy"), "sandbox") {
		t.Fatalf("unsafe download: %d %v", w.Code, w.Header())
	}
	req = httptest.NewRequest("GET", objectURL("/objects/detail", "foreign-bucket", "<script>.html", ""), nil)
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if strings.Contains(w.Body.String(), "<script>.html") || !strings.Contains(w.Body.String(), "&lt;script&gt;") {
		t.Fatal("object metadata was not escaped")
	}
}
func TestStreamingFormUpload(t *testing.T) {
	h, _, store := newSessionTestHandler(t)
	_, cookie := testLogin(t, h, "admin", "secret")
	csrf := sessionCSRF(t, h, cookie)
	upload := func(bucket, key, accept string) *httptest.ResponseRecorder {
		var body bytes.Buffer
		form := multipart.NewWriter(&body)
		_ = form.WriteField("csrf_token", csrf)
		_ = form.WriteField("bucket", bucket)
		_ = form.WriteField("key", key)
		file, err := form.CreateFormFile("file", "test.txt")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.Write([]byte("streamed content"))
		_ = form.Close()
		req := httptest.NewRequest("POST", "/objects/upload", &body)
		req.Header.Set("Content-Type", form.FormDataContentType())
		req.Header.Set("Accept", accept)
		req.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	w := upload("foreign-bucket", "uploaded", "")
	if w.Code != 303 || string(store.data) != "streamed content" {
		t.Fatalf("upload: %d %s", w.Code, w.Body.String())
	}
	// A folder prefix or an empty key is completed with the file name.
	for key, want := range map[string]string{"reports/2026/": "reports/2026/test.txt", "": "test.txt"} {
		w := upload("foreign-bucket", key, "")
		if location := w.Header().Get("Location"); w.Code != 303 || location != objectURL("/objects/detail", "foreign-bucket", want, "") {
			t.Fatalf("key %q: %d %s", key, w.Code, location)
		}
	}
	// Scripted uploads show the error message in place.
	w = upload("INVALID", "uploaded", "text/plain")
	if w.Code != 400 || strings.TrimSpace(w.Body.String()) != "Invalid bucket" {
		t.Fatalf("scripted upload error: %d %q", w.Code, w.Body.String())
	}
}
