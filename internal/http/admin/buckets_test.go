package admin

import (
	"bytes"
	"context"
	"errors"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type bucketTestStorage struct {
	storage.Storage
	buckets []storage.Bucket
	created storage.CreateBucketOptions
	writes  int
	last    any
	failure error
}

func (s *bucketTestStorage) ListBuckets(context.Context) ([]storage.Bucket, error) {
	return s.buckets, s.failure
}
func (s *bucketTestStorage) HeadBucket(_ context.Context, n storage.BucketName) (*storage.Bucket, error) {
	for _, b := range s.buckets {
		if b.Name == n {
			return &b, nil
		}
	}
	return nil, storage.ErrNoSuchBucket
}
func (s *bucketTestStorage) CreateBucket(_ context.Context, _ storage.BucketName, o ...storage.CreateBucketOptions) error {
	s.created = o[0]
	s.writes++
	return s.failure
}
func (s *bucketTestStorage) DeleteBucket(context.Context, storage.BucketName) error {
	s.writes++
	return s.failure
}
func (s *bucketTestStorage) GetBucketLifecycleConfiguration(context.Context, storage.BucketName) (*storage.BucketLifecycleConfiguration, error) {
	return nil, storage.ErrNoSuchLifecycleConfiguration
}
func (s *bucketTestStorage) GetBucketCORSConfiguration(context.Context, storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	return &storage.BucketCORSConfiguration{}, nil
}
func (s *bucketTestStorage) GetBucketVersioningConfiguration(context.Context, storage.BucketName) (*storage.BucketVersioningConfiguration, error) {
	return &storage.BucketVersioningConfiguration{}, nil
}
func (s *bucketTestStorage) GetObjectLockConfiguration(context.Context, storage.BucketName) (*storage.ObjectLockConfiguration, error) {
	return nil, storage.ErrObjectLockConfigurationNotFound
}
func (s *bucketTestStorage) GetBucketWebsiteConfiguration(context.Context, storage.BucketName) (*storage.WebsiteConfiguration, error) {
	return nil, storage.ErrNoSuchWebsiteConfiguration
}
func (s *bucketTestStorage) GetBucketTagging(context.Context, storage.BucketName) (map[string]string, error) {
	return map[string]string{"team": "platform"}, nil
}
func (s *bucketTestStorage) GetBucketNotificationConfiguration(context.Context, storage.BucketName) (*storage.BucketNotificationConfiguration, error) {
	return &storage.BucketNotificationConfiguration{}, nil
}
func (s *bucketTestStorage) PutBucketLifecycleConfiguration(_ context.Context, _ storage.BucketName, c *storage.BucketLifecycleConfiguration) error {
	s.writes++
	s.last = c
	return s.failure
}
func (s *bucketTestStorage) PutBucketCORSConfiguration(_ context.Context, _ storage.BucketName, c *storage.BucketCORSConfiguration) error {
	s.writes++
	s.last = c
	return s.failure
}
func (s *bucketTestStorage) DeleteBucketLifecycleConfiguration(context.Context, storage.BucketName) error {
	s.writes++
	return nil
}
func (s *bucketTestStorage) DeleteBucketCORSConfiguration(context.Context, storage.BucketName) error {
	s.writes++
	return nil
}
func (s *bucketTestStorage) DeleteBucketWebsiteConfiguration(context.Context, storage.BucketName) error {
	s.writes++
	return nil
}
func (s *bucketTestStorage) DeleteBucketTagging(context.Context, storage.BucketName) error {
	s.writes++
	return nil
}
func (s *bucketTestStorage) PutBucketVersioningConfiguration(_ context.Context, _ storage.BucketName, c *storage.BucketVersioningConfiguration) error {
	s.writes++
	s.last = c
	return nil
}
func (s *bucketTestStorage) PutObjectLockConfiguration(_ context.Context, _ storage.BucketName, c *storage.ObjectLockConfiguration) error {
	s.writes++
	s.last = c
	return nil
}
func (s *bucketTestStorage) PutBucketWebsiteConfiguration(_ context.Context, _ storage.BucketName, c *storage.WebsiteConfiguration) error {
	s.writes++
	s.last = c
	return nil
}
func (s *bucketTestStorage) PutBucketTagging(_ context.Context, _ storage.BucketName, c map[string]string) error {
	s.writes++
	s.last = c
	return nil
}
func (s *bucketTestStorage) PutBucketNotificationConfiguration(_ context.Context, _ storage.BucketName, c *storage.BucketNotificationConfiguration) error {
	s.writes++
	s.last = c
	return nil
}

func bucketTestApp(t *testing.T, s *bucketTestStorage) *App {
	t.Helper()
	pages, err := template.New("layout").Parse(`{{define "layout"}}{{.Content}}{{end}}`)
	if err != nil {
		t.Fatal(err)
	}
	pages, err = pages.ParseFS(os.DirFS("."), "templates/buckets*.html")
	if err != nil {
		t.Fatal(err)
	}
	return &App{options: Options{Storage: s}, pages: pages}
}
func bucketRequest(a *App, method, path string, form url.Values) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r = r.WithContext(context.WithValue(r.Context(), stateKey{}, requestState{Credential: authentication.Credential{AccountID: "account-a"}, Session: session.Record{CSRFToken: "csrf-value"}}))
	w := httptest.NewRecorder()
	mux := http.NewServeMux()
	a.registerBuckets(mux)
	mux.ServeHTTP(w, r)
	return w
}
func TestBucketsGlobalViewAndCrossAccountDetail(t *testing.T) {
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("bucket-b"), OwnerAccountID: "account-b"}, {Name: storage.MustNewBucketName("bucket-a"), OwnerAccountID: "account-a"}}}
	a := bucketTestApp(t, s)
	w := bucketRequest(a, "GET", "/buckets", nil)
	if w.Code != 200 {
		t.Fatalf("%d: %s", w.Code, w.Body.String())
	}
	for _, want := range []string{"account-a", "account-b", "/objects?bucket=bucket-b", `href="/buckets/new"`} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("missing %s", want)
		}
	}
	if w := bucketRequest(a, "GET", "/buckets/new", nil); w.Code != 200 || !strings.Contains(w.Body.String(), `name="csrf_token" value="csrf-value"`) || !strings.Contains(w.Body.String(), `value="account-a"`) {
		t.Fatalf("create bucket page: %d %s", w.Code, w.Body.String())
	}
	if w := bucketRequest(a, "GET", "/buckets?q=et-a", nil); !strings.Contains(w.Body.String(), "bucket-a") || strings.Contains(w.Body.String(), "/objects?bucket=bucket-b") {
		t.Fatalf("bucket filter: %s", w.Body.String())
	}
	if strings.Index(w.Body.String(), "bucket-a") > strings.Index(w.Body.String(), "bucket-b") {
		t.Fatal("not sorted")
	}
	for _, tab := range bucketTabs {
		w = bucketRequest(a, "GET", "/buckets/detail?bucket=bucket-b&tab="+tab, nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "account-b") || !strings.Contains(w.Body.String(), `name="configuration"`) {
			t.Fatalf("cross-account detail (%s): %d %s", tab, w.Code, w.Body.String())
		}
	}
}
func TestBucketCreateOwnerAndDeleteConfirmation(t *testing.T) {
	for _, owner := range []string{"", "account-b"} {
		s := &bucketTestStorage{}
		a := bucketTestApp(t, s)
		w := bucketRequest(a, "POST", "/buckets/create", url.Values{"bucket": {"new-bucket"}, "owner": {owner}, "object_lock": {"true"}})
		expected := owner
		if expected == "" {
			expected = "account-a"
		}
		if w.Code != 303 || s.created.OwnerAccountID != expected || !s.created.ObjectLockEnabled {
			t.Fatalf("create: %d %+v", w.Code, s.created)
		}
	}
	for _, tc := range []struct {
		form   url.Values
		status int
	}{
		{url.Values{"bucket": {"INVALID"}}, 400},
		{url.Values{"bucket": {"valid-bucket"}, "owner": {strings.Repeat("x", 257)}}, 400},
		{url.Values{"bucket": {"valid-bucket"}, "owner": {" "}}, 400},
		{url.Values{"bucket": {"valid-bucket"}, "object_lock": {"yes"}}, 400},
	} {
		s := &bucketTestStorage{}
		w := bucketRequest(bucketTestApp(t, s), "POST", "/buckets/create", tc.form)
		if w.Code != tc.status || s.writes != 0 {
			t.Fatalf("invalid create: %d writes %d", w.Code, s.writes)
		}
	}
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	for _, confirm := range []string{"", "true", "other-bucket"} {
		w := bucketRequest(a, "POST", "/buckets/delete", url.Values{"bucket": {"valid-bucket"}, "confirm": {confirm}})
		if w.Code != 400 || s.writes != 0 {
			t.Fatal("unconfirmed deletion")
		}
	}
	w := bucketRequest(a, "POST", "/buckets/delete", url.Values{"bucket": {"valid-bucket"}, "confirm": {"valid-bucket"}})
	if w.Code != 303 || s.writes != 1 {
		t.Fatal("confirmed deletion failed")
	}
	s.failure = storage.ErrBucketNotEmpty
	w = bucketRequest(a, "POST", "/buckets/delete", url.Values{"bucket": {"valid-bucket"}, "confirm": {"valid-bucket"}})
	if w.Code != 409 {
		t.Fatal("nonempty bucket must conflict")
	}
}
func TestBucketConfigurationValidation(t *testing.T) {
	tests := []struct {
		tab, text string
		valid     bool
	}{
		{"versioning", `{"Status":"Enabled"}`, true},
		{"versioning", `{"Status":"Suspended"}`, true},
		{"versioning", `{}`, false},
		{"versioning", `{"Status":"Disabled"}`, false},
		{"object-lock", `{"ObjectLockEnabled":"Enabled"}`, true},
		{"object-lock", `{"ObjectLockEnabled":"Disabled"}`, false},
		{"object-lock", `{"ObjectLockEnabled":"Enabled","DefaultRetention":{"Mode":"GOVERNANCE","Days":0}}`, false},
		{"lifecycle", `{"Rules":[{"ID":"expire","Prefix":"","Status":"Enabled","Expiration":{"Days":30}}]}`, true},
		{"lifecycle", `{"Rules":[]}`, false},
		{"lifecycle", `{"Rules":[{"Prefix":"","Status":"Bad","Expiration":{"Days":1}}]}`, false},
		{"lifecycle", `{"Rules":[{"Prefix":"","Status":"Enabled","Expiration":{"Days":0}}]}`, false},
		{"lifecycle", `{"Rules":[{"Prefix":"","Status":"Enabled","Expiration":{"Days":1,"Date":"2030-01-01T00:00:00Z"}}]}`, false},
		{"cors", `{"Rules":[{"AllowedOrigins":["*"],"AllowedMethods":["get"]}]}`, true},
		{"cors", `{"Rules":[{"AllowedOrigins":[],"AllowedMethods":["GET"]}]}`, false},
		{"cors", `{"Rules":[{"AllowedOrigins":["*"],"AllowedMethods":[]}]}`, false},
		{"cors", `{"Rules":[{"AllowedOrigins":["*"],"AllowedMethods":["BAD"]}]}`, false},
		{"cors", `{"Rules":[{"AllowedOrigins":["**"],"AllowedMethods":["GET"]}]}`, false},
		{"cors", `{"Rules":[{"AllowedOrigins":["*"],"AllowedMethods":["GET"],"AllowedHeaders":["**"]}]}`, false},
		{"cors", `{"Rules":[{"ID":"dup","AllowedOrigins":["*"],"AllowedMethods":["GET"]},{"ID":"dup","AllowedOrigins":["*"],"AllowedMethods":["GET"]}]}`, false},
		{"website", `{"IndexDocumentSuffix":"index.html"}`, true},
		{"website", `{}`, false},
		{"website", `{"RedirectAllRequestsTo":{"HostName":"example.com","Protocol":"ftp"}}`, false},
		{"website", `{"IndexDocumentSuffix":"index.html","RedirectAllRequestsTo":{"HostName":"example.com"}}`, false},
		{"tags", `{"team":"platform"}`, true},
		{"tags", `{"":"value"}`, false},
		{"notifications", `{}`, true},
		{"notifications", `{"QueueConfigurations":[{"DestinationARN":"invalid","Events":["s3:ObjectCreated:*"]}]}`, false},
		{"notifications", `{"QueueConfigurations":[{"DestinationARN":"arn:aws:sqs:us-east-1:123456789012:queue","Events":["bad"]}]}`, false},
	}
	for _, tc := range tests {
		t.Run(tc.tab+tc.text, func(t *testing.T) {
			s := &bucketTestStorage{}
			w := bucketRequest(bucketTestApp(t, s), "POST", "/buckets/configure", url.Values{"bucket": {"valid-bucket"}, "tab": {tc.tab}, "configuration": {tc.text}, "action": {"save"}})
			if tc.valid {
				if w.Code != 303 || s.writes != 1 {
					t.Fatalf("%d: %s", w.Code, w.Body.String())
				}
			} else if w.Code != 400 || s.writes != 0 {
				t.Fatalf("invalid accepted: %d writes %d", w.Code, s.writes)
			}
		})
	}
}
func TestBucketConfigurationNormalizationAndStorageErrors(t *testing.T) {
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	w := bucketRequest(a, "POST", "/buckets/configure", url.Values{
		"bucket": {"valid-bucket"}, "tab": {"cors"},
		"configuration": {`{"Rules":[{"AllowedOrigins":[" * ","*"],"AllowedMethods":[" get ","GET"]}]}`},
	})
	if w.Code != http.StatusSeeOther {
		t.Fatalf("normalization failed: %d %s", w.Code, w.Body.String())
	}
	c := s.last.(*storage.BucketCORSConfiguration)
	if len(c.Rules[0].AllowedOrigins) != 2 || c.Rules[0].AllowedOrigins[0] != "*" || len(c.Rules[0].AllowedMethods) != 2 || c.Rules[0].AllowedMethods[0] != "GET" {
		t.Fatalf("CORS was not normalized: %+v", c)
	}
	s.failure = storage.ErrNoSuchBucket
	w = bucketRequest(a, "POST", "/buckets/configure", url.Values{
		"bucket": {"valid-bucket"}, "tab": {"cors"}, "configuration": {`{"Rules":[]}`},
	})
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing bucket: %d", w.Code)
	}
	w = bucketRequest(a, "GET", "/buckets/detail?bucket=missing-bucket&tab=cors", nil)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing detail: %d", w.Code)
	}
}

func TestBucketJSONBoundsAndActions(t *testing.T) {
	for _, text := range []string{"null", "", `{} {}`, `{"Unknown":true}`, `[]`, strings.Repeat("x", maxBucketFormSize+1)} {
		if err := decodeBucketConfiguration(text, new(storage.BucketCORSConfiguration)); err == nil {
			t.Fatalf("accepted invalid JSON %q", text[:min(len(text), 50)])
		}
	}
	for _, tab := range bucketTabs {
		s := &bucketTestStorage{}
		apply, err := bucketConfigurationMutation(s, storage.MustNewBucketName("valid-bucket"), tab, "", "delete")
		deletable := tab == "lifecycle" || tab == "cors" || tab == "website" || tab == "tags"
		if deletable {
			if err != nil {
				t.Fatal(err)
			}
			if err = apply(httptest.NewRequest("POST", "/", nil)); err != nil || s.writes != 1 {
				t.Fatal("delete failed")
			}
		} else if err == nil {
			t.Fatalf("deleted %s", tab)
		}
	}
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	for _, path := range []string{"/buckets/detail?bucket=valid-bucket&tab=unknown", "/buckets/detail?bucket=INVALID"} {
		if w := bucketRequest(a, "GET", path, nil); w.Code != 400 {
			t.Fatalf("invalid detail: %d", w.Code)
		}
	}
	for _, form := range []url.Values{
		{"bucket": {"valid-bucket"}, "tab": {"unknown"}},
		{"bucket": {"INVALID"}, "tab": {"cors"}},
		{"bucket": {"valid-bucket"}, "tab": {"cors"}, "action": {"unknown"}},
		{"bucket": {"valid-bucket"}, "tab": {"cors"}, "configuration": {strings.Repeat("x", maxBucketFormSize+1)}},
	} {
		if w := bucketRequest(a, "POST", "/buckets/configure", form); w.Code != 400 || s.writes != 0 {
			t.Fatalf("invalid form: %d", w.Code)
		}
	}
	s.failure = errors.New("secret backend error")
	w := bucketRequest(a, "GET", "/buckets", nil)
	if w.Code != 500 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal("backend detail leaked")
	}
}
func TestBucketTemplatesEscapeAndCSRF(t *testing.T) {
	pages, err := template.ParseFS(os.DirFS("."), "templates/buckets*.html")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	data := bucketDetailPage{CSRF: `"><script>`, Bucket: &storage.Bucket{Name: storage.MustNewBucketName("valid-bucket"), OwnerAccountID: `<script>`}, Header: bucketHeader{Name: "valid-bucket", Owner: `<script>`, Active: "properties"}, Tabs: bucketGroupTabs("properties"), Tab: "website", Configuration: `</textarea><script>alert(1)</script>`}
	if err := pages.ExecuteTemplate(&out, "buckets_detail", data); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "<script>") || strings.Count(out.String(), `name="csrf_token"`) != 2 {
		t.Fatal("unsafe template or missing CSRF")
	}
}
