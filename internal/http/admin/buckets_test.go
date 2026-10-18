package admin

import (
	"bytes"
	"context"
	"errors"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/stretchr/testify/require"
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
	// lifecycle and website, when set, are returned as the current configuration.
	lifecycle *storage.BucketLifecycleConfiguration
	website   *storage.WebsiteConfiguration
	deleted   string
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
	if s.lifecycle != nil {
		return s.lifecycle, nil
	}
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
	if s.website != nil {
		return s.website, nil
	}
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
	s.deleted = "lifecycle"
	return nil
}
func (s *bucketTestStorage) DeleteBucketCORSConfiguration(context.Context, storage.BucketName) error {
	s.writes++
	s.deleted = "cors"
	return nil
}
func (s *bucketTestStorage) DeleteBucketWebsiteConfiguration(context.Context, storage.BucketName) error {
	s.writes++
	s.deleted = "website"
	return nil
}
func (s *bucketTestStorage) DeleteBucketTagging(context.Context, storage.BucketName) error {
	s.writes++
	s.deleted = "tagging"
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
	pages, err = pages.ParseFS(os.DirFS("."), "templates/buckets*.html", "templates/lifecycle.html", "templates/confirm.html")
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
		if w.Code != 200 || !strings.Contains(w.Body.String(), "account-b") || (tab != "lifecycle" && !strings.Contains(w.Body.String(), `name="configuration"`)) {
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
	pages, err := template.ParseFS(os.DirFS("."), "templates/buckets*.html", "templates/lifecycle.html")
	if err != nil {
		t.Fatal(err)
	}
	unsafe := `</textarea><script>alert(1)</script>`
	for _, group := range []string{"properties", "permissions", "management"} {
		var out bytes.Buffer
		data := bucketDetailPage{
			CSRF: `"><script>`, Bucket: &storage.Bucket{Name: storage.MustNewBucketName("valid-bucket"), OwnerAccountID: `<script>`},
			Header: bucketHeader{Name: "valid-bucket", Owner: `<script>`, Active: group}, Configuration: unsafe,
			ObjectLock: objectLockView{Enabled: true, Mode: unsafe}, Tags: []keyValue{{unsafe, unsafe}}, Website: websiteView{Enabled: true, Index: unsafe},
			Notifications: []notificationView{{ID: unsafe}}, CORS: []corsView{{Origins: unsafe}},
			Lifecycle: []lifecycleView{{ID: unsafe, EditURL: "/x"}}, LifecycleRule: lifecycleFormView{ID: unsafe, Prefix: unsafe},
		}
		if err := pages.ExecuteTemplate(&out, "buckets_detail", data); err != nil {
			t.Fatal(err)
		}
		forms, tokens := strings.Count(out.String(), "<form"), strings.Count(out.String(), `name="csrf_token"`)
		if strings.Contains(out.String(), "<script>") || forms == 0 || forms != tokens {
			t.Fatalf("%s: unsafe template or missing CSRF (%d forms, %d tokens)", group, forms, tokens)
		}
	}
}

func structuredBucketRequest(a *App, tab string, form url.Values) *httptest.ResponseRecorder {
	form.Set("bucket", "valid-bucket")
	form.Set("tab", tab)
	form.Set("editor", "form")
	form.Set("confirm", "yes")
	return bucketRequest(a, "POST", "/buckets/configure", form)
}

func TestStructuredBucketEditors(t *testing.T) {
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)

	w := structuredBucketRequest(a, "versioning", url.Values{"status": {"Enabled"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, storage.BucketVersioningStatusEnabled, *s.last.(*storage.BucketVersioningConfiguration).Status)
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "versioning", url.Values{"status": {"Disabled"}}).Code)

	w = structuredBucketRequest(a, "tags", url.Values{"tag_key": {"team", " env ", ""}, "tag_value": {"platform", "", ""}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, map[string]string{"team": "platform", "env": ""}, s.last)
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "tags", url.Values{"tag_key": {"a", "a"}, "tag_value": {"1", "2"}}).Code)
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "tags", url.Values{"tag_key": {""}, "tag_value": {"orphan"}}).Code)
	writes := s.writes
	require.Equal(t, http.StatusSeeOther, structuredBucketRequest(a, "tags", url.Values{"tag_key": {""}, "tag_value": {""}}).Code)
	require.Equal(t, "tagging", s.deleted, "removing every tag deletes the tag set")
	require.Equal(t, writes+1, s.writes)

	w = structuredBucketRequest(a, "object-lock", url.Values{"default_retention": {"enabled"}, "mode": {"COMPLIANCE"}, "period": {"2"}, "unit": {"Years"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	lock := s.last.(*storage.ObjectLockConfiguration)
	require.Equal(t, storage.RetentionMode("COMPLIANCE"), lock.DefaultRetention.Mode)
	require.Equal(t, int32(2), *lock.DefaultRetention.Years)
	require.Nil(t, lock.DefaultRetention.Days)
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "object-lock", url.Values{"default_retention": {"enabled"}, "mode": {"GOVERNANCE"}, "period": {"0"}}).Code)
	require.Equal(t, http.StatusSeeOther, structuredBucketRequest(a, "object-lock", url.Values{"default_retention": {"disabled"}}).Code)
	require.Nil(t, s.last.(*storage.ObjectLockConfiguration).DefaultRetention)

	errorDocument := "missing.html"
	s.website = &storage.WebsiteConfiguration{IndexDocumentSuffix: "old.html", RoutingRules: []storage.WebsiteRoutingRule{{Redirect: storage.WebsiteRedirect{ReplaceKeyWith: &errorDocument}}}}
	w = structuredBucketRequest(a, "website", url.Values{"website": {"enabled"}, "hosting": {"website"}, "index_document": {"index.html"}, "error_document": {"error.html"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	website := s.last.(*storage.WebsiteConfiguration)
	require.Equal(t, "index.html", website.IndexDocumentSuffix)
	require.Equal(t, "error.html", *website.ErrorDocumentKey)
	require.Len(t, website.RoutingRules, 1, "routing rules without form fields are kept")
	w = structuredBucketRequest(a, "website", url.Values{"website": {"enabled"}, "hosting": {"redirect"}, "redirect_host": {"example.com"}, "redirect_protocol": {"https"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "example.com", s.last.(*storage.WebsiteConfiguration).RedirectAllRequestsTo.HostName)
	require.Equal(t, http.StatusSeeOther, structuredBucketRequest(a, "website", url.Values{"website": {"disabled"}}).Code)
	require.Equal(t, "website", s.deleted)

	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "cors", url.Values{}).Code, "CORS has no form editor")
}

func TestStructuredLifecycleRules(t *testing.T) {
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	w := structuredBucketRequest(a, "lifecycle", url.Values{"rule_id": {"logs"}, "status": {"Enabled"}, "prefix": {"logs/"}, "expire_days": {"30"}, "noncurrent_days": {"7"}, "newer_noncurrent": {"2"}, "abort_days": {"3"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "/buckets/detail?bucket=valid-bucket&tab=lifecycle#lifecycle", w.Header().Get("Location"))
	config := s.last.(*storage.BucketLifecycleConfiguration)
	require.Len(t, config.Rules, 1)
	rule := config.Rules[0]
	require.Equal(t, "logs/", *rule.Filter.Prefix)
	require.Equal(t, int32(30), *rule.Expiration.Days)
	require.Equal(t, int32(7), *rule.NoncurrentVersionExpiration.NoncurrentDays)
	require.Equal(t, int32(2), *rule.NoncurrentVersionExpiration.NewerNoncurrentVersions)
	require.Equal(t, int32(3), *rule.AbortIncompleteMultipartUpload.DaysAfterInitiation)

	s.lifecycle = config
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "lifecycle", url.Values{"rule_id": {"logs"}, "expire_days": {"1"}}).Code, "duplicate rule names are rejected")
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "lifecycle", url.Values{"rule_id": {"empty"}}).Code, "a rule needs an action")
	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "lifecycle", url.Values{"rule_id": {"both"}, "expire_days": {"1"}, "expired_delete_markers": {"true"}}).Code)

	w = structuredBucketRequest(a, "lifecycle", url.Values{"rule_id": {"markers"}, "expired_delete_markers": {"true"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	s.lifecycle = s.last.(*storage.BucketLifecycleConfiguration)
	require.Len(t, s.lifecycle.Rules, 2)

	w = structuredBucketRequest(a, "lifecycle", url.Values{"original_id": {"logs"}, "rule_id": {"logs-renamed"}, "status": {"Disabled"}, "expire_days": {"90"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	s.lifecycle = s.last.(*storage.BucketLifecycleConfiguration)
	require.Len(t, s.lifecycle.Rules, 2)
	require.Equal(t, "logs-renamed", *s.lifecycle.Rules[1].ID)
	require.Equal(t, "Disabled", s.lifecycle.Rules[1].Status)

	page := bucketRequest(a, "GET", "/buckets/detail?bucket=bucket-b&tab=lifecycle&rule=logs-renamed", nil)
	require.Equal(t, http.StatusNotFound, page.Code, "unknown bucket")
	s.buckets = []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket"), OwnerAccountID: "account"}}
	page = bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle&rule=logs-renamed", nil)
	require.Equal(t, http.StatusOK, page.Code)
	require.Contains(t, page.Body.String(), `name="original_id" value="logs-renamed"`)
	require.Contains(t, page.Body.String(), `name="expire_days" value="90"`)

	require.Equal(t, http.StatusBadRequest, structuredBucketRequest(a, "lifecycle", url.Values{"rule_action": {"delete"}, "original_id": {"missing"}}).Code)
	w = structuredBucketRequest(a, "lifecycle", url.Values{"rule_action": {"delete"}, "original_id": {"markers"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	s.lifecycle = s.last.(*storage.BucketLifecycleConfiguration)
	require.Len(t, s.lifecycle.Rules, 1)
	w = structuredBucketRequest(a, "lifecycle", url.Values{"rule_action": {"delete"}, "original_id": {"logs-renamed"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "lifecycle", s.deleted, "deleting the last rule deletes the configuration")
}

func TestBucketCreateWithVersioning(t *testing.T) {
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	w := bucketRequest(a, "POST", "/buckets/create", url.Values{"bucket": {"new-bucket"}, "owner": {"account-a"}, "versioning": {"Enabled"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "/objects?bucket=new-bucket", w.Header().Get("Location"))
	require.Equal(t, 2, s.writes)
	require.Equal(t, storage.BucketVersioningStatusEnabled, *s.last.(*storage.BucketVersioningConfiguration).Status)

	s = &bucketTestStorage{}
	a = bucketTestApp(t, s)
	w = bucketRequest(a, "POST", "/buckets/create", url.Values{"bucket": {"locked-bucket"}, "versioning": {"Enabled"}, "object_lock": {"true"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, 1, s.writes, "Object Lock enables versioning itself")
	require.Equal(t, http.StatusBadRequest, bucketRequest(a, "POST", "/buckets/create", url.Values{"bucket": {"bad-bucket"}, "versioning": {"Suspended"}}).Code)
}

func TestBucketConfigurationEventNames(t *testing.T) {
	require.Equal(t, "AdminPutBucketLifecycle", bucketConfigurationEvent("lifecycle", "save"))
	require.Equal(t, "AdminPutBucketLifecycle", bucketConfigurationEvent("lifecycle", ""))
	require.Equal(t, "AdminDeleteBucketCors", bucketConfigurationEvent("cors", "delete"))
	for _, tab := range bucketTabs {
		require.NotEqual(t, "AdminPut", bucketConfigurationEvent(tab, "save"), tab)
	}
}

func TestBucketConfigurationDeletesRequireConfirmation(t *testing.T) {
	s := &bucketTestStorage{lifecycle: &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{{ID: ptrutils.ToPtr("keep"), Status: "Enabled", Expiration: &storage.LifecycleExpiration{Days: ptrutils.ToPtr(int32(1))}}}}}
	a := bucketTestApp(t, s)
	for _, form := range []url.Values{
		{"tab": {"cors"}, "action": {"delete"}},
		{"tab": {"lifecycle"}, "action": {"delete"}},
		{"tab": {"tags"}, "editor": {"form"}},
		{"tab": {"website"}, "editor": {"form"}, "website": {"disabled"}},
		{"tab": {"lifecycle"}, "editor": {"form"}, "rule_action": {"delete"}, "original_id": {"keep"}},
	} {
		form.Set("bucket", "valid-bucket")
		w := bucketRequest(a, "POST", "/buckets/configure", form)
		require.Equal(t, http.StatusOK, w.Code, "%v: %s", form, w.Body.String())
		require.Contains(t, w.Body.String(), `name="confirm" value="yes"`)
		require.Contains(t, w.Body.String(), `name="tab" value="`+form.Get("tab")+`"`, "the confirmation re-submits the same form")
		require.Zero(t, s.writes, "%v deleted without confirmation", form)
	}
	writes := s.writes
	w := bucketRequest(a, "POST", "/buckets/configure", url.Values{"bucket": {"valid-bucket"}, "tab": {"versioning"}, "configuration": {`{"Status":"Enabled"}`}})
	require.Equal(t, http.StatusSeeOther, w.Code, "non-destructive changes need no confirmation")
	require.Equal(t, writes+1, s.writes)
}

func TestLifecycleEditorIsSeparateFromList(t *testing.T) {
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}}
	a := bucketTestApp(t, s)
	page := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle", nil)
	require.Equal(t, http.StatusOK, page.Code)
	require.Contains(t, page.Body.String(), "Lifecycle rules")
	require.Contains(t, page.Body.String(), "editor=create")
	require.NotContains(t, page.Body.String(), `name="rule_id"`)

	page = bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle&editor=create", nil)
	require.Equal(t, http.StatusOK, page.Code)
	require.Contains(t, page.Body.String(), `name="rule_id"`)
	require.Contains(t, page.Body.String(), ">Cancel</a>")
	require.Contains(t, page.Body.String(), ">Review rule</button>")
	require.NotContains(t, page.Body.String(), "No lifecycle rules")
	require.NotContains(t, page.Body.String(), "Lifecycle configuration (JSON)")
}

func TestLifecycleSaveRequiresReview(t *testing.T) {
	for _, prefix := range []string{"logs/", ""} {
		t.Run(prefix, func(t *testing.T) {
			s := &bucketTestStorage{}
			a := bucketTestApp(t, s)
			form := url.Values{"bucket": {"valid-bucket"}, "tab": {"lifecycle"}, "editor": {"form"}, "rule_id": {"cleanup"}, "status": {"Enabled"}, "prefix": {prefix}, "expire_days": {"30"}, "noncurrent_days": {"7"}}
			page := bucketRequest(a, "POST", "/buckets/configure", form)
			require.Equal(t, http.StatusOK, page.Code, page.Body.String())
			require.Zero(t, s.writes, "review must not mutate storage")
			require.Contains(t, page.Body.String(), "Rule summary")
			require.Contains(t, page.Body.String(), "Expire after 30 days")
			require.Contains(t, page.Body.String(), "Delete after 7 noncurrent days")
			require.Contains(t, page.Body.String(), ">Create rule</button>")
			require.NotContains(t, page.Body.String(), "Confirm deletion")
			if prefix == "" {
				require.Contains(t, page.Body.String(), "entire bucket")
			} else {
				require.Contains(t, page.Body.String(), "logs/")
				require.NotContains(t, page.Body.String(), "Bucket-wide rule")
			}
			form.Set("confirm", "yes")
			page = bucketRequest(a, "POST", "/buckets/configure", form)
			require.Equal(t, http.StatusSeeOther, page.Code, page.Body.String())
			require.Equal(t, 1, s.writes)
		})
	}
}
