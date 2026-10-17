package admin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"html"
	"html/template"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
)

func TestUploadLimitReaderRejectsOverflow(t *testing.T) {
	for _, tc := range []struct {
		input    string
		oversize bool
	}{{"1234", false}, {"12345", true}} {
		data, err := io.ReadAll(&uploadLimitReader{reader: strings.NewReader(tc.input), remaining: 4})
		if string(data) != "1234" {
			t.Fatalf("data = %q", data)
		}
		if tc.oversize && !errors.Is(err, storage.ErrEntityTooLarge) {
			t.Fatalf("overflow error = %v", err)
		}
		if !tc.oversize && err != nil {
			t.Fatal(err)
		}
	}
}

type paginatedObjectStore struct {
	storage.Storage
	seen []storage.ListObjectsOptions
}

func (s *paginatedObjectStore) ListObjects(ctx context.Context, b storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	s.seen = append(s.seen, opts)
	if opts.StartAfter == nil {
		return &storage.ListBucketResult{CommonPrefixes: []string{"a/"}, Objects: []storage.Object{{Key: storage.MustNewObjectKey("z")}}, IsTruncated: true}, nil
	}
	return &storage.ListBucketResult{Objects: []storage.Object{{Key: storage.MustNewObjectKey("zz")}}}, nil
}
func TestObjectContinuationIsBoundToListing(t *testing.T) {
	store := &paginatedObjectStore{}
	app := &App{options: Options{Storage: store}}
	// Capture validation before rendering; a mismatched cursor must never call storage.
	token, _ := json.Marshal(objectCursor{Bucket: "other-bucket", Prefix: "", Versions: false, Key: "marker"})
	request := httptest.NewRequest("GET", "/objects?bucket=test-bucket&continuation-token="+base64.RawURLEncoding.EncodeToString(token), nil)
	response := httptest.NewRecorder()
	app.objects(response, request)
	if response.Code != http.StatusBadRequest || len(store.seen) != 0 {
		t.Fatal("cross-listing cursor accepted")
	}
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	app.pages = pages
	response = httptest.NewRecorder()
	app.objects(response, httptest.NewRequest("GET", "/objects?bucket=test-bucket", nil))
	if response.Code != 200 {
		t.Fatalf("first page: %d", response.Code)
	}
	next := regexp.MustCompile(`<a href="([^"]+)">Next page</a>`).FindStringSubmatch(response.Body.String())
	if len(next) != 2 {
		t.Fatalf("missing next page: %s", response.Body.String())
	}
	response = httptest.NewRecorder()
	app.objects(response, httptest.NewRequest("GET", html.UnescapeString(next[1]), nil))
	if response.Code != 200 || len(store.seen) != 2 || store.seen[1].StartAfter == nil || *store.seen[1].StartAfter != "z" {
		t.Fatalf("continuation did not resume after last key: %+v", store.seen)
	}
	if store.seen[0].MaxKeys != 100 || store.seen[0].Delimiter == nil || *store.seen[0].Delimiter != "/" {
		t.Fatal("unbounded or flat listing")
	}
}
func TestObjectErrorsDoNotExposeProviderSecrets(t *testing.T) {
	app := &App{}
	response := httptest.NewRecorder()
	app.objectError(response, httptest.NewRequest("GET", "/objects", nil), errors.New("sftp://user:password@private-host connection failed"))
	if strings.Contains(response.Body.String(), "password") || strings.Contains(response.Body.String(), "private-host") {
		t.Fatal("backend details leaked")
	}
}
