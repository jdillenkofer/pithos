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
type paginatedVersionStore struct {
	storage.Storage
	seen []storage.ListObjectVersionsOptions
}

func (s *paginatedVersionStore) ListObjectVersions(_ context.Context, _ storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	s.seen = append(s.seen, opts)
	next := "dir/"
	return &storage.ListObjectVersionsResult{CommonPrefixes: []string{"dir/"}, NextKeyMarker: &next, IsTruncated: true}, nil
}

func TestVersionContinuationDropsStaleVersionMarker(t *testing.T) {
	store := &paginatedVersionStore{}
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	app := &App{options: Options{Storage: store}, pages: pages}
	// The previous page ended on a version; this one ends on a common prefix.
	token, _ := json.Marshal(objectCursor{Bucket: "test-bucket", Versions: true, Key: "a", Version: "v1"})
	response := httptest.NewRecorder()
	app.objects(response, httptest.NewRequest("GET", "/objects?bucket=test-bucket&versions=true&continuation-token="+base64.RawURLEncoding.EncodeToString(token), nil))
	if response.Code != 200 || len(store.seen) != 1 || *store.seen[0].VersionIDMarker != "v1" {
		t.Fatalf("first page: %d %+v", response.Code, store.seen)
	}
	next := regexp.MustCompile(`<a href="([^"]+)">Next page</a>`).FindStringSubmatch(response.Body.String())
	if len(next) != 2 {
		t.Fatalf("missing next page: %s", response.Body.String())
	}
	response = httptest.NewRecorder()
	app.objects(response, httptest.NewRequest("GET", html.UnescapeString(next[1]), nil))
	if response.Code != 200 || len(store.seen) != 2 || store.seen[1].KeyMarker == nil || *store.seen[1].KeyMarker != "dir/" {
		t.Fatalf("continuation did not resume after prefix: %d %+v", response.Code, store.seen)
	}
	if store.seen[1].VersionIDMarker != nil {
		t.Fatalf("stale version marker %q paired with key marker %q", *store.seen[1].VersionIDMarker, *store.seen[1].KeyMarker)
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

type prefixLabelStore struct {
	storage.Storage
	prefix string
}

func (s *prefixLabelStore) ListObjects(_ context.Context, _ storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, error) {
	s.prefix = *opts.Prefix
	return &storage.ListBucketResult{
		Objects:        []storage.Object{{Key: storage.MustNewObjectKey("reports/2026/q1-summary.txt")}},
		CommonPrefixes: []string{"reports/2026/quarterly/"},
	}, nil
}

func (s *prefixLabelStore) ListObjectVersions(_ context.Context, _ storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	s.prefix = *opts.Prefix
	return &storage.ListObjectVersionsResult{
		Versions:       []storage.ObjectVersion{{Key: storage.MustNewObjectKey("reports/2026/q1-summary.txt"), VersionID: "v1"}},
		CommonPrefixes: []string{"reports/2026/quarterly/"},
	}, nil
}

func TestObjectLabelsPreservePartialFilenamePrefix(t *testing.T) {
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"reports/2026/q", "reports/2026/", ""} {
		for _, versions := range []string{"false", "true"} {
			t.Run(prefix+"/versions="+versions, func(t *testing.T) {
				store := &prefixLabelStore{}
				app := &App{options: Options{Storage: store}, pages: pages}
				response := httptest.NewRecorder()
				app.objects(response, httptest.NewRequest("GET", "/objects?bucket=test-bucket&prefix="+prefix+"&versions="+versions, nil))
				if response.Code != http.StatusOK {
					t.Fatalf("status = %d: %s", response.Code, response.Body.String())
				}
				if store.prefix != prefix {
					t.Fatalf("storage prefix = %q, want %q", store.prefix, prefix)
				}
				folder := ""
				if prefix != "" {
					folder = "reports/2026/"
				}
				for _, key := range []string{"reports/2026/q1-summary.txt", "reports/2026/quarterly/"} {
					label := strings.TrimPrefix(key, folder)
					if !strings.Contains(response.Body.String(), ">"+label+"</a>") {
						t.Fatalf("missing label %q: %s", label, response.Body.String())
					}
				}
			})
		}
	}
}
func TestObjectToolbarKeepsActionsTogether(t *testing.T) {
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	app := &App{options: Options{Storage: &prefixLabelStore{}}, pages: pages}
	response := httptest.NewRecorder()
	app.objects(response, httptest.NewRequest("GET", "/objects?bucket=test-bucket", nil))
	body := response.Body.String()
	// The selection form follows the header so it is not a flex item that
	// pushes the toolbar away from the right edge.
	head := regexp.MustCompile(`(?s)<div class="container-head">(.*?<div class="toolbar">(.*?)</div>)\s*</div>\s*<form id="delete-selection"`).FindStringSubmatch(body)
	if len(head) != 3 || strings.Contains(head[1], "<form") {
		t.Fatalf("toolbar must be the last item of the header: %s", body)
	}
	toolbar := head[1:]
	actions := regexp.MustCompile(`>(Show versions|Delete|Upload)</`).FindAllStringSubmatch(toolbar[1], -1)
	if len(actions) != 3 || actions[0][1] != "Show versions" || actions[1][1] != "Delete" || actions[2][1] != "Upload" {
		t.Fatalf("toolbar actions: %v", actions)
	}
	if !strings.Contains(toolbar[1], `form="delete-selection" class="danger-outline"`) || !strings.Contains(toolbar[1], `class="button-link primary" href="/objects/upload`) {
		t.Fatalf("delete must be secondary destructive and upload primary: %s", toolbar[1])
	}
}
func TestUploadPageSupportsProgress(t *testing.T) {
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		t.Fatal(err)
	}
	app := &App{pages: pages}
	response := httptest.NewRecorder()
	app.uploadPage(response, httptest.NewRequest("GET", "/objects/upload?bucket=test-bucket&prefix=reports/", nil))
	body := response.Body.String()
	for _, want := range []string{`data-upload-form`, `data-upload-status hidden`, `data-prefix="reports/"`, `data-upload-cancel`} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	// The server stores the file part as soon as it is read, so the key and
	// all other fields must precede it.
	if key, file := strings.Index(body, `name="key"`), strings.Index(body, `name="file"`); key < 0 || file < key {
		t.Fatal("the key field must precede the file field")
	}
}
