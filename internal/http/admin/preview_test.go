package admin

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type previewTestStore struct {
	storage.Storage
	objects  map[string]previewTestObject
	gets     int
	versions []*string
}
type previewTestObject struct {
	data        []byte
	contentType string
}

func (s *previewTestStore) HeadObject(_ context.Context, _ storage.BucketName, k storage.ObjectKey, _ *storage.HeadObjectOptions) (*storage.Object, error) {
	object, ok := s.objects[k.String()]
	if !ok {
		return nil, storage.ErrNoSuchKey
	}
	version := "v1"
	result := &storage.Object{Key: k, Size: int64(len(object.data)), ETag: `"etag"`, VersionID: &version}
	if object.contentType != "" {
		result.ContentType = &object.contentType
	}
	return result, nil
}

func (s *previewTestStore) GetObject(_ context.Context, _ storage.BucketName, k storage.ObjectKey, ranges []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	s.gets++
	s.versions = append(s.versions, opts.VersionID)
	data := s.objects[k.String()].data
	if len(ranges) == 1 {
		start, end := *ranges[0].Start, int64(len(data))
		if ranges[0].End != nil {
			end = *ranges[0].End
		}
		data = data[start:end]
	}
	return &storage.Object{Key: k}, []io.ReadCloser{io.NopCloser(bytes.NewReader(data))}, nil
}

func TestPreviewClassification(t *testing.T) {
	text := "text/html"
	svg := "image/svg+xml"
	video := "video/mp4"
	generic := "application/octet-stream"
	for _, tc := range []struct {
		contentType *string
		key         string
		kind, typ   string
	}{
		{&video, "clip", previewVideo, "video/mp4"},
		{&generic, "photo.PNG", previewImage, "image/png"},
		{nil, "movie.mp4", previewVideo, "video/mp4"},
		{nil, "song.mp3", previewAudio, "audio/mpeg"},
		{&text, "page.html", previewText, "text/plain; charset=utf-8"},
		{&svg, "logo.svg", previewText, "text/plain; charset=utf-8"},
		{nil, "notes.md", previewText, "text/plain; charset=utf-8"},
		{nil, "archive.bin", "", ""},
		{nil, "report.pdf", "", ""},
	} {
		kind, typ := previewOf(tc.contentType, tc.key)
		require.Equal(t, tc.kind, kind, tc.key)
		require.Equal(t, tc.typ, typ, tc.key)
	}
}

func TestViewServesSandboxedInlineContentWithRanges(t *testing.T) {
	store := &previewTestStore{objects: map[string]previewTestObject{
		"clip.mp4":  {data: []byte("0123456789"), contentType: "video/mp4"},
		"page.html": {data: []byte("<script>alert(1)</script>"), contentType: "text/html"},
		"blob.bin":  {data: []byte{0, 1, 2}},
	}}
	app := &App{options: Options{Storage: store}}
	mux := http.NewServeMux()
	app.registerObjects(mux)

	request := httptest.NewRequest(http.MethodGet, "/objects/view?bucket=bucket&key=clip.mp4", nil)
	request.Header.Set("Range", "bytes=4-6")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	require.Equal(t, http.StatusPartialContent, response.Code)
	require.Equal(t, "456", response.Body.String())
	require.Equal(t, "bytes 4-6/10", response.Header().Get("Content-Range"))
	require.Equal(t, "video/mp4", response.Header().Get("Content-Type"))
	require.Contains(t, response.Header().Get("Content-Security-Policy"), "sandbox")
	require.True(t, strings.HasPrefix(response.Header().Get("Content-Disposition"), "inline"))
	require.Equal(t, "v1", *store.versions[0], "ranges must read the inspected version")

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/objects/view?bucket=bucket&key=page.html", nil))
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "text/plain; charset=utf-8", response.Header().Get("Content-Type"), "HTML must never render as HTML")
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))

	response = httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/objects/view?bucket=bucket&key=blob.bin", nil))
	require.Equal(t, http.StatusUnsupportedMediaType, response.Code)
}

func TestTextPreviewIsBoundedAndValidUTF8(t *testing.T) {
	long := strings.Repeat("a", textPreviewLimit-1) + "é" + "tail"
	store := &previewTestStore{objects: map[string]previewTestObject{"long.txt": {data: []byte(long)}, "short.txt": {data: []byte("hi")}}}
	app := &App{options: Options{Storage: store}}
	text, truncated, err := app.textPreview(t.Context(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("long.txt"), nil, int64(len(long)))
	require.NoError(t, err)
	require.True(t, truncated)
	require.Equal(t, strings.Repeat("a", textPreviewLimit-1), text, "a split character is dropped")
	text, truncated, err = app.textPreview(t.Context(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("short.txt"), nil, 2)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Equal(t, "hi", text)
	gets := store.gets
	text, _, err = app.textPreview(t.Context(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("short.txt"), nil, 0)
	require.NoError(t, err)
	require.Empty(t, text)
	require.Equal(t, gets, store.gets, "empty objects are not read")
}
