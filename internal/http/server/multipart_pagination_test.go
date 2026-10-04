package server

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type multipartPaginationStorage struct {
	storage.Storage
	calls  int
	result *storage.ListMultipartUploadsResult
}

func (s *multipartPaginationStorage) ListMultipartUploads(_ context.Context, _ storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	s.calls++
	return s.result, nil
}

func TestMultipartListingPreservesBackendPageAndMarkers(t *testing.T) {
	for _, prefixesOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "mixed-page", true: "prefix-only-page"}[prefixesOnly], func(t *testing.T) {
			page := &storage.ListMultipartUploadsResult{
				CommonPrefixes:     []string{"a/"},
				IsTruncated:        true,
				NextKeyMarker:      "b",
				NextUploadIdMarker: "upload-b",
				MaxUploads:         2,
			}
			if prefixesOnly {
				page.MaxUploads = 1
				page.NextKeyMarker = "a/"
				page.NextUploadIdMarker = ""
			} else {
				page.Uploads = []storage.Upload{{Key: storage.MustNewObjectKey("b"), UploadId: storage.MustNewUploadId("upload-b")}}
			}
			backend := &multipartPaginationStorage{result: page}
			server := &Server{storage: backend}
			result, keyMarker, uploadMarker, err := server.listAndFilterMultipartUploads(context.Background(), httptest.NewRequest("GET", "/bucket?uploads", nil), storage.MustNewBucketName("bucket"), storage.ListMultipartUploadsOptions{MaxUploads: page.MaxUploads})
			require.NoError(t, err)
			assert.Equal(t, 1, backend.calls)
			assert.Same(t, page, result)
			require.NotNil(t, keyMarker)
			require.NotNil(t, uploadMarker)
			assert.Equal(t, page.NextKeyMarker, *keyMarker)
			assert.Equal(t, page.NextUploadIdMarker, *uploadMarker)
		})
	}
}
