package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type partChecksumStorage struct {
	objectAttributesStorage
}

func (s *partChecksumStorage) GetObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, _ []storage.ByteRange, opts *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	object := *s.object
	if opts != nil && opts.PartNumber != nil {
		object.Size = object.PartSizes[*opts.PartNumber-1]
	}
	return &object, []io.ReadCloser{io.NopCloser(strings.NewReader(strings.Repeat("x", int(object.Size))))}, nil
}

func TestPartNumberChecksums(t *testing.T) {
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			serve := func(object *storage.Object, target string) *httptest.ResponseRecorder {
				store := &partChecksumStorage{objectAttributesStorage{object: object}}
				handler := SetupServer(nil, "eu-central-1", "s3.test", "website.test", &captureAuthorizer{allowed: true}, store)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(method, "http://s3.test"+target, nil))
				return w
			}

			t.Run("multipart part reports its own checksums", func(t *testing.T) {
				w := serve(compositeMultipartObject(), "/bucket/key?partNumber=2")
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Equal(t, "sha2", w.Header().Get(checksumSHA256Header))
				require.Empty(t, w.Header().Get(checksumCRC32Header), "only algorithms of the object checksum are reported")
				require.Empty(t, w.Header().Get(checksumTypeHeader))
				require.Equal(t, "3", w.Header().Get("x-amz-mp-parts-count"))
				require.Equal(t, "5", w.Header().Get(contentLengthHeader))
			})
			t.Run("without part checksums nothing is reported", func(t *testing.T) {
				object := compositeMultipartObject()
				object.PartChecksums = nil
				w := serve(object, "/bucket/key?partNumber=2")
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Empty(t, w.Header().Get(checksumSHA256Header))
				require.Empty(t, w.Header().Get(checksumCRC32Header))
			})
			t.Run("whole multipart object keeps the object checksum", func(t *testing.T) {
				w := serve(compositeMultipartObject(), "/bucket/key")
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Equal(t, "c2hhMjU2LW9mLXNoYTI1NnM=-3", w.Header().Get(checksumSHA256Header))
			})
			t.Run("single part object keeps the object checksum", func(t *testing.T) {
				w := serve(singlePartObject(), "/bucket/key?partNumber=1")
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				require.Equal(t, "AAAAAA==", w.Header().Get(checksumCRC32Header))
				require.Empty(t, w.Header().Get("x-amz-mp-parts-count"))
			})
		})
	}
}
