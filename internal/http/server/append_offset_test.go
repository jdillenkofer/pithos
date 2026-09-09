package server

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

type appendRouteStorage struct {
	storage.Storage
	calls  int
	offset *int64
}

func (s *appendRouteStorage) AppendObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, data io.Reader, checksum *storage.ChecksumInput, opts *storage.AppendObjectOptions) (*storage.AppendObjectResult, error) {
	s.calls++
	if opts != nil {
		s.offset = opts.WriteOffset
	}
	return &storage.AppendObjectResult{ETag: `"etag"`, Size: 4}, nil
}
func TestAppendOffsetRouting(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		values    []string
		status    int
		offset    *int64
	}{
		{"header", "/bucket/key", []string{"3"}, 200, int64Pointer(3)},
		{"zero", "/bucket/key", []string{"0"}, 200, int64Pointer(0)},
		{"legacy query", "/bucket/key?append", nil, 200, nil},
		{"empty", "/bucket/key", []string{""}, 400, nil},
		{"negative", "/bucket/key", []string{"-1"}, 400, nil},
		{"invalid", "/bucket/key", []string{"bad"}, 400, nil},
		{"overflow", "/bucket/key", []string{"9223372036854775808"}, 400, nil},
		{"duplicate", "/bucket/key", []string{"3", "4"}, 400, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Only AppendObject is allowed: the header must also select its permission.
			authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request) return request.operation == "AppendObject" end`)
			require.NoError(t, err)
			backend := &appendRouteStorage{}
			server := &Server{storage: backend, requestAuthorizer: authorizer, tracer: otel.Tracer("append-test")}
			request := httptest.NewRequest("PUT", tc.url, strings.NewReader("data"))
			request.SetPathValue(bucketPath, "bucket")
			request.SetPathValue(keyPath, "key")
			if tc.values != nil {
				request.Header["X-Amz-Write-Offset-Bytes"] = tc.values
			}
			response := httptest.NewRecorder()
			server.uploadPartOrPutObjectHandler(response, request)
			require.Equal(t, tc.status, response.Code)
			if tc.status == 200 {
				require.Equal(t, 1, backend.calls)
				require.Equal(t, tc.offset, backend.offset)
			} else {
				require.Zero(t, backend.calls)
			}
		})
	}
}
func int64Pointer(value int64) *int64 { return &value }
