package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type multipartCompatibilityStorage struct {
	storage.Storage
	limits []int32
}

func (s *multipartCompatibilityStorage) HeadBucket(_ context.Context, name storage.BucketName) (*storage.Bucket, error) {
	return &storage.Bucket{Name: name, OwnerAccountID: "account"}, nil
}

func (s *multipartCompatibilityStorage) ListMultipartUploads(_ context.Context, name storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	s.limits = append(s.limits, opts.MaxUploads)
	return &storage.ListMultipartUploadsResult{BucketName: name, MaxUploads: opts.MaxUploads}, nil
}

func (s *multipartCompatibilityStorage) ListParts(_ context.Context, name storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	s.limits = append(s.limits, opts.MaxParts)
	return &storage.ListPartsResult{BucketName: name, Key: key, UploadId: uploadID, MaxParts: opts.MaxParts}, nil
}

func multipartCompatibilityHandler(t *testing.T, backend storage.Storage) http.Handler {
	t.Helper()
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request) return true end`)
	require.NoError(t, err)
	return SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, backend)
}

func TestMultipartListingLimits(t *testing.T) {
	for _, operation := range []struct {
		path, parameter string
	}{
		{"/bucket?uploads", "max-uploads"},
		{"/bucket/key?uploadId=upload", "max-parts"},
	} {
		for _, value := range []string{"omitted", "", "bad", "-1", "1.5", "2147483648", "1001", "0", "1", "1000"} {
			t.Run(operation.parameter+"/"+value, func(t *testing.T) {
				backend := &multipartCompatibilityStorage{}
				handler := multipartCompatibilityHandler(t, backend)
				path := operation.path
				if value != "omitted" {
					path += "&" + operation.parameter + "=" + value
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("GET", "http://s3.test"+path, nil))
				switch value {
				case "0":
					if operation.parameter == "max-uploads" {
						require.Equal(t, http.StatusBadRequest, response.Code)
						require.Contains(t, response.Body.String(), "InvalidArgument")
						require.Empty(t, backend.limits)
						return
					}
					require.Equal(t, http.StatusOK, response.Code)
					require.Equal(t, []int32{0}, backend.limits)
					var result ListPartsResult
					require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
					require.Zero(t, result.MaxParts)
					require.Empty(t, result.Parts)
				case "omitted", "1000", "1":
					require.Equal(t, http.StatusOK, response.Code, response.Body.String())
					limit := int32(1000)
					if value == "1" {
						limit = 1
					}
					require.Equal(t, []int32{limit}, backend.limits)
				default:
					require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
					var result ErrorResponse
					require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
					require.Equal(t, "InvalidArgument", result.Code)
					require.Empty(t, backend.limits)
				}
			})
		}
	}
}
