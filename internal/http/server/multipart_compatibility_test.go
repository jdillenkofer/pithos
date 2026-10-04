package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type multipartCompatibilityStorage struct {
	storage.Storage
	limits             []int32
	uploads            *storage.ListMultipartUploadsResult
	uploadOptions      []storage.ListMultipartUploadsOptions
	parts              *storage.ListPartsResult
	createOptions      *storage.CreateMultipartUploadOptions
	createChecksumType *string
}

func (s *multipartCompatibilityStorage) HeadBucket(_ context.Context, name storage.BucketName) (*storage.Bucket, error) {
	return &storage.Bucket{Name: name, OwnerAccountID: "account"}, nil
}

func (s *multipartCompatibilityStorage) ListMultipartUploads(_ context.Context, name storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, error) {
	s.limits = append(s.limits, opts.MaxUploads)
	s.uploadOptions = append(s.uploadOptions, opts)
	if s.uploads != nil {
		return s.uploads, nil
	}
	return &storage.ListMultipartUploadsResult{BucketName: name, MaxUploads: opts.MaxUploads}, nil
}

func (s *multipartCompatibilityStorage) ListParts(_ context.Context, name storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, error) {
	s.limits = append(s.limits, opts.MaxParts)
	if s.parts != nil {
		return s.parts, nil
	}
	return &storage.ListPartsResult{BucketName: name, Key: key, UploadId: uploadID, MaxParts: opts.MaxParts}, nil
}

func (s *multipartCompatibilityStorage) CreateMultipartUpload(_ context.Context, name storage.BucketName, key storage.ObjectKey, _ *string, checksumType *string, opts *storage.CreateMultipartUploadOptions) (*storage.InitiateMultipartUploadResult, error) {
	s.createOptions = opts
	s.createChecksumType = checksumType
	return &storage.InitiateMultipartUploadResult{UploadId: storage.MustNewUploadId("upload")}, nil
}

func TestMultipartInitiationPassesChecksumAlgorithm(t *testing.T) {
	backend := &multipartCompatibilityStorage{}
	request := httptest.NewRequest("POST", "http://s3.test/bucket/key?uploads", nil)
	request.Header.Set("x-amz-checksum-algorithm", "SHA256")
	request.Header.Set("x-amz-checksum-type", "COMPOSITE")
	response := httptest.NewRecorder()
	multipartCompatibilityHandler(t, backend).ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotNil(t, backend.createOptions)
	require.NotNil(t, backend.createOptions.ChecksumAlgorithm)
	require.Equal(t, "SHA256", *backend.createOptions.ChecksumAlgorithm)
	require.Equal(t, "COMPOSITE", *backend.createChecksumType)
}

func TestMultipartChecksumConfiguration(t *testing.T) {
	for _, tc := range []struct {
		algorithm, requestedType, effectiveType string
		status                                  int
	}{
		{"SHA256", "", "COMPOSITE", 200},
		{"SHA1", "", "COMPOSITE", 200},
		{"CRC32", "", "COMPOSITE", 200},
		{"CRC32C", "FULL_OBJECT", "FULL_OBJECT", 200},
		{"CRC64NVME", "", "FULL_OBJECT", 200},
		{"SHA256", "FULL_OBJECT", "", 400},
		{"CRC64NVME", "COMPOSITE", "", 400},
		{"unknown", "", "", 400},
		{"SHA256", "unknown", "", 400},
	} {
		t.Run(tc.algorithm+"/"+tc.requestedType, func(t *testing.T) {
			backend := &multipartCompatibilityStorage{}
			request := httptest.NewRequest("POST", "http://s3.test/bucket/key?uploads", nil)
			request.Header.Set("x-amz-checksum-algorithm", tc.algorithm)
			if tc.requestedType != "" {
				request.Header.Set("x-amz-checksum-type", tc.requestedType)
			}
			response := httptest.NewRecorder()
			multipartCompatibilityHandler(t, backend).ServeHTTP(response, request)
			require.Equal(t, tc.status, response.Code, response.Body.String())
			if tc.status == http.StatusOK {
				require.NotNil(t, backend.createChecksumType)
				require.Equal(t, tc.effectiveType, *backend.createChecksumType)
			} else {
				require.Nil(t, backend.createOptions)
				require.Contains(t, response.Body.String(), "InvalidRequest")
			}
		})
	}
}

func TestMultipartListingsExposeChecksumMetadata(t *testing.T) {
	algorithm, checksumType := "SHA256", "COMPOSITE"
	for _, path := range []string{"/bucket?uploads", "/bucket/key?uploadId=upload", "/bucket/key?uploadId=upload&max-parts=0"} {
		t.Run(path, func(t *testing.T) {
			backend := &multipartCompatibilityStorage{
				uploads: &storage.ListMultipartUploadsResult{BucketName: storage.MustNewBucketName("bucket"), Uploads: []storage.Upload{{Key: storage.MustNewObjectKey("key"), UploadId: storage.MustNewUploadId("upload"), ChecksumAlgorithm: &algorithm, ChecksumType: &checksumType}}},
				parts:   &storage.ListPartsResult{BucketName: storage.MustNewBucketName("bucket"), Key: storage.MustNewObjectKey("key"), UploadId: storage.MustNewUploadId("upload"), ChecksumAlgorithm: &algorithm, ChecksumType: &checksumType},
			}
			response := httptest.NewRecorder()
			multipartCompatibilityHandler(t, backend).ServeHTTP(response, httptest.NewRequest("GET", "http://s3.test"+path, nil))
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), "<ChecksumAlgorithm>SHA256</ChecksumAlgorithm>")
			require.Contains(t, response.Body.String(), "<ChecksumType>COMPOSITE</ChecksumType>")
		})
	}
}

func multipartCompatibilityHandler(t *testing.T, backend storage.Storage) http.Handler {
	t.Helper()
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request) return true end`)
	require.NoError(t, err)
	return SetupServer(nil, "us-east-1", "s3.test", "website.test", authorizer, backend)
}

func TestMultipartListingURLEncoding(t *testing.T) {
	key := "f ö/+()%\x01"
	page := &storage.ListMultipartUploadsResult{
		BucketName: storage.MustNewBucketName("bucket"),
		KeyMarker:  key, NextKeyMarker: key,
		Prefix: "f ö/", Delimiter: "/", CommonPrefixes: []string{"f ö/sub/"},
		IsTruncated: true, NextUploadIdMarker: "upload+id",
		Uploads: []storage.Upload{{Key: storage.MustNewObjectKey(key), UploadId: storage.MustNewUploadId("upload+id")}},
	}
	backend := &multipartCompatibilityStorage{uploads: page}
	query := url.Values{"uploads": {""}, "encoding-type": {"url"}, "prefix": {"f ö/"}, "key-marker": {key}, "delimiter": {"/"}}
	response := httptest.NewRecorder()
	multipartCompatibilityHandler(t, backend).ServeHTTP(response, httptest.NewRequest("GET", "http://s3.test/bucket?"+query.Encode(), nil))
	require.Equal(t, http.StatusOK, response.Code)
	var result struct {
		ListMultipartUploadsResult
		EncodingType string `xml:"EncodingType"`
	}
	require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, "url", result.EncodingType)
	require.Equal(t, "f%20%C3%B6%2F%2B%28%29%25%01", *result.KeyMarker)
	require.Equal(t, *result.KeyMarker, *result.NextKeyMarker)
	require.Equal(t, *result.KeyMarker, result.Uploads[0].Key)
	require.Equal(t, "f%20%C3%B6%2F", *result.Prefix)
	require.Equal(t, "%2F", *result.Delimiter)
	require.Equal(t, "f%20%C3%B6%2Fsub%2F", result.CommonPrefixes[0].Prefix)
	require.Equal(t, "upload+id", *result.NextUploadIdMarker)
	require.Equal(t, "upload+id", result.Uploads[0].UploadId)
	// Encoding is a wire concern: storage receives and retains raw keys.
	require.Equal(t, key, *backend.uploadOptions[0].KeyMarker)
	require.Equal(t, "f ö/", *backend.uploadOptions[0].Prefix)
	require.Equal(t, key, page.KeyMarker)
}

func TestMultipartListingEncodingTypeValidation(t *testing.T) {
	for _, value := range []string{"omitted", "", "URL", "base64"} {
		t.Run(value, func(t *testing.T) {
			backend := &multipartCompatibilityStorage{uploads: &storage.ListMultipartUploadsResult{
				BucketName: storage.MustNewBucketName("bucket"),
				Uploads:    []storage.Upload{{Key: storage.MustNewObjectKey("f ö/+%"), UploadId: storage.MustNewUploadId("upload")}},
			}}
			path := "http://s3.test/bucket?uploads"
			if value != "omitted" {
				path += "&encoding-type=" + value
			}
			response := httptest.NewRecorder()
			multipartCompatibilityHandler(t, backend).ServeHTTP(response, httptest.NewRequest("GET", path, nil))
			if value == "omitted" {
				require.Equal(t, http.StatusOK, response.Code)
				require.Contains(t, response.Body.String(), "<Key>f ö/+%</Key>")
				require.NotContains(t, response.Body.String(), "EncodingType")
				require.NotContains(t, response.Body.String(), "NextKeyMarker")
			} else {
				require.Equal(t, http.StatusBadRequest, response.Code)
				require.Contains(t, response.Body.String(), "InvalidArgument")
				require.Empty(t, backend.limits)
			}
		})
	}
}

func TestMultipartExpectedBucketOwner(t *testing.T) {
	for _, path := range []string{"/bucket?uploads", "/bucket/key?uploadId=upload"} {
		for _, tc := range []struct {
			name   string
			owners []string
			status int
		}{
			{"omitted", nil, http.StatusOK},
			{"matching", []string{"account"}, http.StatusOK},
			{"mismatching", []string{"other"}, http.StatusForbidden},
			{"empty", []string{""}, http.StatusForbidden},
			{"repeated", []string{"account", "other"}, http.StatusBadRequest},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				backend := &multipartCompatibilityStorage{}
				handler := multipartCompatibilityHandler(t, backend)
				request := httptest.NewRequest("GET", "http://s3.test"+path, nil)
				for _, owner := range tc.owners {
					request.Header.Add("x-amz-expected-bucket-owner", owner)
				}
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, tc.status, response.Code, response.Body.String())
				if tc.status != http.StatusOK {
					require.Empty(t, backend.limits)
					var result ErrorResponse
					require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &result))
					if tc.status == http.StatusForbidden {
						require.Equal(t, "AccessDenied", result.Code)
					} else {
						require.Equal(t, "InvalidArgument", result.Code)
					}
				}
			})
		}
	}
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
