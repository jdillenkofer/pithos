package server

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

type objectAttributesStorage struct {
	accountStorage
	object *storage.Object
	err    error
	opts   *storage.HeadObjectOptions
	calls  int
}

func (s *objectAttributesStorage) HeadObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, opts *storage.HeadObjectOptions) (*storage.Object, error) {
	s.calls++
	s.opts = opts
	if s.err != nil {
		return nil, s.err
	}
	return s.object, nil
}

var objectAttributesLastModified = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

func singlePartObject() *storage.Object {
	return &storage.Object{
		ETag:          `"9b2cf535f27731c974343645a3985328"`,
		ChecksumCRC32: ptrutils.ToPtr("AAAAAA=="),
		ChecksumType:  ptrutils.ToPtr(storage.ChecksumTypeFullObject),
		Size:          11,
		LastModified:  objectAttributesLastModified,
		VersionID:     ptrutils.ToPtr("v1"),
		PartSizes:     []int64{11},
		PartChecksums: []storage.ChecksumValues{{ChecksumCRC32: ptrutils.ToPtr("AAAAAA==")}},
	}
}

func compositeMultipartObject() *storage.Object {
	return &storage.Object{
		ETag:           `"0123456789abcdef0123456789abcdef-3"`,
		ChecksumSHA256: ptrutils.ToPtr("c2hhMjU2LW9mLXNoYTI1NnM=-3"),
		ChecksumType:   ptrutils.ToPtr(storage.ChecksumTypeComposite),
		Size:           12,
		LastModified:   objectAttributesLastModified,
		StorageClass:   ptrutils.ToPtr("STANDARD_IA"),
		PartSizes:      []int64{5, 5, 2},
		PartChecksums: []storage.ChecksumValues{
			{ChecksumCRC32: ptrutils.ToPtr("crc1"), ChecksumSHA256: ptrutils.ToPtr("sha1")},
			{ChecksumCRC32: ptrutils.ToPtr("crc2"), ChecksumSHA256: ptrutils.ToPtr("sha2")},
			{ChecksumCRC32: ptrutils.ToPtr("crc3"), ChecksumSHA256: ptrutils.ToPtr("sha3")},
		},
	}
}

func serveObjectAttributes(t *testing.T, store storage.Storage, authorizer authorization.RequestAuthorizer, target string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	handler := SetupServer(nil, "eu-central-1", "s3.test", "website.test", authorizer, store)
	r := httptest.NewRequest(http.MethodGet, "http://s3.test"+target, nil)
	for name, value := range headers {
		r.Header.Set(name, value)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func decodeObjectAttributes(t *testing.T, w *httptest.ResponseRecorder) GetObjectAttributesResponse {
	t.Helper()
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var response GetObjectAttributesResponse
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &response))
	require.Equal(t, "http://s3.amazonaws.com/doc/2006-03-01/", response.XMLName.Space)
	return response
}

func TestGetObjectAttributesSinglePartObject(t *testing.T) {
	store := &objectAttributesStorage{object: singlePartObject()}
	w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{
		objectAttributesHeader: "ETag, Checksum,ObjectParts,StorageClass,ObjectSize",
	})
	response := decodeObjectAttributes(t, w)

	require.Equal(t, "9b2cf535f27731c974343645a3985328", *response.ETag)
	require.Equal(t, "AAAAAA==", *response.Checksum.ChecksumCRC32)
	require.Equal(t, storage.ChecksumTypeFullObject, *response.Checksum.ChecksumType)
	require.Nil(t, response.ObjectParts, "single-part objects have no ObjectParts")
	require.Equal(t, "STANDARD", *response.StorageClass)
	require.Equal(t, int64(11), *response.ObjectSize)
	require.Equal(t, "Fri, 06 Nov 2026 12:00:00 GMT", w.Header().Get(lastModifiedHeader))
	require.Equal(t, "v1", w.Header().Get(versionIDHeader))
	require.Empty(t, w.Header().Get(deleteMarkerHeader))
}

func TestGetObjectAttributesReturnsOnlyRequestedAttributes(t *testing.T) {
	store := &objectAttributesStorage{object: singlePartObject()}
	w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{
		objectAttributesHeader: "ObjectSize",
	})
	response := decodeObjectAttributes(t, w)
	require.Equal(t, int64(11), *response.ObjectSize)
	require.Nil(t, response.ETag)
	require.Nil(t, response.Checksum)
	require.Nil(t, response.StorageClass)
	require.NotContains(t, w.Body.String(), "<ETag>")
}

func TestGetObjectAttributesMultipartObject(t *testing.T) {
	store := &objectAttributesStorage{object: compositeMultipartObject()}
	w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{
		objectAttributesHeader: "ETag,Checksum,ObjectParts,StorageClass",
	})
	response := decodeObjectAttributes(t, w)

	require.Equal(t, "0123456789abcdef0123456789abcdef-3", *response.ETag)
	require.Equal(t, "c2hhMjU2LW9mLXNoYTI1NnM=", *response.Checksum.ChecksumSHA256, "composite checksums carry no part count")
	require.Equal(t, storage.ChecksumTypeComposite, *response.Checksum.ChecksumType)
	require.Equal(t, "STANDARD_IA", *response.StorageClass)

	parts := response.ObjectParts
	require.Equal(t, int32(3), parts.PartsCount)
	require.Equal(t, int32(0), *parts.PartNumberMarker)
	require.Equal(t, int32(3), *parts.NextPartNumberMarker)
	require.Equal(t, int32(1000), *parts.MaxParts)
	require.False(t, *parts.IsTruncated)
	require.Len(t, parts.Parts, 3)
	for i, part := range parts.Parts {
		require.Equal(t, int32(i+1), part.PartNumber)
		require.Equal(t, store.object.PartSizes[i], part.Size)
		require.Equal(t, *store.object.PartChecksums[i].ChecksumSHA256, *part.ChecksumSHA256)
		require.Nil(t, part.ChecksumCRC32, "only algorithms of the object checksum are reported")
	}
}

func TestGetObjectAttributesPaginatesParts(t *testing.T) {
	for _, tc := range []struct {
		name, maxParts, marker string
		wantParts              []int32
		wantTruncated          bool
		wantNext               *int32
	}{
		{"first page", "2", "", []int32{1, 2}, true, ptrutils.ToPtr(int32(2))},
		{"last page", "2", "2", []int32{3}, false, ptrutils.ToPtr(int32(3))},
		{"past the end", "2", "3", nil, false, nil},
		{"zero parts", "0", "", nil, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := map[string]string{objectAttributesHeader: "ObjectParts", maxPartsHeader: tc.maxParts}
			if tc.marker != "" {
				headers[partNumberMarkerHeader] = tc.marker
			}
			store := &objectAttributesStorage{object: compositeMultipartObject()}
			parts := decodeObjectAttributes(t, serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", headers)).ObjectParts
			var numbers []int32
			for _, part := range parts.Parts {
				numbers = append(numbers, part.PartNumber)
			}
			require.Equal(t, tc.wantParts, numbers)
			require.Equal(t, tc.wantTruncated, *parts.IsTruncated)
			require.Equal(t, tc.wantNext, parts.NextPartNumberMarker)
			require.Equal(t, int32(3), parts.PartsCount)
		})
	}
}

func TestGetObjectAttributesWithoutPartBoundaries(t *testing.T) {
	object := compositeMultipartObject()
	object.PartSizes, object.PartChecksums = nil, nil
	store := &objectAttributesStorage{object: object}
	w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{
		objectAttributesHeader: "ObjectParts",
	})
	parts := decodeObjectAttributes(t, w).ObjectParts
	require.Equal(t, int32(3), parts.PartsCount)
	require.Empty(t, parts.Parts)
	require.Nil(t, parts.IsTruncated)
}

func TestGetObjectAttributesRejectsInvalidHeaders(t *testing.T) {
	for _, tc := range []struct {
		name    string
		headers map[string]string
	}{
		{"missing attributes", map[string]string{}},
		{"empty attributes", map[string]string{objectAttributesHeader: ""}},
		{"unknown attribute", map[string]string{objectAttributesHeader: "ETag,Owner"}},
		{"lowercase attribute", map[string]string{objectAttributesHeader: "etag"}},
		{"malformed max parts", map[string]string{objectAttributesHeader: "ETag", maxPartsHeader: "many"}},
		{"negative max parts", map[string]string{objectAttributesHeader: "ETag", maxPartsHeader: "-1"}},
		{"excessive max parts", map[string]string{objectAttributesHeader: "ETag", maxPartsHeader: "1001"}},
		{"malformed part number marker", map[string]string{objectAttributesHeader: "ETag", partNumberMarkerHeader: "x"}},
		{"excessive part number marker", map[string]string{objectAttributesHeader: "ETag", partNumberMarkerHeader: "10001"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &objectAttributesStorage{object: singlePartObject()}
			w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", tc.headers)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "InvalidArgument")
			require.Zero(t, store.calls)
		})
	}
}

func TestGetObjectAttributesAuthorization(t *testing.T) {
	t.Run("current version", func(t *testing.T) {
		authorizer := &captureAuthorizer{allowed: true}
		store := &objectAttributesStorage{object: singlePartObject()}
		w := serveObjectAttributes(t, store, authorizer, "/bucket/key?attributes", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, authorization.OperationGetObjectAttributes, authorizer.request.Operation)
		require.Equal(t, "key", *authorizer.request.Key)
		require.Nil(t, authorizer.request.VersionID)
		require.Nil(t, store.opts)
	})
	t.Run("explicit version", func(t *testing.T) {
		authorizer := &captureAuthorizer{allowed: true}
		store := &objectAttributesStorage{object: singlePartObject()}
		w := serveObjectAttributes(t, store, authorizer, "/bucket/key?attributes&versionId=v1", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusOK, w.Code)
		require.Equal(t, authorization.OperationGetObjectVersionAttributes, authorizer.request.Operation)
		require.Equal(t, "v1", *authorizer.request.VersionID)
		require.Equal(t, "v1", *store.opts.VersionID)
	})
	t.Run("denied", func(t *testing.T) {
		store := &objectAttributesStorage{object: singlePartObject()}
		w := serveObjectAttributes(t, store, &captureAuthorizer{}, "/bucket/key?attributes", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusForbidden, w.Code)
		require.Zero(t, store.calls)
	})
}

func TestGetObjectAttributesConditionalHeaders(t *testing.T) {
	for _, tc := range []struct {
		name        string
		ifMatch     *string
		ifNoneMatch *string
		versionID   *string
		storageErr  error
		wantCode    int
	}{
		{"matching ETag", ptrutils.ToPtr(`"matching"`), nil, nil, nil, http.StatusOK},
		{"mismatching ETag", ptrutils.ToPtr(`"different"`), nil, nil, storage.ErrPreconditionFailed, http.StatusPreconditionFailed},
		{"unchanged ETag", nil, ptrutils.ToPtr(`"matching"`), nil, storage.ErrNotModified, http.StatusNotModified},
		{"changed ETag", nil, ptrutils.ToPtr(`"different"`), nil, nil, http.StatusOK},
		{"both conditions with explicit version", ptrutils.ToPtr(`"matching"`), ptrutils.ToPtr(`"different"`), ptrutils.ToPtr("v1"), nil, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &objectAttributesStorage{object: singlePartObject(), err: tc.storageErr}
			headers := map[string]string{objectAttributesHeader: "ETag"}
			if tc.ifMatch != nil {
				headers[ifMatchHeader] = *tc.ifMatch
			}
			if tc.ifNoneMatch != nil {
				headers[ifNoneMatchHeader] = *tc.ifNoneMatch
			}
			target := "/bucket/key?attributes"
			if tc.versionID != nil {
				target += "&versionId=" + *tc.versionID
			}
			w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, target, headers)
			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			require.Equal(t, &storage.HeadObjectOptions{
				VersionID: tc.versionID, IfMatchETag: tc.ifMatch, IfNoneMatchETag: tc.ifNoneMatch,
			}, store.opts)
			if tc.wantCode == http.StatusNotModified {
				require.Empty(t, w.Body.String())
			}
			if tc.wantCode == http.StatusPreconditionFailed {
				require.Contains(t, w.Body.String(), "<Code>PreconditionFailed</Code>")
			}
		})
	}
}

func TestGetObjectAttributesErrors(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		store := &objectAttributesStorage{err: storage.ErrNoSuchKey}
		w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusNotFound, w.Code)
		require.Contains(t, w.Body.String(), "NoSuchKey")
		require.Empty(t, w.Header().Get(deleteMarkerHeader))
	})
	t.Run("current delete marker", func(t *testing.T) {
		store := &objectAttributesStorage{err: &storage.CurrentDeleteMarkerError{VersionID: "dm"}}
		w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusNotFound, w.Code)
		require.Contains(t, w.Body.String(), "NoSuchKey")
		require.Equal(t, "true", w.Header().Get(deleteMarkerHeader))
		require.Equal(t, "dm", w.Header().Get(versionIDHeader))
	})
	t.Run("delete marker version", func(t *testing.T) {
		store := &objectAttributesStorage{err: &storage.VersionDeleteMarkerMethodNotAllowedError{VersionID: "dm", LastModified: objectAttributesLastModified}}
		w := serveObjectAttributes(t, store, &captureAuthorizer{allowed: true}, "/bucket/key?attributes&versionId=dm", map[string]string{objectAttributesHeader: "ETag"})
		require.Equal(t, http.StatusMethodNotAllowed, w.Code)
		require.Contains(t, w.Body.String(), "MethodNotAllowed")
		require.Equal(t, "true", w.Header().Get(deleteMarkerHeader))
		require.Equal(t, "dm", w.Header().Get(versionIDHeader))
		require.Equal(t, "Fri, 06 Nov 2026 12:00:00 GMT", w.Header().Get(lastModifiedHeader))
	})
}

func TestObjectAttributesOnlySupportsGet(t *testing.T) {
	for _, tc := range []struct {
		name, method, target string
		route                func(*Server, http.ResponseWriter, *http.Request)
	}{
		{"put", http.MethodPut, "/bucket/key?attributes", (*Server).uploadPartOrPutObjectHandler},
		{"delete", http.MethodDelete, "/bucket/key?attributes", (*Server).abortMultipartUploadOrDeleteObjectHandler},
		{"post", http.MethodPost, "/bucket/key?attributes", (*Server).createMultipartUploadOrCompleteMultipartUploadHandler},
		{"get combined with acl", http.MethodGet, "/bucket/key?attributes&acl", (*Server).getObjectOrListPartsHandler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.route(&Server{}, w, httptest.NewRequest(tc.method, tc.target, nil))
			require.Equal(t, http.StatusNotImplemented, w.Code)
		})
	}
}
