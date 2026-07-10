package server

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) getBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketVersioningHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	if s.authorizeRequest(ctx, authorization.OperationGetBucketVersioning, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}

	config, err := s.storage.GetBucketVersioningConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := BucketVersioningConfiguration{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	if config.Status != nil {
		status := string(*config.Status)
		response.Status = &status
	}

	writeXMLResponse(w, r, http.StatusOK, response)
}
func (s *Server) putBucketVersioningHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketVersioningHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	if s.authorizeRequest(ctx, authorization.OperationPutBucketVersioning, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}

	data, err := readLimitedBody(r, w, maxPutBucketVersioningBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request BucketVersioningConfiguration
	if err := xml.Unmarshal(data, &request); err != nil {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	if request.Status == nil {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	var status storage.BucketVersioningStatus
	switch *request.Status {
	case string(storage.BucketVersioningStatusEnabled):
		status = storage.BucketVersioningStatusEnabled
	case string(storage.BucketVersioningStatusSuspended):
		status = storage.BucketVersioningStatusSuspended
	default:
		handleError(ErrInvalidRequest, w, r)
		return
	}

	err = s.storage.PutBucketVersioningConfiguration(ctx, bucketName, &storage.BucketVersioningConfiguration{Status: &status})
	if err != nil {
		handleError(err, w, r)
		return
	}

	w.WriteHeader(http.StatusOK)
}
func (s *Server) listObjectVersionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listObjectVersionsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	if s.authorizeRequest(ctx, authorization.OperationListObjectVersions, ptrutils.ToPtr(bucketName.String()), nil, w, r) {
		return
	}

	query := r.URL.Query()
	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	keyMarker := httputils.GetQueryParam(query, keyMarkerQuery)
	versionIDMarker := httputils.GetQueryParam(query, "version-id-marker")

	maxKeys := query.Get(maxKeysQuery)
	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 || maxKeysI64 > maxListLimit {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	result, err := s.storage.ListObjectVersions(ctx, bucketName, storage.ListObjectVersionsOptions{Prefix: prefix, Delimiter: delimiter, KeyMarker: keyMarker, VersionIDMarker: versionIDMarker, MaxKeys: maxKeysI32})
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := ListObjectVersionsResult{
		Name:                bucketName.String(),
		Prefix:              prefix,
		Delimiter:           delimiter,
		KeyMarker:           keyMarker,
		VersionIDMarker:     versionIDMarker,
		NextKeyMarker:       result.NextKeyMarker,
		NextVersionIDMarker: result.NextVersionIDMarker,
		MaxKeys:             maxKeysI32,
		IsTruncated:         result.IsTruncated,
		Versions:            []*VersionEntry{},
		DeleteMarkers:       []*DeleteMarkerVersionEntry{},
		CommonPrefixes:      []*CommonPrefixResult{},
	}

	for _, version := range result.Versions {
		if version.IsDeleteMarker {
			response.DeleteMarkers = append(response.DeleteMarkers, &DeleteMarkerVersionEntry{Key: version.Key.String(), VersionID: version.VersionID, IsLatest: version.IsLatest, LastModified: version.LastModified.UTC().Format(time.RFC3339)})
		} else {
			etag := ""
			if version.ETag != nil {
				etag = *version.ETag
			}
			response.Versions = append(response.Versions, &VersionEntry{Key: version.Key.String(), VersionID: version.VersionID, IsLatest: version.IsLatest, LastModified: version.LastModified.UTC().Format(time.RFC3339), ETag: etag, Size: version.Size, StorageClass: storage.EffectiveStorageClass(version.StorageClass)})
		}
	}
	for _, commonPrefix := range result.CommonPrefixes {
		response.CommonPrefixes = append(response.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}

	writeXMLResponse(w, r, http.StatusOK, response)
}
