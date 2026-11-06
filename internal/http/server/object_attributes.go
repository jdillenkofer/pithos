package server

import (
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

const (
	objectAttributeETag         = "ETag"
	objectAttributeChecksum     = "Checksum"
	objectAttributeObjectParts  = "ObjectParts"
	objectAttributeStorageClass = "StorageClass"
	objectAttributeObjectSize   = "ObjectSize"
)

var validObjectAttributes = []string{objectAttributeETag, objectAttributeChecksum, objectAttributeObjectParts, objectAttributeStorageClass, objectAttributeObjectSize}

// getObjectAttributesHandler answers GetObjectAttributes: the attributes named
// in x-amz-object-attributes, combining HeadObject with a listing of the
// parts of a completed multipart object.
func (s *Server) getObjectAttributesHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getObjectAttributesHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	key, err := storage.NewObjectKey(r.PathValue(keyPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	versionID := httputils.GetQueryParam(r.URL.Query(), versionIDQuery)
	authOperation := authorization.OperationGetObjectAttributes
	if versionID != nil {
		authOperation = authorization.OperationGetObjectVersionAttributes
	}
	authorizationRequest, isAuthenticated := makeAuthorizationRequest(ctx, authOperation, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), r)
	s.bindExistingObjectTagsResolver(authorizationRequest, authorizationRequest.Bucket, authorizationRequest.Key, versionID)
	if s.runAuthorization(ctx, authorizationRequest, isAuthenticated, w, r) {
		return
	}

	attributes, ok := parseObjectAttributesHeader(r.Header)
	if !ok {
		writeS3ErrorResponse(w, r, http.StatusBadRequest, "InvalidArgument", "Invalid attribute name specified.", r.URL.Path)
		return
	}
	maxParts, err := parseIntHeader(r.Header, maxPartsHeader, 0, maxListLimit, maxListLimit)
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberMarker, err := parseIntHeader(r.Header, partNumberMarkerHeader, 0, 10000, 0)
	if err != nil {
		handleError(err, w, r)
		return
	}

	slog.InfoContext(r.Context(), "Getting object attributes", "bucket", bucketName.String(), "key", key.String())

	var headOpts *storage.HeadObjectOptions
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	if versionID != nil || ifMatch != nil || ifNoneMatch != nil {
		headOpts = &storage.HeadObjectOptions{
			VersionID:       versionID,
			IfMatchETag:     ifMatch,
			IfNoneMatchETag: ifNoneMatch,
		}
	}
	object, err := s.storage.HeadObject(ctx, bucketName, key, headOpts)
	if err != nil {
		if err == storage.ErrNotModified {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		if currentDeleteMarkerErr, ok := err.(*storage.CurrentDeleteMarkerError); ok {
			w.Header().Set(deleteMarkerHeader, "true")
			if currentDeleteMarkerErr.VersionID != "" {
				w.Header().Set(versionIDHeader, currentDeleteMarkerErr.VersionID)
			}
			handleError(storage.ErrNoSuchKey, w, r)
			return
		}
		if versionDeleteMarkerErr, ok := err.(*storage.VersionDeleteMarkerMethodNotAllowedError); ok {
			w.Header().Set(deleteMarkerHeader, "true")
			if versionDeleteMarkerErr.VersionID != "" {
				w.Header().Set(versionIDHeader, versionDeleteMarkerErr.VersionID)
			}
			w.Header().Set(lastModifiedHeader, versionDeleteMarkerErr.LastModified.UTC().Format(http.TimeFormat))
			writeS3ErrorResponse(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "The specified method is not allowed against this resource.", r.URL.Path)
			return
		}
		handleError(err, w, r)
		return
	}

	response := GetObjectAttributesResponse{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	if attributes[objectAttributeETag] {
		// S3 returns the ETag here without the quotes of the ETag header.
		response.ETag = ptrutils.ToPtr(strings.Trim(object.ETag, `"`))
	}
	if attributes[objectAttributeChecksum] {
		response.Checksum = objectAttributesChecksum(object)
	}
	if attributes[objectAttributeObjectParts] {
		response.ObjectParts = objectAttributesParts(object, int32(maxParts), int32(partNumberMarker))
	}
	if attributes[objectAttributeStorageClass] {
		response.StorageClass = ptrutils.ToPtr(storage.EffectiveStorageClass(object.StorageClass))
	}
	if attributes[objectAttributeObjectSize] {
		response.ObjectSize = ptrutils.ToPtr(object.Size)
	}

	responseHeaders := w.Header()
	responseHeaders.Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	if object.VersionID != nil {
		responseHeaders.Set(versionIDHeader, *object.VersionID)
	}
	writeXMLResponse(w, r, http.StatusOK, response)
}

// parseObjectAttributesHeader parses the comma-separated attribute names of
// x-amz-object-attributes, which may also be repeated. It fails for unknown
// names and when no attribute is requested.
func parseObjectAttributesHeader(headers http.Header) (map[string]bool, bool) {
	attributes := map[string]bool{}
	for _, value := range headers.Values(objectAttributesHeader) {
		for attribute := range strings.SplitSeq(value, ",") {
			attribute = strings.TrimSpace(attribute)
			if !slices.Contains(validObjectAttributes, attribute) {
				return nil, false
			}
			attributes[attribute] = true
		}
	}
	return attributes, len(attributes) > 0
}

// parseIntHeader parses an optional integer header within [minimum, maximum].
// Only an absent header selects the default value.
func parseIntHeader(headers http.Header, name string, minimum, maximum, defaultValue int64) (int64, error) {
	values := headers.Values(name)
	if len(values) == 0 {
		return defaultValue, nil
	}
	if len(values) != 1 {
		return 0, ErrInvalidArgument
	}
	value, err := strconv.ParseInt(values[0], 10, 32)
	if err != nil || value < minimum || value > maximum {
		return 0, ErrInvalidArgument
	}
	return value, nil
}

// objectAttributesChecksum returns the object's checksums, or nil when it has
// none. Unlike the checksum headers, composite checksums carry no part count
// suffix here.
func objectAttributesChecksum(object *storage.Object) *ObjectAttributesChecksum {
	composite := object.ChecksumType != nil && *object.ChecksumType == storage.ChecksumTypeComposite
	value := func(checksum *string) *string {
		if checksum == nil || !composite {
			return checksum
		}
		withoutPartCount, _, _ := strings.Cut(*checksum, "-")
		return &withoutPartCount
	}
	checksum := &ObjectAttributesChecksum{
		ChecksumCRC32:     value(object.ChecksumCRC32),
		ChecksumCRC32C:    value(object.ChecksumCRC32C),
		ChecksumCRC64NVME: value(object.ChecksumCRC64NVME),
		ChecksumSHA1:      value(object.ChecksumSHA1),
		ChecksumSHA256:    value(object.ChecksumSHA256),
		ChecksumType:      object.ChecksumType,
	}
	if checksum.ChecksumCRC32 == nil && checksum.ChecksumCRC32C == nil && checksum.ChecksumCRC64NVME == nil &&
		checksum.ChecksumSHA1 == nil && checksum.ChecksumSHA256 == nil {
		return nil
	}
	return checksum
}

// objectAttributesParts returns the parts page of a multipart object, or nil
// for objects uploaded in a single part. Part checksums are limited to the
// algorithms the object itself reports.
func objectAttributesParts(object *storage.Object, maxParts int32, partNumberMarker int32) *ObjectAttributesParts {
	_, partsCountSuffix, multipart := strings.Cut(strings.Trim(object.ETag, `"`), "-")
	if !multipart {
		return nil
	}
	partsCount := int32(len(object.PartSizes))
	if partsCount == 0 {
		// Backends that do not expose part boundaries still report the count
		// encoded in the ETag.
		count, err := strconv.ParseInt(partsCountSuffix, 10, 32)
		if err != nil {
			return nil
		}
		return &ObjectAttributesParts{PartsCount: int32(count)}
	}

	result := &ObjectAttributesParts{
		PartsCount:       partsCount,
		PartNumberMarker: ptrutils.ToPtr(partNumberMarker),
		MaxParts:         ptrutils.ToPtr(maxParts),
		Parts:            []*ObjectAttributesPart{},
	}
	for partNumber := partNumberMarker + 1; partNumber <= partsCount && int32(len(result.Parts)) < maxParts; partNumber++ {
		part := &ObjectAttributesPart{PartNumber: partNumber, Size: object.PartSizes[partNumber-1]}
		if int(partNumber) <= len(object.PartChecksums) {
			checksums := object.PartChecksums[partNumber-1]
			part.ChecksumCRC32 = checksumIfReported(object.ChecksumCRC32, checksums.ChecksumCRC32)
			part.ChecksumCRC32C = checksumIfReported(object.ChecksumCRC32C, checksums.ChecksumCRC32C)
			part.ChecksumCRC64NVME = checksumIfReported(object.ChecksumCRC64NVME, checksums.ChecksumCRC64NVME)
			part.ChecksumSHA1 = checksumIfReported(object.ChecksumSHA1, checksums.ChecksumSHA1)
			part.ChecksumSHA256 = checksumIfReported(object.ChecksumSHA256, checksums.ChecksumSHA256)
		}
		result.Parts = append(result.Parts, part)
	}
	lastPartNumber := partNumberMarker + int32(len(result.Parts))
	result.IsTruncated = ptrutils.ToPtr(lastPartNumber < partsCount)
	if len(result.Parts) > 0 {
		result.NextPartNumberMarker = ptrutils.ToPtr(lastPartNumber)
	}
	return result
}

func checksumIfReported(objectChecksum *string, partChecksum *string) *string {
	if objectChecksum == nil {
		return nil
	}
	return partChecksum
}
