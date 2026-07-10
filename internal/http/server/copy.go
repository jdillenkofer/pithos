package server

import (
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func parseCopySource(value string) (bucket string, key string, versionID *string, err error) {
	if value == "" {
		return "", "", nil, ErrInvalidRequest
	}
	if qIdx := strings.IndexByte(value, '?'); qIdx != -1 {
		query, parseErr := url.ParseQuery(value[qIdx+1:])
		if parseErr != nil {
			return "", "", nil, ErrInvalidRequest
		}
		if query.Has(versionIDQuery) {
			versionID = ptrutils.ToPtr(query.Get(versionIDQuery))
		}
		value = value[:qIdx]
	}
	value = strings.TrimPrefix(value, "/")
	slashIdx := strings.IndexByte(value, '/')
	if slashIdx <= 0 || slashIdx == len(value)-1 {
		return "", "", nil, ErrInvalidRequest
	}
	bucket = value[:slashIdx]
	decodedKey, err := url.PathUnescape(value[slashIdx+1:])
	if err != nil {
		return "", "", nil, ErrInvalidRequest
	}
	return bucket, decodedKey, versionID, nil
}

// parseCopySourceConditions extracts the x-amz-copy-source-if-* preconditions
// from the request headers. Invalid HTTP dates are ignored (treated as absent),
// matching lenient S3 behaviour.
func parseCopySourceConditions(r *http.Request) storage.CopySourceConditions {
	conditions := storage.CopySourceConditions{
		IfMatch:     getHeaderAsPtr(r.Header, copySourceIfMatchHeader),
		IfNoneMatch: getHeaderAsPtr(r.Header, copySourceIfNoneMatchHeader),
	}
	if ims := r.Header.Get(copySourceIfModifiedSinceHeader); ims != "" {
		if t, err := http.ParseTime(ims); err == nil {
			conditions.IfModifiedSince = &t
		}
	}
	if ius := r.Header.Get(copySourceIfUnmodifiedSinceHeader); ius != "" {
		if t, err := http.ParseTime(ius); err == nil {
			conditions.IfUnmodifiedSince = &t
		}
	}
	return conditions
}

// parseCopySourceRange parses the optional x-amz-copy-source-range header. It
// returns (nil, nil) when the header is absent and an error for a malformed
// value or anything other than a single byte range.
func parseCopySourceRange(r *http.Request) (*storage.ByteRange, error) {
	rangeValue := r.Header.Get(copySourceRangeHeader)
	if rangeValue == "" {
		return nil, nil
	}
	parsedRanges, err := parseRangeHeader(rangeValue)
	if err != nil || len(parsedRanges) != 1 {
		return nil, ErrInvalidRequest
	}
	return &parsedRanges[0], nil
}

// parseCopyHandlerSource parses and validates the destination path values and
// the x-amz-copy-source header, writing the appropriate error response and
// returning ok=false on failure.
func parseCopyHandlerSource(w http.ResponseWriter, r *http.Request) (srcBucket storage.BucketName, srcKey storage.ObjectKey, sourceVersionID *string, dstBucket storage.BucketName, dstKey storage.ObjectKey, ok bool) {
	dstBucket, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	dstKey, err = storage.NewObjectKey(r.PathValue(keyPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	srcBucketStr, srcKeyStr, sourceVersionID, err := parseCopySource(r.Header.Get(copySourceHeader))
	if err != nil {
		handleError(err, w, r)
		return
	}
	srcBucket, err = storage.NewBucketName(srcBucketStr)
	if err != nil {
		handleError(err, w, r)
		return
	}
	srcKey, err = storage.NewObjectKey(srcKeyStr)
	if err != nil {
		handleError(err, w, r)
		return
	}
	return srcBucket, srcKey, sourceVersionID, dstBucket, dstKey, true
}

func (s *Server) copyObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.copyObjectHandler")
	defer span.End()

	srcBucketName, srcKey, sourceVersionID, dstBucketName, dstKey, ok := parseCopyHandlerSource(w, r)
	if !ok {
		return
	}

	shouldReturn := s.authorizeCopyRequest(ctx, authorization.OperationCopyObject, srcBucketName.String(), srcKey.String(), sourceVersionID, dstBucketName.String(), dstKey.String(), w, r)
	if shouldReturn {
		return
	}

	metadataDirective := strings.ToUpper(r.Header.Get(metadataDirectiveHeader))
	if metadataDirective == "" {
		metadataDirective = metadataDirectiveCopy
	}
	if metadataDirective != metadataDirectiveCopy && metadataDirective != metadataDirectiveReplace {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	taggingDirective := strings.ToUpper(r.Header.Get(taggingDirectiveHeader))
	if taggingDirective == "" {
		taggingDirective = taggingDirectiveCopy
	}
	if taggingDirective != taggingDirectiveCopy && taggingDirective != taggingDirectiveReplace {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	var replaceTags map[string]string
	if taggingDirective == taggingDirectiveReplace {
		tags, err := storage.ParseTaggingHeader(r.Header.Get(taggingHeader))
		if err != nil {
			handleError(err, w, r)
			return
		}
		replaceTags = tags
	}

	copyRange, err := parseCopySourceRange(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// Disallow a no-op self copy (same bucket+key, COPY directive, no range),
	// matching S3 which rejects it as an illegal request. A self copy that
	// supplies x-amz-storage-class is allowed, mirroring S3 where changing the
	// storage class makes the copy meaningful.
	if metadataDirective == metadataDirectiveCopy && copyRange == nil &&
		r.Header.Get(storageClassHeader) == "" &&
		srcBucketName.Equals(dstBucketName) && srcKey.Equals(dstKey) {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	metadata, err := parseObjectMetadataHeaders(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// The destination storage class follows the x-amz-storage-class header on
	// the copy request itself; it is not governed by x-amz-metadata-directive.
	storageClass, err := parseStorageClassHeader(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}

	opts := &storage.CopyObjectOptions{
		SourceVersionID:      sourceVersionID,
		ReplaceMetadata:      metadataDirective == metadataDirectiveReplace,
		Range:                copyRange,
		CopySourceConditions: parseCopySourceConditions(r),
		ReplaceTags:          taggingDirective == taggingDirectiveReplace,
		Tags:                 replaceTags,
		Metadata:             metadata,
		StorageClass:         storageClass,
	}
	if opts.ReplaceMetadata {
		opts.ContentType = getHeaderAsPtr(r.Header, contentTypeHeader)
	}

	slog.InfoContext(r.Context(), "Copying object",
		"srcBucket", srcBucketName.String(), "srcKey", srcKey.String(),
		"dstBucket", dstBucketName.String(), "dstKey", dstKey.String(),
		"metadataDirective", metadataDirective)

	result, err := s.storage.CopyObject(ctx, srcBucketName, srcKey, dstBucketName, dstKey, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	copyObjectResult := CopyObjectResult{
		ETag:         result.ETag,
		LastModified: result.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	responseHeaders := w.Header()
	if result.VersionID != nil {
		responseHeaders.Set(versionIDHeader, *result.VersionID)
	}
	if result.SourceVersionID != nil {
		responseHeaders.Set(copySourceVersionIDHeader, *result.SourceVersionID)
	}
	writeXMLResponse(w, r, http.StatusOK, copyObjectResult)
}

func (s *Server) uploadPartCopyHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.uploadPartCopyHandler")
	defer span.End()

	srcBucketName, srcKey, sourceVersionID, dstBucketName, dstKey, ok := parseCopyHandlerSource(w, r)
	if !ok {
		return
	}

	shouldReturn := s.authorizeCopyRequest(ctx, authorization.OperationUploadPartCopy, srcBucketName.String(), srcKey.String(), sourceVersionID, dstBucketName.String(), dstKey.String(), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	if !query.Has(uploadIdQuery) || !query.Has(partNumberQuery) {
		w.WriteHeader(400)
		return
	}
	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberI64, err := strconv.ParseInt(query.Get(partNumberQuery), 10, 16)
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberI32 := int32(partNumberI64)
	if partNumberI32 < 1 || partNumberI32 > 10000 {
		w.WriteHeader(400)
		return
	}

	copyRange, err := parseCopySourceRange(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	opts := &storage.UploadPartCopyOptions{
		SourceVersionID:      sourceVersionID,
		Range:                copyRange,
		CopySourceConditions: parseCopySourceConditions(r),
	}

	slog.InfoContext(r.Context(), "UploadPartCopy",
		"srcBucket", srcBucketName.String(), "srcKey", srcKey.String(),
		"dstBucket", dstBucketName.String(), "dstKey", dstKey.String(),
		"uploadId", uploadId.String(), "partNumber", partNumberI32)

	result, err := s.storage.UploadPartCopy(ctx, srcBucketName, srcKey, dstBucketName, dstKey, uploadId, partNumberI32, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	copyPartResult := CopyPartResult{
		ETag:         result.ETag,
		LastModified: result.LastModified.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	responseHeaders := w.Header()
	if result.SourceVersionID != nil {
		responseHeaders.Set(copySourceVersionIDHeader, *result.SourceVersionID)
	}
	writeXMLResponse(w, r, http.StatusOK, copyPartResult)
}

func (s *Server) abortMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.abortMultipartUploadHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationAbortMultipartUpload, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	slog.InfoContext(r.Context(), "AbortMultipartUpload", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String())
	err = s.storage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}
