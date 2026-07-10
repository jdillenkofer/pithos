package server

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func (s *Server) createMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.createMultipartUploadHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCreateMultipartUpload, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	contentType := getHeaderAsPtr(r.Header, contentTypeHeader)
	checksumType := getHeaderAsPtr(r.Header, checksumTypeHeader)

	var createOpts *storage.CreateMultipartUploadOptions
	if taggingValue := r.Header.Get(taggingHeader); taggingValue != "" {
		tags, err := storage.ParseTaggingHeader(taggingValue)
		if err != nil {
			handleError(err, w, r)
			return
		}
		createOpts = &storage.CreateMultipartUploadOptions{Tags: tags}
	}

	metadata, err := parseObjectMetadataHeaders(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if metadata != nil {
		if createOpts == nil {
			createOpts = &storage.CreateMultipartUploadOptions{}
		}
		createOpts.Metadata = metadata
	}

	storageClass, err := parseStorageClassHeader(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if storageClass != nil {
		if createOpts == nil {
			createOpts = &storage.CreateMultipartUploadOptions{}
		}
		createOpts.StorageClass = storageClass
	}

	slog.InfoContext(r.Context(), "CreateMultipartUpload", "bucket", bucketName.String(), "key", key.String())
	result, err := s.storage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType, createOpts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	initiateMultipartUploadResult := InitiateMultipartUploadResult{
		Bucket:   bucketName.String(),
		Key:      key.String(),
		UploadId: result.UploadId.String(),
	}

	writeXMLResponse(w, r, http.StatusOK, initiateMultipartUploadResult)
}

func mapCompleteMultipartUploadParts(parts []*Part) []storage.CompleteMultipartUploadPart {
	storageParts := make([]storage.CompleteMultipartUploadPart, 0, len(parts))
	for _, part := range parts {
		storageParts = append(storageParts, storage.CompleteMultipartUploadPart{
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			ETag:              part.ETag,
			PartNumber:        part.PartNumber,
		})
	}
	return storageParts
}

func (s *Server) completeMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.completeMultipartUploadHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCompleteMultipartUpload, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	checksumInput, err := extractChecksumInput(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// Conditional multipart upload: check If-Match and If-None-Match headers.
	// AWS S3 compatible behaviour:
	//   If-Match: <etag>  — the current object at the key must have this ETag, else 412.
	//   If-Match: *       — any existing object satisfies the condition; fail (412) only when no object exists.
	//   If-None-Match: *  — the operation must fail (412) when any object already exists at the key.
	//   Both headers together are not allowed (400).
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	var completeOpts *storage.CompleteMultipartUploadOptions
	if ifMatch != nil {
		completeOpts = &storage.CompleteMultipartUploadOptions{IfMatchETag: ifMatch}
	}
	if ifNoneMatch != nil {
		if *ifNoneMatch != "*" {
			handleError(ErrInvalidRequest, w, r)
			return
		}
		if completeOpts == nil {
			completeOpts = &storage.CompleteMultipartUploadOptions{}
		}
		completeOpts.IfNoneMatchStar = true
	}
	if completeOpts != nil && completeOpts.IfMatchETag != nil && completeOpts.IfNoneMatchStar {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	data, err := readLimitedBody(r, w, maxCompleteMultipartUploadBodySize)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if len(data) > 0 {
		completeMultipartUploadRequest := CompleteMultipartUploadRequest{}
		err = xml.Unmarshal(data, &completeMultipartUploadRequest)
		if err != nil {
			handleError(err, w, r)
			return
		}
		if len(completeMultipartUploadRequest.Parts) > 0 {
			if completeOpts == nil {
				completeOpts = &storage.CompleteMultipartUploadOptions{}
			}
			completeOpts.Parts = mapCompleteMultipartUploadParts(completeMultipartUploadRequest.Parts)
		}
	}

	slog.InfoContext(r.Context(), "CompleteMultipartUpload", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String())
	result, err := s.storage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput, completeOpts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	completeMultipartUploadResult := CompleteMultipartUploadResult{
		Location:          result.Location,
		Bucket:            bucketName.String(),
		Key:               key.String(),
		ETag:              result.ETag,
		ChecksumCRC32:     result.ChecksumCRC32,
		ChecksumCRC32C:    result.ChecksumCRC32C,
		ChecksumCRC64NVME: result.ChecksumCRC64NVME,
		ChecksumSHA1:      result.ChecksumSHA1,
		ChecksumSHA256:    result.ChecksumSHA256,
		ChecksumType:      result.ChecksumType,
	}
	if result.VersionID != nil {
		w.Header().Set(versionIDHeader, *result.VersionID)
	}

	writeXMLResponse(w, r, http.StatusOK, completeMultipartUploadResult)
}

func (s *Server) createMultipartUploadOrCompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// CreateMultipartUpload
	if query.Has(uploadsQuery) {
		s.createMultipartUploadHandler(w, r)
		return
	}

	// CompleteMultipartUpload
	if query.Has(uploadIdQuery) {
		s.completeMultipartUploadHandler(w, r)
		return
	}

	w.WriteHeader(404)
}

// validateMaxEntitySize ensures that the request body does not exceed the maximum allowed entity size.
// It returns true if the request is invalid and the handler should return immediately.
// It also sets r.Body to a MaxBytesReader to enforce the limit during reading.
func validateMaxEntitySize(r *http.Request, w http.ResponseWriter) bool {
	r.Body = http.MaxBytesReader(w, r.Body, storage.MaxEntitySize+1)
	contentLength := getHeaderAsPtr(r.Header, contentLengthHeader)
	if contentLength != nil {
		contentLengthI64, err := strconv.ParseInt(*contentLength, 10, 64)
		if err != nil {
			handleError(err, w, r)
			return true
		}
		if contentLengthI64 > storage.MaxEntitySize {
			handleError(storage.ErrEntityTooLarge, w, r)
			return true
		}
	}
	return false
}

func (s *Server) uploadPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.uploadPartHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationUploadPart, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumber := query.Get(partNumberQuery)
	checksumInput, err := extractChecksumInput(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn = validateMaxEntitySize(r, w)
	if shouldReturn {
		return
	}

	slog.InfoContext(r.Context(), "UploadPart", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String(), "partNumber", partNumber)
	if !query.Has(uploadIdQuery) || !query.Has(partNumberQuery) {
		w.WriteHeader(400)
		return
	}
	partNumberI64, err := strconv.ParseInt(partNumber, 10, 16)
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberI32 := int32(partNumberI64)
	if partNumberI32 < 1 || partNumberI32 > 10000 {
		w.WriteHeader(400)
		return
	}
	uploadPartResult, err := s.storage.UploadPart(ctx, bucketName, key, uploadId, partNumberI32, r.Body, checksumInput)
	if err != nil {
		if _, ok := err.(*http.MaxBytesError); ok {
			err = storage.ErrEntityTooLarge
		}
		handleError(err, w, r)
		return
	}
	responseHeaders := w.Header()
	setChecksumHeadersFromChecksumValues(responseHeaders, storage.ChecksumValues{
		ETag:              &uploadPartResult.ETag,
		ChecksumCRC32:     uploadPartResult.ChecksumCRC32,
		ChecksumCRC32C:    uploadPartResult.ChecksumCRC32C,
		ChecksumCRC64NVME: uploadPartResult.ChecksumCRC64NVME,
		ChecksumSHA1:      uploadPartResult.ChecksumSHA1,
		ChecksumSHA256:    uploadPartResult.ChecksumSHA256,
	})
	w.WriteHeader(200)
}

func (s *Server) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putObjectHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutObject, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	contentType := getHeaderAsPtr(r.Header, contentTypeHeader)
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	var putObjectOptions *storage.PutObjectOptions
	if ifMatch != nil {
		putObjectOptions = &storage.PutObjectOptions{IfMatchETag: ifMatch}
	}
	if ifNoneMatch != nil {
		if *ifNoneMatch != "*" {
			handleError(ErrInvalidRequest, w, r)
			return
		}
		if putObjectOptions == nil {
			putObjectOptions = &storage.PutObjectOptions{}
		}
		putObjectOptions.IfNoneMatchStar = true
	}
	if putObjectOptions != nil && putObjectOptions.IfMatchETag != nil && putObjectOptions.IfNoneMatchStar {
		handleError(ErrInvalidRequest, w, r)
		return
	}

	if taggingValue := r.Header.Get(taggingHeader); taggingValue != "" {
		tags, err := storage.ParseTaggingHeader(taggingValue)
		if err != nil {
			handleError(err, w, r)
			return
		}
		if putObjectOptions == nil {
			putObjectOptions = &storage.PutObjectOptions{}
		}
		putObjectOptions.Tags = tags
	}

	metadata, err := parseObjectMetadataHeaders(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if metadata != nil {
		if putObjectOptions == nil {
			putObjectOptions = &storage.PutObjectOptions{}
		}
		putObjectOptions.Metadata = metadata
	}

	storageClass, err := parseStorageClassHeader(r.Header)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if storageClass != nil {
		if putObjectOptions == nil {
			putObjectOptions = &storage.PutObjectOptions{}
		}
		putObjectOptions.StorageClass = storageClass
	}

	shouldReturn = validateMaxEntitySize(r, w)
	if shouldReturn {
		return
	}

	checksumInput, err := extractChecksumInput(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	slog.InfoContext(r.Context(), "Putting object", "bucket", bucketName.String(), "key", key.String())
	if r.Header.Get(expectHeader) == "100-continue" {
		w.WriteHeader(100)
	}
	putObjectResult, err := s.storage.PutObject(ctx, bucketName, key, contentType, r.Body, checksumInput, putObjectOptions)
	if err != nil {
		if _, ok := err.(*http.MaxBytesError); ok {
			err = storage.ErrEntityTooLarge
		}
		handleError(err, w, r)
		return
	}

	responseHeaders := w.Header()
	setChecksumHeadersFromChecksumValues(responseHeaders, storage.ChecksumValues{
		ETag:              putObjectResult.ETag,
		ChecksumCRC32:     putObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    putObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: putObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      putObjectResult.ChecksumSHA1,
		ChecksumSHA256:    putObjectResult.ChecksumSHA256,
	})
	if putObjectResult.VersionID != nil {
		responseHeaders.Set(versionIDHeader, *putObjectResult.VersionID)
	}
	setChecksumType(responseHeaders, storage.ChecksumTypeFullObject)
	w.WriteHeader(200)
}

func (s *Server) appendObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.appendObjectHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationAppendObject, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	var appendObjectOptions *storage.AppendObjectOptions
	if writeOffsetStr := r.Header.Get(writeOffsetBytesHeader); writeOffsetStr != "" {
		writeOffset, parseErr := strconv.ParseInt(writeOffsetStr, 10, 64)
		if parseErr != nil {
			handleError(ErrInvalidRequest, w, r)
			return
		}
		appendObjectOptions = &storage.AppendObjectOptions{WriteOffset: &writeOffset}
	}

	shouldReturn = validateMaxEntitySize(r, w)
	if shouldReturn {
		return
	}

	checksumInput, err := extractChecksumInput(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	slog.InfoContext(r.Context(), "Appending to object", "bucket", bucketName.String(), "key", key.String())
	if r.Header.Get(expectHeader) == "100-continue" {
		w.WriteHeader(100)
	}
	appendObjectResult, err := s.storage.AppendObject(ctx, bucketName, key, r.Body, checksumInput, appendObjectOptions)
	if err != nil {
		if _, ok := err.(*http.MaxBytesError); ok {
			err = storage.ErrEntityTooLarge
		}
		handleError(err, w, r)
		return
	}

	responseHeaders := w.Header()
	responseHeaders.Set(etagHeader, appendObjectResult.ETag)
	responseHeaders.Set("x-amz-object-size", strconv.FormatInt(appendObjectResult.Size, 10))
	w.WriteHeader(200)
}

func (s *Server) uploadPartOrPutObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// UploadPart / UploadPartCopy
	if query.Has(uploadIdQuery) || query.Has(partNumberQuery) {
		if r.Header.Get(copySourceHeader) != "" {
			s.uploadPartCopyHandler(w, r)
			return
		}
		s.uploadPartHandler(w, r)
		return
	}

	// CopyObject (server-side copy).
	if r.Header.Get(copySourceHeader) != "" {
		s.copyObjectHandler(w, r)
		return
	}

	// AppendObject
	if query.Has(appendQuery) {
		s.appendObjectHandler(w, r)
		return
	}

	// PutObjectTagging
	if query.Has(taggingQuery) {
		s.putObjectTaggingHandler(w, r)
		return
	}

	// PutObject
	s.putObjectHandler(w, r)
}

// parseCopySource parses the x-amz-copy-source header value into its source
// bucket, object key, and optional source version id. The expected form is
// "/sourceBucket/sourceKey" or "sourceBucket/sourceKey"; the key portion is
// URL-encoded by S3 clients and is decoded here.
