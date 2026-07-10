package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	httpmiddleware "github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	storage "github.com/jdillenkofer/pithos/internal/storage"
)

func getHeaderAsPtr(headers http.Header, name string) *string {
	val := headers.Get(name)
	if val == "" {
		return nil
	}
	return &val
}

func setETagHeaderFromObject(headers http.Header, object *storage.Object) {
	headers.Set(etagHeader, object.ETag)
}

// parseStorageClassHeader extracts the x-amz-storage-class request header.
// Absent means the default class (nil); an unrecognized value is rejected with
// ErrInvalidStorageClass.
func parseStorageClassHeader(headers http.Header) (*string, error) {
	storageClass := getHeaderAsPtr(headers, storageClassHeader)
	if storageClass != nil && !storage.IsValidStorageClass(*storageClass) {
		return nil, storage.ErrInvalidStorageClass
	}
	return storageClass, nil
}

// setStorageClassHeaderFromObject sets the x-amz-storage-class response header.
// Per S3, the header is omitted for STANDARD objects.
func setStorageClassHeaderFromObject(headers http.Header, object *storage.Object) {
	if storageClass := storage.EffectiveStorageClass(object.StorageClass); storageClass != storage.StorageClassStandard {
		headers.Set(storageClassHeader, storageClass)
	}
}

// setTagCountHeaderFromObject sets the x-amz-tag-count header to the number of
// tags on the object. Per S3, the header is only present when the object has at
// least one tag.
func setTagCountHeaderFromObject(headers http.Header, object *storage.Object) {
	if len(object.Tags) > 0 {
		headers.Set(taggingCountHeader, strconv.Itoa(len(object.Tags)))
	}
}

// parseObjectMetadataHeaders extracts the user-controllable object metadata
// from the request headers: the user-modifiable system headers plus the
// x-amz-meta-* pairs. Per S3, user metadata keys are lowercased and repeated
// headers of the same name are combined into a comma-delimited list; the total
// user metadata size is limited to 2 KB (ErrMetadataTooLarge). Returns nil when
// the request carries no metadata.
func parseObjectMetadataHeaders(headers http.Header) (*storage.ObjectMetadata, error) {
	metadata := storage.ObjectMetadata{
		CacheControl:            getHeaderAsPtr(headers, cacheControlHeader),
		ContentDisposition:      getHeaderAsPtr(headers, contentDispositionHeader),
		ContentEncoding:         getHeaderAsPtr(headers, contentEncodingHeader),
		ContentLanguage:         getHeaderAsPtr(headers, contentLanguageHeader),
		Expires:                 getHeaderAsPtr(headers, expiresHeader),
		WebsiteRedirectLocation: getHeaderAsPtr(headers, websiteRedirectLocationHeader),
	}
	userMetadata := map[string]string{}
	for name, values := range headers {
		lowerName := strings.ToLower(name)
		key := strings.TrimPrefix(lowerName, userMetadataHeaderPrefix)
		if key == lowerName || key == "" {
			continue
		}
		userMetadata[key] = strings.Join(values, ",")
	}
	if err := storage.ValidateUserMetadata(userMetadata); err != nil {
		return nil, err
	}
	if len(userMetadata) > 0 {
		metadata.UserMetadata = userMetadata
	}
	if metadata.CacheControl == nil && metadata.ContentDisposition == nil &&
		metadata.ContentEncoding == nil && metadata.ContentLanguage == nil &&
		metadata.Expires == nil && metadata.WebsiteRedirectLocation == nil &&
		metadata.UserMetadata == nil {
		return nil, nil
	}
	return &metadata, nil
}

// setMetadataHeadersFromObject sets the user-controllable system metadata
// headers and the x-amz-meta-* headers stored on the object.
func setMetadataHeadersFromObject(headers http.Header, object *storage.Object) {
	metadata := object.Metadata
	if metadata.CacheControl != nil {
		headers.Set(cacheControlHeader, *metadata.CacheControl)
	}
	if metadata.ContentDisposition != nil {
		headers.Set(contentDispositionHeader, *metadata.ContentDisposition)
	}
	if metadata.ContentEncoding != nil {
		headers.Set(contentEncodingHeader, *metadata.ContentEncoding)
	}
	if metadata.ContentLanguage != nil {
		headers.Set(contentLanguageHeader, *metadata.ContentLanguage)
	}
	if metadata.Expires != nil {
		headers.Set(expiresHeader, *metadata.Expires)
	}
	if metadata.WebsiteRedirectLocation != nil {
		headers.Set(websiteRedirectLocationHeader, *metadata.WebsiteRedirectLocation)
	}
	for key, value := range metadata.UserMetadata {
		headers.Set(userMetadataHeaderPrefix+key, value)
	}
}

func setChecksumType(headers http.Header, checksumType string) {
	headers.Set(checksumTypeHeader, checksumType)
}

func setChecksumHeadersFromObject(headers http.Header, object *storage.Object) {
	if object.ChecksumType != nil {
		setChecksumType(headers, *object.ChecksumType)
	}
	if object.ChecksumCRC32 != nil {
		headers.Set(checksumCRC32Header, *object.ChecksumCRC32)
	}
	if object.ChecksumCRC32C != nil {
		headers.Set(checksumCRC32CHeader, *object.ChecksumCRC32C)
	}
	if object.ChecksumCRC64NVME != nil {
		headers.Set(checksumCRC64NVMEHeader, *object.ChecksumCRC64NVME)
	}
	if object.ChecksumSHA1 != nil {
		headers.Set(checksumSHA1Header, *object.ChecksumSHA1)
	}
	if object.ChecksumSHA256 != nil {
		headers.Set(checksumSHA256Header, *object.ChecksumSHA256)
	}
}

func setChecksumHeadersFromChecksumValues(headers http.Header, checksumValues storage.ChecksumValues) {
	if checksumValues.ETag != nil {
		headers.Set(etagHeader, *checksumValues.ETag)
	}
	if checksumValues.ChecksumCRC32 != nil {
		headers.Set(checksumCRC32Header, *checksumValues.ChecksumCRC32)
	}
	if checksumValues.ChecksumCRC32C != nil {
		headers.Set(checksumCRC32CHeader, *checksumValues.ChecksumCRC32C)
	}
	if checksumValues.ChecksumCRC64NVME != nil {
		headers.Set(checksumCRC64NVMEHeader, *checksumValues.ChecksumCRC64NVME)
	}
	if checksumValues.ChecksumSHA1 != nil {
		headers.Set(checksumSHA1Header, *checksumValues.ChecksumSHA1)
	}
	if checksumValues.ChecksumSHA256 != nil {
		headers.Set(checksumSHA256Header, *checksumValues.ChecksumSHA256)
	}
}

func extractChecksumInput(r *http.Request) (*storage.ChecksumInput, error) {
	checksumType := getHeaderAsPtr(r.Header, checksumTypeHeader)
	checksumAlgorithm := getHeaderAsPtr(r.Header, checksumAlgorithmHeader)
	contentMd5 := getHeaderAsPtr(r.Header, contentMd5Header)
	checksumCrc32 := getHeaderAsPtr(r.Header, checksumCRC32Header)
	checksumCrc32c := getHeaderAsPtr(r.Header, checksumCRC32CHeader)
	checksumCrc64Nvme := getHeaderAsPtr(r.Header, checksumCRC64NVMEHeader)
	checksumSha1 := getHeaderAsPtr(r.Header, checksumSHA1Header)
	checksumSha256 := getHeaderAsPtr(r.Header, checksumSHA256Header)

	var etagStr *string = nil
	if contentMd5 != nil {
		etag, err := base64.StdEncoding.DecodeString(*contentMd5)
		if err != nil {
			return nil, err
		}
		etagStr = ptrutils.ToPtr(fmt.Sprintf("\"%x\"", etag))
	}

	checksumInput := storage.ChecksumInput{
		ChecksumType:      checksumType,
		ChecksumAlgorithm: checksumAlgorithm,
		ETag:              etagStr,
		ChecksumCRC32:     checksumCrc32,
		ChecksumCRC32C:    checksumCrc32c,
		ChecksumCRC64NVME: checksumCrc64Nvme,
		ChecksumSHA1:      checksumSha1,
		ChecksumSHA256:    checksumSha256,
	}
	return &checksumInput, nil
}

func handleError(err error, w http.ResponseWriter, r *http.Request) {
	if currentDeleteMarkerErr, ok := err.(*storage.CurrentDeleteMarkerError); ok {
		responseHeaders := w.Header()
		responseHeaders.Set(deleteMarkerHeader, "true")
		if currentDeleteMarkerErr.VersionID != "" {
			responseHeaders.Set(versionIDHeader, currentDeleteMarkerErr.VersionID)
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if versionDeleteMarkerErr, ok := err.(*storage.VersionDeleteMarkerMethodNotAllowedError); ok {
		responseHeaders := w.Header()
		responseHeaders.Set(deleteMarkerHeader, "true")
		if versionDeleteMarkerErr.VersionID != "" {
			responseHeaders.Set(versionIDHeader, versionDeleteMarkerErr.VersionID)
		}
		responseHeaders.Set(lastModifiedHeader, versionDeleteMarkerErr.LastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	statusCode := 500
	errResponse := ErrorResponse{}
	errResponse.Code = err.Error()
	errResponse.Message = err.Error()
	errResponse.Resource = r.URL.Path
	if requestID, ok := httpmiddleware.RequestIDFromContext(r.Context()); ok {
		errResponse.RequestId = requestID
	}
	switch err {
	case storage.ErrInvalidBucketName:
		statusCode = 400
	case storage.ErrBadDigest:
		statusCode = 400
	case authentication.ErrTrailerChecksumMismatch:
		statusCode = 400
	case authentication.ErrMalformedTrailer:
		statusCode = 400
	case storage.ErrInvalidRange:
		statusCode = 416
	case storage.ErrNoSuchBucket:
		statusCode = 404
	case storage.ErrBucketAlreadyExists:
		statusCode = 409
	case storage.ErrBucketNotEmpty:
		statusCode = 409
	case storage.ErrNoSuchKey:
		statusCode = 404
	case storage.ErrNoSuchWebsiteConfiguration:
		statusCode = 404
	case ErrInvalidRequest:
		statusCode = 400
	case ErrInvalidArgument:
		statusCode = 400
	case storage.ErrInvalidPart:
		statusCode = 400
	case storage.ErrInvalidPartOrder:
		statusCode = 400
	case storage.ErrEntityTooLarge:
		statusCode = 413
	case storage.ErrTooManyParts:
		statusCode = 400
	case storage.ErrInvalidTag:
		statusCode = 400
	case storage.ErrMetadataTooLarge:
		statusCode = 400
	case storage.ErrInvalidWriteOffset:
		statusCode = 400
	case storage.ErrInvalidStorageClass:
		statusCode = 400
	case storage.ErrPreconditionFailed:
		statusCode = 412
	case storage.ErrNotModified:
		statusCode = 304
	case storage.ErrNotImplemented:
		statusCode = http.StatusNotImplemented
		errResponse.Code = "NotImplemented"
		errResponse.Message = "NotImplemented"
	default:
		slog.ErrorContext(r.Context(), fmt.Sprintf("Unhandled internal error: %v", err))
		statusCode = 500
		errResponse.Code = "InternalError"
		errResponse.Message = "InternalError"
	}
	writeXMLResponse(w, r, statusCode, errResponse)
}

func writeNoSuchCORSConfiguration(w http.ResponseWriter, r *http.Request) {
	writeS3ErrorResponse(w, r, http.StatusNotFound, "NoSuchCORSConfiguration", "The CORS configuration does not exist", r.URL.Path)
}

func writeInvalidRequest(w http.ResponseWriter, r *http.Request, message string) {
	writeS3ErrorResponse(w, r, http.StatusBadRequest, "InvalidRequest", message, r.URL.Path)
}

func (s *Server) authorizeRequest(ctx context.Context, operation string, bucket *string, key *string, w http.ResponseWriter, r *http.Request) bool {
	return s.authorizeRequestWithRequestTags(ctx, operation, bucket, key, nil, w, r)
}

// authorizeRequestWithRequestTags is like authorizeRequest but overrides the
// request's tag set (s3:RequestObjectTag) with tags that are not derivable from
// the request headers — e.g. the tags carried in the PutObjectTagging XML body.
// A nil requestTags keeps the header-derived tags from makeAuthorizationRequest.
func (s *Server) authorizeRequestWithRequestTags(ctx context.Context, operation string, bucket *string, key *string, requestTags map[string]string, w http.ResponseWriter, r *http.Request) bool {
	request, isAuthenticated := makeAuthorizationRequest(ctx, operation, bucket, key, r)
	if requestTags != nil {
		request.RequestObjectTags = requestTags
	}
	versionID := httputils.GetQueryParam(r.URL.Query(), versionIDQuery)
	s.bindExistingObjectTagsResolver(request, bucket, key, versionID)
	return s.runAuthorization(ctx, request, isAuthenticated, w, r)
}

// makeExistingObjectTagsResolver builds a lazy resolver that fetches the given
// object's currently stored tags, so Lua policies can gate on
// s3:ExistingObjectTag. A missing object resolves to an empty tag set; other
// lookup errors fail closed. Returns nil when bucket/key are absent or invalid.
func (s *Server) makeExistingObjectTagsResolver(bucket *string, key *string, versionID *string) func(ctx context.Context) (map[string]string, error) {
	if bucket == nil || key == nil {
		return nil
	}
	bucketName, errB := storage.NewBucketName(*bucket)
	objectKey, errK := storage.NewObjectKey(*key)
	if errB != nil || errK != nil {
		return nil
	}
	opts := &storage.ObjectTaggingOptions{VersionID: versionID}
	return func(ctx context.Context) (map[string]string, error) {
		tags, err := s.storage.GetObjectTagging(ctx, bucketName, objectKey, opts)
		if err == storage.ErrNoSuchKey {
			return map[string]string{}, nil
		}
		if err != nil {
			return nil, err
		}
		return tags, nil
	}
}

// bindExistingObjectTagsResolver attaches the lazy tag resolver for the
// request's target object.
func (s *Server) bindExistingObjectTagsResolver(request *authorization.Request, bucket *string, key *string, versionID *string) {
	if resolver := s.makeExistingObjectTagsResolver(bucket, key, versionID); resolver != nil {
		request.ResolveExistingObjectTags = resolver
	}
}

// runAuthorization invokes the configured authorizer and writes the appropriate
// error/deny response. It returns true when the caller should stop handling the
// request (error or denied).
func (s *Server) runAuthorization(ctx context.Context, request *authorization.Request, isAuthenticated bool, w http.ResponseWriter, r *http.Request) bool {
	authorized, err := s.requestAuthorizer.AuthorizeRequest(ctx, request)
	if err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("Authorization error: %v", err))
		handleError(err, w, r)
		return true
	}
	if !authorized {
		slog.DebugContext(ctx, fmt.Sprintf("Unauthorized request: %v", request))
		if !isAuthenticated {
			w.WriteHeader(401)
		} else {
			w.WriteHeader(403)
		}
		return true
	}
	return false
}

// authorizeCopyRequest authorizes a server-side copy operation (CopyObject or
// UploadPartCopy). Bucket/Key identify the destination; SourceBucket/SourceKey
// identify the copy source, so a policy can reason about both ends in a single
// check.
func (s *Server) authorizeCopyRequest(ctx context.Context, operation string, srcBucket, srcKey string, sourceVersionID *string, dstBucket, dstKey string, w http.ResponseWriter, r *http.Request) bool {
	request, isAuthenticated := makeAuthorizationRequest(ctx, operation, ptrutils.ToPtr(dstBucket), ptrutils.ToPtr(dstKey), r)
	request.SourceBucket = ptrutils.ToPtr(srcBucket)
	request.SourceKey = ptrutils.ToPtr(srcKey)
	// objectTag* predicates refer to the destination object being
	// created/overwritten; sourceObjectTag* predicates refer to the copy source
	// (matching AWS, which evaluates s3:ExistingObjectTag against the source for
	// the copy's read side).
	s.bindExistingObjectTagsResolver(request, ptrutils.ToPtr(dstBucket), ptrutils.ToPtr(dstKey), nil)
	request.ResolveExistingSourceObjectTags = s.makeExistingObjectTagsResolver(ptrutils.ToPtr(srcBucket), ptrutils.ToPtr(srcKey), sourceVersionID)
	return s.runAuthorization(ctx, request, isAuthenticated, w, r)
}

// taggingHeaderApplies reports whether the x-amz-tagging header has any effect
// for the given operation, i.e. whether the operation persists the header's
// tags on the target object.
func taggingHeaderApplies(operation string, r *http.Request) bool {
	switch operation {
	case authorization.OperationPutObject, authorization.OperationCreateMultipartUpload:
		return true
	case authorization.OperationCopyObject:
		return strings.ToUpper(r.Header.Get(taggingDirectiveHeader)) == taggingDirectiveReplace
	}
	return false
}

func makeAuthorizationRequest(ctx context.Context, operation string, bucket *string, key *string, r *http.Request) (*authorization.Request, bool) {
	isAuthenticated, _ := ctx.Value(authentication.IsAuthenticatedContextKey{}).(bool)

	var accessKeyId *string
	if isAuthenticated {
		keyStr, _ := ctx.Value(authentication.AccessKeyIdContextKey{}).(string)
		accessKeyId = &keyStr
	}

	// Expose tags supplied via the x-amz-tagging header as request tags
	// (s3:RequestObjectTag), but only for operations that actually store the
	// header's tags — otherwise a policy could be satisfied by tags the
	// operation ignores (e.g. CopyObject with the default COPY directive).
	// Malformed values are left unset here; the operation itself rejects them
	// later.
	var requestTags map[string]string
	if taggingHeaderApplies(operation, r) {
		if taggingValue := r.Header.Get(taggingHeader); taggingValue != "" {
			if parsed, err := storage.ParseTaggingHeader(taggingValue); err == nil {
				requestTags = parsed
			}
		}
	}

	request := &authorization.Request{
		Operation: operation,
		Authorization: authorization.Authorization{
			AccessKeyId: accessKeyId,
		},
		Bucket:            bucket,
		Key:               key,
		HttpRequest:       makeAuthorizationHTTPRequest(r),
		RequestObjectTags: requestTags,
	}
	return request, isAuthenticated
}

func (s *Server) authorizeListBucket(ctx context.Context, request *authorization.Request, bucketName string) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
	}
	return requestResourceAuthorizer.AuthorizeListBucket(ctx, request, bucketName)
}

// authorizeListObject filters a single listed key. existingTags, when non-nil,
// is the object's tag set already loaded by the list query; it is served to
// tag predicates directly so filtering a page does not cost one storage lookup
// per key. A nil existingTags (e.g. common prefixes, or backends whose list
// results carry no tags) falls back to a lazy per-key lookup.
func (s *Server) authorizeListObject(ctx context.Context, request *authorization.Request, key string, existingTags map[string]string) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
	}
	// Re-bind the existing-tags resolver to the object currently being filtered so
	// authorizeListObject policies can gate each listed object on its own tags.
	if existingTags != nil {
		request.ResolveExistingObjectTags = func(context.Context) (map[string]string, error) {
			return existingTags, nil
		}
	} else {
		s.bindExistingObjectTagsResolver(request, request.Bucket, &key, nil)
	}
	return requestResourceAuthorizer.AuthorizeListObject(ctx, request, key)
}

func (s *Server) authorizeDeleteObjectEntry(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
	}
	return requestResourceAuthorizer.AuthorizeDeleteObjectEntry(ctx, request, key)
}

func (s *Server) authorizeListMultipartUpload(ctx context.Context, request *authorization.Request, key string, uploadID string) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
	}
	return requestResourceAuthorizer.AuthorizeListMultipartUpload(ctx, request, key, uploadID)
}

func (s *Server) authorizeListPart(ctx context.Context, request *authorization.Request, partNumber int32) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
	}
	return requestResourceAuthorizer.AuthorizeListPart(ctx, request, partNumber)
}

func cloneStringSliceMap(input map[string][]string) map[string][]string {
	cloned := make(map[string][]string, len(input))
	for key, values := range input {
		clonedValues := make([]string, len(values))
		copy(clonedValues, values)
		cloned[key] = clonedValues
	}
	return cloned
}

func getContentLength(contentLength int64) *int {
	if contentLength < 0 {
		return nil
	}

	maxInt := int64(^uint(0) >> 1)
	if contentLength > maxInt {
		return nil
	}

	value := int(contentLength)
	return &value
}

func getRemoteIP(remoteAddr string) *string {
	if remoteAddr == "" {
		return nil
	}

	host, _, err := net.SplitHostPort(remoteAddr)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			parsedIP := ip.String()
			return &parsedIP
		}
		return nil
	}

	if ip := net.ParseIP(remoteAddr); ip != nil {
		parsedIP := ip.String()
		return &parsedIP
	}

	return nil
}

func getRequestScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

func makeAuthorizationHTTPRequest(r *http.Request) authorization.HTTPRequest {
	queryParams := r.URL.Query()
	remoteIP := getRemoteIP(r.RemoteAddr)
	scheme := getRequestScheme(r)

	return authorization.HTTPRequest{
		Method:        r.Method,
		Path:          r.URL.Path,
		Query:         r.URL.RawQuery,
		QueryParams:   cloneStringSliceMap(queryParams),
		Headers:       cloneStringSliceMap(r.Header),
		Host:          r.Host,
		Proto:         r.Proto,
		ContentLength: getContentLength(r.ContentLength),
		RemoteAddr:    r.RemoteAddr,
		RemoteIP:      remoteIP,
		ClientIP:      remoteIP,
		Scheme:        scheme,
	}
}
