package server

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"html"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/http/httputils"
	"github.com/jdillenkofer/pithos/internal/http/middlewares"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	storage "github.com/jdillenkofer/pithos/internal/storage"
)

type Server struct {
	requestAuthorizer authorization.RequestAuthorizer
	storage           storage.Storage
	tracer            trace.Tracer
}

func SetupServer(credentials []settings.Credentials, region string, apiEndpoint string, websiteEndpoint string, requestAuthorizer authorization.RequestAuthorizer, storage storage.Storage) http.Handler {
	server := &Server{
		requestAuthorizer: requestAuthorizer,
		storage:           storage,
		tracer:            otel.Tracer("internal/http/server"),
	}
	apiMux := http.NewServeMux()
	// @TODO(auth): list requests authorization does not filter out buckets and objects the user is not allowed to access
	apiMux.HandleFunc("GET /", server.listBucketsHandler)
	apiMux.HandleFunc("HEAD /{bucket}", server.headBucketHandler)
	apiMux.HandleFunc("GET /{bucket}", server.listObjectsOrListMultipartUploadsOrGetBucketWebsiteHandler)
	apiMux.HandleFunc("PUT /{bucket}", server.createBucketOrPutBucketWebsiteHandler)
	apiMux.HandleFunc("DELETE /{bucket}", server.deleteBucketOrDeleteBucketWebsiteHandler)
	apiMux.HandleFunc("POST /{bucket}", server.postBucketHandler)
	apiMux.HandleFunc("HEAD /{bucket}/{key...}", server.headObjectHandler)
	apiMux.HandleFunc("GET /{bucket}/{key...}", server.getObjectOrListPartsHandler)
	apiMux.HandleFunc("POST /{bucket}/{key...}", server.createMultipartUploadOrCompleteMultipartUploadHandler)
	apiMux.HandleFunc("PUT /{bucket}/{key...}", server.uploadPartOrPutObjectHandler)
	apiMux.HandleFunc("DELETE /{bucket}/{key...}", server.abortMultipartUploadOrDeleteObjectHandler)
	var apiHandler http.Handler = apiMux
	apiHandler = middlewares.MakeVirtualHostBucketAddressingMiddleware(apiEndpoint, apiHandler)

	websiteMux := http.NewServeMux()
	websiteMux.HandleFunc("GET /{bucket}/{key...}", server.serveWebsiteGetObject)
	websiteMux.HandleFunc("HEAD /{bucket}/{key...}", server.serveWebsiteHeadObject)
	websiteMux.HandleFunc("GET /{bucket}", server.serveWebsiteGetObject)
	websiteMux.HandleFunc("HEAD /{bucket}", server.serveWebsiteHeadObject)
	var websiteHandler http.Handler = websiteMux

	fallbackHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		// Strip port if present
		if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
			if bracketIdx := strings.LastIndex(host, "]"); bracketIdx < colonIdx {
				host = host[:colonIdx]
			}
		}
		r.URL.Path = "/" + host + r.URL.Path
		slog.Debug("Custom domain website request", "host", r.Host, "bucket", host, "path", r.URL.Path)
		websiteHandler.ServeHTTP(w, r)
	})

	// Set up handler with hostname-based routing.
	rootHandler := middlewares.MakeHostnameRoutingHandler(apiEndpoint, apiHandler, websiteEndpoint, websiteHandler, fallbackHandler)
	rootHandler = makeAuditRequestContextMiddleware(rootHandler)

	var authCreds []authentication.Credentials
	if credentials != nil {
		slog.Info("Authentication is enabled")
		authCreds = sliceutils.Map(func(cred settings.Credentials) authentication.Credentials {
			return authentication.Credentials{
				AccessKeyId:     cred.AccessKeyId,
				SecretAccessKey: cred.SecretAccessKey,
			}
		}, credentials)
		rootHandler = authentication.MakeSignatureMiddleware(authCreds, region, rootHandler)
	} else {
		slog.Warn("Authentication is disabled, this is not recommended for production use")
	}

	return rootHandler
}

func makeAuditRequestContextMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), authentication.RequestIDContextKey{}, ulid.Make().String())
		if ip := getRemoteIP(r.RemoteAddr); ip != nil {
			ctx = context.WithValue(ctx, authentication.ClientIPContextKey{}, *ip)
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func makeHealthCheckHandler(dbs []database.Database) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for _, db := range dbs {
			err := db.PingContext(ctx)
			if err != nil {
				w.WriteHeader(503)
				w.Write([]byte("Unhealthy"))
				return
			}
		}
		w.WriteHeader(200)
		w.Write([]byte("Healthy"))
	}
}

func SetupMonitoringServer(dbs []database.Database) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", makeHealthCheckHandler(dbs))
	var rootHandler http.Handler = mux
	return rootHandler
}

const bucketPath = "bucket"
const keyPath = "key"

const prefixQuery = "prefix"
const delimiterQuery = "delimiter"
const startAfterQuery = "start-after"
const maxKeysQuery = "max-keys"
const uploadIdQuery = "uploadId"
const uploadsQuery = "uploads"
const partNumberQuery = "partNumber"
const markerQuery = "marker"
const keyMarkerQuery = "key-marker"
const uploadIdMarkerQuery = "upload-id-marker"
const partNumberMarkerQuery = "part-number-marker"
const maxUploadsQuery = "max-uploads"
const maxPartsQuery = "max-parts"
const listTypeQuery = "list-type"
const continuationTokenQuery = "continuation-token"
const websiteQuery = "website"
const appendQuery = "append"

const acceptRangesHeader = "Accept-Ranges"
const expectHeader = "Expect"
const contentRangeHeader = "Content-Range"
const contentLengthHeader = "Content-Length"
const rangeHeader = "Range"
const ifMatchHeader = "If-Match"
const ifNoneMatchHeader = "If-None-Match"
const etagHeader = "ETag"
const lastModifiedHeader = "Last-Modified"
const contentTypeHeader = "Content-Type"
const contentMd5Header = "Content-MD5"
const locationHeader = "Location"
const checksumTypeHeader = "x-amz-checksum-type"
const checksumAlgorithmHeader = "x-amz-sdk-checksum-algorithm"
const checksumCRC32Header = "x-amz-checksum-crc32"
const checksumCRC32CHeader = "x-amz-checksum-crc32c"
const checksumCRC64NVMEHeader = "x-amz-checksum-crc64nvme"
const checksumSHA1Header = "x-amz-checksum-sha1"
const checksumSHA256Header = "x-amz-checksum-sha256"
const writeOffsetBytesHeader = "x-amz-write-offset-bytes"

const applicationXmlContentType = "application/xml"

const storageClassStandard = "STANDARD"

type BucketResult struct {
	XMLName      xml.Name `xml:"Bucket"`
	CreationDate string   `xml:"CreationDate"`
	Name         string   `xml:"Name"`
}

type OwnerResult struct {
	XMLName     xml.Name `xml:"Owner"`
	DisplayName string   `xml:"DisplayName"`
	Id          string   `xml:"ID"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name        `xml:"ListAllMyBucketsResult"`
	Buckets []*BucketResult `xml:">Buckets"`
	Owner   *OwnerResult    `xml:"Owner"`
}

type Prefix struct {
	XMLName xml.Name `xml:"Prefix"`
}

type ContentResult struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixResult struct {
	Prefix string `xml:"Prefix"`
}

type ListBucketResult struct {
	XMLName        xml.Name              `xml:"ListBucketResult"`
	IsTruncated    bool                  `xml:"IsTruncated"`
	Contents       []*ContentResult      `xml:"Contents"`
	Name           string                `xml:"Name"`
	Prefix         *string               `xml:"Prefix"`
	Delimiter      *string               `xml:"Delimiter"`
	MaxKeys        int32                 `xml:"MaxKeys"`
	CommonPrefixes []*CommonPrefixResult `xml:"CommonPrefixes"`
	KeyCount       int32                 `xml:"KeyCount"`
	StartAfter     *string               `xml:"StartAfter"`
	Marker         *string               `xml:"Marker"`
	NextMarker     *string               `xml:"NextMarker"`
}

type ListBucketV2Result struct {
	XMLName               xml.Name              `xml:"ListBucketResult"`
	IsTruncated           bool                  `xml:"IsTruncated"`
	Contents              []*ContentResult      `xml:"Contents"`
	Name                  string                `xml:"Name"`
	Prefix                *string               `xml:"Prefix"`
	Delimiter             *string               `xml:"Delimiter"`
	MaxKeys               int32                 `xml:"MaxKeys"`
	CommonPrefixes        []*CommonPrefixResult `xml:"CommonPrefixes"`
	KeyCount              int32                 `xml:"KeyCount"`
	ContinuationToken     *string               `xml:"ContinuationToken"`
	NextContinuationToken *string               `xml:"NextContinuationToken"`
	StartAfter            *string               `xml:"StartAfter"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type Part struct {
	ChecksumCRC32     *string `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string `xml:"ChecksumSHA256"`
	ETag              string  `xml:"ETag"`
	PartNumber        int32   `xml:"PartNumber"`
}

type CompleteMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []*Part  `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	XMLName           xml.Name `xml:"CompleteMultipartUploadResult"`
	Location          string   `xml:"Location"`
	Bucket            string   `xml:"Bucket"`
	Key               string   `xml:"Key"`
	ETag              string   `xml:"ETag"`
	ChecksumCRC32     *string  `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string  `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string  `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string  `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string  `xml:"ChecksumSHA256"`
	ChecksumType      *string  `xml:"ChecksumType"`
}

type UploadResult struct {
	Key          string `xml:"Key"`
	UploadId     string `xml:"UploadId"`
	Initiated    string `xml:"Initiated"`
	StorageClass string `xml:"StorageClass"`
}

type ListMultipartUploadsResult struct {
	XMLName            xml.Name              `xml:"ListMultipartUploadsResult"`
	Bucket             string                `xml:"Bucket"`
	KeyMarker          *string               `xml:"KeyMarker"`
	UploadIdMarker     *string               `xml:"UploadIdMarker"`
	NextKeyMarker      *string               `xml:"NextKeyMarker"`
	NextUploadIdMarker *string               `xml:"NextUploadIdMarker"`
	MaxUploads         int32                 `xml:"MaxUploads"`
	IsTruncated        bool                  `xml:"IsTruncated"`
	Delimiter          *string               `xml:"Delimiter"`
	Prefix             *string               `xml:"Prefix"`
	Uploads            []*UploadResult       `xml:"Upload"`
	CommonPrefixes     []*CommonPrefixResult `xml:"CommonPrefixes"`
}

type PartResult struct {
	ETag              string  `xml:"ETag"`
	ChecksumCRC32     *string `xml:"ChecksumCRC32"`
	ChecksumCRC32C    *string `xml:"ChecksumCRC32C"`
	ChecksumCRC64NVME *string `xml:"ChecksumCRC64NVME"`
	ChecksumSHA1      *string `xml:"ChecksumSHA1"`
	ChecksumSHA256    *string `xml:"ChecksumSHA256"`
	LastModified      string  `xml:"LastModified"`
	PartNumber        int32   `xml:"PartNumber"`
	Size              int64   `xml:"Size"`
}

type ListPartsResult struct {
	XMLName              xml.Name      `xml:"ListPartsResult"`
	Bucket               string        `xml:"Bucket"`
	Key                  string        `xml:"Key"`
	UploadId             string        `xml:"UploadId"`
	PartNumberMarker     *string       `xml:"PartNumberMarker"`
	NextPartNumberMarker *string       `xml:"NextPartNumberMarker"`
	MaxParts             int32         `xml:"MaxParts"`
	IsTruncated          bool          `xml:"IsTruncated"`
	Parts                []*PartResult `xml:"Part"`
	// @TODO: Initiator and Owner missing
	StorageClass      string  `xml:"StorageClass"`
	ChecksumAlgorithm *string `xml:"ChecksumAlgorithm"`
	ChecksumType      *string `xml:"ChecksumType"`
}

type WebsiteConfigurationIndexDocument struct {
	Suffix string `xml:"Suffix"`
}

type WebsiteConfigurationErrorDocument struct {
	Key string `xml:"Key"`
}

type WebsiteConfigurationRedirectAllRequestsTo struct {
	HostName string `xml:"HostName"`
	Protocol string `xml:"Protocol,omitempty"`
}

type WebsiteConfigurationRoutingRule struct {
	// We only need to detect presence, not parse contents
}

type WebsiteConfigurationRequest struct {
	XMLName               xml.Name                                   `xml:"WebsiteConfiguration"`
	IndexDocument         *WebsiteConfigurationIndexDocument         `xml:"IndexDocument"`
	ErrorDocument         *WebsiteConfigurationErrorDocument         `xml:"ErrorDocument"`
	RedirectAllRequestsTo *WebsiteConfigurationRedirectAllRequestsTo `xml:"RedirectAllRequestsTo"`
	RoutingRules          []WebsiteConfigurationRoutingRule          `xml:"RoutingRules>RoutingRule"`
}

type WebsiteConfigurationResponse struct {
	XMLName       xml.Name                           `xml:"WebsiteConfiguration"`
	Xmlns         string                             `xml:"xmlns,attr"`
	IndexDocument *WebsiteConfigurationIndexDocument `xml:"IndexDocument"`
	ErrorDocument *WebsiteConfigurationErrorDocument `xml:"ErrorDocument,omitempty"`
}

type DeleteObjectEntry struct {
	Key       string  `xml:"Key"`
	VersionId *string `xml:"VersionId"`
	ETag      *string `xml:"ETag"`
}

type DeleteObjectsRequest struct {
	XMLName xml.Name             `xml:"Delete"`
	Quiet   bool                 `xml:"Quiet"`
	Objects []*DeleteObjectEntry `xml:"Object"`
}

type DeletedEntry struct {
	Key string `xml:"Key"`
}

type DeleteErrorEntry struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type DeleteObjectsResult struct {
	XMLName xml.Name            `xml:"DeleteResult"`
	Deleted []*DeletedEntry     `xml:"Deleted"`
	Errors  []*DeleteErrorEntry `xml:"Error"`
}

var ErrInvalidRequest = fmt.Errorf("InvalidRequest")

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestId string   `xml:"RequestId"`
}

func xmlMarshalWithDocType(v any) ([]byte, error) {
	xmlResponse, err := xml.MarshalIndent(v, "", "  ")
	if err != nil {
		slog.Error(fmt.Sprintf("Error during handleError: %v", err))
		return nil, err
	}
	xmlResponse = []byte(xml.Header + string(xmlResponse))
	return xmlResponse, nil
}

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
	statusCode := 500
	errResponse := ErrorResponse{}
	errResponse.Code = err.Error()
	errResponse.Message = err.Error()
	errResponse.Resource = r.URL.Path
	switch err {
	case storage.ErrInvalidBucketName:
		statusCode = 400
	case storage.ErrBadDigest:
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
	case storage.ErrEntityTooLarge:
		statusCode = 413
	case storage.ErrTooManyParts:
		statusCode = 400
	case storage.ErrInvalidWriteOffset:
		statusCode = 400
	case storage.ErrPreconditionFailed:
		statusCode = 412
	case storage.ErrNotModified:
		statusCode = 304
	default:
		slog.Error(fmt.Sprintf("Unhandled internal error: %v", err))
		statusCode = 500
		errResponse.Code = "InternalError"
		errResponse.Message = "InternalError"
	}
	xmlErrorResponse, err := xmlMarshalWithDocType(errResponse)
	if err != nil {
		slog.Error(fmt.Sprintf("Error during handleError: %v", err))
		return
	}
	w.WriteHeader(statusCode)
	w.Write(xmlErrorResponse)
}

func (s *Server) authorizeRequest(ctx context.Context, operation string, bucket *string, key *string, w http.ResponseWriter, r *http.Request) bool {
	request, isAuthenticated := makeAuthorizationRequest(ctx, operation, bucket, key, r)
	authorized, err := s.requestAuthorizer.AuthorizeRequest(ctx, request)
	if err != nil {
		slog.Error(fmt.Sprintf("Authorization error: %v", err))
		handleError(err, w, r)
		return true
	}
	if !authorized {
		slog.Debug(fmt.Sprintf("Unauthorized request: %v", request))
		if !isAuthenticated {
			w.WriteHeader(401)
		} else {
			w.WriteHeader(403)
		}
		return true
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

	request := &authorization.Request{
		Operation: operation,
		Authorization: authorization.Authorization{
			AccessKeyId: accessKeyId,
		},
		Bucket:      bucket,
		Key:         key,
		HttpRequest: makeAuthorizationHTTPRequest(r),
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

func (s *Server) authorizeListObject(ctx context.Context, request *authorization.Request, key string) (bool, error) {
	requestResourceAuthorizer, ok := s.requestAuthorizer.(authorization.RequestResourceAuthorizer)
	if !ok {
		return true, nil
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

func (s *Server) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listBucketsHandler")
	defer span.End()

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListBuckets, nil, nil, w, r)
	if shouldReturn {
		return
	}
	slog.Info("Listing Buckets")
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		handleError(err, w, r)
		return
	}
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListBuckets, nil, nil, r)
	listAllMyBucketsResult := ListAllMyBucketsResult{
		Buckets: []*BucketResult{},
	}
	for _, bucket := range buckets {
		allowed, err := s.authorizeListBucket(ctx, baseRequest, bucket.Name.String())
		if err != nil {
			handleError(err, w, r)
			return
		}
		if !allowed {
			continue
		}
		listAllMyBucketsResult.Buckets = append(listAllMyBucketsResult.Buckets, &BucketResult{
			Name:         bucket.Name.String(),
			CreationDate: bucket.CreationDate.UTC().Format(time.RFC3339),
		})
	}
	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(listAllMyBucketsResult)
	w.Write(out)
}

func (s *Server) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.headBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}
	shouldReturn := s.authorizeRequest(ctx, authorization.OperationHeadBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}
	slog.Info("Head bucket", "bucket", bucketName)
	_, err = s.storage.HeadBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) listObjectsOrListMultipartUploadsOrGetBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(websiteQuery) {
		s.getBucketWebsiteHandler(w, r)
		return
	}
	if query.Has(uploadsQuery) {
		s.listMultipartUploadsHandler(w, r)
		return
	}

	listType := query.Get(listTypeQuery)
	if listType == "2" {
		s.listObjectsV2Handler(w, r)
		return
	}
	s.listObjectsHandler(w, r)
}

func (s *Server) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listMultipartUploadsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListMultipartUploads, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	keyMarker := httputils.GetQueryParam(query, keyMarkerQuery)
	uploadIdMarker := httputils.GetQueryParam(query, uploadIdMarkerQuery)
	maxUploads := query.Get(maxUploadsQuery)
	maxUploadsI64, err := strconv.ParseInt(maxUploads, 10, 32)
	if err != nil || maxUploadsI64 < 0 {
		maxUploadsI64 = 1000
	}
	maxUploadsI32 := int32(maxUploadsI64)

	opts := storage.ListMultipartUploadsOptions{Prefix: prefix, Delimiter: delimiter, KeyMarker: keyMarker, UploadIdMarker: uploadIdMarker, MaxUploads: maxUploadsI32}
	slog.Info("Listing MultipartUploads")
	result, nextKeyMarker, nextUploadIDMarker, err := s.listAndFilterMultipartUploads(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listMultipartUploadsResult := ListMultipartUploadsResult{
		Bucket:             result.BucketName.String(),
		KeyMarker:          ptrutils.ToPtr(result.KeyMarker),
		UploadIdMarker:     ptrutils.ToPtr(result.UploadIdMarker),
		NextKeyMarker:      nextKeyMarker,
		NextUploadIdMarker: nextUploadIDMarker,
		MaxUploads:         maxUploadsI32,
		IsTruncated:        result.IsTruncated,
		Delimiter:          ptrutils.ToPtr(result.Delimiter),
		Prefix:             ptrutils.ToPtr(result.Prefix),
		Uploads:            []*UploadResult{},
		CommonPrefixes:     []*CommonPrefixResult{},
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, upload := range result.Uploads {
		listMultipartUploadsResult.Uploads = append(listMultipartUploadsResult.Uploads, &UploadResult{
			Key:          upload.Key.String(),
			UploadId:     upload.UploadId.String(),
			Initiated:    upload.Initiated.UTC().Format(time.RFC3339),
			StorageClass: storageClassStandard,
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listMultipartUploadsResult.CommonPrefixes = append(listMultipartUploadsResult.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}
	out, _ := xmlMarshalWithDocType(listMultipartUploadsResult)
	w.Write(out)
}

func (s *Server) listAndFilterMultipartUploads(ctx context.Context, r *http.Request, bucketName storage.BucketName, opts storage.ListMultipartUploadsOptions) (*storage.ListMultipartUploadsResult, *string, *string, error) {
	maxUploads := opts.MaxUploads
	if maxUploads <= 0 {
		maxUploads = 1000
	}
	collectedUploads := []storage.Upload{}
	collectedPrefixes := []string{}
	seenPrefixes := map[string]struct{}{}
	keyMarker := opts.KeyMarker
	uploadIDMarker := opts.UploadIdMarker
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListMultipartUploads, ptrutils.ToPtr(bucketName.String()), nil, r)
	var nextKeyMarker *string
	var nextUploadIDMarker *string

	for {
		result, err := s.storage.ListMultipartUploads(ctx, bucketName, storage.ListMultipartUploadsOptions{
			Prefix:         opts.Prefix,
			Delimiter:      opts.Delimiter,
			KeyMarker:      keyMarker,
			UploadIdMarker: uploadIDMarker,
			MaxUploads:     maxUploads,
		})
		if err != nil {
			return nil, nil, nil, err
		}

		lastKeyMarker := keyMarker
		lastUploadIDMarker := uploadIDMarker
		for uploadIndex, upload := range result.Uploads {
			uploadKey := upload.Key.String()
			uploadID := upload.UploadId.String()
			lastKeyMarker = &uploadKey
			lastUploadIDMarker = &uploadID
			allowed, err := s.authorizeListMultipartUpload(ctx, baseRequest, uploadKey, uploadID)
			if err != nil {
				return nil, nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedUploads = append(collectedUploads, upload)
			if int32(len(collectedUploads)) >= maxUploads {
				hasMore := uploadIndex < len(result.Uploads)-1 || len(result.CommonPrefixes) > 0 || result.IsTruncated
				if hasMore {
					nextKeyMarker = lastKeyMarker
					nextUploadIDMarker = lastUploadIDMarker
					return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, NextKeyMarker: *lastKeyMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, NextUploadIdMarker: *lastUploadIDMarker, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: true}, nextKeyMarker, nextUploadIDMarker, nil
				}
				return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
			}
		}
		for _, commonPrefix := range result.CommonPrefixes {
			lastKeyMarker = &commonPrefix
			lastUploadIDMarker = ptrutils.ToPtr("")
			allowed, err := s.authorizeListMultipartUpload(ctx, baseRequest, commonPrefix, "")
			if err != nil {
				return nil, nil, nil, err
			}
			if !allowed {
				continue
			}
			if _, exists := seenPrefixes[commonPrefix]; exists {
				continue
			}
			seenPrefixes[commonPrefix] = struct{}{}
			collectedPrefixes = append(collectedPrefixes, commonPrefix)
		}

		if !result.IsTruncated {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		if lastKeyMarker == nil || lastUploadIDMarker == nil {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		if keyMarker != nil && uploadIDMarker != nil && *keyMarker == *lastKeyMarker && *uploadIDMarker == *lastUploadIDMarker {
			return &storage.ListMultipartUploadsResult{BucketName: result.BucketName, KeyMarker: result.KeyMarker, UploadIdMarker: result.UploadIdMarker, Prefix: result.Prefix, Delimiter: result.Delimiter, MaxUploads: maxUploads, CommonPrefixes: collectedPrefixes, Uploads: collectedUploads, IsTruncated: false}, nil, nil, nil
		}
		keyMarker = ptrutils.ToPtr(*lastKeyMarker)
		uploadIDMarker = ptrutils.ToPtr(*lastUploadIDMarker)
		nextKeyMarker = keyMarker
		nextUploadIDMarker = uploadIDMarker
	}
}

func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listObjectsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	marker := httputils.GetQueryParam(query, markerQuery)
	startAfter := httputils.GetQueryParam(query, startAfterQuery)

	if marker != nil {
		startAfter = marker
	}

	maxKeys := query.Get(maxKeysQuery)
	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	opts := storage.ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, StartAfter: startAfter, MaxKeys: maxKeysI32}
	slog.Info("Listing objects", "bucket", bucketName.String())
	result, nextMarker, err := s.listAndFilterObjects(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listBucketResult := ListBucketResult{
		Name:           bucketName.String(),
		Prefix:         prefix,
		Delimiter:      delimiter,
		StartAfter:     httputils.GetQueryParam(query, startAfterQuery), // Original start-after from request
		Marker:         marker,
		KeyCount:       int32(len(result.Objects)),
		MaxKeys:        maxKeysI32,
		CommonPrefixes: []*CommonPrefixResult{},
		IsTruncated:    result.IsTruncated,
		Contents:       []*ContentResult{},
	}

	// Set NextMarker if results are truncated and we have objects
	if result.IsTruncated && nextMarker != nil {
		listBucketResult.NextMarker = nextMarker
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, object := range result.Objects {
		listBucketResult.Contents = append(listBucketResult.Contents, &ContentResult{
			Key:          object.Key.String(),
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: storageClassStandard,
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listBucketResult.CommonPrefixes = append(listBucketResult.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}
	out, _ := xmlMarshalWithDocType(listBucketResult)
	w.Write(out)
}

func (s *Server) listAndFilterObjects(ctx context.Context, r *http.Request, bucketName storage.BucketName, opts storage.ListObjectsOptions) (*storage.ListBucketResult, *string, error) {
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	collectedObjects := []storage.Object{}
	collectedPrefixes := []string{}
	seenPrefixes := map[string]struct{}{}
	startAfter := opts.StartAfter
	var nextMarker *string
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, r)

	for {
		result, err := s.storage.ListObjects(ctx, bucketName, storage.ListObjectsOptions{
			Prefix:     opts.Prefix,
			Delimiter:  opts.Delimiter,
			StartAfter: startAfter,
			MaxKeys:    maxKeys,
		})
		if err != nil {
			return nil, nil, err
		}

		lastScanned := startAfter
		for objectIndex, object := range result.Objects {
			key := object.Key.String()
			lastScanned = &key
			allowed, err := s.authorizeListObject(ctx, baseRequest, key)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedObjects = append(collectedObjects, object)
			if int32(len(collectedObjects)) >= maxKeys {
				hasMore := objectIndex < len(result.Objects)-1 || len(result.CommonPrefixes) > 0 || result.IsTruncated
				if hasMore {
					nextMarker = lastScanned
					return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: true}, nextMarker, nil
				}
				return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
			}
		}
		for _, commonPrefix := range result.CommonPrefixes {
			lastScanned = &commonPrefix
			allowed, err := s.authorizeListObject(ctx, baseRequest, commonPrefix)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			if _, exists := seenPrefixes[commonPrefix]; exists {
				continue
			}
			seenPrefixes[commonPrefix] = struct{}{}
			collectedPrefixes = append(collectedPrefixes, commonPrefix)
		}

		if !result.IsTruncated {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		if lastScanned == nil {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		if startAfter != nil && *startAfter == *lastScanned {
			return &storage.ListBucketResult{Objects: collectedObjects, CommonPrefixes: collectedPrefixes, IsTruncated: false}, nil, nil
		}
		startAfter = ptrutils.ToPtr(*lastScanned)
		nextMarker = startAfter
	}
}

func (s *Server) listObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listObjectsV2Handler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()

	prefix := httputils.GetQueryParam(query, prefixQuery)
	delimiter := httputils.GetQueryParam(query, delimiterQuery)
	continuationToken := httputils.GetQueryParam(query, continuationTokenQuery)
	maxKeys := query.Get(maxKeysQuery)

	startAfter := httputils.GetQueryParam(query, startAfterQuery)
	if continuationToken != nil {
		startAfter = continuationToken
	}

	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	opts := storage.ListObjectsOptions{Prefix: prefix, Delimiter: delimiter, StartAfter: startAfter, MaxKeys: maxKeysI32}
	slog.Info("Listing objects V2", "bucket", bucketName.String())
	result, nextToken, err := s.listAndFilterObjects(ctx, r, bucketName, opts)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// Prepare the V2 response
	listBucketV2Result := ListBucketV2Result{
		Name:              bucketName.String(),
		Prefix:            prefix,
		Delimiter:         delimiter,
		MaxKeys:           maxKeysI32,
		KeyCount:          int32(len(result.Objects)),
		IsTruncated:       result.IsTruncated,
		ContinuationToken: continuationToken,
		StartAfter:        httputils.GetQueryParam(query, startAfterQuery), // Original start-after from request
		CommonPrefixes:    []*CommonPrefixResult{},
		Contents:          []*ContentResult{},
	}

	if result.IsTruncated && nextToken != nil {
		listBucketV2Result.NextContinuationToken = nextToken
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)

	for _, object := range result.Objects {
		contentResult := &ContentResult{
			Key:          object.Key.String(),
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: storageClassStandard,
		}

		listBucketV2Result.Contents = append(listBucketV2Result.Contents, contentResult)
	}

	for _, commonPrefix := range result.CommonPrefixes {
		listBucketV2Result.CommonPrefixes = append(listBucketV2Result.CommonPrefixes, &CommonPrefixResult{Prefix: commonPrefix})
	}

	out, _ := xmlMarshalWithDocType(listBucketV2Result)
	w.Write(out)
}

func (s *Server) createBucketOrPutBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(websiteQuery) {
		s.putBucketWebsiteHandler(w, r)
		return
	}
	s.createBucketHandler(w, r)
}

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.createBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCreateBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.Info("Creating bucket", "bucket", bucketName.String())
	err = s.storage.CreateBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	responseHeaders := w.Header()
	responseHeaders.Set(locationHeader, bucketName.String())
	w.WriteHeader(200)
}

func (s *Server) deleteBucketOrDeleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(websiteQuery) {
		s.deleteBucketWebsiteHandler(w, r)
		return
	}
	s.deleteBucketHandler(w, r)
}

func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucket, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.Info("Deleting bucket", "bucket", bucketName.String())
	err = s.storage.DeleteBucket(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.headObjectHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationHeadObject, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	slog.Info("Head object", "bucket", bucketName.String(), "key", key.String())

	var headOpts *storage.HeadObjectOptions
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	if ifMatch != nil || ifNoneMatch != nil {
		headOpts = &storage.HeadObjectOptions{}
		if ifMatch != nil {
			headOpts.IfMatchETag = ifMatch
		}
		if ifNoneMatch != nil {
			headOpts.IfNoneMatchETag = ifNoneMatch
		}
	}

	object, err := s.storage.HeadObject(ctx, bucketName, key, headOpts)
	if err != nil {
		if err == storage.ErrNotModified {
			w.WriteHeader(304)
			return
		}
		if err == storage.ErrPreconditionFailed {
			w.WriteHeader(412)
			return
		}
		handleError(err, w, r)
		return
	}
	responseHeaders := w.Header()
	setETagHeaderFromObject(responseHeaders, object)
	setChecksumHeadersFromObject(responseHeaders, object)

	gmtTimeLoc := time.FixedZone("GMT", 0)
	responseHeaders.Set(lastModifiedHeader, object.LastModified.In(gmtTimeLoc).Format(time.RFC1123))
	if object.ContentType != nil {
		responseHeaders.Set(contentTypeHeader, *object.ContentType)
	}
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
}

// generateContentRangeValue creates a Content-Range header value from a storage.ByteRange.
// For suffix ranges (Start=nil), it uses the object size to calculate the actual range.
func generateContentRangeValue(br storage.ByteRange, objectSize int64) string {
	start := int64(0)
	if br.Start != nil {
		start = *br.Start
	} else if br.End != nil {
		// Suffix range: calculate start from object size
		suffixLength := min(*br.End, objectSize)
		start = objectSize - suffixLength
	}

	end := objectSize - 1
	if br.Start != nil && br.End != nil {
		// Normal range with explicit end (exclusive, so subtract 1 for inclusive).
		// Clamp to objectSize to match the storage layer's normalizeAndValidateRanges behavior.
		end = min(*br.End, objectSize) - 1
	} else if br.Start == nil && br.End != nil {
		// Suffix range
		end = objectSize - 1
	}

	contentRangeValue := fmt.Sprintf("bytes %d-%d/%d", start, end, objectSize)
	return contentRangeValue
}

var errInvalidByteRange error = fmt.Errorf("invalid byte range")

// parseRangeHeader parses HTTP Range header and returns storage.ByteRange array.
// It converts HTTP ranges (inclusive end) to storage ranges (exclusive end) automatically.
// Suffix ranges (bytes=-N) are passed through as-is to be resolved by the storage layer.
func parseRangeHeader(rangeHeader string) ([]storage.ByteRange, error) {
	var ranges []storage.ByteRange
	if rangeHeader == "" {
		return ranges, nil
	}

	rangeUnitAndRangesSplit := strings.SplitN(rangeHeader, "=", 2)
	if len(rangeUnitAndRangesSplit) != 2 || rangeUnitAndRangesSplit[0] != "bytes" {
		return nil, errInvalidByteRange
	}

	rangesSplit := strings.SplitSeq(rangeUnitAndRangesSplit[1], ",")
	for rangeVal := range rangesSplit {
		rangeVal = strings.TrimSpace(rangeVal)
		byteSplit := strings.SplitN(rangeVal, "-", 2)
		if len(byteSplit) != 2 {
			continue
		}

		var start *int64
		var end *int64

		if byteSplit[0] != "" {
			startByte, err := strconv.ParseInt(byteSplit[0], 10, 64)
			if err == nil {
				start = &startByte
			}
		}
		if byteSplit[1] != "" {
			endByte, err := strconv.ParseInt(byteSplit[1], 10, 64)
			if err == nil {
				end = &endByte
			}
		}

		if start == nil && end != nil {
			// Suffix range (bytes=-N): pass through as-is
			ranges = append(ranges, storage.ByteRange{Start: nil, End: end})
		} else if start != nil {
			// Normal range: convert inclusive end to exclusive end
			var exclusiveEnd *int64
			if end != nil {
				excEnd := *end + 1
				exclusiveEnd = &excEnd
			}
			ranges = append(ranges, storage.ByteRange{Start: start, End: exclusiveEnd})
		}
	}

	return ranges, nil
}

func (s *Server) getObjectOrListPartsHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has(uploadIdQuery) {
		s.listPartsHandler(w, r)
		return
	}
	s.getObjectHandler(w, r)
}

func (s *Server) listPartsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.listPartsHandler")
	defer span.End()

	query := r.URL.Query()

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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListParts, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	uploadId, err := storage.NewUploadId(query.Get(uploadIdQuery))
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberMarker := httputils.GetQueryParam(query, partNumberMarkerQuery)
	maxParts := query.Get(maxPartsQuery)
	maxPartsI64, err := strconv.ParseInt(maxParts, 10, 32)
	if err != nil {
		maxPartsI64 = 1000
	}
	if maxPartsI64 < 0 || maxPartsI64 > 1000 {
		w.WriteHeader(400)
		return
	}
	maxPartsI32 := int32(maxPartsI64)

	result, nextPartNumberMarker, err := s.listAndFilterParts(ctx, r, bucketName, key, uploadId, storage.ListPartsOptions{
		PartNumberMarker: partNumberMarker,
		MaxParts:         maxPartsI32,
	})
	if err != nil {
		handleError(err, w, r)
		return
	}

	listPartsResult := ListPartsResult{
		Bucket:               result.BucketName.String(),
		Key:                  result.Key.String(),
		UploadId:             result.UploadId.String(),
		PartNumberMarker:     ptrutils.ToPtr(result.PartNumberMarker),
		NextPartNumberMarker: nextPartNumberMarker,
		MaxParts:             result.MaxParts,
		IsTruncated:          result.IsTruncated,
		Parts: sliceutils.Map(func(part *storage.MultipartPart) *PartResult {
			return &PartResult{
				ETag:              part.ETag,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
				LastModified:      part.LastModified.UTC().Format(time.RFC3339),
				PartNumber:        part.PartNumber,
				Size:              part.Size,
			}
		}, result.Parts),
		StorageClass: storageClassStandard,
	}

	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(listPartsResult)
	w.Write(out)
}

func (s *Server) listAndFilterParts(ctx context.Context, r *http.Request, bucketName storage.BucketName, key storage.ObjectKey, uploadID storage.UploadId, opts storage.ListPartsOptions) (*storage.ListPartsResult, *string, error) {
	maxParts := opts.MaxParts
	if maxParts <= 0 {
		maxParts = 1000
	}
	partNumberMarker := opts.PartNumberMarker
	collectedParts := make([]*storage.MultipartPart, 0, maxParts)
	var nextPartNumberMarker *string
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationListParts, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), r)

	for {
		result, err := s.storage.ListParts(ctx, bucketName, key, uploadID, storage.ListPartsOptions{
			PartNumberMarker: partNumberMarker,
			MaxParts:         maxParts,
		})
		if err != nil {
			return nil, nil, err
		}

		lastPartNumberMarker := partNumberMarker
		for partIndex, part := range result.Parts {
			partNumberStr := strconv.Itoa(int(part.PartNumber))
			lastPartNumberMarker = &partNumberStr
			allowed, err := s.authorizeListPart(ctx, baseRequest, part.PartNumber)
			if err != nil {
				return nil, nil, err
			}
			if !allowed {
				continue
			}
			collectedParts = append(collectedParts, part)
			if int32(len(collectedParts)) >= maxParts {
				hasMore := partIndex < len(result.Parts)-1 || result.IsTruncated
				if hasMore {
					nextPartNumberMarker = lastPartNumberMarker
					return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nextPartNumberMarker, MaxParts: maxParts, IsTruncated: true, Parts: collectedParts}, nextPartNumberMarker, nil
				}
				return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts}, nil, nil
			}
		}

		if !result.IsTruncated {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts}, nil, nil
		}
		if lastPartNumberMarker == nil {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts}, nil, nil
		}
		if partNumberMarker != nil && *partNumberMarker == *lastPartNumberMarker {
			return &storage.ListPartsResult{BucketName: result.BucketName, Key: result.Key, UploadId: result.UploadId, PartNumberMarker: result.PartNumberMarker, NextPartNumberMarker: nil, MaxParts: maxParts, IsTruncated: false, Parts: collectedParts}, nil, nil
		}
		partNumberMarker = ptrutils.ToPtr(*lastPartNumberMarker)
		nextPartNumberMarker = partNumberMarker
	}
}

func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getObjectHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetObject, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	rangeHeaderValue := r.Header.Get(rangeHeader)
	slog.Info("Getting object", "bucket", bucketName.String(), "key", key.String())

	// Parse range header and convert to storage.ByteRange (validation will be done in GetObject)
	storageRanges, err := parseRangeHeader(rangeHeaderValue)
	if err != nil {
		w.WriteHeader(416)
		return
	}

	var getOpts *storage.GetObjectOptions
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	ifNoneMatch := getHeaderAsPtr(r.Header, ifNoneMatchHeader)
	if ifMatch != nil || ifNoneMatch != nil {
		getOpts = &storage.GetObjectOptions{}
		if ifMatch != nil {
			getOpts.IfMatchETag = ifMatch
		}
		if ifNoneMatch != nil {
			getOpts.IfNoneMatchETag = ifNoneMatch
		}
	}

	// GetObject now returns metadata and readers in a single transaction
	// It also validates the ranges and returns ErrInvalidRange if invalid
	object, readers, err := s.storage.GetObject(ctx, bucketName, key, storageRanges, getOpts)
	if err != nil {
		if err == storage.ErrNotModified {
			w.WriteHeader(304)
			return
		}
		handleError(err, w, r)
		return
	}

	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}
	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	setETagHeaderFromObject(responseHeaders, object)

	// Calculate sizes for each range
	var sizes []int64
	var totalSize int64 = 0
	if len(storageRanges) > 0 {
		for _, byteRange := range storageRanges {
			var size int64
			if byteRange.Start == nil && byteRange.End != nil {
				// Suffix range: size is the suffix length (or object size if smaller)
				suffixLength := *byteRange.End
				size = min(suffixLength, object.Size)
			} else if byteRange.Start != nil {
				// Normal range (End is exclusive)
				// Clamp end to object size to match what normalizeAndValidateRanges does in the storage layer.
				var end int64 = object.Size
				if byteRange.End != nil {
					end = min(*byteRange.End, object.Size)
				}
				size = end - *byteRange.Start
			}
			sizes = append(sizes, size)
			totalSize += size
		}
		// Wrap readers with tracing
		for i, reader := range readers {
			readers[i] = ioutils.NewTracingReadCloser(ctx, s.tracer, "GetObjectRange", reader)
		}
	} else {
		// No range specified - we only include the headers for requests without range headers
		setChecksumHeadersFromObject(responseHeaders, object)
		// Wrap the single reader with tracing
		readers[0] = ioutils.NewTracingReadCloser(ctx, s.tracer, "GetObject", readers[0])
		size := object.Size
		sizes = append(sizes, size)
		totalSize = size
	}

	defer (func() {
		for _, reader := range readers {
			reader.Close()
		}
	})()
	responseHeaders.Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	responseHeaders.Set(acceptRangesHeader, "bytes")
	if len(storageRanges) > 1 {
		separator := ulid.Make().String()
		rangeHeaderLength := int64(0)
		rangeHeaders := []string{}
		for idx := range readers {
			contentRangeValue := generateContentRangeValue(storageRanges[idx], object.Size)
			rangeHeader := fmt.Sprintf("%s: %s\r\n\r\n", contentRangeHeader, contentRangeValue)
			rangeHeaders = append(rangeHeaders, rangeHeader)
			rangeHeaderLength += int64(len(rangeHeader))
		}
		separatorLength := int64(len(separator))
		rangesCount := int64(len(storageRanges))
		startCrlfLength := (rangesCount - 1) * 2 /* \r\n */
		separatorLineLength := (2 /* -- */ + 2 /* \r\n */ + separatorLength)
		endSeparatorLineLength := separatorLineLength + 2 /* \r\n */ + 2 /* -- at the end */
		totalSize = totalSize + startCrlfLength + rangeHeaderLength + separatorLineLength*rangesCount + endSeparatorLineLength
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		responseHeaders.Set(contentTypeHeader, fmt.Sprintf("multipart/byteranges; boundary=%v", separator))
		w.WriteHeader(206)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		for idx := range readers {
			if idx > 0 {
				io.WriteString(writer, "\r\n")
			}
			io.WriteString(writer, fmt.Sprintf("--%s\r\n", separator))

			io.WriteString(writer, rangeHeaders[idx])

			ioutils.CopyN(writer, readers[idx], sizes[idx])
		}
		io.WriteString(writer, fmt.Sprintf("\r\n--%s--\r\n", separator))
	} else if len(storageRanges) == 1 {
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		contentRangeValue := generateContentRangeValue(storageRanges[0], object.Size)
		responseHeaders.Set(contentRangeHeader, contentRangeValue)
		w.WriteHeader(206)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		ioutils.CopyN(writer, readers[0], totalSize)
	} else {
		responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		w.WriteHeader(200)
		writer := ioutils.NewTracingWriter(ctx, s.tracer, "ResponseWriter", w)
		ioutils.CopyN(writer, readers[0], totalSize)
	}
}

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
	slog.Info("CreateMultipartUpload", "bucket", bucketName.String(), "key", key.String())
	result, err := s.storage.CreateMultipartUpload(ctx, bucketName, key, contentType, checksumType)
	if err != nil {
		handleError(err, w, r)
		return
	}

	initiateMultipartUploadResult := InitiateMultipartUploadResult{
		Bucket:   bucketName.String(),
		Key:      key.String(),
		UploadId: result.UploadId.String(),
	}

	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(initiateMultipartUploadResult)
	w.Write(out)
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

	data, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}
	if len(data) > 0 {
		// @TODO: if available use part checksums to verify upload
		completeMultipartUploadRequest := CompleteMultipartUploadRequest{}
		err = xml.Unmarshal(data, &completeMultipartUploadRequest)
		if err != nil {
			handleError(err, w, r)
			return
		}
	}

	slog.Info("CompleteMultipartUpload", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String())
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

	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(completeMultipartUploadResult)
	w.Write(out)
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

	slog.Info("UploadPart", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String(), "partNumber", partNumber)
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

	shouldReturn = validateMaxEntitySize(r, w)
	if shouldReturn {
		return
	}

	checksumInput, err := extractChecksumInput(r)
	if err != nil {
		handleError(err, w, r)
		return
	}

	slog.Info("Putting object", "bucket", bucketName.String(), "key", key.String())
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
	setChecksumType(responseHeaders, metadatastore.ChecksumTypeFullObject)
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

	slog.Info("Appending to object", "bucket", bucketName.String(), "key", key.String())
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

	// UploadPart
	if query.Has(uploadIdQuery) || query.Has(partNumberQuery) {
		s.uploadPartHandler(w, r)
		return
	}

	// AppendObject
	if query.Has(appendQuery) {
		s.appendObjectHandler(w, r)
		return
	}

	// PutObject
	s.putObjectHandler(w, r)
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
	slog.Info("AbortMultipartUpload", "bucket", bucketName.String(), "key", key.String(), "uploadId", uploadId.String())
	err = s.storage.AbortMultipartUpload(ctx, bucketName, key, uploadId)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteObjectHandler")
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

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteObject, ptrutils.ToPtr(bucketName.String()), ptrutils.ToPtr(key.String()), w, r)
	if shouldReturn {
		return
	}

	slog.Info("Deleting object", "bucket", bucketName.String(), "key", key.String())
	ifMatch := getHeaderAsPtr(r.Header, ifMatchHeader)
	var deleteOpts *storage.DeleteObjectOptions
	if ifMatch != nil {
		deleteOpts = &storage.DeleteObjectOptions{IfMatchETag: ifMatch}
	}
	err = s.storage.DeleteObject(ctx, bucketName, key, deleteOpts)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) abortMultipartUploadOrDeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// AbortMultipartUpload
	if query.Has(uploadIdQuery) {
		s.abortMultipartUploadHandler(w, r)
		return
	}

	// DeleteObject
	s.deleteObjectHandler(w, r)
}

func (s *Server) postBucketHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if query.Has("delete") {
		s.deleteObjectsHandler(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

const maxDeleteObjects = 1000

func (s *Server) deleteObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteObjectsHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteObjects, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var req DeleteObjectsRequest
	if err := xml.Unmarshal(data, &req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if len(req.Objects) > maxDeleteObjects {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	result := DeleteObjectsResult{
		Deleted: []*DeletedEntry{},
		Errors:  []*DeleteErrorEntry{},
	}

	// First pass: validate entries and collect valid keys for bulk delete.
	// We track the original string key alongside the parsed ObjectKey so we can
	// build the response even when validation fails.
	type validEntry struct {
		rawKey string
		key    storage.ObjectKey
		etag   *string
	}
	validEntries := make([]validEntry, 0, len(req.Objects))
	baseRequest, _ := makeAuthorizationRequest(ctx, authorization.OperationDeleteObjects, ptrutils.ToPtr(bucketName.String()), nil, r)

	for _, obj := range req.Objects {
		if obj.VersionId != nil {
			result.Errors = append(result.Errors, &DeleteErrorEntry{
				Key:     obj.Key,
				Code:    "NotImplemented",
				Message: "versioning is not supported",
			})
			continue
		}

		key, err := storage.NewObjectKey(obj.Key)
		if err != nil {
			result.Errors = append(result.Errors, &DeleteErrorEntry{
				Key:     obj.Key,
				Code:    "InvalidArgument",
				Message: err.Error(),
			})
			continue
		}

		validEntries = append(validEntries, validEntry{rawKey: obj.Key, key: key, etag: obj.ETag})
	}

	// Second pass: authorize valid entries and collect entries to bulk-delete.
	authorizedEntries := make([]validEntry, 0, len(validEntries))
	for _, ve := range validEntries {
		allowed, err := s.authorizeDeleteObjectEntry(ctx, baseRequest, ve.key.String())
		if err != nil {
			handleError(err, w, r)
			return
		}
		if !allowed {
			result.Errors = append(result.Errors, &DeleteErrorEntry{
				Key:     ve.rawKey,
				Code:    "AccessDenied",
				Message: "Access Denied",
			})
			continue
		}
		authorizedEntries = append(authorizedEntries, ve)
	}

	// Third pass: bulk delete all authorized entries in one storage call.
	if len(authorizedEntries) > 0 {
		inputEntries := make([]storage.DeleteObjectsInputEntry, len(authorizedEntries))
		for i, ve := range authorizedEntries {
			inputEntries[i] = storage.DeleteObjectsInputEntry{Key: ve.key, IfMatchETag: ve.etag}
		}

		slog.Info("DeleteObjects: deleting objects", "bucket", bucketName.String(), "count", len(inputEntries))
		deleteResult, err := s.storage.DeleteObjects(ctx, bucketName, inputEntries)
		if err != nil {
			if err == storage.ErrNoSuchBucket {
				handleError(err, w, r)
				return
			}
			handleError(err, w, r)
			return
		}

		// Build a map from key string → entry for result lookup.
		resultByKey := make(map[string]storage.DeleteObjectsEntry, len(deleteResult.Entries))
		for _, entry := range deleteResult.Entries {
			resultByKey[entry.Key.String()] = entry
		}

		for _, ve := range authorizedEntries {
			entry, ok := resultByKey[ve.key.String()]
			if !ok || entry.Deleted {
				if !req.Quiet {
					result.Deleted = append(result.Deleted, &DeletedEntry{Key: ve.rawKey})
				}
			} else {
				result.Errors = append(result.Errors, &DeleteErrorEntry{
					Key:     ve.rawKey,
					Code:    entry.ErrCode,
					Message: entry.ErrMsg,
				})
			}
		}
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(http.StatusOK)
	out, _ := xmlMarshalWithDocType(result)
	w.Write(out)
}

func (s *Server) getBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.getBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.Info("Getting bucket website configuration", "bucket", bucketName.String())
	config, err := s.storage.GetBucketWebsiteConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}

	response := WebsiteConfigurationResponse{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		IndexDocument: &WebsiteConfigurationIndexDocument{
			Suffix: config.IndexDocumentSuffix,
		},
	}
	if config.ErrorDocumentKey != nil {
		response.ErrorDocument = &WebsiteConfigurationErrorDocument{
			Key: *config.ErrorDocumentKey,
		}
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(response)
	w.Write(out)
}

func (s *Server) putBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.putBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}

	var request WebsiteConfigurationRequest
	err = xml.Unmarshal(data, &request)
	if err != nil {
		handleError(err, w, r)
		return
	}

	// Reject unsupported features
	if request.RedirectAllRequestsTo != nil {
		handleError(storage.ErrNotImplemented, w, r)
		return
	}
	if len(request.RoutingRules) > 0 {
		handleError(storage.ErrNotImplemented, w, r)
		return
	}

	// Validate IndexDocument is present
	if request.IndexDocument == nil || request.IndexDocument.Suffix == "" {
		handleError(fmt.Errorf("InvalidArgument"), w, r)
		return
	}

	config := &storage.WebsiteConfiguration{
		IndexDocumentSuffix: request.IndexDocument.Suffix,
	}
	if request.ErrorDocument != nil && request.ErrorDocument.Key != "" {
		config.ErrorDocumentKey = &request.ErrorDocument.Key
	}

	slog.Info("Putting bucket website configuration", "bucket", bucketName.String())
	err = s.storage.PutBucketWebsiteConfiguration(ctx, bucketName, config)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteBucketWebsiteHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.deleteBucketWebsiteHandler")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		handleError(err, w, r)
		return
	}

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucketWebsite, ptrutils.ToPtr(bucketName.String()), nil, w, r)
	if shouldReturn {
		return
	}

	slog.Info("Deleting bucket website configuration", "bucket", bucketName.String())
	err = s.storage.DeleteBucketWebsiteConfiguration(ctx, bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

// websiteResolveKey resolves the object key for a website request,
// appending the index document suffix when the key is empty or ends with "/".
func websiteResolveKey(keyStr string, websiteConfig *storage.WebsiteConfiguration) string {
	if keyStr == "" || strings.HasSuffix(keyStr, "/") {
		return keyStr + websiteConfig.IndexDocumentSuffix
	}
	return keyStr
}

// websitePrepare fetches the website configuration, resolves the request key,
// and authorizes the request. It writes an error response and returns false if
// any step fails; on success it returns the resolved config, object key, and
// resolved key string.
func (s *Server) websitePrepare(ctx context.Context, w http.ResponseWriter, r *http.Request, operation string, bucketName storage.BucketName) (websiteConfig *storage.WebsiteConfiguration, objectKey storage.ObjectKey, resolvedKey string, ok bool) {
	websiteConfig, configErr := s.storage.GetBucketWebsiteConfiguration(ctx, bucketName)

	// Resolve key and authorize with it when possible, fall back to nil key
	// when the config is unavailable (key resolution requires the index suffix).
	// Auth always runs before any existence information is revealed.
	var keyStr *string
	if configErr == nil {
		resolvedKey = websiteResolveKey(r.PathValue(keyPath), websiteConfig)
		if k, err := storage.NewObjectKey(resolvedKey); err == nil {
			objectKey = k
			s := k.String()
			keyStr = &s
		}
	}

	isAuthenticated, _ := ctx.Value(authentication.IsAuthenticatedContextKey{}).(bool)
	var accessKeyId *string
	if isAuthenticated {
		keyIdStr, _ := ctx.Value(authentication.AccessKeyIdContextKey{}).(string)
		accessKeyId = &keyIdStr
	}

	bucketStr := bucketName.String()
	authRequest := &authorization.Request{
		Operation:     operation,
		Authorization: authorization.Authorization{AccessKeyId: accessKeyId},
		Bucket:        &bucketStr,
		Key:           keyStr,
		HttpRequest:   makeAuthorizationHTTPRequest(r),
	}
	allowed, err := s.requestAuthorizer.AuthorizeRequest(ctx, authRequest)
	if err != nil {
		writePlainError(w, http.StatusInternalServerError)
		return nil, storage.ObjectKey{}, "", false
	}
	if !allowed {
		if !isAuthenticated {
			writePlainError(w, http.StatusUnauthorized)
		} else {
			writePlainError(w, http.StatusForbidden)
		}
		return nil, storage.ObjectKey{}, "", false
	}

	// Auth passed — now it is safe to reveal specific error codes.
	if configErr != nil {
		if configErr == storage.ErrNoSuchWebsiteConfiguration || configErr == storage.ErrNoSuchBucket {
			writePlainError(w, http.StatusNotFound)
		} else {
			writePlainError(w, http.StatusInternalServerError)
		}
		return nil, storage.ObjectKey{}, "", false
	}

	if keyStr == nil {
		s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
			fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
		return nil, storage.ObjectKey{}, "", false
	}

	return websiteConfig, objectKey, resolvedKey, true
}

func (s *Server) serveWebsiteGetObject(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.serveWebsiteGetObject")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		writePlainError(w, http.StatusBadRequest)
		return
	}

	websiteConfig, objectKey, resolvedKey, ok := s.websitePrepare(ctx, w, r, authorization.OperationGetObject, bucketName)
	if !ok {
		return
	}

	slog.Info("Website: getting object", "bucket", bucketName.String(), "key", resolvedKey)

	object, readers, err := s.storage.GetObject(ctx, bucketName, objectKey, nil, nil)
	if err != nil {
		if err == storage.ErrNoSuchKey {
			s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
				fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
			return
		}
		writePlainError(w, http.StatusInternalServerError)
		return
	}

	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
	}()

	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	responseHeaders.Set(etagHeader, fmt.Sprintf("\"%s\"", object.ETag))
	responseHeaders.Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(http.StatusOK)

	writer := ioutils.NewTracingWriter(ctx, s.tracer, "WebsiteGetObject", w)
	ioutils.CopyN(writer, readers[0], object.Size)
}

func (s *Server) serveWebsiteHeadObject(w http.ResponseWriter, r *http.Request) {
	ctx, span := s.tracer.Start(r.Context(), "Server.serveWebsiteHeadObject")
	defer span.End()

	bucketName, err := storage.NewBucketName(r.PathValue(bucketPath))
	if err != nil {
		writePlainError(w, http.StatusBadRequest)
		return
	}

	websiteConfig, objectKey, resolvedKey, ok := s.websitePrepare(ctx, w, r, authorization.OperationHeadObject, bucketName)
	if !ok {
		return
	}

	slog.Info("Website: head object", "bucket", bucketName.String(), "key", resolvedKey)

	object, err := s.storage.HeadObject(ctx, bucketName, objectKey, nil)
	if err != nil {
		if err == storage.ErrNoSuchKey {
			s.serveErrorDocument(w, r, bucketName, websiteConfig, http.StatusNotFound, "NoSuchKey",
				fmt.Sprintf("The specified key does not exist: %s", resolvedKey))
			return
		}
		writePlainError(w, http.StatusInternalServerError)
		return
	}

	responseHeaders := w.Header()
	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}
	responseHeaders.Set(contentTypeHeader, contentType)
	responseHeaders.Set(etagHeader, fmt.Sprintf("\"%s\"", object.ETag))
	gmtTimeLoc := time.FixedZone("GMT", 0)
	responseHeaders.Set(lastModifiedHeader, object.LastModified.In(gmtTimeLoc).Format(time.RFC1123))
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(http.StatusOK)
}

// serveErrorDocument tries to serve the configured error document for the bucket.
// If no error document is configured or the error document itself cannot be found,
// it falls back to a default HTML error page.
func (s *Server) serveErrorDocument(w http.ResponseWriter, r *http.Request,
	bucketName storage.BucketName, config *storage.WebsiteConfiguration,
	statusCode int, code string, message string) {

	if config.ErrorDocumentKey == nil {
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	ctx := r.Context()
	errorKey, err := storage.NewObjectKey(*config.ErrorDocumentKey)
	if err != nil {
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	object, readers, err := s.storage.GetObject(ctx, bucketName, errorKey, nil, nil)
	if err != nil {
		// Error document not found — fall back to default HTML error
		s.writeHTMLError(w, statusCode, code, message)
		return
	}

	defer func() {
		for _, reader := range readers {
			reader.Close()
		}
	}()

	contentType := "text/html"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}

	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, contentType)
	responseHeaders.Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(statusCode)

	if r.Method == http.MethodHead {
		return
	}

	io.Copy(w, readers[0])
}

// writePlainError writes a plain-text error response for the website endpoint.
// Using a body prevents browsers (notably Safari) from treating bodyless
// responses as file downloads.
func writePlainError(w http.ResponseWriter, statusCode int) {
	w.Header().Set(contentTypeHeader, "text/plain; charset=utf-8")
	body := fmt.Sprintf("%d %s\n", statusCode, http.StatusText(statusCode))
	w.Header().Set(contentLengthHeader, fmt.Sprintf("%d", len(body)))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

// writeHTMLError writes a simple HTML error response, similar to how AWS S3
// website hosting returns errors.
func (s *Server) writeHTMLError(w http.ResponseWriter, statusCode int, code string, message string) {
	w.Header().Set(contentTypeHeader, "text/html; charset=utf-8")
	w.WriteHeader(statusCode)
	body := fmt.Sprintf(`<html>
<head><title>%d %s</title></head>
<body>
<h1>%d %s</h1>
<ul>
<li>Code: %s</li>
<li>Message: %s</li>
</ul>
</body>
</html>`, statusCode, http.StatusText(statusCode), statusCode, http.StatusText(statusCode), html.EscapeString(code), html.EscapeString(message))
	w.Write([]byte(body))
}
