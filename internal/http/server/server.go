package server

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/metadatastore"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	storage "github.com/jdillenkofer/pithos/internal/storage"
)

type Server struct {
	requestAuthorizer authorization.RequestAuthorizer
	storage           storage.Storage
	tracer            trace.Tracer
}

func SetupServer(credentials []settings.Credentials, region string, baseEndpoint string, requestAuthorizer authorization.RequestAuthorizer, storage storage.Storage) http.Handler {
	server := &Server{
		requestAuthorizer: requestAuthorizer,
		storage:           storage,
		tracer:            otel.Tracer("internal/http/server"),
	}
	mux := http.NewServeMux()
	// @TODO(auth): list requests authorization does not filter out buckets and objects the user is not allowed to access
	mux.HandleFunc("GET /", server.listBucketsHandler)
	mux.HandleFunc("HEAD /{bucket}", server.headBucketHandler)
	mux.HandleFunc("GET /{bucket}", server.listObjectsOrListMultipartUploadsHandler)
	mux.HandleFunc("PUT /{bucket}", server.createBucketHandler)
	mux.HandleFunc("DELETE /{bucket}", server.deleteBucketHandler)
	mux.HandleFunc("HEAD /{bucket}/{key...}", server.headObjectHandler)
	mux.HandleFunc("GET /{bucket}/{key...}", server.getObjectOrListPartsHandler)
	mux.HandleFunc("POST /{bucket}/{key...}", server.createMultipartUploadOrCompleteMultipartUploadHandler)
	mux.HandleFunc("PUT /{bucket}/{key...}", server.uploadPartOrPutObjectHandler)
	mux.HandleFunc("DELETE /{bucket}/{key...}", server.abortMultipartUploadOrDeleteObjectHandler)
	var rootHandler http.Handler = mux
	rootHandler = middlewares.MakeVirtualHostBucketAddressingMiddleware(baseEndpoint, rootHandler)
	if credentials != nil {
		slog.Info("Authentication is enabled")
		authCredentials := sliceutils.Map(func(cred settings.Credentials) authentication.Credentials {
			return authentication.Credentials{
				AccessKeyId:     cred.AccessKeyId,
				SecretAccessKey: cred.SecretAccessKey,
			}
		}, credentials)
		rootHandler = authentication.MakeSignatureMiddleware(authCredentials, region, rootHandler)
	} else {
		slog.Warn("Authentication is disabled, this is not recommended for production use")
	}
	return rootHandler
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

const acceptRangesHeader = "Accept-Ranges"
const expectHeader = "Expect"
const contentRangeHeader = "Content-Range"
const contentLengthHeader = "Content-Length"
const rangeHeader = "Range"
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
	case storage.ErrEntityTooLarge:
		statusCode = 413
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
	accessKeyId, _ := ctx.Value(authentication.AccessKeyIdContextKey{}).(string)
	request := &authorization.Request{
		Operation: operation,
		Authorization: authorization.Authorization{
			AccessKeyId: accessKeyId,
		},
		Bucket: bucket,
		Key:    key,
	}
	authorized, err := s.requestAuthorizer.AuthorizeRequest(ctx, request)
	if err != nil {
		slog.Error(fmt.Sprintf("Authorization error: %v", err))
		handleError(err, w, r)
		return true
	}
	if !authorized {
		slog.Debug(fmt.Sprintf("Unauthorized request: %v", request))
		w.WriteHeader(403)
		return true
	}
	return false
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
	responseHeaders := w.Header()
	responseHeaders.Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	listAllMyBucketsResult := ListAllMyBucketsResult{
		Buckets: []*BucketResult{},
	}
	for _, bucket := range buckets {
		listAllMyBucketsResult.Buckets = append(listAllMyBucketsResult.Buckets, &BucketResult{
			Name:         bucket.Name.String(),
			CreationDate: bucket.CreationDate.UTC().Format(time.RFC3339),
		})
	}
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

func (s *Server) listObjectsOrListMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
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

	slog.Info("Listing MultipartUploads")
	result, err := s.storage.ListMultipartUploads(ctx, bucketName, storage.ListMultipartUploadsOptions{
		Prefix:         prefix,
		Delimiter:      delimiter,
		KeyMarker:      keyMarker,
		UploadIdMarker: uploadIdMarker,
		MaxUploads:     maxUploadsI32,
	})
	if err != nil {
		handleError(err, w, r)
		return
	}
	listMultipartUploadsResult := ListMultipartUploadsResult{
		Bucket:             result.BucketName.String(),
		KeyMarker:          ptrutils.ToPtr(result.KeyMarker),
		UploadIdMarker:     ptrutils.ToPtr(result.UploadIdMarker),
		NextKeyMarker:      ptrutils.ToPtr(result.NextKeyMarker),
		NextUploadIdMarker: ptrutils.ToPtr(result.NextUploadIdMarker),
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

	// In ListObjects V1, use marker as startAfter if provided
	if marker != nil {
		startAfter = marker
	}

	maxKeys := query.Get(maxKeysQuery)
	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	slog.Info("Listing objects", "bucket", bucketName.String())
	result, err := s.storage.ListObjects(ctx, bucketName, storage.ListObjectsOptions{
		Prefix:     prefix,
		Delimiter:  delimiter,
		StartAfter: startAfter,
		MaxKeys:    maxKeysI32,
	})
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
	if result.IsTruncated && len(result.Objects) > 0 {
		// Use the last object's key as the next marker
		listBucketResult.NextMarker = ptrutils.ToPtr(result.Objects[len(result.Objects)-1].Key.String())
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

	slog.Info("Listing objects V2", "bucket", bucketName.String())
	result, err := s.storage.ListObjects(ctx, bucketName, storage.ListObjectsOptions{
		Prefix:     prefix,
		Delimiter:  delimiter,
		StartAfter: startAfter,
		MaxKeys:    maxKeysI32,
	})
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

	if result.IsTruncated && len(result.Objects) > 0 {
		// Use the last object's key as the next continuation token
		listBucketV2Result.NextContinuationToken = ptrutils.ToPtr(result.Objects[len(result.Objects)-1].Key.String())
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
	object, err := s.storage.HeadObject(ctx, bucketName, key)
	if err != nil {
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
		// Normal range with explicit end (exclusive, so subtract 1 for inclusive)
		end = *br.End - 1
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

	result, err := s.storage.ListParts(ctx, bucketName, key, uploadId, storage.ListPartsOptions{
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
		NextPartNumberMarker: result.NextPartNumberMarker,
		MaxParts:             result.MaxParts,
		IsTruncated:          result.IsTruncated,
		Parts: sliceutils.Map(func(part *storage.Part) *PartResult {
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

	// GetObject now returns metadata and readers in a single transaction
	// It also validates the ranges and returns ErrInvalidRange if invalid
	object, readers, err := s.storage.GetObject(ctx, bucketName, key, storageRanges)
	if err != nil {
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
				var end int64 = object.Size
				if byteRange.End != nil {
					end = *byteRange.End
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
	result, err := s.storage.CompleteMultipartUpload(ctx, bucketName, key, uploadId, checksumInput)
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
	putObjectResult, err := s.storage.PutObject(ctx, bucketName, key, contentType, r.Body, checksumInput)
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

func (s *Server) uploadPartOrPutObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// UploadPart
	if query.Has(uploadIdQuery) || query.Has(partNumberQuery) {
		s.uploadPartHandler(w, r)
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
	err = s.storage.DeleteObject(ctx, bucketName, key)
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
