package server

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/middlewares"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
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
}

func SetupServer(credentials []settings.Credentials, region string, baseEndpoint string, requestAuthorizer authorization.RequestAuthorizer, storage storage.Storage) http.Handler {
	server := &Server{
		requestAuthorizer: requestAuthorizer,
		storage:           storage,
	}
	mux := http.NewServeMux()
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
		authCredentials := sliceutils.Map(func(cred settings.Credentials) authentication.Credentials {
			return authentication.Credentials{
				AccessKeyId:     cred.AccessKeyId,
				SecretAccessKey: cred.SecretAccessKey,
			}
		}, credentials)
		rootHandler = authentication.MakeSignatureMiddleware(authCredentials, region, rootHandler)
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
const startAfterQuery = "startAfter"
const maxKeysQuery = "max-keys"
const uploadIdQuery = "uploadId"
const uploadsQuery = "uploads"
const partNumberQuery = "partNumber"
const keyMarkerQuery = "key-marker"
const uploadIdMarkerQuery = "upload-id-marker"
const partNumberMarkerQuery = "part-number-marker"
const maxUploadsQuery = "max-uploads"
const maxPartsQuery = "max-parts"

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
	Prefix         string                `xml:"Prefix"`
	Delimiter      string                `xml:"Delimiter"`
	MaxKeys        int32                 `xml:"MaxKeys"`
	CommonPrefixes []*CommonPrefixResult `xml:"CommonPrefixes"`
	KeyCount       int32                 `xml:"KeyCount"`
	StartAfter     string                `xml:"StartAfter"`
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
	KeyMarker          string                `xml:"KeyMarker"`
	UploadIdMarker     string                `xml:"UploadIdMarker"`
	NextKeyMarker      string                `xml:"NextKeyMarker"`
	NextUploadIdMarker string                `xml:"NextUploadIdMarker"`
	MaxUploads         int32                 `xml:"MaxUploads"`
	IsTruncated        bool                  `xml:"IsTruncated"`
	Delimiter          string                `xml:"Delimiter"`
	Prefix             string                `xml:"Prefix"`
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
	PartNumberMarker     string        `xml:"PartNumberMarker"`
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
		log.Printf("Error during handleError: %v", err)
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

func setChecksumType(headers http.Header, checksumType string) {
	headers.Set(checksumTypeHeader, checksumType)
}

func setChecksumHeadersFromObject(headers http.Header, object *storage.Object) {
	if object.ChecksumType != nil {
		setChecksumType(headers, *object.ChecksumType)
	}
	headers.Set(etagHeader, object.ETag)
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

func extractChecksumInput(r *http.Request) *storage.ChecksumInput {
	checksumType := getHeaderAsPtr(r.Header, checksumTypeHeader)
	checksumAlgorithm := getHeaderAsPtr(r.Header, checksumAlgorithmHeader)
	contentMd5 := getHeaderAsPtr(r.Header, contentMd5Header)
	checksumCrc32 := getHeaderAsPtr(r.Header, checksumCRC32Header)
	checksumCrc32c := getHeaderAsPtr(r.Header, checksumCRC32CHeader)
	checksumCrc64Nvme := getHeaderAsPtr(r.Header, checksumCRC64NVMEHeader)
	checksumSha1 := getHeaderAsPtr(r.Header, checksumSHA1Header)
	checksumSha256 := getHeaderAsPtr(r.Header, checksumSHA256Header)

	checksumInput := storage.ChecksumInput{
		ChecksumType:      checksumType,
		ChecksumAlgorithm: checksumAlgorithm,
		ETag:              contentMd5,
		ChecksumCRC32:     checksumCrc32,
		ChecksumCRC32C:    checksumCrc32c,
		ChecksumCRC64NVME: checksumCrc64Nvme,
		ChecksumSHA1:      checksumSha1,
		ChecksumSHA256:    checksumSha256,
	}
	return &checksumInput
}

func handleError(err error, w http.ResponseWriter, r *http.Request) {
	statusCode := 500
	errResponse := ErrorResponse{}
	errResponse.Code = err.Error()
	errResponse.Message = err.Error()
	errResponse.Resource = r.URL.Path
	switch err {
	case storage.ErrNoSuchBucket:
		statusCode = 404
	case storage.ErrBucketAlreadyExists:
		statusCode = 409
	case storage.ErrBucketNotEmpty:
		statusCode = 409
	case storage.ErrNoSuchKey:
		statusCode = 404
	case storage.ErrBadDigest:
		statusCode = 400
	default:
		statusCode = 500
		errResponse.Code = "InternalError"
		errResponse.Message = "InternalError"
	}
	xmlErrorResponse, err := xmlMarshalWithDocType(errResponse)
	if err != nil {
		log.Printf("Error during handleError: %v", err)
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
	authorized, err := s.requestAuthorizer.AuthorizeRequest(request)
	if err != nil {
		log.Printf("Authorization error: %v", err)
		handleError(err, w, r)
		return true
	}
	if !authorized {
		log.Printf("Unauthorized request: %v", request)
		w.WriteHeader(403)
		return true
	}
	return false
}

func (s *Server) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.listBucketsHandler()")
	defer task.End()
	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListBuckets, nil, nil, w, r)
	if shouldReturn {
		return
	}
	log.Println("Listing Buckets")
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	listAllMyBucketsResult := ListAllMyBucketsResult{
		Buckets: []*BucketResult{},
	}
	for _, bucket := range buckets {
		listAllMyBucketsResult.Buckets = append(listAllMyBucketsResult.Buckets, &BucketResult{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate.UTC().Format(time.RFC3339),
		})
	}
	out, _ := xmlMarshalWithDocType(listAllMyBucketsResult)
	w.Write(out)
}

func (s *Server) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.headBucketHandler()")
	defer task.End()
	bucketName := r.PathValue(bucketPath)
	shouldReturn := s.authorizeRequest(ctx, authorization.OperationHeadBucket, ptrutils.ToPtr(bucketName), nil, w, r)
	if shouldReturn {
		return
	}
	log.Printf("Head bucket %s\n", bucketName)
	_, err := s.storage.HeadBucket(ctx, bucketName)
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
	s.listObjectsHandler(w, r)
}

func (s *Server) listMultipartUploadsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.listMultipartUploadsHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListMultipartUploads, ptrutils.ToPtr(bucket), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	prefix := query.Get(prefixQuery)
	delimiter := query.Get(delimiterQuery)
	keyMarker := query.Get(keyMarkerQuery)
	uploadIdMarker := query.Get(uploadIdMarkerQuery)
	maxUploads := query.Get(maxUploadsQuery)
	maxUploadsI64, err := strconv.ParseInt(maxUploads, 10, 32)
	if err != nil || maxUploadsI64 < 0 {
		maxUploadsI64 = 1000
	}
	maxUploadsI32 := int32(maxUploadsI64)

	log.Println("Listing MultipartUploads")
	result, err := s.storage.ListMultipartUploads(ctx, bucket, prefix, delimiter, keyMarker, uploadIdMarker, maxUploadsI32)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listMultipartUploadsResult := ListMultipartUploadsResult{
		Bucket:             result.Bucket,
		KeyMarker:          result.KeyMarker,
		UploadIdMarker:     result.UploadIdMarker,
		NextKeyMarker:      result.NextKeyMarker,
		NextUploadIdMarker: result.NextUploadIdMarker,
		MaxUploads:         maxUploadsI32,
		IsTruncated:        result.IsTruncated,
		Delimiter:          result.Delimiter,
		Prefix:             result.Prefix,
		Uploads:            []*UploadResult{},
		CommonPrefixes:     []*CommonPrefixResult{},
	}

	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, upload := range result.Uploads {
		listMultipartUploadsResult.Uploads = append(listMultipartUploadsResult.Uploads, &UploadResult{
			Key:          upload.Key,
			UploadId:     upload.UploadId,
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
	ctx, task := trace.NewTask(r.Context(), "Server.listObjectsHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListObjects, ptrutils.ToPtr(bucket), nil, w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	prefix := query.Get(prefixQuery)
	delimiter := query.Get(delimiterQuery)
	startAfter := query.Get(startAfterQuery)
	maxKeys := query.Get(maxKeysQuery)
	maxKeysI64, err := strconv.ParseInt(maxKeys, 10, 32)
	if err != nil || maxKeysI64 < 0 {
		maxKeysI64 = 1000
	}
	maxKeysI32 := int32(maxKeysI64)

	log.Printf("Listing objects in bucket %s\n", bucket)
	result, err := s.storage.ListObjects(ctx, bucket, prefix, delimiter, startAfter, maxKeysI32)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listBucketResult := ListBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		StartAfter:     startAfter,
		KeyCount:       int32(len(result.Objects)),
		MaxKeys:        maxKeysI32,
		CommonPrefixes: []*CommonPrefixResult{},
		IsTruncated:    result.IsTruncated,
		Contents:       []*ContentResult{},
	}

	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, object := range result.Objects {
		listBucketResult.Contents = append(listBucketResult.Contents, &ContentResult{
			Key:          object.Key,
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

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.createBucketHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCreateBucket, ptrutils.ToPtr(bucket), nil, w, r)
	if shouldReturn {
		return
	}

	log.Printf("Creating bucket %s\n", bucket)
	err := s.storage.CreateBucket(ctx, bucket)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Set(locationHeader, bucket)
	w.WriteHeader(200)
}

func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.deleteBucketHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteBucket, ptrutils.ToPtr(bucket), nil, w, r)
	if shouldReturn {
		return
	}

	log.Printf("Deleting bucket %s\n", bucket)
	err := s.storage.DeleteBucket(ctx, bucket)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.headObjectHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationHeadObject, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	log.Printf("Head object with key %s in bucket %s\n", key, bucket)
	object, err := s.storage.HeadObject(ctx, bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	setChecksumHeadersFromObject(w.Header(), object)

	gmtTimeLoc := time.FixedZone("GMT", 0)
	w.Header().Set(lastModifiedHeader, object.LastModified.In(gmtTimeLoc).Format(time.RFC1123))
	w.Header().Set(contentLengthHeader, fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
}

type byteRange struct {
	start *int64
	end   *int64
}

func (br byteRange) generateContentRangeValue(length int64) string {
	start := int64(0)
	if br.start != nil {
		start = *br.start
	}
	end := length - 1
	if br.end != nil {
		end = *br.end
	}
	contentRangeValue := fmt.Sprintf("bytes %d-%d/%d", start, end, length)
	return contentRangeValue
}

var errInvalidByteRange error = fmt.Errorf("invalid byte range")

func parseAndValidateRangeHeader(rangeHeader string, object *storage.Object) ([]byteRange, error) {
	byteRanges := []byteRange{}
	if rangeHeader != "" {
		rangeUnitAndRangesSplit := strings.SplitN(rangeHeader, "=", 2)
		if len(rangeUnitAndRangesSplit) != 2 || len(rangeUnitAndRangesSplit) > 0 && rangeUnitAndRangesSplit[0] != "bytes" {
			return nil, errInvalidByteRange
		}
		ranges := rangeUnitAndRangesSplit[1]
		rangesSplit := strings.Split(ranges, ",")
		for _, rangeVal := range rangesSplit {
			var start *int64 = nil
			var end *int64 = nil

			rangeVal = strings.TrimSpace(rangeVal)
			byteSplit := strings.SplitN(rangeVal, "-", 2)
			if len(byteSplit) == 2 {
				startByte, err := strconv.ParseInt(byteSplit[0], 10, 64)
				if err == nil {
					start = &startByte
				}
				endByte, err := strconv.ParseInt(byteSplit[1], 10, 64)
				if err == nil {
					end = &endByte
				}

				// Translate suffix range to int range
				if start == nil && end != nil {
					startByte = object.Size - *end
					start = &startByte
					endByte = object.Size - 1
					end = &endByte
				}
			}

			if start != nil && *start < 0 {
				return nil, errInvalidByteRange
			}
			if end != nil && *end >= object.Size {
				return nil, errInvalidByteRange
			}

			byteRangeVal := byteRange{
				start: start,
				end:   end,
			}
			byteRanges = append(byteRanges, byteRangeVal)
		}
	}
	return byteRanges, nil
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
	ctx, task := trace.NewTask(r.Context(), "Server.listPartsHandler()")
	defer task.End()

	query := r.URL.Query()

	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationListParts, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	uploadId := query.Get(uploadIdQuery)
	partNumberMarker := query.Get(partNumberMarkerQuery)
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

	result, err := s.storage.ListParts(ctx, bucket, key, uploadId, partNumberMarker, maxPartsI32)
	if err != nil {
		handleError(err, w, r)
		return
	}

	listPartsResult := ListPartsResult{
		Bucket:               result.Bucket,
		Key:                  result.Key,
		UploadId:             result.UploadId,
		PartNumberMarker:     result.PartNumberMarker,
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
	ctx, task := trace.NewTask(r.Context(), "Server.getObjectHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationGetObject, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	rangeHeaderValue := r.Header.Get(rangeHeader)
	log.Printf("Getting object with key %s from bucket %s\n", key, bucket)
	// @Concurrency: headObject and getObject run in different transactions and possibly return inconsistent data
	object, err := s.storage.HeadObject(ctx, bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}

	byteRanges, err := parseAndValidateRangeHeader(rangeHeaderValue, object)
	if err != nil {
		w.WriteHeader(416)
		return
	}

	contentType := "application/octet-stream"
	if object.ContentType != nil {
		contentType = *object.ContentType
	}
	w.Header().Set(contentTypeHeader, contentType)
	setChecksumHeadersFromObject(w.Header(), object)

	var readers []io.ReadCloser
	var sizes []int64
	var totalSize int64 = 0
	if len(byteRanges) > 0 {
		for _, byteRange := range byteRanges {
			var end int64 = object.Size
			if byteRange.end != nil {
				// convert to exclusive end indexing
				end = *byteRange.end + 1
			}
			if byteRange.start != nil {
				size := end - *byteRange.start
				sizes = append(sizes, size)
				totalSize += size
			}
			rangeReader, err := s.storage.GetObject(ctx, bucket, key, byteRange.start, &end)
			if err != nil {
				for _, rangeReader := range readers {
					rangeReader.Close()
				}
				handleError(err, w, r)
				return
			}
			readers = append(readers, rangeReader)
		}
	} else {
		reader, err := s.storage.GetObject(ctx, bucket, key, nil, nil)
		if err != nil {
			handleError(err, w, r)
			return
		}
		readers = append(readers, reader)
		size := object.Size
		sizes = append(sizes, size)
		totalSize = size
	}

	defer (func() {
		for _, reader := range readers {
			reader.Close()
		}
	})()
	w.Header().Set(lastModifiedHeader, object.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Set(acceptRangesHeader, "bytes")
	if len(byteRanges) > 1 {
		separator := ulid.Make().String()
		rangeHeaderLength := int64(0)
		rangeHeaders := []string{}
		for idx := range readers {
			byteRangeEntry := byteRanges[idx]
			contentRangeValue := byteRangeEntry.generateContentRangeValue(object.Size)
			rangeHeader := fmt.Sprintf("%s: %s\r\n\r\n", contentRangeHeader, contentRangeValue)
			rangeHeaders = append(rangeHeaders, rangeHeader)
			rangeHeaderLength += int64(len(rangeHeader))
		}
		separatorLength := int64(len(separator))
		byteRangesCount := int64(len(byteRanges))
		startCrlfLength := (byteRangesCount - 1) * 2 /* \r\n */
		separatorLineLength := (2 /* -- */ + 2 /* \r\n */ + separatorLength)
		endSeparatorLineLength := separatorLineLength + 2 /* \r\n */ + 2 /* -- at the end */
		totalSize = totalSize + startCrlfLength + rangeHeaderLength + separatorLineLength*byteRangesCount + endSeparatorLineLength
		w.Header().Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		w.Header().Set(contentTypeHeader, fmt.Sprintf("multipart/byteranges; boundary=%v", separator))
		w.WriteHeader(206)
		for idx := range readers {
			if idx > 0 {
				io.WriteString(w, "\r\n")
			}
			io.WriteString(w, fmt.Sprintf("--%s\r\n", separator))

			io.WriteString(w, rangeHeaders[idx])

			io.CopyN(w, readers[idx], sizes[idx])
		}
		io.WriteString(w, fmt.Sprintf("\r\n--%s--\r\n", separator))
	} else if len(byteRanges) == 1 {
		w.Header().Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		firstRangeEntry := byteRanges[0]
		contentRangeValue := firstRangeEntry.generateContentRangeValue(object.Size)
		w.Header().Set(contentRangeHeader, contentRangeValue)
		w.WriteHeader(206)
		io.CopyN(w, readers[0], totalSize)
	} else {
		w.Header().Set(contentLengthHeader, fmt.Sprintf("%v", totalSize))
		w.WriteHeader(200)
		io.CopyN(w, readers[0], totalSize)
	}
}

func (s *Server) createMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.createMultipartUploadHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCreateMultipartUpload, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	contentType := getHeaderAsPtr(r.Header, contentTypeHeader)
	checksumType := getHeaderAsPtr(r.Header, checksumTypeHeader)
	log.Printf("CreateMultipartUpload with key %s to bucket %s\n", key, bucket)
	result, err := s.storage.CreateMultipartUpload(ctx, bucket, key, contentType, checksumType)
	if err != nil {
		handleError(err, w, r)
		return
	}

	initiateMultipartUploadResult := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: result.UploadId,
	}

	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(initiateMultipartUploadResult)
	w.Write(out)
}

func (s *Server) completeMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.completeMultipartUploadHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationCompleteMultipartUpload, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId := query.Get(uploadIdQuery)
	checksumInput := extractChecksumInput(r)

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

	log.Printf("CompleteMultipartUpload with key %s and uploadId %s to bucket %s\n", key, uploadId, bucket)
	result, err := s.storage.CompleteMultipartUpload(ctx, bucket, key, uploadId, checksumInput)
	if err != nil {
		handleError(err, w, r)
		return
	}

	completeMultipartUploadResult := CompleteMultipartUploadResult{
		Location:          result.Location,
		Bucket:            bucket,
		Key:               key,
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

func (s *Server) uploadPartHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.uploadPartHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationUploadPart, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId := query.Get(uploadIdQuery)
	partNumber := query.Get(partNumberQuery)
	checksumInput := extractChecksumInput(r)

	log.Printf("UploadPart with key %s to bucket %s (uploadId %s, partNumber %s)\n", key, bucket, uploadId, partNumber)
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
	uploadPartResult, err := s.storage.UploadPart(ctx, bucket, key, uploadId, partNumberI32, r.Body, checksumInput)
	if err != nil {
		handleError(err, w, r)
		return
	}
	setChecksumHeadersFromChecksumValues(w.Header(), storage.ChecksumValues{
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
	ctx, task := trace.NewTask(r.Context(), "Server.putObjectHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationPutObject, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	contentType := getHeaderAsPtr(r.Header, contentTypeHeader)

	checksumInput := extractChecksumInput(r)

	log.Printf("Putting object with key %s to bucket %s\n", key, bucket)
	if r.Header.Get(expectHeader) == "100-continue" {
		w.WriteHeader(100)
	}
	putObjectResult, err := s.storage.PutObject(ctx, bucket, key, contentType, r.Body, checksumInput)
	if err != nil {
		handleError(err, w, r)
		return
	}

	setChecksumHeadersFromChecksumValues(w.Header(), storage.ChecksumValues{
		ETag:              putObjectResult.ETag,
		ChecksumCRC32:     putObjectResult.ChecksumCRC32,
		ChecksumCRC32C:    putObjectResult.ChecksumCRC32C,
		ChecksumCRC64NVME: putObjectResult.ChecksumCRC64NVME,
		ChecksumSHA1:      putObjectResult.ChecksumSHA1,
		ChecksumSHA256:    putObjectResult.ChecksumSHA256,
	})
	setChecksumType(w.Header(), metadatastore.ChecksumTypeFullObject)
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
	ctx, task := trace.NewTask(r.Context(), "Server.abortMultipartUploadHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationAbortMultipartUpload, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	query := r.URL.Query()
	uploadId := query.Get(uploadIdQuery)
	log.Printf("AbortMultipartUpload with key %s and uploadId %s to bucket %s\n", key, uploadId, bucket)
	err := s.storage.AbortMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.deleteObjectHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)

	shouldReturn := s.authorizeRequest(ctx, authorization.OperationDeleteObject, ptrutils.ToPtr(bucket), ptrutils.ToPtr(key), w, r)
	if shouldReturn {
		return
	}

	log.Printf("Deleting object with key %s from bucket %s\n", key, bucket)
	err := s.storage.DeleteObject(ctx, bucket, key)
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
