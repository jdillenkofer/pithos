package server

import (
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
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	storage "github.com/jdillenkofer/pithos/internal/storage"
)

type Server struct {
	storage storage.Storage
}

func SetupServer(accessKeyId string, secretAccessKey string, region string, baseEndpoint string, storage storage.Storage) http.Handler {
	server := &Server{
		storage: storage,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", server.listBucketsHandler)
	mux.HandleFunc("HEAD /{bucket}", server.headBucketHandler)
	mux.HandleFunc("GET /{bucket}", server.listObjectsOrListMultipartUploadsHandler)
	mux.HandleFunc("PUT /{bucket}", server.createBucketHandler)
	mux.HandleFunc("DELETE /{bucket}", server.deleteBucketHandler)
	mux.HandleFunc("HEAD /{bucket}/{key...}", server.headObjectHandler)
	mux.HandleFunc("GET /{bucket}/{key...}", server.getObjectHandler)
	mux.HandleFunc("POST /{bucket}/{key...}", server.createMultipartUploadOrCompleteMultipartUploadHandler)
	mux.HandleFunc("PUT /{bucket}/{key...}", server.putObjectHandler)
	mux.HandleFunc("DELETE /{bucket}/{key...}", server.deleteObjectHandler)
	var rootHandler http.Handler = mux
	rootHandler = middlewares.MakeVirtualHostBucketAddressingMiddleware(baseEndpoint, rootHandler)
	if accessKeyId != "" && secretAccessKey != "" {
		rootHandler = middlewares.MakeSignatureMiddleware(accessKeyId, secretAccessKey, region, rootHandler)
	}
	return rootHandler
}

func SetupMonitoringServer(dbs []database.Database) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
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
	})
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
const maxUploadsQuery = "max-uploads"

const acceptRangesHeader = "Accept-Ranges"
const expectHeader = "Expect"
const contentRangeHeader = "Content-Range"
const contentLengthHeader = "Content-Length"
const rangeHeader = "Range"
const etagHeader = "ETag"
const lastModifiedHeader = "Last-Modified"
const contentTypeHeader = "Content-Type"
const locationHeader = "Location"

const applicationXmlContentType = "application/xml"

type Bucket struct {
	XMLName      xml.Name `xml:"Bucket"`
	CreationDate string   `xml:"CreationDate"`
	Name         string   `xml:"Name"`
}

type Owner struct {
	XMLName     xml.Name `xml:"Owner"`
	DisplayName string   `xml:"DisplayName"`
	Id          string   `xml:"ID"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name  `xml:"ListAllMyBucketsResult"`
	Buckets []*Bucket `xml:">Buckets"`
	Owner   *Owner    `xml:"Owner"`
}

type Prefix struct {
	XMLName xml.Name `xml:"Prefix"`
}

type Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

type ListBucketResult struct {
	XMLName        xml.Name          `xml:"ListBucketResult"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []*Content        `xml:"Contents"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Delimiter      string            `xml:"Delimiter"`
	MaxKeys        int32             `xml:"MaxKeys"`
	CommonPrefixes []*CommonPrefixes `xml:"CommonPrefixes"`
	KeyCount       int32             `xml:"KeyCount"`
	StartAfter     string            `xml:"StartAfter"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadId string   `xml:"UploadId"`
}

type CompleteMultipartUploadResult struct {
	XMLName        xml.Name `xml:"CompleteMultipartUploadResult"`
	Location       string   `xml:"Location"`
	Bucket         string   `xml:"Bucket"`
	Key            string   `xml:"Key"`
	ETag           string   `xml:"ETag"`
	ChecksumCRC32  string   `xml:"ChecksumCRC32"`
	ChecksumCRC32C string   `xml:"ChecksumCRC32C"`
	ChecksumSHA1   string   `xml:"ChecksumSHA1"`
	ChecksumSHA256 string   `xml:"ChecksumSHA256"`
}

type Upload struct {
	Key          string `xml:"Key"`
	UploadId     string `xml:"UploadId"`
	Initiated    string `xml:"Initiated"`
	StorageClass string `xml:"StorageClass"`
}

type ListMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIdMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker"`
	NextUploadIdMarker string            `xml:"NextUploadIdMarker"`
	MaxUploads         int32             `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Delimiter          string            `xml:"Delimiter"`
	Prefix             string            `xml:"Prefix"`
	Uploads            []*Upload         `xml:"Upload"`
	CommonPrefixes     []*CommonPrefixes `xml:"CommonPrefixes"`
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

func (s *Server) listBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.listBucketsHandler()")
	defer task.End()
	log.Println("Listing Buckets")
	buckets, err := s.storage.ListBuckets(ctx)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	listAllMyBucketsResult := ListAllMyBucketsResult{
		Buckets: []*Bucket{},
		Owner: &Owner{
			DisplayName: "own-display-name",
			Id:          "examplee7a2f25102679df27bb0ae12b3f85be6f290b936c4393484be31",
		},
	}
	for _, bucket := range buckets {
		listAllMyBucketsResult.Buckets = append(listAllMyBucketsResult.Buckets, &Bucket{
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
	listMultipartUploadResult := ListMultipartUploadsResult{
		Bucket:             result.Bucket,
		KeyMarker:          result.KeyMarker,
		UploadIdMarker:     result.UploadIdMarker,
		NextKeyMarker:      result.NextKeyMarker,
		NextUploadIdMarker: result.NextUploadIdMarker,
		MaxUploads:         maxUploadsI32,
		IsTruncated:        result.IsTruncated,
		Delimiter:          result.Delimiter,
		Prefix:             result.Prefix,
		Uploads:            []*Upload{},
		CommonPrefixes:     []*CommonPrefixes{},
	}

	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, upload := range result.Uploads {
		listMultipartUploadResult.Uploads = append(listMultipartUploadResult.Uploads, &Upload{
			Key:          upload.Key,
			UploadId:     upload.UploadId,
			Initiated:    upload.Initiated.UTC().Format(time.RFC3339),
			StorageClass: "STANDARD",
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listMultipartUploadResult.CommonPrefixes = append(listMultipartUploadResult.CommonPrefixes, &CommonPrefixes{Prefix: commonPrefix})
	}
	out, _ := xmlMarshalWithDocType(listMultipartUploadResult)
	w.Write(out)
}

func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.listObjectsHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
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
		CommonPrefixes: []*CommonPrefixes{},
		IsTruncated:    result.IsTruncated,
		Contents:       []*Content{},
	}

	w.Header().Set(contentTypeHeader, applicationXmlContentType)
	w.WriteHeader(200)
	for _, object := range result.Objects {
		listBucketResult.Contents = append(listBucketResult.Contents, &Content{
			Key:          object.Key,
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, commonPrefix := range result.CommonPrefixes {
		listBucketResult.CommonPrefixes = append(listBucketResult.CommonPrefixes, &CommonPrefixes{Prefix: commonPrefix})
	}
	out, _ := xmlMarshalWithDocType(listBucketResult)
	w.Write(out)
}

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.createBucketHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
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
	log.Printf("Head object with key %s in bucket %s\n", key, bucket)
	object, err := s.storage.HeadObject(ctx, bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	gmtTimeLoc := time.FixedZone("GMT", 0)
	w.Header().Set(etagHeader, object.ETag)
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

func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.getObjectHandler()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	rangeHeaderValue := r.Header.Get(rangeHeader)
	log.Printf("Getting object with key %s from bucket %s\n", key, bucket)
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
	if object.ContentType != "" {
		contentType = object.ContentType
	}
	w.Header().Set(contentTypeHeader, contentType)

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

func (s *Server) createMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.createMultipartUpload()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	contentType := r.Header.Get(contentTypeHeader)
	log.Printf("CreateMultipartUpload with key %s to bucket %s\n", key, bucket)
	result, err := s.storage.CreateMultipartUpload(ctx, bucket, key, contentType)
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

func (s *Server) completeMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.completeMultipartUpload()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	query := r.URL.Query()
	uploadId := query.Get(uploadIdQuery)
	log.Printf("CompleteMultipartUpload with key %s and uploadId %s to bucket %s\n", key, uploadId, bucket)
	result, err := s.storage.CompleteMultipartUpload(ctx, bucket, key, uploadId)
	if err != nil {
		handleError(err, w, r)
		return
	}

	completeMultipartUploadResult := CompleteMultipartUploadResult{
		Location:       result.Location,
		Bucket:         bucket,
		Key:            key,
		ETag:           result.ETag,
		ChecksumCRC32:  result.ChecksumCRC32,
		ChecksumCRC32C: result.ChecksumCRC32C,
		ChecksumSHA1:   result.ChecksumSHA1,
		ChecksumSHA256: result.ChecksumSHA256,
	}

	w.WriteHeader(200)
	out, _ := xmlMarshalWithDocType(completeMultipartUploadResult)
	w.Write(out)
}

func (s *Server) createMultipartUploadOrCompleteMultipartUploadHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// CreateMultipartUpload
	if query.Has(uploadsQuery) {
		s.createMultipartUpload(w, r)
		return
	}

	// CompleteMultipartUpload
	if query.Has(uploadIdQuery) {
		s.completeMultipartUpload(w, r)
		return
	}

	w.WriteHeader(404)
}

func (s *Server) uploadPart(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.uploadPart()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	query := r.URL.Query()
	uploadId := query.Get(uploadIdQuery)
	partNumber := query.Get(partNumberQuery)
	log.Printf("UploadPart with key %s to bucket %s (uploadId %s, partNumber %s)\n", key, bucket, uploadId, partNumber)
	if !query.Has(uploadIdQuery) || !query.Has(partNumberQuery) {
		w.WriteHeader(400)
		return
	}
	partNumberUint64, err := strconv.ParseInt(partNumber, 10, 16)
	if err != nil {
		handleError(err, w, r)
		return
	}
	partNumberInt32 := int32(partNumberUint64)
	if partNumberInt32 < 1 || partNumberInt32 > 10000 {
		w.WriteHeader(400)
		return
	}
	uploadPartResult, err := s.storage.UploadPart(ctx, bucket, key, uploadId, partNumberInt32, r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Set(etagHeader, uploadPartResult.ETag)
	w.WriteHeader(200)
}

func (s *Server) putObject(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.putObject()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	contentType := r.Header.Get(contentTypeHeader)

	log.Printf("Putting object with key %s to bucket %s\n", key, bucket)
	if r.Header.Get(expectHeader) == "100-continue" {
		w.WriteHeader(100)
	}
	err := s.storage.PutObject(ctx, bucket, key, contentType, r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)

}

func (s *Server) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// UploadPart
	if query.Has(uploadIdQuery) || query.Has(partNumberQuery) {
		s.uploadPart(w, r)
		return
	}

	// PutObject
	s.putObject(w, r)
}

func (s *Server) abortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.abortMultipartUpload()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
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

func (s *Server) deleteObject(w http.ResponseWriter, r *http.Request) {
	ctx, task := trace.NewTask(r.Context(), "Server.deleteObject()")
	defer task.End()
	bucket := r.PathValue(bucketPath)
	key := r.PathValue(keyPath)
	log.Printf("Deleting object with key %s from bucket %s\n", key, bucket)
	err := s.storage.DeleteObject(ctx, bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()

	// AbortMultipartUpload
	if query.Has(uploadIdQuery) {
		s.abortMultipartUpload(w, r)
		return
	}

	// DeleteObject
	s.deleteObject(w, r)
}
