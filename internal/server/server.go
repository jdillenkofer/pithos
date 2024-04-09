package server

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	storage "github.com/jdillenkofer/pithos/internal/storage"
)

type Server struct {
	storage storage.Storage
}

func SetupServer(storage storage.Storage) http.Handler {
	server := &Server{
		storage: storage,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", server.listBucketHandler)
	mux.HandleFunc("HEAD /{bucket}", server.headBucketHandler)
	mux.HandleFunc("GET /{bucket}", server.listObjectsHandler)
	mux.HandleFunc("PUT /{bucket}", server.createBucketHandler)
	mux.HandleFunc("DELETE /{bucket}", server.deleteBucketHandler)
	mux.HandleFunc("HEAD /{bucket}/{key...}", server.headObjectHandler)
	mux.HandleFunc("GET /{bucket}/{key...}", server.getObjectHandler)
	mux.HandleFunc("PUT /{bucket}/{key...}", server.putObjectHandler)
	mux.HandleFunc("DELETE /{bucket}/{key...}", server.deleteObjectHandler)
	return mux
}

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
	MaxKeys        int               `xml:"MaxKeys"`
	CommonPrefixes []*CommonPrefixes `xml:"CommonPrefixes"`
	KeyCount       int               `xml:"KeyCount"`
	StartAfter     string            `xml:"StartAfter"`
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

func (s *Server) listBucketHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Listing Buckets")
	buckets, err := s.storage.ListBuckets()
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Add("Content-Type", "application/xml")
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
	bucketName := r.PathValue("bucket")
	log.Printf("Head bucket %s\n", bucketName)
	_, err := s.storage.HeadBucket(bucketName)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	startAfter := query.Get("startAfter")
	maxKeysI64, err := strconv.ParseInt(query.Get("maxKeys"), 10, 32)
	if err != nil || maxKeysI64 < 0 {
		maxKeysI64 = 1000
	}
	maxKeys := int(maxKeysI64)

	log.Printf("Listing objects in bucket %s\n", bucket)
	objects, commonPrefixes, err := s.storage.ListObjects(bucket, prefix, delimiter, startAfter, maxKeys)
	if err != nil {
		handleError(err, w, r)
		return
	}
	listBucketResult := ListBucketResult{
		Name:           bucket,
		Prefix:         prefix,
		Delimiter:      delimiter,
		StartAfter:     startAfter,
		KeyCount:       len(objects),
		MaxKeys:        maxKeys,
		CommonPrefixes: []*CommonPrefixes{},
		IsTruncated:    len(objects) == maxKeys,
		Contents:       []*Content{},
	}

	w.Header().Add("Content-Type", "application/xml")
	w.WriteHeader(200)
	for _, object := range objects {
		listBucketResult.Contents = append(listBucketResult.Contents, &Content{
			Key:          object.Key,
			LastModified: object.LastModified.Format(time.RFC3339),
			ETag:         object.ETag,
			Size:         object.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, commonPrefix := range commonPrefixes {
		listBucketResult.CommonPrefixes = append(listBucketResult.CommonPrefixes, &CommonPrefixes{Prefix: commonPrefix})
	}
	out, _ := xmlMarshalWithDocType(listBucketResult)
	w.Write(out)
}

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Creating bucket %s\n", bucket)
	err := s.storage.CreateBucket(bucket)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Add("Location", bucket)
	w.WriteHeader(200)
}

func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Deleting bucket %s\n", bucket)
	err := s.storage.DeleteBucket(bucket)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Head object with key %s in bucket %s\n", key, bucket)
	object, err := s.storage.HeadObject(bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.Header().Add("Last-Modified", object.LastModified.Format(time.RFC3339))
	w.Header().Add("Content-Length", fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
}

func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Getting object with key %s from bucket %s\n", key, bucket)
	object, err := s.storage.HeadObject(bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	reader, err := s.storage.GetObject(bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	defer reader.Close()
	w.Header().Add("Last-Modified", object.LastModified.UTC().Format(http.TimeFormat))
	w.Header().Add("Content-Length", fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
	io.Copy(w, reader)
}

func (s *Server) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Putting object with key %s to bucket %s\n", key, bucket)
	if r.Header.Get("Expect") == "100-continue" {
		w.WriteHeader(100)
	}
	err := s.storage.PutObject(bucket, key, r.Body)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Deleting object with key %s from bucket %s\n", key, bucket)
	err := s.storage.DeleteObject(bucket, key)
	if err != nil {
		handleError(err, w, r)
		return
	}
	w.WriteHeader(204)
}
