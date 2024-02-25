package server

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	storage "github.com/jdillenkofer/pithos/internal/storage"
)

type Server struct {
	httpServer *http.Server
	storage    storage.Storage
}

func New(addr string, storage storage.Storage) *Server {
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
	server.httpServer = &http.Server{Addr: addr, Handler: mux}
	return server
}

func (s *Server) ListenAndServe() error {
	return s.httpServer.ListenAndServe()
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

type Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type ListBucketResult struct {
	XMLName     xml.Name   `xml:"ListBucketResult"`
	Name        string     `xml:"Name"`
	Prefix      string     `xml:"Prefix"`
	StartAfter  string     `xml:"StartAfter"`
	KeyCount    int        `xml:"KeyCount"`
	MaxKeys     int        `xml:"MaxKeys"`
	IsTruncated bool       `xml:"IsTruncated"`
	Contents    []*Content `xml:"Contents"`
}

func (s *Server) listBucketHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Listing Buckets")
	buckets, err := s.storage.ListBuckets()
	if err != nil {
		w.WriteHeader(500)
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
	out, _ := xml.MarshalIndent(listAllMyBucketsResult, " ", "  ")
	fmt.Fprint(w, string(out))
}

func (s *Server) headBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucketName := r.PathValue("bucket")
	log.Printf("Head bucket %v\n", bucketName)
	_, err := s.storage.ExistBucket(bucketName)
	if err == storage.ErrBucketNotFound {
		w.WriteHeader(404)
		return
	}
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Listing objects in bucket %v\n", bucket)
	objects, err := s.storage.ListObjects(bucket)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	listBucketResult := ListBucketResult{
		Name:        bucket,
		Prefix:      "",
		StartAfter:  "",
		KeyCount:    len(objects),
		MaxKeys:     len(objects),
		IsTruncated: false,
		Contents:    []*Content{},
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
	out, _ := xml.MarshalIndent(listBucketResult, " ", "  ")
	fmt.Fprint(w, string(out))
}

func (s *Server) createBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Creating bucket %v\n", bucket)
	err := s.storage.CreateBucket(bucket)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Header().Add("Location", bucket)
	w.WriteHeader(200)
}

func (s *Server) deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Deleting bucket %v\n", bucket)
	err := s.storage.DeleteBucket(bucket)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(204)
}

func (s *Server) headObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Head object with key %v in bucket %v\n", key, bucket)
	object, err := s.storage.ExistObject(bucket, key)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.Header().Add("Last-Modified", object.LastModified.Format(time.RFC3339))
	w.Header().Add("Content-Length", fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
}

func (s *Server) getObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Getting object with key %v from bucket %v\n", key, bucket)
	object, err := s.storage.ExistObject(bucket, key)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	reader, err := s.storage.GetObject(bucket, key)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	defer reader.Close()
	w.Header().Add("Last-Modified", object.LastModified.Format(time.RFC3339))
	w.Header().Add("Content-Length", fmt.Sprintf("%v", object.Size))
	w.WriteHeader(200)
	io.Copy(w, reader)
}

func (s *Server) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Putting object with key %v to bucket %v\n", key, bucket)
	if r.Header.Get("Expect") == "100-continue" {
		w.WriteHeader(100)
	}
	err := s.storage.PutObject(bucket, key, r.Body)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(200)
}

func (s *Server) deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Deleting object with key %v from bucket %v\n", key, bucket)
	err := s.storage.DeleteObject(bucket, key)
	if err != nil {
		w.WriteHeader(500)
		return
	}
	w.WriteHeader(204)
}
