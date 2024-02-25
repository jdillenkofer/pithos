package main

import (
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"strings"
)

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

func listBucketHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("Listing Buckets")
	w.Header().Add("Content-Type", "application/xml")
	w.WriteHeader(200)
	out, _ := xml.MarshalIndent(ListAllMyBucketsResult{
		Buckets: []*Bucket{
			{
				CreationDate: "2012-02-15T21:03:02.000Z",
				Name:         "examplebucket",
			},
			{
				CreationDate: "2011-07-24T19:33:50.000Z",
				Name:         "examplebucket2",
			},
			{
				CreationDate: "2010-12-17T00:56:49.000Z",
				Name:         "examplebucket3",
			},
		},
		Owner: &Owner{
			DisplayName: "own-display-name",
			Id:          "examplee7a2f25102679df27bb0ae12b3f85be6f290b936c4393484be31",
		},
	}, " ", "  ")
	fmt.Fprint(w, string(out))
}

func headBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Head bucket %v\n", bucket)
	w.WriteHeader(200)
}

func listObjectsHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Listing objects in bucket %v\n", bucket)
	w.Header().Add("Content-Type", "application/xml")
	w.WriteHeader(200)
	out, _ := xml.MarshalIndent(ListBucketResult{
		Name:        "quotes",
		Prefix:      "E",
		StartAfter:  "ExampleGuide.pdf",
		KeyCount:    1,
		MaxKeys:     3,
		IsTruncated: false,
		Contents: []*Content{
			{
				Key:          "ExampleObject.txt",
				LastModified: "2013-09-17T18:07:53.000Z",
				ETag:         "\"599bab3ed2c697f1d26842727561fd94\"",
				Size:         857,
				StorageClass: "STANDARD",
			},
			{
				Key:          "ExampleObject2.txt",
				LastModified: "2013-09-17T18:07:54.000Z",
				ETag:         "\"599bab3ed2c697f1d26842727561fd95\"",
				Size:         100,
				StorageClass: "STANDARD",
			},
			{
				Key:          "ExampleObject3.txt",
				LastModified: "2013-09-17T19:07:54.000Z",
				ETag:         "\"599bab3ed2a697f1d26842727561fd95\"",
				Size:         100,
				StorageClass: "STANDARD",
			},
		},
	}, " ", "  ")
	fmt.Fprint(w, string(out))
}

func createBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Creating bucket %v\n", bucket)
	w.Header().Add("Location", bucket)
	w.WriteHeader(200)
}

func deleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	log.Printf("Deleting bucket %v\n", bucket)
	w.WriteHeader(204)
}

func headObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Head object with key %v in bucket %v\n", key, bucket)
	w.Header().Add("Last-Modified", "2013-09-17T18:07:53.000Z")
	w.Header().Add("Content-Length", "100")
	w.WriteHeader(200)
}

func getObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Getting object with key %v from bucket %v\n", key, bucket)
	w.Header().Add("Last-Modified", "2013-09-17T18:07:53.000Z")
	w.Header().Add("Content-Length", "100")
	w.WriteHeader(200)
	fmt.Fprint(w, strings.Repeat("a", 100))
}

func putObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Putting object with key %v to bucket %v\n", key, bucket)
	// Body contains the upload file
	w.WriteHeader(200)
}

func deleteObjectHandler(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	key := r.PathValue("key")
	log.Printf("Deleting object with key %v from bucket %v\n", key, bucket)
	w.WriteHeader(204)
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", listBucketHandler)
	mux.HandleFunc("HEAD /{bucket}", headBucketHandler)
	mux.HandleFunc("GET /{bucket}", listObjectsHandler)
	mux.HandleFunc("PUT /{bucket}", createBucketHandler)
	mux.HandleFunc("DELETE /{bucket}", deleteBucketHandler)
	mux.HandleFunc("HEAD /{bucket}/{key...}", headObjectHandler)
	mux.HandleFunc("GET /{bucket}/{key...}", getObjectHandler)
	mux.HandleFunc("PUT /{bucket}/{key...}", putObjectHandler)
	mux.HandleFunc("DELETE /{bucket}/{key...}", deleteObjectHandler)

	log.Fatal(http.ListenAndServe("localhost:9000", mux))
}
