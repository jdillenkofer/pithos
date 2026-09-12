package prometheus

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	client "github.com/prometheus/client_golang/prometheus"
)

var once sync.Once
var requests *client.CounterVec
var duration *client.HistogramVec

func register() {
	once.Do(func() {
		requests = client.NewCounterVec(client.CounterOpts{Namespace: "pithos", Subsystem: "http", Name: "requests_total", Help: "Number of HTTP requests by operation and status"}, []string{"operation", "status"})
		duration = client.NewHistogramVec(client.HistogramOpts{Namespace: "pithos", Subsystem: "http", Name: "request_duration_seconds", Help: "Duration of HTTP requests by operation"}, []string{"operation"})
	})
	pithosmetrics.Register(requests, duration)
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func operation(r *http.Request) string {
	path := strings.Trim(r.URL.Path, "/")
	if path == "" && r.Method == http.MethodGet {
		return "ListBuckets"
	}
	isObject := strings.Contains(path, "/")
	query := r.URL.Query()
	has := func(key string) bool { _, ok := query[key]; return ok }
	if !isObject {
		switch r.Method {
		case http.MethodHead:
			return "HeadBucket"
		case http.MethodGet:
			for key, op := range map[string]string{"website": "GetBucketWebsite", "cors": "GetBucketCORS", "lifecycle": "GetBucketLifecycle", "notification": "GetBucketNotification", "versioning": "GetBucketVersioning", "versions": "ListObjectVersions", "uploads": "ListMultipartUploads"} {
				if has(key) {
					return op
				}
			}
			return "ListObjects"
		case http.MethodPut:
			for key, op := range map[string]string{"website": "PutBucketWebsite", "cors": "PutBucketCORS", "lifecycle": "PutBucketLifecycle", "notification": "PutBucketNotification", "versioning": "PutBucketVersioning"} {
				if has(key) {
					return op
				}
			}
			return "CreateBucket"
		case http.MethodDelete:
			for key, op := range map[string]string{"website": "DeleteBucketWebsite", "cors": "DeleteBucketCORS", "lifecycle": "DeleteBucketLifecycle"} {
				if has(key) {
					return op
				}
			}
			return "DeleteBucket"
		case http.MethodPost:
			if has("delete") {
				return "DeleteObjects"
			}
		}
	} else {
		switch r.Method {
		case http.MethodHead:
			return "HeadObject"
		case http.MethodGet:
			if has("uploadId") {
				return "ListParts"
			}
			return "GetObject"
		case http.MethodPut:
			if has("uploadId") {
				if r.Header.Get("x-amz-copy-source") != "" {
					return "UploadPartCopy"
				}
				return "UploadPart"
			}
			if r.Header.Get("x-amz-copy-source") != "" {
				return "CopyObject"
			}
			if r.Header.Get("x-amz-write-offset-bytes") != "" {
				return "AppendObject"
			}
			return "PutObject"
		case http.MethodPost:
			if has("uploads") {
				return "CreateMultipartUpload"
			}
			if has("uploadId") {
				return "CompleteMultipartUpload"
			}
		case http.MethodDelete:
			if has("uploadId") {
				return "AbortMultipartUpload"
			}
			return "DeleteObject"
		}
	}
	return r.Method + " unknown"
}

func New(next http.Handler) http.Handler {
	register()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := operation(r)
		started := time.Now()
		wrapped := &statusWriter{ResponseWriter: w}
		next.ServeHTTP(wrapped, r)
		status := wrapped.status
		if status == 0 {
			status = http.StatusOK
		}
		requests.WithLabelValues(op, strconv.Itoa(status)).Inc()
		duration.WithLabelValues(op).Observe(time.Since(started).Seconds())
	})
}
