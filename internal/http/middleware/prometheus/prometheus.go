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
	pattern := "/"
	if path != "" {
		if strings.Contains(path, "/") {
			pattern = "/{bucket}/{key...}"
		} else {
			pattern = "/{bucket}"
		}
	}
	return r.Method + " " + pattern
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
