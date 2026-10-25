package server

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
)

const readinessTimeout = time.Second

// Readiness tracks whether the application is accepting traffic. Its zero value
// is not ready; the application enables it after initialization and listener setup.
type Readiness struct {
	ready atomic.Bool
}

func (r *Readiness) SetReady(ready bool) {
	r.ready.Store(ready)
}

func serveLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("Alive\n"))
}

func (r *Readiness) handler(dbs []database.Database, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if !r.ready.Load() {
			http.Error(w, "Not ready", http.StatusServiceUnavailable)
			return
		}

		// All checks share one budget, also respecting client cancellation.
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		for _, db := range dbs {
			if err := db.PingContext(ctx); err != nil {
				http.Error(w, "Not ready", http.StatusServiceUnavailable)
				return
			}
		}
		// Recheck lifecycle state in case shutdown began during a database ping.
		if ctx.Err() != nil || !r.ready.Load() {
			http.Error(w, "Not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ready\n"))
	}
}
