package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
)

type probeDatabase struct {
	database.Database
	ping func(context.Context) error
}

func (db probeDatabase) PingContext(ctx context.Context) error { return db.ping(ctx) }

func TestMonitoringHealthRoutes(t *testing.T) {
	readiness := &Readiness{}
	handler := SetupMonitoringServer(nil, readiness)
	for _, ready := range []bool{false, true, false} {
		readiness.SetReady(ready)
		readyStatus := http.StatusServiceUnavailable
		if ready {
			readyStatus = http.StatusOK
		}
		for _, tc := range []struct {
			path   string
			status int
		}{
			{"/health/livez", http.StatusOK},
			{"/health/readyz", readyStatus},
			{"/health", http.StatusNotFound},
			{"/health/unknown", http.StatusNotFound},
		} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if w.Code != tc.status {
				t.Errorf("ready=%v path=%s: got %d, want %d", ready, tc.path, w.Code, tc.status)
			}
		}
	}
}

func TestReadinessChecks(t *testing.T) {
	for _, tc := range []struct {
		name       string
		ready      bool
		fail       bool
		wantStatus int
		wantCalls  int
	}{
		{"starting", false, false, http.StatusServiceUnavailable, 0},
		{"healthy", true, false, http.StatusOK, 1},
		{"database failure", true, true, http.StatusServiceUnavailable, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readiness := &Readiness{}
			readiness.SetReady(tc.ready)
			calls := 0
			db := probeDatabase{ping: func(ctx context.Context) error {
				calls++
				if _, ok := ctx.Deadline(); !ok {
					t.Error("database check has no deadline")
				}
				if tc.fail {
					return errors.New("private database error")
				}
				return nil
			}}
			w := httptest.NewRecorder()
			readiness.handler([]database.Database{db}, readinessTimeout)(w, httptest.NewRequest(http.MethodGet, "/health/readyz", nil))
			if w.Code != tc.wantStatus || calls != tc.wantCalls {
				t.Fatalf("got status=%d calls=%d; want status=%d calls=%d", w.Code, calls, tc.wantStatus, tc.wantCalls)
			}
			if tc.fail && w.Body.String() != "Not ready\n" {
				t.Fatalf("unexpected failure body: %q", w.Body.String())
			}
		})
	}
}

func TestReadinessSharedBudget(t *testing.T) {
	readiness := &Readiness{}
	readiness.SetReady(true)
	var firstDeadline time.Time
	first := probeDatabase{ping: func(ctx context.Context) error {
		firstDeadline, _ = ctx.Deadline()
		return nil
	}}
	second := probeDatabase{ping: func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(firstDeadline) {
			t.Error("checks do not share the same deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	w := httptest.NewRecorder()
	readiness.handler([]database.Database{first, second}, 10*time.Millisecond)(w, httptest.NewRequest(http.MethodGet, "/health/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestReadinessClientCancellation(t *testing.T) {
	readiness := &Readiness{}
	readiness.SetReady(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db := probeDatabase{ping: func(ctx context.Context) error {
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Errorf("expected client cancellation, got %v", ctx.Err())
		}
		return ctx.Err()
	}}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health/readyz", nil).WithContext(ctx)
	readiness.handler([]database.Database{db}, readinessTimeout)(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestReadinessShutdownDuringCheck(t *testing.T) {
	readiness := &Readiness{}
	readiness.SetReady(true)
	db := probeDatabase{ping: func(context.Context) error {
		readiness.SetReady(false)
		return nil
	}}
	w := httptest.NewRecorder()
	readiness.handler([]database.Database{db}, readinessTimeout)(w, httptest.NewRequest(http.MethodGet, "/health/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", w.Code)
	}
}

func TestLivenessDoesNotCheckDependencies(t *testing.T) {
	readiness := &Readiness{}
	db := probeDatabase{ping: func(context.Context) error {
		t.Fatal("liveness must not ping dependencies")
		return nil
	}}
	w := httptest.NewRecorder()
	SetupMonitoringServer([]database.Database{db}, readiness).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/livez", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", w.Code)
	}
}
