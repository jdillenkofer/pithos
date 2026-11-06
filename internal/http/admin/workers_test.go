package admin

import (
	"context"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/integrity"
)

func TestHandlerCloseCancelsAndWaitsForJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	group := &backgroundWorkers{}
	handler := &Handler{cancel: cancel, workers: group}
	job := &adminJob{workers: group}
	started, finished := make(chan struct{}), make(chan struct{})
	if !job.start(ctx, func(ctx context.Context, _ func(int)) (*integrity.ValidationReport, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return nil, ctx.Err()
	}) {
		t.Fatal("job did not start")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not run")
	}
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("Close returned before job stopped")
	}
	if job.snapshot().Running {
		t.Fatal("job still marked running")
	}
	if job.start(ctx, func(context.Context, func(int)) (*integrity.ValidationReport, error) {
		t.Error("job ran after shutdown")
		return nil, nil
	}) {
		t.Fatal("accepted job after shutdown")
	}
	if group.goRun(func() { t.Error("worker ran after shutdown") }) {
		t.Fatal("accepted worker after shutdown")
	}
}
