package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	storageconfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/jdillenkofer/pithos/internal/storage/integrity"
	partconfig "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/config"
)

// adminJob is owned by a route registration, not a global registry. Only the
// latest report is retained, and all work inherits the application's lifetime.
type adminJob struct {
	mu      sync.Mutex
	state   adminJobState
	workers *backgroundWorkers
	// bytes is updated for every read, so it avoids the state mutex.
	bytes atomic.Int64
}
type adminJobState struct {
	Running   bool
	Started   time.Time
	Finished  time.Time
	Processed int
	Message   string
	Report    *integrity.ValidationReport
	// ProcessedBytes and the estimated totals drive the progress bar. Totals
	// are zero when no estimate is available.
	ProcessedBytes           int64
	TotalObjects, TotalBytes int64
	// Notes describe what the job checked; Failure explains a failed job when
	// the error is safe to show.
	Notes   []string
	Failure string
}

// jobFailure is an error whose message may be shown in the console.
type jobFailure struct{ message string }

func (e jobFailure) Error() string { return e.message }

func (j *adminJob) setNotes(notes ...string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.Notes = notes
}

func (j *adminJob) snapshot() adminJobState {
	j.mu.Lock()
	defer j.mu.Unlock()
	state := j.state
	state.ProcessedBytes = j.bytes.Load()
	return state
}

func (j *adminJob) setEstimate(objects, bytes int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state.TotalObjects, j.state.TotalBytes = objects, bytes
}
func (j *adminJob) start(parent context.Context, work func(context.Context, func(int)) (*integrity.ValidationReport, error)) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state.Running || parent.Err() != nil {
		return false
	}
	j.state = adminJobState{Running: true, Started: time.Now(), Message: "Running", Report: j.state.Report}
	j.bytes.Store(0)
	// Jobs have no time limit; they end with the application's lifetime.
	run := func() {
		ctx := parent
		report, err := work(ctx, func(n int) { j.mu.Lock(); j.state.Processed = n; j.mu.Unlock() })
		j.mu.Lock()
		defer j.mu.Unlock()
		j.state.Running = false
		j.state.Finished = time.Now()
		if report != nil {
			j.state.Report = report
		}
		j.state.Message = "Completed"
		if err != nil {
			j.state.Message = "Failed"
			var failure jobFailure
			if errors.As(err, &failure) {
				j.state.Failure = failure.message
			} else {
				j.state.Failure = "Consult the server logs for details."
			}
			slog.ErrorContext(ctx, "Admin job failed", "error", err)
		}
		if ctx.Err() != nil {
			j.state.Message, j.state.Failure = "Cancelled", "The server is shutting down."
		}
	}
	if j.workers != nil {
		if !j.workers.goRun(run) {
			j.state.Running = false
			j.state.Message = "Shutdown has begun"
			return false
		}
	} else {
		go run()
	}
	return true
}

var adminProgressTemplates = template.Must(template.ParseFS(assets, "templates/audit_progress.html", "templates/storage_progress.html", "templates/buckets_empty.html"))

// jobProgressView adds display helpers and the CSRF token for the
// restart form to a validation job snapshot.
type jobProgressView struct {
	adminJobState
	CSRFToken string
	// Source is the audit log a verification fragment restarts.
	Source int
}

// Percent is the validated share of the estimated bytes, or of the estimated
// objects when no bytes are known. Unfinished jobs never report 100%, because
// estimates come from periodically sampled gauges and may be stale.
func (v jobProgressView) Percent() int {
	if v.Message == "Completed" && !v.Running {
		return 100
	}
	var percent int64
	switch {
	case v.TotalBytes > 0:
		percent = 100 * v.ProcessedBytes / v.TotalBytes
	case v.TotalObjects > 0:
		percent = 100 * int64(v.Processed) / v.TotalObjects
	default:
		return -1
	}
	return int(min(percent, 99))
}

func (v jobProgressView) BytesText() string { return humanBytes(float64(v.ProcessedBytes)) }

func (v jobProgressView) TotalText() string {
	if v.TotalBytes == 0 {
		return ""
	}
	return humanBytes(float64(v.TotalBytes))
}

func (v jobProgressView) elapsed() time.Duration {
	end := v.Finished
	if v.Running || end.IsZero() {
		end = time.Now()
	}
	return end.Sub(v.Started)
}

func (v jobProgressView) RateText() string {
	seconds := v.elapsed().Seconds()
	if seconds < 1 || v.ProcessedBytes == 0 {
		return "-"
	}
	return humanBytes(float64(v.ProcessedBytes)/seconds) + "/s"
}

func (v jobProgressView) DurationText() string {
	if v.Started.IsZero() {
		return "-"
	}
	return v.elapsed().Round(time.Second).String()
}

// RemainingText estimates the time left from the current throughput.
func (v jobProgressView) RemainingText() string {
	seconds := v.elapsed().Seconds()
	if !v.Running || v.TotalBytes == 0 || v.ProcessedBytes == 0 || seconds < 2 {
		return "-"
	}
	remaining := float64(max(v.TotalBytes-v.ProcessedBytes, 0)) / (float64(v.ProcessedBytes) / seconds)
	return (time.Duration(remaining) * time.Second).Round(time.Second).String()
}

// Polling endpoints return a fragment even when refreshed without htmx. They do
// not use the full-page renderer, which would nest the layout inside the job.
func renderAdminProgress(w http.ResponseWriter, name string, data any) {
	var body bytes.Buffer
	if err := adminProgressTemplates.ExecuteTemplate(&body, name, data); err != nil {
		http.Error(w, "Unable to render progress", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(body.Bytes())
}

type storageNode struct {
	Kind     string
	Fields   []storageField
	Children []storageNode
}
type storageField struct {
	Name  string
	Value string
}

// No raw configuration, providers, URLs, connection strings, or signing fields
// may be sent to a template. New types must explicitly opt into this projection.
func storageTree(si storageconfig.StorageInstantiator) storageNode {
	n := storageNode{Kind: "Unsupported storage"}
	child := func(s storageconfig.StorageInstantiator) { n.Children = append(n.Children, storageTree(s)) }
	switch s := si.(type) {
	case *storageconfig.MetadataPartStorageConfiguration:
		n.Kind = "MetadataPartStorage"
		n.Children = append(n.Children, partTree(s.PartStoreInstantiator))
		keys := sortedConfigKeys(s.ExtraPartStoreInstantiators)
		for _, k := range keys {
			n.Children = append(n.Children, partTree(s.ExtraPartStoreInstantiators[k]))
		}
	case *storageconfig.AuditStorageMiddlewareConfiguration:
		n.Kind = "AuditStorageMiddleware"
		child(s.InnerStorageInstantiator)
		n.Fields = append(n.Fields, storageField{"Sinks", fmt.Sprint(len(s.SinkInstantiators))})
	case *storageconfig.OutboxStorageConfiguration:
		n.Kind = "OutboxStorage"
		child(s.InnerStorageInstantiator)
	case *storageconfig.ObjectCacheStorageMiddlewareConfiguration:
		n.Kind = "ObjectCacheStorageMiddleware"
		child(s.InnerStorageInstantiator)
		n.Fields = append(n.Fields, storageField{"Maximum object bytes", fmt.Sprint(s.MaxObjectSizeBytes.Value())})
	case *storageconfig.ReplicationStorageConfiguration:
		n.Kind = "ReplicationStorage"
		child(s.PrimaryStorageInstantiator)
		for _, c := range s.SecondaryStorageInstantiators {
			child(c)
		}
	case *storageconfig.ConditionalStorageMiddlewareConfiguration:
		n.Kind = "ConditionalStorageMiddleware"
		child(s.DefaultStorageInstantiator)
		for _, k := range sortedConfigKeys(s.BucketToStorageInstantiatorMap) {
			child(s.BucketToStorageInstantiatorMap[k])
		}
	case *storageconfig.S3ClientStorageConfiguration:
		n.Kind = "S3ClientStorage"
		n.Fields = append(n.Fields, storageField{"Path-style requests", fmt.Sprint(s.UsePathStyle)})
	}
	return n
}
func sortedConfigKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func partTree(si partconfig.PartStoreInstantiator) storageNode {
	n := storageNode{Kind: "Part store (details hidden)"}
	child := func(p partconfig.PartStoreInstantiator) { n.Children = append(n.Children, partTree(p)) }
	switch p := si.(type) {
	case *partconfig.FilesystemPartStoreConfiguration:
		n.Kind = "FilesystemPartStore"
	case *partconfig.SqlPartStoreConfiguration:
		n.Kind = "SqlPartStore"
	case *partconfig.SftpPartStoreConfiguration:
		n.Kind = "SftpPartStore"
	case *partconfig.GoogleDrivePartStoreConfiguration:
		n.Kind = "GoogleDrivePartStore"
	case *partconfig.OneDrivePartStoreConfiguration:
		n.Kind = "OneDrivePartStore"
	case *partconfig.DropboxPartStoreConfiguration:
		n.Kind = "DropboxPartStore"
	case *partconfig.TinkEncryptionPartStoreMiddlewareConfiguration:
		n.Kind = "TinkEncryptionPartStoreMiddleware"
		child(p.InnerPartStoreInstantiator)
	case *partconfig.CompressionPartStoreMiddlewareConfiguration:
		n.Kind = "CompressionPartStoreMiddleware"
		child(p.InnerPartStoreInstantiator)
	case *partconfig.OutboxPartStoreConfiguration:
		n.Kind = "OutboxPartStore"
		child(p.InnerPartStoreInstantiator)
	case *partconfig.CachePartStoreConfiguration:
		n.Kind = "CachePartStore"
		child(p.InnerPartStoreInstantiator)
	case *partconfig.ErasureCodedPartStoreMiddlewareConfiguration:
		n.Kind = "ErasureCodedPartStoreMiddleware"
		n.Fields = []storageField{{"Data shards", fmt.Sprint(p.DataShards)}, {"Parity shards", fmt.Sprint(p.ParityShards)}}
		for _, c := range p.PartStoreInstantiators {
			child(c)
		}
	}
	return n
}
func loadAdminStorageConfig(path string) (storageconfig.StorageInstantiator, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	const max = 2 << 20
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return nil, fmt.Errorf("storage configuration too large")
	}
	return storageconfig.CreateStorageInstantiatorFromJson(b)
}

// adminValidationLimits keeps the last report, which lives as long as the
// server process, small regardless of how many objects are scanned. Error
// details are never displayed, so only a short excerpt is retained for logs.
var adminValidationLimits = integrity.ResultLimits{
	FailuresOnly:         true,
	MaxResults:           100,
	MaxFailuresPerResult: 10,
	MaxErrorLength:       256,
}

func (a *App) registerStorage(mux *http.ServeMux) {
	job := &adminJob{workers: &a.workers}
	mux.HandleFunc("GET /storage", func(w http.ResponseWriter, r *http.Request) {
		si, err := loadAdminStorageConfig(a.options.Settings.StorageJsonPath())
		if err != nil {
			a.fail(w, r, http.StatusInternalServerError, "Unable to read storage configuration")
			return
		}
		a.render(w, r, "Storage", "storage", struct {
			Tree      storageNode
			Job       jobProgressView
			CSRFToken string
		}{storageTree(si), jobProgressView{adminJobState: job.snapshot(), CSRFToken: csrfToken(r)}, csrfToken(r)})
	})
	mux.HandleFunc("GET /storage/validate/progress", func(w http.ResponseWriter, r *http.Request) {
		renderAdminProgress(w, "storage_progress", jobProgressView{adminJobState: job.snapshot(), CSRFToken: csrfToken(r)})
	})
	mux.HandleFunc("POST /storage/validate", func(w http.ResponseWriter, r *http.Request) {
		if len(a.options.Databases) != 1 {
			a.fail(w, r, http.StatusUnprocessableEntity, "Validation requires exactly one local database")
			return
		}
		if !job.start(a.options.Context, func(ctx context.Context, progress func(int)) (*integrity.ValidationReport, error) {
			// Validation reads the parts of every version, current and
			// noncurrent, so the stored part bytes estimate the total best.
			if snapshot, err := gatherMetrics(); err == nil && snapshot.hasParts {
				job.setEstimate(0, int64(snapshot.logicalBytes))
			}
			// These constants are intentionally not controlled by any request parameter.
			return integrity.NewValidator(a.options.Storage, false, false).
				WithProgress(progress).
				WithByteProgress(func(n int64) { job.bytes.Add(n) }).
				WithResultLimits(adminValidationLimits).
				ValidateAll(ctx)
		}) {
			if r.Header.Get("HX-Request") == "true" {
				// The fragment already shows the running job; just refresh it.
				renderAdminProgress(w, "storage_progress", jobProgressView{adminJobState: job.snapshot(), CSRFToken: csrfToken(r)})
				return
			}
			a.fail(w, r, http.StatusConflict, "A validation is already running or shutdown has begun")
			return
		}
		a.audit(r, "AdminValidateStorage")
		if r.Header.Get("HX-Request") == "true" {
			renderAdminProgress(w, "storage_progress", jobProgressView{adminJobState: job.snapshot(), CSRFToken: csrfToken(r)})
			return
		}
		http.Redirect(w, r, "/storage", http.StatusSeeOther)
	})
}
