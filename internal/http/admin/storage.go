package admin

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/config"
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
}
type adminJobState struct {
	Running   bool
	Started   time.Time
	Finished  time.Time
	Processed int
	Message   string
	Report    *integrity.ValidationReport
}

func (j *adminJob) snapshot() adminJobState { j.mu.Lock(); defer j.mu.Unlock(); return j.state }
func (j *adminJob) start(parent context.Context, work func(context.Context, func(int)) (*integrity.ValidationReport, error)) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.state.Running || parent.Err() != nil {
		return false
	}
	j.state = adminJobState{Running: true, Started: time.Now(), Message: "Running", Report: j.state.Report}
	run := func() {
		ctx, cancel := context.WithTimeout(parent, 10*time.Minute)
		defer cancel()
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
			j.state.Message = "Validation failed; consult server logs"
			slog.ErrorContext(ctx, "Admin validation failed", "error", err)
		}
		if ctx.Err() != nil {
			j.state.Message = "Cancelled or time limit exceeded"
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

var adminProgressTemplates = template.Must(template.ParseFS(assets, "templates/audit_progress.html", "templates/storage_progress.html"))

// Polling endpoints return a fragment even when refreshed without htmx. They do
// not use the full-page renderer, which would nest the layout inside the job.
func renderAdminProgress(w http.ResponseWriter, name string, state adminJobState) {
	var body bytes.Buffer
	if err := adminProgressTemplates.ExecuteTemplate(&body, name, state); err != nil {
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
			Job       adminJobState
			CSRFToken string
		}{storageTree(si), job.snapshot(), csrfToken(r)})
	})
	mux.HandleFunc("GET /storage/validate/progress", func(w http.ResponseWriter, r *http.Request) {
		renderAdminProgress(w, "storage_progress", job.snapshot())
	})
	mux.HandleFunc("POST /storage/validate", func(w http.ResponseWriter, r *http.Request) {
		if len(a.options.Databases) != 1 {
			a.fail(w, r, http.StatusUnprocessableEntity, "Validation requires exactly one local database")
			return
		}
		if !job.start(a.options.Context, func(ctx context.Context, progress func(int)) (*integrity.ValidationReport, error) {
			dbs := config.NewDbContainer()
			for _, db := range a.options.Databases {
				dbs.AddDb(db)
			}
			// These constants are intentionally not controlled by any request parameter.
			return integrity.NewValidator(a.options.Storage, dbs, false, false).
				WithProgress(progress).
				WithResultLimits(adminValidationLimits).
				ValidateAll(ctx)
		}) {
			a.fail(w, r, http.StatusConflict, "A validation is already running or shutdown has begun")
			return
		}
		a.audit(r, "AdminValidateStorage")
		http.Redirect(w, r, "/storage", http.StatusSeeOther)
	})
}
