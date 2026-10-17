package admin

import (
	"container/heap"
	"context"
	"crypto/ed25519"
	"crypto/mldsa"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	serialconfig "github.com/jdillenkofer/pithos/internal/auditlog/serialization/config"
	sinkconfig "github.com/jdillenkofer/pithos/internal/auditlog/sink/config"
	storageconfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/jdillenkofer/pithos/internal/storage/integrity"
)

const auditPageSize = 50
const maxAuditPage = 1000
const maxAuditBytes = 128 << 20
const maxAuditEntries = 1000000

type auditSource struct {
	Path       string
	Serializer serialization.Serializer
}

// Traverse only decoded storage nodes and known FileSink configurations. Request
// data is never used as a filesystem path or serializer name.
func auditSources(si storageconfig.StorageInstantiator) []auditSource {
	var sources []auditSource
	var walk func(storageconfig.StorageInstantiator)
	walk = func(si storageconfig.StorageInstantiator) {
		switch s := si.(type) {
		case *storageconfig.AuditStorageMiddlewareConfiguration:
			for _, sink := range s.SinkInstantiators {
				f, ok := sink.(*sinkconfig.FileSinkConfiguration)
				if !ok {
					continue
				}
				var serializer serialization.Serializer
				switch f.SerializerInstantiator.(type) {
				case *serialconfig.BinarySerializerConfiguration:
					serializer = &serialization.BinarySerializer{MaxFieldBytes: 1 << 20}
				case *serialconfig.JsonSerializerConfiguration:
					serializer = &serialization.JsonSerializer{}
				case *serialconfig.TextSerializerConfiguration:
					serializer = &serialization.TextSerializer{}
				}
				if serializer != nil {
					sources = append(sources, auditSource{f.Path, serializer})
				}
			}
			walk(s.InnerStorageInstantiator)
		case *storageconfig.OutboxStorageConfiguration:
			walk(s.InnerStorageInstantiator)
		case *storageconfig.ObjectCacheStorageMiddlewareConfiguration:
			walk(s.InnerStorageInstantiator)
		case *storageconfig.ReplicationStorageConfiguration:
			walk(s.PrimaryStorageInstantiator)
			for _, c := range s.SecondaryStorageInstantiators {
				walk(c)
			}
		case *storageconfig.ConditionalStorageMiddlewareConfiguration:
			walk(s.DefaultStorageInstantiator)
			for _, k := range sortedConfigKeys(s.BucketToStorageInstantiatorMap) {
				walk(s.BucketToStorageInstantiatorMap[k])
			}
		}
	}
	walk(si)
	return sources
}

type auditFilter struct {
	Page                         int
	Source                       int
	Principal, Operation, Bucket string
	From, To                     time.Time
}

func parseAuditFilter(q url.Values) (auditFilter, error) {
	f := auditFilter{Page: 1, Principal: q.Get("principal"), Operation: q.Get("operation"), Bucket: q.Get("bucket")}
	var err error
	if q.Get("page") != "" {
		f.Page, err = strconv.Atoi(q.Get("page"))
		if err != nil || f.Page < 1 || f.Page > maxAuditPage {
			return f, errors.New("invalid page")
		}
	}
	if q.Get("source") != "" {
		f.Source, err = strconv.Atoi(q.Get("source"))
		if err != nil || f.Source < 0 {
			return f, errors.New("invalid log source")
		}
	}
	if len(f.Principal) > 256 || len(f.Operation) > 256 || len(f.Bucket) > 256 {
		return f, errors.New("filter too long")
	}
	if q.Get("from") != "" {
		f.From, err = time.Parse(time.RFC3339, q.Get("from"))
		if err != nil {
			return f, errors.New("invalid from time; use RFC3339")
		}
	}
	if q.Get("to") != "" {
		f.To, err = time.Parse(time.RFC3339, q.Get("to"))
		if err != nil {
			return f, errors.New("invalid to time; use RFC3339")
		}
	}
	if !f.From.IsZero() && !f.To.IsZero() && f.From.After(f.To) {
		return f, errors.New("from time must not exceed to time")
	}
	return f, nil
}

type auditRow struct {
	Timestamp                                  time.Time
	Index                                      int
	Type                                       auditlog.EntryType
	Principal, Operation, Bucket, Key, Outcome string
}

func (f auditFilter) matches(row auditRow) bool {
	return (f.Principal == "" || f.Principal == row.Principal) && (f.Operation == "" || f.Operation == row.Operation) && (f.Bucket == "" || f.Bucket == row.Bucket) && (f.From.IsZero() || !row.Timestamp.Before(f.From)) && (f.To.IsZero() || !row.Timestamp.After(f.To))
}
func auditEntryRow(e *auditlog.Entry, index int) auditRow {
	row := auditRow{Timestamp: e.Timestamp, Index: index, Type: e.Type}
	if d, ok := e.Details.(*auditlog.LogDetails); ok {
		row.Principal = d.Actor.PrincipalID
		if row.Principal == "" {
			row.Principal = d.Actor.CredentialID
		}
		row.Operation = string(d.Operation)
		row.Bucket = d.Resource.Bucket
		row.Key = d.Resource.Key
		row.Outcome = string(d.Outcome.Outcome)
	}
	return row
}

type auditRowHeap []auditRow

func (h auditRowHeap) Len() int { return len(h) }
func (h auditRowHeap) Less(i, j int) bool {
	if h[i].Timestamp.Equal(h[j].Timestamp) {
		return h[i].Index < h[j].Index
	}
	return h[i].Timestamp.Before(h[j].Timestamp)
}
func (h auditRowHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *auditRowHeap) Push(v any)   { *h = append(*h, v.(auditRow)) }
func (h *auditRowHeap) Pop() any     { old := *h; n := len(old); v := old[n-1]; *h = old[:n-1]; return v }

type auditContextReader struct {
	ctx      context.Context
	reader   io.Reader
	consumed int64
}

func (r *auditContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	r.consumed += int64(n)
	return n, err
}

// Scan a finite snapshot of a regular file. Appends cannot extend a job forever;
// a concurrently incomplete trailing entry is reported, never silently trusted.
func scanAudit(ctx context.Context, source auditSource, visit func(*auditlog.Entry, int) error) error {
	info, err := os.Stat(source.Path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("audit source is not a regular file")
	}
	f, err := os.Open(source.Path)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if !stat.Mode().IsRegular() {
		return errors.New("audit source is not a regular file")
	}
	if stat.Size() > maxAuditBytes {
		return errors.New("audit log exceeds scan limit")
	}
	reader := &auditContextReader{ctx: ctx, reader: io.NewSectionReader(f, 0, stat.Size())}
	dec := source.Serializer.NewDecoder(reader)
	_, binaryInput := source.Serializer.(*serialization.BinarySerializer)
	for index := 0; ; index++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		before := reader.consumed
		e, err := dec.Decode()
		if err == io.EOF {
			if binaryInput && reader.consumed != before {
				return fmt.Errorf("incomplete binary entry %d", index)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot decode entry %d", index)
		}
		if index >= maxAuditEntries {
			return errors.New("audit entry limit exceeded")
		}
		if err := validateAuditEnvelope(e); err != nil {
			return err
		}
		if err = visit(e, index); err != nil {
			return err
		}
	}
}
func validateAuditEnvelope(e *auditlog.Entry) error {
	if e == nil || e.Version < 1 || e.Version > auditlog.CurrentVersion {
		return errors.New("unsupported audit entry version")
	}
	if len(e.Hash) != sha512.Size || len(e.PreviousHash) != sha512.Size || len(e.SignatureEd25519) != ed25519.SignatureSize {
		return errors.New("invalid audit envelope")
	}
	switch e.Type {
	case auditlog.EntryTypeGenesis:
		if d, ok := e.Details.(*auditlog.GenesisDetails); !ok || d == nil {
			return errors.New("invalid genesis details")
		}
	case auditlog.EntryTypeLog:
		if d, ok := e.Details.(*auditlog.LogDetails); !ok || d == nil {
			return errors.New("invalid log details")
		}
	case auditlog.EntryTypeGrounding:
		d, ok := e.Details.(*auditlog.GroundingDetails)
		if !ok || d == nil || len(d.MerkleRootHash) != sha512.Size || len(d.SignatureEd25519) != ed25519.SignatureSize || len(d.SignatureMlDsa87) != mldsa.MLDSA87SignatureSize {
			return errors.New("invalid grounding details")
		}
	default:
		return errors.New("unknown audit entry type")
	}
	return nil
}

func readAuditPage(ctx context.Context, source auditSource, filter auditFilter) ([]auditRow, bool, error) {
	rows := &auditRowHeap{}
	limit := filter.Page*auditPageSize + 1
	err := scanAudit(ctx, source, func(e *auditlog.Entry, index int) error {
		row := auditEntryRow(e, index)
		if !filter.matches(row) {
			return nil
		}
		heap.Push(rows, row)
		if rows.Len() > limit {
			heap.Pop(rows)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	sort.Sort(sort.Reverse(rows))
	start := (filter.Page - 1) * auditPageSize
	if start >= len(*rows) {
		return nil, false, nil
	}
	end := start + auditPageSize
	hasNext := len(*rows) > end
	if end > len(*rows) {
		end = len(*rows)
	}
	return (*rows)[start:end], hasNext, nil
}

type auditPageData struct {
	Rows           []auditRow
	Sources        []int
	Filter         auditFilter
	Previous, Next string
	CSRFToken      string
	Job            adminJobState
}

func (a *App) registerAudit(mux *http.ServeMux) {
	job := &adminJob{workers: &a.workers}
	getSources := func() ([]auditSource, error) {
		si, err := loadAdminStorageConfig(a.options.Settings.StorageJsonPath())
		if err != nil {
			return nil, err
		}
		return auditSources(si), nil
	}
	mux.HandleFunc("GET /audit", func(w http.ResponseWriter, r *http.Request) {
		filter, err := parseAuditFilter(r.URL.Query())
		if err != nil {
			a.fail(w, r, http.StatusBadRequest, err.Error())
			return
		}
		sources, err := getSources()
		if err != nil {
			a.fail(w, r, http.StatusInternalServerError, "Unable to discover audit logs")
			return
		}
		data := auditPageData{Filter: filter, CSRFToken: csrfToken(r), Job: job.snapshot()}
		for i := range sources {
			data.Sources = append(data.Sources, i)
		}
		if len(sources) > 0 {
			if filter.Source >= len(sources) {
				a.fail(w, r, http.StatusBadRequest, "Unknown audit source")
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			defer cancel()
			rows, next, err := readAuditPage(ctx, sources[filter.Source], filter)
			if err != nil {
				a.fail(w, r, http.StatusUnprocessableEntity, "Unable to read audit log: malformed, unavailable, or scan limit exceeded")
				return
			}
			data.Rows = rows
			link := func(page int) string {
				q := r.URL.Query()
				q.Set("page", strconv.Itoa(page))
				return "/audit?" + q.Encode()
			}
			if filter.Page > 1 {
				data.Previous = link(filter.Page - 1)
			}
			if next && filter.Page < maxAuditPage {
				data.Next = link(filter.Page + 1)
			}
		}
		a.render(w, r, "Audit log", "audit", data)
	})
	mux.HandleFunc("GET /audit/verify/progress", func(w http.ResponseWriter, r *http.Request) { renderAdminProgress(w, "audit_progress", job.snapshot()) })
	mux.HandleFunc("POST /audit/verify", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			a.fail(w, r, http.StatusBadRequest, "Invalid form")
			return
		}
		filter, err := parseAuditFilter(r.PostForm)
		if err != nil {
			a.fail(w, r, http.StatusBadRequest, err.Error())
			return
		}
		sources, err := getSources()
		if err != nil || filter.Source >= len(sources) {
			a.fail(w, r, http.StatusBadRequest, "Unknown audit source")
			return
		}
		source := sources[filter.Source]
		if !job.start(a.options.Context, func(ctx context.Context, progress func(int)) (*integrity.ValidationReport, error) {
			validator := auditlog.NewValidator(nil, nil)
			err := scanAudit(ctx, source, func(e *auditlog.Entry, index int) error {
				if err := validator.ValidateEntry(e); err != nil {
					return err
				}
				progress(index + 1)
				return nil
			})
			if err == nil && validator.Index == 0 {
				err = errors.New("empty audit log")
			}
			return nil, err
		}) {
			a.fail(w, r, http.StatusConflict, "A verification is already running or shutdown has begun")
			return
		}
		a.audit(r, "AdminVerifyAuditLog")
		http.Redirect(w, r, "/audit?source="+strconv.Itoa(filter.Source), http.StatusSeeOther)
	})
}
