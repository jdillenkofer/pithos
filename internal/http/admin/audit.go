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
	"strings"
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
	// Range is a relative preset such as "1h"; empty or "custom" uses From/To.
	Range string
	// All includes request start phases and chain entries (genesis, grounding).
	All bool
}

// auditRanges are the relative time presets, as in CloudTrail's event history.
var auditRanges = []struct {
	Value, Label string
	Duration     time.Duration
}{
	{"15m", "15 minutes", 15 * time.Minute}, {"1h", "1 hour", time.Hour}, {"12h", "12 hours", 12 * time.Hour},
	{"24h", "1 day", 24 * time.Hour}, {"7d", "7 days", 7 * 24 * time.Hour}, {"30d", "30 days", 30 * 24 * time.Hour},
}

// auditTimeLayouts accept RFC3339 and the values of datetime-local inputs,
// which carry no zone and are interpreted as UTC.
var auditTimeLayouts = []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02T15:04"}

func parseAuditTime(value string) (time.Time, error) {
	for _, layout := range auditTimeLayouts {
		if t, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errors.New("invalid time")
}

// LocalInput formats a filter time for a datetime-local input.
func (f auditFilter) LocalInput(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05")
}

func (f auditFilter) RangeLabel() string {
	for _, preset := range auditRanges {
		if preset.Value == f.Range {
			return "Last " + preset.Label
		}
	}
	if f.From.IsZero() && f.To.IsZero() {
		return "All time"
	}
	return "Custom range"
}

func parseAuditFilter(q url.Values) (auditFilter, error) {
	f := auditFilter{Page: 1, Principal: q.Get("principal"), Operation: q.Get("operation"), Bucket: q.Get("bucket"), Range: q.Get("range"), All: q.Get("all") == "true"}
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
	if f.Range != "" && f.Range != "custom" {
		found := false
		for _, preset := range auditRanges {
			if preset.Value == f.Range {
				f.From, found = time.Now().UTC().Add(-preset.Duration), true
			}
		}
		if !found {
			return f, errors.New("invalid time range")
		}
		return f, nil
	}
	if q.Get("from") != "" {
		f.From, err = parseAuditTime(q.Get("from"))
		if err != nil {
			return f, errors.New("invalid start time")
		}
	}
	if q.Get("to") != "" {
		f.To, err = parseAuditTime(q.Get("to"))
		if err != nil {
			return f, errors.New("invalid end time")
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
	Phase                                      auditlog.Phase
	StatusCode                                 int32
	ErrorCode, ClientIP                        string
	DurationMs                                 int64
}

func (f auditFilter) matches(row auditRow) bool {
	if !f.All && (row.Type != auditlog.EntryTypeLog || row.Phase == auditlog.PhaseStart) {
		return false
	}
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
		row.Phase = d.Phase
		row.StatusCode, row.ErrorCode, row.DurationMs = d.Outcome.StatusCode, d.Outcome.ErrorCode, d.Outcome.DurationMs
		row.ClientIP = d.Request.ClientIP
	}
	return row
}

// maxAuditChoices bounds the distinct values offered in filter suggestions.
const maxAuditChoices = 500

// auditChoices collects distinct filter values seen while scanning.
type auditChoices map[string]struct{}

func (c auditChoices) add(value string) {
	if value != "" && len(c) < maxAuditChoices {
		c[value] = struct{}{}
	}
}

func (c auditChoices) sorted(selected string) []string {
	if selected != "" {
		c[selected] = struct{}{}
	}
	values := make([]string, 0, len(c))
	for value := range c {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

type auditPage struct {
	Rows                            []auditRow
	HasNext                         bool
	Operations, Principals, Buckets []string
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

func readAuditPage(ctx context.Context, source auditSource, filter auditFilter) (auditPage, error) {
	rows := &auditRowHeap{}
	limit := filter.Page*auditPageSize + 1
	operations, principals, buckets := auditChoices{}, auditChoices{}, auditChoices{}
	err := scanAudit(ctx, source, func(e *auditlog.Entry, index int) error {
		row := auditEntryRow(e, index)
		operations.add(row.Operation)
		principals.add(row.Principal)
		buckets.add(row.Bucket)
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
		return auditPage{}, err
	}
	page := auditPage{Operations: operations.sorted(filter.Operation), Principals: principals.sorted(filter.Principal), Buckets: buckets.sorted(filter.Bucket)}
	sort.Sort(sort.Reverse(rows))
	start := (filter.Page - 1) * auditPageSize
	if start >= len(*rows) {
		return page, nil
	}
	end := start + auditPageSize
	page.HasNext = len(*rows) > end
	if end > len(*rows) {
		end = len(*rows)
	}
	page.Rows = (*rows)[start:end]
	return page, nil
}

type auditPageData struct {
	auditPage
	Sources        []int
	Filter         auditFilter
	Ranges         []string
	RangeLabels    map[string]string
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
		data := auditPageData{Filter: filter, CSRFToken: csrfToken(r), Job: job.snapshot(), RangeLabels: map[string]string{}}
		for _, preset := range auditRanges {
			data.Ranges = append(data.Ranges, preset.Value)
			data.RangeLabels[preset.Value] = preset.Label
		}
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
			page, err := readAuditPage(ctx, sources[filter.Source], filter)
			if err != nil {
				a.fail(w, r, http.StatusUnprocessableEntity, "Unable to read audit log: malformed, unavailable, or scan limit exceeded")
				return
			}
			data.auditPage = page
			next := page.HasNext
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
	mux.HandleFunc("GET /audit/event", func(w http.ResponseWriter, r *http.Request) {
		sources, err := getSources()
		if err != nil {
			a.fail(w, r, http.StatusInternalServerError, "Unable to discover audit logs")
			return
		}
		a.auditEvent(w, r, sources)
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

var errAuditEntryFound = errors.New("audit entry found")

// readAuditEntry returns the entry at index, scanning the same bounded snapshot.
func readAuditEntry(ctx context.Context, source auditSource, index int) (*auditlog.Entry, error) {
	var found *auditlog.Entry
	err := scanAudit(ctx, source, func(e *auditlog.Entry, i int) error {
		if i == index {
			found = e
			return errAuditEntryFound
		}
		return nil
	})
	if errors.Is(err, errAuditEntryFound) {
		return found, nil
	}
	if err != nil {
		return nil, err
	}
	return nil, errors.New("audit entry not found")
}

type auditSection struct {
	Title  string
	Fields []keyValue
}

type auditEventView struct {
	Source, Index        int
	Title, Time, BackURL string
	Sections             []auditSection
}

func addField(fields []keyValue, label, value string) []keyValue {
	if value == "" {
		return fields
	}
	return append(fields, keyValue{label, value})
}

// describeAuditEntry lists an entry's recorded fields. Backend error messages
// are omitted, as elsewhere in the console; the error code is shown.
func describeAuditEntry(e *auditlog.Entry, index int) auditEventView {
	view := auditEventView{Index: index, Title: string(e.Type), Time: e.Timestamp.UTC().Format("2006-01-02 15:04:05.000") + " UTC"}
	event := []keyValue{{"Event time", view.Time}, {"Entry type", string(e.Type)}, {"Entry index", strconv.Itoa(index)}}
	if d, ok := e.Details.(*auditlog.LogDetails); ok && d != nil {
		view.Title = string(d.Operation)
		event = addField(event, "Operation", string(d.Operation))
		event = addField(event, "Phase", string(d.Phase))
		view.Sections = append(view.Sections, auditSection{"Event", event})
		var identity []keyValue
		identity = addField(identity, "Principal ID", d.Actor.PrincipalID)
		identity = addField(identity, "Account ID", d.Actor.AccountID)
		identity = addField(identity, "Access key ID", d.Actor.CredentialID)
		identity = addField(identity, "Authentication", string(d.Actor.AuthType))
		if len(identity) == 0 {
			identity = []keyValue{{"Principal", "Anonymous"}}
		}
		view.Sections = append(view.Sections, auditSection{"Identity", identity})
		var request []keyValue
		request = addField(request, "Source IP address", d.Request.ClientIP)
		request = addField(request, "Request ID", d.Request.RequestID)
		request = addField(request, "Trace ID", d.Request.TraceID)
		if len(request) > 0 {
			view.Sections = append(view.Sections, auditSection{"Request", request})
		}
		var resource []keyValue
		resource = addField(resource, "Bucket", d.Resource.Bucket)
		resource = addField(resource, "Key", d.Resource.Key)
		resource = addField(resource, "Version ID", d.Resource.VersionID)
		resource = addField(resource, "Upload ID", d.Resource.UploadID)
		if d.Resource.PartNumber != 0 {
			resource = addField(resource, "Part number", strconv.Itoa(int(d.Resource.PartNumber)))
		}
		resource = addField(resource, "Copy source bucket", d.Resource.SourceBucket)
		resource = addField(resource, "Copy source key", d.Resource.SourceKey)
		if len(resource) > 0 {
			view.Sections = append(view.Sections, auditSection{"Resource", resource})
		}
		var outcome []keyValue
		outcome = addField(outcome, "Outcome", string(d.Outcome.Outcome))
		if d.Outcome.StatusCode != 0 {
			outcome = addField(outcome, "HTTP status", strconv.Itoa(int(d.Outcome.StatusCode)))
		}
		outcome = addField(outcome, "Error code", d.Outcome.ErrorCode)
		if d.Phase != auditlog.PhaseStart {
			outcome = addField(outcome, "Duration", strconv.FormatInt(d.Outcome.DurationMs, 10)+" ms")
		}
		view.Sections = append(view.Sections, auditSection{"Outcome", outcome})
		if lock := d.ObjectLock; lock != nil {
			var fields []keyValue
			fields = addField(fields, "Requested mode", lock.Requested.Mode)
			fields = addField(fields, "Requested retain until", lock.Requested.RetainUntilDate)
			fields = addField(fields, "Requested legal hold", lock.Requested.LegalHold)
			fields = addField(fields, "Effective mode", lock.Effective.Mode)
			fields = addField(fields, "Effective retain until", lock.Effective.RetainUntilDate)
			fields = addField(fields, "Effective legal hold", lock.Effective.LegalHold)
			fields = append(fields, keyValue{"Governance bypass", fmt.Sprintf("requested %t, authorized %t, used %t", lock.BypassRequested, lock.BypassAuthorized, lock.BypassUsed)})
			view.Sections = append(view.Sections, auditSection{"Object Lock", fields})
		}
	} else {
		view.Sections = append(view.Sections, auditSection{"Event", event})
	}
	view.Sections = append(view.Sections, auditSection{"Integrity", []keyValue{
		{"Entry hash (SHA-512)", fmt.Sprintf("%x", e.Hash)},
		{"Previous hash", fmt.Sprintf("%x", e.PreviousHash)},
	}})
	return view
}

func (a *App) auditEvent(w http.ResponseWriter, r *http.Request, sources []auditSource) {
	source, err := strconv.Atoi(r.URL.Query().Get("source"))
	if err != nil || source < 0 || source >= len(sources) {
		a.fail(w, r, http.StatusBadRequest, "Unknown audit source")
		return
	}
	index, err := strconv.Atoi(r.URL.Query().Get("index"))
	if err != nil || index < 0 || index >= maxAuditEntries {
		a.fail(w, r, http.StatusBadRequest, "Invalid audit entry")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	entry, err := readAuditEntry(ctx, sources[source], index)
	if err != nil {
		a.fail(w, r, http.StatusNotFound, "Audit entry not found or log unreadable")
		return
	}
	view := describeAuditEntry(entry, index)
	view.Source = source
	view.BackURL = "/audit?source=" + strconv.Itoa(source)
	if back := sameOriginBackLink(r); strings.HasPrefix(back, "/audit?") {
		view.BackURL = back
	}
	a.render(w, r, "Audit log", "audit-event", view)
}
