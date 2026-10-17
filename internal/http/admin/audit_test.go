package admin

import (
	"context"
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/json"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
	"github.com/jdillenkofer/pithos/internal/auditlog/signing"
	"github.com/jdillenkofer/pithos/internal/settings"
	storageconfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/stretchr/testify/require"
)

func auditTestSource(t *testing.T, serializer serialization.Serializer, count int) auditSource {
	t.Helper()
	path := filepath.Join(t.TempDir(), "audit.log")
	file, err := os.Create(path)
	require.NoError(t, err)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer := signing.NewEd25519Signer(priv)
	previous := sha512.Sum512([]byte("pithos"))
	prev := previous[:]
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < count; i++ {
		entry := &auditlog.Entry{Version: auditlog.CurrentVersion, Timestamp: base.Add(time.Duration(i) * time.Second), Type: auditlog.EntryTypeLog, PreviousHash: prev, Details: &auditlog.LogDetails{Operation: auditlog.OpGetObject, Actor: auditlog.ActorDetails{PrincipalID: "principal"}, Resource: auditlog.ResourceDetails{Bucket: "bucket", Key: "key"}}}
		if i == 0 {
			entry.Type = auditlog.EntryTypeGenesis
			entry.Details = &auditlog.GenesisDetails{}
		}
		require.NoError(t, entry.Sign(signer))
		require.NoError(t, serializer.Encode(file, entry))
		prev = entry.Hash
	}
	require.NoError(t, file.Close())
	return auditSource{path, serializer}
}
func TestParseAuditFilter(t *testing.T) {
	for _, query := range []string{"page=0", "page=-1", "page=1001", "page=x", "source=../../etc/passwd", "source=-1", "from=yesterday", "to=tomorrow", "from=2026-02-01T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		q, err := url.ParseQuery(query)
		require.NoError(t, err)
		_, err = parseAuditFilter(q)
		require.Error(t, err, query)
	}
	for _, query := range []string{"range=2h", "range=../../x", "from=2026-13-01T00:00"} {
		q, err := url.ParseQuery(query)
		require.NoError(t, err)
		_, err = parseAuditFilter(q)
		require.Error(t, err, query)
	}
	q, _ := url.ParseQuery("from=2026-01-02T03:04&to=2026-01-02T03:05:06")
	f, err := parseAuditFilter(q)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC), f.From, "datetime-local values are UTC")
	require.Equal(t, time.Date(2026, 1, 2, 3, 5, 6, 0, time.UTC), f.To)
	require.Equal(t, "2026-01-02T03:04:00", f.LocalInput(f.From))
	require.Equal(t, "Custom range", f.RangeLabel())
	q, _ = url.ParseQuery("range=1h&from=2020-01-01T00:00:00Z")
	f, err = parseAuditFilter(q)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().Add(-time.Hour), f.From, time.Minute, "presets override explicit times")
	require.True(t, f.To.IsZero())
	require.Equal(t, "Last 1 hour", f.RangeLabel())
	q, _ = url.ParseQuery("page=2&source=1&principal=alice&operation=GetObject&bucket=bucket&from=2026-01-01T00:00:00Z")
	f, err = parseAuditFilter(q)
	require.NoError(t, err)
	require.Equal(t, 2, f.Page)
	require.Equal(t, 1, f.Source)
	require.Equal(t, "alice", f.Principal)
}
func TestAuditPageNewestFirstAllSerializers(t *testing.T) {
	for name, serializer := range map[string]serialization.Serializer{"json": &serialization.JsonSerializer{Indent: true}, "binary": &serialization.BinarySerializer{MaxFieldBytes: 1 << 20}, "text": &serialization.TextSerializer{}} {
		t.Run(name, func(t *testing.T) {
			source := auditTestSource(t, serializer, 121)
			page, err := readAuditPage(t.Context(), source, auditFilter{Page: 1, All: true})
			rows, next := page.Rows, page.HasNext
			require.NoError(t, err)
			require.True(t, next)
			require.Len(t, rows, 50)
			require.Equal(t, 120, rows[0].Index)
			require.Equal(t, 71, rows[49].Index)
			page, err = readAuditPage(t.Context(), source, auditFilter{Page: 2, All: true})
			rows, next = page.Rows, page.HasNext
			require.NoError(t, err)
			require.True(t, next)
			require.Equal(t, 70, rows[0].Index)
			page, err = readAuditPage(t.Context(), source, auditFilter{Page: 3, All: true})
			rows, next = page.Rows, page.HasNext
			require.NoError(t, err)
			require.False(t, next)
			require.Len(t, rows, 21)
			require.Equal(t, 0, rows[20].Index)
			page, err = readAuditPage(t.Context(), source, auditFilter{Page: 1, Principal: "principal", Operation: "GetObject", Bucket: "bucket", From: time.Date(2026, 1, 1, 0, 1, 59, 0, time.UTC)})
			require.NoError(t, err)
			require.Len(t, page.Rows, 2)
			require.Equal(t, []string{"GetObject"}, page.Operations)
			require.Equal(t, []string{"principal"}, page.Principals)
			require.Equal(t, []string{"bucket"}, page.Buckets)
			page, err = readAuditPage(t.Context(), source, auditFilter{Page: 3})
			require.NoError(t, err)
			require.Len(t, page.Rows, 20, "the genesis entry is hidden unless all entries are requested")
			validator := auditlog.NewValidator(nil, nil)
			require.NoError(t, scanAudit(t.Context(), source, func(e *auditlog.Entry, _ int) error { return validator.ValidateEntry(e) }))
			require.Equal(t, 121, validator.Index)
		})
	}
}
func TestAuditVerificationDetectsTampering(t *testing.T) {
	source := auditTestSource(t, &serialization.JsonSerializer{}, 3)
	data, err := os.ReadFile(source.Path)
	require.NoError(t, err)
	changed := strings.Replace(string(data), `"key":"key"`, `"key":"tampered"`, 1)
	require.NotEqual(t, string(data), changed)
	require.NoError(t, os.WriteFile(source.Path, []byte(changed), 0600))
	validator := auditlog.NewValidator(nil, nil)
	require.Error(t, scanAudit(t.Context(), source, func(e *auditlog.Entry, _ int) error { return validator.ValidateEntry(e) }))
}

func TestScanAuditRejectsTruncatedBinaryEntry(t *testing.T) {
	source := auditTestSource(t, &serialization.BinarySerializer{MaxFieldBytes: 1 << 20}, 2)
	f, err := os.OpenFile(source.Path, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0, 6})
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.Error(t, scanAudit(t.Context(), source, func(*auditlog.Entry, int) error { return nil }))
}

func TestScanAuditRejectsMalformedAndCancelled(t *testing.T) {
	source := auditTestSource(t, &serialization.JsonSerializer{}, 2)
	file, err := os.OpenFile(source.Path, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = file.WriteString("{\"truncated\":")
	require.NoError(t, err)
	require.NoError(t, file.Close())
	_, err = readAuditPage(t.Context(), source, auditFilter{Page: 1})
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, scanAudit(ctx, source, func(*auditlog.Entry, int) error { return nil }), context.Canceled)
	require.Error(t, validateAuditEnvelope(&auditlog.Entry{Version: auditlog.CurrentVersion, Type: auditlog.EntryTypeGrounding}))
}
func TestAuditRoutesVerifyAndRejectUserPaths(t *testing.T) {
	source := auditTestSource(t, &serialization.JsonSerializer{}, 3)
	encodedPath, err := json.Marshal(source.Path)
	require.NoError(t, err)
	configPath := filepath.Join(t.TempDir(), "storage.json")
	raw := `{"type":"AuditStorageMiddleware","innerStorage":{"type":"S3ClientStorage","secretAccessKey":"CONFIG_SECRET"},"sinks":[{"type":"FileSink","path":` + string(encodedPath) + `,"serializer":{"type":"JsonSerializer"}}]}`
	require.NoError(t, os.WriteFile(configPath, []byte(raw), 0600))
	opts, err := settings.LoadSettings([]string{"--storageJsonPath", configPath})
	require.NoError(t, err)
	pages, err := template.ParseFS(assets, "templates/*.html")
	require.NoError(t, err)
	a := &App{options: Options{Context: t.Context(), Settings: opts}, pages: pages}
	mux := http.NewServeMux()
	a.registerAudit(mux)
	get := httptest.NewRecorder()
	mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/audit", nil))
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	require.Contains(t, get.Body.String(), "principal")
	require.NotContains(t, get.Body.String(), "CONFIG_SECRET")
	require.NotContains(t, get.Body.String(), source.Path)
	invalid := httptest.NewRecorder()
	mux.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/audit?source=/etc/passwd", nil))
	require.Equal(t, http.StatusBadRequest, invalid.Code)
	post := httptest.NewRequest(http.MethodPost, "/audit/verify", strings.NewReader("source=0"))
	post.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, post)
	require.Equal(t, http.StatusSeeOther, response.Code)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		progress := httptest.NewRecorder()
		mux.ServeHTTP(progress, httptest.NewRequest(http.MethodGet, "/audit/verify/progress", nil))
		require.NotContains(t, progress.Body.String(), "<!doctype")
		if strings.Contains(progress.Body.String(), "Completed") {
			require.Contains(t, progress.Body.String(), `<div class="label">Entries checked</div><div class="value">3</div>`)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("verification did not complete")
}

func TestAuditSourcesTypedTraversal(t *testing.T) {
	leaf := `{"type":"S3ClientStorage","secretAccessKey":"SECRET"}`
	wrap := func(path string) string {
		return `{"type":"AuditStorageMiddleware","innerStorage":` + leaf + `,"sinks":[{"type":"FileSink","path":` + path + `,"serializer":{"type":"JsonSerializer"}}]}`
	}
	var path1, path2 []byte
	path1, _ = json.Marshal("configured-one")
	path2, _ = json.Marshal("configured-two")
	raw := `{"type":"ConditionalStorageMiddleware","defaultStorage":` + wrap(string(path1)) + `,"bucketToStorageMap":{"bucket":` + wrap(string(path2)) + `}}`
	si, err := storageconfig.CreateStorageInstantiatorFromJson([]byte(raw))
	require.NoError(t, err)
	sources := auditSources(si)
	require.Len(t, sources, 2)
	require.Equal(t, "configured-one", sources[0].Path)
	require.Equal(t, "configured-two", sources[1].Path)
}

func TestAuditFilterHidesStartPhasesAndChainEntries(t *testing.T) {
	filter := auditFilter{}
	require.False(t, filter.matches(auditRow{Type: auditlog.EntryTypeGrounding}))
	require.False(t, filter.matches(auditRow{Type: auditlog.EntryTypeLog, Phase: auditlog.PhaseStart}))
	require.True(t, filter.matches(auditRow{Type: auditlog.EntryTypeLog, Phase: auditlog.PhaseComplete}))
	filter.All = true
	require.True(t, filter.matches(auditRow{Type: auditlog.EntryTypeGrounding}))
	require.True(t, filter.matches(auditRow{Type: auditlog.EntryTypeLog, Phase: auditlog.PhaseStart}))
}

func TestAuditEventDetails(t *testing.T) {
	source := auditTestSource(t, &serialization.JsonSerializer{}, 5)
	entry, err := readAuditEntry(t.Context(), source, 3)
	require.NoError(t, err)
	view := describeAuditEntry(entry, 3)
	require.Equal(t, "GetObject", view.Title)
	titles := []string{}
	for _, section := range view.Sections {
		titles = append(titles, section.Title)
	}
	require.Equal(t, []string{"Event", "Identity", "Resource", "Outcome", "Integrity"}, titles)
	_, err = readAuditEntry(t.Context(), source, 99)
	require.Error(t, err)
	genesis, err := readAuditEntry(t.Context(), source, 0)
	require.NoError(t, err)
	require.Equal(t, "GENESIS", describeAuditEntry(genesis, 0).Title)
}
