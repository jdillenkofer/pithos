package admin

import (
	"bytes"
	"context"
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	storageconfig "github.com/jdillenkofer/pithos/internal/storage/config"
	"github.com/jdillenkofer/pithos/internal/storage/integrity"
	"github.com/stretchr/testify/require"
)

func TestStorageTreeNeverDisplaysSecrets(t *testing.T) {
	raw := `{"type":"AuditStorageMiddleware","signing":{"ed25519":{"privateKey":"PRIVATE_SECRET","vault":{"token":"VAULT_SECRET","secretId":"ROLE_SECRET"}}},"sinks":[{"type":"FileSink","path":"SECRET_PATH","serializer":{"type":"JsonSerializer"}}],"innerStorage":{"type":"S3ClientStorage","baseEndpoint":"https://user:URL_SECRET@example.test","accessKeyId":"ACCESS_SECRET","secretAccessKey":"S3_SECRET","region":"REGION_SECRET","usePathStyle":true}}`
	si, err := storageconfig.CreateStorageInstantiatorFromJson([]byte(raw))
	require.NoError(t, err)
	pages, err := template.ParseFS(assets, "templates/*.html")
	require.NoError(t, err)
	var out bytes.Buffer
	require.NoError(t, pages.ExecuteTemplate(&out, "storage", struct {
		Tree      storageNode
		Job       adminJobState
		CSRFToken string
	}{Tree: storageTree(si)}))
	for _, secret := range []string{"PRIVATE_SECRET", "VAULT_SECRET", "ROLE_SECRET", "SECRET_PATH", "URL_SECRET", "ACCESS_SECRET", "S3_SECRET", "REGION_SECRET"} {
		require.NotContains(t, out.String(), secret)
	}
	require.Contains(t, out.String(), "S3ClientStorage")
	require.Contains(t, out.String(), "Path-style requests")
}
func TestStorageProgressHidesBackendErrorsAndEscapesKeys(t *testing.T) {
	rec := httptest.NewRecorder()
	renderAdminProgress(rec, "storage_progress", adminJobState{Started: time.Now(), Report: &integrity.ValidationReport{FailedObjects: 1, Results: []integrity.ValidationResult{{BucketName: "bucket", ObjectKey: "<script>alert(1)</script>", ErrorType: "postgres://user:SECRET@db", PartFailures: []integrity.PartFailure{{Error: "SECRET"}}}}}})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "SECRET")
	require.NotContains(t, rec.Body.String(), "<script>")
	require.Contains(t, rec.Body.String(), "&lt;script&gt;")
	require.NotContains(t, rec.Body.String(), "<!doctype")
}
func TestStorageProgressIndicatesTruncatedReport(t *testing.T) {
	rec := httptest.NewRecorder()
	renderAdminProgress(rec, "storage_progress", adminJobState{Started: time.Now(), Report: &integrity.ValidationReport{TotalObjects: 5000, FailedObjects: 150, OmittedResults: 50, Results: []integrity.ValidationResult{{BucketName: "bucket", ObjectKey: "listed"}}}})
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "<dd>150</dd>")
	require.Contains(t, rec.Body.String(), "List truncated: 50 further failed objects")
}
func TestAdminValidationLimitsAreBounded(t *testing.T) {
	require.True(t, adminValidationLimits.FailuresOnly)
	require.Positive(t, adminValidationLimits.MaxResults)
	require.Positive(t, adminValidationLimits.MaxFailuresPerResult)
	require.Positive(t, adminValidationLimits.MaxErrorLength)
}
func waitAdminJob(t *testing.T, job *adminJob) adminJobState {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		s := job.snapshot()
		if !s.Running {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("job did not finish")
	return adminJobState{}
}
func TestAdminJobExclusionCancellationAndLastReport(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	job := &adminJob{}
	entered := make(chan struct{})
	require.True(t, job.start(ctx, func(ctx context.Context, progress func(int)) (*integrity.ValidationReport, error) {
		progress(3)
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}))
	<-entered
	require.Equal(t, 3, job.snapshot().Processed)
	require.False(t, job.start(ctx, func(context.Context, func(int)) (*integrity.ValidationReport, error) {
		t.Error("second job started")
		return nil, nil
	}))
	cancel()
	state := waitAdminJob(t, job)
	require.False(t, state.Running)
	require.Contains(t, state.Message, "Cancelled")
	require.False(t, job.start(ctx, nil))
	report := &integrity.ValidationReport{TotalObjects: 2, SuccessfulObjects: 2}
	require.True(t, job.start(t.Context(), func(context.Context, func(int)) (*integrity.ValidationReport, error) { return report, nil }))
	require.Same(t, report, waitAdminJob(t, job).Report)
	release := make(chan struct{})
	require.True(t, job.start(t.Context(), func(context.Context, func(int)) (*integrity.ValidationReport, error) { <-release; return nil, nil }))
	require.Same(t, report, job.snapshot().Report)
	close(release)
	require.Same(t, report, waitAdminJob(t, job).Report)
}
func TestStorageValidationRejectsNoDatabaseAndDeleteRoute(t *testing.T) {
	a := &App{options: Options{Context: t.Context()}}
	mux := http.NewServeMux()
	a.registerStorage(mux)
	for _, target := range []string{"/storage/validate", "/storage/validate?delete=true&force=true"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, target, strings.NewReader("")))
		require.Equal(t, http.StatusUnprocessableEntity, rec.Code)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/storage/validate", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
