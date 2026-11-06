package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// emptyBucketPageSize bounds every listing and DeleteObjects batch of a job.
// Tests lower it to exercise paging.
var emptyBucketPageSize int32 = 1000

// maxEmptyBucketFailures bounds the failures a job retains for display;
// further failures are only counted.
const maxEmptyBucketFailures = 100

type emptyBucketFailure struct {
	Key, Version, Error string
}

type emptyBucketState struct {
	Bucket            string
	Running, Bypass   bool
	Started, Finished time.Time
	// Deleted counts object versions and delete markers.
	Deleted, Failed  int
	Failures         []emptyBucketFailure
	Message, Failure string
	// Remaining reports that objects were still found afterwards,
	// for example because they were written while the job ran.
	Remaining bool
}

// Succeeded reports whether the bucket was empty when the job finished.
func (s emptyBucketState) Succeeded() bool {
	return !s.Running && s.Message == "Completed" && s.Failed == 0 && !s.Remaining
}

func (s emptyBucketState) DurationText() string {
	end := s.Finished
	if s.Running || end.IsZero() {
		end = time.Now()
	}
	return end.Sub(s.Started).Round(time.Second).String()
}

type emptyBucketJob struct {
	mu    sync.Mutex
	state emptyBucketState
}

func (j *emptyBucketJob) snapshot() emptyBucketState {
	j.mu.Lock()
	defer j.mu.Unlock()
	state := j.state
	state.Failures = append([]emptyBucketFailure(nil), state.Failures...)
	return state
}

func (j *emptyBucketJob) fail(failure emptyBucketFailure) {
	j.state.Failed++
	if len(j.state.Failures) < maxEmptyBucketFailures {
		j.state.Failures = append(j.state.Failures, failure)
	}
}

// emptyBucketJobs keeps the latest job per bucket for the process lifetime.
// Entries only exist for buckets that existed when a job was started.
type emptyBucketJobs struct {
	mu   sync.Mutex
	jobs map[string]*emptyBucketJob
}

func (j *emptyBucketJobs) get(bucket string) *emptyBucketJob {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.jobs[bucket]
}

// start registers a running job unless one is already running for bucket.
func (j *emptyBucketJobs) start(bucket string, bypass bool) (*emptyBucketJob, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if job := j.jobs[bucket]; job != nil && job.snapshot().Running {
		return job, false
	}
	if j.jobs == nil {
		j.jobs = map[string]*emptyBucketJob{}
	}
	job := &emptyBucketJob{state: emptyBucketState{Bucket: bucket, Running: true, Bypass: bypass, Started: time.Now(), Message: "Running"}}
	j.jobs[bucket] = job
	return job, true
}

type emptyBucketView struct {
	CSRF, Name, Owner, Confirmation string
}

func (a *App) registerEmptyBucket(mux *http.ServeMux) {
	mux.HandleFunc("GET /buckets/empty", a.confirmEmptyBucket)
	mux.HandleFunc("POST /buckets/empty", a.startEmptyBucket)
	mux.HandleFunc("GET /buckets/empty/status", a.emptyBucketStatus)
	mux.HandleFunc("GET /buckets/empty/progress", a.emptyBucketProgress)
}

func (a *App) confirmEmptyBucket(w http.ResponseWriter, r *http.Request) {
	name, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Select a bucket to empty")
		return
	}
	bucket, err := a.options.Storage.HeadBucket(r.Context(), name)
	if err != nil {
		a.bucketError(w, r, err)
		return
	}
	if job := a.emptyJobs.get(name.String()); job != nil && job.snapshot().Running {
		http.Redirect(w, r, emptyBucketStatusURL(name.String()), http.StatusSeeOther)
		return
	}
	a.render(w, r, "Empty bucket", "bucket_empty", emptyBucketView{CSRF: csrfToken(r), Name: name.String(), Owner: bucket.OwnerAccountID, Confirmation: permanentDeleteConfirmation})
}

func emptyBucketStatusURL(bucket string) string {
	return "/buckets/empty/status?" + url.Values{"bucket": {bucket}}.Encode()
}

func (a *App) startEmptyBucket(w http.ResponseWriter, r *http.Request) {
	if !a.bucketForm(w, r) {
		return
	}
	name, err := storage.NewBucketName(r.PostForm.Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return
	}
	if strings.TrimSpace(r.PostForm.Get("confirm_text")) != permanentDeleteConfirmation {
		a.fail(w, r, 400, "Type \""+permanentDeleteConfirmation+"\" to confirm")
		return
	}
	if _, err := a.options.Storage.HeadBucket(r.Context(), name); err != nil {
		a.bucketError(w, r, err)
		return
	}
	bypass := r.PostForm.Get("bypass_governance") == "yes"
	job, ok := a.emptyJobs.start(name.String(), bypass)
	if !ok {
		http.Redirect(w, r, emptyBucketStatusURL(name.String()), http.StatusSeeOther)
		return
	}
	parent := a.options.Context
	if parent == nil {
		parent = context.Background()
	}
	// Jobs have no time limit; they end with the application's lifetime.
	run := func() { runEmptyBucket(parent, a.options.Storage, name, job) }
	if !a.workers.goRun(run) {
		job.mu.Lock()
		job.state.Running, job.state.Finished = false, time.Now()
		job.state.Message, job.state.Failure = "Cancelled", "The server is shutting down."
		job.mu.Unlock()
		a.fail(w, r, http.StatusServiceUnavailable, "Shutdown has begun")
		return
	}
	// Only allowlisted global admins reach this route, including the explicit
	// governance bypass capability. Compliance retention is never bypassed.
	a.audit(r, "AdminEmptyBucket")
	if bypass {
		a.audit(r, "AdminBypassGovernanceRetention")
	}
	http.Redirect(w, r, emptyBucketStatusURL(name.String()), http.StatusSeeOther)
}

// emptyBucketJobFor resolves the job named by the bucket query parameter.
func (a *App) emptyBucketJobFor(w http.ResponseWriter, r *http.Request) (*emptyBucketJob, bool) {
	name, err := storage.NewBucketName(r.URL.Query().Get("bucket"))
	if err != nil {
		a.fail(w, r, 400, "Invalid bucket name")
		return nil, false
	}
	job := a.emptyJobs.get(name.String())
	if job == nil {
		a.fail(w, r, http.StatusNotFound, "This bucket has not been emptied since the server started")
		return nil, false
	}
	return job, true
}

func (a *App) emptyBucketStatus(w http.ResponseWriter, r *http.Request) {
	if job, ok := a.emptyBucketJobFor(w, r); ok {
		state := job.snapshot()
		a.render(w, r, "Empty bucket", "bucket_empty_status", state)
	}
}

func (a *App) emptyBucketProgress(w http.ResponseWriter, r *http.Request) {
	if job, ok := a.emptyBucketJobFor(w, r); ok {
		renderAdminProgress(w, "bucket_empty_progress", job.snapshot())
	}
}

// runEmptyBucket permanently deletes every object version and delete marker.
// Incomplete multipart uploads are left alone; DeleteBucket aborts them.
func runEmptyBucket(ctx context.Context, st storage.Storage, bucket storage.BucketName, job *emptyBucketJob) {
	err := emptyBucketContents(ctx, st, bucket, job)
	remaining := false
	if err == nil {
		remaining, err = bucketHasContents(ctx, st, bucket)
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	job.state.Running, job.state.Finished, job.state.Remaining = false, time.Now(), remaining
	job.state.Message = "Completed"
	switch {
	case ctx.Err() != nil:
		job.state.Message, job.state.Failure = "Cancelled", "The server is shutting down."
	case errors.Is(err, storage.ErrNoSuchBucket):
		job.state.Message, job.state.Failure = "Failed", "The bucket no longer exists."
	case err != nil:
		job.state.Message, job.state.Failure = "Failed", "Consult the server logs for details."
		slog.ErrorContext(ctx, "Admin job failed", "error", err)
	}
}

func emptyBucketContents(ctx context.Context, st storage.Storage, bucket storage.BucketName, job *emptyBucketJob) error {
	bypass := job.snapshot().Bypass
	options := storage.ListObjectVersionsOptions{MaxKeys: emptyBucketPageSize}
	for {
		page, err := st.ListObjectVersions(ctx, bucket, options)
		if err != nil {
			return err
		}
		if len(page.Versions) > 0 {
			entries := make([]storage.DeleteObjectsInputEntry, 0, len(page.Versions))
			for _, version := range page.Versions {
				id := version.VersionID
				entries = append(entries, storage.DeleteObjectsInputEntry{Key: version.Key, VersionID: &id, BypassGovernanceRetention: bypass})
			}
			result, err := st.DeleteObjects(ctx, bucket, entries)
			if err != nil {
				return err
			}
			job.mu.Lock()
			for _, entry := range result.Entries {
				if entry.Deleted {
					job.state.Deleted++
					continue
				}
				failure := emptyBucketFailure{Key: entry.Key.String(), Error: "Object deletion failed"}
				if entry.VersionID != nil {
					failure.Version = *entry.VersionID
				}
				if entry.ErrCode == "AccessDenied" {
					failure.Error = "Object retention or legal hold prevents deletion"
				}
				job.fail(failure)
			}
			job.mu.Unlock()
		}
		// Deleted entries sort before the markers, so paging stays stable.
		if !page.IsTruncated || page.NextKeyMarker == nil {
			break
		}
		options.KeyMarker, options.VersionIDMarker = page.NextKeyMarker, page.NextVersionIDMarker
	}
	return nil
}

func bucketHasContents(ctx context.Context, st storage.Storage, bucket storage.BucketName) (bool, error) {
	versions, err := st.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 1})
	if err != nil {
		return false, err
	}
	return len(versions.Versions) > 0, nil
}
