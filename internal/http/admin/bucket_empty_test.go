package admin

import (
	"bytes"
	"context"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlMetadataStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	sqlPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/sql"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func newEmptyBucketTestStorage(t *testing.T) storage.Storage {
	t.Helper()
	testutils.SkipIfIntegration(t)
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "pithos.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	partContentRepository, err := repositoryFactory.NewPartContentRepository(db)
	require.NoError(t, err)
	partStore, err := sqlPartStore.New(db, partContentRepository)
	require.NoError(t, err)
	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	require.NoError(t, err)
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	require.NoError(t, err)
	partRepository, err := repositoryFactory.NewPartRepository(db)
	require.NoError(t, err)
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	require.NoError(t, err)
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	require.NoError(t, err)
	metadataStore, err := sqlMetadataStore.New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	require.NoError(t, err)
	st, err := metadatapart.NewStorage(db, metadataStore, partStore)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, st.Start(ctx))
	t.Cleanup(func() { st.Stop(ctx) })
	return st
}

// emptyBucketRun starts a job through the console and waits for it to finish.
func emptyBucketRun(t *testing.T, st storage.Storage, bucket string, bypass bool) (*App, emptyBucketState) {
	t.Helper()
	a := bucketTestApp(t, &bucketTestStorage{})
	a.options = Options{Storage: st, Context: context.Background()}
	form := url.Values{"bucket": {bucket}, "confirm_text": {permanentDeleteConfirmation}}
	if bypass {
		form.Set("bypass_governance", "yes")
	}
	w := bucketRequest(a, "POST", "/buckets/empty", form)
	require.Equal(t, 303, w.Code, w.Body.String())
	require.Equal(t, "/buckets/empty/status?bucket="+bucket, w.Header().Get("Location"))
	a.workers.wait()
	return a, a.emptyJobs.get(bucket).snapshot()
}

func TestEmptyBucketRemovesVersionsAndMarkers(t *testing.T) {
	previous := emptyBucketPageSize
	emptyBucketPageSize = 2
	t.Cleanup(func() { emptyBucketPageSize = previous })
	st := newEmptyBucketTestStorage(t)
	ctx := context.Background()
	bucket := storage.MustNewBucketName("locked-bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{OwnerAccountID: "account-a", ObjectLockEnabled: true}))
	put := func(key string) {
		_, err := st.PutObject(ctx, bucket, storage.MustNewObjectKey(key), nil, bytes.NewReader([]byte(key)), nil, nil)
		require.NoError(t, err)
	}
	put("a.txt")
	put("a.txt")
	put("nested/b.txt")
	_, err := st.DeleteObject(ctx, bucket, storage.MustNewObjectKey("nested/b.txt"), nil)
	require.NoError(t, err)
	put("locked.txt")
	require.NoError(t, st.PutObjectRetention(ctx, bucket, storage.MustNewObjectKey("locked.txt"), &storage.ObjectRetention{Mode: storage.RetentionModeGovernance, RetainUntilDate: time.Now().Add(time.Hour)}, nil))
	// Incomplete uploads are left to DeleteBucket, which aborts them.
	_, err = st.CreateMultipartUpload(ctx, bucket, storage.MustNewObjectKey("upload.bin"), nil, nil, nil)
	require.NoError(t, err)

	a, state := emptyBucketRun(t, st, bucket.String(), false)
	require.Equal(t, "Completed", state.Message)
	require.Equal(t, 4, state.Deleted, "two versions of a.txt, nested/b.txt and its delete marker")
	require.Equal(t, 1, state.Failed)
	require.True(t, state.Remaining)
	require.False(t, state.Succeeded())
	require.Equal(t, "locked.txt", state.Failures[0].Key)
	require.ErrorIs(t, st.DeleteBucket(ctx, bucket), storage.ErrBucketNotEmpty)
	body := bucketRequest(a, "GET", "/buckets/empty/status?bucket=locked-bucket", nil).Body.String()
	for _, want := range []string{"was not fully emptied", "Object retention or legal hold prevents deletion", `href="/buckets/empty?bucket=locked-bucket"`} {
		require.Contains(t, body, want)
	}
	require.NotContains(t, body, "hx-get", "finished jobs stop polling")

	_, state = emptyBucketRun(t, st, bucket.String(), true)
	require.True(t, state.Succeeded(), "%+v", state)
	require.Equal(t, 1, state.Deleted)
	require.NoError(t, st.DeleteBucket(ctx, bucket))
}

func TestEmptyUnversionedBucket(t *testing.T) {
	st := newEmptyBucketTestStorage(t)
	ctx := context.Background()
	bucket := storage.MustNewBucketName("plain-bucket")
	require.NoError(t, st.CreateBucket(ctx, bucket, storage.CreateBucketOptions{OwnerAccountID: "account-a"}))
	for _, key := range []string{"a", "b/c", "b/d/e"} {
		_, err := st.PutObject(ctx, bucket, storage.MustNewObjectKey(key), nil, bytes.NewReader([]byte(key)), nil, nil)
		require.NoError(t, err)
	}
	a, state := emptyBucketRun(t, st, bucket.String(), false)
	require.True(t, state.Succeeded(), "%+v", state)
	require.Equal(t, 3, state.Deleted)
	body := bucketRequest(a, "GET", "/buckets/empty/status?bucket=plain-bucket", nil).Body.String()
	require.Contains(t, body, "Successfully emptied bucket")
	require.Contains(t, body, `href="/buckets/delete?bucket=plain-bucket"`)
	require.NoError(t, st.DeleteBucket(ctx, bucket))
}

func TestEmptyBucketRequiresConfirmation(t *testing.T) {
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("bucket-b"), OwnerAccountID: "account-b"}}}
	a := bucketTestApp(t, s)
	body := bucketRequest(a, "GET", "/buckets", nil).Body.String()
	require.Contains(t, body, `form="bucket-selection" formaction="/buckets/empty" class="danger-outline"`)
	require.Contains(t, bucketRequest(a, "GET", "/buckets/delete?bucket=bucket-b", nil).Body.String(), `href="/buckets/empty?bucket=bucket-b"`)

	w := bucketRequest(a, "GET", "/buckets/empty?bucket=bucket-b", nil)
	require.Equal(t, 200, w.Code)
	for _, want := range []string{`method="post" action="/buckets/empty"`, `name="csrf_token" value="csrf-value"`, `name="bucket" value="bucket-b"`, `name="confirm_text" required`, `name="bypass_governance"`, "account-b"} {
		require.Contains(t, w.Body.String(), want)
	}
	require.Equal(t, 400, bucketRequest(a, "GET", "/buckets/empty", nil).Code)
	require.Equal(t, 404, bucketRequest(a, "GET", "/buckets/empty?bucket=missing-bucket", nil).Code)
	for _, confirm := range []string{"", "bucket-b", "delete"} {
		w := bucketRequest(a, "POST", "/buckets/empty", url.Values{"bucket": {"bucket-b"}, "confirm_text": {confirm}})
		require.Equal(t, 400, w.Code)
	}
	require.Equal(t, 404, bucketRequest(a, "POST", "/buckets/empty", url.Values{"bucket": {"missing-bucket"}, "confirm_text": {permanentDeleteConfirmation}}).Code)
	require.Nil(t, a.emptyJobs.get("bucket-b"), "no job may start without confirmation")
	require.Equal(t, 404, bucketRequest(a, "GET", "/buckets/empty/status?bucket=bucket-b", nil).Code)
	require.Equal(t, 404, bucketRequest(a, "GET", "/buckets/empty/progress?bucket=bucket-b", nil).Code)
}

func TestEmptyBucketRunsOnceAtATime(t *testing.T) {
	a := bucketTestApp(t, &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("bucket-b")}}})
	job, ok := a.emptyJobs.start("bucket-b", false)
	require.True(t, ok)
	w := bucketRequest(a, "POST", "/buckets/empty", url.Values{"bucket": {"bucket-b"}, "confirm_text": {permanentDeleteConfirmation}})
	require.Equal(t, 303, w.Code)
	require.Same(t, job, a.emptyJobs.get("bucket-b"), "a running job is not replaced")
	require.Equal(t, 303, bucketRequest(a, "GET", "/buckets/empty?bucket=bucket-b", nil).Code, "confirmation redirects to the running job")
	progress := bucketRequest(a, "GET", "/buckets/empty/progress?bucket=bucket-b", nil).Body.String()
	require.True(t, strings.Contains(progress, `hx-get="/buckets/empty/progress?bucket=bucket-b"`) && !strings.Contains(progress, "<html"), progress)
}
