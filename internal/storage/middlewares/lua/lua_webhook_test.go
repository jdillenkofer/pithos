package lua

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/webhook"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const webhookTestOutboxId = "default"

type recordedRequest struct {
	method  string
	headers http.Header
	body    string
}

func newWebhookTestServer(t *testing.T) (*httptest.Server, <-chan recordedRequest) {
	t.Helper()
	received := make(chan recordedRequest, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- recordedRequest{method: r.Method, headers: r.Header.Clone(), body: string(body)}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, received
}

func newWebhookTestStore(t *testing.T, inner storage.Storage, code string) (storage.Storage, database.Database) {
	t.Helper()
	storagePath := t.TempDir()
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	repo, err := repositoryFactory.NewWebhookOutboxEntryRepository(db)
	require.NoError(t, err)
	dispatcher, err := webhook.NewDispatcher(db, webhookTestOutboxId, repo, prometheus.NewRegistry(), 0, 0)
	require.NoError(t, err)

	store, err := NewStorageMiddlewareWithWebhooks(inner, code, db, dispatcher)
	require.NoError(t, err)

	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { _ = store.Stop(context.Background()) })
	return store, db
}

func countWebhookEntries(t *testing.T, db database.Database) int {
	t.Helper()
	repo, err := repositoryFactory.NewWebhookOutboxEntryRepository(db)
	require.NoError(t, err)
	var count int
	err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var e error
		count, e = repo.Count(ctx, tx.SqlTx(), webhookTestOutboxId)
		return e
	})
	require.NoError(t, err)
	return count
}

func TestWebhookEnqueuedAndDeliveredAfterCommit(t *testing.T) {
	testutils.SkipIfIntegration(t)
	server, received := newWebhookTestServer(t)
	inner := &testStorage{}
	code := fmt.Sprintf(`
function PutObject(ctx, bucket, key, contentType, data, checksumInput, opts)
  local result, err = innerStorage.PutObject(ctx, bucket, key, contentType, data, checksumInput, opts)
  if err ~= nil then return nil, err end
  webhooks.enqueue(ctx, {
    url = %q,
    method = "POST",
    headers = { ["X-Test"] = "yes" },
    body = "event:" .. bucket .. "/" .. key,
  })
  return result, nil
end
`, server.URL)
	store, db := newWebhookTestStore(t, inner, code)

	_, err := store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("obj"), nil, strings.NewReader("payload"), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "payload", inner.putContent)

	select {
	case req := <-received:
		assert.Equal(t, http.MethodPost, req.method)
		assert.Equal(t, "yes", req.headers.Get("X-Test"))
		assert.Equal(t, "event:bucket/obj", req.body)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for webhook delivery")
	}

	require.Eventually(t, func() bool {
		return countWebhookEntries(t, db) == 0
	}, 5*time.Second, 50*time.Millisecond, "webhook outbox entry should be deleted after delivery")
}

func TestWebhookRolledBackWhenOperationFails(t *testing.T) {
	testutils.SkipIfIntegration(t)
	server, received := newWebhookTestServer(t)
	inner := &testStorage{}
	code := fmt.Sprintf(`
function PutObject(ctx, bucket, key, contentType, data, checksumInput, opts)
  webhooks.enqueue(ctx, {
    url = %q,
    method = "POST",
    body = "should-not-send",
  })
  return nil, "NoSuchBucket"
end
`, server.URL)
	store, db := newWebhookTestStore(t, inner, code)

	_, err := store.PutObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("obj"), nil, strings.NewReader("payload"), nil, nil)
	require.ErrorIs(t, err, storage.ErrNoSuchBucket)

	assert.Equal(t, 0, countWebhookEntries(t, db), "rolled back webhook must not be persisted")

	select {
	case req := <-received:
		t.Fatalf("webhook was delivered despite rollback: %q", req.body)
	case <-time.After(1500 * time.Millisecond):
		// no delivery expected
	}
}

func TestWebhookEnqueueOutsideTransactionFails(t *testing.T) {
	testutils.SkipIfIntegration(t)
	server, _ := newWebhookTestServer(t)
	inner := &testStorage{}
	// HeadObject is not a mutating method, so it is not wrapped in a transaction;
	// enqueue must therefore fail.
	code := fmt.Sprintf(`
function HeadObject(ctx, bucket, key, opts)
  webhooks.enqueue(ctx, { url = %q, method = "POST", body = "x" })
  return nil, nil
end
`, server.URL)
	store, _ := newWebhookTestStore(t, inner, code)

	_, err := store.HeadObject(context.Background(), storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("obj"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be called inside a mutating operation")
}
