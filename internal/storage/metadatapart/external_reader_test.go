package metadatapart

import (
	"context"
	"database/sql"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	httpserver "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	repositoryfactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

// Deletion removes metadata immediately, but physical content and zero-reference
// registry rows must survive for existing bounded readers.
func assertRetainedUnreferencedParts(t *testing.T, st *metadataPartStorage) {
	t.Helper()
	require.NotEmpty(t, physicalPartIDs(t, st))
	require.NoError(t, database.WithTx(context.Background(), st.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var live, invalidState int
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM parts").Scan(&live))
		require.Zero(t, live)
		require.NoError(t, tx.SqlTx().QueryRowContext(ctx, "SELECT COUNT(*) FROM part_registry WHERE ref_count != 0 OR unreferenced_at IS NOT NULL").Scan(&invalidState))
		require.Zero(t, invalidState)
		return nil
	}))
}

func TestExternalGetShortTransactionLazyDeleteAndSharedDeadline(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, mode := range []string{"external", "mixed", "capabilityless"} {
		t.Run(mode, func(t *testing.T) { testExternalGet(t, mode) })
	}
}

// Embedding the interface deliberately hides optional capabilities. Such a
// store is not evidence of SQL snapshot protection and must still be bounded.
type capabilitylessExternalStore struct{ partstore.PartStore }

func testExternalGet(t *testing.T, mode string) {
	st, cleanup := newTestStorage(t)
	defer cleanup()
	ctx := context.Background()
	fs, err := filesystem.New(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, fs.Start(ctx))
	defer fs.Stop(ctx)
	original := st.partStores
	defer func() { st.partStores = original }()
	var extra map[string]partstore.PartStore
	if mode == "mixed" {
		extra = map[string]partstore.PartStore{"sql": original.Default()}
	}
	var external partstore.PartStore = fs
	if mode == "capabilityless" {
		external = &capabilitylessExternalStore{PartStore: fs}
	}
	st.partStores, err = partstore.NewNamedPartStores(external, extra, nil)
	require.NoError(t, err)
	bucket, key := storage.MustNewBucketName("bucket"), storage.MustNewObjectKey("key")
	require.NoError(t, st.CreateBucket(ctx, bucket))
	_, err = st.PutObject(ctx, bucket, key, nil, ioutils.NewByteReadSeekCloser([]byte("abcdef")), nil, nil)
	require.NoError(t, err)
	zero, two, four, six := int64(0), int64(2), int64(4), int64(6)
	// Exercise the assembled production HTTP stack, including metrics and
	// request-context response wrappers, over a real connection.
	authorizer, err := lua.NewLuaAuthorizer(`function authorizeRequest(request) return true end`)
	require.NoError(t, err)
	errorKey := "key"
	require.NoError(t, st.PutBucketWebsiteConfiguration(ctx, bucket, &storage.WebsiteConfiguration{IndexDocumentSuffix: "index.html", ErrorDocumentKey: &errorKey}))
	handler := httpserver.SetupServer(nil, "region", "api.localhost", "website.localhost", authorizer, st)
	httpServer := httptest.NewServer(handler)
	defer httpServer.Close()
	req, err := http.NewRequest(http.MethodGet, httpServer.URL+"/bucket/key", nil)
	require.NoError(t, err)
	req.Host = "api.localhost"
	response, err := httpServer.Client().Do(req)
	require.NoError(t, err)
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode, string(data))
	require.Equal(t, "abcdef", string(data))
	// A genuinely unsupported transport must fail before exposing body bytes.
	recorder := httptest.NewRecorder()
	unsupported := httptest.NewRequest(http.MethodGet, "http://api.localhost/bucket/key", nil)
	handler.ServeHTTP(recorder, unsupported)
	require.Equal(t, http.StatusInternalServerError, recorder.Code)
	require.NotContains(t, recorder.Body.String(), "abcdef")
	for _, path := range []string{"/bucket/key", "/bucket/missing"} {
		websitePath := strings.TrimPrefix(path, "/bucket")
		websiteRequest, err := http.NewRequest(http.MethodGet, httpServer.URL+websitePath, nil)
		require.NoError(t, err)
		websiteRequest.Host = "bucket.website.localhost"
		websiteResponse, err := httpServer.Client().Do(websiteRequest)
		require.NoError(t, err)
		body, err := io.ReadAll(websiteResponse.Body)
		require.NoError(t, err)
		require.NoError(t, websiteResponse.Body.Close())
		wantStatus := http.StatusOK
		if path == "/bucket/missing" {
			wantStatus = http.StatusNotFound
		}
		require.Equal(t, wantStatus, websiteResponse.StatusCode)
		require.Equal(t, "abcdef", string(body))
		unsupported := httptest.NewRequest(http.MethodGet, "http://bucket.website.localhost"+websitePath, nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, unsupported)
		require.Equal(t, http.StatusInternalServerError, recorder.Code)
		require.NotContains(t, recorder.Body.String(), "abcdef")
	}

	// No part is opened until Read; both ranges share exactly one deadline.
	_, readers, err := st.GetObject(ctx, bucket, key, []storage.ByteRange{{Start: &zero, End: &two}, {Start: &four, End: &six}}, nil)
	require.NoError(t, err)
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	first := readers[0].(interface{ Deadline() time.Time }).Deadline()
	require.Equal(t, first, readers[1].(interface{ Deadline() time.Time }).Deadline())
	// Keep another manifest completely lazy until after retention and physical
	// deletion. Mixed stores must not silently omit its deadline protection.
	st.readerLifetime = 50 * time.Millisecond
	_, expiring, err := st.GetObject(ctx, bucket, key, nil, nil)
	require.NoError(t, err)
	defer expiring[0].Close()
	expires := expiring[0].(interface{ Deadline() time.Time }).Deadline()

	// A metadata writer can complete while the lazy external readers are idle.
	_, err = st.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	for i, want := range []string{"ab", "ef"} {
		data, err := io.ReadAll(readers[i])
		require.NoError(t, err)
		require.Equal(t, want, string(data))
		require.NoError(t, readers[i].Close())
		require.NoError(t, readers[i].Close()) // Must not cancel the other ranges twice.
	}
	<-time.After(time.Until(expires))
	repo, err := repositoryfactory.NewPartRegistryRepository(st.db)
	require.NoError(t, err)
	ids := physicalPartIDs(t, st)
	require.NotEmpty(t, ids)
	for _, id := range ids {
		require.NoError(t, database.WithTx(ctx, st.db, nil, func(ctx context.Context, tx database.Tx) error {
			ok, err := repo.Condemn(ctx, tx.SqlTx(), id, time.Now().UTC().Add(-time.Hour))
			require.NoError(t, err)
			require.False(t, ok)
			_, err = tx.SqlTx().ExecContext(ctx, "UPDATE part_registry SET unreferenced_at=$1 WHERE part_id=$2", time.Now().UTC().Add(-2*time.Hour), id.String())
			require.NoError(t, err)
			ok, err = repo.Condemn(ctx, tx.SqlTx(), id, time.Now().UTC().Add(-time.Hour))
			require.NoError(t, err)
			require.True(t, ok)
			return nil
		}))
		require.NoError(t, fs.DeletePart(ctx, nil, id))
	}
	_, err = expiring[0].Read(make([]byte, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded, "late lazy read must not depend on deleted backend content")

	// Reusing an older ambient snapshot cannot safely establish a new lifetime.
	require.NoError(t, st.WithTransaction(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, _ storage.Storage) error {
		_, _, err := st.GetObject(ctx, bucket, key, nil, nil)
		require.ErrorContains(t, err, "ambient transaction")
		return nil
	}))
}
