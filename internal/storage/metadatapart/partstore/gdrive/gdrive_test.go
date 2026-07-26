package gdrive

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const testFolderName = "pithos-parts"

func openTestDb(t *testing.T) database.Database {
	t.Helper()
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	assert.Nil(t, err)
	db, err := sqlite.OpenDatabase(filepath.Join(storagePath, "pithos.db"))
	assert.Nil(t, err)
	t.Cleanup(func() {
		assert.Nil(t, db.Close())
		assert.Nil(t, os.RemoveAll(storagePath))
	})
	return db
}

func newTestStore(t *testing.T, fakeServer *fakeDriveServer) partstore.PartStore {
	t.Helper()
	store, err := New(testFolderName, option.WithEndpoint(fakeServer.URL()), option.WithoutAuthentication())
	assert.Nil(t, err)
	return store
}

func startTestStore(t *testing.T, fakeServer *fakeDriveServer) partstore.PartStore {
	t.Helper()
	store := newTestStore(t, fakeServer)
	ctx := context.Background()
	assert.Nil(t, store.Start(ctx))
	t.Cleanup(func() {
		assert.Nil(t, store.Stop(context.Background()))
	})
	return store
}

func readPart(t *testing.T, store partstore.PartStore, partId partstore.PartId) ([]byte, error) {
	t.Helper()
	reader, err := store.GetPart(context.Background(), nil, partId)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func TestGoogleDrivePartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)

	store := newTestStore(t, fakeServer)
	err := partstore.Tester(store, db, []byte("GoogleDrivePartStore"))
	assert.Nil(t, err)

	// Only the part folder itself may remain: no temp, backup or part files.
	assert.Equal(t, 1, fakeServer.fileCount())
}

func TestGoogleDrivePartStorePutPartAndDeletePartWorkWithoutTx(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)

	err = store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("content")))
	assert.Nil(t, err)
	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("content"), content)

	err = store.DeletePart(context.Background(), nil, *partId)
	assert.Nil(t, err)
	_, err = readPart(t, store, *partId)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestGoogleDrivePartStoreReconcilesAmbiguousFolderCreate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	fakeServer.failNextFolderCreateAfterCommitOnce()
	store := newTestStore(t, fakeServer)

	assert.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		assert.NoError(t, store.Stop(context.Background()))
	})

	assert.NotEmpty(t, store.(*gdrivePartStore).folderId)
	assert.Equal(t, 1, fakeServer.fileCount())
}

func TestGoogleDrivePartStoreDisablesSDKUploadRetries(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)
	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	content := io.LimitReader(
		ioutils.NewRepeatingReader([]byte("x")),
		int64(googleapi.DefaultUploadChunkSize+1),
	)

	// The fake only accepts a single multipart upload. Without ChunkSize(0),
	// the SDK turns this payload into a resumable upload with chunk retries.
	assert.NoError(t, store.PutPart(context.Background(), nil, *partId, content))
}

func TestGoogleDrivePartStoreGetPartIdsIgnoresTempAndBackupFiles(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("content"))))

	partName := store.(*gdrivePartStore).getPartName(*partId)
	folderId := store.(*gdrivePartStore).folderId
	fakeServer.addFile("."+partName+".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAV", "application/octet-stream", []string{folderId}, []byte("temp"))
	fakeServer.addFile(partName+".txbackup.01ARZ3NDEKTSV4RRFFQ69G5FAV", "application/octet-stream", []string{folderId}, []byte("backup"))

	partIds, err := store.GetPartIds(context.Background(), nil)
	assert.Nil(t, err)
	assert.Equal(t, []partstore.PartId{*partId}, partIds)
}

func TestGoogleDrivePartStorePutPartReusesExistingFile(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("old content"))))

	partName := store.(*gdrivePartStore).getPartName(*partId)
	cached, ok := store.(*gdrivePartStore).fileIdCache.Load(partName)
	assert.True(t, ok)
	originalFileID := cached.(string)

	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("new content"))))

	cached, ok = store.(*gdrivePartStore).fileIdCache.Load(partName)
	assert.True(t, ok)
	assert.Equal(t, originalFileID, cached.(string))

	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new content"), content)
	assert.Equal(t, 2, fakeServer.fileCount())
}

// TestGoogleDrivePartStoreAgainstRealDrive runs the conformance suite against
// the real Google Drive API. It is skipped unless PITHOS_TEST_GDRIVE_CLIENT_ID,
// PITHOS_TEST_GDRIVE_CLIENT_SECRET and PITHOS_TEST_GDRIVE_TOKEN (token JSON as
// printed by `pithos gdrive-auth`) are set.
func TestGoogleDrivePartStoreAgainstRealDrive(t *testing.T) {
	clientId := os.Getenv("PITHOS_TEST_GDRIVE_CLIENT_ID")
	clientSecret := os.Getenv("PITHOS_TEST_GDRIVE_CLIENT_SECRET")
	tokenJson := os.Getenv("PITHOS_TEST_GDRIVE_TOKEN")
	if clientId == "" || clientSecret == "" || tokenJson == "" {
		t.Skip("PITHOS_TEST_GDRIVE_CLIENT_ID, PITHOS_TEST_GDRIVE_CLIENT_SECRET and PITHOS_TEST_GDRIVE_TOKEN are not set")
	}

	var token oauth2.Token
	assert.Nil(t, json.Unmarshal([]byte(tokenJson), &token))
	oauthConfig := &oauth2.Config{
		ClientID:     clientId,
		ClientSecret: clientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{Scope},
	}
	tokenSource := oauthConfig.TokenSource(context.Background(), &token)

	db := openTestDb(t)
	store, err := New("pithos-parts-integration-test", option.WithTokenSource(tokenSource))
	assert.Nil(t, err)
	err = partstore.Tester(store, db, []byte("GoogleDrivePartStore"))
	assert.Nil(t, err)
}

func TestGoogleDrivePartStoreDeletesManyParts(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partIds := make([]partstore.PartId, 0, 20)
	for range 20 {
		partId, err := partstore.NewRandomPartId()
		assert.Nil(t, err)
		assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("content"))))
		partIds = append(partIds, *partId)
	}

	for _, partId := range partIds {
		assert.Nil(t, store.DeletePart(context.Background(), nil, partId))
	}
	for _, partId := range partIds {
		_, err := readPart(t, store, partId)
		assert.ErrorIs(t, err, partstore.ErrPartNotFound)
	}
	assert.Equal(t, 1, fakeServer.fileCount())
}

// The dedup path (internal/storage/metadatapart/dedup.go) puts a part and
// deletes it again inside the same operation sequence.
func TestGoogleDrivePartStorePutThenDeleteSameFlow(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)

	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("content"))))
	assert.Nil(t, store.DeletePart(context.Background(), nil, *partId))

	_, err = readPart(t, store, *partId)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
	assert.Equal(t, 1, fakeServer.fileCount())
}

// The outbox worker replays PutPart without a transaction and may retry after
// a failure; retries must keep using the same part file.
func TestGoogleDrivePartStoreTxFreePutIsIdempotent(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)

	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("first attempt"))))
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("second attempt"))))

	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("second attempt"), content)
	// Part folder + exactly one part file.
	assert.Equal(t, 2, fakeServer.fileCount())
}

func TestGoogleDrivePartStoreDoesNotRetryAmbiguousUpload(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)

	// Drive commits the file but returns a transient error. PutPart must not
	// replay the already-consumed reader internally.
	fakeServer.failNextUploadAfterCommitOnce()
	err = store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("first attempt")))
	assert.Error(t, err)
	assert.Equal(t, 2, fakeServer.fileCount())

	// A caller/outbox retry supplies a fresh reader and updates the committed
	// file instead of creating a duplicate.
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("second attempt"))))
	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("second attempt"), content)
	assert.Equal(t, 2, fakeServer.fileCount())
}

func TestGoogleDrivePartStoreDeletePartFindsDuplicatesAcrossPages(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)
	fakeServer.setMaxPageSize(2)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	partName := store.(*gdrivePartStore).getPartName(*partId)
	folderId := store.(*gdrivePartStore).folderId
	for range 5 {
		fakeServer.addFile(partName, "application/octet-stream", []string{folderId}, []byte("duplicate"))
	}

	assert.Nil(t, store.DeletePart(context.Background(), nil, *partId))
	assert.Equal(t, 1, fakeServer.fileCount())
}

// Ranged object reads skip into the middle of a part via Seek; the store must
// serve that with an HTTP Range request instead of downloading the part head.
func TestGoogleDrivePartStoreReaderSeeks(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	assert.Nil(t, store.PutPart(context.Background(), nil, *partId, ioutils.NewByteReadSeekCloser([]byte("0123456789"))))

	reader, err := store.GetPart(context.Background(), nil, *partId)
	assert.Nil(t, err)
	defer reader.Close()
	seeker, ok := reader.(io.ReadSeekCloser)
	assert.True(t, ok)

	// Forward seek from the start.
	_, err = seeker.Seek(4, io.SeekStart)
	assert.Nil(t, err)
	content, err := io.ReadAll(seeker)
	assert.Nil(t, err)
	assert.Equal(t, []byte("456789"), content)

	// Suffix read via SeekEnd (fetches the size lazily).
	_, err = seeker.Seek(-3, io.SeekEnd)
	assert.Nil(t, err)
	content, err = io.ReadAll(seeker)
	assert.Nil(t, err)
	assert.Equal(t, []byte("789"), content)

	// Seeking past EOF reads as empty, like a file.
	_, err = seeker.Seek(100, io.SeekStart)
	assert.Nil(t, err)
	content, err = io.ReadAll(seeker)
	assert.Nil(t, err)
	assert.Empty(t, content)
}
