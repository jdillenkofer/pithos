package gdrive

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/oklog/ulid/v2"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
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

func putPartInTx(t *testing.T, db database.Database, store partstore.PartStore, partId partstore.PartId, content []byte) {
	t.Helper()
	err := database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		return store.PutPart(ctx, tx, partId, ioutils.NewByteReadSeekCloser(content))
	})
	assert.Nil(t, err)
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

var errForcedRollback = errors.New("forced rollback")

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

func TestGoogleDrivePartStorePutRollbackLeavesNoPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)

	err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		err := store.PutPart(ctx, tx, *partId, ioutils.NewByteReadSeekCloser([]byte("content")))
		assert.Nil(t, err)
		return errForcedRollback
	})
	assert.ErrorIs(t, err, errForcedRollback)

	_, err = readPart(t, store, *partId)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
	assert.Equal(t, 1, fakeServer.fileCount())
}

func TestGoogleDrivePartStoreOverwriteRollbackKeepsOldContent(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	putPartInTx(t, db, store, *partId, []byte("old content"))

	err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		err := store.PutPart(ctx, tx, *partId, ioutils.NewByteReadSeekCloser([]byte("new content")))
		assert.Nil(t, err)
		return errForcedRollback
	})
	assert.ErrorIs(t, err, errForcedRollback)

	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("old content"), content)
	// Part folder + the old part file.
	assert.Equal(t, 2, fakeServer.fileCount())
}

func TestGoogleDrivePartStoreDeleteRollbackKeepsPart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	putPartInTx(t, db, store, *partId, []byte("content"))

	err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		err := store.DeletePart(ctx, tx, *partId)
		assert.Nil(t, err)
		return errForcedRollback
	})
	assert.ErrorIs(t, err, errForcedRollback)

	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("content"), content)
}

func TestGoogleDrivePartStoreGetPartIdsIgnoresTempAndBackupFiles(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	putPartInTx(t, db, store, *partId, []byte("content"))

	partName := store.(*gdrivePartStore).getPartName(*partId)
	folderId := store.(*gdrivePartStore).folderId
	fakeServer.addFile("."+partName+".tmp.01ARZ3NDEKTSV4RRFFQ69G5FAV", "application/octet-stream", []string{folderId}, []byte("temp"))
	fakeServer.addFile(partName+".txbackup.01ARZ3NDEKTSV4RRFFQ69G5FAV", "application/octet-stream", []string{folderId}, []byte("backup"))

	partIds, err := store.GetPartIds(context.Background(), nil)
	assert.Nil(t, err)
	assert.Equal(t, []partstore.PartId{*partId}, partIds)
}

func TestGoogleDrivePartStoreReadsNewestDuplicate(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partId, err := partstore.NewRandomPartId()
	assert.Nil(t, err)
	putPartInTx(t, db, store, *partId, []byte("old content"))

	// Simulate a duplicate left behind by an interrupted overwrite: Drive
	// allows several files with the same name, the newest must win.
	partName := store.(*gdrivePartStore).getPartName(*partId)
	folderId := store.(*gdrivePartStore).folderId
	fakeServer.addFile(partName, "application/octet-stream", []string{folderId}, []byte("new content"))

	content, err := readPart(t, store, *partId)
	assert.Nil(t, err)
	assert.Equal(t, []byte("new content"), content)
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

func TestGoogleDrivePartStoreDeletesManyPartsInOneTx(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)
	db := openTestDb(t)
	store := startTestStore(t, fakeServer)

	partIds := make([]partstore.PartId, 0, 20)
	for range 20 {
		partId, err := partstore.NewRandomPartId()
		assert.Nil(t, err)
		putPartInTx(t, db, store, *partId, []byte("content"))
		partIds = append(partIds, *partId)
	}

	// Rolled back bulk delete keeps every part.
	err := database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		for _, partId := range partIds {
			assert.Nil(t, store.DeletePart(ctx, tx, partId))
		}
		return errForcedRollback
	})
	assert.ErrorIs(t, err, errForcedRollback)
	for _, partId := range partIds {
		content, err := readPart(t, store, partId)
		assert.Nil(t, err)
		assert.Equal(t, []byte("content"), content)
	}

	// Committed bulk delete removes every part and leaves no backups.
	err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
		for _, partId := range partIds {
			assert.Nil(t, store.DeletePart(ctx, tx, partId))
		}
		return nil
	})
	assert.Nil(t, err)
	for _, partId := range partIds {
		_, err := readPart(t, store, partId)
		assert.ErrorIs(t, err, partstore.ErrPartNotFound)
	}
	// Only the part folder remains.
	assert.Equal(t, 1, fakeServer.fileCount())
}

// The dedup path (internal/storage/metadatapart/dedup.go) puts a part and
// deletes it again inside the same transaction.
func TestGoogleDrivePartStorePutThenDeleteSameTx(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, commit := range []bool{true, false} {
		fakeServer := newFakeDriveServer()
		db := openTestDb(t)
		store := startTestStore(t, fakeServer)

		partId, err := partstore.NewRandomPartId()
		assert.Nil(t, err)

		err = database.WithTx(context.Background(), db, &sql.TxOptions{ReadOnly: false}, func(ctx context.Context, tx database.Tx) error {
			assert.Nil(t, store.PutPart(ctx, tx, *partId, ioutils.NewByteReadSeekCloser([]byte("content"))))
			assert.Nil(t, store.DeletePart(ctx, tx, *partId))
			if !commit {
				return errForcedRollback
			}
			return nil
		})
		if commit {
			assert.Nil(t, err)
		} else {
			assert.ErrorIs(t, err, errForcedRollback)
		}

		_, err = readPart(t, store, *partId)
		assert.ErrorIs(t, err, partstore.ErrPartNotFound)
		assert.Equal(t, 1, fakeServer.fileCount())
		fakeServer.Close()
	}
}

func TestGoogleDrivePartStoreSweepsStaleTransientFilesOnStart(t *testing.T) {
	testutils.SkipIfIntegration(t)

	fakeServer := newFakeDriveServer()
	t.Cleanup(fakeServer.Close)

	// Seed the folder and files before the store starts.
	folderId := fakeServer.addFile(testFolderName, "application/vnd.google-apps.folder", nil, nil)
	partName := "0123456789abcdef0123456789abcdef"
	staleUlid := ulid.MustNew(ulid.Timestamp(time.Now().Add(-48*time.Hour)), rand.Reader).String()
	freshUlid := ulid.Make().String()
	fakeServer.addFile(partName, "application/octet-stream", []string{folderId}, []byte("part"))
	fakeServer.addFile("."+partName+".tmp."+staleUlid, "application/octet-stream", []string{folderId}, []byte("stale temp"))
	fakeServer.addFile(partName+".txbackup."+staleUlid, "application/octet-stream", []string{folderId}, []byte("stale backup"))
	fakeServer.addFile("."+partName+".tmp."+freshUlid, "application/octet-stream", []string{folderId}, []byte("fresh temp"))

	startTestStore(t, fakeServer)

	// Folder + part + fresh temp survive; the two stale files are gone.
	assert.Equal(t, 3, fakeServer.fileCount())
}
