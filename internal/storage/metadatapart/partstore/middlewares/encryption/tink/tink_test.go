package tink

import (
	"crypto/mlkem"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestTinkEncryptionPartStoreWithLocalKMS(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()
	filesystemPartStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	// Test with LocalKMS variant
	tinkEncryptionPartStoreMiddleware, err := NewWithLocalKMS("test-password", filesystemPartStore, nil)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TinkEncryptionPartStoreMiddleware: %s", err))
		os.Exit(1)
	}

	content := []byte("TinkEncryptionPartStoreMiddleware with LocalKMS")
	err = partstore.Tester(tinkEncryptionPartStoreMiddleware, db, content)
	assert.Nil(t, err)
}

func TestTinkEncryptionPartStoreWithLocalKMS_DifferentPasswords(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-data-")
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create temp directory: %s", err))
		os.Exit(1)
	}
	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		slog.Error("Couldn't open database")
		os.Exit(1)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			slog.Error(fmt.Sprintf("Could not close database %s", err))
			os.Exit(1)
		}
		err = os.RemoveAll(storagePath)
		if err != nil {
			slog.Error(fmt.Sprintf("Could not remove storagePath %s: %s", storagePath, err))
			os.Exit(1)
		}
	}()
	filesystemPartStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create FilesystemPartStore: %s", err))
		os.Exit(1)
	}

	// Test that different passwords create different encryption results
	middleware1, err := NewWithLocalKMS("password1", filesystemPartStore, nil)
	assert.Nil(t, err)

	middleware2, err := NewWithLocalKMS("password2", filesystemPartStore, nil)
	assert.Nil(t, err)

	// Ensure they are different instances with different master keys
	assert.NotEqual(t, middleware1, middleware2)
}

func TestTinkEncryptionPartStoreWithMLKEM(t *testing.T) {
	testutils.SkipIfIntegration(t)
	storagePath, err := os.MkdirTemp("", "pithos-test-pq-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(storagePath)

	dbPath := filepath.Join(storagePath, "pithos.db")
	db, err := sqlite.OpenDatabase(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	filesystemPartStore, err := filesystemPartStore.New(storagePath)
	if err != nil {
		t.Fatal(err)
	}

	// Generate a 64-byte seed for ML-KEM-1024
	seed := make([]byte, 64)
	for i := range seed {
		seed[i] = byte(i)
	}

	mlkemKey, err := mlkem.NewDecapsulationKey1024(seed)
	if err != nil {
		t.Fatal(err)
	}

	// Test with ML-KEM hybrid encryption
	middleware, err := NewWithLocalKMS("test-password", filesystemPartStore, mlkemKey)
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("TinkEncryptionPartStoreMiddleware with ML-KEM")
	err = partstore.Tester(middleware, db, content)
	assert.Nil(t, err)
}
