package sql

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	repositoryFactory "github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestValidateCompleteMultipartUploadParts(t *testing.T) {
	testutils.SkipIfIntegration(t)

	checksumCRC32 := "AAAAAA=="
	otherChecksumCRC32 := "BBBBBB=="
	storedParts := []part.Entity{
		{SequenceNumber: 1, ETag: "\"etag-1\"", ChecksumCRC32: &checksumCRC32},
		{SequenceNumber: 2, ETag: "\"etag-2\""},
	}

	tests := []struct {
		name          string
		declaredParts []metadatastore.CompleteMultipartUploadPart
		wantErr       error
	}{
		{
			name: "matching parts",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1", ChecksumCRC32: &checksumCRC32},
				{PartNumber: 2, ETag: "\"etag-2\""},
			},
		},
		{
			name: "wrong etag",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "wrong"},
				{PartNumber: 2, ETag: "\"etag-2\""},
			},
			wantErr: metadatastore.ErrInvalidPart,
		},
		{
			name: "wrong checksum",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1", ChecksumCRC32: &otherChecksumCRC32},
				{PartNumber: 2, ETag: "\"etag-2\""},
			},
			wantErr: metadatastore.ErrInvalidPart,
		},
		{
			name: "missing stored part",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1"},
				{PartNumber: 3, ETag: "\"etag-3\""},
			},
			wantErr: metadatastore.ErrInvalidPart,
		},
		{
			name: "omitted uploaded part",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1"},
			},
			wantErr: metadatastore.ErrInvalidPart,
		},
		{
			name: "out of order parts",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 2, ETag: "\"etag-2\""},
				{PartNumber: 1, ETag: "etag-1"},
			},
			wantErr: metadatastore.ErrInvalidPartOrder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCompleteMultipartUploadParts(tt.declaredParts, storedParts)
			if tt.wantErr == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestSqlMetadataStore(t *testing.T) {
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

	bucketRepository, err := repositoryFactory.NewBucketRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create BucketRepository: %s", err))
		os.Exit(1)
	}
	objectRepository, err := repositoryFactory.NewObjectRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create ObjectRepository: %s", err))
		os.Exit(1)
	}
	partRepository, err := repositoryFactory.NewPartRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create PartRepository: %s", err))
		os.Exit(1)
	}
	tagRepository, err := repositoryFactory.NewTagRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create TagRepository: %s", err))
		os.Exit(1)
	}
	userMetadataRepository, err := repositoryFactory.NewUserMetadataRepository(db)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create UserMetadataRepository: %s", err))
		os.Exit(1)
	}
	sqlMetadataStore, err := New(db, bucketRepository, objectRepository, partRepository, tagRepository, userMetadataRepository)
	if err != nil {
		slog.Error(fmt.Sprintf("Could not create SqlMetadataStore: %s", err))
		os.Exit(1)
	}
	err = metadatastore.Tester(sqlMetadataStore, db)
	assert.Nil(t, err)
}
