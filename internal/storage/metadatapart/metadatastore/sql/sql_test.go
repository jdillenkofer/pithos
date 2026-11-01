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
	"github.com/stretchr/testify/require"
)

func TestSelectCompleteMultipartUploadParts(t *testing.T) {
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
		wantNumbers   []int
		wantErr       error
	}{
		{
			name: "matching parts",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1", ChecksumCRC32: &checksumCRC32},
				{PartNumber: 2, ETag: "\"etag-2\""},
			},
			wantNumbers: []int{1, 2},
		},
		{
			name:          "empty manifest",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{},
			wantErr:       metadatastore.ErrMalformedXML,
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
			name: "omitted first uploaded part",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 2, ETag: "etag-2"},
			},
			wantNumbers: []int{2},
		},
		{
			name: "omitted last uploaded part",
			declaredParts: []metadatastore.CompleteMultipartUploadPart{
				{PartNumber: 1, ETag: "etag-1"},
			},
			wantNumbers: []int{1},
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
			selectedParts, err := selectCompleteMultipartUploadParts(tt.declaredParts, storedParts, nil, false)
			if tt.wantErr == nil {
				assert.NoError(t, err)
				numbers := make([]int, len(selectedParts))
				for i, selectedPart := range selectedParts {
					numbers[i] = selectedPart.SequenceNumber
				}
				assert.Equal(t, tt.wantNumbers, numbers)
			} else {
				assert.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestSelectCompleteMultipartUploadPartsRequiresChecksumOfUploadAlgorithm(t *testing.T) {
	testutils.SkipIfIntegration(t)

	checksumSHA256 := "c2hhMjU2"
	storedParts := []part.Entity{
		{SequenceNumber: 1, ETag: "\"etag-1\"", ChecksumSHA256: &checksumSHA256},
		{SequenceNumber: 2, ETag: "\"etag-2\"", ChecksumSHA256: &checksumSHA256},
	}
	algorithm := "SHA256"

	_, err := selectCompleteMultipartUploadParts([]metadatastore.CompleteMultipartUploadPart{
		{PartNumber: 1, ETag: "etag-1", ChecksumSHA256: &checksumSHA256},
		{PartNumber: 2, ETag: "etag-2"},
	}, storedParts, &algorithm, true)
	var missingErr *metadatastore.MissingPartChecksumError
	require.ErrorAs(t, err, &missingErr)
	assert.Equal(t, int32(2), missingErr.PartNumber)
	assert.Equal(t, "The upload was created using a sha256 checksum. The complete request must include the checksum for each part. It was missing for part 2 in the request.", err.Error())

	selectedParts, err := selectCompleteMultipartUploadParts([]metadatastore.CompleteMultipartUploadPart{
		{PartNumber: 1, ETag: "etag-1", ChecksumSHA256: &checksumSHA256},
		{PartNumber: 2, ETag: "etag-2", ChecksumSHA256: &checksumSHA256},
	}, storedParts, &algorithm, true)
	require.NoError(t, err)
	assert.Len(t, selectedParts, 2)
}

func TestSelectCompleteMultipartUploadPartsNumbering(t *testing.T) {
	testutils.SkipIfIntegration(t)

	checksum := "AAAAAA=="
	storedParts := []part.Entity{
		{SequenceNumber: 1, ETag: "etag-1", ChecksumCRC32: &checksum},
		{SequenceNumber: 2, ETag: "etag-2", ChecksumCRC32: &checksum},
		{SequenceNumber: 3, ETag: "etag-3", ChecksumCRC32: &checksum},
		{SequenceNumber: 5, ETag: "etag-5", ChecksumCRC32: &checksum},
	}

	for _, mode := range []string{"no additional checksum", "COMPOSITE", "FULL_OBJECT"} {
		t.Run(mode, func(t *testing.T) {
			var requiredAlgorithm *string
			if mode == "COMPOSITE" {
				algorithm := "CRC32"
				requiredAlgorithm = &algorithm
			}
			requireConsecutive := mode != "no additional checksum"
			for _, tt := range []struct {
				name    string
				numbers []int32
				invalid bool
			}{
				{name: "consecutive selected subset", numbers: []int32{1, 2}},
				{name: "single first part", numbers: []int32{1}},
				{name: "gaps", numbers: []int32{1, 3, 5}, invalid: requireConsecutive},
				{name: "starts above one", numbers: []int32{2, 3}, invalid: requireConsecutive},
				{name: "single later part", numbers: []int32{3}, invalid: requireConsecutive},
				{name: "descending", numbers: []int32{2, 1}, invalid: true},
				{name: "duplicate", numbers: []int32{1, 1}, invalid: true},
			} {
				t.Run(tt.name, func(t *testing.T) {
					manifest := make([]metadatastore.CompleteMultipartUploadPart, len(tt.numbers))
					for i, number := range tt.numbers {
						manifest[i] = metadatastore.CompleteMultipartUploadPart{
							PartNumber: number,
							ETag:       fmt.Sprintf("etag-%d", number),
						}
						if requiredAlgorithm != nil {
							manifest[i].ChecksumCRC32 = &checksum
						}
					}
					selected, err := selectCompleteMultipartUploadParts(manifest, storedParts, requiredAlgorithm, requireConsecutive)
					if tt.invalid {
						require.ErrorIs(t, err, metadatastore.ErrInvalidPartOrder)
						return
					}
					require.NoError(t, err)
					require.Len(t, selected, len(tt.numbers))
					for i, number := range tt.numbers {
						assert.Equal(t, int(number), selected[i].SequenceNumber)
					}
				})
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
