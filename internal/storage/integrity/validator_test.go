package integrity

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart"
	sqlmetadata "github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func newStorage(t *testing.T, named bool) (storage.Storage, string) {
	t.Helper()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "metadata.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	buckets, err := repository.NewBucketRepository(db)
	require.NoError(t, err)
	objects, err := repository.NewObjectRepository(db)
	require.NoError(t, err)
	parts, err := repository.NewPartRepository(db)
	require.NoError(t, err)
	tags, err := repository.NewTagRepository(db)
	require.NoError(t, err)
	metadata, err := repository.NewUserMetadataRepository(db)
	require.NoError(t, err)
	meta, err := sqlmetadata.New(db, buckets, objects, parts, tags, metadata)
	require.NoError(t, err)
	root := t.TempDir()
	defaultStore, err := filesystem.New(root)
	require.NoError(t, err)
	var extras map[string]partstore.PartStore
	var classes map[string]string
	if named {
		root = t.TempDir()
		coldStore, err := filesystem.New(root)
		require.NoError(t, err)
		extras = map[string]partstore.PartStore{"cold": coldStore}
		classes = map[string]string{"GLACIER": "cold"}
	}
	store, err := metadatapart.NewStorageWithNamedPartStores(db, meta, defaultStore, extras, classes)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Stop(context.Background())) })
	return store, root
}

func TestValidateStorageAndNamedStores(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, named := range []bool{false, true} {
		name := "default"
		if named {
			name = "named"
		}
		t.Run(name, func(t *testing.T) {
			store, _ := newStorage(t, named)
			ctx := t.Context()
			bucket := storage.MustNewBucketName("integrity-test")
			require.NoError(t, store.CreateBucket(ctx, bucket))
			for _, content := range []string{"", "some content"} {
				_, err := store.PutObject(ctx, bucket, storage.MustNewObjectKey("object"+content), nil, strings.NewReader(content), nil, &storage.PutObjectOptions{StorageClass: ptrutils.ToPtr("GLACIER")})
				require.NoError(t, err)
			}
			// Inspection must survive ordinary storage middleware without reflection.
			wrapped := &delegator.DelegatingStorage{Next: store}
			report, err := NewValidator(wrapped, false, false).ValidateAll(ctx)
			require.NoError(t, err)
			require.Equal(t, 2, report.SuccessfulObjects)
			require.False(t, report.HasFailures(), "%+v", report.Results)
			require.False(t, report.EndTime.IsZero())
		})
	}
}

func TestValidateHistoricalVersionsAndDeleteOnlyCorruptVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	store, root := newStorage(t, true)
	ctx := t.Context()
	bucket := storage.MustNewBucketName("versioned-bucket")
	key := storage.MustNewObjectKey("object")
	require.NoError(t, store.CreateBucket(ctx, bucket))
	require.NoError(t, store.PutBucketVersioningConfiguration(ctx, bucket, &storage.BucketVersioningConfiguration{Status: ptrutils.ToPtr(storage.BucketVersioningStatusEnabled)}))
	old, err := store.PutObject(ctx, bucket, key, nil, strings.NewReader("old content"), nil, &storage.PutObjectOptions{StorageClass: ptrutils.ToPtr("GLACIER")})
	require.NoError(t, err)
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	// Change the bytes without changing their length, testing checksum detection.
	require.NoError(t, os.WriteFile(filepath.Join(root, entries[0].Name()), []byte("bad content"), 0600))
	latest, err := store.PutObject(ctx, bucket, key, nil, strings.NewReader("latest content"), nil, nil)
	require.NoError(t, err)
	marker, err := store.DeleteObject(ctx, bucket, key, nil)
	require.NoError(t, err)
	report, err := NewValidator(store, true, true).ValidateAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, report.TotalObjects)
	require.Equal(t, 1, report.SuccessfulObjects)
	require.Equal(t, 1, report.FailedObjects)
	require.Equal(t, 1, report.DeletedObjects)
	for _, result := range report.Results {
		if !result.Success {
			require.Equal(t, old.VersionID, result.VersionID)
			require.True(t, result.Corrupted)
		}
	}
	_, err = store.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: old.VersionID})
	require.ErrorIs(t, err, storage.ErrNoSuchKey)
	_, err = store.HeadObject(ctx, bucket, key, &storage.HeadObjectOptions{VersionID: latest.VersionID})
	require.NoError(t, err)
	versions, err := store.ListObjectVersions(ctx, bucket, storage.ListObjectVersionsOptions{MaxKeys: 1000})
	require.NoError(t, err)
	require.Len(t, versions.Versions, 2)
	require.Equal(t, *marker.VersionID, versions.Versions[0].VersionID)
}

// Embedding Storage keeps the fake focused: unexpected method calls fail the test.
type fakeStorage struct {
	storage.Storage
	list    func(storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error)
	inspect func(func(storage.Object, []storage.IntegrityPart) error) error
	deleted []*storage.DeleteObjectOptions
}

func (s *fakeStorage) ListBuckets(context.Context) ([]storage.Bucket, error) {
	return []storage.Bucket{{Name: storage.MustNewBucketName("test-bucket")}}, nil
}
func (s *fakeStorage) ListObjectVersions(_ context.Context, _ storage.BucketName, opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
	return s.list(opts)
}
func (s *fakeStorage) InspectObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, _ *string, inspect func(storage.Object, []storage.IntegrityPart) error) error {
	return s.inspect(inspect)
}
func (s *fakeStorage) DeleteObject(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, opts *storage.DeleteObjectOptions) (*storage.DeleteObjectResult, error) {
	s.deleted = append(s.deleted, opts)
	return &storage.DeleteObjectResult{}, nil
}

func checksums(t *testing.T, content string) storage.ChecksumValues {
	t.Helper()
	_, calculated, err := checksumutils.CalculateChecksumsStreaming(t.Context(), strings.NewReader(content), func(r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	})
	require.NoError(t, err)
	return *calculated
}

func oneObjectFake(t *testing.T) (*fakeStorage, storage.Object, []storage.IntegrityPart) {
	t.Helper()
	values := checksums(t, "data")
	key := storage.MustNewObjectKey("object")
	object := storage.Object{Key: key, Size: 4, ETag: *values.ETag}
	parts := []storage.IntegrityPart{{ID: "part", Size: 4, Checksums: values, Open: func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("data")), nil }}}
	s := &fakeStorage{list: func(storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
		return &storage.ListObjectVersionsResult{Versions: []storage.ObjectVersion{{Key: key, VersionID: "null", ETag: values.ETag}}}, nil
	}}
	s.inspect = func(visit func(storage.Object, []storage.IntegrityPart) error) error { return visit(object, parts) }
	return s, object, parts
}

func TestValidationReadFailuresNeverDelete(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, failure := range []string{"open", "read", "close", "transaction"} {
		t.Run(failure, func(t *testing.T) {
			s, object, parts := oneObjectFake(t)
			parts[0].Open = func() (io.ReadCloser, error) {
				switch failure {
				case "open":
					return nil, errors.New("backend unavailable")
				case "read":
					return &failedReader{}, nil
				case "close":
					return &closeFailure{Reader: strings.NewReader("data")}, nil
				default:
					return io.NopCloser(strings.NewReader("oops")), nil
				}
			}
			s.inspect = func(visit func(storage.Object, []storage.IntegrityPart) error) error {
				err := visit(object, parts)
				if failure == "transaction" {
					return errors.New("commit failed")
				}
				return err
			}
			report, err := NewValidator(s, true, true).ValidateAll(t.Context())
			require.NoError(t, err)
			require.Equal(t, 1, report.FailedObjects)
			require.False(t, report.Results[0].Corrupted)
			require.Empty(t, s.deleted)
		})
	}
}

type failedReader struct{}

func (*failedReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (*failedReader) Close() error             { return nil }

type closeFailure struct{ io.Reader }

func (*closeFailure) Close() error { return errors.New("close failed") }

func TestValidationPaginationAndListingErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s, _, _ := oneObjectFake(t)
	calls := 0
	s.list = func(opts storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
		calls++
		if calls == 1 {
			return &storage.ListObjectVersionsResult{IsTruncated: true, NextKeyMarker: ptrutils.ToPtr("key"), NextVersionIDMarker: ptrutils.ToPtr("version")}, nil
		}
		require.Equal(t, "key", *opts.KeyMarker)
		require.Equal(t, "version", *opts.VersionIDMarker)
		return nil, errors.New("listing unavailable")
	}
	report, err := NewValidator(s, false, false).ValidateAll(t.Context())
	require.ErrorContains(t, err, "listing unavailable")
	require.NotNil(t, report)
	require.False(t, report.EndTime.IsZero())
	require.Equal(t, 2, calls)
}

func TestValidationReportIncludesVersion(t *testing.T) {
	testutils.SkipIfIntegration(t)
	report := &ValidationReport{FailedObjects: 1, Results: []ValidationResult{{BucketName: "bucket", ObjectKey: "key", VersionID: ptrutils.ToPtr("version-1"), ErrorType: "Checksum mismatch"}}}
	var output bytes.Buffer
	require.NoError(t, OutputHumanReadable(report, &output))
	require.Contains(t, output.String(), "Version: version-1")
	output.Reset()
	require.NoError(t, OutputJSON(report, &output))
	require.Contains(t, output.String(), `"VersionID": "version-1"`)
}

func TestValidationUnsupportedBackend(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := NewValidator(&delegator.DelegatingStorage{}, false, false).ValidateAll(t.Context())
	require.ErrorContains(t, err, "does not support integrity inspection")
}

func TestValidationCancelledDoesNotDelete(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s, _, _ := oneObjectFake(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := NewValidator(s, true, true).ValidateAll(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, s.deleted)
}

func TestValidateMultipartChecksums(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, count := range []int{1, 2} {
		for _, checksumType := range []string{storage.ChecksumTypeComposite, storage.ChecksumTypeFullObject} {
			t.Run(checksumType+strings.Repeat("-part", count), func(t *testing.T) {
				s, object, parts := oneObjectFake(t)
				var calculated []checksumutils.PartChecksums
				for i := 0; i < count; i++ {
					p := parts[0]
					calculated = append(calculated, checksumutils.PartChecksums{ETag: *p.Checksums.ETag, Size: p.Size, ChecksumCRC32: p.Checksums.ChecksumCRC32, ChecksumCRC32C: p.Checksums.ChecksumCRC32C, ChecksumCRC64NVME: p.Checksums.ChecksumCRC64NVME, ChecksumSHA1: p.Checksums.ChecksumSHA1, ChecksumSHA256: p.Checksums.ChecksumSHA256})
				}
				values, err := checksumutils.CalculateMultipartChecksums(calculated, checksumType)
				require.NoError(t, err)
				object.ETag, object.Size, object.ChecksumType = *values.ETag, int64(count)*4, &checksumType
				object.ChecksumCRC32, object.ChecksumCRC32C, object.ChecksumCRC64NVME = values.ChecksumCRC32, values.ChecksumCRC32C, values.ChecksumCRC64NVME
				object.ChecksumSHA1, object.ChecksumSHA256 = values.ChecksumSHA1, values.ChecksumSHA256
				// Keep the listing consistent with the multipart object's metadata.
				s.list = func(storage.ListObjectVersionsOptions) (*storage.ListObjectVersionsResult, error) {
					return &storage.ListObjectVersionsResult{Versions: []storage.ObjectVersion{{Key: object.Key, VersionID: "null", ETag: &object.ETag}}}, nil
				}
				if count == 2 {
					parts = append(parts, parts[0])
				}
				s.inspect = func(visit func(storage.Object, []storage.IntegrityPart) error) error { return visit(object, parts) }
				report, err := NewValidator(s, false, false).ValidateAll(t.Context())
				require.NoError(t, err)
				require.False(t, report.HasFailures(), "%+v", report.Results)
			})
		}
	}
}
