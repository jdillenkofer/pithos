package sql

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/objectinitiator"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

type multipartIdentityBucketRepository struct {
	bucket.Repository
}

func (r *multipartIdentityBucketRepository) FindBucketByName(context.Context, *sql.Tx, storage.BucketName) (*bucket.Entity, error) {
	return &bucket.Entity{OwnerAccountID: "owner"}, nil
}

type multipartIdentityObjectRepository struct {
	object.Repository
	uploads []object.Entity
}

func (r *multipartIdentityObjectRepository) CountUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarker(context.Context, *sql.Tx, storage.BucketName, string, string, string) (*int, error) {
	count := len(r.uploads)
	return &count, nil
}

func (r *multipartIdentityObjectRepository) FindUploadsByBucketNameAndPrefixAndKeyMarkerAndUploadIdMarkerOrderByKeyAscAndUploadIdAsc(_ context.Context, _ *sql.Tx, _ storage.BucketName, _ string, keyMarker string, uploadIDMarker string) ([]object.Entity, error) {
	var uploads []object.Entity
	for _, upload := range r.uploads {
		key := upload.Key.String()
		if key > keyMarker || (key == keyMarker && uploadIDMarker != "" && upload.UploadId.String() > uploadIDMarker) {
			uploads = append(uploads, upload)
		}
	}
	return uploads, nil
}

type multipartIdentityInitiatorRepository struct {
	objectinitiator.Repository
	requestedIDs []ulid.ULID
}

func (r *multipartIdentityInitiatorRepository) FindObjectInitiatorsByObjectIdsOrderByObjectId(_ context.Context, _ *sql.Tx, ids []ulid.ULID) ([]objectinitiator.Entity, error) {
	r.requestedIDs = append(r.requestedIDs, ids...)
	identities := make([]objectinitiator.Entity, 0, len(ids))
	for _, id := range ids {
		identities = append(identities, objectinitiator.Entity{ObjectId: id, AccountId: "writer-account", PrincipalId: "writer"})
	}
	return identities, nil
}

func TestListMultipartUploadsLoadsInitiatorsOnlyForReturnedUploads(t *testing.T) {
	// Exceed SQLite's bind limit with uploads that only contribute a common
	// prefix, plus two direct uploads of which only one fits on the page.
	uploads := make([]object.Entity, 0, 32769)
	for i := 0; i < 32769; i++ {
		id := ulid.Make()
		uploadID := storage.MustNewUploadId(id.String())
		key := fmt.Sprintf("folder/%05d", i)
		if i < 2 {
			key = fmt.Sprintf("direct-%d", i)
		}
		uploads = append(uploads, object.Entity{Id: &id, Key: storage.MustNewObjectKey(key), UploadId: &uploadID})
	}

	initiatorRepository := &multipartIdentityInitiatorRepository{}
	store := &sqlMetadataStore{
		bucketRepository:          &multipartIdentityBucketRepository{},
		objectRepository:          &multipartIdentityObjectRepository{uploads: uploads},
		objectInitiatorRepository: initiatorRepository,
		tracer:                    otel.Tracer("multipart-identity-test"),
	}
	delimiter := "/"
	result, err := store.ListMultipartUploads(context.Background(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListMultipartUploadsOptions{Delimiter: &delimiter, MaxUploads: 1})
	require.NoError(t, err)
	require.Len(t, result.Uploads, 1)
	require.Len(t, initiatorRepository.requestedIDs, 1)
	assert.Equal(t, *uploads[0].Id, initiatorRepository.requestedIDs[0])
	assert.Empty(t, result.CommonPrefixes)
	assert.True(t, result.IsTruncated)
	assert.Equal(t, "direct-0", result.NextKeyMarker)
	require.NotNil(t, result.Uploads[0].Initiator)
	assert.Equal(t, "writer", result.Uploads[0].Initiator.PrincipalID)
	assert.Equal(t, "owner", result.Uploads[0].Owner.AccountID)

	// A page containing only common prefixes needs no initiator lookup.
	initiatorRepository.requestedIDs = nil
	store.objectRepository = &multipartIdentityObjectRepository{uploads: uploads[2:]}
	result, err = store.ListMultipartUploads(context.Background(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListMultipartUploadsOptions{Delimiter: &delimiter, MaxUploads: 1})
	require.NoError(t, err)
	assert.Empty(t, result.Uploads)
	assert.Empty(t, initiatorRepository.requestedIDs)
	assert.Equal(t, []string{"folder/"}, result.CommonPrefixes)
	assert.False(t, result.IsTruncated)
}

func TestListMultipartUploadsDelimiterPagination(t *testing.T) {
	// Duplicate uploads at one key require the upload-ID marker; duplicate
	// prefixes must count only once and must be skipped when resuming.
	keys := []string{"a/1", "a/2", "b", "b", "c/1", "c/2", "d"}
	var uploads []object.Entity
	for _, key := range keys {
		id := ulid.Make()
		uploadID := storage.MustNewUploadId(id.String())
		uploads = append(uploads, object.Entity{Id: &id, Key: storage.MustNewObjectKey(key), UploadId: &uploadID})
	}
	for _, pageSize := range []int32{1, 2, 3, 5} {
		t.Run(fmt.Sprintf("page-size-%d", pageSize), func(t *testing.T) {
			store := &sqlMetadataStore{
				bucketRepository:          &multipartIdentityBucketRepository{},
				objectRepository:          &multipartIdentityObjectRepository{uploads: uploads},
				objectInitiatorRepository: &multipartIdentityInitiatorRepository{},
				tracer:                    otel.Tracer("multipart-pagination-test"),
			}
			delimiter := "/"
			opts := metadatastore.ListMultipartUploadsOptions{Delimiter: &delimiter, MaxUploads: pageSize}
			var prefixes []string
			var uploadIDs []storage.UploadId
			for page := 0; ; page++ {
				require.Less(t, page, 6, "pagination must make progress")
				result, err := store.ListMultipartUploads(context.Background(), nil, storage.MustNewBucketName("bucket"), opts)
				require.NoError(t, err)
				count := len(result.Uploads) + len(result.CommonPrefixes)
				assert.LessOrEqual(t, count, int(pageSize))
				prefixes = append(prefixes, result.CommonPrefixes...)
				for _, upload := range result.Uploads {
					uploadIDs = append(uploadIDs, upload.UploadId)
				}
				if !result.IsTruncated {
					break
				}
				assert.Equal(t, int(pageSize), count)
				opts.KeyMarker = &result.NextKeyMarker
				opts.UploadIdMarker = &result.NextUploadIdMarker
			}
			assert.Equal(t, []string{"a/", "c/"}, prefixes)
			assert.Equal(t, []storage.UploadId{*uploads[2].UploadId, *uploads[3].UploadId, *uploads[6].UploadId}, uploadIDs)
		})
	}
}
