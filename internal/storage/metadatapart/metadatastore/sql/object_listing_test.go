package sql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/tag"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// Embedding the interface deliberately leaves count and unbounded fetch methods
// unimplemented: calling either makes this regression test fail.
type seekingObjectRepository struct {
	object.Repository
	keys     []string
	minimums []string
}

func (r *seekingObjectRepository) FindNextObject(_ context.Context, _ *sql.Tx, _ storage.BucketName, prefix, startAfter, minimumKey string) (*object.Entity, error) {
	r.minimums = append(r.minimums, minimumKey)
	i := sort.Search(len(r.keys), func(i int) bool { return r.keys[i] >= prefix && r.keys[i] > startAfter && r.keys[i] >= minimumKey })
	if i == len(r.keys) {
		return nil, nil
	}
	upper := object.PrefixUpperBound(prefix)
	if upper != "" && r.keys[i] >= upper {
		return nil, nil
	}
	id := ulid.Make()
	return &object.Entity{Id: &id, Key: storage.MustNewObjectKey(r.keys[i])}, nil
}

type listingTagRepository struct{ tag.Repository }

func (r *listingTagRepository) FindTagsByObjectIdsOrderByObjectIdAndKeyAsc(context.Context, *sql.Tx, []ulid.ULID) ([]tag.Entity, error) {
	return nil, nil
}

func TestDelimiterListingSeeksPastHugeSubtree(t *testing.T) {
	for _, descendants := range []int{1, 100000} {
		t.Run(fmt.Sprint(descendants), func(t *testing.T) {
			repo := &seekingObjectRepository{}
			for i := range descendants {
				repo.keys = append(repo.keys, fmt.Sprintf("a/%06d", i))
			}
			// The successor itself must not be skipped by an exclusive seek.
			repo.keys = append(repo.keys, "a0", "z")
			store := &sqlMetadataStore{objectRepository: repo, tagRepository: &listingTagRepository{}}
			delimiter := "/"
			result, err := store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, MaxKeys: 1, SkipPartFetch: true})
			require.NoError(t, err)
			require.Equal(t, []string{"a/"}, result.CommonPrefixes)
			require.Empty(t, result.Objects)
			require.True(t, result.IsTruncated)
			require.Equal(t, []string{"", "a0"}, repo.minimums)

			repo.minimums = nil
			marker := "a/050000"
			result, err = store.listObjects(t.Context(), nil, storage.MustNewBucketName("bucket"), metadatastore.ListObjectsOptions{Delimiter: &delimiter, StartAfter: &marker, MaxKeys: 1, SkipPartFetch: true})
			require.NoError(t, err)
			require.Empty(t, result.CommonPrefixes)
			require.Len(t, result.Objects, 1)
			require.Equal(t, "a0", result.Objects[0].Key.String())
			require.True(t, result.IsTruncated)
			require.Equal(t, []string{"a0", "a0"}, repo.minimums)
		})
	}
}
